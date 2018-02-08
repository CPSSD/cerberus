use std::collections::HashMap;
use std::collections::VecDeque;

use chrono::prelude::*;

use cerberus_proto::worker as pb;
use common::{Task, TaskStatus, Worker};
use errors::*;

const MAX_TASK_FAILURE_COUNT: u16 = 10;

#[derive(Default)]
pub struct State {
    // A map of worker id to worker.
    workers: HashMap<String, Worker>,
    // Tasks that are in the queue to be ran.
    task_queue: VecDeque<Task>,
    // Tasks that are currently running on a worker.
    // A map of worker id to ScheduledTask.
    assigned_tasks: HashMap<String, Task>,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_workers(&self) -> Vec<&Worker> {
        let mut workers = Vec::new();
        for worker in self.workers.values() {
            workers.push(worker)
        }
        workers
    }

    // Returns a list of worker not currently assigned a task sorted by most recent health checks.
    pub fn get_available_workers(&self) -> Vec<String> {
        let mut workers = Vec::new();
        for worker in self.workers.values() {
            if worker.current_task_id == "" {
                workers.push(worker)
            }
        }

        // Sort workers by most recent health checks, to avoid repeatedly trying to assign work to a
        // worker which is not responding.
        workers.sort_by(|a, b| {
            a.status_last_updated.cmp(&b.status_last_updated).reverse()
        });

        workers
            .into_iter()
            .map(|worker| worker.worker_id.clone())
            .collect()
    }

    pub fn add_worker(&mut self, worker: Worker) -> Result<()> {
        if self.workers.contains_key(&worker.worker_id) {
            return Err(
                format!(
                    "Worker with ID {} is already registered.",
                    &worker.worker_id
                ).into(),
            );
        }
        self.workers.insert(worker.worker_id.clone(), worker);
        Ok(())
    }

    pub fn update_worker_status(
        &mut self,
        worker_id: &str,
        worker_status: pb::WorkerStatus,
        operation_status: pb::OperationStatus,
    ) -> Result<()> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        worker.status = worker_status;
        worker.operation_status = operation_status;
        worker.status_last_updated = Utc::now();

        if !worker.current_task_id.is_empty() &&
            (operation_status == pb::OperationStatus::FAILED ||
                 operation_status == pb::OperationStatus::COMPLETE)
        {
            if let Some(assigned_task) = self.assigned_tasks.remove(worker_id) {
                self.task_queue.push_front(assigned_task);
            }
            worker.current_task_id = String::new();
        }

        Ok(())
    }

    pub fn process_reduce_task_result(&mut self, reduce_result: &pb::ReduceResult) -> Result<Task> {
        if reduce_result.status == pb::ResultStatus::SUCCESS {
            let mut scheduled_task = self.assigned_tasks
                .remove(reduce_result.get_worker_id())
                .chain_err(|| {
                    format!(
                        "Task not found for Worker with ID {}.",
                        reduce_result.get_worker_id(),
                    )
                })?;

            scheduled_task.status = TaskStatus::Complete;
            scheduled_task.cpu_time = reduce_result.get_cpu_time();
            return Ok(scheduled_task);
        } else {
            return self.task_failed(
                reduce_result.get_worker_id(),
                reduce_result.get_failure_details(),
            ).chain_err(|| "Error marking task as failed");
        }
    }

    pub fn process_map_task_result(&mut self, map_result: &pb::MapResult) -> Result<Task> {
        if map_result.status == pb::ResultStatus::SUCCESS {
            let mut scheduled_task = self.assigned_tasks
                .remove(map_result.get_worker_id())
                .chain_err(|| {
                    format!(
                        "Task not found for Worker with ID {}.",
                        map_result.get_worker_id(),
                    )
                })?;

            for (partition, output_file) in map_result.get_map_results() {
                scheduled_task.map_output_files.insert(
                    *partition,
                    output_file.to_owned(),
                );
            }
            scheduled_task.status = TaskStatus::Complete;
            scheduled_task.cpu_time = map_result.get_cpu_time();
            return Ok(scheduled_task);
        } else {
            return self.task_failed(map_result.get_worker_id(), map_result.get_failure_details())
                .chain_err(|| "Error marking task as failed");
        }
    }

    fn task_failed(&mut self, worker_id: &str, failure_details: &str) -> Result<(Task)> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        worker.current_task_id = String::new();

        let mut assigned_task = self.assigned_tasks.remove(worker_id).chain_err(|| {
            format!(
                        "Task not found for Worker with ID {}.",
                        worker_id,
                    )
        })?;

        if failure_details != "" {
            assigned_task.failure_details = Some(failure_details.to_owned());
        }
        assigned_task.failure_count += 1;

        if assigned_task.failure_count > MAX_TASK_FAILURE_COUNT {
            assigned_task.status = TaskStatus::Failed;
        } else {
            assigned_task.status = TaskStatus::Queued;
            self.task_queue.push_front(assigned_task.clone());
        }

        return Ok(assigned_task);
    }

    // Mark that a given worker has returned a result for it's task.
    pub fn set_worker_operation_completed(
        &mut self,
        worker_id: &str,
        result: pb::ResultStatus,
    ) -> Result<()> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        worker.status = pb::WorkerStatus::AVAILABLE;
        if result == pb::ResultStatus::SUCCESS {
            worker.operation_status = pb::OperationStatus::COMPLETE;
        } else {
            worker.operation_status = pb::OperationStatus::FAILED;
        }
        worker.status_last_updated = Utc::now();
        worker.current_task_id = String::new();

        Ok(())
    }

    pub fn remove_worker(&mut self, worker_id: &str) -> Result<()> {
        if !self.workers.contains_key(worker_id) {
            return Err(format!("Worker with ID {} not found.", worker_id).into());
        }

        // If this worker is a assigned a task, requeue the task.
        if let Some(assigned_task) = self.assigned_tasks.remove(worker_id) {
            self.task_queue.push_front(assigned_task);
        }
        self.workers.remove(worker_id);

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        // TODO(conor): Don't add tasks that we already have some info for.
        self.task_queue.push_back(task);
    }

    // Unassign a task assigned to a worker and put the task back in the queue.
    pub fn unassign_worker(&mut self, worker_id: &str) -> Result<()> {
        if let Some(assigned_task) = self.assigned_tasks.remove(worker_id) {
            self.task_queue.push_front(assigned_task);
        }

        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        worker.current_task_id = String::new();

        Ok(())
    }

    // Tries to assign a worker the next task in the queue.
    // Returns the task if one exists.
    pub fn try_assign_worker_task(&mut self, worker_id: &str) -> Result<(Option<Task>)> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        let mut scheduled_task = match self.task_queue.pop_front() {
            Some(scheduled_task) => scheduled_task,
            None => return Ok(None),
        };

        scheduled_task.status = TaskStatus::InProgress;

        let task = scheduled_task.clone();
        worker.current_task_id = scheduled_task.id.to_owned();
        self.assigned_tasks.insert(
            worker_id.to_owned(),
            scheduled_task,
        );

        Ok(Some(task))
    }
}
