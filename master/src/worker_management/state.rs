use std::collections::HashMap;
use std::collections::VecDeque;

use chrono::prelude::*;
use serde_json;

use cerberus_proto::worker as pb;
use common::{Task, TaskStatus, Worker};
use errors::*;
use state;

const MAX_TASK_FAILURE_COUNT: u16 = 10;
const MAX_TASK_ASSIGNMENT_FAILURE: u16 = 5;

#[derive(Default)]
pub struct State {
    // A map of worker id to worker.
    workers: HashMap<String, Worker>,
    // A map of task id to task.
    tasks: HashMap<String, Task>,
    // Tasks that are in the queue to be ran.
    task_queue: VecDeque<String>,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_worker_count(&self) -> u32 {
        self.workers.len() as u32
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

        Ok(())
    }

    pub fn process_reduce_task_result(&mut self, reduce_result: &pb::ReduceResult) -> Result<Task> {
        if reduce_result.status == pb::ResultStatus::SUCCESS {
            let mut scheduled_task = self.tasks.remove(reduce_result.get_task_id()).chain_err(
                || {
                    format!(
                        "Task with ID {} not found, Worker ID: {}.",
                        reduce_result.get_task_id(),
                        reduce_result.get_worker_id()
                    )
                },
            )?;

            if scheduled_task.id != reduce_result.task_id {
                return Err("Task id does not match expected task id.".into());
            }

            scheduled_task.status = TaskStatus::Complete;
            scheduled_task.time_completed = Some(Utc::now());
            scheduled_task.cpu_time = reduce_result.get_cpu_time();
            return Ok(scheduled_task);
        }

        self.task_failed(
            reduce_result.get_worker_id(),
            reduce_result.get_task_id(),
            reduce_result.get_failure_details(),
        ).chain_err(|| "Error marking task as failed")
    }

    pub fn process_map_task_result(&mut self, map_result: &pb::MapResult) -> Result<Task> {
        if map_result.status == pb::ResultStatus::SUCCESS {
            let mut scheduled_task = self.tasks.remove(map_result.get_task_id()).chain_err(|| {
                format!(
                    "Task with ID {} not found, Worker ID: {}.",
                    map_result.get_task_id(),
                    map_result.get_worker_id()
                )
            })?;

            if scheduled_task.id != map_result.task_id {
                return Err("Task id does not match expected task id.".into());
            }

            let worker = self.workers.get_mut(&map_result.worker_id).chain_err(|| {
                format!("Worker with ID {} not found.", map_result.worker_id)
            })?;

            for (partition, output_file) in map_result.get_map_results() {
                scheduled_task.map_output_files.insert(
                    *partition,
                    format!(
                        "{}{}",
                        worker.address,
                        output_file
                    ),
                );
            }
            scheduled_task.status = TaskStatus::Complete;
            scheduled_task.time_completed = Some(Utc::now());
            scheduled_task.cpu_time = map_result.get_cpu_time();

            return Ok(scheduled_task);
        }

        self.task_failed(
            map_result.get_worker_id(),
            map_result.get_task_id(),
            map_result.get_failure_details(),
        ).chain_err(|| "Error marking task as failed")
    }

    fn task_failed(
        &mut self,
        worker_id: &str,
        task_id: &str,
        failure_details: &str,
    ) -> Result<(Task)> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        worker.current_task_id = String::new();

        let mut assigned_task = self.tasks.remove(task_id).chain_err(|| {
            format!(
                "Task with ID {} not found, Worker ID: {}.",
                task_id,
                worker_id
            )
        })?;

        if failure_details != "" {
            assigned_task.failure_details = Some(failure_details.to_owned());
        }
        assigned_task.failure_count += 1;

        if assigned_task.failure_count > MAX_TASK_FAILURE_COUNT {
            assigned_task.status = TaskStatus::Failed;
            assigned_task.time_completed = Some(Utc::now());
        } else {
            assigned_task.status = TaskStatus::Queued;
            self.tasks.insert(
                task_id.clone().to_owned(),
                assigned_task.clone(),
            );
            self.task_queue.push_front(task_id.clone().to_owned());
        }

        Ok(assigned_task)
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
        if let Some(worker) = self.workers.remove(worker_id) {

            // If this worker is a assigned a task, requeue the task.
            if !worker.current_task_id.is_empty() {
                self.task_queue.push_front(worker.current_task_id);
            }
        } else {
            return Err(format!("Worker with ID {} not found.", worker_id).into());
        }

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.tasks.insert(task.id.clone(), task.clone());
        self.task_queue.push_back(task.id);
    }

    // Unassign a task assigned to a worker and put the task back in the queue.
    pub fn unassign_worker(&mut self, worker_id: &str) -> Result<()> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;

        if !worker.current_task_id.is_empty() {
            self.task_queue.push_front(worker.current_task_id.clone());
        }

        worker.current_task_id = String::new();

        Ok(())
    }

    // Assigns a given task_id to a worker
    fn assign_worker_task(&mut self, worker_id: &str, task_id: &str) -> Result<(Task)> {
        let assigned_task = {
            if let Some(scheduled_task) = self.tasks.get_mut(task_id) {
                scheduled_task.status = TaskStatus::InProgress;
                if scheduled_task.time_started == None {
                    scheduled_task.time_started = Some(Utc::now());
                }
                scheduled_task.assigned_worker_id = worker_id.clone().to_owned();

                scheduled_task.clone()
            } else {
                return Err(format!("Could not get Task with ID {}", task_id).into());
            }
        };

        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;
        worker.current_task_id = task_id.clone().to_owned();

        Ok(assigned_task)
    }

    // Tries to assign a worker the next task in the queue.
    // Returns the task if one exists.
    pub fn try_assign_worker_task(&mut self, worker_id: &str) -> Result<(Option<Task>)> {
        let scheduled_task_id = match self.task_queue.pop_front() {
            Some(scheduled_task_id) => scheduled_task_id,
            None => return Ok(None),
        };

        match self.assign_worker_task(worker_id, &scheduled_task_id) {
            Ok(task) => Ok(Some(task)),
            Err(err) => Err(err),
        }
    }

    pub fn has_task(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    pub fn increment_failed_task_assignments(&mut self, worker_id: &str) -> Result<()> {
        let should_remove_worker = {
            let worker = self.workers.get_mut(worker_id).chain_err(|| {
                format!("Worker with ID {} not found.", worker_id)
            })?;

            worker.task_assignments_failed += 1;
            worker.task_assignments_failed == MAX_TASK_ASSIGNMENT_FAILURE
        };

        if should_remove_worker {
            self.remove_worker(worker_id).chain_err(|| {
                format!(
                    "Error removing Worker with id {} because of task assignment failure.",
                    worker_id
                )
            })?;
        }

        Ok(())
    }

    pub fn reset_failed_task_assignments(&mut self, worker_id: &str) -> Result<()> {
        let worker = self.workers.get_mut(worker_id).chain_err(|| {
            format!("Worker with ID {} not found.", worker_id)
        })?;
        worker.task_assignments_failed = 0;

        Ok(())
    }
}

impl state::StateHandling for State {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create WorkerManager State from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut workers_json: Vec<serde_json::Value> = Vec::new();
        for worker in self.workers.values() {
            workers_json.push(worker.dump_state().chain_err(
                || "Error dumping worker state.",
            )?);
        }

        let mut tasks_json: Vec<serde_json::Value> = Vec::new();
        for task in self.tasks.values() {
            tasks_json.push(task.dump_state().chain_err(|| "Error dumping task state.")?);
        }

        Ok(json!({
            "workers": json!(workers_json),
            "task_queue": json!(self.task_queue),
            "tasks": json!(tasks_json),
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        if let serde_json::Value::Array(ref workers_array) = data["workers"] {
            for worker in workers_array {
                let worker = Worker::new_from_json(worker.clone()).chain_err(
                    || "Unable to create ScheduledJob from json.",
                )?;
                self.workers.insert(worker.worker_id.to_owned(), worker);
            }
        } else {
            return Err("Error processing workers array.".into());
        }

        if let serde_json::Value::Array(ref task_queue) = data["task_queue"] {
            for task_id in task_queue {
                let task_id_str: String = serde_json::from_value(task_id.clone()).chain_err(
                    || "Error processing task_id in tasks queue",
                )?;

                self.task_queue.push_front(task_id_str);
            }
        } else {
            return Err("Error processing tasks queue.".into());
        }

        if let serde_json::Value::Array(ref tasks_array) = data["tasks"] {
            for task in tasks_array {
                let task = Task::new_from_json(task.clone()).chain_err(
                    || "Unable to create Task from json.",
                )?;
                debug!("Loaded task from state:\n {:?}", task);
                self.tasks.insert(task.id.to_owned(), task);
            }
        } else {
            return Err("Error processing tasks array.".into());
        }

        Ok(())
    }
}
