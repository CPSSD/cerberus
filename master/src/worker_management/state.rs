use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::prelude::*;
use serde_json;

use cerberus_proto::worker as pb;
use common::{PriorityTask, Task, TaskStatus, TaskType, Worker};
use errors::*;
use util::data_layer::AbstractionLayer;
use util::state::StateHandling;

const MAX_TASK_FAILURE_COUNT: u16 = 10;
const MAX_TASK_ASSIGNMENT_FAILURE: u16 = 5;

// Priority for different task types.
const DEFAULT_TASK_PRIORITY: u32 = 10;
const FAILED_TASK_PRIORITY: u32 = 20;
const REQUEUED_TASK_PRIORITY: u32 = 15;

// Max tasks to consider from the top of the task queue when trying to find the best task to assign.
const MAX_TASKS_TO_CONSIDER: u32 = 5;

// If a worker reports being available this many seconds after being assigned a task, assume that
// the masters view of the worker is out of sync and reassign the task.
const TIME_REASSIGN_REPORTING_AVAILABLE_S: i64 = 10;

pub struct State {
    // A map of worker id to worker.
    workers: HashMap<String, Worker>,
    // A map of task id to task.
    tasks: HashMap<String, Task>,
    // A map of task id to task for completed map tasks of running jobs.
    completed_tasks: HashMap<String, Task>,
    // Prioritised list of tasks that are in the queue to be ran.
    priority_task_queue: BinaryHeap<PriorityTask>,

    data_layer: Arc<AbstractionLayer + Send + Sync>,
}

impl State {
    pub fn new(data_layer: Arc<AbstractionLayer + Send + Sync>) -> Self {
        State {
            workers: HashMap::new(),
            tasks: HashMap::new(),

            completed_tasks: HashMap::new(),
            priority_task_queue: BinaryHeap::new(),

            data_layer,
        }
    }

    pub fn remove_queued_tasks_for_job(&mut self, job_id: &str) -> Result<()> {
        let mut new_priority_queue: BinaryHeap<PriorityTask> = BinaryHeap::new();

        self.tasks.retain(|_, v| v.job_id != job_id);

        for priority_task in self.priority_task_queue.drain() {
            if let Some(task) = self.tasks.get_mut(&priority_task.id.clone()) {
                if task.job_id != job_id {
                    new_priority_queue.push(priority_task);
                }
            }
        }

        self.priority_task_queue = new_priority_queue;

        Ok(())
    }

    fn get_in_progress_tasks_for_job(&self, job_id: &str) -> Result<Vec<String>> {
        let mut tasks: Vec<String> = Vec::new();

        for task in self.tasks.values() {
            if task.job_id == job_id {
                tasks.push(task.id.clone());
            }
        }

        Ok(tasks)
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

    pub fn get_tasks(&self) -> Vec<&Task> {
        let mut tasks = Vec::new();
        for task in self.tasks.values() {
            tasks.push(task)
        }
        tasks
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
        workers.sort_by(|a, b| a.status_last_updated.cmp(&b.status_last_updated).reverse());

        workers
            .into_iter()
            .map(|worker| worker.worker_id.clone())
            .collect()
    }

    pub fn add_worker(&mut self, worker: Worker) -> Result<()> {
        if self.workers.contains_key(&worker.worker_id) {
            return Err(format!(
                "Worker with ID {} is already registered.",
                &worker.worker_id
            ).into());
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
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

        worker.status = worker_status;
        worker.operation_status = operation_status;
        worker.status_last_updated = Utc::now();

        // If a worker has not been recently assigned a task and is reporting available,
        // we assume that the masters view of the workers current task is out of date and
        // reassign the worker.
        // If the worker has recently been assigned a task, it may not be reporting the most up to
        // date status.
        let time_since_task_assigned =
            Utc::now().timestamp() - worker.task_last_updated.timestamp();
        if time_since_task_assigned > TIME_REASSIGN_REPORTING_AVAILABLE_S
            && worker_status == pb::WorkerStatus::AVAILABLE
            && !worker.current_task_id.is_empty()
        {
            if let Some(assigned_task) = self.tasks.get_mut(&worker.current_task_id) {
                self.priority_task_queue.push(PriorityTask::new(
                    worker.current_task_id.clone(),
                    REQUEUED_TASK_PRIORITY * assigned_task.job_priority,
                ));
                assigned_task.assigned_worker_id = String::new();
            }
            worker.current_task_id = String::new();
        }

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

            let worker = self.workers
                .get_mut(&map_result.worker_id)
                .chain_err(|| format!("Worker with ID {} not found.", map_result.worker_id))?;

            for (partition, output_file) in map_result.get_map_results() {
                scheduled_task
                    .map_output_files
                    .insert(*partition, format!("{}{}", worker.address, output_file));
            }
            scheduled_task.status = TaskStatus::Complete;
            scheduled_task.time_completed = Some(Utc::now());
            scheduled_task.cpu_time = map_result.get_cpu_time();

            self.completed_tasks
                .insert(scheduled_task.id.clone(), scheduled_task.clone());

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
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

        worker.current_task_id = String::new();

        let mut assigned_task = self.tasks.remove(task_id).chain_err(|| {
            format!(
                "Task with ID {} not found, Worker ID: {}.",
                task_id, worker_id
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
            self.tasks.insert(task_id.to_owned(), assigned_task.clone());
            self.priority_task_queue.push(PriorityTask::new(
                task_id.to_owned().clone(),
                FAILED_TASK_PRIORITY * assigned_task.job_priority,
            ));
        }

        Ok(assigned_task)
    }

    pub fn get_workers_running_job(&self, job_id: &str) -> Result<Vec<String>> {
        let mut worker_ids = Vec::new();

        let workers = self.get_workers();
        for worker in workers {
            if let Some(task) = self.tasks.get(&worker.current_task_id) {
                if task.job_id == job_id {
                    worker_ids.push(worker.worker_id.clone());
                }
            }
        }

        Ok(worker_ids)
    }

    // Mark that a given worker has returned a result for it's task.
    pub fn set_worker_operation_completed(
        &mut self,
        worker_id: &str,
        result: pb::ResultStatus,
    ) -> Result<()> {
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

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

    pub fn set_worker_operation_cancelled(&mut self, worker_id: &str) -> Result<()> {
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

        worker.status = pb::WorkerStatus::AVAILABLE;
        worker.operation_status = pb::OperationStatus::CANCELLED;
        worker.status_last_updated = Utc::now();
        worker.current_task_id = String::new();

        Ok(())
    }

    pub fn remove_worker(&mut self, worker_id: &str) -> Result<()> {
        if let Some(worker) = self.workers.remove(worker_id) {
            // If this worker is a assigned a task, requeue the task.
            if !worker.current_task_id.is_empty() {
                let assigned_task = self.tasks
                    .get(&worker.current_task_id)
                    .chain_err(|| "Unable to get worker task")?;
                self.priority_task_queue.push(PriorityTask::new(
                    worker.current_task_id,
                    REQUEUED_TASK_PRIORITY * assigned_task.job_priority,
                ));
            }
        } else {
            return Err(format!("Worker with ID {} not found.", worker_id).into());
        }

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.tasks.insert(task.id.clone(), task.clone());
        self.priority_task_queue.push(PriorityTask::new(
            task.id,
            DEFAULT_TASK_PRIORITY * task.job_priority,
        ));
    }

    pub fn requeue_slow_task(&mut self, task_id: &str) -> Result<()> {
        let task = self.tasks
            .get(task_id)
            .chain_err(|| format!("No task found with id {}", task_id))?;

        self.priority_task_queue.push(PriorityTask::new(
            task_id.clone().to_string(),
            REQUEUED_TASK_PRIORITY * task.job_priority,
        ));

        Ok(())
    }

    // Unassign a task assigned to a worker and put the task back in the queue.
    pub fn unassign_worker(&mut self, worker_id: &str) -> Result<()> {
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

        let assigned_task = self.tasks
            .get(&worker.current_task_id)
            .chain_err(|| "Unable to get worker task")?;
        if !worker.current_task_id.is_empty() {
            self.priority_task_queue.push(PriorityTask::new(
                worker.current_task_id.clone(),
                REQUEUED_TASK_PRIORITY * assigned_task.job_priority,
            ));
        }

        worker.current_task_id = String::new();

        Ok(())
    }

    // Assigns a given task_id to a worker
    fn assign_worker_task(&mut self, worker_id: &str, task_id: &str) -> Result<(Task)> {
        let assigned_task = {
            if let Some(scheduled_task) = self.tasks.get_mut(task_id) {
                scheduled_task.status = TaskStatus::InProgress;
                scheduled_task.time_started = Some(Utc::now());
                scheduled_task.requeued = false;
                scheduled_task.assigned_worker_id = worker_id.to_owned();

                scheduled_task.clone()
            } else {
                return Err(format!("Could not get Task with ID {}", task_id).into());
            }
        };

        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;
        worker.task_last_updated = Utc::now();
        worker.current_task_id = task_id.to_owned();

        Ok(assigned_task)
    }

    fn get_data_score(&self, map_request: &pb::PerformMapRequest, worker_id: &str) -> Result<u64> {
        let mut score: u64 = 0;
        for input_location in map_request.get_input().get_input_locations() {
            score += self.data_layer
                .get_data_closeness(
                    Path::new(&input_location.input_path),
                    input_location.start_byte,
                    input_location.end_byte,
                    worker_id,
                )
                .chain_err(|| {
                    format!(
                        "Could not get closeness for file {} and worker {}",
                        input_location.input_path, worker_id
                    )
                })?;
        }

        Ok(score)
    }

    fn get_data_score_if_map(&self, task_id: &str, worker_id: &str) -> Result<u64> {
        let task = match self.tasks.get(task_id) {
            Some(task) => task,
            None => return Err(format!("Task with ID {} not found.", task_id).into()),
        };

        let data_score = match task.map_request {
            Some(ref map_request) => self.get_data_score(map_request, worker_id).chain_err(|| {
                format!(
                    "Could not get data closeness score for task_id {} and worker_id {}",
                    task.id, worker_id
                )
            })?,
            None => 0,
        };

        Ok(data_score)
    }

    fn get_best_task_for_worker(&mut self, worker_id: &str) -> Result<Option<PriorityTask>> {
        let mut tasks_to_consider = Vec::new();

        for _ in 0..MAX_TASKS_TO_CONSIDER {
            if let Some(task) = self.priority_task_queue.pop() {
                tasks_to_consider.push(task);
            } else {
                break;
            }
        }

        let mut best_task: Option<PriorityTask> = None;
        let mut best_data_score = 0;

        for task in &tasks_to_consider {
            // Don't take lower priority tasks
            if let Some(ref best_task) = best_task {
                if best_task.priority > task.priority {
                    break;
                }
            }

            let data_score = self.get_data_score_if_map(&task.id, worker_id)
                .chain_err(|| "Unable to get data score")?;

            if data_score > best_data_score || best_task.is_none() {
                best_data_score = data_score;
                best_task = Some(task.clone());
            }
        }

        // Add tasks back to queue
        for task in tasks_to_consider.drain(0..) {
            if let Some(ref best_task) = best_task {
                if task.id != best_task.id {
                    self.priority_task_queue.push(task);
                }
            } else {
                self.priority_task_queue.push(task);
            }
        }

        Ok(best_task)
    }

    // Tries to assign a worker the next task in the queue.
    // Returns the task if one exists.
    pub fn try_assign_worker_task(&mut self, worker_id: &str) -> Result<(Option<Task>)> {
        let task_option = self.get_best_task_for_worker(worker_id)
            .chain_err(|| "Could not get task for worker")?;

        let scheduled_task_id: String = match task_option {
            Some(priority_task) => {
                info!(
                    "Popped off task {} with priority {}",
                    priority_task.id, priority_task.priority
                );
                priority_task.id
            }
            None => return Ok(None),
        };

        match self.assign_worker_task(worker_id, &scheduled_task_id) {
            Ok(task) => Ok(Some(task)),
            Err(err) => Err(err),
        }
    }

    // Clears the workers current_task_id and returns the previous value.
    pub fn cancel_task_for_worker(&mut self, worker_id: &str) -> Result<String> {
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

        let previous_task_id = worker.current_task_id.clone();
        worker.current_task_id = String::new();

        self.tasks.remove(&previous_task_id);

        Ok(previous_task_id)
    }

    pub fn has_task(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    pub fn has_worker(&self, worker_id: &str) -> bool {
        self.workers.contains_key(worker_id)
    }

    pub fn increment_failed_task_assignments(&mut self, worker_id: &str) -> Result<()> {
        let should_remove_worker = {
            let worker = self.workers
                .get_mut(worker_id)
                .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;

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
        let worker = self.workers
            .get_mut(worker_id)
            .chain_err(|| format!("Worker with ID {} not found.", worker_id))?;
        worker.task_assignments_failed = 0;

        Ok(())
    }

    pub fn reschedule_map_task(&mut self, task_id: &str) -> Result<()> {
        let map_task = self.completed_tasks
            .get(task_id)
            .chain_err(|| format!("Unable to get map task with ID {}", task_id))?
            .clone();

        let queued_tasks = self.get_in_progress_tasks_for_job(&map_task.job_id)
            .chain_err(|| format!("Unable to get queued tasks for job {}", map_task.job_id))?;

        let mut remove_tasks: Vec<String> = Vec::new();
        for queued_task_id in queued_tasks.clone() {
            let queued_task = self.tasks
                .get(&queued_task_id)
                .chain_err(|| format!("Unable to get task {} from task queue", queued_task_id))?;

            if queued_task.task_type != TaskType::Reduce {
                continue;
            }

            let from_map_task = self.reduce_from_map_task(&map_task, queued_task)
                .chain_err(|| "Unable to determine if reduce stems from map task")?;

            if from_map_task {
                remove_tasks.push(queued_task.id.clone());
            }
        }

        self.remove_tasks_from_queue(&remove_tasks)
            .chain_err(|| "Unable to remove tasks from queue")?;

        // Reschedule the map task
        let mut new_map_task = map_task.clone();
        new_map_task.reset_map_task();
        self.tasks
            .insert(new_map_task.id.clone(), new_map_task.clone());
        self.priority_task_queue.push(PriorityTask::new(
            task_id.to_owned(),
            new_map_task.job_priority * FAILED_TASK_PRIORITY,
        ));

        info!("Rescheduled map task with ID {}", new_map_task.id);
        Ok(())
    }

    pub fn handle_worker_report(&mut self, request: &pb::ReportWorkerRequest) -> Result<()> {
        let mut reschedule_task = String::new();

        if self.tasks.contains_key(&request.task_id) {
            for task in self.completed_tasks.values() {
                if self.path_from_map_task(task, &request.path) {
                    reschedule_task = task.id.clone();
                    break;
                }
            }
        }

        if !reschedule_task.is_empty() {
            self.reschedule_map_task(&reschedule_task).chain_err(|| {
                format!("Unable to reschedule map task with ID {}", reschedule_task)
            })?;
        }

        self.set_worker_operation_cancelled(&request.worker_id)
            .chain_err(|| "Unable to set worker operation to cancelled")
    }

    // Returns true if the path was created by the given map task.
    pub fn path_from_map_task(&self, map_task: &Task, path: &str) -> bool {
        for partition in map_task.map_output_files.values() {
            if partition.ends_with(&path) {
                return true;
            }
        }
        false
    }

    // Returns true if a reduce task stems from the given map task.
    pub fn reduce_from_map_task(&self, map_task: &Task, reduce_task: &Task) -> Result<bool> {
        match reduce_task.reduce_request {
            Some(ref req) => Ok(map_task.map_output_files.contains_key(&req.partition)),
            None => Err(format!(
                "Unabale to get reduce request for task with ID {}",
                map_task.id
            ).into()),
        }
    }

    pub fn remove_tasks_from_queue(&mut self, task_ids: &[String]) -> Result<()> {
        let mut new_priority_queue: BinaryHeap<PriorityTask> = BinaryHeap::new();

        for task_id in task_ids {
            self.tasks.remove(task_id);
        }

        for priority_task in self.priority_task_queue.drain() {
            if self.tasks.contains_key(&priority_task.id) {
                new_priority_queue.push(priority_task);
            }
        }

        self.priority_task_queue = new_priority_queue;
        Ok(())
    }
}

impl StateHandling<Error> for State {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create WorkerManager State from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut workers_json: Vec<serde_json::Value> = Vec::new();
        for worker in self.workers.values() {
            workers_json.push(worker
                .dump_state()
                .chain_err(|| "Error dumping worker state.")?);
        }

        let mut tasks_json: Vec<serde_json::Value> = Vec::new();
        for task in self.tasks.values() {
            tasks_json.push(task.dump_state().chain_err(|| "Error dumping task state.")?);
        }

        Ok(json!({
            "workers": json!(workers_json),
            "priority_task_queue": json!(self.priority_task_queue),
            "tasks": json!(tasks_json),
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        if let serde_json::Value::Array(ref workers_array) = data["workers"] {
            for worker in workers_array {
                let worker = Worker::new_from_json(worker.clone())
                    .chain_err(|| "Unable to create ScheduledJob from json.")?;
                self.workers.insert(worker.worker_id.to_owned(), worker);
            }
        } else {
            return Err("Error processing workers array.".into());
        }

        self.priority_task_queue = serde_json::from_value(data["priority_task_queue"].clone())
            .chain_err(|| "Error processing priority task queue")?;

        if let serde_json::Value::Array(ref tasks_array) = data["tasks"] {
            for task in tasks_array {
                let task = Task::new_from_json(task.clone())
                    .chain_err(|| "Unable to create Task from json.")?;
                debug!("Loaded task from state:\n {:?}", task);
                self.tasks.insert(task.id.to_owned(), task);
            }
        } else {
            return Err("Error processing tasks array.".into());
        }

        Ok(())
    }
}
