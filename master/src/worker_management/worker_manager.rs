use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::{thread, time};

use chrono::prelude::*;
use futures::future;
use futures::prelude::*;
use futures_cpupool::CpuPool;
use serde_json;

use cerberus_proto::worker as pb;
use common::{Task, TaskStatus, TaskType, Worker};
use errors::*;
use util::data_layer::AbstractionLayer;
use util::distributed_filesystem::{WorkerInfoUpdate, WorkerInfoUpdateType};
use util::output_error;
use util::state::{SimpleStateHandling, StateHandling};
use worker_communication::WorkerInterface;
use worker_management::state::State;

const HEALTH_CHECK_LOOP_MS: u64 = 100;
const TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S: i64 = 60;
const TIME_BEFORE_WORKER_TERMINATION_S: i64 = 120;

const ASSIGN_TASK_LOOP_MS: u64 = 100;

/// The `WorkerManager` is responsible for the assignment of `Task`s to workers.
/// It also handles worker health checking and stores the list of `Worker`s currently registered.
pub struct WorkerManager {
    state: Arc<Mutex<State>>,
    worker_interface: Arc<WorkerInterface + Send + Sync>,

    task_update_sender: Mutex<Sender<Task>>,
    task_update_receiver: Arc<Mutex<Receiver<Task>>>,

    worker_info_sender: Mutex<Sender<WorkerInfoUpdate>>,
}

impl WorkerManager {
    pub fn new(
        worker_interface: Arc<WorkerInterface + Send + Sync>,
        data_layer: Arc<AbstractionLayer + Send + Sync>,
        worker_info_sender: Sender<WorkerInfoUpdate>,
    ) -> Self {
        let (sender, receiver) = channel();
        WorkerManager {
            state: Arc::new(Mutex::new(State::new(data_layer))),
            worker_interface,

            task_update_sender: Mutex::new(sender),
            task_update_receiver: Arc::new(Mutex::new(receiver)),

            worker_info_sender: Mutex::new(worker_info_sender),
        }
    }

    fn send_worker_info_update(&self, info_update: WorkerInfoUpdate) -> Result<()> {
        let worker_info_sender = self.worker_info_sender.lock().unwrap();
        worker_info_sender
            .send(info_update)
            .chain_err(|| "Error sending worker info update.")?;

        Ok(())
    }

    pub fn get_update_receiver(&self) -> Arc<Mutex<Receiver<Task>>> {
        Arc::clone(&self.task_update_receiver)
    }

    // Returns a count of workers registered with the master.
    pub fn get_available_workers(&self) -> u32 {
        let state = self.state.lock().unwrap();
        state.get_worker_count()
    }

    pub fn register_worker(&self, worker: Worker) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        self.worker_interface
            .add_client(&worker)
            .chain_err(|| "Error registering worker.")?;

        let worker_id = worker.worker_id.clone();
        let worker_address = Some(worker.address);

        state
            .add_worker(worker)
            .chain_err(|| "Error registering worker.")?;

        self.send_worker_info_update(WorkerInfoUpdate::new(
            WorkerInfoUpdateType::Available,
            worker_id,
            worker_address,
        )).chain_err(|| "Error sending worker info update")?;

        Ok(())
    }

    pub fn update_worker_status(
        &self,
        worker_id: &str,
        worker_status: pb::WorkerStatus,
        operation_status: pb::OperationStatus,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.update_worker_status(worker_id, worker_status, operation_status)
    }

    pub fn process_reduce_task_result(&self, reduce_result: &pb::ReduceResult) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        info!(
            "Got result for reduce task {} from {}",
            reduce_result.task_id, reduce_result.worker_id
        );

        let task_option = state
            .process_reduce_task_result(reduce_result)
            .chain_err(|| "Error processing reduce result.")?;

        state
            .set_worker_operation_completed(
                reduce_result.get_worker_id(),
                reduce_result.get_status(),
            )
            .chain_err(|| "Error processing reduce result.")?;

        let task = match task_option {
            Some(task) => task,
            None => return Ok(()),
        };

        if task.status == TaskStatus::Complete || task.status == TaskStatus::Failed {
            let task_update_sender = self.task_update_sender.lock().unwrap();
            task_update_sender
                .send(task)
                .chain_err(|| "Error processing reduce result.")?;
        }

        Ok(())
    }

    pub fn process_map_task_result(&self, map_result: &pb::MapResult) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        info!(
            "Got result for map task {} from {}",
            map_result.task_id, map_result.worker_id
        );

        let task_option = state
            .process_map_task_result(map_result)
            .chain_err(|| "Error processing map result.")?;

        state
            .set_worker_operation_completed(map_result.get_worker_id(), map_result.get_status())
            .chain_err(|| "Error processing map result.")?;

        let task = match task_option {
            Some(task) => task,
            None => return Ok(()),
        };

        if task.status == TaskStatus::Complete || task.status == TaskStatus::Failed {
            let task_update_sender = self.task_update_sender.lock().unwrap();
            task_update_sender
                .send(task)
                .chain_err(|| "Error processing map result.")?;
        }

        Ok(())
    }

    fn reassign_workers(&self, workers: Vec<String>) {
        let mut state = self.state.lock().unwrap();
        for worker_id in workers {
            info!(
                "Reassigning task for worker {} due to health check failure.",
                worker_id
            );
            let result = state.unassign_worker(&worker_id);
            if let Err(err) = result {
                output_error(&err.chain_err(|| "Error reassigning task."));
            }
        }
    }

    pub fn cancel_workers_tasks(&self, workers: Vec<String>) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        for worker_id in workers {
            // Clear the task from the worker so that we can ignore it's result.
            let task_id = state
                .cancel_task_for_worker(&worker_id)
                .chain_err(|| format!("Error cancelling task on worker: {}", worker_id))?;

            // Create a request to cancel the task the worker is currently running.
            let mut cancel_request = pb::CancelTaskRequest::new();
            cancel_request.task_id = task_id;

            // Tell the worker to cancel what it's doing.
            self.worker_interface
                .cancel_task(cancel_request, &worker_id)
                .chain_err(|| "Error telling worker to cancel task")?;
        }
        Ok(())
    }

    fn remove_workers(&self, workers: Vec<String>) {
        let mut state = self.state.lock().unwrap();
        for worker_id in workers {
            info!(
                "Removing worker {} from list of active workers due to health check failure.",
                worker_id
            );

            // Remove worker interface.
            let result = self.worker_interface.remove_client(&worker_id);
            if let Err(err) = result {
                output_error(&err.chain_err(|| "Error removing worker."));
                continue;
            }

            let result = state.remove_worker(&worker_id);
            if let Err(err) = result {
                output_error(&err.chain_err(|| "Error removing worker."));
            }

            let result = self.send_worker_info_update(WorkerInfoUpdate::new(
                WorkerInfoUpdateType::Unavailable,
                worker_id,
                None, /* address */
            ));

            if let Err(err) = result {
                output_error(&err.chain_err(|| "Error sending worker info update."));
            }
        }
    }

    pub fn perform_health_check(&self) {
        let mut workers_to_reassign = Vec::new();
        let mut workers_to_remove = Vec::new();

        {
            let state = self.state.lock().unwrap();
            let workers = state.get_workers();
            for worker in workers {
                let time_since_worker_updated =
                    Utc::now().timestamp() - worker.status_last_updated.timestamp();
                if time_since_worker_updated >= TIME_BEFORE_WORKER_TERMINATION_S {
                    workers_to_remove.push(worker.worker_id.to_owned());
                } else if time_since_worker_updated >= TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S
                    && worker.current_task_id != ""
                {
                    workers_to_reassign.push(worker.worker_id.to_owned());
                }
            }
        }

        self.reassign_workers(workers_to_reassign);
        self.remove_workers(workers_to_remove);
    }

    // Attempts to assign a map task to a worker.
    // Returns a future containing the result of the assignment.
    fn assign_map_task(
        &self,
        worker_id: String,
        task: Task,
    ) -> Box<Future<Item = TaskAssignmentResult, Error = Error> + Send> {
        info!("Assigning map task {} to Worker {}", task.id, worker_id);
        let worker_interface = Arc::clone(&self.worker_interface);
        Box::new(future::lazy(move || {
            let mut task_assignment_result = TaskAssignmentResult {
                worker_id: worker_id.clone(),
                result: Ok(()),
            };
            let map_request = match task.map_request {
                Some(map_request) => map_request,
                None => {
                    task_assignment_result.result =
                        Err("task.map_request was unexpectedly None.".into());
                    return future::ok(task_assignment_result);
                }
            };
            task_assignment_result.result = worker_interface.schedule_map(map_request, &worker_id);
            future::ok(task_assignment_result)
        }))
    }

    // Attempts to assign a reduce task to a worker.
    // Returns a future containing the result of the assignment.
    fn assign_reduce_task(
        &self,
        worker_id: String,
        task: Task,
    ) -> Box<Future<Item = TaskAssignmentResult, Error = Error> + Send> {
        info!("Assigning reduce task {} to Worker {}", task.id, worker_id);
        let worker_interface = Arc::clone(&self.worker_interface);
        Box::new(future::lazy(move || {
            let mut task_assignment_result = TaskAssignmentResult {
                worker_id: worker_id.clone(),
                result: Ok(()),
            };
            let reduce_request = match task.reduce_request {
                Some(reduce_request) => reduce_request,
                None => {
                    task_assignment_result.result =
                        Err("task.reduce_request was unexpectedly None.".into());
                    return future::ok(task_assignment_result);
                }
            };
            task_assignment_result.result =
                worker_interface.schedule_reduce(reduce_request, &worker_id);
            future::ok(task_assignment_result)
        }))
    }

    // Returns a list of futures of task assignment results
    pub fn assign_tasks(
        &self,
    ) -> Vec<Box<Future<Item = TaskAssignmentResult, Error = Error> + Send>> {
        let mut task_assignment_futures = Vec::new();
        let mut state = self.state.lock().unwrap();
        let available_workers_ids = state.get_available_workers();

        for worker_id in available_workers_ids {
            let task_option = match state.try_assign_worker_task(&worker_id) {
                Ok(task_option) => task_option,
                Err(err) => {
                    output_error(&err.chain_err(|| "Error assigning worker task."));
                    continue;
                }
            };

            if let Some(task) = task_option {
                let task_assignment_future = match task.task_type {
                    TaskType::Map => self.assign_map_task(worker_id, task.clone()),
                    TaskType::Reduce => self.assign_reduce_task(worker_id, task.clone()),
                };

                let task_update_sender = self.task_update_sender.lock().unwrap();
                if let Err(err) = task_update_sender.send(task) {
                    error!("Error updating scheduler on task status: {}", err);
                }

                task_assignment_futures.push(task_assignment_future);
            } else {
                break;
            }
        }
        task_assignment_futures
    }

    pub fn handle_task_assignment_failure(&self, worker_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .unassign_worker(worker_id)
            .chain_err(|| format!("Failed to unassign task from worker {}", worker_id))?;
        state
            .increment_failed_task_assignments(worker_id)
            .chain_err(|| "Error when incrementing task assignment failures")
    }

    pub fn handle_task_assignment_success(&self, worker_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .reset_failed_task_assignments(worker_id)
            .chain_err(|| "Error when recording task assignment success")
    }

    pub fn run_task(&self, task: Task) {
        let mut state = self.state.lock().unwrap();
        state.add_task(task);
    }

    pub fn has_task(&self, task_id: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.has_task(task_id)
    }

    pub fn remove_queued_tasks_for_job(&self, job_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.remove_queued_tasks_for_job(job_id)
    }

    pub fn get_workers_running_job(&self, job_id: &str) -> Result<Vec<String>> {
        let state = self.state.lock().unwrap();
        state.get_workers_running_job(job_id)
    }

    pub fn requeue_slow_task(&self, task_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.requeue_slow_task(task_id)
    }

    pub fn get_workers_info(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();

        let mut results_vec = Vec::new();
        for worker in state.get_workers() {
            let worker_info = json!({
                "worker_id": worker.worker_id,
                "address": worker.address.to_string(),
                "status": worker.get_serializable_worker_status(),
                "operation_status": worker.get_serializable_operation_status(),
                "current_task_id": worker.current_task_id,
                "task_assignments_failed": worker.task_assignments_failed,
            });

            results_vec.push(worker_info);
        }

        Ok(json!(results_vec))
    }

    pub fn get_tasks_info(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();

        let mut results_vec = Vec::new();
        for task in state.get_tasks() {
            let task_info = json!({
                "task_id": task.id,
                "job_id": task.job_id,
                "task_type": task.task_type,
                "assigned_worker_id": task.assigned_worker_id,
                "status": task.status,
                "failure_count": task.failure_count,
            });

            results_vec.push(task_info);
        }

        Ok(json!(results_vec))
    }

    pub fn remove_worker_if_exist(&self, worker_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        if !state.has_worker(worker_id) {
            return Ok(());
        }

        info!("Removing worker {} from list of active workers.", worker_id);

        // Remove worker interface.
        self.worker_interface
            .remove_client(worker_id)
            .chain_err(|| "Error removing worker client")?;

        state
            .remove_worker(worker_id)
            .chain_err(|| "Error removing worker from state")?;

        self.send_worker_info_update(WorkerInfoUpdate::new(
            WorkerInfoUpdateType::Unavailable,
            worker_id.to_string(),
            None, /* address */
        )).chain_err(|| "Error sending worker info update")?;

        Ok(())
    }

    pub fn handle_worker_report(&self, request: &pb::ReportWorkerRequest) -> Result<()> {
        info!(
            "Worker on '{}' failed to provide map output data to '{}' for task with ID {}",
            request.report_address, request.worker_id, request.task_id,
        );

        let mut state = self.state.lock().unwrap();
        state.handle_worker_report(request)
    }
}

impl SimpleStateHandling<Error> for WorkerManager {
    fn dump_state(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();
        state.dump_state()
    }

    fn load_state(&self, data: serde_json::Value) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .load_state(data)
            .chain_err(|| "Error creating worker manager from state.")?;

        let mut workers_to_remove = Vec::new();
        {
            let workers = state.get_workers();
            for worker in workers {
                let add_client_result = self.worker_interface.add_client(worker);

                self.send_worker_info_update(WorkerInfoUpdate::new(
                    WorkerInfoUpdateType::Available,
                    worker.worker_id.clone(),
                    Some(worker.address),
                )).chain_err(|| "Error sending worker info update")?;

                if let Err(e) = add_client_result {
                    output_error(&e.chain_err(|| {
                        format!("Unable to reconnect to worker {}", worker.worker_id)
                    }));
                    workers_to_remove.push(worker.worker_id.to_owned());
                }
            }
        }
        for worker_id in workers_to_remove {
            state
                .remove_worker(&worker_id)
                .chain_err(|| "Error creating worker manager from state.")?;

            self.send_worker_info_update(WorkerInfoUpdate::new(
                WorkerInfoUpdateType::Unavailable,
                worker_id,
                None, /* address */
            )).chain_err(|| "Error sending worker info update")?;
        }
        Ok(())
    }
}

pub fn run_health_check_loop(worker_manager: Arc<WorkerManager>) {
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(HEALTH_CHECK_LOOP_MS));
        worker_manager.perform_health_check();
    });
}

pub struct TaskAssignmentResult {
    pub worker_id: String,
    pub result: Result<()>,
}

fn handle_task_assignment_result(
    worker_manager: &WorkerManager,
    assignment_result: TaskAssignmentResult,
) {
    let worker_id = assignment_result.worker_id.clone();
    if let Err(err) = assignment_result.result {
        output_error(&err.chain_err(|| format!("Error assigning task to worker {}", worker_id)));
        let result = worker_manager.handle_task_assignment_failure(&worker_id);
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error handling task assignment failure."));
        }
    } else {
        let result = worker_manager.handle_task_assignment_success(&worker_id);
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error handling task assignment success."));
        }
    }
}

pub fn run_task_assigment_loop(worker_manager: Arc<WorkerManager>) {
    let cpu_pool = CpuPool::new_num_cpus();
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(ASSIGN_TASK_LOOP_MS));
        let task_assignment_futures = worker_manager.assign_tasks();
        for task_assignment in task_assignment_futures {
            let worker_manager = Arc::clone(&worker_manager);
            let cpu_future = cpu_pool.spawn(task_assignment.and_then(move |result| {
                handle_task_assignment_result(&worker_manager, result);
                future::ok(())
            }));
            cpu_future.forget();
        }
    });
}
