use std::{thread, time};
use std::sync::{Arc, Mutex};

use chrono::prelude::*;
use futures::future;
use futures::prelude::*;
use futures::sync::oneshot;
use futures_cpupool::CpuPool;

use cerberus_proto::worker as pb;
use common::{Task, TaskType, Worker};
use errors::*;
use util::output_error;
use worker_communication::WorkerInterface;
use worker_management::state::{ScheduledTask, State};

const HEALTH_CHECK_LOOP_MS: u64 = 100;
const TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S: i64 = 60;
const TIME_BEFORE_WORKER_TERMINATION_S: i64 = 120;

const ASSIGN_TASK_LOOP_MS: u64 = 100;

/// The `WorkerManager` is responsible for the assignment of `Task`s to workers.
/// It also handles worker health checking and stores the list of `Worker`s currently registered.
pub struct WorkerManager {
    state: Arc<Mutex<State>>,
    worker_interface: Arc<WorkerInterface + Send>,
}

impl WorkerManager {
    pub fn new(worker_interface: Arc<WorkerInterface + Send>) -> Self {
        WorkerManager {
            state: Arc::new(Mutex::new(State::new())),
            worker_interface: worker_interface,
        }
    }

    pub fn register_worker(&self, worker: Worker) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        self.worker_interface.add_client(&worker).chain_err(
            || "Error registering worker.",
        )?;

        state.add_worker(worker).chain_err(
            || "Error registering worker.",
        )?;

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
        state.process_reduce_task_result(reduce_result).chain_err(
            || "Error processing reduce result.",
        )?;

        state
            .set_worker_operation_completed(
                reduce_result.get_worker_id(),
                reduce_result.get_status(),
            )
            .chain_err(|| "Error processing reduce result.")?;
        Ok(())
    }

    pub fn process_map_task_result(&self, map_result: &pb::MapResult) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.process_map_task_result(map_result).chain_err(
            || "Error processing map result.",
        )?;

        state
            .set_worker_operation_completed(map_result.get_worker_id(), map_result.get_status())
            .chain_err(|| "Error processing map result.")?;
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
        }
    }

    pub fn perform_health_check(&self) {
        let mut workers_to_reassign = Vec::new();
        let mut workers_to_remove = Vec::new();

        {
            let state = self.state.lock().unwrap();
            let workers = state.get_workers();
            for worker in workers {
                let time_since_worker_updated = Utc::now().timestamp() -
                    worker.status_last_updated.timestamp();
                if time_since_worker_updated >= TIME_BEFORE_WORKER_TERMINATION_S {
                    workers_to_remove.push(worker.worker_id.to_owned());
                } else if time_since_worker_updated >= TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S {
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
                    TaskType::Map => self.assign_map_task(worker_id, task),
                    TaskType::Reduce => self.assign_reduce_task(worker_id, task),
                };
                task_assignment_futures.push(task_assignment_future);
            } else {
                break;
            }
        }
        task_assignment_futures
    }

    pub fn handle_task_assignment_failure(&self, worker_id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.unassign_worker(worker_id)
    }

    pub fn run_task(&self, task: Task) -> Box<Future<Item = Task, Error = Error> + Send> {
        let (send, recv) = oneshot::channel::<(Result<Task>)>();

        let scheduled_task = ScheduledTask {
            task: task,
            completed_channel: send,
        };

        let state = Arc::clone(&self.state);
        Box::new(
            future::lazy(move || {
                let mut state = state.lock().unwrap();
                state.add_task(scheduled_task);
                future::ok(())
            }).and_then(|_| {
                recv.then(|future_result| {
                    future_result.unwrap_or_else(|_| Err("Task was cancelled.".into()))
                })
            }),
        )
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
        output_error(&err.chain_err(|| {
            format!("Error assigning task to worker {}", worker_id)
        }));
        let result = worker_manager.handle_task_assignment_failure(&worker_id);
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error handling task assignment failure."));
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
