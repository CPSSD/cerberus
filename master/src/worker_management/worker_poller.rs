use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::{thread, time};

use cerberus_proto::worker::*;
use errors::*;
use scheduler::MapReduceScheduler;
use util::output_error;
use worker_communication::{WorkerInterface, WorkerInterfaceImpl};
use worker_management::{WorkerTaskType, WorkerManager};

const WORKER_MANAGER_UNAVAILABLE: &str = "Worker manager unavailable";
const WORKER_INTERFACE_UNAVAILABLE: &str = "Worker Interface unavailable";
const SCHEDULER_UNAVAILABLE: &str = "Scheduler unavailable";

const POLLING_LOOP_INTERVAL_MS: u64 = 2000;

// After a worker poll has failed this number of times, reschedule the task for that worker.
const FAILED_POLLS_BEFORE_TASK_RESTART: u64 = 5;
// After a worker poll has failed this number of times, remove the worker from the list of active
// workers.
const FAILED_POLLS_BEFORE_WORKER_TERMINATION: u64 = 10;

enum PollStatus {
    Okay,
    Failure,
}

struct PollResult {
    operation_status: Option<WorkerStatusResponse_OperationStatus>,
    poll_status: PollStatus,
    worker_id: String,
    worker_status: Option<WorkerStatusResponse_WorkerStatus>,
}

#[derive(Clone)]
struct WorkerInfo {
    worker_id: String,
    task_id: String,
    task_type: WorkerTaskType,
}

pub struct WorkerPoller {
    scheduler: Arc<Mutex<MapReduceScheduler>>,
    worker_manager: Arc<Mutex<WorkerManager>>,
    worker_interface: Arc<RwLock<WorkerInterfaceImpl>>,
    failed_polls: RefCell<HashMap<String, u64>>,
}

impl WorkerPoller {
    pub fn new(
        scheduler: Arc<Mutex<MapReduceScheduler>>,
        worker_manager: Arc<Mutex<WorkerManager>>,
        worker_interface: Arc<RwLock<WorkerInterfaceImpl>>,
    ) -> Self {
        WorkerPoller {
            scheduler: scheduler,
            worker_manager: worker_manager,
            worker_interface: worker_interface,
            failed_polls: RefCell::new(HashMap::new()),
        }
    }

    pub fn poll(&self) -> Result<()> {
        let worker_ids = match self.worker_manager.lock() {
            Ok(worker_manager) => worker_manager.get_worker_ids(),
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
        };

        let poll_results = match self.worker_interface.read() {
            Ok(worker_interface) => self.step(worker_ids, &worker_interface),
            Err(_) => return Err(WORKER_INTERFACE_UNAVAILABLE.into()),
        };

        let mut failed_this_round = Vec::new();

        match self.worker_manager.lock() {
            Ok(mut worker_manager) => {
                for poll in poll_results {
                    match poll.poll_status {
                        PollStatus::Okay => {
                            let result = self.update_worker(&poll, &mut worker_manager);
                            if let Err(err) = result {
                                output_error(&err.chain_err(|| "Unable to update worker."));
                            }
                        }
                        PollStatus::Failure => {
                            failed_this_round.push(poll);
                        }
                    }
                }
            }
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
        }

        for poll in &failed_this_round {
            let result = self.handle_failed_worker_poll(poll);
            if let Err(err) = result {
                output_error(&err.chain_err(|| "Unable to handle failed worker poll."));
            }
        }
        self.poll_mapreduce_results()
    }

    fn step(
        &self,
        worker_ids: Vec<String>,
        interface: &RwLockReadGuard<WorkerInterfaceImpl>,
    ) -> Vec<PollResult> {
        let mut results = Vec::new();
        for worker_id in worker_ids {
            let status_response = interface.get_worker_status(worker_id.as_str());
            let result = match status_response {
                Ok(status) => {
                    PollResult {
                        operation_status: Some(status.operation_status),
                        poll_status: PollStatus::Okay,
                        worker_id: worker_id.to_owned(),
                        worker_status: Some(status.worker_status),
                    }
                }
                Err(_) => {
                    PollResult {
                        operation_status: None,
                        poll_status: PollStatus::Failure,
                        worker_id: worker_id.to_owned(),
                        worker_status: None,
                    }
                }
            };
            results.push(result);
        }
        results
    }

    fn handle_map_result(
        &self,
        wi: &WorkerInfo,
        interface: &RwLockReadGuard<WorkerInterfaceImpl>,
    ) -> Result<()> {
        let map_result = interface.get_map_result(wi.worker_id.as_str()).chain_err(
            || "Unable to retrieve results of map.",
        )?;
        match self.scheduler.lock() {
            Ok(mut scheduler) => scheduler.process_map_task_response(&wi.task_id, &map_result),
            Err(_) => Err(SCHEDULER_UNAVAILABLE.into()),
        }
    }

    fn handle_reduce_result(
        &self,
        wi: &WorkerInfo,
        interface: &RwLockReadGuard<WorkerInterfaceImpl>,
    ) -> Result<()> {
        let reduce_result = interface.get_reduce_result(wi.worker_id.as_str());
        match reduce_result {
            Ok(reduce_result) => {
                match self.scheduler.lock() {
                    Ok(mut scheduler) => {
                        scheduler.process_reduce_task_response(&wi.task_id, &reduce_result)
                    }
                    Err(_) => Err(SCHEDULER_UNAVAILABLE.into()),
                }
            }
            Err(_) => Err("Error retrieving ReduceResponse".into()),
        }
    }

    fn handle_mapreduce_results(
        &self,
        worker_info_list: Vec<WorkerInfo>,
        interface: &RwLockReadGuard<WorkerInterfaceImpl>,
    ) {
        for wi in worker_info_list {
            let worker_id = wi.worker_id.clone();
            let result = match wi.task_type {
                WorkerTaskType::Map => self.handle_map_result(&wi, interface),
                WorkerTaskType::Reduce => self.handle_reduce_result(&wi, interface),
                WorkerTaskType::Idle => Ok(()),
            };

            match result {
                Ok(_) => {
                    match self.worker_manager.lock() {
                        Ok(mut worker_manager) => {
                            let worker = worker_manager.get_worker(worker_id.as_str());
                            match worker {
                                Some(worker) => {
                                    worker.set_completed_operation_flag(false);
                                    worker.set_current_task_id("");
                                }
                                None => error!("No worker exists with the given ID: {}", worker_id),
                            }
                        }
                        Err(_) => error!("{}", WORKER_MANAGER_UNAVAILABLE),
                    };
                }
                Err(err) => output_error(&err.chain_err(|| {
                    format!(
                        "Error occured handling mapreduce results for worker '{}'",
                        worker_id
                    )
                })),
            }
        }
    }

    fn update_worker(
        &self,
        poll: &PollResult,
        worker_manager: &mut MutexGuard<WorkerManager>,
    ) -> Result<()> {
        let worker_result = worker_manager.get_worker(poll.worker_id.as_str());
        match worker_result {
            None => Err("Unable to get worker".into()),
            Some(worker) => {
                worker.set_status(poll.worker_status.unwrap());
                worker.set_operation_status(poll.operation_status.unwrap());
                Ok(())
            }
        }
    }

    /// If the master fails to poll a worker, increment the counter of failed polls and decide what
    /// to do next.
    fn handle_failed_worker_poll(&self, poll_result: &PollResult) -> Result<()> {
        warn!(
            "Worker {} failed to respond to poll.",
            poll_result.worker_id
        );
        let num_failed_polls = {
            let mut failed_polls_mut = self.failed_polls.borrow_mut();
            let num_failed_polls_mut = failed_polls_mut
                .entry(poll_result.worker_id.clone())
                .or_insert(0);
            *num_failed_polls_mut += 1;
            warn!(
                "Worker {} has failed {} times.",
                poll_result.worker_id,
                *num_failed_polls_mut
            );
            *num_failed_polls_mut
        };

        let mut worker_manager = self.worker_manager.lock().map_err(
            |_| "Failed to obtain lock on worker manager.",
        )?;
        let mut scheduler = self.scheduler.lock().map_err(
            |_| "Failed to obtain lock on scheduler.",
        )?;
        let task_id = worker_manager
            .get_worker(&poll_result.worker_id)
            .chain_err(|| "Failed to get worker from worker manager.")?
            .get_current_task_id()
            .to_owned();
        if num_failed_polls >= FAILED_POLLS_BEFORE_WORKER_TERMINATION {
            self.remove_worker(
                &poll_result.worker_id,
                &mut worker_manager,
                &mut scheduler,
            )?;
        }
        if num_failed_polls >= FAILED_POLLS_BEFORE_TASK_RESTART {
            if task_id.is_empty() {
                info!("No task assigned to worker.");
            } else {
                self.reschedule_task(&task_id, &mut scheduler)?;
            }
        }
        Ok(())
    }

    /// Remove a task from its worker and push it back onto the queue.
    fn reschedule_task(
        &self,
        task_id: &str,
        scheduler: &mut MutexGuard<MapReduceScheduler>,
    ) -> Result<()> {
        scheduler.unschedule_task(task_id).chain_err(|| {
            format!("Unable to unschedule task with ID {:?}.", task_id)
        })?;
        Ok(())
    }

    /// Remove a worker from the list of available workers.
    fn remove_worker(
        &self,
        worker_id: &str,
        worker_manager: &mut MutexGuard<WorkerManager>,
        scheduler: &mut MutexGuard<MapReduceScheduler>,
    ) -> Result<()> {
        worker_manager.remove_worker(worker_id);
        let available_workers = scheduler.get_available_workers();
        scheduler.set_available_workers(available_workers - 1);

        Ok(())
    }

    fn get_workers_with_completed_task(&self) -> Result<Vec<WorkerInfo>> {
        let mut worker_info_list = Vec::new();
        match self.worker_manager.lock() {
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
            Ok(worker_manager) => {
                let workers = worker_manager.get_workers();
                for worker in workers {
                    if worker.get_operation_status() ==
                        WorkerStatusResponse_OperationStatus::COMPLETE &&
                        worker.get_completed_operation_flag()
                    {
                        worker_info_list.push(WorkerInfo {
                            worker_id: worker.get_worker_id().to_owned(),
                            task_id: worker.get_current_task_id().to_owned(),
                            task_type: worker.get_current_task_type(),
                        });
                    }
                }
            }
        };
        Ok(worker_info_list)
    }

    fn poll_mapreduce_results(&self) -> Result<()> {
        let worker_info_list_result = self.get_workers_with_completed_task();

        let worker_info_list = match worker_info_list_result {
            Err(_) => return Err("Unable to retrieve worker info list.".into()),
            Ok(worker_info_list) => worker_info_list,
        };

        if worker_info_list.is_empty() {
            return Ok(());
        }

        match self.worker_interface.read() {
            Err(_) => Err(WORKER_INTERFACE_UNAVAILABLE.into()),
            Ok(ref worker_interface) => {
                self.handle_mapreduce_results(worker_info_list, worker_interface);
                Ok(())
            }
        }
    }
}

pub fn run_polling_loop(worker_poller: WorkerPoller) {
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(POLLING_LOOP_INTERVAL_MS));
        let result = worker_poller.poll();
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error encountered during polling loop"));
        }
    });
}
