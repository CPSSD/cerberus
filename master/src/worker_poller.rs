use errors::*;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::{thread, time};
use worker_manager::{WorkerTaskType, WorkerManager};
use worker_interface::WorkerInterface;
use cerberus_proto::mrworker::*;
use scheduler::MapReduceScheduler;

const WORKER_MANAGER_UNAVAILABLE: &'static str = "Worker manager unavailable";
const WORKER_INTERFACE_UNAVAILABLE: &'static str = "Worker Interface unavailable";
const SCHEDULER_UNAVAILABLE: &'static str = "Scheduler unavailable";
const POLLING_LOOP_INTERVAL_MS: u64 = 5000;

pub struct WorkerPoller {
    scheduler: Arc<Mutex<MapReduceScheduler>>,
    worker_manager: Arc<Mutex<WorkerManager>>,
    worker_interface: Arc<RwLock<WorkerInterface>>,
}

struct PollResult {
    worker_id: String,
    worker_status: WorkerStatusResponse_WorkerStatus,
    operation_status: WorkerStatusResponse_OperationStatus,
}

#[derive(Clone)]
struct WorkerInfo {
    worker_id: String,
    task_id: String,
    task_type: WorkerTaskType,
}

impl WorkerPoller {
    pub fn new(
        scheduler: Arc<Mutex<MapReduceScheduler>>,
        worker_manager: Arc<Mutex<WorkerManager>>,
        worker_interface: Arc<RwLock<WorkerInterface>>,
    ) -> Self {
        WorkerPoller {
            scheduler: scheduler,
            worker_manager: worker_manager,
            worker_interface: worker_interface,
        }
    }

    fn step(
        &self,
        worker_ids: Vec<String>,
        interface: RwLockReadGuard<WorkerInterface>,
    ) -> Vec<Result<PollResult>> {
        let mut results = Vec::new();
        for worker_id in worker_ids {
            let status_response = interface.get_worker_status(worker_id.as_str());
            let result = match status_response {
                Err(_) => Err("Unable to poll worker".into()),
                Ok(status) => {
                    Ok(PollResult {
                        worker_id: worker_id.to_owned(),
                        worker_status: status.worker_status,
                        operation_status: status.operation_status,
                    })
                }
            };
            results.push(result);
        }
        results
    }

    fn handle_map_result(
        &self,
        wi: WorkerInfo,
        interface: &RwLockReadGuard<WorkerInterface>,
    ) -> Result<()> {
        let map_result = interface.get_map_result(wi.worker_id.as_str());
        match map_result {
            Err(_) => Err("Error retrieving MapResponse".into()),
            Ok(map_result) => {
                match self.scheduler.lock() {
                    Err(_) => Err(SCHEDULER_UNAVAILABLE.into()),
                    Ok(mut scheduler) => {
                        scheduler.process_map_task_response(wi.task_id, map_result)
                    }
                }
            }
        }
    }

    fn handle_reduce_result(
        &self,
        wi: WorkerInfo,
        interface: &RwLockReadGuard<WorkerInterface>,
    ) -> Result<()> {
        let reduce_result_response = interface.get_reduce_result(wi.worker_id.as_str());
        match reduce_result_response {
            Err(_) => Err("Error retrieving ReduceResponse".into()),
            Ok(reduce_result) => {
                match self.scheduler.lock() {
                    Err(_) => Err(SCHEDULER_UNAVAILABLE.into()),
                    Ok(mut scheduler) => {
                        scheduler.process_reduce_task_response(wi.task_id, reduce_result)
                    }
                }
            }
        }
    }

    fn handle_mapreduce_results(
        &self,
        worker_info_list: Vec<WorkerInfo>,
        interface: &RwLockReadGuard<WorkerInterface>,
    ) {
        for wi in worker_info_list {
            let worker_id = wi.worker_id.clone();
            let result = match wi.task_type {
                WorkerTaskType::Map => self.handle_map_result(wi, interface),
                WorkerTaskType::Reduce => self.handle_reduce_result(wi, interface),
                WorkerTaskType::Idle => Ok(()),
            };

            match result {
                Err(err) => error!("Error occured for worker '{}': {:?}", worker_id, err),
                Ok(_) => {
                    match self.worker_manager.lock() {
                        Err(_) => error!("{}", WORKER_MANAGER_UNAVAILABLE),
                        Ok(mut worker_manager) => {
                            let worker = worker_manager.get_worker(worker_id.as_str());
                            match worker {
                                None => error!("No worker exists with the given ID: {}", worker_id),
                                Some(worker) => {
                                    worker.set_completed_operation_flag(false);
                                }
                            }
                        }
                    };
                }
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
                worker.set_status(poll.worker_status);
                worker.set_operation_status(poll.operation_status);
                Ok(())
            }
        }
    }

    pub fn poll(&self) -> Result<()> {
        let worker_ids = match self.worker_manager.lock() {
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
            Ok(worker_manager) => worker_manager.get_worker_ids(),
        };

        let poll_results = match self.worker_interface.read() {
            Err(_) => return Err(WORKER_INTERFACE_UNAVAILABLE.into()),
            Ok(worker_interface) => self.step(worker_ids, worker_interface),
        };

        match self.worker_manager.lock() {
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
            Ok(mut worker_manager) => {
                for poll_result in poll_results {
                    match poll_result {
                        Err(_) => return Err("Unable to get polling result.".into()),
                        Ok(poll) => {
                            let result = self.update_worker(&poll, &mut worker_manager);
                            if result.is_err() {
                                error!("Unable to update worker");
                            }
                        }
                    }
                }
            }
        }

        self.poll_mapreduce_results()
    }

    fn get_worker_info_list(&self) -> Result<Vec<WorkerInfo>> {
        let mut worker_info_list = Vec::new();
        match self.worker_manager.lock() {
            Err(_) => return Err(WORKER_MANAGER_UNAVAILABLE.into()),
            Ok(worker_manager) => {
                let workers = worker_manager.get_workers();
                for worker in workers {
                    if worker.get_operation_status() ==
                        WorkerStatusResponse_OperationStatus::COMPLETE
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
        let worker_info_list_result = self.get_worker_info_list();

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
        if result.is_err() {
            error!("Error encountered during polling loop: {:?}", result);
        }
    });
}
