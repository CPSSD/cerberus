use chrono::prelude::*;
use grpc::{RequestOptions, SingleResponse, Error};
use std::sync::{Arc, Mutex, RwLock};
use worker_communication::{WorkerInterface, WorkerInterfaceImpl};
use worker_management::{Worker, WorkerManager};
use scheduler::MapReduceScheduler;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;

const WORKER_INTERFACE_UNAVAILABLE: &str = "Worker interface not available.";
const WORKER_MANAGER_UNAVAILABLE: &str = "Worker manager not available.";
const SCHEDULER_UNAVAILABLE: &str = "Scheduler not available.";
const INVALID_TASK_ID: &str = "No valid Task ID for this worker.";
const INVALID_WORKER_ID: &str = "No worker found for provided id.";

pub struct WorkerServiceImpl {
    worker_manager: Arc<Mutex<WorkerManager>>,
    worker_interface: Arc<RwLock<WorkerInterfaceImpl>>,
    scheduler: Arc<Mutex<MapReduceScheduler>>,
}

impl WorkerServiceImpl {
    pub fn new(
        worker_manager: Arc<Mutex<WorkerManager>>,
        worker_interface: Arc<RwLock<WorkerInterfaceImpl>>,
        scheduler: Arc<Mutex<MapReduceScheduler>>,
    ) -> Self {
        WorkerServiceImpl {
            worker_manager,
            worker_interface,
            scheduler,
        }
    }
}

impl grpc_pb::WorkerService for WorkerServiceImpl {
    fn register_worker(
        &self,
        _o: RequestOptions,
        request: pb::RegisterWorkerRequest,
    ) -> SingleResponse<pb::RegisterWorkerResponse> {
        let worker = match Worker::new(request.get_worker_address().to_string()) {
            Ok(worker) => worker,
            Err(err) => return SingleResponse::err(Error::Panic(err.to_string())),
        };

        // Add client for worker to worker interface.
        match self.worker_interface.write() {
            Ok(mut interface) => {
                let result = interface.add_client(&worker);
                if let Err(err) = result {
                    return SingleResponse::err(Error::Panic(err.to_string()));
                }
            }
            Err(_) => return SingleResponse::err(Error::Other(WORKER_INTERFACE_UNAVAILABLE)),
        }

        // Add worker to worker manager.
        match self.worker_manager.lock() {
            Ok(mut manager) => {
                let worker_id = worker.get_worker_id().to_owned();
                manager.add_worker(worker);

                let mut response = pb::RegisterWorkerResponse::new();
                response.set_worker_id(worker_id);
                SingleResponse::completed(response)
            }
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
        }
    }

    fn update_worker_status(
        &self,
        _o: RequestOptions,
        request: pb::UpdateStatusRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.worker_manager.lock() {
            Ok(mut manager) => {
                match manager.get_worker(request.get_worker_id()) {
                    Some(ref mut worker) => {
                        worker.set_status(request.get_worker_status());
                        worker.set_operation_status(request.get_operation_status());
                        worker.set_status_last_updated(Utc::now());

                        let response = pb::EmptyMessage::new();
                        SingleResponse::completed(response)
                    }
                    None => SingleResponse::err(Error::Other(INVALID_WORKER_ID)),
                }
            }
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
        }
    }

    fn return_map_result(
        &self,
        _o: RequestOptions,
        request: pb::MapResult,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.worker_manager.lock() {
            Ok(mut manager) => {
                let worker = match manager.get_worker(request.get_worker_id()) {
                    Some(worker) => worker,
                    None => return SingleResponse::err(Error::Other(INVALID_WORKER_ID)),
                };

                let task_id = worker.get_current_task_id().to_owned();
                if task_id.is_empty() {
                    return SingleResponse::err(Error::Other(INVALID_TASK_ID));
                }

                match self.scheduler.lock() {
                    Ok(mut scheduler) => {
                        let result = scheduler.process_map_task_response(&task_id, &request);
                        if let Err(err) = result {
                            error!("Could not process map response: {}", err);
                            return SingleResponse::err(Error::Panic(err.to_string()));
                        }
                    }
                    Err(_) => return SingleResponse::err(Error::Other(SCHEDULER_UNAVAILABLE)),
                }

                worker.set_status(pb::UpdateStatusRequest_WorkerStatus::AVAILABLE);
                if request.get_status() == pb::ResultStatus::SUCCESS {
                    worker.set_operation_status(pb::UpdateStatusRequest_OperationStatus::COMPLETE);
                } else {
                    worker.set_operation_status(pb::UpdateStatusRequest_OperationStatus::FAILED);
                }
                worker.set_status_last_updated(Utc::now());
                worker.set_current_task_id("");

                let response = pb::EmptyMessage::new();
                SingleResponse::completed(response)
            }
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
        }
    }

    fn return_reduce_result(
        &self,
        _o: RequestOptions,
        request: pb::ReduceResult,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.worker_manager.lock() {
            Ok(mut manager) => {
                let worker = match manager.get_worker(request.get_worker_id()) {
                    Some(worker) => worker,
                    None => return SingleResponse::err(Error::Other(INVALID_WORKER_ID)),
                };

                let task_id = worker.get_current_task_id().to_owned();
                if task_id.is_empty() {
                    return SingleResponse::err(Error::Other(INVALID_TASK_ID));
                }

                match self.scheduler.lock() {
                    Ok(mut scheduler) => {
                        let result = scheduler.process_reduce_task_response(&task_id, &request);
                        if let Err(err) = result {
                            error!("Could not process reduce response: {}", err);
                            return SingleResponse::err(Error::Panic(err.to_string()));
                        }
                    }
                    Err(_) => return SingleResponse::err(Error::Other(SCHEDULER_UNAVAILABLE)),
                }

                worker.set_status(pb::UpdateStatusRequest_WorkerStatus::AVAILABLE);
                if request.get_status() == pb::ResultStatus::SUCCESS {
                    worker.set_operation_status(pb::UpdateStatusRequest_OperationStatus::COMPLETE);
                } else {
                    worker.set_operation_status(pb::UpdateStatusRequest_OperationStatus::FAILED);
                }
                worker.set_status_last_updated(Utc::now());
                worker.set_current_task_id("");

                let response = pb::EmptyMessage::new();
                SingleResponse::completed(response)
            }
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cerberus_proto::worker_grpc::WorkerService;
    use mapreduce_tasks::TaskProcessor;

    #[test]
    fn test_register_worker() {
        let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));
        let worker_interface = Arc::new(RwLock::new(WorkerInterfaceImpl::new()));
        let task_processor = TaskProcessor;
        let scheduler = Arc::new(Mutex::new(
            MapReduceScheduler::new(Box::new(task_processor)),
        ));

        let worker_service =
            WorkerServiceImpl::new(worker_manager.clone(), worker_interface, scheduler);

        let mut register_worker_request = pb::RegisterWorkerRequest::new();
        register_worker_request.set_worker_address(String::from("127.0.0.1:8080"));

        // Register worker
        worker_service.register_worker(RequestOptions::new(), register_worker_request);

        let locked_worker_manager = worker_manager.lock().unwrap();
        assert_eq!(locked_worker_manager.get_workers().len(), 1);
    }
}
