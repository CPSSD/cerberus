use grpc::{RequestOptions, SingleResponse, Error};
use std::sync::{Arc, Mutex, RwLock};
use worker_communication::WorkerInterface;
use worker_management::{Worker, WorkerManager};

use cerberus_proto::mrworker as pb;
use cerberus_proto::mrworker_grpc as grpc_pb;

const WORKER_INTERFACE_UNAVAILABLE: &'static str = "Worker interface not available";
const WORKER_MANAGER_UNAVAILABLE: &'static str = "Worker manager not available";

pub struct WorkerRegistrationServiceImpl {
    worker_manager: Arc<Mutex<WorkerManager>>,
    worker_interface: Arc<RwLock<WorkerInterface>>,
}

impl WorkerRegistrationServiceImpl {
    pub fn new(
        worker_manager: Arc<Mutex<WorkerManager>>,
        worker_interface: Arc<RwLock<WorkerInterface>>,
    ) -> Self {
        WorkerRegistrationServiceImpl {
            worker_manager,
            worker_interface,
        }
    }
}

impl grpc_pb::MRWorkerRegistrationService for WorkerRegistrationServiceImpl {
    fn register_worker(
        &self,
        _o: RequestOptions,
        request: pb::RegisterWorkerRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let worker_result = Worker::new(request.get_worker_address().to_string());
        let worker = {
            match worker_result {
                Ok(worker) => worker,
                Err(worker_error) => {
                    return SingleResponse::err(Error::Panic(worker_error.to_string()))
                }
            }
        };

        // Add client for worker to worker interface.
        match self.worker_interface.write() {
            Err(_) => return SingleResponse::err(Error::Other(WORKER_INTERFACE_UNAVAILABLE)),
            Ok(mut interface) => {
                let result = interface.add_client(&worker);
                if let Err(err) = result {
                    return SingleResponse::err(Error::Panic(err.to_string()));
                }
            }
        }

        // Add worker to worker manager.
        match self.worker_manager.lock() {
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
            Ok(mut manager) => {
                manager.add_worker(worker);
                let response = pb::EmptyMessage::new();
                SingleResponse::completed(response)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cerberus_proto::mrworker_grpc::MRWorkerRegistrationService;

    #[test]
    fn test_register_worker() {
        let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));
        let worker_interface = Arc::new(RwLock::new(WorkerInterface::new()));
        let worker_registration_service =
            WorkerRegistrationServiceImpl::new(worker_manager.clone(), worker_interface);

        let mut register_worker_request = pb::RegisterWorkerRequest::new();
        register_worker_request.set_worker_address(String::from("127.0.0.1:8080"));

        // Register worker
        worker_registration_service.register_worker(RequestOptions::new(), register_worker_request);

        let locked_worker_manager = worker_manager.lock().unwrap();
        assert_eq!(locked_worker_manager.get_workers().len(), 1);
    }
}
