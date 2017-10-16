use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::mrworker::*;
use cerberus_proto::mrworker_grpc::*;
use std::sync::{Arc, Mutex};
use worker_manager::WorkerManager;
use worker_manager::Worker;

const WORKER_MANAGER_UNAVAILABLE: &'static str = "Worker manager not available";

pub struct WorkerRegistrationServiceImpl {
    worker_manager: Arc<Mutex<WorkerManager>>,
}

impl WorkerRegistrationServiceImpl {
    pub fn new(worker_manager: Arc<Mutex<WorkerManager>>) -> Self {
        WorkerRegistrationServiceImpl { worker_manager }
    }
}

impl MRWorkerRegistrationService for WorkerRegistrationServiceImpl {
    fn register_worker(
        &self,
        _o: RequestOptions,
        request: RegisterWorkerRequest,
    ) -> SingleResponse<EmptyMessage> {
        let worker_result = Worker::new(request.get_worker_address().to_string());
        if let Err(worker_error) = worker_result {
            return SingleResponse::err(Error::Panic(worker_error.to_string()));
        }

        let worker = worker_result.unwrap();

        match self.worker_manager.lock() {
            Err(_) => SingleResponse::err(Error::Other(WORKER_MANAGER_UNAVAILABLE)),
            Ok(mut manager) => {
                manager.add_worker(worker);
                let response = EmptyMessage::new();
                SingleResponse::completed(response)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_worker() {
        let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));
        let worker_registration_service =
            WorkerRegistrationServiceImpl::new(worker_manager.clone());

        let mut register_worker_request = RegisterWorkerRequest::new();
        register_worker_request.set_worker_address(String::from("127.0.0.1:8080"));

        // Register worker
        worker_registration_service.register_worker(RequestOptions::new(), register_worker_request);

        let locked_worker_manager = worker_manager.lock().unwrap();
        assert_eq!(locked_worker_manager.get_workers().len(), 1);
    }
}
