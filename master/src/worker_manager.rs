use errors::*;
use cerberus_proto::mrworker::*;
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    address: SocketAddr,
    status: WorkerStatusResponse_WorkerStatus,
    operation_status: WorkerStatusResponse_OperationStatus,
    current_task_id: String,
}

impl Worker {
    pub fn new(address: String) -> Result<Self> {
        Ok(Worker {
            address: SocketAddr::from_str(&address).chain_err(
                || "Invalid address when creating worker",
            )?,
            status: WorkerStatusResponse_WorkerStatus::AVAILABLE,
            operation_status: WorkerStatusResponse_OperationStatus::UNKNOWN,
            current_task_id: String::new(),
        })
    }

    pub fn get_address(&self) -> SocketAddr {
        self.address
    }

    pub fn get_status(&self) -> WorkerStatusResponse_WorkerStatus {
        self.status
    }

    pub fn get_operation_status(&self) -> WorkerStatusResponse_OperationStatus {
        self.operation_status
    }

    pub fn get_current_task_id(&self) -> &str {
        &self.current_task_id
    }

    pub fn set_status(&mut self, status: WorkerStatusResponse_WorkerStatus) {
        self.status = status;
    }

    pub fn set_operation_status(&mut self, operation_status: WorkerStatusResponse_OperationStatus) {
        self.operation_status = operation_status;
    }

    pub fn set_current_task_id(&mut self, task_id: String) {
        self.current_task_id = task_id;
    }
}

#[derive(Default)]
pub struct WorkerManager {
    workers: Vec<Worker>,
}

impl WorkerManager {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_workers(&self) -> &Vec<Worker> {
        &self.workers
    }

    pub fn add_worker(&mut self, worker: Worker) {
        self.workers.push(worker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_worker() {
        let worker_result = Worker::new(String::from("127.0.0.1:8080"));
        assert!(!worker_result.is_err());

        let worker = worker_result.unwrap();
        assert_eq!(
            worker.address,
            SocketAddr::from_str("127.0.0.1:8080").unwrap()
        );

        assert_eq!(worker.status, WorkerStatusResponse_WorkerStatus::AVAILABLE);
        assert_eq!(
            worker.operation_status,
            WorkerStatusResponse_OperationStatus::UNKNOWN
        );
    }

    #[test]
    fn test_construct_worker_invalid_ip() {
        let worker_result = Worker::new(String::from("127.0.0.0.01:8080"));
        assert!(worker_result.is_err());
    }

    #[test]
    fn test_add_worker() {
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let mut worker_manager = WorkerManager::new();
        worker_manager.add_worker(worker.clone());

        assert_eq!(worker_manager.get_workers().len(), 1);
        assert_eq!(worker_manager.get_workers()[0], worker);
    }
}
