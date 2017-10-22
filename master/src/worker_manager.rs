use errors::*;
use uuid::Uuid;
use std::net::SocketAddr;
use std::str::FromStr;
use mapreduce_tasks::TaskType;

use cerberus_proto::mrworker as pb;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkerTaskType {
    Map,
    Reduce,
    Idle,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    address: SocketAddr,

    scheduling_in_progress: bool,
    status: pb::WorkerStatusResponse_WorkerStatus,
    operation_status: pb::WorkerStatusResponse_OperationStatus,

    current_task_id: String,
    current_task_type: WorkerTaskType,
    worker_id: String,

    completed_operation_flag: bool,
}

impl Worker {
    pub fn new<S: Into<String>>(address: S) -> Result<Self> {
        Ok(Worker {
            address: SocketAddr::from_str(&address.into()).chain_err(
                || "Invalid address when creating worker",
            )?,

            scheduling_in_progress: false,
            status: pb::WorkerStatusResponse_WorkerStatus::AVAILABLE,
            operation_status: pb::WorkerStatusResponse_OperationStatus::UNKNOWN,

            current_task_id: String::new(),
            current_task_type: WorkerTaskType::Idle,
            worker_id: Uuid::new_v4().to_string(),

            completed_operation_flag: false,
        })
    }

    pub fn is_available_for_scheduling(&self) -> bool {
        !self.scheduling_in_progress && self.current_task_id == ""
    }

    pub fn get_scheduling_in_progress(&self) -> bool {
        self.scheduling_in_progress
    }

    pub fn set_scheduling_in_progress(&mut self, in_progress: bool) {
        self.scheduling_in_progress = in_progress;
    }

    pub fn get_worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn get_address(&self) -> SocketAddr {
        self.address
    }

    pub fn get_status(&self) -> pb::WorkerStatusResponse_WorkerStatus {
        self.status
    }

    pub fn get_operation_status(&self) -> pb::WorkerStatusResponse_OperationStatus {
        self.operation_status
    }

    pub fn get_current_task_id(&self) -> &str {
        &self.current_task_id
    }

    pub fn set_status(&mut self, status: pb::WorkerStatusResponse_WorkerStatus) {
        self.status = status;
    }

    pub fn get_current_task_type(&self) -> WorkerTaskType {
        self.current_task_type.clone()
    }

    pub fn set_operation_status(
        &mut self,
        operation_status: pb::WorkerStatusResponse_OperationStatus,
    ) {
        if self.operation_status != pb::WorkerStatusResponse_OperationStatus::COMPLETE &&
            operation_status == pb::WorkerStatusResponse_OperationStatus::COMPLETE
        {
            self.completed_operation_flag = true;
        }
        self.operation_status = operation_status;
    }

    pub fn set_current_task_id<S: Into<String>>(&mut self, task_id: S) {
        self.current_task_id = task_id.into();
    }

    pub fn set_current_task_type(&mut self, task_type: Option<TaskType>) {
        self.current_task_type = match task_type {
            None => WorkerTaskType::Idle,
            Some(task_type) => {
                match task_type {
                    TaskType::Map => WorkerTaskType::Map,
                    TaskType::Reduce => WorkerTaskType::Reduce,
                }
            }
        }
    }

    pub fn set_completed_operation_flag(&mut self, val: bool) {
        self.completed_operation_flag = val;
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

    pub fn get_worker_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for worker in &self.workers {
            ids.push(worker.get_worker_id().to_owned());
        }
        ids
    }

    pub fn get_available_workers(&mut self) -> Vec<&mut Worker> {
        let mut available_workers = Vec::new();
        for worker in &mut self.workers {
            if worker.is_available_for_scheduling() {
                available_workers.push(worker);
            }
        }

        available_workers
    }

    pub fn add_worker(&mut self, worker: Worker) {
        self.workers.push(worker);
    }

    pub fn get_worker(&mut self, worker_id: &str) -> Option<&mut Worker> {
        for worker in &mut self.workers {
            if worker.get_worker_id() == worker_id {
                return Some(worker);
            }
        }
        None
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

        assert_eq!(
            worker.status,
            pb::WorkerStatusResponse_WorkerStatus::AVAILABLE
        );
        assert_eq!(
            worker.operation_status,
            pb::WorkerStatusResponse_OperationStatus::UNKNOWN
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
