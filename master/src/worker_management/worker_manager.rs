use chrono::prelude::*;
use errors::*;
use uuid::Uuid;
use std::net::SocketAddr;
use std::str::FromStr;

use cerberus_proto::worker as pb;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    address: SocketAddr,

    status: pb::WorkerStatus,
    operation_status: pb::OperationStatus,
    status_last_updated: DateTime<Utc>,

    current_task_id: String,
    worker_id: String,
}

impl Worker {
    pub fn new<S: Into<String>>(address: S) -> Result<Self> {
        Ok(Worker {
            address: SocketAddr::from_str(&address.into()).chain_err(
                || "Invalid address when creating worker",
            )?,

            status: pb::WorkerStatus::AVAILABLE,
            operation_status: pb::OperationStatus::UNKNOWN,
            status_last_updated: Utc::now(),

            current_task_id: String::new(),
            worker_id: Uuid::new_v4().to_string(),
        })
    }

    pub fn is_available_for_scheduling(&self) -> bool {
        self.current_task_id == ""
    }

    pub fn get_worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn get_address(&self) -> SocketAddr {
        self.address
    }

    pub fn get_current_task_id(&self) -> &str {
        &self.current_task_id
    }

    pub fn set_status(&mut self, status: pb::WorkerStatus) {
        self.status = status;
    }

    pub fn set_operation_status(&mut self, operation_status: pb::OperationStatus) {
        self.operation_status = operation_status;
    }

    pub fn get_status_last_updated(&self) -> DateTime<Utc> {
        self.status_last_updated
    }

    pub fn set_status_last_updated(&mut self, time: DateTime<Utc>) {
        self.status_last_updated = time
    }

    pub fn set_current_task_id<S: Into<String>>(&mut self, task_id: S) {
        self.current_task_id = task_id.into();
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

    pub fn get_total_workers(&self) -> u32 {
        self.workers.len() as u32
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
        info!(
            "New Worker ({}) connected from {}",
            worker.worker_id,
            worker.address
        );

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

    pub fn remove_worker(&mut self, worker_id: &str) {
        self.workers
            .iter()
            .position(|w| w.get_worker_id() == worker_id)
            .map(|index| self.workers.remove(index));
        info!("Removed {} from list of active workers.", worker_id);
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

        assert_eq!(worker.status, pb::WorkerStatus::AVAILABLE);
        assert_eq!(worker.operation_status, pb::OperationStatus::UNKNOWN);
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
