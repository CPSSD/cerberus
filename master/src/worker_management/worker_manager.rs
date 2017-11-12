use chrono::prelude::*;
use errors::*;
use uuid::Uuid;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, Mutex};
use std::str::FromStr;

use serde_json;

use worker_communication::{WorkerInterface, WorkerInterfaceImpl};
use scheduler::MapReduceScheduler;
use state_management::StateHandling;
use cerberus_proto::worker as pb;

#[derive(Serialize, Deserialize)]
/// `WorkerStatus` is the serializable counterpart to `pb::WorkerStatus`.
pub enum WorkerStatus {
    AVAILABLE,
    BUSY,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
/// `OperationStatus` is the serializable counterpart to `pb::OperationStatus`.
pub enum OperationStatus {
    IN_PROGRESS,
    COMPLETE,
    FAILED,
    UNKNOWN,
}

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
        let address: String = address.into();

        Ok(Worker {
            address: SocketAddr::from_str(&address).chain_err(
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

    fn operation_status_from_state(&self, state: &OperationStatus) -> pb::OperationStatus {
        match *state {
            OperationStatus::IN_PROGRESS => pb::OperationStatus::IN_PROGRESS,
            OperationStatus::COMPLETE => pb::OperationStatus::COMPLETE,
            OperationStatus::FAILED => pb::OperationStatus::FAILED,
            OperationStatus::UNKNOWN => pb::OperationStatus::UNKNOWN,
        }
    }

    fn get_serializable_operation_status(&self) -> OperationStatus {
        match self.operation_status {
            pb::OperationStatus::IN_PROGRESS => OperationStatus::IN_PROGRESS,
            pb::OperationStatus::COMPLETE => OperationStatus::COMPLETE,
            pb::OperationStatus::FAILED => OperationStatus::FAILED,
            pb::OperationStatus::UNKNOWN => OperationStatus::UNKNOWN,
        }
    }

    fn worker_status_from_state(&self, state: &WorkerStatus) -> pb::WorkerStatus {
        match *state {
            WorkerStatus::AVAILABLE => pb::WorkerStatus::AVAILABLE,
            WorkerStatus::BUSY => pb::WorkerStatus::BUSY,
        }
    }

    fn get_serializable_worker_status(&self) -> WorkerStatus {
        match self.status {
            pb::WorkerStatus::AVAILABLE => WorkerStatus::AVAILABLE,
            pb::WorkerStatus::BUSY => WorkerStatus::BUSY,
        }
    }
}

impl StateHandling for Worker {
    fn new_from_json(data: serde_json::Value) -> Result<Self> {
        // Convert address from a serde_json::Value to a String.
        let address: String = serde_json::from_value(data["address"].clone()).chain_err(
            || "Unable to create String from serde_json::Value",
        )?;

        // Create the worker with the above address.
        let worker_result = Worker::new(address);
        let mut worker = match worker_result {
            Ok(worker) => worker,
            Err(worker_error) => return Err(worker_error),
        };

        // Update worker state to match the given state.
        worker.load_state(data).chain_err(
            || "Unable to recreate worker from previous state",
        )?;

        Ok(worker)
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        Ok(json!({
            "address": self.address.to_string(),
            "status": self.get_serializable_worker_status(),
            "operation_status": self.get_serializable_operation_status(),
            "current_task_id": self.current_task_id,
            "worker_id": self.worker_id,
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        // Sets the worker status.
        let worker_status: WorkerStatus = serde_json::from_value(data["status"].clone())
            .chain_err(|| "Unable to convert status")?;
        self.status = self.worker_status_from_state(&worker_status);

        // Sets the operation status.
        let operation_status: OperationStatus =
            serde_json::from_value(data["operation_status"].clone())
                .chain_err(|| "Unable to convert operation status")?;
        self.operation_status = self.operation_status_from_state(&operation_status);

        // Set values of types that derive Deserialize.
        self.current_task_id = serde_json::from_value(data["current_task_id"].clone())
            .chain_err(|| "Unable to convert current_task_id")?;
        self.worker_id = serde_json::from_value(data["worker_id"].clone())
            .chain_err(|| "Unable to convert worker_id")?;

        Ok(())
    }
}

pub struct WorkerManager {
    workers: Vec<Worker>,

    // This is required for reloading workers from state.
    worker_interface: Option<Arc<RwLock<WorkerInterfaceImpl>>>,
    scheduler: Option<Arc<Mutex<MapReduceScheduler>>>,
}

impl WorkerManager {
    pub fn new(
        worker_interface: Option<Arc<RwLock<WorkerInterfaceImpl>>>,
        scheduler: Option<Arc<Mutex<MapReduceScheduler>>>,
    ) -> Self {
        WorkerManager {
            workers: Vec::new(),
            worker_interface: worker_interface,
            scheduler: scheduler,
        }
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

    pub fn recreate_worker_from_state(&mut self, data: serde_json::Value) -> Result<()> {
        let mut worker = Worker::new_from_json(data).chain_err(
            || "Unable to recreate worker",
        )?;

        // Add client for worker to worker interface.
        match self.worker_interface {
            Some(ref worker_interface) => {
                match worker_interface.write() {
                    Err(_) => return Err("WORKER_INTERFACE_UNAVAILABLE".into()),
                    Ok(mut interface) => {
                        interface.add_client(&worker).chain_err(
                            || "Unable to add client for worker",
                        )?;
                    }
                }
            }
            None => return Err("WorkerManager has no valid WorkerInterface Arc".into()),
        };

        if worker.operation_status == pb::OperationStatus::FAILED {
            info!("Requeueing task: {}", worker.get_current_task_id());

            let scheduler = match self.scheduler {
                Some(ref scheduler) => scheduler,
                None => return Err("WorkerManager has no valid MapReduceScheduler Arc".into()),
            };

            scheduler
                .lock()
                .unwrap()
                .unschedule_task(worker.get_current_task_id())
                .chain_err(|| "Unable to move task back to queue")?;
            worker.set_current_task_id(String::new());
        }

        // Add worker to worker manager.
        self.add_worker(worker);

        Ok(())
    }
}

impl StateHandling for WorkerManager {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create WorkerManager from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut worker_list_json: Vec<serde_json::Value> = Vec::new();
        for worker in &self.workers {
            worker_list_json.push(worker.dump_state().chain_err(
                || "Unable to dump Worker state",
            )?);
        }
        Ok(json!({
            "total_workers": self.get_total_workers(),
            "workers": worker_list_json,
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        let worker_count: usize = serde_json::from_value(data["total_workers"].clone())
            .chain_err(|| "Unable to convert worker_count")?;

        for i in 0..worker_count {
            let worker_data = data["workers"][i].clone();

            // Re-establish connection with the worker.
            let worker_creation_result = self.recreate_worker_from_state(worker_data);
            if let Err(err) = worker_creation_result {
                warn!("Unable to re-establish connection with a worker: {}", err);
            }

        }
        Ok(())
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

        let mut worker_manager = WorkerManager::new(None, None);
        worker_manager.add_worker(worker.clone());

        assert_eq!(worker_manager.get_workers().len(), 1);
        assert_eq!(worker_manager.get_workers()[0], worker);
    }
}
