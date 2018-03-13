use std::net::SocketAddr;
use std::str::FromStr;

use chrono::prelude::*;
use uuid::Uuid;
use serde_json;
use errors::*;

use state::StateHandling;

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
    CANCELLED,
    UNKNOWN,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    pub address: SocketAddr,

    pub status: pb::WorkerStatus,
    pub operation_status: pb::OperationStatus,
    pub status_last_updated: DateTime<Utc>,

    pub current_task_id: String,
    pub last_cancelled_task_id: Option<String>,
    pub worker_id: String,

    pub task_assignments_failed: u16,
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
            last_cancelled_task_id: None,
            worker_id: Uuid::new_v4().to_string(),

            task_assignments_failed: 0,
        })
    }

    fn operation_status_from_state(&self, state: &OperationStatus) -> pb::OperationStatus {
        match *state {
            OperationStatus::IN_PROGRESS => pb::OperationStatus::IN_PROGRESS,
            OperationStatus::COMPLETE => pb::OperationStatus::COMPLETE,
            OperationStatus::FAILED => pb::OperationStatus::FAILED,
            OperationStatus::CANCELLED => pb::OperationStatus::CANCELLED,
            OperationStatus::UNKNOWN => pb::OperationStatus::UNKNOWN,
        }
    }

    pub fn get_serializable_operation_status(&self) -> OperationStatus {
        match self.operation_status {
            pb::OperationStatus::IN_PROGRESS => OperationStatus::IN_PROGRESS,
            pb::OperationStatus::COMPLETE => OperationStatus::COMPLETE,
            pb::OperationStatus::FAILED => OperationStatus::FAILED,
            pb::OperationStatus::CANCELLED => OperationStatus::CANCELLED,
            pb::OperationStatus::UNKNOWN => OperationStatus::UNKNOWN,
        }
    }

    fn worker_status_from_state(&self, state: &WorkerStatus) -> pb::WorkerStatus {
        match *state {
            WorkerStatus::AVAILABLE => pb::WorkerStatus::AVAILABLE,
            WorkerStatus::BUSY => pb::WorkerStatus::BUSY,
        }
    }

    pub fn get_serializable_worker_status(&self) -> WorkerStatus {
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
            "task_assignments_failed": self.task_assignments_failed,
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

        self.task_assignments_failed = serde_json::from_value(
            data["task_assignments_failed"].clone(),
        ).chain_err(|| "Unable to convert task_assignments_failed")?;

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
}
