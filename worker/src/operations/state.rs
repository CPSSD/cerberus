use std::collections::HashMap;
use std::path::PathBuf;

use serde_json;

use cerberus_proto::worker as pb;

/// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
pub struct OperationState {
    pub worker_status: pb::WorkerStatus,
    pub operation_status: pb::OperationStatus,

    // Initial CPU time of the current operation. This is used to calculate the total cpu time used
    // for an operation.
    pub initial_cpu_time: u64,

    pub intermediate_file_store: Vec<PathBuf>,

    pub waiting_map_operations: usize,
    pub intermediate_map_results: HashMap<u64, Vec<serde_json::Value>>,

    pub last_cancelled_task_id: Option<String>,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            worker_status: pb::WorkerStatus::AVAILABLE,
            operation_status: pb::OperationStatus::UNKNOWN,
            initial_cpu_time: 0,
            intermediate_file_store: Vec::new(),
            waiting_map_operations: 0,
            intermediate_map_results: HashMap::new(),
            last_cancelled_task_id: None,
        }
    }

    pub fn add_intermediate_files(&mut self, files: Vec<PathBuf>) {
        self.intermediate_file_store.extend(files.into_iter());
    }

    pub fn task_cancelled(&self, task_id: &str) -> bool {
        match self.last_cancelled_task_id.clone() {
            Some(id) => task_id == id,
            None => false,
        }
    }
}
