
use cerberus_proto::worker as pb;

/// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
pub struct OperationState {
    pub worker_status: pb::WorkerStatus,
    pub operation_status: pb::OperationStatus,

    // Initial CPU time of the current operation. This is used to calculate the total cpu time used
    // for an operation.
    pub initial_cpu_time: u64,

    pub waiting_map_operations: usize,
    pub map_operation_failed: bool,
    pub intermediate_map_results: Vec<String>,

    pub last_cancelled_task_id: Option<String>,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            worker_status: pb::WorkerStatus::AVAILABLE,
            operation_status: pb::OperationStatus::UNKNOWN,

            initial_cpu_time: 0,

            waiting_map_operations: 0,
            map_operation_failed: false,
            intermediate_map_results: Vec::new(),

            last_cancelled_task_id: None,
        }
    }

    pub fn task_cancelled(&self, task_id: &str) -> bool {
        match self.last_cancelled_task_id.clone() {
            Some(id) => task_id == id,
            None => false,
        }
    }
}
