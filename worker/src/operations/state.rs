
use cerberus_proto::worker as pb;

/// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
pub struct OperationState {
    pub current_task_id: String,
    pub operation_status: pb::OperationStatus,

    // Initial CPU time of the current operation. This is used to calculate the total cpu time used
    // for an operation.
    pub initial_cpu_time: u64,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            current_task_id: String::new(),
            operation_status: pb::OperationStatus::UNKNOWN,

            initial_cpu_time: 0,
        }
    }

    pub fn task_cancelled(&self, task_id: &str) -> bool {
        self.current_task_id != task_id
    }
}
