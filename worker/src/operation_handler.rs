use cerberus_proto::mrworker::*;

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
#[derive(Default)]
pub struct OperationHandler {
    worker_status: WorkerStatusResponse_WorkerStatus,
    operation_status: WorkerStatusResponse_OperationStatus,
}

impl OperationHandler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_worker_status(&self) -> WorkerStatusResponse_WorkerStatus {
        self.worker_status
    }

    pub fn get_worker_operation_status(&self) -> WorkerStatusResponse_OperationStatus {
        self.operation_status
    }

    #[allow(unused_variables)]
    pub fn perform_map(&mut self, map_options: PerformMapRequest) {}

    pub fn get_map_result(&self) -> MapResponse {
        MapResponse::new()
    }

    #[allow(unused_variables)]
    pub fn perform_reduce(&mut self, reduce_options: PerformReduceRequest) {}

    pub fn get_reduce_result(&self) -> ReduceResponse {
        ReduceResponse::new()
    }
}
