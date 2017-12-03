use std::sync::Arc;

use futures::Future;
use grpc::{RequestOptions, SingleResponse, Error};

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use operations::OperationHandler;

pub struct ScheduleOperationService {
    operation_handler: Arc<OperationHandler>,
}

impl ScheduleOperationService {
    pub fn new(operation_handler: Arc<OperationHandler>) -> Self {
        ScheduleOperationService { operation_handler: operation_handler }
    }
}

impl grpc_pb::ScheduleOperationService for ScheduleOperationService {
    fn perform_map(
        &self,
        _o: RequestOptions,
        map_options: pb::PerformMapRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let perform_map_future = self.operation_handler.perform_map(map_options);
        match perform_map_future.wait() {
            Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
            Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
        }
    }

    fn perform_reduce(
        &self,
        _o: RequestOptions,
        reduce_options: pb::PerformReduceRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let perform_reduce_future = self.operation_handler.perform_reduce(reduce_options);
        match perform_reduce_future.wait() {
            Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
            Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
        }
    }
}
