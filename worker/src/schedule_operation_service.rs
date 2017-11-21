use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use operations::OperationHandler;
use std::sync::{Arc, Mutex};

pub struct ScheduleOperationServiceImpl {
    operation_handler: Arc<Mutex<OperationHandler>>,
}

impl ScheduleOperationServiceImpl {
    pub fn new(operation_handler: Arc<Mutex<OperationHandler>>) -> Self {
        ScheduleOperationServiceImpl { operation_handler: operation_handler }
    }
}

impl grpc_pb::ScheduleOperationService for ScheduleOperationServiceImpl {
    fn perform_map(
        &self,
        _o: RequestOptions,
        map_options: pb::PerformMapRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let mut handler = self.operation_handler.lock().unwrap();

        match handler.perform_map(&map_options) {
            Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
            Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
        }
    }

    fn perform_reduce(
        &self,
        _o: RequestOptions,
        reduce_options: pb::PerformReduceRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let mut handler = self.operation_handler.lock().unwrap();

        match handler.perform_reduce(&reduce_options) {
            Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
            Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
        }
    }
}
