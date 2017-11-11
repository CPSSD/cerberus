use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use operation_handler::OperationHandler;
use std::sync::{Arc, Mutex};

const OPERATION_HANDLER_UNAVAILABLE: &str = "Operation Handler not available";

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
        match self.operation_handler.lock() {
            Ok(mut handler) => {
                let result = handler.perform_map(&map_options);

                match result {
                    Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                }
            }
            Err(err) => {
                error!("Error locking operation handler to perform map: {}", err);
                SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE))
            }
        }
    }

    fn perform_reduce(
        &self,
        _o: RequestOptions,
        reduce_options: pb::PerformReduceRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.operation_handler.lock() {
            Ok(mut handler) => {
                let result = handler.perform_reduce(&reduce_options);
                match result {
                    Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                }
            }
            Err(err) => {
                error!("Error locking operation handler to perform reduce: {}", err);
                SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE))
            }
        }
    }
}
