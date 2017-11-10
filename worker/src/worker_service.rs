use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use operation_handler::OperationHandler;
use std::sync::{Arc, Mutex};

const OPERATION_HANDLER_UNAVAILABLE: &str = "Operation Handler not available";

pub struct WorkerServiceImpl {
    operation_handler: Arc<Mutex<OperationHandler>>,
}

impl WorkerServiceImpl {
    pub fn new(operation_handler: Arc<Mutex<OperationHandler>>) -> Self {
        WorkerServiceImpl { operation_handler: operation_handler }
    }

    fn get_worker_status(&self) -> pb::WorkerStatusResponse_WorkerStatus {
        match self.operation_handler.lock() {
            Ok(handler) => handler.get_worker_status(),
            Err(err) => {
                error!("Error getting worker status: {}", err);
                pb::WorkerStatusResponse_WorkerStatus::BUSY
            }
        }
    }

    fn get_worker_operation_status(&self) -> pb::WorkerStatusResponse_OperationStatus {
        match self.operation_handler.lock() {
            Ok(handler) => handler.get_worker_operation_status(),
            Err(err) => {
                error!("Error getting worker operation status: {}", err);
                pb::WorkerStatusResponse_OperationStatus::UNKNOWN
            }
        }
    }
}

impl grpc_pb::WorkerService for WorkerServiceImpl {
    fn worker_status(
        &self,
        _o: RequestOptions,
        _message: pb::EmptyMessage,
    ) -> SingleResponse<pb::WorkerStatusResponse> {
        let mut response = pb::WorkerStatusResponse::new();
        response.set_worker_status(self.get_worker_status());
        response.set_operation_status(self.get_worker_operation_status());

        SingleResponse::completed(response)
    }

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

    fn get_map_result(
        &self,
        _o: RequestOptions,
        _message: pb::EmptyMessage,
    ) -> SingleResponse<pb::MapResponse> {
        match self.operation_handler.lock() {
            Ok(handler) => {
                let result = handler.get_map_result();
                match result {
                    Ok(response) => SingleResponse::completed(response),
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                }
            }
            Err(err) => {
                error!("Error locking operation handler to get map result: {}", err);
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
            Err(err) => {
                error!("Error locking operation handler to perform reduce: {}", err);
                SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE))
            }
            Ok(mut handler) => {
                let result = handler.perform_reduce(&reduce_options);
                match result {
                    Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                }
            }
        }
    }

    fn get_reduce_result(
        &self,
        _o: RequestOptions,
        _message: pb::EmptyMessage,
    ) -> SingleResponse<pb::ReduceResponse> {
        match self.operation_handler.lock() {
            Ok(handler) => {
                let result = handler.get_reduce_result();
                match result {
                    Ok(response) => SingleResponse::completed(response),
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                }
            }
            Err(err) => {
                error!("Error locking operation handler for reduce result: {}", err);
                SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE))
            }
        }
    }
}
