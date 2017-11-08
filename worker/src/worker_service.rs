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
            Err(_) => pb::WorkerStatusResponse_WorkerStatus::BUSY,
            Ok(handler) => handler.get_worker_status(),
        }
    }

    fn get_worker_operation_status(&self) -> pb::WorkerStatusResponse_OperationStatus {
        match self.operation_handler.lock() {
            Err(_) => pb::WorkerStatusResponse_OperationStatus::UNKNOWN,
            Ok(handler) => handler.get_worker_operation_status(),
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
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(mut handler) => {
                let result = handler.perform_map(&map_options);

                match result {
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                    Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
                }
            }
        }
    }

    fn get_map_result(
        &self,
        _o: RequestOptions,
        _message: pb::EmptyMessage,
    ) -> SingleResponse<pb::MapResponse> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(handler) => {
                let result = handler.get_map_result();
                match result {
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                    Ok(response) => SingleResponse::completed(response),
                }
            }
        }
    }

    fn perform_reduce(
        &self,
        _o: RequestOptions,
        reduce_options: pb::PerformReduceRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(mut handler) => {
                let result = handler.perform_reduce(reduce_options);
                match result {
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                    Ok(_) => SingleResponse::completed(pb::EmptyMessage::new()),
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
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(handler) => {
                let result = handler.get_reduce_result();
                match result {
                    Err(err) => SingleResponse::err(Error::Panic(err.to_string())),
                    Ok(response) => SingleResponse::completed(response),
                }
            }
        }
    }
}
