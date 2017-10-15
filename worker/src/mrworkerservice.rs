use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::mrworker::*;
use cerberus_proto::mrworker_grpc::*;
use operation_handler::OperationHandler;
use std::sync::{Arc, Mutex};

const OPERATION_HANDLER_UNAVAILABLE: &'static str = "Operation Handler not available";

pub struct MRWorkerServiceImpl {
    operation_handler: Arc<Mutex<OperationHandler>>,
}

impl MRWorkerServiceImpl {
    pub fn new(operation_handler: Arc<Mutex<OperationHandler>>) -> Self {
        MRWorkerServiceImpl { operation_handler: operation_handler }
    }

    fn get_worker_status(&self) -> WorkerStatusResponse_WorkerStatus {
        match self.operation_handler.lock() {
            Err(_) => WorkerStatusResponse_WorkerStatus::BUSY,
            Ok(handler) => handler.get_worker_status(),
        }
    }

    fn get_worker_operation_status(&self) -> WorkerStatusResponse_OperationStatus {
        match self.operation_handler.lock() {
            Err(_) => WorkerStatusResponse_OperationStatus::UNKNOWN,
            Ok(handler) => handler.get_worker_operation_status(),
        }
    }
}

impl MRWorkerService for MRWorkerServiceImpl {
    fn worker_status(
        &self,
        _o: ::grpc::RequestOptions,
        _message: EmptyMessage,
    ) -> SingleResponse<WorkerStatusResponse> {
        let mut response = WorkerStatusResponse::new();
        response.set_worker_status(self.get_worker_status());
        response.set_operation_status(self.get_worker_operation_status());

        SingleResponse::completed(response)
    }

    fn perform_map(
        &self,
        _o: RequestOptions,
        map_options: PerformMapRequest,
    ) -> SingleResponse<EmptyMessage> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(mut handler) => {
                handler.perform_map(map_options);

                let response = EmptyMessage::new();
                SingleResponse::completed(response)
            }
        }
    }

    fn get_map_result(
        &self,
        _o: ::grpc::RequestOptions,
        _message: EmptyMessage,
    ) -> SingleResponse<MapResponse> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(handler) => {
                let response = handler.get_map_result();
                SingleResponse::completed(response)
            }
        }
    }

    fn perform_reduce(
        &self,
        _o: ::grpc::RequestOptions,
        reduce_options: PerformReduceRequest,
    ) -> SingleResponse<EmptyMessage> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(mut handler) => {
                handler.perform_reduce(reduce_options);

                let response = EmptyMessage::new();
                SingleResponse::completed(response)
            }
        }
    }

    fn get_reduce_result(
        &self,
        _o: ::grpc::RequestOptions,
        _message: EmptyMessage,
    ) -> SingleResponse<ReduceResponse> {
        match self.operation_handler.lock() {
            Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Ok(handler) => {
                let response = handler.get_reduce_result();
                SingleResponse::completed(response)
            }
        }
    }
}
