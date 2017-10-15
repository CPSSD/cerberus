use grpc::{RequestOptions, SingleResponse, Error};
use cerberus_proto::mrworker::*;
use cerberus_proto::mrworker_grpc::*;
use operation_handler::OperationHandler;
use std::sync::{Arc, Mutex};

pub const WORKER_IMPL: MRWorkerServiceImpl = MRWorkerServiceImpl { operation_handler: None };
const OPERATION_HANDLER_UNAVAILABLE: &'static str = "Operation Handler not available";

pub struct MRWorkerServiceImpl {
    operation_handler: Option<Arc<Mutex<OperationHandler>>>,
}

impl MRWorkerServiceImpl {
    pub fn set_operation_handler(&mut self, new_handler: Arc<Mutex<OperationHandler>>) {
        self.operation_handler = Some(new_handler);
    }

    fn get_worker_status(&self) -> WorkerStatusResponse_WorkerStatus {
        match self.operation_handler {
            None => WorkerStatusResponse_WorkerStatus::BUSY,
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(handler) => handler.get_worker_status(),
                    Err(_) => WorkerStatusResponse_WorkerStatus::BUSY,
                }
            }
        }
    }

    fn get_worker_operation_status(&self) -> WorkerStatusResponse_OperationStatus {
        match self.operation_handler {
            None => WorkerStatusResponse_OperationStatus::UNKNOWN,
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(handler) => handler.get_worker_operation_status(),
                    Err(_) => WorkerStatusResponse_OperationStatus::UNKNOWN,
                }
            }
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
        match self.operation_handler {
            None => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(mut handler) => {
                        handler.perform_map(map_options);

                        let response = EmptyMessage::new();
                        SingleResponse::completed(response)
                    }
                    Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
                }
            }
        }
    }

    fn get_map_result(
        &self,
        _o: ::grpc::RequestOptions,
        _message: EmptyMessage,
    ) -> SingleResponse<MapResponse> {
        match self.operation_handler {
            None => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(handler) => {
                        let response = handler.get_map_result();
                        SingleResponse::completed(response)
                    }
                    Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
                }
            }
        }
    }

    fn perform_reduce(
        &self,
        _o: ::grpc::RequestOptions,
        reduce_options: PerformReduceRequest,
    ) -> SingleResponse<EmptyMessage> {
        match self.operation_handler {
            None => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(mut handler) => {
                        handler.perform_reduce(reduce_options);

                        let response = EmptyMessage::new();
                        SingleResponse::completed(response)
                    }
                    Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
                }
            }
        }
    }

    fn get_reduce_result(
        &self,
        _o: ::grpc::RequestOptions,
        _message: EmptyMessage,
    ) -> SingleResponse<ReduceResponse> {
        match self.operation_handler {
            None => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
            Some(ref handler_mutex) => {
                match handler_mutex.lock() {
                    Ok(handler) => {
                        let response = handler.get_reduce_result();
                        SingleResponse::completed(response)
                    }
                    Err(_) => SingleResponse::err(Error::Other(OPERATION_HANDLER_UNAVAILABLE)),
                }
            }
        }
    }
}
