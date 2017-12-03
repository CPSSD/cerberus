use std::sync::Arc;

use grpc::{RequestOptions, SingleResponse, Error};

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use common::Worker;
use worker_management::NewWorkerManager as WorkerManager;

pub struct WorkerService {
    worker_manager: Arc<WorkerManager>,
}

impl WorkerService {
    pub fn new(worker_manager: Arc<WorkerManager>) -> Self {
        WorkerService { worker_manager }
    }
}

impl grpc_pb::WorkerService for WorkerService {
    fn register_worker(
        &self,
        _o: RequestOptions,
        request: pb::RegisterWorkerRequest,
    ) -> SingleResponse<pb::RegisterWorkerResponse> {
        let worker = match Worker::new(request.get_worker_address().to_string()) {
            Ok(worker) => worker,
            Err(err) => return SingleResponse::err(Error::Panic(err.to_string())),
        };

        let worker_id = worker.worker_id.to_owned();
        let result = self.worker_manager.register_worker(worker);
        if let Err(err) = result {
            return SingleResponse::err(Error::Panic(err.to_string()));
        }

        let mut response = pb::RegisterWorkerResponse::new();
        response.set_worker_id(worker_id);
        SingleResponse::completed(response)
    }

    fn update_worker_status(
        &self,
        _o: RequestOptions,
        request: pb::UpdateStatusRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        let result = self.worker_manager.update_worker_status(
            request.get_worker_id(),
            request.get_worker_status(),
            request.get_operation_status(),
        );

        if let Err(err) = result {
            return SingleResponse::err(Error::Panic(err.to_string()));
        }

        let response = pb::EmptyMessage::new();
        SingleResponse::completed(response)
    }

    fn return_map_result(
        &self,
        _o: RequestOptions,
        request: pb::MapResult,
    ) -> SingleResponse<pb::EmptyMessage> {
        let result = self.worker_manager.process_map_task_result(&request);
        if let Err(err) = result {
            return SingleResponse::err(Error::Panic(err.to_string()));
        }

        let response = pb::EmptyMessage::new();
        SingleResponse::completed(response)
    }

    fn return_reduce_result(
        &self,
        _o: RequestOptions,
        request: pb::ReduceResult,
    ) -> SingleResponse<pb::EmptyMessage> {
        let result = self.worker_manager.process_reduce_task_result(&request);
        if let Err(err) = result {
            return SingleResponse::err(Error::Panic(err.to_string()));
        }

        let response = pb::EmptyMessage::new();
        SingleResponse::completed(response)
    }
}
