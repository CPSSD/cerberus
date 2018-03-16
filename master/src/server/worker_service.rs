use std::sync::Arc;

use grpc::{RequestOptions, SingleResponse, Error};

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use common::Worker;
use worker_management::WorkerManager;
use util;

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
        let worker_id: String = request.get_worker_id().to_string();
        if worker_id != "" {
            if let Err(err) = self.worker_manager.remove_worker_if_exist(&worker_id) {
                let response = SingleResponse::err(Error::Panic(err.to_string()));
                util::output_error(&err.chain_err(|| "Unable to create new worker"));
                return response;
            }
        }

        let worker = match Worker::new(request.get_worker_address().to_string(), worker_id) {
            Ok(worker) => worker,
            Err(err) => {
                let response = SingleResponse::err(Error::Panic(err.to_string()));
                util::output_error(&err.chain_err(|| "Unable to create new worker"));
                return response;
            }
        };

        let worker_id = worker.worker_id.to_owned();
        let result = self.worker_manager.register_worker(worker);
        if let Err(err) = result {
            let response = SingleResponse::err(Error::Panic(err.to_string()));
            util::output_error(&err.chain_err(|| "Unable to register worker"));
            return response;
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
            let response = SingleResponse::err(Error::Panic(err.to_string()));
            util::output_error(&err.chain_err(|| "Unable to update worker status"));
            return response;
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
            let response = SingleResponse::err(Error::Panic(err.to_string()));
            util::output_error(&err.chain_err(|| "Unable to return map results"));
            return response;
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
            let response = SingleResponse::err(Error::Panic(err.to_string()));
            util::output_error(&err.chain_err(|| "Unable to return reduce results"));
            return response;
        }

        let response = pb::EmptyMessage::new();
        SingleResponse::completed(response)
    }
}
