use errors::*;
use grpc::RequestOptions;
use std::collections::HashMap;
use worker_management::Worker;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerService; // do not use

const NO_CLIENT_FOUND_ERR: &str = "No client found for this worker";

pub trait WorkerInterface {
    fn add_client(&mut self, worker: &Worker) -> Result<()>;
    fn remove_client(&mut self, worker_id: &str) -> Result<()>;

    fn get_worker_status(&self, worker_id: &str) -> Result<pb::WorkerStatusResponse>;

    fn schedule_map(&self, request: pb::PerformMapRequest, worker_id: &str) -> Result<()>;
    fn schedule_reduce(&self, request: pb::PerformReduceRequest, worker_id: &str) -> Result<()>;

    fn get_map_result(&self, worker_id: &str) -> Result<pb::MapResponse>;
    fn get_reduce_result(&self, worker_id: &str) -> Result<pb::ReduceResponse>;
}

#[derive(Default)]
pub struct WorkerInterfaceImpl {
    clients: HashMap<String, grpc_pb::WorkerServiceClient>,
}


/// `WorkerInterfaceImpl` is used to schedule `MapReduce` operations on the workers.
impl WorkerInterfaceImpl {
    pub fn new() -> Self {
        Default::default()
    }
}

impl WorkerInterface for WorkerInterfaceImpl {
    fn add_client(&mut self, worker: &Worker) -> Result<()> {
        if self.clients.get(worker.get_worker_id()).is_some() {
            return Err("Client already exists for this worker".into());
        }

        let client = grpc_pb::WorkerServiceClient::new_plain(
            &worker.get_address().ip().to_string(),
            worker.get_address().port(),
            Default::default(),
        ).chain_err(|| "Error building client for worker")?;

        self.clients.insert(worker.get_worker_id().into(), client);
        Ok(())
    }

    fn remove_client(&mut self, worker_id: &str) -> Result<()> {
        if self.clients.get(worker_id).is_none() {
            return Err(NO_CLIENT_FOUND_ERR.into());
        }

        self.clients.remove(worker_id);
        Ok(())
    }

    fn get_worker_status(&self, worker_id: &str) -> Result<pb::WorkerStatusResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .worker_status(RequestOptions::new(), pb::EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get worker status")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    fn schedule_map(&self, request: pb::PerformMapRequest, worker_id: &str) -> Result<()> {
        if let Some(client) = self.clients.get(worker_id) {
            client
                .perform_map(RequestOptions::new(), request)
                .wait()
                .chain_err(|| "Failed to schedule map operation")?;
            Ok(())
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    fn schedule_reduce(&self, request: pb::PerformReduceRequest, worker_id: &str) -> Result<()> {
        if let Some(client) = self.clients.get(worker_id) {
            client
                .perform_reduce(RequestOptions::new(), request)
                .wait()
                .chain_err(|| "Failed to schedule reduce operation")?;
            Ok(())
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    fn get_map_result(&self, worker_id: &str) -> Result<pb::MapResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .get_map_result(RequestOptions::new(), pb::EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get map result")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    fn get_reduce_result(&self, worker_id: &str) -> Result<pb::ReduceResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .get_reduce_result(RequestOptions::new(), pb::EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get reduce result")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }
}
