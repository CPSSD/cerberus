use std::collections::HashMap;
use std::sync::RwLock;

use grpc::RequestOptions;

use errors::*;
use common::Worker;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::ScheduleOperationService; // Importing methods, don't use directly

const NO_CLIENT_FOUND_ERR: &str = "No client found for this worker";

pub trait WorkerInterface: Sync + Send {
    fn add_client(&self, worker: &Worker) -> Result<()>;
    fn remove_client(&self, worker_id: &str) -> Result<()>;

    fn schedule_map(&self, request: pb::PerformMapRequest, worker_id: &str) -> Result<()>;
    fn schedule_reduce(&self, request: pb::PerformReduceRequest, worker_id: &str) -> Result<()>;
}

#[derive(Default)]
pub struct WorkerInterfaceImpl {
    clients: RwLock<HashMap<String, grpc_pb::ScheduleOperationServiceClient>>,
}


/// `WorkerInterfaceImpl` is used to schedule `MapReduce` operations on the workers.
impl WorkerInterfaceImpl {
    pub fn new() -> Self {
        Default::default()
    }
}

impl WorkerInterface for WorkerInterfaceImpl {
    fn add_client(&self, worker: &Worker) -> Result<()> {
        let mut clients = self.clients.write().unwrap();

        if clients.get(&worker.worker_id).is_some() {
            return Err(
                format!("client already exists for worker {}", &worker.worker_id).into(),
            );
        }

        info!(
            "Worker is getting added: IP={} PORT={}",
            worker.address.ip().to_string(),
            worker.address.port(),
        );

        let client = grpc_pb::ScheduleOperationServiceClient::new_plain(
            &worker.address.ip().to_string(),
            worker.address.port(),
            Default::default(),
        ).chain_err(|| "Error building client for worker")?;

        clients.insert(worker.worker_id.to_owned(), client);
        Ok(())
    }

    fn remove_client(&self, worker_id: &str) -> Result<()> {
        let mut clients = self.clients.write().unwrap();

        if clients.get(worker_id).is_none() {
            return Err(NO_CLIENT_FOUND_ERR.into());
        }

        clients.remove(worker_id);
        Ok(())
    }

    fn schedule_map(&self, request: pb::PerformMapRequest, worker_id: &str) -> Result<()> {
        let clients = self.clients.read().unwrap();

        if let Some(client) = clients.get(worker_id) {
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
        let clients = self.clients.read().unwrap();

        if let Some(client) = clients.get(worker_id) {
            client
                .perform_reduce(RequestOptions::new(), request)
                .wait()
                .chain_err(|| "Failed to schedule reduce operation")?;
            Ok(())
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }
}
