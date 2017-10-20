use errors::*;
use grpc::{Server, ServerBuilder, RequestOptions};
use std::collections::HashMap;
use cerberus_proto::mrworker::*;
use cerberus_proto::mrworker_grpc::*;
use worker_registration_service::WorkerRegistrationServiceImpl;
use worker_manager::Worker;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const MASTER_PORT: u16 = 8008;
const NO_CLIENT_FOUND_ERR: &'static str = "No client found for this worker";

#[derive(Default)]
pub struct WorkerInterface {
    clients: HashMap<String, MRWorkerServiceClient>,
}


/// `WorkerInterface` is used to schedule `MapReduce` operations on the workers.
impl WorkerInterface {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_client(&mut self, worker: &Worker) -> Result<()> {
        if self.clients.get(worker.get_worker_id()).is_some() {
            return Err("Client already exists for this worker".into());
        }

        let client = MRWorkerServiceClient::new_plain(
            &worker.get_address().ip().to_string(),
            worker.get_address().port(),
            Default::default(),
        ).chain_err(|| "Error building client for worker")?;

        self.clients.insert(worker.get_worker_id().into(), client);
        Ok(())
    }

    pub fn remove_client(&mut self, worker_id: &str) -> Result<()> {
        if self.clients.get(worker_id).is_none() {
            return Err(NO_CLIENT_FOUND_ERR.into());
        }

        self.clients.remove(worker_id);
        Ok(())
    }

    pub fn get_worker_status(&self, worker_id: &str) -> Result<WorkerStatusResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .worker_status(RequestOptions::new(), EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get worker status")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    pub fn schedule_map(&self, request: PerformMapRequest, worker_id: &str) -> Result<()> {
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

    pub fn schedule_reduce(&self, request: PerformReduceRequest, worker_id: &str) -> Result<()> {
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

    pub fn get_map_result(&self, worker_id: &str) -> Result<MapResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .get_map_result(RequestOptions::new(), EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get map result")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }

    pub fn get_reduce_result(&self, worker_id: &str) -> Result<ReduceResponse> {
        if let Some(client) = self.clients.get(worker_id) {
            Ok(
                client
                    .get_reduce_result(RequestOptions::new(), EmptyMessage::new())
                    .wait()
                    .chain_err(|| "Failed to get reduce result")?
                    .1,
            )
        } else {
            Err(NO_CLIENT_FOUND_ERR.into())
        }
    }
}

pub struct WorkerRegistrationInterface {
    server: Server,
}

/// `WorkerRegistrationInterface` handles the registration of workers.
impl WorkerRegistrationInterface {
    pub fn new(worker_registration_service: WorkerRegistrationServiceImpl) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(MASTER_PORT);
        server_builder.add_service(MRWorkerRegistrationServiceServer::new_service_def(
            worker_registration_service,
        ));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        Ok(WorkerRegistrationInterface {
            server: server_builder.build().chain_err(
                || "Error building worker server",
            )?,
        })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}
