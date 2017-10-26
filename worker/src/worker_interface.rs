use grpc::{Server, RequestOptions, ServerBuilder};
use errors::*;
use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerRegistrationService;
use worker_service::WorkerServiceImpl;
use std::net::IpAddr;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const WORKER_PORT: u16 = 0; // Setting the port to 0 means a random available port will be selected
const MASTER_PORT: u16 = 8081;

pub struct WorkerInterface {
    server: Server,
    client: grpc_pb::WorkerRegistrationServiceClient,
}

/// `WorkerInterface` is the implementation of the interface used by the worker to recieve commands
/// from the master.
/// This will be used by the master to schedule `MapReduce` operations on the worker
impl WorkerInterface {
    pub fn new(worker_service_impl: WorkerServiceImpl) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(WORKER_PORT);
        server_builder.add_service(grpc_pb::WorkerServiceServer::new_service_def(
            worker_service_impl,
        ));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        let client = grpc_pb::WorkerRegistrationServiceClient::new_plain(
            "localhost",
            MASTER_PORT,
            Default::default(),
        ).chain_err(|| "Error building client")?;

        Ok(WorkerInterface {
            server: server_builder.build().chain_err(|| "Error building server")?,
            client: client,
        })
    }

    pub fn register_worker(&self) -> Result<()> {
        let worker_addr = self.server.local_addr().to_string();

        let mut req = pb::RegisterWorkerRequest::new();
        req.set_worker_address(worker_addr);

        self.client
            .register_worker(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to register worker")?;

        Ok(())
    }

    pub fn get_ip(&self) -> IpAddr {
        self.server.local_addr().ip()
    }

    pub fn get_port(&self) -> u16 {
        self.server.local_addr().port()
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
