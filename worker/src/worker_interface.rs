use grpc::{Server, RequestOptions, ServerBuilder};
use errors::*;
use cerberus_proto::mrworker::*;
use cerberus_proto::mrworker_grpc::*;
use mrworkerservice::MRWorkerServiceImpl;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const WORKER_PORT: u16 = 0; // Setting the port to 0 means a random available port will be selected
const MASTER_PORT: u16 = 8008;

pub struct WorkerInterface {
    server: Server,
    client: MRWorkerRegistrationServiceClient,
}

/// `WorkerInterface` is the implementation of the interface used by the worker to recieve commands
/// from the master. This will be used by the master to schedule `MapReduce` operations on the worker
impl WorkerInterface {
    pub fn new(worker_service_impl: MRWorkerServiceImpl) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(WORKER_PORT);
        server_builder.add_service(MRWorkerServiceServer::new_service_def(worker_service_impl));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        let client = MRWorkerRegistrationServiceClient::new_plain(
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
        let mut worker_addr = String::from("localhost:");
        worker_addr.push_str(&(self.get_port().to_string()));

        let mut req = RegisterWorkerRequest::new();
        req.set_worker_address(worker_addr);

        self.client
            .register_worker(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to register worker")?;

        Ok(())
    }

    fn get_port(&self) -> u16 {
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
