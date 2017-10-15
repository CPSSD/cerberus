use grpc::{Server, ServerBuilder};
use errors::*;
use cerberus_proto::mrworker_grpc::*;
use mrworkerservice::WORKER_IMPL;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const WORKER_PORT: u16 = 0; // Setting the port to 0 means a random available port will be selected

pub struct WorkerInterface {
    server: Server,
}

/// `WorkerInterface` is the implementation of the interface used by the worker to recieve commands
/// from the master. This will be used by the master to schedule `MapReduce` operations on the worker
impl WorkerInterface {
    pub fn new() -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(WORKER_PORT);
        server_builder.add_service(MRWorkerServiceServer::new_service_def(WORKER_IMPL));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        Ok(WorkerInterface {
            server: server_builder.build().chain_err(|| "Error building server")?,
        })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
