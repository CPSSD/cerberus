use errors::*;
use grpc::{Server, ServerBuilder};
use cerberus_proto::mrworker_grpc::*;
use worker_registration_service::WorkerRegistrationServiceImpl;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const MASTER_PORT: u16 = 8008;

pub struct WorkerInterface {
    server: Server,
}

/// `WorkerInterface` is used by to schedule `MapReduce` operations on the worker and
/// handle registration of workers.
impl WorkerInterface {
    pub fn new(worker_registration_service: WorkerRegistrationServiceImpl) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(MASTER_PORT);
        server_builder.add_service(MRWorkerRegistrationServiceServer::new_service_def(
            worker_registration_service,
        ));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        Ok(WorkerInterface {
            server: server_builder.build().chain_err(
                || "Error building worker server",
            )?,
        })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}
