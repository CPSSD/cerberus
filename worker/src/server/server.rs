use std::net::SocketAddr;

use grpc;

use errors::*;
use server::master_service::ScheduleOperationServer;

use cerberus_proto::worker_grpc;

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct Server {
    server: grpc::Server,
}

impl Server {
    pub fn new(
        port: u16,
        scheduler_server: ScheduleOperationServer,
    ) -> Result<Self> {
        let mut server_builder = grpc::ServerBuilder::new_plain();
        server_builder.http.set_port(port);
        server_builder.http.set_cpu_pool_threads(GRPC_THREAD_POOL_SIZE);

        // Register the ScheduleOperationServer
        server_builder.add_service(worker_grpc::ScheduleOperationServiceServer::new_service_def(
            scheduler_server,
        ));

        Ok(Server {
            server: server_builder.build().chain_err(|| "Error building gRPC server")?,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.server.is_alive()
    }

    pub fn addr(&self) -> &SocketAddr {
        self.server.local_addr()
    }
}
