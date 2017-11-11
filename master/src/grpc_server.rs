use errors::*;
use grpc::{ServerBuilder, Server};

use client_communication::MapReduceServiceImpl;
use worker_communication::WorkerServiceImpl;

use cerberus_proto::{mapreduce_grpc, worker_grpc};

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct GRPCServer {
    server: Server,
}

impl GRPCServer {
    pub fn new(
        port: u16,
        mapreduce_service: MapReduceServiceImpl,
        worker_service: WorkerServiceImpl,
    ) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(port);
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        // Register the MapReduceService
        server_builder.add_service(mapreduce_grpc::MapReduceServiceServer::new_service_def(
            mapreduce_service,
        ));

        // Register the WorkerServiceServer
        server_builder.add_service(worker_grpc::WorkerServiceServer::new_service_def(
            worker_service,
        ));

        Ok(GRPCServer {
            server: server_builder.build().chain_err(
                || "Error building grpc server",
            )?,
        })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}

#[cfg(test)]
mod tests {}
