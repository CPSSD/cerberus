use errors::*;
use grpc::{ServerBuilder, Server};

use client_communication::MapReduceServiceImpl;

use cerberus_proto::mapreduce_grpc as grpc_pb;

const GRPC_THREAD_POOL_SIZE: usize = 1;
const GRPC_PORT: u16 = 8081;

pub struct ClientInterface {
    server: Server,
}

impl ClientInterface {
    pub fn new(mapreduce_service: MapReduceServiceImpl) -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(GRPC_PORT);
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );
        server_builder.add_service(grpc_pb::MapReduceServiceServer::new_service_def(
            mapreduce_service,
        ));

        Ok(ClientInterface { server: server_builder.build()? })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}

#[cfg(test)]
mod tests {}
