use errors::*;
use std::sync::Arc;
use grpcio::{Environment, ServerBuilder};
use grpcio::Server;

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct ClientInterface {
    server: Server,
}

impl ClientInterface {
    pub fn new() -> Result<Self> {
        let env = Arc::new(Environment::new(GRPC_THREAD_POOL_SIZE));
        let server_builder: ServerBuilder = ServerBuilder::new(env);

        Ok(ClientInterface { server: server_builder.build()? })
    }

    pub fn start_server(&mut self) {
        self.server.start();
    }
}

#[cfg(test)]
mod tests {}
