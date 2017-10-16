use errors::*;
use grpc::{ServerBuilder, Server};

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct ClientInterface {
    server: Server,
}

impl ClientInterface {
    pub fn new() -> Result<Self> {
        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        Ok(ClientInterface { server: server_builder.build()? })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}

#[cfg(test)]
mod tests {}
