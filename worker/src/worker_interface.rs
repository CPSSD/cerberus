extern crate error_chain;

use std::sync::Arc;
use grpcio::{Environment, Server, ServerBuilder, Error};

const GRPC_THREAD_POOL_SIZE: usize = 1;

struct WorkerInterface {
    server: Server,
}

/// `WorkerInterface` is the implementation of the interface used by the worker to recieve commands
/// from the master. This will be used by the master to schedule MapReduce operations on the worker
impl WorkerInterface {
    pub fn new() -> Result<Self, Error> {
        let env = Arc::new(Environment::new(GRPC_THREAD_POOL_SIZE));
        let mut serverBuild: ServerBuilder = ServerBuilder::new(env);

        Ok(WorkerInterface { server: serverBuild.build()? })
    }

    pub fn start_server(&mut self) {
        self.server.start();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
