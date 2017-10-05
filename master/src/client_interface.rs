use errors::*;
use std::sync::Arc;
use grpcio::{Environment, ServerBuilder};
use grpcio::Server;

struct ClientInterface {
    server: Server,
}

impl ClientInterface {
    pub fn new() -> Self {
        let env = Arc::new(Environment::new(1));
        let mut server: Server = ServerBuilder::new(env).build().unwrap();
        ClientInterface { server: server }
    }

    pub fn start_server(&mut self) {
        self.server.start();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
