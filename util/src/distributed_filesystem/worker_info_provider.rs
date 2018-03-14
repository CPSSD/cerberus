
use std::collections::HashMap;
use std::net::SocketAddr;

pub trait WorkerInfoProvider {
    // Returns a map of Worker Id -> Worker Address
    fn get_workers(&self) -> HashMap<String, SocketAddr>;
}
