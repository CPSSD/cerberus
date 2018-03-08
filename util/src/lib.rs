extern crate cerberus_proto;
extern crate chrono;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate grpc;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate rand;
extern crate uuid;

pub mod errors {
    error_chain!{}
}

pub mod logging;
pub mod data_layer;
pub mod distributed_filesystem;

pub use logging::init_logger;
pub use logging::output_error;
