extern crate chrono;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

pub mod errors {
    error_chain!{}
}

pub mod logging;

pub use logging::init_logger;
pub use logging::output_error;
