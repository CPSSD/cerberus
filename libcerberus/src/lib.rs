#![recursion_limit = "1024"]

extern crate bson;
extern crate chrono;
extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate uuid;

const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

mod errors {
    error_chain!{
        foreign_links {
            Clap(::clap::Error);
            Io(::std::io::Error);
            SerdeJson(::serde_json::error::Error);
        }
    }
}

pub mod combiner;
pub mod emitter;
pub mod intermediate;
pub mod io;
pub mod mapper;
pub mod partition;
pub mod reducer;
pub mod registry;
pub mod runner;
pub mod serialise;

pub use combiner::Combine;
pub use emitter::{EmitFinal, EmitIntermediate};
pub use errors::*;
pub use intermediate::IntermediateInputKV;
pub use mapper::{Map, MapInputKV};
pub use partition::{HashPartitioner, Partition, PartitionInputKV};
pub use reducer::Reduce;
pub use registry::{UserImplRegistry, UserImplRegistryBuilder};
pub use runner::*;
pub use serialise::{FinalOutputObject, IntermediateOutputObject};
