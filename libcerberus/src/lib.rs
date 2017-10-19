#![recursion_limit = "1024"]

extern crate chrono;
extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
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

pub mod emitter;
pub mod io;
pub mod mapper;
pub mod reducer;
pub mod runner;
pub mod serialise;

pub use errors::*;
pub use emitter::{EmitIntermediate, EmitFinal};
pub use mapper::{Map, MapInputKV};
pub use reducer::{Reduce, ReduceInputKV};
pub use runner::*;
pub use serialise::{FinalOutputObject, IntermediateOutputObject};
