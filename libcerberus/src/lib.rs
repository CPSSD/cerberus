#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate multimap;
extern crate serde;

mod errors {
    error_chain!{}
}

pub mod emitter;
pub mod mapper;
