#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod errors {
    error_chain!{
        foreign_links {
            SerdeJson(::serde_json::error::Error);
        }
    }
}

pub mod emitter;
pub mod mapper;
pub mod reducer;
pub mod serialise;
