#![cfg_attr(test, feature(proc_macro))]

extern crate bson;
extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate futures_cpupool;
extern crate grpc;
extern crate libc;
#[macro_use]
extern crate log;
extern crate procinfo;
extern crate protobuf;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate tls_api;
extern crate util;
extern crate uuid;

#[cfg(test)]
extern crate mocktopus;

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpc::Error);
            Io(::std::io::Error);
        }
        links {
            Util(::util::errors::Error, ::util::errors::ErrorKind);
        }
    }
}

mod communication;
mod initialization;
mod main_loop;
mod operations;
mod parser;
mod server;
mod state;

use std::net::SocketAddr;
use std::str::FromStr;

use errors::*;
use initialization::{register_worker, WorkerResources};
use util::init_logger;

const DEFAULT_WORKER_IP: &str = "[::]";

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();

    let mut resources =
        WorkerResources::new(&matches).chain_err(|| "Error initializing worker resources")?;

    let local_ip_addr = matches.value_of("ip").unwrap_or(DEFAULT_WORKER_IP);
    let local_addr = SocketAddr::from_str(&format!(
        "{}:{}",
        local_ip_addr,
        resources.grpc_server.addr().port(),
    )).chain_err(|| "Not a valid address of the worker")?;

    let worker_id = register_worker(
        &resources.master_interface,
        &local_addr,
        &resources.state_handler.get_worker_id(),
    ).chain_err(|| "Failed to register worker.")?;
    resources.state_handler.set_worker_id(&worker_id);

    info!(
        "Successfully registered worker ({}) with master on {}",
        local_addr.to_string(),
        resources.master_interface.get_master_addr().to_string(),
    );

    main_loop::run_main_loop(resources, local_addr)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
