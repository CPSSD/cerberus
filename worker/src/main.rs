extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate tls_api;
extern crate util;
extern crate uuid;

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

pub mod operation_handler;
pub mod worker_interface;
pub mod worker_service;
mod parser;

use errors::*;
use std::{thread, time};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use util::init_logger;
use worker_interface::WorkerInterface;
use worker_service::WorkerServiceImpl;
use operation_handler::OperationHandler;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAIN_LOOP_SLEEP_MS: u64 = 100;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(matches.value_of("master").unwrap_or("localhost:8081"))
        .chain_err(|| "Error parsing master address")?;

    let operation_handler = Arc::new(Mutex::new(OperationHandler::new()));
    let worker_service_impl = WorkerServiceImpl::new(operation_handler);
    let worker_server_interface = WorkerInterface::new(worker_service_impl, master_addr)
        .chain_err(|| "Error building worker interface.")?;

    let mut retries = WORKER_REGISTRATION_RETRIES;
    while retries > 0 {
        match worker_server_interface.register_worker() {
            Err(err) => {
                if retries == 0 {
                    return Err(err.chain_err(|| "Error registering worker with master"));
                }
            }
            Ok(_) => break,
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
        retries -= 1;
    }

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        let server = worker_server_interface.get_server();
        if !server.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
