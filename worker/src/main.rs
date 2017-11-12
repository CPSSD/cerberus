extern crate bson;
extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
extern crate libc;
#[macro_use]
extern crate log;
extern crate procinfo;
extern crate protobuf;
extern crate serde;
#[macro_use]
extern crate serde_derive;
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

mod operation_handler;
mod master_interface;
mod worker_interface;
mod schedule_operation_service;
mod parser;

use errors::*;
use std::{thread, time};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use util::init_logger;
use worker_interface::WorkerInterface;
use master_interface::MasterInterface;
use operation_handler::OperationHandler;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAIN_LOOP_SLEEP_MS: u64 = 3000;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;
// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";

fn register_worker(
    master_interface: &Arc<Mutex<MasterInterface>>,
    address: &SocketAddr,
) -> Result<()> {
    let mut retries = WORKER_REGISTRATION_RETRIES;
    while retries > 0 {
        match master_interface.lock() {
            Ok(mut interface) => {
                match interface.register_worker(address) {
                    Err(err) => {
                        if retries == 0 {
                            return Err(err.chain_err(|| "Error registering worker with master"));
                        }
                    }
                    Ok(_) => break,
                }
            }
            Err(err) => {
                error!(
                    "Could not lock master_interface to register worker: {}",
                    err
                )
            }
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
        retries -= 1;
    }

    Ok(())
}

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(matches.value_of("master").unwrap_or("[::]:8081"))
        .chain_err(|| "Error parsing master address")?;
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let master_interface = Arc::new(Mutex::new(MasterInterface::new(master_addr).chain_err(
        || "Error creating master interface.",
    )?));
    let operation_handler = Arc::new(Mutex::new(
        OperationHandler::new(Arc::clone(&master_interface)),
    ));
    let worker_interface = WorkerInterface::new(Arc::clone(&operation_handler), port)
        .chain_err(|| "Error building worker interface.")?;

    // TODO: Replace this by machine's address rather than the listening server's address.
    let local_addr = worker_interface.get_server().local_addr();
    register_worker(&master_interface, local_addr).chain_err(
        || "Failed to register worker.",
    )?;

    info!("Successfully registered worker with Master.");
    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        match operation_handler.lock() {
            Ok(handler) => {
                let result = handler.update_worker_status();
                if let Err(err) = result {
                    error!("Could not send updated worker status to master: {}", err);
                }
            }
            Err(err) => {
                error!(
                    "Could not lock operation_handler to update worker status: {}",
                    err
                )
            }
        }

        let server = worker_interface.get_server();
        if !server.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
