extern crate bson;
extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
extern crate local_ip;
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

mod master_interface;
mod operations;
mod server;
mod parser;

use std::{thread, time};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use errors::*;
use master_interface::MasterInterface;
use operations::OperationHandler;
use server::{Server, ScheduleOperationService};
use util::init_logger;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAX_HEALTH_CHECK_FAILURES: u16 = 10;
const MAIN_LOOP_SLEEP_MS: u64 = 3000;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;
// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";
const DEFAULT_MASTER_ADDR: &str = "[::]:8081";

fn register_worker(
    master_interface: &Arc<Mutex<MasterInterface>>,
    address: &SocketAddr,
) -> Result<()> {
    let mut retries = WORKER_REGISTRATION_RETRIES;
    while retries > 0 {
        retries -= 1;

        let mut interface = master_interface.lock().unwrap();
        match interface.register_worker(address) {
            Ok(_) => break,
            Err(err) => {
                if retries == 0 {
                    return Err(err.chain_err(|| "Error registering worker with master"));
                }
            }
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
    }

    Ok(())
}

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(matches.value_of("master").unwrap_or(DEFAULT_MASTER_ADDR))
        .chain_err(|| "Error parsing master address")?;
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let master_interface = Arc::new(Mutex::new(MasterInterface::new(master_addr).chain_err(
        || "Error creating master interface.",
    )?));
    let operation_handler = Arc::new(Mutex::new(
        OperationHandler::new(Arc::clone(&master_interface)),
    ));

    let scheduler_server = ScheduleOperationService::new(Arc::clone(&operation_handler));
    let srv = Server::new(port, scheduler_server).chain_err(||"Can't create server")?;

    let local_addr = SocketAddr::from_str(&format!(
        "{}:{}",
        local_ip::get().expect("Could not get IP"),
        srv.addr().port(),
    )).chain_err(|| "Not a valid address of the worker")?;
    register_worker(&master_interface, &local_addr).chain_err(
        || "Failed to register worker.",
    )?;

    info!(
        "Successfully registered worker ({}) with master on {}",
        local_addr.to_string(),
        master_addr.to_string(),
    );

    let mut current_health_check_failures = 0;

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        let handler = operation_handler.lock().unwrap();

        if let Err(err) = handler.update_worker_status() {
            error!("Could not send updated worker status to master: {}", err);
            current_health_check_failures += 1;
        } else {
            current_health_check_failures = 0;
        }

        if current_health_check_failures >= MAX_HEALTH_CHECK_FAILURES {
            if let Err(err) = register_worker(&master_interface, &local_addr) {
                error!("Failed to re-register worker after disconnecting: {}", err);
            } else {
                info!("Successfully re-registered with master after being disconnected.");
                current_health_check_failures = 0;
            }
        }

        if !srv.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
