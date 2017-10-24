#[macro_use]
extern crate error_chain;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate grpc;
extern crate tls_api;
extern crate uuid;
extern crate protobuf;
extern crate cerberus_proto;
#[macro_use]
extern crate serde_json;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAIN_LOOP_SLEEP_MS: u64 = 100;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;

use errors::*;
use std::{thread, time};
use std::sync::{Arc, Mutex};
use worker_interface::WorkerInterface;
use mrworkerservice::MRWorkerServiceImpl;
use operation_handler::OperationHandler;

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    env_logger::init().chain_err(
        || "Failed to initialise logging.",
    )?;

    let operation_handler = Arc::new(Mutex::new(OperationHandler::new()));
    let worker_service_impl = MRWorkerServiceImpl::new(operation_handler);
    let worker_server_interface = WorkerInterface::new(worker_service_impl).chain_err(
        || "Error building worker interface.",
    )?;

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

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpc::Error);
            Io(::std::io::Error);
        }
    }
}

pub mod worker_interface;
pub mod mrworkerservice;
pub mod operation_handler;
