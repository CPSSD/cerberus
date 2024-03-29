extern crate cerberus_proto;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate futures_cpupool;
extern crate grpc;
extern crate iron;
#[macro_use]
extern crate log;
extern crate mount;
extern crate protobuf;
extern crate router;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate staticfile;
extern crate urlencoded;
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

mod common;
mod dashboard;
mod initialization;
mod main_loop;
mod parser;
mod scheduling;
mod server;
mod state;
mod worker_communication;
mod worker_management;

use std::sync::Arc;

use errors::*;
use initialization::MasterResources;
use util::init_logger;

const DEFAULT_LOG_FILE_PATH: &str = "/tmp/cerberus/logs/master.log";

fn run() -> Result<()> {
    println!("Cerberus Master!");
    let matches = parser::parse_command_line();

    let log_file_path = matches
        .value_of("log-file-path")
        .unwrap_or(DEFAULT_LOG_FILE_PATH);

    init_logger(log_file_path, matches.is_present("verbose-logging"))
        .chain_err(|| "Failed to initialise logging.")?;

    let resources =
        MasterResources::new(&matches, log_file_path).chain_err(|| "Error initilizing master")?;

    // Startup worker management loops
    worker_management::run_task_assigment_loop(Arc::clone(&resources.worker_manager));
    worker_management::run_health_check_loop(Arc::clone(&resources.worker_manager));

    // Startup scheduler loop
    scheduling::run_scheduler_loop(
        Arc::clone(&resources.scheduler),
        Arc::clone(&resources.worker_manager),
    );

    main_loop::run_main_loop(resources)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
