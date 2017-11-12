extern crate cerberus_proto;
extern crate chrono;
extern crate env_logger;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
#[macro_use]
extern crate log;
extern crate uuid;
extern crate util;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate serde_json;

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

mod client_communication;
mod mapreduce_job;
mod mapreduce_tasks;
mod queued_work_store;
mod scheduler;
mod worker_communication;
mod worker_management;
mod grpc_server;
mod parser;
mod state_management;

use std::sync::{Arc, Mutex, RwLock};
use std::{thread, time};
use std::path::Path;
use std::str::FromStr;

use client_communication::MapReduceServiceImpl;
use errors::*;
use mapreduce_tasks::TaskProcessor;
use scheduler::{MapReduceScheduler, run_scheduling_loop};
use util::init_logger;
use worker_communication::WorkerServiceImpl;
use worker_communication::WorkerInterfaceImpl;
use worker_management::WorkerManager;
use grpc_server::GRPCServer;
use state_management::StateHandler;

const MAIN_LOOP_SLEEP_MS: u64 = 100;
const DUMP_LOOP_MS: u64 = 5000;
const DEFAULT_PORT: &str = "8081";

fn run() -> Result<()> {
    println!("Cerberus Master!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let fresh = matches.is_present("fresh");

    let task_processor = TaskProcessor;
    let map_reduce_scheduler = Arc::new(Mutex::new(
        MapReduceScheduler::new(Box::new(task_processor)),
    ));
    let worker_interface = Arc::new(RwLock::new(WorkerInterfaceImpl::new()));
    let worker_manager = Arc::new(Mutex::new(WorkerManager::new(
        Some(Arc::clone(&worker_interface)),
        Some(Arc::clone(&map_reduce_scheduler)),
    )));

    let worker_service = WorkerServiceImpl::new(
        Arc::clone(&worker_manager),
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
    );

    // Cli to Master Communications
    let mapreduce_service = MapReduceServiceImpl::new(Arc::clone(&map_reduce_scheduler));
    let grpc_server = GRPCServer::new(port, mapreduce_service, worker_service)
        .chain_err(|| "Error building grpc server.")?;

    let state_handler = StateHandler::new(
        port,
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    ).chain_err(|| "Unable to create StateHandler")?;

    // If our state dump file exists and we aren't running a fresh copy of master we
    // should load from state.
    if !fresh && Path::new("/var/lib/cerberus/master.dump").exists() {
        state_handler.load_state().chain_err(
            || "Unable to load state from file",
        )?;
    }

    run_scheduling_loop(
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    );

    let mut count = 0;
    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        let server = grpc_server.get_server();
        if !server.is_alive() {
            return Err("GRPC server unexpectedly died".into());
        }

        count += 1;
        if count * MAIN_LOOP_SLEEP_MS >= DUMP_LOOP_MS {
            state_handler.dump_state().chain_err(
                || "Unable to dump state",
            )?;
            count = 0
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
