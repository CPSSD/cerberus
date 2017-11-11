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

use std::sync::{Arc, Mutex, RwLock};
use std::{thread, time};
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

const MAIN_LOOP_SLEEP_MS: u64 = 100;
const DEFAULT_PORT: &str = "8081";

fn run() -> Result<()> {
    println!("Cerberus Master!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let task_processor = TaskProcessor;
    let map_reduce_scheduler = Arc::new(Mutex::new(
        MapReduceScheduler::new(Box::new(task_processor)),
    ));
    let worker_interface = Arc::new(RwLock::new(WorkerInterfaceImpl::new()));
    let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));

    let worker_service = WorkerServiceImpl::new(
        Arc::clone(&worker_manager),
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
    );

    // Cli to Master Communications
    let mapreduce_service = MapReduceServiceImpl::new(Arc::clone(&map_reduce_scheduler));
    let grpc_server = GRPCServer::new(port, mapreduce_service, worker_service)
        .chain_err(|| "Error building grpc server.")?;

    run_scheduling_loop(
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    );

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        let server = grpc_server.get_server();
        if !server.is_alive() {
            return Err("GRPC server unexpectedly died".into());
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
