extern crate cerberus_proto;
extern crate chrono;
extern crate env_logger;
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

use std::sync::{Arc, Mutex, RwLock};
use std::{thread, time};

use client_communication::MapReduceServiceImpl;
use errors::*;
use mapreduce_tasks::TaskProcessor;
use scheduler::{MapReduceScheduler, run_scheduling_loop};
use util::init_logger;
use worker_communication::WorkerRegistrationServiceImpl;
use worker_communication::WorkerInterfaceImpl;
use worker_management::WorkerManager;
use worker_management::{WorkerPoller, run_polling_loop};
use grpc_server::GRPCServer;

const MAIN_LOOP_SLEEP_MS: u64 = 100;

fn run() -> Result<()> {
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let task_processor = TaskProcessor;
    let map_reduce_scheduler = Arc::new(Mutex::new(
        MapReduceScheduler::new(Box::new(task_processor)),
    ));
    let worker_interface = Arc::new(RwLock::new(WorkerInterfaceImpl::new()));
    let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));

    let worker_registration_service = WorkerRegistrationServiceImpl::new(
        Arc::clone(&worker_manager),
        Arc::clone(&worker_interface),
    );

    // Cli to Master Communications
    let mapreduce_service = MapReduceServiceImpl::new(Arc::clone(&map_reduce_scheduler));
    let grpc_server = GRPCServer::new(mapreduce_service, worker_registration_service)
        .chain_err(|| "Error building grpc server.")?;

    run_scheduling_loop(
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    );

    let worker_poller = WorkerPoller::new(
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
        Arc::clone(&worker_interface),
    );
    run_polling_loop(worker_poller);

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
