extern crate cerberus_proto;
#[macro_use]
extern crate error_chain;
extern crate env_logger;
extern crate grpc;
#[macro_use]
extern crate log;
extern crate uuid;

const MAIN_LOOP_SLEEP_MS: u64 = 100;

use mapreduce_tasks::TaskProcessor;
use worker_manager::WorkerManager;
use worker_registration_service::WorkerRegistrationServiceImpl;
use worker_interface::{WorkerInterface, WorkerRegistrationInterface};
use mapreduce_service::MapReduceServiceImpl;
use client_interface::ClientInterface;
use scheduler::{MapReduceScheduler, run_scheduling_loop};
use std::{thread, time};
use std::sync::{Arc, Mutex, RwLock};

fn main() {
    env_logger::init().unwrap();

    let task_processor = TaskProcessor;
    let map_reduce_scheduler = Arc::new(Mutex::new(
        MapReduceScheduler::new(Box::new(task_processor)),
    ));
    let worker_interface = Arc::new(RwLock::new(WorkerInterface::new()));
    let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));

    let worker_registration_service = WorkerRegistrationServiceImpl::new(
        Arc::clone(&worker_manager),
        Arc::clone(&worker_interface),
    );
    let worker_registration_interface =
        WorkerRegistrationInterface::new(worker_registration_service).unwrap();

    // Cli to Master Communications
    let mapreduce_service = MapReduceServiceImpl::new(Arc::clone(&map_reduce_scheduler));
    let client_interface = ClientInterface::new(mapreduce_service).unwrap();

    run_scheduling_loop(
        Arc::clone(&worker_interface),
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    );


    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        // TODO: Merge the client_interface and worker_registration_interface into one server.
        let server = client_interface.get_server();
        if !server.is_alive() {
            break;
        }

        let server = worker_registration_interface.get_server();
        if !server.is_alive() {
            break;
        }
    }
}

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpc::Error);
            Io(::std::io::Error);
        }
    }
}

pub mod client_interface;
pub mod scheduler;
pub mod queued_work_store;
pub mod mapreduce_job;
pub mod mapreduce_tasks;
pub mod mapreduce_service;
pub mod worker_interface;
pub mod worker_manager;
pub mod worker_registration_service;
