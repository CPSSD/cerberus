#[macro_use]
extern crate error_chain;
extern crate grpc;
extern crate grpc_rust;
extern crate cerberus_proto;
extern crate uuid;

const MAIN_LOOP_SLEEP_MS: u64 = 100;

use worker_manager::WorkerManager;
use worker_registration_service::WorkerRegistrationServiceImpl;
use worker_interface::WorkerInterface;
use std::{thread, time};
use std::sync::{Arc, Mutex};

fn main() {
    println!("Cerberus Master!");

    let worker_manager = Arc::new(Mutex::new(WorkerManager::new()));
    let worker_registration_service = WorkerRegistrationServiceImpl::new(worker_manager);
    let worker_interface = WorkerInterface::new(worker_registration_service).unwrap();

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        let server = worker_interface.get_server();
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
pub mod worker_interface;
pub mod worker_manager;
pub mod worker_registration_service;
