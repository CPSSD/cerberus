#[macro_use]
extern crate error_chain;
extern crate grpc;
extern crate grpc_rust;
extern crate tls_api;
extern crate cerberus_proto;

use std::{thread, time};
use std::sync::{Arc, Mutex};
use worker_interface::WorkerInterface;
use mrworkerservice::WORKER_IMPL;
use operation_handler::OperationHandler;

fn main() {
    println!("Cerberus Worker!");

    let operation_handler = Arc::new(Mutex::new(OperationHandler::new()));
    WORKER_IMPL.set_operation_handler(operation_handler);
    let worker_server_interface = WorkerInterface::new().unwrap();

    loop {
        thread::sleep(time::Duration::from_millis(100));

        let server = worker_server_interface.get_server();
        if !server.is_alive() {
            break;
        }
    }
}

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpc::Error);
        }
    }
}

pub mod worker_interface;
pub mod mrworkerservice;
pub mod operation_handler;
