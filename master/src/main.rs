#![feature(conservative_impl_trait)]

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
use scheduling::run_task_update_loop;
use util::init_logger;
use worker_management::{run_health_check_loop, run_task_assigment_loop};

fn run() -> Result<()> {
    println!("Cerberus Master!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let state = MasterResources::new(&matches).chain_err(
        || "Error initilizing master",
    )?;

    // Startup worker management loops
    run_task_assigment_loop(Arc::clone(&state.worker_manager));
    run_health_check_loop(Arc::clone(&state.worker_manager));

    // Startup scheduler loop
    run_task_update_loop(
        Arc::clone(&state.scheduler),
        &Arc::clone(&state.worker_manager),
    );

    main_loop::run_main_loop(state)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
