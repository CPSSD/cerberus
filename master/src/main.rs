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
mod scheduling;
mod state;
mod worker_communication;
mod worker_management;
mod server;
mod parser;

use std::sync::Arc;
use std::{thread, time};
use std::path::Path;
use std::str::FromStr;

use dashboard::DashboardServer;
use errors::*;
use scheduling::{TaskProcessorImpl, Scheduler, run_task_update_loop};
use util::init_logger;
use util::data_layer::{AbstractionLayer, NullAbstractionLayer, NFSAbstractionLayer};
use worker_communication::WorkerInterfaceImpl;
use worker_management::{WorkerManager, run_health_check_loop, run_task_assigment_loop};
use server::{Server, ClientService, FileSystemService, WorkerService};
use state::StateHandler;

const MAIN_LOOP_SLEEP_MS: u64 = 100;
const DUMP_LOOP_MS: u64 = 5000;
const DEFAULT_PORT: &str = "8081";
const DEFAULT_DUMP_DIR: &str = "/var/lib/cerberus";
const DEFAULT_DASHBOARD_ADDRESS: &str = "127.0.0.1:3000";

fn run() -> Result<()> {
    println!("Cerberus Master!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let fresh = matches.is_present("fresh");
    let create_dump_dir = !matches.is_present("nodump");

    let dump_dir = matches.value_of("state-location").unwrap_or(
        DEFAULT_DUMP_DIR,
    );

    let nfs_path = matches.value_of("nfs");
    let data_abstraction_layer_arc: Arc<AbstractionLayer + Send + Sync> = match nfs_path {
        Some(path) => Arc::new(NFSAbstractionLayer::new(Path::new(path))),
        None => Arc::new(NullAbstractionLayer::new()),
    };

    let task_processor = Arc::new(TaskProcessorImpl::new(
        Arc::clone(&data_abstraction_layer_arc),
    ));
    let worker_interface = Arc::new(WorkerInterfaceImpl::new());
    let worker_manager = Arc::new(WorkerManager::new(worker_interface));

    let map_reduce_scheduler =
        Arc::new(Scheduler::new(Arc::clone(&worker_manager), task_processor));

    let worker_service = WorkerService::new(Arc::clone(&worker_manager));

    // Cli to Master Communications
    let client_service = ClientService::new(
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&data_abstraction_layer_arc),
    );

    let file_system_service = FileSystemService::new(data_abstraction_layer_arc);

    let srv = Server::new(port, client_service, worker_service, file_system_service)
        .chain_err(|| "Error building grpc server.")?;

    let state_handler = StateHandler::new(
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
        create_dump_dir,
        dump_dir,
    ).chain_err(|| "Unable to create StateHandler")?;

    // If our state dump file exists and we aren't running a fresh copy of master we
    // should load from state.
    if !fresh && Path::new(&format!("{}/master.dump", dump_dir)).exists() {
        state_handler.load_state().chain_err(
            || "Unable to load state from file",
        )?;
    }


    // Startup worker management loops
    run_task_assigment_loop(Arc::clone(&worker_manager));
    run_health_check_loop(Arc::clone(&worker_manager));

    // Startup scheduler loop
    run_task_update_loop(
        Arc::clone(&map_reduce_scheduler),
        &Arc::clone(&worker_manager),
    );

    let dashboard_address = matches.value_of("dashboard-address").unwrap_or(
        DEFAULT_DASHBOARD_ADDRESS,
    );
    let mut dashboard = DashboardServer::new(
        &dashboard_address,
        Arc::clone(&map_reduce_scheduler),
        Arc::clone(&worker_manager),
    ).chain_err(|| "Failed to create cluster dashboard server.")?;

    let mut count = 0;
    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        if !srv.is_alive() {
            dashboard.iron_server.close().chain_err(
                || "Failed to close dashboard server",
            )?;
            return Err("GRPC server unexpectedly died".into());
        }

        if create_dump_dir {
            count += 1;
            if count * MAIN_LOOP_SLEEP_MS >= DUMP_LOOP_MS {
                state_handler.dump_state().chain_err(
                    || "Unable to dump state",
                )?;
                count = 0
            }
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
