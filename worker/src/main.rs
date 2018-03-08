#![feature(conservative_impl_trait)]
#![cfg_attr(test, feature(proc_macro))]

extern crate bson;
extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate grpc;
extern crate local_ip;
extern crate libc;
#[macro_use]
extern crate log;
extern crate procinfo;
extern crate protobuf;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate tls_api;
extern crate util;
extern crate uuid;

#[cfg(test)]
extern crate mocktopus;

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

mod master_interface;
mod operations;
mod server;
mod parser;
mod worker_interface;

use std::{thread, time};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::path::{Path, PathBuf};

use clap::ArgMatches;

use errors::*;
use master_interface::MasterInterface;
use operations::OperationHandler;
use server::{Server, ScheduleOperationService, IntermediateDataService, FileSystemService};
use util::init_logger;
use util::data_layer::{AbstractionLayer, NullAbstractionLayer, NFSAbstractionLayer};
use util::distributed_filesystem::{LocalFileManager, DFSAbstractionLayer,
                                   NetworkFileSystemMasterInterface};

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAX_HEALTH_CHECK_FAILURES: u16 = 10;
const MAIN_LOOP_SLEEP_MS: u64 = 3000;
const RECONNECT_FAILED_WAIT_MS: u64 = 5000;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;
// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";
const DEFAULT_MASTER_ADDR: &str = "[::]:8081";
const DEFAULT_WORKER_IP: &str = "[::]";
const DFS_FILE_DIRECTORY: &str = "/tmp/cerberus/dfs/";

fn register_worker(master_interface: &MasterInterface, address: &SocketAddr) -> Result<()> {
    let mut retries = WORKER_REGISTRATION_RETRIES;
    while retries > 0 {
        info!("Attempting to register with the master");
        retries -= 1;

        match master_interface.register_worker(address) {
            Ok(_) => break,
            Err(err) => {
                debug!("Error registering worker with master: {:?}", err);
                if retries == 0 {
                    return Err(err.chain_err(|| "Error registering worker with master"));
                }
            }
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
    }

    Ok(())
}

type AbstractionLayerArc = Arc<AbstractionLayer + Send + Sync>;

fn get_data_abstraction_layer(
    master_addr: SocketAddr,
    matches: &ArgMatches,
) -> Result<(AbstractionLayerArc, Option<Arc<LocalFileManager>>)> {
    let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>;
    let local_file_manager: Option<Arc<LocalFileManager>>;

    let nfs_path = matches.value_of("nfs");
    let dfs = matches.is_present("dfs");
    if let Some(path) = nfs_path {
        data_abstraction_layer = Arc::new(NFSAbstractionLayer::new(Path::new(path)));
        local_file_manager = None;
    } else if dfs {
        let mut storage_dir = PathBuf::new();
        storage_dir.push(DFS_FILE_DIRECTORY);
        let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));

        let master_interface = Box::new(
            NetworkFileSystemMasterInterface::new(master_addr)
                .chain_err(|| "Error creating filesystem master interface.")?,
        );

        data_abstraction_layer = Arc::new(DFSAbstractionLayer::new(
            Arc::clone(&local_file_manager_arc),
            master_interface,
        ));

        local_file_manager = Some(local_file_manager_arc);
    } else {
        data_abstraction_layer = Arc::new(NullAbstractionLayer::new());
        local_file_manager = None;
    }

    Ok((data_abstraction_layer, local_file_manager))
}

fn run() -> Result<()> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(
        matches.value_of("master").unwrap_or(DEFAULT_MASTER_ADDR),
    ).chain_err(|| "Error parsing master address")?;
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let master_interface = Arc::new(MasterInterface::new(master_addr).chain_err(
        || "Error creating master interface.",
    )?);

    let (data_abstraction_layer, local_file_manager) =
        get_data_abstraction_layer(master_addr, &matches)
            .chain_err(|| "Error creating data abstraction layer.")?;

    let operation_handler = Arc::new(OperationHandler::new(
        Arc::clone(&master_interface),
        Arc::clone(&data_abstraction_layer),
    ));

    let scheduler_service = ScheduleOperationService::new(Arc::clone(&operation_handler));
    let interm_data_service = IntermediateDataService;
    let filesystem_service = FileSystemService::new(local_file_manager);

    let srv = Server::new(
        port,
        scheduler_service,
        interm_data_service,
        filesystem_service,
    ).chain_err(|| "Can't create server")?;

    let local_ip_addr = matches.value_of("ip").unwrap_or(DEFAULT_WORKER_IP);

    let local_addr = SocketAddr::from_str(&format!(
        "{}:{}",
        local_ip_addr,
        srv.addr().port(),
    )).chain_err(|| "Not a valid address of the worker")?;
    register_worker(&*master_interface, &local_addr).chain_err(
        || "Failed to register worker.",
    )?;

    info!(
        "Successfully registered worker ({}) with master on {}",
        local_addr.to_string(),
        master_addr.to_string(),
    );

    let mut current_health_check_failures = 0;

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        if current_health_check_failures >= MAX_HEALTH_CHECK_FAILURES {
            if let Err(err) = register_worker(&*master_interface, &local_addr) {
                error!("Failed to re-register worker after disconnecting: {}", err);
                thread::sleep(time::Duration::from_millis(RECONNECT_FAILED_WAIT_MS));
            } else {
                info!("Successfully re-registered with master after being disconnected.");
                current_health_check_failures = 0;
            }
        } else if let Err(err) = operation_handler.update_worker_status() {
            error!("Could not send updated worker status to master: {}", err);
            current_health_check_failures += 1;
        } else {
            current_health_check_failures = 0;
        }

        if !srv.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }
    }
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
