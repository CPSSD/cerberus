use std::str::FromStr;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use scheduling::Scheduler;
use server::{ClientService, FileSystemService, Server, WorkerService};
use util::data_layer::AbstractionLayer;
use util::distributed_filesystem::FileSystemManager;
use worker_management::WorkerManager;

const DEFAULT_PORT: &str = "8081";

pub fn initialize_grpc_server(
    matches: &ArgMatches,
    worker_manager: &Arc<WorkerManager>,
    scheduler: &Arc<Scheduler>,
    data_layer: &Arc<AbstractionLayer + Send + Sync>,
    filesystem_manager: Option<Arc<FileSystemManager>>,
) -> Result<Server> {
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    // Cli to Master Communications
    let client_service = ClientService::new(Arc::clone(scheduler), Arc::clone(data_layer));

    let file_system_service = FileSystemService::new(filesystem_manager);

    let worker_service = WorkerService::new(Arc::clone(worker_manager));

    let server = Server::new(port, client_service, worker_service, file_system_service)
        .chain_err(|| "Error building grpc server.")?;

    Ok(server)
}
