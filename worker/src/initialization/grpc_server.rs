use std::str::FromStr;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use operations::OperationHandler;
use server::{FileSystemService, IntermediateDataService, ScheduleOperationService, Server,
             WorkerLogService};
use util::distributed_filesystem::LocalFileManager;

// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";

pub fn initialize_grpc_server(
    matches: &ArgMatches,
    operation_handler: &Arc<OperationHandler>,
    log_file_path: &str,
    local_file_manager: Option<Arc<LocalFileManager>>,
) -> Result<Server> {
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let scheduler_service = ScheduleOperationService::new(Arc::clone(operation_handler));
    let interm_data_service = IntermediateDataService;
    let log_service = WorkerLogService::new(log_file_path.to_string());
    let filesystem_service = FileSystemService::new(local_file_manager);

    let server = Server::new(
        port,
        scheduler_service,
        interm_data_service,
        log_service,
        filesystem_service,
    ).chain_err(|| "Error building grpc server")?;

    Ok(server)
}
