use std::str::FromStr;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use operations::OperationHandler;
use server::{Server, ScheduleOperationService, IntermediateDataService, FileSystemService};
use util::distributed_filesystem::LocalFileManager;

// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";

pub fn initialize_grpc_server(
    matches: &ArgMatches,
    operation_handler: &Arc<OperationHandler>,
    local_file_manager: Option<Arc<LocalFileManager>>,
) -> Result<Server> {
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let scheduler_service = ScheduleOperationService::new(Arc::clone(operation_handler));
    let interm_data_service = IntermediateDataService;
    let filesystem_service = FileSystemService::new(local_file_manager);

    let server = Server::new(
        port,
        scheduler_service,
        interm_data_service,
        filesystem_service,
    ).chain_err(|| "Error building grpc server")?;

    Ok(server)
}
