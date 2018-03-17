
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use initialization::{get_data_abstraction_layer, initialize_grpc_server, initialize_state_handler};
use master_interface::MasterInterface;
use operations::OperationHandler;
use server::Server;
use state::StateHandler;

const DEFAULT_MASTER_ADDR: &str = "[::]:8081";

pub struct WorkerResources {
    pub grpc_server: Server,
    pub master_interface: Arc<MasterInterface>,
    pub operation_handler: Arc<OperationHandler>,
    pub state_handler: StateHandler,
}

impl WorkerResources {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let master_addr = SocketAddr::from_str(
            matches.value_of("master").unwrap_or(DEFAULT_MASTER_ADDR),
        ).chain_err(|| "Error parsing master address")?;

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

        Ok(WorkerResources {
            grpc_server: initialize_grpc_server(
                matches,
                &operation_handler,
                local_file_manager.clone(),
            ).chain_err(|| "Error initializing grpc server")?,

            master_interface: master_interface,

            operation_handler: operation_handler,

            state_handler: initialize_state_handler(matches, local_file_manager)
                .chain_err(|| "Error initializing state handler")?,
        })
    }
}
