use std::sync::mpsc::channel;
use std::sync::Arc;

use clap::ArgMatches;

use dashboard::DashboardServer;
use errors::*;
use initialization;
use initialization::{initialize_dashboard_server, initialize_grpc_server, initialize_state_handler};
use scheduling::{Scheduler, TaskProcessorImpl};
use server::Server;
use state::StateHandler;
use worker_communication::WorkerInterfaceImpl;
use worker_management::WorkerManager;

pub struct MasterResources {
    pub dashboard_server: DashboardServer,
    pub grpc_server: Server,
    pub scheduler: Arc<Scheduler>,
    pub state_handler: StateHandler,
    pub worker_manager: Arc<WorkerManager>,
}

impl MasterResources {
    pub fn new(matches: &ArgMatches, log_file_path: &str) -> Result<Self> {
        let (worker_info_sender, worker_info_receiver) = channel();

        let (data_abstraction_layer_arc, filesystem_manager) =
            initialization::get_data_abstraction_layer(matches, worker_info_receiver)
                .chain_err(|| "Error initilizing data abstraction layer")?;

        let worker_manager = Arc::new(WorkerManager::new(
            Arc::new(WorkerInterfaceImpl::new()),
            Arc::clone(&data_abstraction_layer_arc),
            worker_info_sender,
        ));

        let task_processor = Arc::new(TaskProcessorImpl::new(Arc::clone(
            &data_abstraction_layer_arc,
        )));
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&worker_manager), task_processor));

        Ok(MasterResources {
            dashboard_server: initialize_dashboard_server(
                matches,
                &worker_manager,
                &scheduler,
                &data_abstraction_layer_arc,
                log_file_path,
            ).chain_err(|| "Error initilizing cluster dashboard")?,

            grpc_server: initialize_grpc_server(
                matches,
                &worker_manager,
                &scheduler,
                &data_abstraction_layer_arc,
                filesystem_manager.clone(),
            ).chain_err(|| "Error creating master server")?,

            scheduler: Arc::clone(&scheduler),

            state_handler: initialize_state_handler(
                matches,
                &worker_manager,
                &scheduler,
                filesystem_manager,
            ).chain_err(|| "Error initilizing state handler")?,

            worker_manager,
        })
    }
}
