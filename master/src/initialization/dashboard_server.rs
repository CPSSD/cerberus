use std::sync::Arc;

use clap::ArgMatches;

use dashboard::DashboardServer;
use errors::*;
use scheduling::Scheduler;
use util::data_layer::AbstractionLayer;
use worker_management::WorkerManager;

const DEFAULT_DASHBOARD_ADDRESS: &str = "127.0.0.1:3000";

pub fn initialize_dashboard_server(
    matches: &ArgMatches,
    worker_manager: &Arc<WorkerManager>,
    scheduler: &Arc<Scheduler>,
    data_layer: &Arc<AbstractionLayer + Send + Sync>,
) -> Result<DashboardServer> {
    let dashboard_address = matches
        .value_of("dashboard-address")
        .unwrap_or(DEFAULT_DASHBOARD_ADDRESS);

    let dashboard = DashboardServer::new(
        dashboard_address,
        Arc::clone(scheduler),
        Arc::clone(worker_manager),
        Arc::clone(data_layer),
    ).chain_err(|| "Failed to create cluster dashboard server.")?;

    Ok(dashboard)
}
