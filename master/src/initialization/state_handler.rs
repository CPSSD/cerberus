use std::path::Path;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use scheduling::Scheduler;
use state::StateHandler;
use util::distributed_filesystem::FileSystemManager;
use worker_management::WorkerManager;

const DEFAULT_DUMP_DIR: &str = "/var/lib/cerberus";

pub fn initialize_state_handler(
    matches: &ArgMatches,
    worker_manager: &Arc<WorkerManager>,
    scheduler: &Arc<Scheduler>,
    filesystem_manager: Option<Arc<FileSystemManager>>,
) -> Result<StateHandler> {
    let should_dump_state = !matches.is_present("nodump");

    let fresh = matches.is_present("fresh");

    let dump_dir = matches
        .value_of("state-location")
        .unwrap_or(DEFAULT_DUMP_DIR);

    let state_handler = StateHandler::new(
        Arc::clone(scheduler),
        Arc::clone(worker_manager),
        filesystem_manager,
        should_dump_state,
        dump_dir,
    ).chain_err(|| "Unable to create StateHandler")?;

    // If our state dump file exists and we aren't running a fresh copy of master we
    // should load from state.
    if !fresh && Path::new(&format!("{}/master.dump", dump_dir)).exists() {
        state_handler
            .load_state()
            .chain_err(|| "Unable to load state from file")?;
    }

    Ok(state_handler)
}
