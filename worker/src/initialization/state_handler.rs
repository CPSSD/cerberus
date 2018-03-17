use std::path::Path;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use state::StateHandler;
use util::distributed_filesystem::LocalFileManager;

const DEFAULT_DUMP_DIR: &str = "/var/lib/cerberus";

pub fn initialize_state_handler(
    matches: &ArgMatches,
    local_file_manager: Option<Arc<LocalFileManager>>,
) -> Result<StateHandler> {
    let fresh = matches.is_present("fresh");
    let should_dump_state = !matches.is_present("nodump");

    let dump_dir = matches.value_of("state-location").unwrap_or(
        DEFAULT_DUMP_DIR,
    );

    let mut state_handler = StateHandler::new(local_file_manager, should_dump_state, dump_dir)
        .chain_err(|| "Unable to create StateHandler")?;

    if !fresh && Path::new(&format!("{}/worker.dump", dump_dir)).exists() {
        state_handler.load_state().chain_err(
            || "Error loading state",
        )?;
    }

    Ok(state_handler)
}
