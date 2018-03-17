use std::{thread, time};

use errors::*;
use initialization::MasterResources;

const MAIN_LOOP_SLEEP_MS: u64 = 100;
const DUMP_LOOP_MS: u64 = 5000;

pub fn run_main_loop(mut state: MasterResources) -> Result<()> {
    let mut iterations_since_state_dump = 0;
    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        if !state.grpc_server.is_alive() {
            state.dashboard_server.iron_server.close().chain_err(
                || "Failed to close dashboard server",
            )?;
            return Err("GRPC server unexpectedly died".into());
        }

        if state.state_handler.get_should_dump_state() {
            iterations_since_state_dump += 1;
            if iterations_since_state_dump * MAIN_LOOP_SLEEP_MS >= DUMP_LOOP_MS {
                state.state_handler.dump_state().chain_err(
                    || "Unable to dump state",
                )?;
                iterations_since_state_dump = 0
            }
        }
    }
}
