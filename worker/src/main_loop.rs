use std::net::SocketAddr;
use std::{thread, time};

use errors::*;
use initialization::{register_worker, WorkerResources};

const MAX_HEALTH_CHECK_FAILURES: u16 = 10;
const MAIN_LOOP_SLEEP_MS: u64 = 3000;
const DUMP_LOOP_MS: u64 = 5000;
const RECONNECT_FAILED_WAIT_MS: u64 = 5000;

pub fn run_main_loop(mut resources: WorkerResources, local_addr: SocketAddr) -> Result<()> {
    let mut current_health_check_failures = 0;
    let mut iterations_since_state_dump = 0;
    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        if current_health_check_failures >= MAX_HEALTH_CHECK_FAILURES {
            match register_worker(
                &resources.master_interface,
                &local_addr,
                &resources.state_handler.get_worker_id(),
            ) {
                Ok(worker_id) => {
                    info!("Successfully re-registered with master after being disconnected.");
                    resources.state_handler.set_worker_id(&worker_id);
                    current_health_check_failures = 0;
                }
                Err(err) => {
                    error!("Failed to re-register worker after disconnecting: {}", err);
                    thread::sleep(time::Duration::from_millis(RECONNECT_FAILED_WAIT_MS));
                }
            }
        } else if let Err(err) = resources.operation_handler.update_worker_status() {
            error!("Could not send updated worker status to master: {}", err);
            current_health_check_failures += 1;
        } else {
            current_health_check_failures = 0;
        }

        if !resources.grpc_server.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }

        if resources.state_handler.get_should_dump_state() {
            iterations_since_state_dump += 1;
            if iterations_since_state_dump * MAIN_LOOP_SLEEP_MS >= DUMP_LOOP_MS {
                resources
                    .state_handler
                    .dump_state()
                    .chain_err(|| "Unable to dump state")?;
                iterations_since_state_dump = 0
            }
        }
    }
}
