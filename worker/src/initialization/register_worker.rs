use std::{thread, time};
use std::net::SocketAddr;

use errors::*;
use master_interface::MasterInterface;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;

pub fn register_worker(
    master_interface: &MasterInterface,
    address: &SocketAddr,
    worker_id: &str,
) -> Result<String> {
    let mut retries = WORKER_REGISTRATION_RETRIES;
    let mut new_worker_id = String::new();
    while retries > 0 {
        info!("Attempting to register with the master");
        retries -= 1;

        match master_interface.register_worker(address, worker_id) {
            Ok(new_id) => {
                new_worker_id = new_id;
                break;
            }
            Err(err) => {
                debug!("Error registering worker with master: {:?}", err);
                if retries == 0 {
                    return Err(err.chain_err(|| "Error registering worker with master"));
                }
            }
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
    }

    Ok(new_worker_id)
}
