use std::sync::Arc;
use std::fs::File;
use std::fs;
use std::io::{Read, Write};

use serde_json;
use serde_json::Value as json;

use errors::*;
use util::distributed_filesystem::LocalFileManager;
use util::state::SimpleStateHandling;

pub struct StateHandler {
    local_file_manager: Option<Arc<LocalFileManager>>,
    dump_dir: String,
}

impl StateHandler {
    pub fn new(
        local_file_manager: Option<Arc<LocalFileManager>>,
        create_dir: bool,
        dir: &str,
    ) -> Result<Self> {
        if create_dir {
            fs::create_dir_all(dir).chain_err(|| {
                format!("Unable to create dir: {}", dir)
            })?;
        }

        Ok(StateHandler {
            local_file_manager: local_file_manager,
            dump_dir: dir.into(),
        })
    }

    pub fn dump_state(&self) -> Result<()> {
        // Get the filesystem manager state as JSON.
        let local_file_manager_json = match self.local_file_manager {
            Some(ref local_file_manager) => {
                local_file_manager.dump_state().chain_err(
                    || "Unable to dump LocalFileManager state",
                )?
            }
            None => json!(null),
        };

        let json = json!({
            "local_file_manager": local_file_manager_json,
        });

        // Write the state to file.
        let mut file = File::create(format!("{}/workertemp.dump", self.dump_dir))
            .chain_err(|| "Unable to create file")?;
        file.write_all(json.to_string().as_bytes()).chain_err(
            || "Unable to write data",
        )?;

        fs::rename(
            format!("{}/workertemp.dump", self.dump_dir),
            format!("{}/worker.dump", self.dump_dir),
        ).chain_err(|| "Unable to rename file")?;

        Ok(())
    }

    pub fn load_state(&self) -> Result<()> {
        info!("Loading worker from state!");
        let mut file = File::open(format!("{}/worker.dump", self.dump_dir))
            .chain_err(|| "Unable to open file")?;
        let mut data = String::new();
        file.read_to_string(&mut data).chain_err(
            || "Unable to read from state file",
        )?;

        let json: serde_json::Value = serde_json::from_str(&data).chain_err(
            || "Unable to parse string as JSON",
        )?;

        // Reset the local file manager state from json.
        let local_file_manager_json = json["local_file_manager"].clone();
        // May be Null if the previous worker was not running in distributed filesystem mode
        if local_file_manager_json != json::Null {
            if let Some(ref local_file_manager) = self.local_file_manager {
                local_file_manager
                    .load_state(local_file_manager_json)
                    .chain_err(|| "Error reloading local file manager state")?;
            }
        }

        info!("Succesfully loaded from state!");
        Ok(())
    }
}
