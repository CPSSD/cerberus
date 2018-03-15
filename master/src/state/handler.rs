use std::sync::Arc;
use std::fs::File;
use std::fs;
use std::io::{Read, Write};

use serde_json;
use serde_json::Value as json;

use errors::*;
use scheduling::Scheduler;
use util::distributed_filesystem::FileSystemManager;
use util::state::SimpleStateHandling;
use worker_management::WorkerManager;

pub struct StateHandler {
    scheduler: Arc<Scheduler>,
    worker_manager: Arc<WorkerManager>,
    filesystem_manager: Option<Arc<FileSystemManager>>,
    dump_dir: String,
}

impl StateHandler {
    pub fn new(
        scheduler: Arc<Scheduler>,
        worker_manager: Arc<WorkerManager>,
        filesystem_manager: Option<Arc<FileSystemManager>>,
        create_dir: bool,
        dir: &str,
    ) -> Result<Self> {
        if create_dir {
            fs::create_dir_all(dir).chain_err(|| {
                format!("Unable to create dir: {}", dir)
            })?;
        }

        Ok(StateHandler {
            scheduler: scheduler,
            worker_manager: worker_manager,
            filesystem_manager: filesystem_manager,
            dump_dir: dir.into(),
        })
    }

    pub fn dump_state(&self) -> Result<()> {
        // Get Scheduler state as JSON.
        let scheduler_json = self.scheduler.dump_state().chain_err(
            || "Unable to dump Scheduler state",
        )?;

        // Get WorkerManager state as JSON.
        let worker_manager_json = self.worker_manager.dump_state().chain_err(
            || "Unable to dump WorkerManager state",
        )?;

        // Get the filesystem manager state as JSON.
        let filesystem_manager_json = match self.filesystem_manager {
            Some(ref filesystem_manager) => {
                filesystem_manager.dump_state().chain_err(
                    || "Unable to dump FileSystemManager state",
                )?
            }
            None => json!(null),
        };

        let json = json!({
            "scheduler": scheduler_json,
            "worker_manager": worker_manager_json,
            "filesystem_manager": filesystem_manager_json,
        });

        // Write the state to file.
        let mut file = File::create(format!("{}/temp.dump", self.dump_dir))
            .chain_err(|| "Unable to create file")?;
        file.write_all(json.to_string().as_bytes()).chain_err(
            || "Unable to write data",
        )?;

        fs::rename(
            format!("{}/temp.dump", self.dump_dir),
            format!("{}/master.dump", self.dump_dir),
        ).chain_err(|| "Unable to rename file")?;

        Ok(())
    }

    pub fn load_state(&self) -> Result<()> {
        info!("Loading master from state!");
        let mut file = File::open(format!("{}/master.dump", self.dump_dir))
            .chain_err(|| "Unable to open file")?;
        let mut data = String::new();
        file.read_to_string(&mut data).chain_err(
            || "Unable to read from state file",
        )?;

        let json: serde_json::Value = serde_json::from_str(&data).chain_err(
            || "Unable to parse string as JSON",
        )?;

        // Worker manager state needs to be reset first so that the scheduler knows what tasks it
        // doesn't need to reschedule.
        // Re-establish connections with workers and update worker_manager and worker state.
        let worker_manager_json = json["worker_manager"].clone();
        if worker_manager_json == json::Null {
            return Err("Unable to retrieve WorkerManager state from JSON".into());
        }

        self.worker_manager
            .load_state(worker_manager_json)
            .chain_err(|| "Error reloading worker_manager state")?;

        // Reset scheduler state (including Jobs and Tasks) to the dumped state.
        let scheduler_json = json["scheduler"].clone();
        if scheduler_json == json::Null {
            return Err("Unable to retrieve Scheduler state from JSON".into());
        }

        self.scheduler.load_state(scheduler_json).chain_err(
            || "Error reloading scheduler state",
        )?;

        // Reset file system manager state from json.
        let filesystem_manager_json = json["filesystem_manager"].clone();
        // May be Null if the previous master was not running in distributed filesystem mode
        if filesystem_manager_json != json::Null {
            if let Some(ref filesystem_manager) = self.filesystem_manager {
                filesystem_manager
                    .load_state(filesystem_manager_json)
                    .chain_err(|| "Error reloading filesystem manager state")?;
            }
        }

        info!("Succesfully loaded from state!");
        Ok(())
    }
}
