use std::sync::Arc;
use std::fs::File;
use std::fs;
use std::io::{Read, Write};

use serde_json;
use serde_json::Value as json;

use errors::*;
use scheduler::Scheduler;
use worker_management::WorkerManager;

/// The `StateHandling` trait defines an object that can have it's state saved
/// and subsequently loaded from a file.
pub trait StateHandling {
    // Creates a new object from the JSON data provided.
    fn new_from_json(data: serde_json::Value) -> Result<Self>
    where
        Self: Sized;

    // Returns a JSON representation of the object.
    fn dump_state(&self) -> Result<serde_json::Value>;

    // Updates the object to match the JSON state provided.
    fn load_state(&mut self, data: serde_json::Value) -> Result<()>;
}

/// The `SimpleStateHandling` trait defines an object that can have it's state saved
/// and subsequently loaded from a file but doesn't have a new from json method. A mutable
/// reference is not needed to load it's state.
pub trait SimpleStateHandling {
    // Returns a JSON representation of the object.
    fn dump_state(&self) -> Result<serde_json::Value>;

    // Updates the object to match the JSON state provided.
    fn load_state(&self, data: serde_json::Value) -> Result<()>;
}

pub struct StateHandler {
    port: u16,
    scheduler: Arc<Scheduler>,
    worker_manager: Arc<WorkerManager>,
    dump_dir: String,
}

impl StateHandler {
    pub fn new(
        port: u16,
        scheduler: Arc<Scheduler>,
        worker_manager: Arc<WorkerManager>,
        create_dir: bool,
        dir: &str,
    ) -> Result<Self> {
        if create_dir {
            fs::create_dir_all(dir).chain_err(|| {
                format!("Unable to create dir: {}", dir)
            })?;
        }

        Ok(StateHandler {
            port: port,
            scheduler: scheduler,
            worker_manager: worker_manager,
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

        let json = json!({
            "port": self.port,
            "scheduler": scheduler_json,
            "worker_manager": worker_manager_json,
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


        Ok(())
    }
}
