use std::sync::{Arc, Mutex};
use std::fs::File;
use std::fs;
use std::io::{Read, Write};
use errors::*;
use serde_json;
use serde_json::Value as json;

use scheduler::MapReduceScheduler;
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

pub struct StateHandler {
    port: u16,
    scheduler: Arc<Mutex<MapReduceScheduler>>,
    worker_manager: Arc<Mutex<WorkerManager>>,
}

impl StateHandler {
    pub fn new(
        port: u16,
        scheduler: Arc<Mutex<MapReduceScheduler>>,
        worker_manager: Arc<Mutex<WorkerManager>>,
    ) -> Result<Self> {
        fs::create_dir_all("/var/lib/cerberus").chain_err(
            || "Unable to create dir: /var/lib/cerberus",
        )?;

        Ok(StateHandler {
            port: port,
            scheduler: scheduler,
            worker_manager: worker_manager,
        })
    }

    pub fn dump_state(&self) -> Result<()> {
        // Get Scheduler state as JSON.
        let scheduler = match self.scheduler.lock() {
            Err(_) => return Err("Unable to get scheduler lock".into()),
            Ok(sched) => sched,
        };
        let scheduler_json = scheduler.dump_state().chain_err(
            || "Unable to dump Scheduler state",
        )?;

        // Get WorkerManager state as JSON.
        let worker_manager = match self.worker_manager.lock() {
            Err(_) => return Err("Unable to get worker manager lock".into()),
            Ok(manager) => manager,
        };
        let worker_manager_json = worker_manager.dump_state().chain_err(
            || "Unable to dump WorkerManager state",
        )?;

        let json = json!({
            "port": self.port,
            "scheduler": scheduler_json,
            "worker_manager": worker_manager_json,
        });

        // Write the state to file.
        let mut file = File::create("/var/lib/cerberus/temp.dump").chain_err(
            || "Unable to create file",
        )?;
        file.write_all(json.to_string().as_bytes()).chain_err(
            || "Unable to write data",
        )?;

        fs::rename(
            "/var/lib/cerberus/temp.dump",
            "/var/lib/cerberus/master.dump",
        ).chain_err(|| "Unable to rename file")?;

        Ok(())
    }

    pub fn load_state(&self) -> Result<()> {
        let mut file = File::open("/var/lib/cerberus/master.dump").chain_err(
            || "Unable to open file",
        )?;
        let mut data = String::new();
        file.read_to_string(&mut data).chain_err(
            || "Unable to read from state file",
        )?;

        let json: serde_json::Value = serde_json::from_str(&data).chain_err(
            || "Unable to parse string as JSON",
        )?;

        // Reset scheduler state (including MapReduceJobs and MapReduceTasks) to the dumped state.
        let scheduler_json = json["scheduler"].clone();
        if scheduler_json == json::Null {
            return Err("Unable to retrieve Scheduler state from JSON".into());
        }
        let mut scheduler = match self.scheduler.lock() {
            Ok(scheduler) => scheduler,
            Err(_) => return Err("Unable to get scheduler lock".into()),
        };
        scheduler.load_state(scheduler_json).chain_err(
            || "Error reloading scheduler state",
        )?;

        // Re-establish connections with workers and update worker_manager and worker state.
        let worker_manager_json = json["worker_manager"].clone();
        if worker_manager_json == json::Null {
            return Err("Unable to retrieve WorkerManager state from JSON".into());
        }
        let mut worker_manager = match self.worker_manager.lock() {
            Ok(manager) => manager,
            Err(_) => return Err("Unable to get scheduler lock".into()),
        };
        worker_manager.load_state(worker_manager_json).chain_err(
            || "Error reloading worker_manager state",
        )?;

        Ok(())
    }
}
