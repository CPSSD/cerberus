use std::sync::{Arc, RwLock, Mutex};

use serde_json;

use common::Worker;
use errors::*;
use scheduler::MapReduceScheduler;
use state::StateHandling;
use worker_communication::{WorkerInterface, WorkerInterfaceImpl};

use cerberus_proto::worker as pb;

pub struct WorkerManager {
    workers: Vec<Worker>,

    // This is required for reloading workers from state.
    worker_interface: Option<Arc<RwLock<WorkerInterfaceImpl>>>,
    scheduler: Option<Arc<Mutex<MapReduceScheduler>>>,
}

impl WorkerManager {
    pub fn new(
        worker_interface: Option<Arc<RwLock<WorkerInterfaceImpl>>>,
        scheduler: Option<Arc<Mutex<MapReduceScheduler>>>,
    ) -> Self {
        WorkerManager {
            workers: Vec::new(),
            worker_interface: worker_interface,
            scheduler: scheduler,
        }
    }

    pub fn get_workers(&self) -> &Vec<Worker> {
        &self.workers
    }

    pub fn get_total_workers(&self) -> u32 {
        self.workers.len() as u32
    }

    pub fn get_available_workers(&mut self) -> Vec<&mut Worker> {
        let mut available_workers = Vec::new();
        for worker in &mut self.workers {
            if worker.is_available_for_scheduling() {
                available_workers.push(worker);
            }
        }

        available_workers
    }

    pub fn add_worker(&mut self, worker: Worker) {
        info!(
            "New Worker ({}) connected from {}",
            worker.get_worker_id(),
            worker.get_address(),
        );

        self.workers.push(worker);
    }

    pub fn get_worker(&mut self, worker_id: &str) -> Option<&mut Worker> {
        for worker in &mut self.workers {
            if worker.get_worker_id() == worker_id {
                return Some(worker);
            }
        }
        None
    }

    pub fn remove_worker(&mut self, worker_id: &str) {
        self.workers
            .iter()
            .position(|w| w.get_worker_id() == worker_id)
            .map(|index| self.workers.remove(index));
        info!("Removed {} from list of active workers.", worker_id);
    }

    pub fn recreate_worker_from_state(&mut self, data: serde_json::Value) -> Result<()> {
        let mut worker = Worker::new_from_json(data).chain_err(
            || "Unable to recreate worker",
        )?;

        // Add client for worker to worker interface.
        match self.worker_interface {
            Some(ref worker_interface) => {
                match worker_interface.write() {
                    Err(_) => return Err("WORKER_INTERFACE_UNAVAILABLE".into()),
                    Ok(mut interface) => {
                        interface.add_client(&worker).chain_err(
                            || "Unable to add client for worker",
                        )?;
                    }
                }
            }
            None => return Err("WorkerManager has no valid WorkerInterface Arc".into()),
        };

        if worker.get_operation_status() == pb::OperationStatus::FAILED {
            info!("Requeueing task: {}", worker.get_current_task_id());

            let scheduler = match self.scheduler {
                Some(ref scheduler) => scheduler,
                None => return Err("WorkerManager has no valid MapReduceScheduler Arc".into()),
            };

            scheduler
                .lock()
                .unwrap()
                .unschedule_task(worker.get_current_task_id())
                .chain_err(|| "Unable to move task back to queue")?;
            worker.set_current_task_id(String::new());
        }

        // Add worker to worker manager.
        self.add_worker(worker);

        Ok(())
    }
}

impl StateHandling for WorkerManager {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create WorkerManager from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut worker_list_json: Vec<serde_json::Value> = Vec::new();
        for worker in &self.workers {
            worker_list_json.push(worker.dump_state().chain_err(
                || "Unable to dump Worker state",
            )?);
        }
        Ok(json!({
            "total_workers": self.get_total_workers(),
            "workers": worker_list_json,
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        let worker_count: usize = serde_json::from_value(data["total_workers"].clone())
            .chain_err(|| "Unable to convert worker_count")?;

        for i in 0..worker_count {
            let worker_data = data["workers"][i].clone();

            // Re-establish connection with the worker.
            let worker_creation_result = self.recreate_worker_from_state(worker_data);
            if let Err(err) = worker_creation_result {
                warn!("Unable to re-establish connection with a worker: {}", err);
            }

        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_worker() {
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let mut worker_manager = WorkerManager::new(None, None);
        worker_manager.add_worker(worker.clone());

        assert_eq!(worker_manager.get_workers().len(), 1);
        assert_eq!(worker_manager.get_workers()[0], worker);
    }
}
