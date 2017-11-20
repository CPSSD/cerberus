use std::sync::{Arc, Mutex};

use common::Worker;
use state::job_queue::JobQueue;

// StateStore contains the state of the master. All other components of the master interact with
// this structure to read and write their individial state.
pub struct StateStore {
    // Jobs scheduled, running and done on the master.
    pub jobs: Arc<Mutex<JobQueue>>,
    // List of workers which are registered with the master. They contain both free and occupied
    // workers.
    pub workers: Arc<Mutex<Vec<Worker>>>,
}

impl StateStore {
    pub fn new() -> Self {
        StateStore {
            jobs: Arc::new(Mutex::new(JobQueue::new())),
            workers: Arc::new(Mutex::new(Vec::new())),
        }
    }
}
