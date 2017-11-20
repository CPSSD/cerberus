use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use common::Job;

// JobQueue contains a custom queue which can hold both new jobs and old.
pub struct JobQueue {
    pub remaining: Arc<Mutex<VecDeque<Job>>>,
    pub completed: Arc<Mutex<Vec<Job>>>,

    pub active: Arc<Mutex<Option<Job>>>,
}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            remaining: Arc::new(Mutex::new(VecDeque::new())),
            completed: Arc::new(Mutex::new(Vec::new())),
            active: Arc::new(Mutex::new(None)),
        }
    }
}
