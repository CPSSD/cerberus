use std::sync::Arc;

use futures::future;
use futures::prelude::*;

use common::Task;
use errors::*;
use worker_management::WorkerManager;

/// The `TaskManager` is responsible for communicating with the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct TaskManager {
    worker_manager: Arc<WorkerManager>,
}

impl TaskManager {
    pub fn new(manager: Arc<WorkerManager>) -> Self {
        TaskManager { worker_manager: manager }
    }

    pub fn run_tasks<'a>(
        &self,
        tasks: Vec<Task>,
    ) -> impl Future<Item = Vec<Task>, Error = Error> + 'a {
        let task_futures: Vec<Box<Future<Item = Task, Error = Error> + Send>> = tasks
            .into_iter()
            .map(|task| self.worker_manager.run_task(task))
            .collect();
        future::join_all(task_futures)
    }
}
