use futures::prelude::*;

use common::Task;
use errors::*;

pub trait WorkerManager: Send + Sync {
    // Rust does not yet support using `impl Trait` in trait definitions, so we have to return a
    // Boxed trait object instead.
    fn run_task(&self, task: Task) -> Box<Future<Item = Task, Error = Error> + Send>;
}
