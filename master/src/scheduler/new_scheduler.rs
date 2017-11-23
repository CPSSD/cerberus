use std::sync::Arc;

use futures::future::{Future, ok};
use futures_cpupool;

use common::Job;
use errors::*;
use super::job_processing;
use super::state::State;
use super::task_manager::TaskManager;

/// The `Scheduler` is responsible for the managing of `Job`s and `Task`s.
///
/// It delegates work to several child modules, and to the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct Scheduler {
    cpu_pool: futures_cpupool::CpuPool,
    state: Arc<State>,

    task_manager: Arc<TaskManager>,
}

impl Scheduler {
    /// Construct a new `Scheduler`, using the default [`CpuPool`](futures_cpupool::CpuPool).
    pub fn new() -> Self {
        // The default number of threads in a CpuPool is the same as the number of CPUs on the
        // host.
        let mut builder = futures_cpupool::Builder::new();
        Scheduler::from_cpu_pool_builder(&mut builder)
    }

    /// Construct a new `Scheduler` using a provided [`CpuPool builder`](futures_cpupool::Builder).
    pub fn from_cpu_pool_builder(builder: &mut futures_cpupool::Builder) -> Self {
        let pool = builder.create();
        let state = Arc::new(State::new());
        Scheduler {
            // Cloning a CpuPool simply clones the reference to the underlying pool.
            cpu_pool: pool.clone(),
            state: Arc::clone(&state),

            task_manager: Arc::new(TaskManager::new(pool.clone(), Arc::clone(&state))),
        }
    }

    /// Schedule a [`Job`](common::Job) to be executed.
    ///
    /// Returns the ID of the `Job`.
    #[allow(needless_pass_by_value)]
    pub fn schedule_job(job: Job) -> String {
        unimplemented!();
    }

    /*     pub fn unschedule_job(job_id: &str) -> impl Future<Item = Job, Error = Error> {
     *         unimplemented!();
     *     }
     *
     *     pub fn get_job(job_id: &str) -> impl Future<Item = Job, Error = Error> {
     *         unimplemented!();
     *     } */
}
