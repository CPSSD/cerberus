use std::sync::Arc;

use futures::future;
use futures::prelude::*;
use futures::sync::oneshot;
use futures_cpupool;

use common::{Job, JobOptions};
use errors::*;
use super::job_processing;
use super::state::{ScheduledJob, State};
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
    ///
    /// The mechanism for cancellation involves a [oneshot channel](futures::sync::mpsc::oneshot)
    /// which always produces an error when complete. We select on the job's future and the
    /// cancellation future, so that if the cancellation future fires we can drop the job future.
    /// When it is dropped, all currently-running child futures will complete but no new ones will
    /// be scheduled and the job will eventually halt.
    pub fn schedule_job(&self, job: Job) -> Result<String> {
        // Cancellation oneshot
        let (send, recv) = oneshot::channel::<()>();
        let failure = recv.then(|_| Err("Job cancelled.".into()));

        let job_id = job.id.clone();
        let task_manager = Arc::clone(&self.task_manager);

        let job_future = future::lazy(move || {
            future::result(job_processing::create_map_tasks(&job))
                .and_then(|_| {
                    // TODO(tbolt): This is just a placeholder so that the result type of the
                    // future matches what is expected. Will be removed soon.
                    future::ok(Job::new(JobOptions::default()).unwrap())
                })
        }).select(failure)
        // We only care about whichever future resolves first, so map the ok pair and the error
        // pair to single values.
            .map(|(win, _)| win)
            .map_err(|(win, _)| win);

        let job_cpu_future = self.cpu_pool.spawn(job_future);
        let scheduled_job = ScheduledJob {
            cancellation_channel: send,
            job_future: Box::new(job_cpu_future),
            job_id: job_id.clone(),
        };

        self.state.add_job(scheduled_job).and(Ok(job_id))
    }

    /*     pub fn unschedule_job(job_id: &str) -> impl Future<Item = Job, Error = Error> {
     *         unimplemented!();
     *     }
     *
     *     pub fn get_job(job_id: &str) -> impl Future<Item = Job, Error = Error> {
     *         unimplemented!();
     *     } */
}
