use std::sync::Arc;

use futures::future;
use futures::prelude::*;
use futures::sync::oneshot;
use futures_cpupool;

use chrono::Utc;

use cerberus_proto::mapreduce::Status as JobStatus;
use common::Job;
use errors::*;
use scheduler::state::{ScheduledJob, State};
use scheduler::task_manager::TaskManager;
use scheduler::task_processor::TaskProcessor;
use worker_management::WorkerManager;

/// The `Scheduler` is responsible for the managing of `Job`s and `Task`s.
///
/// It delegates work to several child modules, and to the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct Scheduler {
    cpu_pool: futures_cpupool::CpuPool,
    state: Arc<State>,

    task_manager: Arc<TaskManager>,
    task_processor: Arc<TaskProcessor + Send + Sync>,
}

impl Scheduler {
    /// Construct a new `Scheduler`, using the default [`CpuPool`](futures_cpupool::CpuPool).
    pub fn new(
        worker_manager: Arc<WorkerManager>,
        task_processor: Arc<TaskProcessor + Send + Sync>,
    ) -> Self {
        // The default number of threads in a CpuPool is the same as the number of CPUs on the
        // host.
        let mut builder = futures_cpupool::Builder::new();
        Scheduler::from_cpu_pool_builder(&mut builder, worker_manager, task_processor)
    }

    /// Construct a new `Scheduler` using a provided [`CpuPool builder`](futures_cpupool::Builder).
    pub fn from_cpu_pool_builder(
        builder: &mut futures_cpupool::Builder,
        worker_manager: Arc<WorkerManager>,
        task_processor: Arc<TaskProcessor + Send + Sync>,
    ) -> Self {
        let pool = builder.create();
        let state = Arc::new(State::new());
        Scheduler {
            // Cloning a CpuPool simply clones the reference to the underlying pool.
            cpu_pool: pool.clone(),
            state: state,

            task_manager: Arc::new(TaskManager::new(worker_manager)),
            task_processor: task_processor,
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

        let map_task_manager = Arc::clone(&self.task_manager);
        let reduce_task_manager = Arc::clone(&self.task_manager);
        let map_task_processor = Arc::clone(&self.task_processor);
        let reduce_task_processor = Arc::clone(&self.task_processor);

        let job_future = future::lazy(move || {
            future::ok(activate_job(job))
        }).and_then(move |job| {
            future::result(map_task_processor.create_map_tasks(&job)).join(future::ok(job))
        }).and_then(move |(map_tasks, job)| {
            map_task_manager.run_tasks(map_tasks).join(future::ok(job))
        }).and_then(move |(completed_map_tasks, job)| {
            future::result(reduce_task_processor.create_reduce_tasks(&job, completed_map_tasks))
                .join(future::ok(job))
        }).and_then(move |(reduce_tasks, job)| {
            reduce_task_manager.run_tasks(reduce_tasks).and_then(|_| future::ok(job))
        }).and_then(|job| {
            future::ok(complete_job(job))
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

    // TODO(conor): Finish these functions
    pub fn get_job_queue_size(&self) -> usize {
        0
    }

    pub fn get_available_workers(&self) -> u32 {
        0
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: &str) -> Result<&Job> {
        Err("There was an error getting the result".into())
    }

    /// `get_mapreduce_client_status` returns a vector of `Job`s for a given client.
    pub fn get_mapreduce_client_status(&self, client_id: &str) -> Result<Vec<&Job>> {
        Ok(Vec::new())
    }
}

pub fn activate_job(mut job: Job) -> Job {
    info!("Activating job with ID {}.", job.id);
    job.status = JobStatus::IN_PROGRESS;
    job.time_started = Some(Utc::now());
    job
}

pub fn complete_job(mut job: Job) -> Job {
    info!("Job with ID {} completed.", job.id);
    job.status = JobStatus::DONE;
    job.time_completed = Some(Utc::now());
    job
}
