use std::sync::{Arc, Condvar, Mutex};

use futures::future;
use futures::prelude::*;
use futures::sync::oneshot;
use futures_cpupool;

use common::Job;
use errors::*;
use scheduler::job_processing;
use scheduler::state::{ScheduledJob, State};
use scheduler::task_manager::TaskManager;
use scheduler::worker_manager_adapter::WorkerManager;
use state::StateStore;

/// Serves as a lock to ensure that only one job is running at a time.
/// A value of `true` means that a job is currently running.
type JobLock = (Mutex<bool>, Condvar);

/// The `Scheduler` is responsible for the managing of `Job`s and `Task`s.
///
/// It delegates work to several child modules, and to the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct Scheduler {
    cpu_pool: futures_cpupool::CpuPool,
    state: Arc<State>,
    shared_state: Arc<StateStore>,

    job_lock: Arc<JobLock>,

    task_manager: Arc<TaskManager>,
}

impl Scheduler {
    /// Construct a new `Scheduler`, using the default [`CpuPool`](futures_cpupool::CpuPool).
    pub fn new(shared_state: Arc<StateStore>, worker_manager: Arc<WorkerManager>) -> Self {
        // The default number of threads in a CpuPool is the same as the number of CPUs on the
        // host.
        let mut builder = futures_cpupool::Builder::new();
        Scheduler::from_cpu_pool_builder(&mut builder, shared_state, worker_manager)
    }

    /// Construct a new `Scheduler` using a provided [`CpuPool builder`](futures_cpupool::Builder).
    // In this case, this warning is a false positive.
    #[allow(mutex_atomic)]
    pub fn from_cpu_pool_builder(
        builder: &mut futures_cpupool::Builder,
        shared_state: Arc<StateStore>,
        worker_manager: Arc<WorkerManager>,
    ) -> Self {
        let pool = builder.create();
        let state = Arc::new(State::new());
        Scheduler {
            // Cloning a CpuPool simply clones the reference to the underlying pool.
            cpu_pool: pool.clone(),
            state: state,
            shared_state: shared_state,

            job_lock: Arc::new((Mutex::new(false), Condvar::new())),

            task_manager: Arc::new(TaskManager::new(worker_manager)),
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
        // We clone job_lock twice because it needs to be moved into two different closures.
        let job_lock_acquire = Arc::clone(&self.job_lock);
        let job_lock_release = Arc::clone(&self.job_lock);
        let map_task_manager = Arc::clone(&self.task_manager);
        let reduce_task_manager = Arc::clone(&self.task_manager);

        let job_future = future::lazy(move || {
            Scheduler::acquire_job_lock(&job_lock_acquire);
            future::ok(())
        }).and_then(|_| {
            future::ok(job_processing::activate_job(job))
        }).and_then(|job| {
            future::result(job_processing::create_map_tasks(&job)).join(future::ok(job))
        }).and_then(move |(map_tasks, job)| {
            map_task_manager.run_tasks(map_tasks).join(future::ok(job))
        }).and_then(|(completed_map_tasks, job)| {
            future::result(job_processing::create_reduce_tasks(&job, completed_map_tasks))
                .join(future::ok(job))
        }).and_then(move |(reduce_tasks, job)| {
            reduce_task_manager.run_tasks(reduce_tasks).and_then(|_| future::ok(job))
        }).and_then(|job| {
            future::ok(job_processing::complete_job(job))
        }).select(failure)
        // We only care about whichever future resolves first, so map the ok pair and the error
        // pair to single values.
            .map(|(win, _)| win)
            .map_err(|(win, _)| win)
        // No matter the result of the main future, we have to release the job lock.
        .then(move |result| {
            Scheduler::release_job_lock(&job_lock_release);
            result
        });

        let job_cpu_future = self.cpu_pool.spawn(job_future);
        let scheduled_job = ScheduledJob {
            cancellation_channel: send,
            job_future: Box::new(job_cpu_future),
            job_id: job_id.clone(),
        };

        self.state.add_job(scheduled_job).and(Ok(job_id))
    }

    /// We could use a spinlock for this, but then threads are in a "busy wait" state, which
    /// needlessly consumes CPU time. So instead we use a Mutex and a Condvar. While the value
    /// inside the mutex is true (i.e., there is a running job), the thread sleeps until there is
    /// no running job.
    fn acquire_job_lock(job_lock: &JobLock) {
        let &(ref lock, ref cvar) = &*job_lock;
        let mut is_job_running = lock.lock().unwrap();
        while *is_job_running {
            is_job_running = cvar.wait(is_job_running).unwrap();
        }
        *is_job_running = true;
    }

    /// This function acquires the mutex, sets its value to false (i.e., there is no running
    /// job), and tells the condvar to notify all waiting threads that they can attempt to acquire
    /// the lock again.
    fn release_job_lock(job_lock: &JobLock) {
        let &(ref lock, ref cvar) = &*job_lock;
        let mut is_job_running = lock.lock().unwrap();
        *is_job_running = false;
        cvar.notify_all();
    }
}
