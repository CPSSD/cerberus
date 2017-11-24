use std::collections::HashMap;
use std::sync::Mutex;

use futures::Future;
use futures::sync::oneshot::Sender;

use common::Job;
use errors::*;

/// A `ScheduledJob` holds the [`CpuFuture`](futures_cpupool::CpuFuture) corresponding to a running
/// job, along with a [`oneshot::Sender`](futures::sync::oneshot::Sender) used to cancel a running
/// job future.
pub struct ScheduledJob {
    pub cancellation_channel: Sender<()>,
    pub job_future: Box<Future<Item = Job, Error = Error>>,
    pub job_id: String,
}

impl ScheduledJob {
    /// Sends a cancellation signal to the underlying future.
    ///
    /// This consumes `self` and returns the Boxed future.
    ///
    /// # Panics
    ///
    /// Panics if the `Receiver` end of the [`oneshot`](futures::sync::oneshot) has already been
    /// dropped.
    pub fn cancel(self) -> Box<Future<Item = Job, Error = Error>> {
        self.cancellation_channel.send(()).expect(
            "ScheduledJob cancellation channel: Receiver was dropped before sending.",
        );
        self.job_future
    }
}

/// The `State` object holds all of the `ScheduledJob`s inside a `HashMap`, with the job IDs as
/// keys.
#[derive(Default)]
pub struct State {
    scheduled_jobs: Mutex<HashMap<String, ScheduledJob>>,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_job(&self, job: ScheduledJob) -> Result<()> {
        let mut map = self.scheduled_jobs.lock().unwrap();
        if map.contains_key(&job.job_id) {
            return Err(
                format!("Job with ID {} is already running.", &job.job_id).into(),
            );
        }
        map.insert(job.job_id.clone(), job);
        Ok(())
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<Box<Future<Item = Job, Error = Error>>> {
        let mut map = self.scheduled_jobs.lock().unwrap();
        let job_future = map.remove(job_id).ok_or_else(|| {
            format!("Job with ID {} does not exist.", job_id)
        })?;
        Ok(job_future.cancel())
    }
}
