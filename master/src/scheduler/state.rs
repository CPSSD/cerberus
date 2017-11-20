use std::collections::HashMap;
use std::sync::RwLock;

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
}

impl ScheduledJob {
    pub fn new(cancel: Sender<()>, future: Box<Future<Item = Job, Error = Error>>) -> Self {
        ScheduledJob {
            cancellation_channel: cancel,
            job_future: future,
        }
    }

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
    pub scheduled_jobs: RwLock<HashMap<String, ScheduledJob>>,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }
}
