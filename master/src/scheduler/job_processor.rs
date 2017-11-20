use std::sync::Arc;

use futures_cpupool;

use super::state::State;

/// The `JobProcessor` is responsible for processing a [`Job`](common::Job) into a set of
/// [`Task`s](common::Task).
pub struct JobProcessor {
    cpu_pool: futures_cpupool::CpuPool,
    state: Arc<State>,
}

impl JobProcessor {
    pub fn new(cpu_pool: futures_cpupool::CpuPool, state: Arc<State>) -> Self {
        JobProcessor {
            cpu_pool: cpu_pool,
            state: state,
        }
    }
}
