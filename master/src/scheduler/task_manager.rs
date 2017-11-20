use std::sync::Arc;

use futures_cpupool;

use super::state::State;

/// The `TaskManager` is responsible for communicating with the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct TaskManager {
    cpu_pool: futures_cpupool::CpuPool,
    state: Arc<State>,
}

impl TaskManager {
    pub fn new(cpu_pool: futures_cpupool::CpuPool, state: Arc<State>) -> Self {
        TaskManager {
            cpu_pool: cpu_pool,
            state: state,
        }
    }
}
