mod worker_manager;

// TODO(conor) Remove this when the refactor is completed.
pub use self::worker_manager::WorkerManager;

// TODO(conor) Rename this when the refactor is completed.
pub use self::new_worker_manager::WorkerManager as NewWorkerManager;

mod new_worker_manager;
mod state;
