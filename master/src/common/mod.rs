/// Common module contains the common shared data containers which are used throughout the
/// application.
/// Job: This is MapReduce job the user has specified. It needs the user provided code used for
///      the map Reduce framework, as well as in input and output locations.
/// Task: When a Job is scheduled, it gets split into individual Tasks which are scheduled to be
///       ran on inididual workers.
/// Worker: These are the actual machines at which the individial tasks are being ran.
pub mod job;
pub mod task;
pub mod worker;

// TODO: Remove the unnecessary `MapReduce` prefixes.
pub use self::job::MapReduceJob;
pub use self::job::MapReduceJobOptions;
pub use self::task::MapReduceTask;
pub use self::task::MapReduceTaskStatus;
pub use self::task::TaskType;
pub use self::worker::Worker;
