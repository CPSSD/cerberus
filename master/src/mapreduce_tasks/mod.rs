mod mapreduce_task;
mod task_processor;

pub use self::mapreduce_task::MapReduceTask;
pub use self::mapreduce_task::MapReduceTaskStatus;
pub use self::mapreduce_task::TaskType;

pub use self::task_processor::TaskProcessorTrait;
pub use self::task_processor::TaskProcessor;
