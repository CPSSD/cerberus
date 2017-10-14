use errors::*;
use mapreduce_job::MapReduceJob;
use mapreduce_tasks::{MapReduceTask, TaskProcessorTrait};
use queued_work_store::QueuedWorkStore;

pub struct MapReduceScheduler {
    map_reduce_job_queue: QueuedWorkStore<MapReduceJob>,
    map_reduce_task_queue: QueuedWorkStore<MapReduceTask>,

    map_reduce_in_progress: bool,
    in_progress_map_reduce_id: Option<String>,

    available_workers: u32,
    task_processor: Box<TaskProcessorTrait>,
}

impl MapReduceScheduler {
    pub fn new(task_processor: Box<TaskProcessorTrait>) -> Self {
        let job_queue: QueuedWorkStore<MapReduceJob> = QueuedWorkStore::new();
        let task_queue: QueuedWorkStore<MapReduceTask> = QueuedWorkStore::new();
        MapReduceScheduler {
            map_reduce_job_queue: job_queue,
            map_reduce_task_queue: task_queue,

            map_reduce_in_progress: false,
            in_progress_map_reduce_id: None,

            available_workers: 0,
            task_processor: task_processor,
        }
    }

    fn process_next_map_reduce(&mut self) -> Result<()> {
        match self.map_reduce_job_queue.pop_queue_top() {
            Some(map_reduce_job) => {
                self.map_reduce_in_progress = true;
                self.in_progress_map_reduce_id =
                    Some(map_reduce_job.get_map_reduce_id().to_owned());
                let map_tasks: Vec<MapReduceTask> =
                    self.task_processor.create_map_tasks(map_reduce_job)?;
                for task in map_tasks {
                    self.map_reduce_task_queue
                        .add_to_store(Box::new(task))
                        .chain_err(|| "Error adding map reduce task to queue")?;
                }
                Ok(())
            }
            None => Err("no queued map reduce".into()),
        }
    }

    pub fn get_map_reduce_in_progress(&self) -> bool {
        self.map_reduce_in_progress
    }

    pub fn get_in_progress_map_reduce_id(&self) -> Option<String> {
        self.in_progress_map_reduce_id.clone()
    }

    pub fn schedule_map_reduce(&mut self, map_reduce_job: MapReduceJob) -> Result<()> {
        self.map_reduce_job_queue
            .add_to_store(Box::new(map_reduce_job))
            .chain_err(|| "Error adding map reduce job to queue.")?;
        if !self.map_reduce_in_progress {
            self.process_next_map_reduce().chain_err(
                || "Error processing next map reduce.",
            )?;
        }
        Ok(())
    }

    pub fn get_available_workers(&self) -> u32 {
        self.available_workers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mapreduce_tasks::TaskType;

    struct TaskProcessorStub {
        map_reduce_tasks: Vec<MapReduceTask>,
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
            Ok(self.map_reduce_tasks.clone())
        }
    }


    fn create_map_reduce_scheduler() -> MapReduceScheduler {
        MapReduceScheduler::new(Box::new(TaskProcessorStub {
            map_reduce_tasks: vec![
                MapReduceTask::new(
                    TaskType::Map,
                    "map-reduce1".to_owned(),
                    "/tmp/bin".to_owned(),
                    vec!["input-1".to_owned()]
                ),
            ],
        }))
    }

    #[test]
    fn test_schedule_map_reduce() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        let map_reduce_job = MapReduceJob::new(
            "client-1".to_string(),
            "/tmp/bin".to_owned(),
            "/tmp/input".to_owned(),
        );
        map_reduce_scheduler
            .schedule_map_reduce(map_reduce_job.clone())
            .unwrap();
        assert_eq!(
            map_reduce_scheduler
                .get_in_progress_map_reduce_id()
                .unwrap(),
            map_reduce_job.get_map_reduce_id()
        );
    }

    #[test]
    fn test_get_map_reduce_in_progress() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        // Assert that map reduce in progress starts as false.
        assert!(!map_reduce_scheduler.get_map_reduce_in_progress());
        map_reduce_scheduler
            .schedule_map_reduce(MapReduceJob::new(
                "client-1".to_string(),
                "/tmp/bin".to_owned(),
                "/tmp/input".to_owned(),
            ))
            .unwrap();
        assert!(map_reduce_scheduler.get_map_reduce_in_progress());
    }
}
