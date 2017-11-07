use errors::*;
use mapreduce_job::MapReduceJob;
use mapreduce_tasks::{MapReduceTask, MapReduceTaskStatus, TaskProcessorTrait};
use queued_work_store::{QueuedWork, QueuedWorkStore};

use cerberus_proto::mapreduce::Status as MapReduceJobStatus;
use cerberus_proto::worker as pb;

pub struct MapReduceScheduler {
    map_reduce_job_queue: QueuedWorkStore<MapReduceJob>,
    map_reduce_task_queue: QueuedWorkStore<MapReduceTask>,

    map_reduce_in_progress: bool,
    in_progress_map_reduce_id: Option<String>,

    available_workers: u32,
    task_processor: Box<TaskProcessorTrait + Send>,
}

impl MapReduceScheduler {
    pub fn new(task_processor: Box<TaskProcessorTrait + Send>) -> Self {
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
                map_reduce_job.status = MapReduceJobStatus::IN_PROGRESS;
                self.map_reduce_in_progress = true;
                self.in_progress_map_reduce_id = Some(map_reduce_job.map_reduce_id.clone());

                let map_tasks: Vec<MapReduceTask> =
                    self.task_processor.create_map_tasks(map_reduce_job)?;
                map_reduce_job.map_tasks_total = map_tasks.len() as u32;

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

    //TODO(conor): Remove this when get_map_reduce_in_progress is used.
    #[allow(dead_code)]
    pub fn get_map_reduce_in_progress(&self) -> bool {
        self.map_reduce_in_progress
    }

    //TODO(conor): Remove this when get_in_progress_map_reduce_id is used.
    #[allow(dead_code)]
    pub fn get_in_progress_map_reduce_id(&self) -> Option<String> {
        self.in_progress_map_reduce_id.clone()
    }

    pub fn get_map_reduce_job_queue_size(&self) -> usize {
        if self.map_reduce_in_progress {
            return self.map_reduce_job_queue.queue_size() + (1 as usize);
        }
        self.map_reduce_job_queue.queue_size()
    }

    pub fn get_map_reduce_task_queue_size(&self) -> usize {
        self.map_reduce_task_queue.queue_size()
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

    pub fn set_available_workers(&mut self, available_workers: u32) {
        self.available_workers = available_workers;
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: String) -> Result<&MapReduceJob> {
        let result = self.map_reduce_job_queue.get_work_by_id(&mapreduce_id);
        match result {
            None => Err("There was an error getting the result".into()),
            Some(job) => Ok(job),
        }
    }

    pub fn get_mapreduce_client_status(&self, client_id: String) -> Result<Vec<&MapReduceJob>> {
        self.map_reduce_job_queue.get_work_bucket_items(&client_id)
    }

    pub fn pop_queued_map_reduce_task(&mut self) -> Option<&mut MapReduceTask> {
        self.map_reduce_task_queue.pop_queue_top()
    }

    pub fn unschedule_task(&mut self, task_id: String) -> Result<()> {
        self.map_reduce_task_queue
            .move_task_to_queue(task_id.to_owned())
            .chain_err(|| "Error unscheduling map reduce task")?;

        let task = self.map_reduce_task_queue
            .get_work_by_id_mut(&task_id)
            .chain_err(|| "Error unschuling map reduce task")?;

        task.set_assigned_worker_id(String::new());
        task.set_status(MapReduceTaskStatus::Queued);

        Ok(())
    }

    fn create_reduce_tasks(&mut self, map_reduce_id: String) -> Result<()> {
        let map_reduce_job = self.map_reduce_job_queue
            .get_work_by_id_mut(&map_reduce_id)
            .chain_err(|| "Error creating reduce tasks.")?;

        let reduce_tasks = {
            let map_tasks = self.map_reduce_task_queue
                .get_work_bucket_items(&map_reduce_id)
                .chain_err(|| "Error creating reduce tasks.")?;

            self.task_processor
                .create_reduce_tasks(map_reduce_job, map_tasks.as_slice())
                .chain_err(|| "Error creating reduce tasks.")?
        };

        map_reduce_job.reduce_tasks_total = reduce_tasks.len() as u32;

        for reduce_task in reduce_tasks {
            self.map_reduce_task_queue
                .add_to_store(Box::new(reduce_task))
                .chain_err(|| "Error adding reduce task to store.")?;
        }

        Ok(())
    }

    fn increment_map_tasks_completed(&mut self, map_reduce_id: String) -> Result<()> {
        let all_maps_completed = {
            let map_reduce_job = self.map_reduce_job_queue
                .get_work_by_id_mut(&map_reduce_id)
                .chain_err(|| "Error incrementing completed map tasks.")?;

            map_reduce_job.map_tasks_completed += 1;
            map_reduce_job.map_tasks_completed == map_reduce_job.map_tasks_total
        };

        if all_maps_completed {
            // Create Reduce tasks.
            self.create_reduce_tasks(map_reduce_id).chain_err(
                || "Error incrementing completed map tasks.",
            )?;
        }
        Ok(())
    }

    pub fn process_map_task_response(
        &mut self,
        map_task_id: String,
        map_response: pb::MapResponse,
    ) -> Result<()> {
        if map_response.get_status() == pb::OperationStatus::SUCCESS {
            let map_reduce_id = {
                let map_task = self.map_reduce_task_queue
                    .get_work_by_id_mut(&map_task_id)
                    .chain_err(|| "Error marking map task as completed.")?;

                map_task.set_status(MapReduceTaskStatus::Complete);
                for map_result in map_response.get_map_results() {
                    map_task.push_output_file(
                        map_result.get_key(),
                        map_result.get_output_file_path(),
                    );
                }
                map_task.get_map_reduce_id().to_owned()
            };
            self.increment_map_tasks_completed(map_reduce_id)
                .chain_err(|| "Error marking map task as completed.")?;
        } else {
            self.unschedule_task(map_task_id).chain_err(
                || "Error marking map task as complete.",
            )?;
        }
        Ok(())
    }

    fn increment_reduce_tasks_completed(&mut self, map_reduce_id: String) -> Result<()> {
        let completed_map_reduce: bool = {
            let map_reduce_job = self.map_reduce_job_queue
                .get_work_by_id_mut(&map_reduce_id)
                .chain_err(|| "Mapreduce job not found in queue.")?;

            map_reduce_job.reduce_tasks_completed += 1;

            if map_reduce_job.reduce_tasks_completed == map_reduce_job.reduce_tasks_total {
                self.map_reduce_task_queue
                    .remove_work_bucket(&map_reduce_job.get_work_id())
                    .chain_err(|| "Error marking map reduce job as complete.")?;
                map_reduce_job.status = MapReduceJobStatus::DONE;
            }
            map_reduce_job.reduce_tasks_completed == map_reduce_job.reduce_tasks_total
        };

        if completed_map_reduce && self.map_reduce_job_queue.queue_size() > 0 {
            self.process_next_map_reduce().chain_err(
                || "Error incrementing completed reduce tasks.",
            )?;
        }
        Ok(())
    }

    pub fn process_reduce_task_response(
        &mut self,
        reduce_task_id: String,
        reduce_response: pb::ReduceResponse,
    ) -> Result<()> {
        if reduce_response.get_status() == pb::OperationStatus::SUCCESS {
            let map_reduce_id = {
                let reduce_task = self.map_reduce_task_queue
                    .get_work_by_id_mut(&reduce_task_id)
                    .chain_err(|| "Error marking reduce task as completed.")?;

                let perform_reduce_request = reduce_task.get_perform_reduce_request().chain_err(
                    || "Error marking reduce task as complete.",
                )?;
                let reduce_key = perform_reduce_request.get_intermediate_key();

                reduce_task.set_status(MapReduceTaskStatus::Complete);
                reduce_task.push_output_file(reduce_key, reduce_response.get_output_file_path());
                reduce_task.get_map_reduce_id().to_owned()
            };

            self.increment_reduce_tasks_completed(map_reduce_id)
                .chain_err(|| "Error marking reduce task as completed.")?;
        } else {
            self.unschedule_task(reduce_task_id).chain_err(
                || "Error marking reduce task as complete.",
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queued_work_store::QueuedWork;
    use mapreduce_job::MapReduceJobOptions;

    fn get_test_job_options() -> MapReduceJobOptions {
        MapReduceJobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input".to_owned(),
            ..Default::default()
        }
    }

    struct TaskProcessorStub {
        map_tasks: Vec<MapReduceTask>,
        reduce_tasks: Vec<MapReduceTask>,
    }

    impl TaskProcessorStub {
        fn new(map_tasks: Vec<MapReduceTask>, reduce_tasks: Vec<MapReduceTask>) -> Self {
            TaskProcessorStub {
                map_tasks: map_tasks,
                reduce_tasks: reduce_tasks,
            }
        }
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
            Ok(self.map_tasks.clone())
        }
        fn create_reduce_tasks(
            &self,
            _map_reduce_job: &MapReduceJob,
            _completed_map_tasks: &[&MapReduceTask],
        ) -> Result<Vec<MapReduceTask>> {
            Ok(self.reduce_tasks.clone())
        }
    }


    fn create_map_reduce_scheduler() -> MapReduceScheduler {
        MapReduceScheduler::new(Box::new(TaskProcessorStub::new(
            vec![
                MapReduceTask::new_map_task(
                    "map-reduce1",
                    "/tmp/bin",
                    "input-1"
                ),
            ],
            Vec::new(),
        )))
    }

    #[test]
    fn test_schedule_map_reduce() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        map_reduce_scheduler
            .schedule_map_reduce(map_reduce_job.clone())
            .unwrap();
        assert_eq!(
            map_reduce_scheduler
                .get_in_progress_map_reduce_id()
                .unwrap(),
            map_reduce_job.map_reduce_id
        );
    }

    #[test]
    fn test_get_map_reduce_in_progress() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        // Assert that map reduce in progress starts as false.
        assert!(!map_reduce_scheduler.get_map_reduce_in_progress());
        map_reduce_scheduler
            .schedule_map_reduce(MapReduceJob::new(get_test_job_options()).unwrap())
            .unwrap();
        assert!(map_reduce_scheduler.get_map_reduce_in_progress());
    }

    #[test]
    fn test_process_completed_task() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        let map_task1 = MapReduceTask::new_map_task(
            map_reduce_job.map_reduce_id.as_str(),
            "/tmp/bin",
            "input-1",
        );

        let mut map_response = pb::MapResponse::new();
        map_response.set_status(pb::OperationStatus::SUCCESS);

        let mut map_result1 = pb::MapResponse_MapResult::new();
        map_result1.set_key("key-1".to_owned());
        map_result1.set_output_file_path("/tmp/worker/intermediate1".to_owned());

        let mut map_result2 = pb::MapResponse_MapResult::new();
        map_result2.set_key("key-2".to_owned());
        map_result2.set_output_file_path("/tmp/worker/intermediate2".to_owned());

        map_response.mut_map_results().push(map_result1);
        map_response.mut_map_results().push(map_result2);

        let reduce_task1 = MapReduceTask::new_reduce_task(
            map_reduce_job.map_reduce_id.as_str(),
            "/tmp/bin",
            "key-1",
            vec!["/tmp/worker/intermediate1".to_owned()],
            "/tmp/output",
        );

        let mut reduce_response1 = pb::ReduceResponse::new();
        reduce_response1.set_status(pb::OperationStatus::SUCCESS);
        reduce_response1.set_output_file_path("/tmp/worker/key-2".to_owned());

        let reduce_task2 = MapReduceTask::new_reduce_task(
            map_reduce_job.map_reduce_id.as_str(),
            "/tmp/bin",
            "key-2",
            vec!["/tmp/worker/intermediate2".to_owned()],
            "/tmp/output/",
        );

        let mut reduce_response2 = pb::ReduceResponse::new();
        reduce_response2.set_status(pb::OperationStatus::SUCCESS);
        reduce_response2.set_output_file_path("/tmp/worker/key-2".to_owned());

        let mock_map_tasks = vec![map_task1.clone()];
        let mock_reduce_tasks = vec![reduce_task1.clone(), reduce_task2.clone()];

        let mut map_reduce_scheduler = MapReduceScheduler::new(Box::new(
            TaskProcessorStub::new(mock_map_tasks, mock_reduce_tasks),
        ));
        map_reduce_scheduler
            .schedule_map_reduce(map_reduce_job.clone())
            .unwrap();

        // Assert that the scheduler state starts as good.
        assert_eq!(
            map_reduce_job.map_reduce_id,
            map_reduce_scheduler
                .get_in_progress_map_reduce_id()
                .unwrap()
        );
        assert_eq!(1, map_reduce_scheduler.map_reduce_task_queue.queue_size());

        map_reduce_scheduler.pop_queued_map_reduce_task().unwrap();

        // Process response for map task
        map_reduce_scheduler
            .process_map_task_response(map_task1.get_task_id().to_owned(), map_response)
            .unwrap();

        {
            let map_reduce_job = map_reduce_scheduler
                .map_reduce_job_queue
                .get_work_by_id(&map_reduce_job.get_work_id())
                .unwrap();

            let map_task1 = map_reduce_scheduler
                .map_reduce_task_queue
                .get_work_by_id(&map_task1.get_work_id())
                .unwrap();

            assert_eq!(1, map_reduce_job.map_tasks_completed);
            assert_eq!(MapReduceTaskStatus::Complete, map_task1.get_status());
            assert_eq!(
                vec![
                    ("key-1".to_owned(), "/tmp/worker/intermediate1".to_owned()),
                    ("key-2".to_owned(), "/tmp/worker/intermediate2".to_owned()),
                ],
                map_task1.get_output_files()
            );
            assert_eq!(2, map_reduce_scheduler.map_reduce_task_queue.queue_size());
        }

        map_reduce_scheduler.pop_queued_map_reduce_task().unwrap();
        map_reduce_scheduler.pop_queued_map_reduce_task().unwrap();

        // Process response for reduce task 1.
        map_reduce_scheduler
            .process_reduce_task_response(reduce_task1.get_task_id().to_owned(), reduce_response1)
            .unwrap();

        {
            let map_reduce_job = map_reduce_scheduler
                .map_reduce_job_queue
                .get_work_by_id(&map_reduce_job.get_work_id())
                .unwrap();

            let reduce_task1 = map_reduce_scheduler
                .map_reduce_task_queue
                .get_work_by_id(&reduce_task1.get_work_id())
                .unwrap();

            assert_eq!(1, map_reduce_job.reduce_tasks_completed);
            assert_eq!(MapReduceTaskStatus::Complete, reduce_task1.get_status());

        }

        map_reduce_scheduler
            .process_reduce_task_response(reduce_task2.get_task_id().to_owned(), reduce_response2)
            .unwrap();

        let map_reduce_job = map_reduce_scheduler
            .map_reduce_job_queue
            .get_work_by_id(&map_reduce_job.get_work_id())
            .unwrap();

        assert!(
            map_reduce_scheduler
                .map_reduce_task_queue
                .get_work_by_id(&reduce_task2.get_work_id())
                .is_none()
        );
        assert_eq!(2, map_reduce_job.reduce_tasks_completed);
        assert_eq!(MapReduceJobStatus::DONE, map_reduce_job.status);
    }
}
