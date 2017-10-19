use uuid::Uuid;
use queued_work_store::QueuedWork;
use cerberus_proto::mapreduce::MapReduceStatusResponse_MapReduceReport_Status as MapReduceJobStatus;

/// The `MapReduceJob` is a struct that represents a `MapReduce` job submitted by a client.
#[derive(Clone)]
pub struct MapReduceJob {
    client_id: String,
    map_reduce_id: String,
    binary_path: String,
    input_directory: String,

    status: MapReduceJobStatus,

    map_tasks_completed: u32,
    map_tasks_total: u32,

    reduce_tasks_completed: u32,
    reduce_tasks_total: u32,
}

impl MapReduceJob {
    pub fn new<S: Into<String>>(client_id: S, binary_path: S, input_directory: S) -> Self {
        let map_reduce_id = Uuid::new_v4();
        MapReduceJob {
            client_id: client_id.into(),
            map_reduce_id: map_reduce_id.to_string(),
            binary_path: binary_path.into(),
            input_directory: input_directory.into(),

            status: MapReduceJobStatus::IN_QUEUE,

            map_tasks_completed: 0,
            map_tasks_total: 0,

            reduce_tasks_completed: 0,
            reduce_tasks_total: 0,
        }
    }

    pub fn get_client_id(&self) -> &str {
        &self.client_id
    }

    pub fn get_map_reduce_id(&self) -> &str {
        &self.map_reduce_id
    }

    pub fn get_binary_path(&self) -> &str {
        &self.binary_path
    }

    pub fn get_input_directory(&self) -> &str {
        &self.input_directory
    }

    pub fn get_status(&self) -> MapReduceJobStatus {
        self.status
    }

    pub fn set_status(&mut self, new_status: MapReduceJobStatus) {
        self.status = new_status;
    }

    pub fn get_map_tasks_completed(&self) -> u32 {
        self.map_tasks_completed
    }

    pub fn set_map_tasks_completed(&mut self, map_tasks_completed: u32) {
        self.map_tasks_completed = map_tasks_completed;
    }

    pub fn get_map_tasks_total(&self) -> u32 {
        self.map_tasks_total
    }

    pub fn set_map_tasks_total(&mut self, map_tasks_total: u32) {
        self.map_tasks_total = map_tasks_total;
    }

    pub fn get_reduce_tasks_completed(&self) -> u32 {
        self.reduce_tasks_completed
    }

    pub fn set_reduce_tasks_completed(&mut self, reduce_tasks_completed: u32) {
        self.reduce_tasks_completed = reduce_tasks_completed;
    }

    pub fn get_reduce_tasks_total(&self) -> u32 {
        self.reduce_tasks_total
    }

    pub fn set_reduce_tasks_total(&mut self, reduce_tasks_total: u32) {
        self.reduce_tasks_total = reduce_tasks_total;
    }
}

impl QueuedWork for MapReduceJob {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.get_client_id().to_owned()
    }

    fn get_work_id(&self) -> String {
        self.get_map_reduce_id().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_client_id() {
        let map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        assert_eq!(map_reduce_job.get_client_id(), "client-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        assert_eq!(map_reduce_job.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_get_input_directory() {
        let map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        assert_eq!(map_reduce_job.get_input_directory(), "/tmp/input/");
    }

    #[test]
    fn test_set_status() {
        let mut map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        // Assert that the default status for a map reduce job is Queued.
        assert_eq!(MapReduceJobStatus::IN_QUEUE, map_reduce_job.get_status());

        // Set the status to Completed and assert success.
        map_reduce_job.set_status(MapReduceJobStatus::DONE);
        assert_eq!(MapReduceJobStatus::DONE, map_reduce_job.get_status());
    }

    #[test]
    fn test_tasks_completed() {
        let mut map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        // Assert that completed tasks starts at 0.
        assert_eq!(0, map_reduce_job.get_map_tasks_completed());
        assert_eq!(0, map_reduce_job.get_reduce_tasks_completed());

        map_reduce_job.set_map_tasks_completed(1337);
        map_reduce_job.set_reduce_tasks_completed(7331);

        assert_eq!(1337, map_reduce_job.get_map_tasks_completed());
        assert_eq!(7331, map_reduce_job.get_reduce_tasks_completed());
    }

    #[test]
    fn test_tasks_total() {
        let mut map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");
        // Assert that total tasks starts at 0.
        assert_eq!(0, map_reduce_job.get_map_tasks_total());
        assert_eq!(0, map_reduce_job.get_reduce_tasks_total());

        map_reduce_job.set_map_tasks_total(1337);
        map_reduce_job.set_reduce_tasks_total(7331);

        assert_eq!(1337, map_reduce_job.get_map_tasks_total());
        assert_eq!(7331, map_reduce_job.get_reduce_tasks_total());
    }

    #[test]
    fn test_queued_work_impl() {
        let map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input/");

        assert_eq!(map_reduce_job.get_work_bucket(), "client-1");
        assert_eq!(
            map_reduce_job.get_work_id(),
            map_reduce_job.get_map_reduce_id()
        );
    }
}
