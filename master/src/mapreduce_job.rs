use uuid::Uuid;
use queued_work_store::QueuedWork;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapReduceJobStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
}

/// The `MapReduceJob` is a struct that represents a `MapReduce` job submitted by a client.
pub struct MapReduceJob {
    client_id: String,
    map_reduce_id: String,
    binary_path: String,
    input_directory: String,

    status: MapReduceJobStatus,
}

impl MapReduceJob {
    pub fn new(client_id: String, binary_path: String, input_directory: String) -> Self {
        let map_reduce_id = Uuid::new_v4();
        MapReduceJob {
            client_id: client_id,
            map_reduce_id: map_reduce_id.to_string(),
            binary_path: binary_path,
            input_directory: input_directory,

            status: MapReduceJobStatus::Queued,
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
        self.status.clone()
    }

    pub fn set_status(&mut self, new_status: MapReduceJobStatus) {
        self.status = new_status;
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
        let map_reduce_job = MapReduceJob::new(
            "client-1".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/input/".to_owned(),
        );
        assert_eq!(map_reduce_job.get_client_id(), "client-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_reduce_job = MapReduceJob::new(
            "client-1".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/input/".to_owned(),
        );
        assert_eq!(map_reduce_job.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_get_input_directory() {
        let map_reduce_job = MapReduceJob::new(
            "client-1".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/input/".to_owned(),
        );
        assert_eq!(map_reduce_job.get_input_directory(), "/tmp/input/");
    }

    #[test]
    fn test_set_status() {
        let mut map_reduce_job = MapReduceJob::new(
            "client-1".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/input/".to_owned(),
        );
        // Assert that the default status for a map reduce job is Queued.
        assert_eq!(map_reduce_job.get_status(), MapReduceJobStatus::Queued);

        // Set the status to Completed and assert success.
        map_reduce_job.set_status(MapReduceJobStatus::Complete);
        assert_eq!(map_reduce_job.get_status(), MapReduceJobStatus::Complete);
    }

    #[test]
    fn test_queued_work_impl() {
        let map_reduce_job = MapReduceJob::new(
            "client-1".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/input/".to_owned(),
        );

        assert_eq!(map_reduce_job.get_work_bucket(), "client-1");
        assert_eq!(
            map_reduce_job.get_work_id(),
            map_reduce_job.get_map_reduce_id()
        );
    }
}
