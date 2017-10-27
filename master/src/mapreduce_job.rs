use errors::*;
use queued_work_store::QueuedWork;
use std::path::PathBuf;
use uuid::Uuid;

use cerberus_proto::mapreduce as mr_proto;

/// `MapReduceJobOptions` stores arguments used to construct a `MapReduceJob`.
#[derive(Default)]
pub struct MapReduceJobOptions {
    /// An ID for the client owning the job.
    pub client_id: String,
    /// The path to the payload binary for the job.
    pub binary_path: String,
    /// The path containing the input files to be used by the job.
    pub input_directory: String,
    /// The path to which the output of the job should be written.
    pub output_directory: Option<String>,
}

impl From<mr_proto::MapReduceRequest> for MapReduceJobOptions {
    fn from(other: mr_proto::MapReduceRequest) -> Self {
        MapReduceJobOptions {
            client_id: other.client_id,
            binary_path: other.binary_path,
            input_directory: other.input_directory,
            output_directory: if other.output_directory.is_empty() {
                Some(other.output_directory)
            } else {
                None
            },
        }
    }
}

/// The `MapReduceJob` is a struct that represents a `MapReduce` job submitted by a client.
#[derive(Clone)]
pub struct MapReduceJob {
    client_id: String,
    map_reduce_id: String,
    binary_path: String,
    input_directory: String,
    output_directory: String,

    status: mr_proto::Status,

    map_tasks_completed: u32,
    map_tasks_total: u32,

    reduce_tasks_completed: u32,
    reduce_tasks_total: u32,
}

impl MapReduceJob {
    pub fn new(options: MapReduceJobOptions) -> Result<Self> {
        let map_reduce_id = Uuid::new_v4();
        let input_directory = options.input_directory;
        let output_directory = match options.output_directory {
            Some(dir) => dir,
            None => {
                let mut output_path_buf = PathBuf::new();
                output_path_buf.push(input_directory.as_str());
                output_path_buf.push("output/");
                output_path_buf
                    .to_str()
                    .ok_or("Error generating output file path")?
                    .to_owned()
            }
        };

        Ok(MapReduceJob {
            client_id: options.client_id,
            map_reduce_id: map_reduce_id.to_string(),
            binary_path: options.binary_path,
            input_directory: input_directory,
            output_directory: output_directory,

            status: mr_proto::Status::IN_QUEUE,

            map_tasks_completed: 0,
            map_tasks_total: 0,

            reduce_tasks_completed: 0,
            reduce_tasks_total: 0,
        })
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

    pub fn get_output_directory(&self) -> &str {
        &self.output_directory
    }

    pub fn get_status(&self) -> mr_proto::Status {
        self.status
    }

    pub fn set_status(&mut self, new_status: mr_proto::Status) {
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

    fn get_test_job_options() -> MapReduceJobOptions {
        MapReduceJobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input/".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn test_get_client_id() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        assert_eq!(map_reduce_job.get_client_id(), "client-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        assert_eq!(map_reduce_job.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_get_input_directory() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        assert_eq!(map_reduce_job.get_input_directory(), "/tmp/input/");
    }

    #[test]
    fn test_set_status() {
        let mut map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        // Assert that the default status for a map reduce job is Queued.
        assert_eq!(mr_proto::Status::IN_QUEUE, map_reduce_job.get_status());

        // Set the status to Completed and assert success.
        map_reduce_job.set_status(mr_proto::Status::DONE);
        assert_eq!(mr_proto::Status::DONE, map_reduce_job.get_status());
    }

    #[test]
    fn test_tasks_completed() {
        let mut map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
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
        let mut map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
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
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();

        assert_eq!(map_reduce_job.get_work_bucket(), "client-1");
        assert_eq!(
            map_reduce_job.get_work_id(),
            map_reduce_job.get_map_reduce_id()
        );
    }

    #[test]
    fn test_output_directory() {
        let map_reduce_job1 = MapReduceJob::new(get_test_job_options()).unwrap();
        let map_reduce_job2 = MapReduceJob::new(MapReduceJobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/binary".to_owned(),
            input_directory: "/tmp/input/".to_owned(),
            output_directory: Some("/tmp/output/".to_owned()),
        }).unwrap();

        assert_eq!("/tmp/input/output/", map_reduce_job1.get_output_directory());
        assert_eq!("/tmp/output/", map_reduce_job2.get_output_directory());
    }
}
