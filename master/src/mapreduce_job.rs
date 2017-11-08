use std::path::PathBuf;

use chrono::prelude::*;
use uuid::Uuid;

use cerberus_proto::mapreduce as mr_proto;
use errors::*;
use queued_work_store::QueuedWork;

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
            output_directory: if !other.output_directory.is_empty() {
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
    pub client_id: String,
    pub map_reduce_id: String,
    pub binary_path: String,
    pub input_directory: String,
    pub output_directory: String,

    pub status: mr_proto::Status,

    pub map_tasks_completed: u32,
    pub map_tasks_total: u32,

    pub reduce_tasks_completed: u32,
    pub reduce_tasks_total: u32,

    pub time_requested: DateTime<Utc>,
    pub time_started: Option<DateTime<Utc>>,
    pub time_completed: Option<DateTime<Utc>>,
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

            time_requested: Utc::now(),
            time_started: None,
            time_completed: None,
        })
    }
}

impl QueuedWork for MapReduceJob {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.client_id.clone()
    }

    fn get_work_id(&self) -> String {
        self.map_reduce_id.clone()
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
    fn test_defaults() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();
        // Assert that the default status for a map reduce job is Queued.
        assert_eq!(mr_proto::Status::IN_QUEUE, map_reduce_job.status);
        // Assert that completed tasks starts at 0.
        assert_eq!(0, map_reduce_job.map_tasks_completed);
        assert_eq!(0, map_reduce_job.reduce_tasks_completed);
        // Assert that total tasks starts at 0.
        assert_eq!(0, map_reduce_job.map_tasks_total);
        assert_eq!(0, map_reduce_job.reduce_tasks_total);
    }

    #[test]
    fn test_queued_work_impl() {
        let map_reduce_job = MapReduceJob::new(get_test_job_options()).unwrap();

        assert_eq!(map_reduce_job.get_work_bucket(), "client-1");
        assert_eq!(map_reduce_job.get_work_id(), map_reduce_job.map_reduce_id);
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

        assert_eq!("/tmp/input/output/", map_reduce_job1.output_directory);
        assert_eq!("/tmp/output/", map_reduce_job2.output_directory);
    }
}
