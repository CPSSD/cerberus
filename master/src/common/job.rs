use errors::*;
use std::path::{PathBuf, Path};
use std::process::{Command, Stdio};

use chrono::prelude::*;
use serde_json;
use uuid::Uuid;

use state::StateHandling;
use cerberus_proto::mapreduce as pb;
use queued_work_store::QueuedWork;

/// `JobOptions` stores arguments used to construct a `Job`.
#[derive(Default)]
pub struct JobOptions {
    /// An ID for the client owning the job.
    pub client_id: String,
    /// The path to the payload binary for the job.
    pub binary_path: String,
    /// The path containing the input files to be used by the job.
    pub input_directory: String,
    /// The path to which the output of the job should be written.
    pub output_directory: Option<String>,
    /// Determines if paths should be validated. Should only be disabled during tests.
    pub validate_paths: bool,
}

impl From<pb::MapReduceRequest> for JobOptions {
    fn from(other: pb::MapReduceRequest) -> Self {
        JobOptions {
            client_id: other.client_id,
            binary_path: other.binary_path,
            input_directory: other.input_directory,
            output_directory: if !other.output_directory.is_empty() {
                Some(other.output_directory)
            } else {
                None
            },
            validate_paths: true,
        }
    }
}

/// The `Job` is a struct that represents a mapreduce job submitted by a client.
#[derive(Clone)]
pub struct Job {
    pub client_id: String,
    pub id: String,
    pub binary_path: String,
    pub input_directory: String,
    pub output_directory: String,

    pub status: pb::Status,
    pub status_details: Option<String>,

    pub map_tasks_completed: u32,
    pub map_tasks_total: u32,

    pub reduce_tasks_completed: u32,
    pub reduce_tasks_total: u32,

    pub time_requested: DateTime<Utc>,
    pub time_started: Option<DateTime<Utc>>,
    pub time_completed: Option<DateTime<Utc>>,

    /// Total CPU time used by the job.
    pub cpu_time: u64,
}

#[derive(Serialize, Deserialize)]
#[allow(non_camel_case_types)]
/// `SerializableJobStatus` is the Serializable counterpart to `mapreduce_proto::Status`.
pub enum SerializableJobStatus {
    DONE,
    IN_PROGRESS,
    IN_QUEUE,
    FAILED,
    UNKNOWN,
}

impl Job {
    pub fn new(options: JobOptions) -> Result<Self> {
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

        let job = Job {
            client_id: options.client_id,
            id: Uuid::new_v4().to_string(),
            binary_path: options.binary_path,
            input_directory: input_directory,
            output_directory: output_directory,

            status: pb::Status::IN_QUEUE,
            status_details: None,

            map_tasks_completed: 0,
            map_tasks_total: 0,

            reduce_tasks_completed: 0,
            reduce_tasks_total: 0,

            time_requested: Utc::now(),
            time_started: None,
            time_completed: None,

            cpu_time: 0,
        };
        if options.validate_paths {
            job.validate_input().chain_err(|| "Error validating input")?;
        }
        Ok(job)
    }

    fn validate_input(&self) -> Result<()> {
        // Validate the existence of the input directory and the binary file.
        let input_path = Path::new(self.input_directory.as_str().clone());
        if !(input_path.exists() && input_path.is_dir()) {
            return Err(
                format!("Input directory does not exist: {}", self.input_directory).into(),
            );
        }

        let binary_path = Path::new(self.binary_path.as_str().clone());
        if !(binary_path.exists() && binary_path.is_file()) {
            return Err(
                format!("Binary does not exist: {}", self.binary_path).into(),
            );
        }

        // Binary exists, so run sanity-check on it to verify that it's a libcerberus binary.
        self.run_sanity_check()
    }

    fn run_sanity_check(&self) -> Result<()> {
        let binary_path = Path::new(self.binary_path.as_str());
        let child = Command::new(binary_path)
            .arg("sanity-check")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .chain_err(|| "Unable to run sanity-check on binary")?;
        let output = child.wait_with_output().chain_err(
            || "Error waiting for output from binary",
        )?;
        let output_str = String::from_utf8(output.stdout).chain_err(
            || "Unable to read output from binary",
        )?;

        if output_str.contains("sanity located") {
            Ok(())
        } else {
            Err("Binary failed sanity-check".into())
        }
    }

    fn status_from_state(&self, state: &SerializableJobStatus) -> pb::Status {
        match *state {
            SerializableJobStatus::DONE => pb::Status::DONE,
            SerializableJobStatus::IN_PROGRESS => pb::Status::IN_PROGRESS,
            SerializableJobStatus::IN_QUEUE => pb::Status::IN_QUEUE,
            SerializableJobStatus::FAILED => pb::Status::FAILED,
            SerializableJobStatus::UNKNOWN => pb::Status::UNKNOWN,
        }
    }

    fn get_serializable_status(&self) -> SerializableJobStatus {
        match self.status {
            pb::Status::DONE => SerializableJobStatus::DONE,
            pb::Status::IN_PROGRESS => SerializableJobStatus::IN_PROGRESS,
            pb::Status::IN_QUEUE => SerializableJobStatus::IN_QUEUE,
            pb::Status::FAILED => SerializableJobStatus::FAILED,
            pb::Status::UNKNOWN => SerializableJobStatus::UNKNOWN,
        }
    }
}

impl StateHandling for Job {
    fn new_from_json(data: serde_json::Value) -> Result<Self> {
        let options = JobOptions {
            client_id: serde_json::from_value(data["client_id"].clone())
                .chain_err(|| "Unable to convert client_id")?,
            binary_path: serde_json::from_value(data["binary_path"].clone())
                .chain_err(|| "Unable to convert binary_path")?,
            input_directory: serde_json::from_value(data["input_directory"].clone())
                .chain_err(|| "Unable to convert input_directory")?,
            output_directory: serde_json::from_value(data["output_directory"].clone())
                .chain_err(|| "Unable to convert output dir")?,
            validate_paths: true,
        };

        let mut job = Job::new(options).chain_err(
            || "Unable to create map reduce job",
        )?;

        job.load_state(data).chain_err(|| "Unable to load state")?;

        Ok(job)
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let time_started = match self.time_started {
            Some(timestamp) => timestamp.timestamp(),
            None => -1,
        };
        let time_completed = match self.time_completed {
            Some(timestamp) => timestamp.timestamp(),
            None => -1,
        };
        Ok(json!({
            "client_id": self.client_id,
            "id": self.id,
            "binary_path": self.binary_path,
            "input_directory": self.input_directory,
            "output_directory": self.output_directory,

            "status": self.get_serializable_status(),
            "status_details": self.status_details,

            "map_tasks_completed": self.map_tasks_completed,
            "map_tasks_total": self.map_tasks_total,

            "reduce_tasks_completed": self.reduce_tasks_completed,
            "reduce_tasks_total": self.reduce_tasks_total,

            "time_requested": self.time_requested.timestamp(),
            "time_started": time_started,
            "time_completed": time_completed,
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        self.id = serde_json::from_value(data["id"].clone()).chain_err(
            || "Unable to convert id",
        )?;

        let status: SerializableJobStatus =
            serde_json::from_value(data["status"].clone()).chain_err(
                || "Unable to convert mapreduce status",
            )?;
        self.status = self.status_from_state(&status);
        self.status_details = serde_json::from_value(data["status_details"].clone())
            .chain_err(|| "Unable to convert status_details.")?;

        self.map_tasks_completed = serde_json::from_value(data["map_tasks_completed"].clone())
            .chain_err(|| "Unable to convert map_tasks_complete")?;
        self.map_tasks_total = serde_json::from_value(data["map_tasks_total"].clone())
            .chain_err(|| "Unable to convert map_tasks_total")?;

        self.reduce_tasks_completed = serde_json::from_value(
            data["reduce_tasks_completed"].clone(),
        ).chain_err(|| "Unable to convert reduce_tasks_complete")?;
        self.reduce_tasks_total = serde_json::from_value(data["reduce_tasks_total"].clone())
            .chain_err(|| "Unable to convert reduce_tasks_total")?;

        let time_requested: i64 = serde_json::from_value(data["time_requested"].clone())
            .chain_err(|| "Unable to convert time_requested")?;
        self.time_requested =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(time_requested, 0), Utc);

        let time_started: i64 = serde_json::from_value(data["time_started"].clone())
            .chain_err(|| "Unable to convert time_started")?;
        self.time_started = match time_started {
            -1 => None,
            _ => Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(time_started, 0),
                Utc,
            )),
        };

        let time_completed: i64 = serde_json::from_value(data["time_completed"].clone())
            .chain_err(|| "Unable to convert time_completed")?;
        self.time_completed = match time_completed {
            -1 => None,
            _ => Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(time_completed, 0),
                Utc,
            )),
        };

        Ok(())
    }
}

impl QueuedWork for Job {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.client_id.clone()
    }

    fn get_work_id(&self) -> String {
        self.id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_job_options() -> JobOptions {
        JobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input/".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn test_defaults() {
        let job = Job::new(get_test_job_options()).unwrap();
        // Assert that the default status for a map reduce job is Queued.
        assert_eq!(pb::Status::IN_QUEUE, job.status);
        // Assert that completed tasks starts at 0.
        assert_eq!(0, job.map_tasks_completed);
        assert_eq!(0, job.reduce_tasks_completed);
        // Assert that total tasks starts at 0.
        assert_eq!(0, job.map_tasks_total);
        assert_eq!(0, job.reduce_tasks_total);
    }

    #[test]
    fn test_queued_work_impl() {
        let job = Job::new(get_test_job_options()).unwrap();

        assert_eq!(job.get_work_bucket(), "client-1");
        assert_eq!(job.get_work_id(), job.id);
    }

    #[test]
    fn test_output_directory() {
        let job1 = Job::new(get_test_job_options()).unwrap();
        let job2 = Job::new(JobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/binary".to_owned(),
            input_directory: "/tmp/input/".to_owned(),
            output_directory: Some("/tmp/output/".to_owned()),
            validate_paths: false,
        }).unwrap();

        assert_eq!("/tmp/input/output/", job1.output_directory);
        assert_eq!("/tmp/output/", job2.output_directory);
    }

    #[test]
    fn test_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Job>();
    }

    #[test]
    fn test_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<Job>();
    }
}
