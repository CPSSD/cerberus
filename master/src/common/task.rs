use std::collections::HashMap;
use std::cmp::Ordering;

use protobuf::repeated::RepeatedField;
use chrono::prelude::*;
use serde_json;
use uuid::Uuid;

use errors::*;
use util::state::StateHandling;

use cerberus_proto::worker as pb;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskType {
    Map,
    Reduce,
}

#[derive(Serialize, Deserialize)]
pub struct InputLocation {
    input_path: String,
    start_byte: u64,
    end_byte: u64,
}

/// The `Task` is a struct that represents a map or reduce task.
/// This is the unit of work that is processed by a worker.
#[derive(Clone, Debug)]
pub struct Task {
    pub task_type: TaskType,
    pub job_id: String,
    pub id: String,

    pub job_priority: u32,
    pub has_completed_before: bool,

    // This will only exist if TaskType is Map.
    pub map_request: Option<pb::PerformMapRequest>,
    // This will only exist if TaskType is Reduce.
    pub reduce_request: Option<pb::PerformReduceRequest>,

    // A map of partition to output file.
    pub map_output_files: HashMap<u64, String>,

    pub assigned_worker_id: String,
    pub status: TaskStatus,

    // The cpu_time used by the task if it completes sucessfully.
    pub cpu_time: u64,

    // Number of times this task has failed in the past.
    pub failure_count: u16,
    pub failure_details: Option<String>,

    pub time_started: Option<DateTime<Utc>>,
    pub time_completed: Option<DateTime<Utc>>,
}

impl Task {
    pub fn new_map_task<S: Into<String>>(
        job_id: S,
        binary_path: S,
        input_locations: Vec<pb::InputLocation>,
        job_priority: u32,
    ) -> Self {
        let mut map_task_input = pb::MapTaskInput::new();
        map_task_input.set_input_locations(RepeatedField::from_vec(input_locations));

        let id = Uuid::new_v4().to_string();

        let mut map_request = pb::PerformMapRequest::new();
        map_request.set_input(map_task_input);
        map_request.set_mapper_file_path(binary_path.into());
        map_request.set_task_id(id.clone());

        Task {
            task_type: TaskType::Map,
            job_id: job_id.into(),
            id: id,

            job_priority: job_priority,
            has_completed_before: false,

            map_request: Some(map_request),
            reduce_request: None,

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: TaskStatus::Queued,

            cpu_time: 0,

            failure_count: 0,
            failure_details: None,

            time_started: None,
            time_completed: None,
        }
    }

    pub fn reset_map_task(&mut self) {
        self.map_output_files = HashMap::new();
        self.assigned_worker_id = String::new();
        self.status = TaskStatus::Queued;
        self.cpu_time = 0;
        self.failure_count = 0;
        self.time_started = None;
        self.time_completed = None;
        self.has_completed_before = true;
    }

    fn new_map_task_from_json(data: serde_json::Value) -> Result<Self> {
        let request_data = data["request"].clone();

        // Create a basic Map task.
        let id: String = serde_json::from_value(data["job_id"].clone()).chain_err(
            || "Unable to convert job_id",
        )?;
        let binary_path: String = serde_json::from_value(request_data["binary_path"].clone())
            .chain_err(|| "Unable to convert binary_path")?;
        let input_locations: Vec<InputLocation> = serde_json::from_value(
            request_data["input_locations"].clone(),
        ).chain_err(|| "Unable to convert input")?;

        let input_locations_pb = input_locations
            .into_iter()
            .map(|loc| {
                let mut loc_pb = pb::InputLocation::new();
                loc_pb.set_input_path(loc.input_path.clone());
                loc_pb.set_start_byte(loc.start_byte);
                loc_pb.set_end_byte(loc.end_byte);

                loc_pb
            })
            .collect();

        let job_priority: u32 = serde_json::from_value(data["job_priority"].clone())
            .chain_err(|| "Unable to convert job priority")?;

        let mut task = Task::new_map_task(id, binary_path, input_locations_pb, job_priority);

        // Update the state.
        task.load_state(data).chain_err(
            || "Unable to load Task from state",
        )?;

        Ok(task)
    }

    pub fn new_reduce_task<S: Into<String>>(
        job_id: S,
        binary_path: S,
        input_partition: u64,
        input_files: Vec<String>,
        output_directory: S,
        job_priority: u32,
    ) -> Self {
        let id = Uuid::new_v4().to_string();

        let mut reduce_request = pb::PerformReduceRequest::new();
        reduce_request.set_partition(input_partition);
        for input_file in input_files {
            reduce_request.mut_input_file_paths().push(input_file);
        }
        reduce_request.set_reducer_file_path(binary_path.into());
        reduce_request.set_output_directory(output_directory.into());
        reduce_request.set_task_id(id.clone());

        Task {
            task_type: TaskType::Reduce,
            job_id: job_id.into(),
            id: id,

            job_priority: job_priority,
            has_completed_before: false,

            map_request: None,
            reduce_request: Some(reduce_request),

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: TaskStatus::Queued,

            cpu_time: 0,

            failure_count: 0,
            failure_details: None,

            time_started: None,
            time_completed: None,
        }
    }

    fn new_reduce_task_from_json(data: serde_json::Value) -> Result<Self> {
        let request_data = data["request"].clone();

        let input_partition: u64 = serde_json::from_value(request_data["input_partition"].clone())
            .chain_err(|| "Unable to convert input_partition")?;
        let input_files: Vec<String> = serde_json::from_value(request_data["input_files"].clone())
            .chain_err(|| "Unable to convert input_files")?;

        let id: String = serde_json::from_value(data["job_id"].clone()).chain_err(
            || "Unable to convert job_id",
        )?;
        let binary_path: String = serde_json::from_value(request_data["binary_path"].clone())
            .chain_err(|| "Unable to convert binary_path")?;
        let output_dir: String = serde_json::from_value(request_data["output_directory"].clone())
            .chain_err(|| "Unable to convert output_directory")?;

        let job_priority: u32 = serde_json::from_value(data["job_priority"].clone())
            .chain_err(|| "Unable to convert job priority")?;
        let mut task = Task::new_reduce_task(
            id,
            binary_path,
            input_partition,
            input_files,
            output_dir,
            job_priority,
        );

        task.load_state(data).chain_err(
            || "Unable to load Task from state",
        )?;

        Ok(task)
    }
}

impl StateHandling<Error> for Task {
    fn new_from_json(data: serde_json::Value) -> Result<Self> {
        let task_type: TaskType = serde_json::from_value(data["task_type"].clone())
            .chain_err(|| "Unable to convert task_type")?;

        match task_type {
            TaskType::Map => Task::new_map_task_from_json(data),
            TaskType::Reduce => Task::new_reduce_task_from_json(data),
        }
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let request = match self.task_type {
            TaskType::Map => {
                match self.map_request {
                    Some(ref req) => {
                        let input_locations: Vec<InputLocation> = req.get_input()
                            .get_input_locations()
                            .into_iter()
                            .map(|loc| {
                                InputLocation {
                                    input_path: loc.input_path.clone(),
                                    start_byte: loc.start_byte,
                                    end_byte: loc.end_byte,
                                }
                            })
                            .collect();

                        json!({
                            "input_locations": input_locations,
                            "binary_path":req.mapper_file_path,
                        })
                    }
                    None => return Err("Unable to serialize map_request".into()),
                }
            }

            TaskType::Reduce => {
                match self.reduce_request {
                    Some(ref req) => {
                        json!({
                            "input_partition": req.partition,
                            "input_files": req.input_file_paths.clone().into_vec(),
                            "binary_path": req.reducer_file_path,
                            "output_directory": req.output_directory,
                        })
                    }
                    None => return Err("Unable to serialize reduce_request".into()),
                }
            }
        };

        let time_started = match self.time_started {
            Some(timestamp) => timestamp.timestamp(),
            None => -1,
        };
        let time_completed = match self.time_completed {
            Some(timestamp) => timestamp.timestamp(),
            None => -1,
        };

        Ok(json!({
            "task_type": self.task_type,
            "request": request,
            "job_id": self.job_id,
            "id": self.id,
            "map_output_files": self.map_output_files,
            "assigned_worker_id": self.assigned_worker_id,
            "status": self.status,
            "failure_count": self.failure_count,
            "failure_details": self.failure_details,
            "time_started": time_started,
            "time_completed": time_completed,
            "job_priority": self.job_priority,
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        self.id = serde_json::from_value(data["id"].clone()).chain_err(
            || "Unable to convert id",
        )?;

        match self.task_type {
            TaskType::Map => {
                let mut map_request = self.map_request.clone().chain_err(
                    || "Map Request should exist",
                )?;
                map_request.task_id = self.id.clone();
                self.map_request = Some(map_request);
            }
            TaskType::Reduce => {
                let mut reduce_request = self.reduce_request.clone().chain_err(
                    || "Reduce Request should exist",
                )?;
                reduce_request.task_id = self.id.clone();
                self.reduce_request = Some(reduce_request);
            }
        }

        self.map_output_files = serde_json::from_value(data["map_output_files"].clone())
            .chain_err(|| "Unable to convert map_output_files")?;
        self.assigned_worker_id = serde_json::from_value(data["assigned_worker_id"].clone())
            .chain_err(|| "Unable to convert assigned_worker_id")?;
        self.status = serde_json::from_value(data["status"].clone()).chain_err(
            || "Unable to convert status",
        )?;
        self.failure_count = serde_json::from_value(data["failure_count"].clone())
            .chain_err(|| "Unable to convert failure_count")?;
        self.failure_details = serde_json::from_value(data["failure_details"].clone())
            .chain_err(|| "Unable to convert failure_details")?;

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

#[derive(Eq, PartialEq, Serialize, Deserialize)]
pub struct PriorityTask {
    pub id: String,
    pub priority: u32,
}

impl PriorityTask {
    pub fn new(id: String, priority: u32) -> Self {
        PriorityTask { id, priority }
    }
}

impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_task_type() {
        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path("/tmp/input/".to_owned());
        input_location.set_start_byte(0);
        input_location.set_end_byte(0);

        let map_task = Task::new_map_task("map-1", "/tmp/bin", vec![input_location], 1);

        let reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/".to_owned()],
            "/tmp/output/",
            1,
        );

        assert_eq!(map_task.task_type, TaskType::Map);
        assert_eq!(reduce_task.task_type, TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path("/tmp/input/".to_owned());
        input_location.set_start_byte(0);
        input_location.set_end_byte(0);

        let map_task = Task::new_map_task("map-1", "/tmp/bin", vec![input_location], 1);
        assert_eq!(map_task.job_id, "map-1");
    }

    #[test]
    fn test_map_task_request() {
        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path("/tmp/input/file1".to_owned());
        input_location.set_start_byte(0);
        input_location.set_end_byte(0);

        let map_task = Task::new_map_task("map-1", "/tmp/bin", vec![input_location], 1);
        let map_request = map_task.map_request.unwrap();

        assert_eq!("/tmp/bin", map_request.get_mapper_file_path());
        assert_eq!("/tmp/input/file1", map_request.get_input().get_input_locations()[0]
            .input_path);
        assert!(map_task.reduce_request.is_none());
    }

    #[test]
    fn test_reduce_task_request() {
        let reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/file1".to_owned(), "/tmp/input/file2".to_owned()],
            "/tmp/output/",
            1,
        );
        let reduce_request = reduce_task.reduce_request.unwrap();

        assert_eq!("/tmp/bin", reduce_request.get_reducer_file_path());
        assert_eq!(0, reduce_request.get_partition());
        assert_eq!("/tmp/input/file1", reduce_request.get_input_file_paths()[0]);
        assert_eq!("/tmp/input/file2", reduce_request.get_input_file_paths()[1]);
        assert_eq!("/tmp/output/", reduce_request.get_output_directory());
        assert!(reduce_task.map_request.is_none());
    }

    #[test]
    fn test_set_output_files() {
        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path("/tmp/input/".to_owned());
        input_location.set_start_byte(0);
        input_location.set_end_byte(0);

        let mut map_task = Task::new_map_task("map-1", "/tmp/bin", vec![input_location], 1);

        map_task.map_output_files.insert(
            0,
            "output_file_1".to_owned(),
        );
        assert_eq!("output_file_1", map_task.map_output_files.get(&0).unwrap());

        map_task.map_output_files.insert(
            1,
            "output_file_2".to_owned(),
        );
        assert_eq!("output_file_1", map_task.map_output_files.get(&0).unwrap());
        assert_eq!("output_file_2", map_task.map_output_files.get(&1).unwrap());
    }

    #[test]
    fn test_assigned_worker_id() {
        let mut reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
            1,
        );
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.assigned_worker_id, "");

        reduce_task.assigned_worker_id = "worker-1".to_owned();
        assert_eq!(reduce_task.assigned_worker_id, "worker-1");
    }

    #[test]
    fn test_set_status() {
        let mut reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
            1,
        );
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.status, TaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.status = TaskStatus::Complete;
        assert_eq!(reduce_task.status, TaskStatus::Complete);
    }

    #[test]
    fn test_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Task>();
    }

    #[test]
    fn test_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<Task>();
    }
}
