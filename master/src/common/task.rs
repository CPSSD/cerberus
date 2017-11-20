use std::collections::HashMap;

use serde_json;
use uuid::Uuid;

use errors::*;
use queued_work_store::QueuedWork;
use state::StateHandling;

use cerberus_proto::worker as pb;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    InProgress,
    Complete,
    //TODO(conor): Remove this when Failed is used.
    #[allow(dead_code)]
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskType {
    Map,
    Reduce,
}

/// The `Task` is a struct that represents a map or reduce task.
/// This is the unit of work that is processed by a worker.
#[derive(Clone)]
pub struct Task {
    pub task_type: TaskType,
    pub job_id: String,
    pub id: String,

    // This will only exist if TaskType is Map.
    pub map_request: Option<pb::PerformMapRequest>,
    // This will only exist if TaskType is Reduce.
    pub reduce_request: Option<pb::PerformReduceRequest>,

    // A map of partition to output file.
    pub map_output_files: HashMap<u64, String>,

    pub assigned_worker_id: String,
    pub status: TaskStatus,

    // Number of times this task has failed in the past.
    pub failure_count: u16,
    pub failure_details: Option<String>,
}

impl Task {
    pub fn new_map_task<S: Into<String>>(job_id: S, binary_path: S, input_file: S) -> Self {
        let mut map_request = pb::PerformMapRequest::new();
        map_request.set_input_file_path(input_file.into());
        map_request.set_mapper_file_path(binary_path.into());

        Task {
            task_type: TaskType::Map,
            job_id: job_id.into(),
            id: Uuid::new_v4().to_string(),

            map_request: Some(map_request),
            reduce_request: None,

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: TaskStatus::Queued,

            failure_count: 0,
            failure_details: None,
        }
    }

    fn new_map_task_from_json(data: serde_json::Value) -> Result<Self> {
        let request_data = data["request"].clone();

        // Create a basic Map task.
        let id: String = serde_json::from_value(data["map_reduce_id"].clone())
            .chain_err(|| "Unable to convert map_reduce_id")?;
        let binary_path: String = serde_json::from_value(request_data["binary_path"].clone())
            .chain_err(|| "Unable to convert binary_path")?;
        let input_file: String = serde_json::from_value(request_data["input_file"].clone())
            .chain_err(|| "Unable to convert input_file")?;
        let mut task = Task::new_map_task(id, binary_path, input_file);

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
    ) -> Self {
        let mut reduce_request = pb::PerformReduceRequest::new();
        reduce_request.set_partition(input_partition);
        for input_file in input_files {
            reduce_request.mut_input_file_paths().push(input_file);
        }
        reduce_request.set_reducer_file_path(binary_path.into());
        reduce_request.set_output_directory(output_directory.into());

        Task {
            task_type: TaskType::Reduce,
            job_id: job_id.into(),
            id: Uuid::new_v4().to_string(),

            map_request: None,
            reduce_request: Some(reduce_request),

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: TaskStatus::Queued,

            failure_count: 0,
            failure_details: None,
        }
    }

    fn new_reduce_task_from_json(data: serde_json::Value) -> Result<Self> {
        let request_data = data["request"].clone();

        let input_partition: u64 = serde_json::from_value(request_data["input_partition"].clone())
            .chain_err(|| "Unable to convert input_partition")?;
        let input_files: Vec<String> = serde_json::from_value(request_data["input_files"].clone())
            .chain_err(|| "Unable to convert input_files")?;

        let id: String = serde_json::from_value(data["map_reduce_id"].clone())
            .chain_err(|| "Unable to convert map_reduce_id")?;
        let binary_path: String = serde_json::from_value(request_data["binary_path"].clone())
            .chain_err(|| "Unable to convert binary_path")?;
        let output_dir: String = serde_json::from_value(request_data["output_directory"].clone())
            .chain_err(|| "Unable to convert output_directory")?;
        let mut task =
            Task::new_reduce_task(id, binary_path, input_partition, input_files, output_dir);

        task.load_state(data).chain_err(
            || "Unable to load Task from state",
        )?;

        Ok(task)
    }
}

impl StateHandling for Task {
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
                        json!({
                            "input_file": req.input_file_path,
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
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        self.id = serde_json::from_value(data["id"].clone()).chain_err(
            || "Unable to convert id",
        )?;
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

        Ok(())
    }
}

impl QueuedWork for Task {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.job_id.to_owned()
    }

    fn get_work_id(&self) -> String {
        self.id.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_task_type() {
        let map_task = Task::new_map_task("map-1", "/tmp/bin", "/tmp/input/");

        let reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/".to_owned()],
            "/tmp/output/",
        );

        assert_eq!(map_task.task_type, TaskType::Map);
        assert_eq!(reduce_task.task_type, TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let map_task = Task::new_map_task("map-1", "/tmp/bin", "/tmp/input/");
        assert_eq!(map_task.job_id, "map-1");
    }

    #[test]
    fn test_map_task_request() {
        let map_task = Task::new_map_task("map-1", "/tmp/bin", "/tmp/input/file1");
        let map_request = map_task.map_request.unwrap();

        assert_eq!("/tmp/bin", map_request.get_mapper_file_path());
        assert_eq!("/tmp/input/file1", map_request.get_input_file_path());
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
        let mut map_task = Task::new_map_task("map-1", "/tmp/bin", "/tmp/bin/input");

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
        );
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.status, TaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.status = TaskStatus::Complete;
        assert_eq!(reduce_task.status, TaskStatus::Complete);
    }

    #[test]
    fn test_queued_work_impl() {
        let reduce_task = Task::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
        );

        assert_eq!(reduce_task.get_work_bucket(), "reduce-1");
        assert_eq!(reduce_task.get_work_id(), reduce_task.id);
    }
}
