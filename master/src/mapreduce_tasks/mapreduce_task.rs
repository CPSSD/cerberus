use errors::*;
use uuid::Uuid;
use queued_work_store::QueuedWork;

use cerberus_proto::mrworker as pb;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapReduceTaskStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskType {
    Map,
    Reduce,
}

/// The `MapReduceTask` is a struct that represents a map or reduce task.
#[derive(Clone)]
pub struct MapReduceTask {
    task_type: TaskType,
    map_reduce_id: String,
    task_id: String,

    // This will only exist if TaskType is Map.
    perform_map_request: Option<pb::PerformMapRequest>,
    // This will only exist if TaskType is Reduce.
    perform_reduce_request: Option<pb::PerformReduceRequest>,

    // Output files are a key-value pair.
    output_files: Vec<(String, String)>,

    assigned_worker_id: String,
    status: MapReduceTaskStatus,
}

impl MapReduceTask {
    pub fn new<S: Into<String>>(
        task_type: TaskType,
        map_reduce_id: S,
        binary_path: S,
        input_key: Option<String>,
        input_files: Vec<String>,
    ) -> Result<Self> {
        if input_files.is_empty() {
            return Err("Input files cannot be empty".into());
        }
        let mut map_request: Option<pb::PerformMapRequest> = None;
        let mut reduce_request: Option<pb::PerformReduceRequest> = None;
        match task_type {
            TaskType::Reduce => {
                let key = {
                    match input_key {
                        Some(key) => key,
                        None => return Err("Input key cannot be None for reduce task".into()),
                    }
                };
                let mut reduce_req = pb::PerformReduceRequest::new();
                reduce_req.set_intermediate_key(key);
                for input_file in input_files {
                    reduce_req.mut_input_file_paths().push(input_file);
                }
                reduce_req.set_reducer_file_path(binary_path.into());
                reduce_request = Some(reduce_req);
            }
            TaskType::Map => {
                if input_files.len() != 1 {
                    return Err("Map task can only have one input file".into());
                }

                let mut map_req = pb::PerformMapRequest::new();
                map_req.set_input_file_path(input_files[0].clone());
                map_req.set_mapper_file_path(binary_path.into());
                map_request = Some(map_req);
            }
        }

        let task_id = Uuid::new_v4();
        Ok(MapReduceTask {
            task_type: task_type,
            map_reduce_id: map_reduce_id.into(),
            task_id: task_id.to_string(),

            perform_map_request: map_request,
            perform_reduce_request: reduce_request,

            output_files: Vec::new(),

            assigned_worker_id: String::new(),
            status: MapReduceTaskStatus::Queued,
        })
    }

    pub fn get_task_type(&self) -> TaskType {
        self.task_type.clone()
    }

    pub fn get_map_reduce_id(&self) -> &str {
        &self.map_reduce_id
    }

    pub fn get_task_id(&self) -> &str {
        &self.task_id
    }

    pub fn get_perform_map_request(&self) -> Option<pb::PerformMapRequest> {
        self.perform_map_request.clone()
    }

    pub fn get_perform_reduce_request(&self) -> Option<pb::PerformReduceRequest> {
        self.perform_reduce_request.clone()
    }

    pub fn get_output_files(&self) -> &[(String, String)] {
        self.output_files.as_slice()
    }

    pub fn push_output_file<S: Into<String>>(&mut self, output_key: S, output_file: S) {
        self.output_files.push(
            (output_key.into(), output_file.into()),
        );
    }

    pub fn get_assigned_worker_id(&self) -> &str {
        &self.assigned_worker_id
    }

    pub fn set_assigned_worker_id<S: Into<String>>(&mut self, worker_id: S) {
        self.assigned_worker_id = worker_id.into();
    }

    pub fn get_status(&self) -> MapReduceTaskStatus {
        self.status.clone()
    }

    pub fn set_status(&mut self, new_status: MapReduceTaskStatus) {
        self.status = new_status;
    }
}

impl QueuedWork for MapReduceTask {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.get_map_reduce_id().to_owned()
    }

    fn get_work_id(&self) -> String {
        self.get_task_id().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // MapReduceTask Tests
    #[test]
    fn test_get_task_type() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        assert_eq!(map_task.get_task_type(), TaskType::Map);
        assert_eq!(reduce_task.get_task_type(), TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();
        assert_eq!(map_task.get_map_reduce_id(), "map-1");
    }

    #[test]
    fn test_map_task_request() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/file1".to_owned()],
        ).unwrap();
        let map_request = map_task.get_perform_map_request().unwrap();

        assert_eq!("/tmp/bin", map_request.get_mapper_file_path());
        assert_eq!("/tmp/input/file1", map_request.get_input_file_path());
        assert!(map_task.get_perform_reduce_request().is_none());
    }

    #[test]
    fn test_map_task_double_input() {
        let result = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/file1".to_owned(), "/tmp/input/file2".to_owned()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_reduce_task_request() {
        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("key-1".to_owned()),
            vec!["/tmp/input/file1".to_owned(), "/tmp/input/file2".to_owned()],
        ).unwrap();
        let reduce_request = reduce_task.get_perform_reduce_request().unwrap();

        assert_eq!("/tmp/bin", reduce_request.get_reducer_file_path());
        assert_eq!("key-1", reduce_request.get_intermediate_key());
        assert_eq!("/tmp/input/file1", reduce_request.get_input_file_paths()[0]);
        assert_eq!("/tmp/input/file2", reduce_request.get_input_file_paths()[1]);
        assert!(reduce_task.get_perform_map_request().is_none());
    }

    #[test]
    fn test_input_key_reduce_none() {
        let result = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/".to_owned()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_set_output_files() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();
        reduce_task.push_output_file("output-key1", "output_file_1");
        {
            let output_files: &[(String, String)] = reduce_task.get_output_files();
            assert_eq!(
                ("output-key1".to_owned(), "output_file_1".to_owned()),
                output_files[0]
            );
        }

        reduce_task.push_output_file("output-key2", "output_file_2");
        {
            let output_files: &[(String, String)] = reduce_task.get_output_files();
            assert_eq!(
                ("output-key1".to_owned(), "output_file_1".to_owned()),
                output_files[0]
            );
            assert_eq!(
                ("output-key2".to_owned(), "output_file_2".to_owned()),
                output_files[1]
            );
        }
    }

    #[test]
    fn test_assigned_worker_id() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.get_assigned_worker_id(), "");

        reduce_task.set_assigned_worker_id("worker-1");
        assert_eq!(reduce_task.get_assigned_worker_id(), "worker-1");
    }

    #[test]
    fn test_set_status() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.set_status(MapReduceTaskStatus::Complete);
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Complete);
    }

    #[test]
    fn test_queued_work_impl() {
        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1",
            "/tmp/bin",
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();

        assert_eq!(reduce_task.get_work_bucket(), "reduce-1");
        assert_eq!(reduce_task.get_work_id(), reduce_task.get_task_id());
    }
}
