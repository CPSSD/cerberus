use std::collections::HashMap;
use uuid::Uuid;
use queued_work_store::QueuedWork;

use cerberus_proto::worker as pb;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapReduceTaskStatus {
    Queued,
    InProgress,
    Complete,
    //TODO(conor): Remove this when Failed is used.
    #[allow(dead_code)]
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskType {
    Map,
    Reduce,
}

/// The `MapReduceTask` is a struct that represents a map or reduce task.
/// This is the unit of work that is processed by a worker.
#[derive(Clone)]
pub struct MapReduceTask {
    task_type: TaskType,
    map_reduce_id: String,
    task_id: String,

    // This will only exist if TaskType is Map.
    perform_map_request: Option<pb::PerformMapRequest>,
    // This will only exist if TaskType is Reduce.
    perform_reduce_request: Option<pb::PerformReduceRequest>,

    // A map of partition to output file.
    map_output_files: HashMap<u64, String>,

    assigned_worker_id: String,
    status: MapReduceTaskStatus,
}

impl MapReduceTask {
    pub fn new_map_task<S: Into<String>>(map_reduce_id: S, binary_path: S, input_file: S) -> Self {
        let mut map_request = pb::PerformMapRequest::new();
        map_request.set_input_file_path(input_file.into());
        map_request.set_mapper_file_path(binary_path.into());

        let task_id = Uuid::new_v4();
        MapReduceTask {
            task_type: TaskType::Map,
            map_reduce_id: map_reduce_id.into(),
            task_id: task_id.to_string(),

            perform_map_request: Some(map_request),
            perform_reduce_request: None,

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: MapReduceTaskStatus::Queued,
        }
    }

    pub fn new_reduce_task<S: Into<String>>(
        map_reduce_id: S,
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

        let task_id = Uuid::new_v4();
        MapReduceTask {
            task_type: TaskType::Reduce,
            map_reduce_id: map_reduce_id.into(),
            task_id: task_id.to_string(),

            perform_map_request: None,
            perform_reduce_request: Some(reduce_request),

            map_output_files: HashMap::new(),

            assigned_worker_id: String::new(),
            status: MapReduceTaskStatus::Queued,
        }
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

    pub fn get_map_output_files(&self) -> &HashMap<u64, String> {
        &self.map_output_files
    }

    pub fn insert_map_output_file<S: Into<String>>(
        &mut self,
        output_partition: u64,
        output_file: S,
    ) {
        self.map_output_files.insert(
            output_partition,
            output_file.into(),
        );
    }

    //TODO(conor): Remove this when get_assigned_worker_id is used.
    #[allow(dead_code)]
    pub fn get_assigned_worker_id(&self) -> &str {
        &self.assigned_worker_id
    }

    pub fn set_assigned_worker_id<S: Into<String>>(&mut self, worker_id: S) {
        self.assigned_worker_id = worker_id.into();
    }

    //TODO(conor): Remove this when get_status is used.
    #[allow(dead_code)]
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

    #[test]
    fn test_get_task_type() {
        let map_task = MapReduceTask::new_map_task("map-1", "/tmp/bin", "/tmp/input/");

        let reduce_task = MapReduceTask::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/".to_owned()],
            "/tmp/output/",
        );

        assert_eq!(map_task.get_task_type(), TaskType::Map);
        assert_eq!(reduce_task.get_task_type(), TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let map_task = MapReduceTask::new_map_task("map-1", "/tmp/bin", "/tmp/input/");
        assert_eq!(map_task.get_map_reduce_id(), "map-1");
    }

    #[test]
    fn test_map_task_request() {
        let map_task = MapReduceTask::new_map_task("map-1", "/tmp/bin", "/tmp/input/file1");
        let map_request = map_task.get_perform_map_request().unwrap();

        assert_eq!("/tmp/bin", map_request.get_mapper_file_path());
        assert_eq!("/tmp/input/file1", map_request.get_input_file_path());
        assert!(map_task.get_perform_reduce_request().is_none());
    }

    #[test]
    fn test_reduce_task_request() {
        let reduce_task = MapReduceTask::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/file1".to_owned(), "/tmp/input/file2".to_owned()],
            "/tmp/output/",
        );
        let reduce_request = reduce_task.get_perform_reduce_request().unwrap();

        assert_eq!("/tmp/bin", reduce_request.get_reducer_file_path());
        assert_eq!(0, reduce_request.get_partition());
        assert_eq!("/tmp/input/file1", reduce_request.get_input_file_paths()[0]);
        assert_eq!("/tmp/input/file2", reduce_request.get_input_file_paths()[1]);
        assert_eq!("/tmp/output/", reduce_request.get_output_directory());
        assert!(reduce_task.get_perform_map_request().is_none());
    }

    #[test]
    fn test_set_output_files() {
        let mut map_task = MapReduceTask::new_map_task("map-1", "/tmp/bin", "/tmp/bin/input");

        map_task.insert_map_output_file(0, "output_file_1");
        {
            let output_files = map_task.get_map_output_files();
            assert_eq!("output_file_1", output_files.get(&0).unwrap());
        }

        map_task.insert_map_output_file(1, "output_file_2");
        {
            let output_files = map_task.get_map_output_files();
            assert_eq!("output_file_1", output_files.get(&0).unwrap());
            assert_eq!("output_file_2", output_files.get(&1).unwrap());
        }
    }

    #[test]
    fn test_assigned_worker_id() {
        let mut reduce_task = MapReduceTask::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
        );
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.get_assigned_worker_id(), "");

        reduce_task.set_assigned_worker_id("worker-1");
        assert_eq!(reduce_task.get_assigned_worker_id(), "worker-1");
    }

    #[test]
    fn test_set_status() {
        let mut reduce_task = MapReduceTask::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
        );
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.set_status(MapReduceTaskStatus::Complete);
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Complete);
    }

    #[test]
    fn test_queued_work_impl() {
        let reduce_task = MapReduceTask::new_reduce_task(
            "reduce-1",
            "/tmp/bin",
            0,
            vec!["/tmp/input/inter_mediate".to_owned()],
            "/tmp/output/",
        );

        assert_eq!(reduce_task.get_work_bucket(), "reduce-1");
        assert_eq!(reduce_task.get_work_id(), reduce_task.get_task_id());
    }
}
