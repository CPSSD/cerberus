use uuid::Uuid;

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
pub struct MapReduceTask {
    task_type: TaskType,
    map_reduce_id: String,
    task_id: String,

    binary_path: String,
    input_files: Vec<String>,
    output_files: Vec<String>,

    assigned_worker_id: String,
    status: MapReduceTaskStatus,
}

impl MapReduceTask {
    pub fn new(
        task_type: TaskType,
        map_reduce_id: String,
        binary_path: String,
        input_files: Vec<String>,
    ) -> Self {
        let task_id = Uuid::new_v4();
        MapReduceTask {
            task_type: task_type,
            map_reduce_id: map_reduce_id,
            task_id: task_id.to_string(),

            binary_path: binary_path,
            input_files: input_files,
            output_files: Vec::new(),

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

    pub fn get_binary_path(&self) -> &str {
        &self.binary_path
    }

    pub fn get_input_files(&self) -> &[String] {
        self.input_files.as_slice()
    }

    pub fn get_output_files(&self) -> &[String] {
        self.output_files.as_slice()
    }

    pub fn push_output_file(&mut self, output_file: String) {
        self.output_files.push(output_file);
    }

    pub fn get_assigned_worker_id(&self) -> &str {
        &self.assigned_worker_id
    }

    pub fn set_assigned_worker_id(&mut self, worker_id: String) {
        self.assigned_worker_id = worker_id;
    }

    pub fn get_status(&self) -> MapReduceTaskStatus {
        self.status.clone()
    }

    pub fn set_status(&mut self, new_status: MapReduceTaskStatus) {
        self.status = new_status;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_task_type() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );

        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );

        assert_eq!(map_task.get_task_type(), TaskType::Map);
        assert_eq!(reduce_task.get_task_type(), TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        assert_eq!(map_task.get_map_reduce_id(), "map-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        assert_eq!(map_task.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_get_input_files() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        let input_files: &[String] = map_task.get_input_files();
        assert_eq!(input_files[0], "/tmp/input/");
    }

    #[test]
    fn test_set_output_files() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        reduce_task.push_output_file("output_file_1".to_owned());
        {
            let output_files: &[String] = reduce_task.get_output_files();
            assert_eq!(output_files[0], "output_file_1");
        }

        reduce_task.push_output_file("output_file_2".to_owned());
        {
            let output_files: &[String] = reduce_task.get_output_files();
            assert_eq!(output_files[0], "output_file_1");
            assert_eq!(output_files[1], "output_file_2");
        }
    }

    #[test]
    fn test_assigned_worker_id() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.get_assigned_worker_id(), "");

        reduce_task.set_assigned_worker_id("worker-1".to_owned());
        assert_eq!(reduce_task.get_assigned_worker_id(), "worker-1");
    }

    /*

    status: MapReduceTaskStatus,
    */
    #[test]
    fn test_set_status() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.set_status(MapReduceTaskStatus::Complete);
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Complete);
    }
}
