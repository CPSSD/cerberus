use cerberus_proto::mrworker::{PerformMapRequest, PerformReduceRequest};
use errors::*;
use uuid::Uuid;
use mapreduce_job::MapReduceJob;
use queued_work_store::QueuedWork;
use std::collections::HashMap;
use std::io::{Write, BufRead, BufReader};
use std::path::PathBuf;
use std::fs;

const MEGA_BYTE: usize = 1000 * 1000;
const MAP_INPUT_SIZE: usize = MEGA_BYTE * 64;

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
    perform_map_request: Option<PerformMapRequest>,
    // This will only exist if TaskType is Reduce.
    perform_reduce_request: Option<PerformReduceRequest>,

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
        let mut map_request: Option<PerformMapRequest> = None;
        let mut reduce_request: Option<PerformReduceRequest> = None;
        match task_type {
            TaskType::Reduce => {
                let key = {
                    match input_key {
                        Some(key) => key,
                        None => return Err("Input key cannot be None for reduce task".into()),
                    }
                };
                let mut reduce_req = PerformReduceRequest::new();
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

                let mut map_req = PerformMapRequest::new();
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

    pub fn get_perform_map_request(&self) -> Option<PerformMapRequest> {
        self.perform_map_request.clone()
    }

    pub fn get_perform_reduce_request(&self) -> Option<PerformReduceRequest> {
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

struct MapTaskFile {
    task_num: u32,
    bytes_to_write: usize,

    file: fs::File,
    file_path: String,
}

pub trait TaskProcessorTrait {
    fn create_map_tasks(&self, map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>>;
    fn create_reduce_tasks(
        &self,
        map_reduce_job: &MapReduceJob,
        completed_map_tasks: &[&MapReduceTask],
    ) -> Result<Vec<MapReduceTask>>;
}

pub struct TaskProcessor;

impl TaskProcessor {
    fn create_new_task_file(
        &self,
        task_num: u32,
        output_directory: &PathBuf,
    ) -> Result<(MapTaskFile)> {
        let mut current_task_path: PathBuf = output_directory.clone();
        current_task_path.push(format!("map_task_{}", task_num));
        let current_task_file = fs::File::create(current_task_path.clone()).chain_err(
            || "Error creating Map input chunk file.",
        )?;

        let current_task_path_str;
        match current_task_path.to_str() {
            Some(path_str) => {
                current_task_path_str = path_str.to_owned();
            }
            None => return Err("Error getting output task path.".into()),
        }

        Ok(MapTaskFile {
            task_num: task_num,
            bytes_to_write: MAP_INPUT_SIZE,

            file: current_task_file,
            file_path: current_task_path_str,
        })
    }

    // Reads a given input file and splits it into chunks.
    fn read_input_file(
        &self,
        map_reduce_job: &MapReduceJob,
        map_task_file: &mut MapTaskFile,
        input_file: &fs::File,
        output_directory: &PathBuf,
        map_tasks: &mut Vec<MapReduceTask>,
    ) -> Result<()> {
        if map_task_file.bytes_to_write != MAP_INPUT_SIZE {
            map_task_file.file.write_all(b"\n").chain_err(
                || "Error writing line break to file",
            )?;
        }

        let buf_reader = BufReader::new(input_file);
        for line in buf_reader.lines() {
            let read_str = line.chain_err(|| "Error reading Map input.")?;
            map_task_file
                .file
                .write_all(read_str.as_bytes())
                .chain_err(|| "Error writing to Map input chunk file.")?;

            let ammount_read: usize = read_str.len();
            if ammount_read > map_task_file.bytes_to_write {
                map_tasks.push(MapReduceTask::new(
                    TaskType::Map,
                    map_reduce_job.get_map_reduce_id(),
                    map_reduce_job.get_binary_path(),
                    None,
                    vec![map_task_file.file_path.to_owned()],
                ).chain_err(|| "Error creating map task")?);

                *map_task_file =
                    self.create_new_task_file(map_task_file.task_num + 1, output_directory)
                        .chain_err(|| "Error creating Map input chunk file.")?;
            } else {
                map_task_file.bytes_to_write -= ammount_read;
            }
        }
        Ok(())
    }
}

impl TaskProcessorTrait for TaskProcessor {
    fn create_map_tasks(&self, map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
        let mut map_tasks = Vec::new();
        let input_directory = PathBuf::from(map_reduce_job.get_input_directory());

        let mut output_directory: PathBuf = input_directory.clone();
        output_directory.push("MapReduceTasks");
        fs::create_dir_all(output_directory.clone()).chain_err(
            || "Error creating Map tasks output directory.",
        )?;

        let mut map_task_file: MapTaskFile =
            self.create_new_task_file(1, &output_directory).chain_err(
                || "Error creating new Map input file chunk.",
            )?;

        for entry in fs::read_dir(map_reduce_job.get_input_directory())? {
            let entry: fs::DirEntry = entry.chain_err(|| "Error reading input directory.")?;
            let path: PathBuf = entry.path();
            if path.is_file() {
                let file = fs::File::open(path).chain_err(
                    || "Error opening input file.",
                )?;
                self.read_input_file(
                    map_reduce_job,
                    &mut map_task_file,
                    &file,
                    &output_directory,
                    &mut map_tasks,
                ).chain_err(|| "Error reading input file.")?;
            }
        }
        if map_task_file.bytes_to_write != MAP_INPUT_SIZE {
            map_tasks.push(MapReduceTask::new(
                TaskType::Map,
                map_reduce_job.get_map_reduce_id(),
                map_reduce_job.get_binary_path(),
                None,
                vec![map_task_file.file_path.to_owned()],
            ).chain_err(|| "Error creating map task")?);
        }
        Ok(map_tasks)
    }

    fn create_reduce_tasks(
        &self,
        map_reduce_job: &MapReduceJob,
        completed_map_tasks: &[&MapReduceTask],
    ) -> Result<Vec<MapReduceTask>> {
        let mut reduce_tasks = Vec::new();
        let mut key_results_map: HashMap<String, Vec<String>> = HashMap::new();

        for completed_map in completed_map_tasks {
            for output_pair in completed_map.get_output_files() {
                let map_results: &mut Vec<String> = key_results_map
                    .entry(output_pair.0.to_owned())
                    .or_insert_with(Vec::new);
                map_results.push(output_pair.1.to_owned());
            }
        }

        for (reduce_key, reduce_input) in key_results_map {
            reduce_tasks.push(MapReduceTask::new(
                TaskType::Reduce,
                map_reduce_job.get_map_reduce_id(),
                map_reduce_job.get_binary_path(),
                Some(reduce_key),
                reduce_input,
            ).chain_err(|| "Error creating reduce task")?);
        }

        Ok(reduce_tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::io::Read;
    use std::collections::HashSet;

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

    #[test]
    fn test_create_map_tasks() {
        let task_processor = TaskProcessor;

        let test_path = Path::new("/tmp/cerberus/create_task_test/").to_path_buf();
        let mut input_path1 = test_path.clone();
        input_path1.push("input-1");
        let mut input_path2 = test_path.clone();
        input_path2.push("input-2");

        fs::create_dir_all(test_path.clone()).unwrap();
        let mut input_file1 = fs::File::create(input_path1.clone()).unwrap();
        input_file1
            .write_all(b"this is the first test file")
            .unwrap();
        let mut input_file2 = fs::File::create(input_path2.clone()).unwrap();
        input_file2
            .write_all(b"this is the second test file")
            .unwrap();

        let map_reduce_job =
            MapReduceJob::new("test-client", "/tmp/bin", test_path.to_str().unwrap());

        let map_tasks: Vec<MapReduceTask> =
            task_processor.create_map_tasks(&map_reduce_job).unwrap();

        assert_eq!(map_tasks.len(), 1);
        assert_eq!(map_tasks[0].get_task_type(), TaskType::Map);
        assert_eq!(
            map_tasks[0].get_map_reduce_id(),
            map_reduce_job.get_map_reduce_id()
        );

        let perform_map_req = map_tasks[0].get_perform_map_request().unwrap();

        assert_eq!(
            map_reduce_job.get_binary_path(),
            perform_map_req.get_mapper_file_path()
        );

        // Read map task input and make sure it is as expected.
        let mut input_file = fs::File::open(perform_map_req.get_input_file_path().clone()).unwrap();
        let mut map_input = String::new();
        input_file.read_to_string(&mut map_input).unwrap();

        // Either input file order is fine.
        let mut good_inputs = HashSet::new();
        good_inputs.insert(
            "this is the first test file\nthis is the second test file".to_owned(),
        );
        good_inputs.insert(
            "this is the second test file\nthis is the first test file".to_owned(),
        );

        assert!(good_inputs.contains(&map_input));
    }

    #[test]
    fn test_create_reduce_tasks() {
        let task_processor = TaskProcessor;

        let map_reduce_job = MapReduceJob::new("test-client", "/tmp/bin", "/tmp/inputdir");

        let mut map_task1 = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        map_task1.push_output_file("intermediate-key1", "/tmp/output/1");
        map_task1.push_output_file("intermediate-key2", "/tmp/output/2");

        let mut map_task2 = MapReduceTask::new(
            TaskType::Map,
            "map-1",
            "/tmp/bin",
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        map_task2.push_output_file("intermediate-key1", "/tmp/output/3");

        let map_tasks: Vec<&MapReduceTask> = vec![&map_task1, &map_task2];
        let mut reduce_tasks: Vec<MapReduceTask> = task_processor
            .create_reduce_tasks(&map_reduce_job, &map_tasks)
            .unwrap();

        reduce_tasks.sort_by_key(|task| {
            task.get_perform_reduce_request()
                .unwrap()
                .get_intermediate_key()
                .to_owned()
        });

        assert_eq!(2, reduce_tasks.len());

        let reduce_req1 = reduce_tasks[0].get_perform_reduce_request().unwrap();
        let reduce_req2 = reduce_tasks[1].get_perform_reduce_request().unwrap();

        assert_eq!("intermediate-key1", reduce_req1.get_intermediate_key());
        assert_eq!(TaskType::Reduce, reduce_tasks[0].get_task_type());
        assert_eq!(
            map_reduce_job.get_map_reduce_id(),
            reduce_tasks[0].get_map_reduce_id()
        );
        assert_eq!(
            vec!["/tmp/output/1", "/tmp/output/3"],
            reduce_req1.get_input_file_paths()
        );

        assert_eq!("intermediate-key2", reduce_req2.get_intermediate_key());
        assert_eq!(TaskType::Reduce, reduce_tasks[1].get_task_type());
        assert_eq!(
            map_reduce_job.get_map_reduce_id(),
            reduce_tasks[1].get_map_reduce_id()
        );
        assert_eq!(vec!["/tmp/output/2"], reduce_req2.get_input_file_paths());
    }
}
