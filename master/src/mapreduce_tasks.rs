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

    binary_path: String,
    // The input_key will only exist for Reduce operations.
    input_key: Option<String>,
    input_files: Vec<String>,

    // Output files are a key-value pair.
    output_files: Vec<(String, String)>,

    assigned_worker_id: String,
    status: MapReduceTaskStatus,
}

impl MapReduceTask {
    pub fn new(
        task_type: TaskType,
        map_reduce_id: String,
        binary_path: String,
        input_key: Option<String>,
        input_files: Vec<String>,
    ) -> Result<Self> {
        if task_type == TaskType::Reduce && input_key.is_none() {
            return Err("Input key cannot be None for reduce task".into());
        }
        let task_id = Uuid::new_v4();
        Ok(MapReduceTask {
            task_type: task_type,
            map_reduce_id: map_reduce_id,
            task_id: task_id.to_string(),

            binary_path: binary_path,
            input_key: input_key,
            input_files: input_files,

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

    pub fn get_binary_path(&self) -> &str {
        &self.binary_path
    }

    pub fn get_input_key(&self) -> Option<String> {
        self.input_key.clone()
    }

    pub fn get_input_files(&self) -> &[String] {
        self.input_files.as_slice()
    }

    pub fn get_output_files(&self) -> &[(String, String)] {
        self.output_files.as_slice()
    }

    pub fn push_output_file(&mut self, output_key: String, output_file: String) {
        self.output_files.push((output_key, output_file));
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
        completed_map_tasks: &Vec<MapReduceTask>,
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
                    map_reduce_job.get_map_reduce_id().to_owned(),
                    map_reduce_job.get_binary_path().to_owned(),
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
                map_reduce_job.get_map_reduce_id().to_owned(),
                map_reduce_job.get_binary_path().to_owned(),
                None,
                vec![map_task_file.file_path.to_owned()],
            ).chain_err(|| "Error creating map task")?);
        }
        Ok(map_tasks)
    }

    fn create_reduce_tasks(
        &self,
        map_reduce_job: &MapReduceJob,
        completed_map_tasks: &Vec<MapReduceTask>,
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
                map_reduce_job.get_map_reduce_id().to_owned(),
                map_reduce_job.get_binary_path().to_owned(),
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
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
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
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();
        assert_eq!(map_task.get_map_reduce_id(), "map-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();
        assert_eq!(map_task.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_input_key_reduce_none() {
        let result = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_get_input_key() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();
        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        assert_eq!(None, map_task.get_input_key());
        assert_eq!(
            Some("intermediate-key".to_owned()),
            reduce_task.get_input_key()
        );
    }

    #[test]
    fn test_get_input_files() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();
        let input_files: &[String] = map_task.get_input_files();
        assert_eq!(input_files[0], "/tmp/input/");
    }

    #[test]
    fn test_set_output_files() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();
        reduce_task.push_output_file("output-key1".to_owned(), "output_file_1".to_owned());
        {
            let output_files: &[(String, String)] = reduce_task.get_output_files();
            assert_eq!(
                ("output-key1".to_owned(), "output_file_1".to_owned()),
                output_files[0]
            );
        }

        reduce_task.push_output_file("output-key2".to_owned(), "output_file_2".to_owned());
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
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            Some("intermediate-key".to_owned()),
            vec!["/tmp/input/inter_mediate".to_owned()],
        ).unwrap();
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.get_assigned_worker_id(), "");

        reduce_task.set_assigned_worker_id("worker-1".to_owned());
        assert_eq!(reduce_task.get_assigned_worker_id(), "worker-1");
    }

    #[test]
    fn test_set_status() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
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
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
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

        let map_reduce_job = MapReduceJob::new(
            "test-client".to_owned(),
            "/tmp/bin".to_owned(),
            test_path.to_str().unwrap().to_owned(),
        );

        let map_tasks: Vec<MapReduceTask> =
            task_processor.create_map_tasks(&map_reduce_job).unwrap();

        assert_eq!(map_tasks.len(), 1);
        assert_eq!(map_tasks[0].get_task_type(), TaskType::Map);
        assert_eq!(
            map_tasks[0].get_map_reduce_id(),
            map_reduce_job.get_map_reduce_id()
        );
        assert_eq!(
            map_tasks[0].get_binary_path(),
            map_reduce_job.get_binary_path()
        );
        assert_eq!(map_tasks[0].input_files.len(), 1);

        // Read map task input and make sure it is as expected.
        let mut input_file = fs::File::open(map_tasks[0].input_files[0].clone()).unwrap();
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

        let map_reduce_job = MapReduceJob::new(
            "test-client".to_owned(),
            "/tmp/bin".to_owned(),
            "/tmp/inputdir".to_owned(),
        );

        let mut map_task1 = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        map_task1.push_output_file("intermediate-key1".to_owned(), "/tmp/output/1".to_owned());
        map_task1.push_output_file("intermediate-key2".to_owned(), "/tmp/output/2".to_owned());

        let mut map_task2 = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            None,
            vec!["/tmp/input/".to_owned()],
        ).unwrap();

        map_task2.push_output_file("intermediate-key1".to_owned(), "/tmp/output/3".to_owned());

        let map_tasks: Vec<MapReduceTask> = vec![map_task1, map_task2];
        let mut reduce_tasks: Vec<MapReduceTask> = task_processor
            .create_reduce_tasks(&map_reduce_job, &map_tasks)
            .unwrap();

        reduce_tasks.sort_by_key(|task| task.get_input_key().unwrap());

        assert_eq!(2, reduce_tasks.len());

        assert_eq!(
            "intermediate-key1",
            reduce_tasks[0].get_input_key().unwrap()
        );
        assert_eq!(TaskType::Reduce, reduce_tasks[0].get_task_type());
        assert_eq!(
            map_reduce_job.get_map_reduce_id(),
            reduce_tasks[0].get_map_reduce_id()
        );
        assert_eq!(
            vec!["/tmp/output/1", "/tmp/output/3"],
            reduce_tasks[0].get_input_files()
        );

        assert_eq!(
            "intermediate-key2",
            reduce_tasks[1].get_input_key().unwrap()
        );
        assert_eq!(TaskType::Reduce, reduce_tasks[1].get_task_type());
        assert_eq!(
            map_reduce_job.get_map_reduce_id(),
            reduce_tasks[1].get_map_reduce_id()
        );
        assert_eq!(vec!["/tmp/output/2"], reduce_tasks[1].get_input_files());
    }
}
