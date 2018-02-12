use std::collections::HashMap;
use std::fs;
use std::io::{Write, BufRead, BufReader};
use std::path::PathBuf;

use common::{Job, Task};
use errors::*;

const MEGA_BYTE: usize = 1000 * 1000;
const MAP_INPUT_SIZE: usize = MEGA_BYTE * 64;

struct MapTaskFile {
    task_num: u32,
    bytes_to_write: usize,

    file: fs::File,
    file_path: String,
}

/// `TaskProcessor` describes an object that can be used to create map and reduce tasks.
pub trait TaskProcessor {
    /// `create_map_tasks` creates a set of map tasks when given a `Job`
    fn create_map_tasks(&self, job: &Job) -> Result<Vec<Task>>;

    /// `create_reduce_tasks` creates reduce tasks from a `Job` and a list of it's
    /// completed map tasks.
    fn create_reduce_tasks(&self, job: &Job, completed_map_tasks: Vec<&Task>) -> Result<Vec<Task>>;
}

pub struct TaskProcessorImpl;

impl TaskProcessorImpl {
    /// `create_new_task_file` creates a new file that will contain one chunk of the map input.
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

    /// `read_input_file` reads a given input file and splits it into chunks for map tasks.
    fn read_input_file(
        &self,
        job: &Job,
        map_task_file: &mut MapTaskFile,
        input_file: &fs::File,
        output_directory: &PathBuf,
        map_tasks: &mut Vec<Task>,
    ) -> Result<()> {
        let buf_reader = BufReader::new(input_file);
        for line in buf_reader.lines() {
            let mut read_str = line.chain_err(|| "Error reading Map input.")?;
            read_str.push_str("\n");

            map_task_file
                .file
                .write_all(read_str.as_bytes())
                .chain_err(|| "Error writing to Map input chunk file.")?;

            let ammount_read: usize = read_str.len();
            if ammount_read > map_task_file.bytes_to_write {
                map_tasks.push(Task::new_map_task(
                    job.id.as_str(),
                    job.binary_path.as_str(),
                    &map_task_file.file_path,
                ));

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

impl TaskProcessor for TaskProcessorImpl {
    fn create_map_tasks(&self, job: &Job) -> Result<Vec<Task>> {
        let mut map_tasks = Vec::new();
        let input_directory = PathBuf::from(job.input_directory.as_str());

        let mut output_directory: PathBuf = input_directory.clone();
        output_directory.push(".MapReduceTasks");
        fs::create_dir_all(output_directory.clone()).chain_err(
            || "Error creating Map tasks output directory.",
        )?;

        let mut map_task_file: MapTaskFile =
            self.create_new_task_file(1, &output_directory).chain_err(
                || "Error creating new Map input file chunk.",
            )?;

        for entry in fs::read_dir(job.input_directory.as_str())? {
            let entry: fs::DirEntry = entry.chain_err(|| "Error reading input directory.")?;
            let path: PathBuf = entry.path();
            if path.is_file() {
                let file = fs::File::open(path).chain_err(
                    || "Error opening input file.",
                )?;
                self.read_input_file(
                    job,
                    &mut map_task_file,
                    &file,
                    &output_directory,
                    &mut map_tasks,
                ).chain_err(|| "Error reading input file.")?;
            }
        }
        if map_task_file.bytes_to_write != MAP_INPUT_SIZE {
            map_tasks.push(Task::new_map_task(
                job.id.as_str(),
                job.binary_path.as_str(),
                &map_task_file.file_path,
            ));
        }
        Ok(map_tasks)
    }

    fn create_reduce_tasks(&self, job: &Job, completed_map_tasks: Vec<&Task>) -> Result<Vec<Task>> {
        let mut reduce_tasks = Vec::new();
        let mut key_results_map: HashMap<u64, Vec<String>> = HashMap::new();

        for completed_map in completed_map_tasks {
            for (partition, output_file) in &completed_map.map_output_files {
                let map_results: &mut Vec<String> =
                    key_results_map.entry(*partition).or_insert_with(Vec::new);
                map_results.push(output_file.to_owned());
            }
        }

        for (reduce_partition, reduce_input) in key_results_map {
            reduce_tasks.push(Task::new_reduce_task(
                job.id.as_str(),
                job.binary_path.as_str(),
                reduce_partition,
                reduce_input,
                job.output_directory.as_str(),
            ));
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
    use common::{JobOptions, TaskType};

    #[test]
    fn test_create_map_tasks() {
        let task_processor = TaskProcessorImpl;

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

        let job = Job::new(JobOptions {
            client_id: "test-client".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: test_path.to_str().unwrap().to_owned(),
            ..Default::default()
        }).unwrap();

        let map_tasks: Vec<Task> = task_processor.create_map_tasks(&job).unwrap();

        assert_eq!(map_tasks.len(), 1);
        assert_eq!(map_tasks[0].task_type, TaskType::Map);
        assert_eq!(map_tasks[0].job_id, job.id);

        let perform_map_req = map_tasks[0].map_request.clone().unwrap();

        assert_eq!(job.binary_path, perform_map_req.get_mapper_file_path());

        // Read map task input and make sure it is as expected.
        let mut input_file = fs::File::open(perform_map_req.get_input_file_path().clone()).unwrap();
        let mut map_input = String::new();
        input_file.read_to_string(&mut map_input).unwrap();

        // Either input file order is fine.
        let mut good_inputs = HashSet::new();
        good_inputs.insert(
            "this is the first test file\nthis is the second test file\n".to_owned(),
        );
        good_inputs.insert(
            "this is the second test file\nthis is the first test file\n".to_owned(),
        );

        assert!(good_inputs.contains(&map_input));
    }

    #[test]
    fn test_create_reduce_tasks() {
        let task_processor = TaskProcessorImpl;

        let job = Job::new(JobOptions {
            client_id: "test-client".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/inputdir".to_owned(),
            ..Default::default()
        }).unwrap();

        let mut map_task1 = Task::new_map_task("map-1", "/tmp/bin", "/tmp/input/");
        map_task1.map_output_files.insert(
            0,
            "/tmp/output/1".to_owned(),
        );
        map_task1.map_output_files.insert(
            1,
            "/tmp/output/2".to_owned(),
        );

        let mut map_task2 = Task::new_map_task("map-2", "/tmp/bin", "/tmp/input/");
        map_task2.map_output_files.insert(
            0,
            "/tmp/output/3".to_owned(),
        );

        let map_tasks: Vec<&Task> = vec![&map_task1, &map_task2];
        let mut reduce_tasks: Vec<Task> =
            task_processor.create_reduce_tasks(&job, map_tasks).unwrap();

        reduce_tasks.sort_by_key(|task| task.reduce_request.clone().unwrap().get_partition());

        assert_eq!(2, reduce_tasks.len());

        let reduce_req1 = reduce_tasks[0].reduce_request.clone().unwrap();
        let reduce_req2 = reduce_tasks[1].reduce_request.clone().unwrap();

        assert_eq!(0, reduce_req1.get_partition());
        assert_eq!(TaskType::Reduce, reduce_tasks[0].task_type);
        assert_eq!(job.id, reduce_tasks[0].job_id);
        assert_eq!(
            vec!["/tmp/output/1", "/tmp/output/3"],
            reduce_req1.get_input_file_paths()
        );

        assert_eq!(1, reduce_req2.get_partition());
        assert_eq!(TaskType::Reduce, reduce_tasks[1].task_type);
        assert_eq!(job.id, reduce_tasks[1].job_id);
        assert_eq!(vec!["/tmp/output/2"], reduce_req2.get_input_file_paths());
    }
}
