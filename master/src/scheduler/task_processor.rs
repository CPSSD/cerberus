use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cerberus_proto::worker as pb;
use common::{Job, Task};
use util::data_layer::AbstractionLayer;
use errors::*;

const MEGA_BYTE: usize = 1000 * 1000;
const MAP_INPUT_SIZE: usize = MEGA_BYTE * 64;

struct MapTaskFileInformation {
    task_num: u32,
    bytes_to_write: usize,

    input_locations: Vec<pb::InputLocation>,
}

/// `TaskProcessor` describes an object that can be used to create map and reduce tasks.
pub trait TaskProcessor {
    /// `create_map_tasks` creates a set of map tasks when given a `Job`
    fn create_map_tasks(&self, job: &Job) -> Result<Vec<Task>>;

    /// `create_reduce_tasks` creates reduce tasks from a `Job` and a list of it's
    /// completed map tasks.
    fn create_reduce_tasks(&self, job: &Job, completed_map_tasks: Vec<&Task>) -> Result<Vec<Task>>;
}

pub struct TaskProcessorImpl {
    data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
}

impl TaskProcessorImpl {
    pub fn new(data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>) -> Self {
        TaskProcessorImpl { data_abstraction_layer: data_abstraction_layer }
    }

    /// `read_input_file` reads a given input file and splits it into chunks for map tasks.
    fn read_input_file(
        &self,
        job: &Job,
        map_task_file_info: &mut MapTaskFileInformation,
        input_file_path: &PathBuf,
        map_tasks: &mut Vec<Task>,
    ) -> Result<()> {
        let input_path_str = input_file_path.to_str().ok_or("Invalid input file path.")?;

        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path(input_path_str.to_owned());
        input_location.set_start_byte(0);

        let input_file = self.data_abstraction_layer
            .open_file(input_file_path)
            .chain_err(|| "Error opening input file.")?;

        let mut bytes_read: usize = 0;

        let buf_reader = BufReader::new(input_file);
        for line in buf_reader.lines() {
            let mut read_str = line.chain_err(|| "Error reading Map input.")?;
            bytes_read += read_str.len() + 1; // Add one byte for newline character

            input_location.set_end_byte(bytes_read as u64);

            let ammount_read: usize = read_str.len();
            if ammount_read > map_task_file_info.bytes_to_write {
                map_task_file_info.input_locations.push(input_location);

                input_location = pb::InputLocation::new();
                input_location.set_input_path(input_path_str.to_owned());
                input_location.set_start_byte(bytes_read as u64 + 1);
                input_location.set_end_byte(bytes_read as u64 + 1);

                map_tasks.push(Task::new_map_task(
                    job.id.as_str(),
                    job.binary_path.as_str(),
                    map_task_file_info.input_locations.clone(),
                ));

                *map_task_file_info = MapTaskFileInformation {
                    task_num: map_task_file_info.task_num + 1,
                    bytes_to_write: MAP_INPUT_SIZE,

                    input_locations: Vec::new(),
                };
            } else {
                map_task_file_info.bytes_to_write -= ammount_read;
            }
        }

        if input_location.start_byte != input_location.end_byte {
            map_task_file_info.input_locations.push(input_location);
        }

        Ok(())
    }
}

impl TaskProcessor for TaskProcessorImpl {
    fn create_map_tasks(&self, job: &Job) -> Result<Vec<Task>> {
        let mut map_tasks = Vec::new();

        let mut map_task_file_info = MapTaskFileInformation {
            task_num: 1,
            bytes_to_write: MAP_INPUT_SIZE,

            input_locations: Vec::new(),
        };

        let paths = self.data_abstraction_layer
            .read_dir(Path::new(&job.input_directory))
            .chain_err(|| "Unable to read directory")?;
        for path in paths {
            if self.data_abstraction_layer
                .is_file(path.as_path())
                .chain_err(|| "Failed to check if path is a file")?
            {
                self.read_input_file(job, &mut map_task_file_info, &path, &mut map_tasks)
                    .chain_err(|| "Error reading input file.")?;
            }
        }

        if map_task_file_info.bytes_to_write != MAP_INPUT_SIZE {
            map_tasks.push(Task::new_map_task(
                job.id.as_str(),
                job.binary_path.as_str(),
                map_task_file_info.input_locations,
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
    use std::io::{Read, Write};
    use std::collections::HashSet;
    use std::fs;
    use common::{JobOptions, TaskType};
    use util::data_layer::NullAbstractionLayer;

    #[test]
    fn test_create_map_tasks() {
        let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync> =
            Arc::new(NullAbstractionLayer);
        let task_processor = TaskProcessorImpl::new(Arc::clone(&data_abstraction_layer));

        let test_path = Path::new("/tmp/cerberus/create_task_test/").to_path_buf();
        let mut input_path1 = test_path.clone();
        input_path1.push("input-1");
        let mut input_path2 = test_path.clone();
        input_path2.push("input-2");

        fs::create_dir_all(test_path.clone()).unwrap();
        let mut input_file1 = task_processor
            .data_abstraction_layer
            .create_file(input_path1.as_path().clone())
            .unwrap();
        input_file1
            .write_all(b"this is the first test file")
            .unwrap();
        let mut input_file2 = task_processor
            .data_abstraction_layer
            .create_file(input_path2.as_path().clone())
            .unwrap();
        input_file2
            .write_all(b"this is the second test file")
            .unwrap();

        let job = Job::new(
            JobOptions {
                client_id: "test-client".to_owned(),
                binary_path: "/tmp/bin".to_owned(),
                input_directory: test_path.to_str().unwrap().to_owned(),
                ..Default::default()
            },
            &data_abstraction_layer,
        ).unwrap();

        let map_tasks: Vec<Task> = task_processor.create_map_tasks(&job).unwrap();

        assert_eq!(map_tasks.len(), 1);
        assert_eq!(map_tasks[0].task_type, TaskType::Map);
        assert_eq!(map_tasks[0].job_id, job.id);

        let perform_map_req = map_tasks[0].map_request.clone().unwrap();

        assert_eq!(job.binary_path, perform_map_req.get_mapper_file_path());

        // Read map task input and make sure it is as expected.
        let path = perform_map_req.get_input().get_input_locations()[0]
            .input_path
            .clone();
        let mut input_file0 = task_processor
            .data_abstraction_layer
            .open_file(&Path::new(&path))
            .unwrap();
        let mut map_input0 = String::new();
        input_file0.read_to_string(&mut map_input0).unwrap();

        let path = perform_map_req.get_input().get_input_locations()[1]
            .input_path
            .clone();
        let mut input_file1 = task_processor
            .data_abstraction_layer
            .open_file(&Path::new(&path))
            .unwrap();
        let mut map_input1 = String::new();
        input_file1.read_to_string(&mut map_input1).unwrap();

        let map_input = map_input0 + "\n" + &map_input1;

        // Either input file order is fine.
        let mut good_inputs = HashSet::new();
        good_inputs.insert(
            "this is the first test file\nthis is the second test file".to_owned(),
        );
        good_inputs.insert(
            "this is the second test file\nthis is the first test file".to_owned(),
        );

        println!("{}", map_input.clone());

        assert!(good_inputs.contains(&map_input));
    }

    #[test]
    fn test_create_reduce_tasks() {
        let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync> =
            Arc::new(NullAbstractionLayer);
        let task_processor = TaskProcessorImpl::new(data_abstraction_layer);

        let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync> =
            Arc::new(NullAbstractionLayer);
        let job = Job::new(
            JobOptions {
                client_id: "test-client".to_owned(),
                binary_path: "/tmp/bin".to_owned(),
                input_directory: "/tmp/inputdir".to_owned(),
                ..Default::default()
            },
            &data_abstraction_layer,
        ).unwrap();

        let mut input_location = pb::InputLocation::new();
        input_location.set_input_path("/tmp/input/".to_owned());
        input_location.set_start_byte(0);
        input_location.set_end_byte(0);

        let mut map_task1 = Task::new_map_task("map-1", "/tmp/bin", vec![input_location.clone()]);
        map_task1.map_output_files.insert(
            0,
            "/tmp/output/1".to_owned(),
        );
        map_task1.map_output_files.insert(
            1,
            "/tmp/output/2".to_owned(),
        );

        let mut map_task2 = Task::new_map_task("map-2", "/tmp/bin", vec![input_location]);
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
