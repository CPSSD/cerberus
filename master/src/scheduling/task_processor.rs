use std::collections::HashMap;
use std::cmp::max;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cerberus_proto::worker as pb;
use common::{Job, Task};
use util::data_layer::AbstractionLayer;
use errors::*;

const MEGA_BYTE: u64 = 1000 * 1000;
const MAP_INPUT_SIZE: u64 = MEGA_BYTE * 64;
const CLOSEST_ENDLINE_STEP: u64 = 1000;
const NEWLINE: u8 = 0x0A;

#[derive(Clone)]
struct MapTaskInformation {
    bytes_remaining: u64,

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

    /// `get_closest_endline` files the endline closest to the end of a given range for input file
    /// splitting.
    fn get_closest_endline(
        &self,
        input_file_path: &PathBuf,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<u64> {
        let mut current_byte = end_byte;
        while current_byte > start_byte {
            let new_current_byte = max(start_byte, current_byte - CLOSEST_ENDLINE_STEP);

            let bytes = self.data_abstraction_layer
                .read_file_location(input_file_path, new_current_byte, current_byte)
                .chain_err(|| "Error getting closest newline")?;

            current_byte = new_current_byte;

            for i in (0..bytes.len()).rev() {
                if bytes[i] == NEWLINE {
                    // Start next chunk after newline.
                    return Ok(current_byte + (i as u64) + 1);
                }
            }
        }

        // Could not find a newline, just split at the end of the chunk.
        Ok(end_byte)
    }

    /// `read_input_file` reads a given input file and splits it into chunks for map tasks.
    /// If a file can fit into one map task, it will not be split.
    fn read_input_file(&self, input_file_path: &PathBuf) -> Result<Vec<pb::InputLocation>> {
        let input_path_str = input_file_path.to_str().ok_or("Invalid input file path.")?;

        let mut input_locations = Vec::new();

        let mut start_byte: u64 = 0;
        let end_byte: u64 = self.data_abstraction_layer
            .get_file_length(input_file_path)
            .chain_err(|| "Error reading input file")?;

        while end_byte - start_byte > MAP_INPUT_SIZE {
            let new_start_byte =
                self.get_closest_endline(input_file_path, start_byte, start_byte + MAP_INPUT_SIZE)
                    .chain_err(|| "Error reading input file")?;
            start_byte = new_start_byte;

            let mut input_location = pb::InputLocation::new();
            input_location.set_input_path(input_path_str.to_owned());
            input_location.set_start_byte(start_byte);
            input_location.set_end_byte(new_start_byte);
            input_locations.push(input_location);
        }

        if start_byte != end_byte {
            let mut input_location = pb::InputLocation::new();
            input_location.set_input_path(input_path_str.to_owned());
            input_location.set_start_byte(start_byte);
            input_location.set_end_byte(end_byte);
            input_locations.push(input_location);
        }

        Ok(input_locations)
    }

    /// `get_map_task_infos` reads a directory and creates a set of `MapTaskFileInformations`
    fn get_map_task_infos(&self, input_directory: &Path) -> Result<Vec<MapTaskInformation>> {
        let mut map_task_infos = Vec::new();

        let mut map_task_info = MapTaskInformation {
            bytes_remaining: MAP_INPUT_SIZE,

            input_locations: Vec::new(),
        };

        let paths = self.data_abstraction_layer
            .read_dir(input_directory)
            .chain_err(|| "Unable to read directory")?;

        if paths.is_empty() {
            return Err("No files in map input directory".into());
        }

        for path in paths {
            if self.data_abstraction_layer
                .is_file(path.as_path())
                .chain_err(|| "Failed to check if path is a file")?
            {
                let input_locations = self.read_input_file(&path).chain_err(
                    || "Error reading input file.",
                )?;

                for input_location in input_locations {
                    let bytes_to_read = input_location.end_byte - input_location.start_byte;
                    if bytes_to_read > map_task_info.bytes_remaining {
                        map_task_infos.push(map_task_info);

                        map_task_info = MapTaskInformation {
                            bytes_remaining: MAP_INPUT_SIZE,
                            input_locations: Vec::new(),
                        };
                    }

                    map_task_info.bytes_remaining -= bytes_to_read;
                    map_task_info.input_locations.push(input_location);
                }
            }
        }

        if map_task_info.bytes_remaining != MAP_INPUT_SIZE {
            map_task_infos.push(map_task_info);
        }

        Ok(map_task_infos)
    }
}

impl TaskProcessor for TaskProcessorImpl {
    fn create_map_tasks(&self, job: &Job) -> Result<Vec<Task>> {
        let map_task_infos = self.get_map_task_infos(Path::new(&job.input_directory))
            .chain_err(|| "Error creating map tasks")?;

        // TODO(conor): Consider adding together any map tasks that can be combined here.

        let mut map_tasks = Vec::new();

        for map_task_info in map_task_infos {
            map_tasks.push(Task::new_map_task(
                job.id.as_str(),
                job.binary_path.as_str(),
                map_task_info.input_locations,
                job.priority,
            ));
        }

        if map_tasks.is_empty() {
            return Err("No map tasks created for job".into());
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
                job.priority,
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
    use std::fs::File;
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
        let mut input_file1 = File::create(&input_path1.as_path()).unwrap();
        input_file1
            .write_all(b"this is the first test file")
            .unwrap();
        let mut input_file2 = File::create(&input_path2.as_path()).unwrap();
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
        let mut input_file0 = File::open(&path).unwrap();
        let mut map_input0 = String::new();
        input_file0.read_to_string(&mut map_input0).unwrap();

        let path = perform_map_req.get_input().get_input_locations()[1]
            .input_path
            .clone();
        let mut input_file1 = File::open(&path).unwrap();
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

        let mut map_task1 =
            Task::new_map_task("map-1", "/tmp/bin", vec![input_location.clone()], 1);
        map_task1.map_output_files.insert(
            0,
            "/tmp/output/1".to_owned(),
        );
        map_task1.map_output_files.insert(
            1,
            "/tmp/output/2".to_owned(),
        );

        let mut map_task2 = Task::new_map_task("map-2", "/tmp/bin", vec![input_location], 1);
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
