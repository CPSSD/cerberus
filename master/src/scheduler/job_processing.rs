/// `job_processing` is responsible for processing a [`Job`](common::Job) into a set of
/// [`Task`s](common::Task).
///
/// It also handles the management of Job metadata, such as its status, the time it was started,
/// etc.

// TODO(tbolt): Refactor the contents of these functions after the scheduler refactor is complete.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use chrono::Utc;

use cerberus_proto::mapreduce::Status as JobStatus;
use common::{Job, Task};
use errors::*;

const MEBIBYTE: usize = 1024 * 1024;
const MAP_INPUT_SIZE: usize = MEBIBYTE * 64;

struct MapTaskFile {
    task_num: u32,
    bytes_to_write: usize,

    file: fs::File,
    file_path: String,
}

pub fn activate_job(mut job: Job) -> Job {
    info!("Activating job with ID {}.", job.id);
    job.status = JobStatus::IN_PROGRESS;
    job.time_started = Some(Utc::now());
    job
}

pub fn complete_job(mut job: Job) -> Job {
    info!("Job with ID {} completed.", job.id);
    job.status = JobStatus::DONE;
    job.time_completed = Some(Utc::now());
    job
}

pub fn create_map_tasks(job: &Job) -> Result<Vec<Task>> {
    let mut map_tasks = Vec::new();
    let input_directory = PathBuf::from(job.input_directory.as_str());

    let mut output_directory = input_directory.clone();
    output_directory.push(".MapReduceTasks");
    fs::create_dir_all(output_directory.clone()).chain_err(
        || "Error creating tasks output directory.",
    )?;

    let mut map_task_file: MapTaskFile = create_new_task_file(1, &output_directory).chain_err(
        || "Error creating new Map input file chunk.",
    )?;

    for entry in fs::read_dir(job.input_directory.as_str())? {
        let path = entry
            .chain_err(|| "Failed to access directory entry.")?
            .path();
        if path.is_file() {
            let file = fs::File::open(&path).chain_err(|| {
                format!("Error opening input file {:?}", &path)
            })?;
            read_input_file(
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

pub fn create_reduce_tasks(job: &Job, completed_map_tasks: Vec<Task>) -> Result<Vec<Task>> {
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

/// `create_new_task_file` creates a new file that will contain one chunk of the map input.
fn create_new_task_file(task_num: u32, output_directory: &PathBuf) -> Result<(MapTaskFile)> {
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

            *map_task_file = create_new_task_file(map_task_file.task_num + 1, output_directory)
                .chain_err(|| "Error creating Map input chunk file.")?;
        } else {
            map_task_file.bytes_to_write -= ammount_read;
        }
    }
    Ok(())
}
