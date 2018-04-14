use std::collections::HashMap;
use std::fs;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use bson;
use futures::future;
use futures::Future;
use futures_cpupool::{CpuFuture, CpuPool};
use serde_json;
use uuid::Uuid;

use super::combine;
use super::io;
use super::operation_handler;
use super::operation_handler::{OperationResources, PartitionMap};
use super::state::OperationState;
use cerberus_proto::worker as pb;
use communication::MasterInterface;
use errors::*;
use util::output_error;

const WORKER_OUTPUT_DIRECTORY: &str = "/tmp/cerberus/";

/// The `MapInput` is a struct used for serialising input data for a map operation.
#[derive(Default, Serialize)]
pub struct MapInput {
    pub key: String,
    pub value: String,
}

fn log_map_operation_err(
    err: Error,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    task_id: &str,
) {
    output_error(&err.chain_err(|| "Error running map operation."));
    operation_handler::set_failed_status(operation_state_arc, task_id);
}

fn send_map_result(
    master_interface_arc: &Arc<MasterInterface>,
    map_result: pb::MapResult,
) -> Result<()> {
    master_interface_arc
        .return_map_result(map_result)
        .chain_err(|| "Error sending map result to master.")?;
    Ok(())
}

fn parse_map_results(map_result_string: &str, partition_map: &mut PartitionMap) -> Result<()> {
    let parse_value: serde_json::Value =
        serde_json::from_str(map_result_string).chain_err(|| "Error parsing map response.")?;

    let partition_map_object = match parse_value["partitions"].as_object() {
        None => return Err("Error parsing partition map.".into()),
        Some(map) => map,
    };

    for (partition_str, pairs) in partition_map_object.iter() {
        let partition: u64 = partition_str
            .to_owned()
            .parse()
            .chain_err(|| "Error parsing map response.")?;
        let partition_hashmap = partition_map.entry(partition).or_insert_with(HashMap::new);
        if let serde_json::Value::Array(ref pairs) = *pairs {
            for pair in pairs {
                let key_str = pair["key"].to_string();
                let value_vec = partition_hashmap.entry(key_str).or_insert_with(Vec::new);
                value_vec.push(pair["value"].clone());
            }
        }
    }

    Ok(())
}

fn map_operation_thread_impl(map_input_value: &bson::Document, mut child: Child) -> Result<String> {
    let mut input_buf = Vec::new();
    bson::encode_document(&mut input_buf, map_input_value)
        .chain_err(|| "Could not encode map_input as BSON.")?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(&input_buf[..])
            .chain_err(|| "Error writing to payload stdin.")?;
    } else {
        return Err("Error accessing stdin of payload binary.".into());
    }

    let output = child
        .wait_with_output()
        .chain_err(|| "Error waiting for payload result.")?;

    let output_str =
        String::from_utf8(output.stdout).chain_err(|| "Error accessing payload output.")?;

    let stderr_str =
        String::from_utf8(output.stderr).chain_err(|| "Error accessing payload output.")?;

    if !stderr_str.is_empty() {
        return Err(format!("MapReduce binary failed with stderr:\n {}", stderr_str).into());
    }

    Ok(output_str)
}

pub fn perform_map(
    map_options: &pb::PerformMapRequest,
    resources: &OperationResources,
    output_dir_uuid: &str,
) -> Result<()> {
    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        operation_state.initial_cpu_time = operation_handler::get_cpu_time();
    }

    info!(
        "Performing map operation. mapper={} number of inputs={}",
        map_options.mapper_file_path,
        map_options.get_input().get_input_locations().len()
    );

    {
        let mut state = resources.operation_state.lock().unwrap();
        if state.current_task_id != "" {
            warn!("Map operation requested while worker is busy");
            return Err("Worker is busy.".into());
        }

        state.current_task_id = map_options.task_id.clone();
        state.operation_status = pb::OperationStatus::IN_PROGRESS;
    }

    let result = internal_perform_map(map_options, resources, output_dir_uuid);
    if let Err(err) = result {
        log_map_operation_err(err, &resources.operation_state, &map_options.task_id);
        return Err("Error starting map operation.".into());
    }

    Ok(())
}

fn combine_map_results(
    resources: &OperationResources,
    initial_cpu_time: u64,
    output_dir: &str,
    task_id: &str,
    map_results_vec: Vec<String>,
) -> Result<()> {
    if operation_handler::check_task_cancelled(&resources.operation_state, task_id) {
        return Ok(());
    }

    // Map of partition to maps of key to value.
    let mut partition_map: PartitionMap = HashMap::new();
    for map_result in map_results_vec {
        parse_map_results(&map_result, &mut partition_map)
            .chain_err(|| "Error parsing map result")?;
    }

    combine::optional_run_combine(resources, &mut partition_map)
        .chain_err(|| "Error running combine operation.")?;

    let mut map_result_files: HashMap<u64, String> = HashMap::new();
    for (partition, values) in partition_map {
        let file_name = Uuid::new_v4().to_string();
        let mut file_path = PathBuf::new();
        file_path.push(output_dir);
        file_path.push(&file_name);

        let json_values = json!(values);
        io::write_local(file_path.clone(), json_values.to_string().as_bytes())
            .chain_err(|| "failed to write map output")?;

        map_result_files.insert(partition, (*file_path.to_string_lossy()).to_owned());
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_map_results(map_result_files);
    map_result.set_status(pb::ResultStatus::SUCCESS);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    map_result.set_task_id(task_id.to_owned());

    if let Err(err) = send_map_result(&resources.master_interface, map_result) {
        log_map_operation_err(err, &resources.operation_state, task_id);
    } else {
        info!("Map operation completed sucessfully.");
        operation_handler::set_complete_status(&resources.operation_state, task_id);
    }

    Ok(())
}

fn process_map_operation_error(
    err: Error,
    resources: &OperationResources,
    initial_cpu_time: u64,
    task_id: &str,
) {
    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        if operation_state.current_task_id != task_id {
            // Already processed an error, no need to send status again.
            return;
        }
        operation_state.current_task_id = String::new();
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_status(pb::ResultStatus::FAILURE);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    map_result.set_failure_details(operation_handler::failure_details_from_error(&err));
    map_result.set_task_id(task_id.to_owned());

    if let Err(err) = send_map_result(&resources.master_interface, map_result) {
        error!("Could not send map operation failed: {}", err);
    }
    log_map_operation_err(err, &resources.operation_state, task_id);
}

fn run_map_input(
    input_location: &pb::InputLocation,
    map_options: &pb::PerformMapRequest,
    resources: &OperationResources,
) -> Result<String> {
    if operation_handler::check_task_cancelled(&resources.operation_state, &map_options.task_id) {
        return Ok(String::new());
    }

    info!(
        "Running map task for {} ({} - > {})",
        input_location.input_path, input_location.start_byte, input_location.end_byte
    );

    let map_input_value = io::read_location(&resources.data_abstraction_layer, input_location)
        .chain_err(|| "unable to open input file")?;

    let absolute_path = resources
        .data_abstraction_layer
        .get_local_file(Path::new(map_options.get_mapper_file_path()))
        .chain_err(|| "Unable to get absolute path")?;

    let child = Command::new(absolute_path)
        .arg("map")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .chain_err(|| "Failed to start map operation process.")?;

    let map_input = MapInput {
        key: input_location.get_input_path().to_owned(),
        value: map_input_value,
    };

    let serialized_map_input =
        bson::to_bson(&map_input).chain_err(|| "Could not serialize map input to bson.")?;

    let map_input_document;
    if let bson::Bson::Document(document) = serialized_map_input {
        map_input_document = document;
    } else {
        return Err("Could not convert map input to bson::Document.".into());
    }

    map_operation_thread_impl(&map_input_document, child)
}

fn handle_map_results(
    map_options: &pb::PerformMapRequest,
    initial_cpu_time: u64,
    resources: &OperationResources,
    output_dir_uuid: &str,
    map_result_futures: Vec<CpuFuture<String, String>>,
) {
    let mut output_path = PathBuf::new();
    output_path.push(WORKER_OUTPUT_DIRECTORY);
    output_path.push(output_dir_uuid);
    output_path.push("map");
    let output_path_str: String = (*output_path.to_string_lossy()).to_owned();

    let results_future = future::join_all(map_result_futures);
    let resources = resources.to_owned();
    let map_options = map_options.to_owned();

    thread::spawn(move || {
        let map_results = results_future.wait();
        if let Ok(map_output) = map_results {
            let combine_result = combine_map_results(
                &resources,
                initial_cpu_time,
                &output_path_str,
                &map_options.task_id,
                map_output,
            );

            if let Err(err) = combine_result {
                process_map_operation_error(
                    err,
                    &resources,
                    initial_cpu_time,
                    &map_options.task_id,
                );
            }
        }
    });
}

// Internal implementation for performing a map task.
fn internal_perform_map(
    map_options: &pb::PerformMapRequest,
    resources: &OperationResources,
    output_dir_uuid: &str,
) -> Result<()> {
    let mut output_path = PathBuf::new();
    output_path.push(WORKER_OUTPUT_DIRECTORY);
    output_path.push(output_dir_uuid);
    output_path.push("map");

    fs::create_dir_all(&output_path).chain_err(|| "Failed to create output directory")?;

    let input_locations = map_options.get_input().get_input_locations();
    let initial_cpu_time;

    {
        let operation_state = resources.operation_state.lock().unwrap();
        initial_cpu_time = operation_state.initial_cpu_time;
    }

    let cpu_pool = CpuPool::new_num_cpus();
    let mut map_result_futures = Vec::new();
    for input_location in input_locations {
        let resources = resources.to_owned();
        let map_options = map_options.to_owned();
        let input_location = input_location.to_owned();

        let map_result_future = cpu_pool.spawn_fn(move || {
            let map_result = run_map_input(&input_location, &map_options, &resources);

            match map_result {
                Ok(map_output) => future::ok(map_output),
                Err(err) => {
                    process_map_operation_error(
                        err,
                        &resources,
                        initial_cpu_time,
                        &map_options.task_id,
                    );
                    future::err::<String, String>("Running map input failed".into())
                }
            }
        });

        map_result_futures.push(map_result_future);
    }

    handle_map_results(
        map_options,
        initial_cpu_time,
        resources,
        output_dir_uuid,
        map_result_futures,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mocktopus::mocking::*;

    #[test]
    fn test_parse_map_results() {
        io::write_local.mock_safe(|_: PathBuf, _| MockResult::Return(Ok(())));

        let map_results =
        r#"{"partitions":{"1":[{"key":"zar","value":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;
        let mut partition_map: PartitionMap = HashMap::new();

        parse_map_results(map_results, &mut partition_map).unwrap();
        assert_eq!(partition_map.len(), 2);
    }

    #[test]
    fn test_parse_map_results_error() {
        io::write_local.mock_safe(|_: PathBuf, _| MockResult::Return(Ok(())));

        let map_results =
            r#"{:{"1":[{"key":"zavalue":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;
        let mut partition_map: PartitionMap = HashMap::new();

        let result = parse_map_results(map_results, &mut partition_map);
        assert!(result.is_err());
    }
}
