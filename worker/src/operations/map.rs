use std::collections::HashMap;
use std::fs;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use bson;
use serde_json;
use uuid::Uuid;

use errors::*;
use cerberus_proto::worker as pb;
use master_interface::MasterInterface;
use super::combine;
use super::io;
use super::operation_handler;
use super::operation_handler::OperationResources;
use super::state::OperationState;
use util::output_error;

const WORKER_OUTPUT_DIRECTORY: &str = "/tmp/cerberus/";

/// The `MapInput` is a struct used for serialising input data for a map operation.
#[derive(Default, Serialize)]
pub struct MapInput {
    pub key: String,
    pub value: String,
}

fn log_map_operation_err(err: Error, operation_state_arc: &Arc<Mutex<OperationState>>) {
    output_error(&err.chain_err(|| "Error running map operation."));
    operation_handler::set_failed_status(operation_state_arc);
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

/// `IntermediateMapResults` is a `HashMap` mapping a partition to a JSON value containing
/// the output for that partition.
type IntermediateMapResults = HashMap<u64, serde_json::Value>;

fn parse_map_results(map_result_string: &str) -> Result<IntermediateMapResults> {
    let parse_value: serde_json::Value = serde_json::from_str(map_result_string).chain_err(
        || "Error parsing map response.",
    )?;

    let mut map_results: IntermediateMapResults = HashMap::new();

    let partition_map = match parse_value["partitions"].as_object() {
        None => return Err("Error parsing partition map.".into()),
        Some(map) => map,
    };

    for (partition_str, pairs) in partition_map.iter() {
        let partition: u64 = partition_str.to_owned().parse().chain_err(
            || "Error parsing map response.",
        )?;
        map_results.insert(partition, pairs.clone());
    }

    Ok(map_results)
}

fn map_operation_thread_impl(
    map_input_value: &bson::Document,
    mut child: Child,
) -> Result<IntermediateMapResults> {
    let mut input_buf = Vec::new();
    bson::encode_document(&mut input_buf, map_input_value)
        .chain_err(|| "Could not encode map_input as BSON.")?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(&input_buf[..]).chain_err(
            || "Error writing to payload stdin.",
        )?;
    } else {
        return Err("Error accessing stdin of payload binary.".into());
    }

    let output = child.wait_with_output().chain_err(
        || "Error waiting for payload result.",
    )?;

    let output_str = String::from_utf8(output.stdout).chain_err(
        || "Error accessing payload output.",
    )?;

    let stderr_str = String::from_utf8(output.stderr).chain_err(
        || "Error accessing payload output.",
    )?;

    if !stderr_str.is_empty() {
        return Err(
            format!("MapReduce binary failed with stderr:\n {}", stderr_str).into(),
        );
    }

    let map_results = parse_map_results(&output_str).chain_err(
        || "Error parsing map results.",
    )?;

    Ok(map_results)
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

    let input_files: Vec<String> = map_options
        .get_input()
        .get_input_locations()
        .into_iter()
        .map(|loc| loc.input_path.clone())
        .collect();


    info!(
        "Performing map operation. mapper={} number of inputs={}",
        map_options.mapper_file_path,
        map_options.get_input().get_input_locations().len()
    );

    debug!("Input files: {:?}", input_files);

    if operation_handler::get_worker_status(&resources.operation_state) == pb::WorkerStatus::BUSY {
        warn!("Map operation requested while worker is busy");
        return Err("Worker is busy.".into());
    }
    operation_handler::set_busy_status(&resources.operation_state);

    let result = internal_perform_map(map_options, resources, output_dir_uuid);
    if let Err(err) = result {
        log_map_operation_err(err, &resources.operation_state);
        return Err("Error starting map operation.".into());
    }

    Ok(())
}

fn combine_map_results(
    resources: &OperationResources,
    initial_cpu_time: u64,
    output_dir: &str,
    task_id: &str,
) -> Result<()> {
    combine::optional_run_combine(resources).chain_err(
        || "Error running combine operation.",
    )?;

    let partition_map;
    {
        let operation_state = resources.operation_state.lock().unwrap();
        partition_map = operation_state.intermediate_map_results.clone();
    }

    let mut map_results: HashMap<u64, String> = HashMap::new();
    let mut intermediate_files = Vec::new();

    for (partition, values) in (&partition_map).iter() {
        let file_name = Uuid::new_v4().to_string();
        let mut file_path = PathBuf::new();
        file_path.push(output_dir);
        file_path.push(&file_name);

        let json_values = json!(values);
        io::write_local(file_path.clone(), json_values.to_string().as_bytes())
            .chain_err(|| "failed to write map output")?;
        intermediate_files.push(file_path.clone());

        map_results.insert(*partition, (*file_path.to_string_lossy()).to_owned());
    }

    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        operation_state.add_intermediate_files(intermediate_files);
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_map_results(map_results);
    map_result.set_status(pb::ResultStatus::SUCCESS);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    map_result.set_task_id(task_id.to_owned());

    if let Err(err) = send_map_result(&resources.master_interface, map_result) {
        log_map_operation_err(err, &resources.operation_state);
    } else {
        info!("Map operation completed sucessfully.");
        operation_handler::set_complete_status(&resources.operation_state);
    }

    Ok(())
}

fn process_map_operation_error(err: Error, resources: &OperationResources, initial_cpu_time: u64) {
    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        operation_state.waiting_map_operations = 0;
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_status(pb::ResultStatus::FAILURE);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    map_result.set_failure_details(operation_handler::failure_details_from_error(&err));

    if let Err(err) = send_map_result(&resources.master_interface, map_result) {
        error!("Could not send map operation failed: {}", err);
    }
    log_map_operation_err(err, &resources.operation_state);
}

fn process_map_result(
    result: Result<IntermediateMapResults>,
    resources: &OperationResources,
    initial_cpu_time: u64,
    output_dir: &str,
    task_id: &str,
) {
    {
        // The number of operations waiting will be 0 if we have already processed one
        // that has failed.
        let operation_state = resources.operation_state.lock().unwrap();
        if operation_state.waiting_map_operations == 0 {
            return;
        }
    }

    // If we have cancelled the current task then we should avoid processing the map results.
    {
        let cancelled = {
            let operation_state = resources.operation_state.lock().unwrap();
            operation_state.task_cancelled(task_id)
        };
        if cancelled {
            operation_handler::set_cancelled_status(&resources.operation_state);
            return;
        }
    }

    match result {
        Ok(map_result) => {
            let (finished, parse_failed) = {
                let mut parse_failed = false;

                let mut operation_state = resources.operation_state.lock().unwrap();
                for (partition, value) in map_result {
                    let vec = operation_state
                        .intermediate_map_results
                        .entry(partition)
                        .or_insert_with(Vec::new);

                    if let serde_json::Value::Array(ref pairs) = value {
                        for pair in pairs {
                            vec.push(pair.clone());
                        }
                    } else {
                        parse_failed = true;
                    }
                }

                operation_state.waiting_map_operations -= 1;
                (operation_state.waiting_map_operations == 0, parse_failed)
            };

            if parse_failed {
                process_map_operation_error(
                    Error::from_kind(ErrorKind::Msg(
                        "Failed to parse map operation output".to_owned(),
                    )),
                    resources,
                    initial_cpu_time,
                );
            } else if finished {
                let result = combine_map_results(resources, initial_cpu_time, output_dir, task_id);

                if let Err(err) = result {
                    process_map_operation_error(err, resources, initial_cpu_time);
                }
            }
        }
        Err(err) => {
            process_map_operation_error(err, resources, initial_cpu_time);
        }
    }
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

    fs::create_dir_all(&output_path).chain_err(
        || "Failed to create output directory",
    )?;

    let input_locations = map_options.get_input().get_input_locations();
    let initial_cpu_time;

    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        operation_state.waiting_map_operations = input_locations.len();
        operation_state.intermediate_map_results = HashMap::new();

        initial_cpu_time = operation_state.initial_cpu_time;
    }

    for input_location in input_locations {
        // Make sure the job hasn't been cancelled before continuing.
        {
            let cancelled = {
                let operation_state = resources.operation_state.lock().unwrap();
                operation_state.task_cancelled(&map_options.task_id)
            };
            if cancelled {
                operation_handler::set_cancelled_status(&resources.operation_state);
                println!("Succesfully cancelled task: {}", map_options.task_id);
                return Ok(());
            }
        }

        info!(
            "Running map task for {} ({} - > {})",
            input_location.input_path,
            input_location.start_byte,
            input_location.end_byte
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

        let serialized_map_input = bson::to_bson(&map_input).chain_err(
            || "Could not serialize map input to bson.",
        )?;

        let map_input_document;
        if let bson::Bson::Document(document) = serialized_map_input {
            map_input_document = document;
        } else {
            return Err("Could not convert map input to bson::Document.".into());
        }

        let output_path_str: String = (*output_path.to_string_lossy()).to_owned();
        let task_id = map_options.task_id.clone();

        let resources = resources.clone();

        thread::spawn(move || {
            let result = map_operation_thread_impl(&map_input_document, child);

            process_map_result(
                result,
                &resources,
                initial_cpu_time,
                &output_path_str,
                &task_id,
            );
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mocktopus::mocking::*;

    #[test]
    fn test_parse_map_results() {
        io::write_local.mock_safe(|_: PathBuf, _| { return MockResult::Return(Ok(())); });

        let map_results =
        r#"{"partitions":{"1":[{"key":"zar","value":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;

        let map = parse_map_results(&map_results).unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_parse_map_results_error() {
        io::write_local.mock_safe(|_: PathBuf, _| { return MockResult::Return(Ok(())); });

        let map_results =
            r#"{:{"1":[{"key":"zavalue":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;

        let result = parse_map_results(&map_results);
        assert!(result.is_err());
    }
}
