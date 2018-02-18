use std::collections::HashMap;
use std::fs;
use std::io::prelude::*;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use bson;
use serde_json;
use uuid::Uuid;

use errors::*;
use cerberus_proto::worker as pb;
use master_interface::MasterInterface;
use super::io;
use super::operation_handler;
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

    let map_results = parse_map_results(&output_str).chain_err(
        || "Error parsing map results.",
    )?;

    Ok(map_results)
}

pub fn perform_map(
    map_options: &pb::PerformMapRequest,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: &Arc<MasterInterface>,
    output_dir_uuid: &str,
) -> Result<()> {
    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.initial_cpu_time = operation_handler::get_cpu_time();
    }

    info!(
        "Performing map operation. mapper={} input={:?}",
        map_options.mapper_file_path,
        map_options.input
    );

    if operation_handler::get_worker_status(operation_state_arc) == pb::WorkerStatus::BUSY {
        warn!("Map operation requested while worker is busy");
        return Err("Worker is busy.".into());
    }
    operation_handler::set_busy_status(operation_state_arc);

    let result = internal_perform_map(
        map_options,
        operation_state_arc,
        master_interface_arc,
        output_dir_uuid,
    );
    if let Err(err) = result {
        log_map_operation_err(err, operation_state_arc);
        return Err("Error starting map operation.".into());
    }

    Ok(())
}

fn combine_map_results(
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: &Arc<MasterInterface>,
    initial_cpu_time: u64,
    output_dir: &str,
) -> Result<()> {
    let partition_map;
    {
        let operation_state = operation_state_arc.lock().unwrap();
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
        io::write(file_path.clone(), json_values.to_string().as_bytes())
            .chain_err(|| "failed to write map output")?;
        intermediate_files.push(file_path.clone());

        map_results.insert(*partition, (*file_path.to_string_lossy()).to_owned());
    }

    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.add_intermediate_files(intermediate_files);
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_map_results(map_results);
    map_result.set_status(pb::ResultStatus::SUCCESS);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);

    if let Err(err) = send_map_result(master_interface_arc, map_result) {
        log_map_operation_err(err, operation_state_arc);
    } else {
        info!("Map operation completed sucessfully.");
        operation_handler::set_complete_status(operation_state_arc);
    }

    Ok(())
}

fn process_map_operation_error(
    err: Error,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: &Arc<MasterInterface>,
    initial_cpu_time: u64,
) {
    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.waiting_map_operations = 0;
    }

    let mut map_result = pb::MapResult::new();
    map_result.set_status(pb::ResultStatus::FAILURE);
    map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    map_result.set_failure_details(operation_handler::failure_details_from_error(&err));

    if let Err(err) = send_map_result(master_interface_arc, map_result) {
        error!("Could not send map operation failed: {}", err);
    }
    log_map_operation_err(err, operation_state_arc);
}

fn process_map_result(
    result: Result<IntermediateMapResults>,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: &Arc<MasterInterface>,
    initial_cpu_time: u64,
    output_dir: &str,
) {
    {
        // The number of operations waiting will be 0 if we have already processed one
        // that has failed.
        let operation_state = operation_state_arc.lock().unwrap();
        if operation_state.waiting_map_operations == 0 {
            return;
        }
    }

    match result {
        Ok(map_result) => {
            let (finished, parse_failed) = {
                let mut parse_failed = false;

                let mut operation_state = operation_state_arc.lock().unwrap();
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
                    operation_state_arc,
                    master_interface_arc,
                    initial_cpu_time,
                );
            } else if finished {
                let result = combine_map_results(
                    operation_state_arc,
                    master_interface_arc,
                    initial_cpu_time,
                    output_dir,
                );

                if let Err(err) = result {
                    process_map_operation_error(
                        err,
                        operation_state_arc,
                        master_interface_arc,
                        initial_cpu_time,
                    );
                }
            }
        }
        Err(err) => {
            process_map_operation_error(
                err,
                operation_state_arc,
                master_interface_arc,
                initial_cpu_time,
            );
        }
    }
}

// Internal implementation for performing a map task.
fn internal_perform_map(
    map_options: &pb::PerformMapRequest,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: &Arc<MasterInterface>,
    output_dir_uuid: &str,
) -> Result<()> {
    let mut output_path = PathBuf::new();
    output_path.push(WORKER_OUTPUT_DIRECTORY);
    output_path.push(output_dir_uuid);
    output_path.push("map");

    fs::create_dir_all(output_path.clone()).chain_err(
        || "Failed to create output directory",
    )?;

    let input_locations = map_options.get_input().get_input_locations();
    let initial_cpu_time;

    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.waiting_map_operations = input_locations.len();
        operation_state.intermediate_map_results = HashMap::new();

        initial_cpu_time = operation_state.initial_cpu_time;
    }

    for input_location in input_locations {
        let map_input_value = io::read_location(input_location).chain_err(
            || "unable to open input file",
        )?;

        let child = Command::new(map_options.get_mapper_file_path())
            .arg("map")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
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

        let operation_state_arc_clone = Arc::clone(operation_state_arc);
        let master_interface_arc_clone = Arc::clone(master_interface_arc);
        let output_path_str: String = (*output_path.to_string_lossy()).to_owned();

        thread::spawn(move || {
            let result = map_operation_thread_impl(&map_input_document, child);

            process_map_result(
                result,
                &operation_state_arc_clone,
                &master_interface_arc_clone,
                initial_cpu_time,
                &output_path_str,
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
        io::write.mock_safe(|_: PathBuf, _| { return MockResult::Return(Ok(())); });

        let map_results =
        r#"{"partitions":{"1":[{"key":"zar","value":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;

        let map = parse_map_results(&map_results).unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_parse_map_results_error() {
        io::write.mock_safe(|_: PathBuf, _| { return MockResult::Return(Ok(())); });

        let map_results =
            r#"{:{"1":[{"key":"zavalue":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;

        let result = parse_map_results(&map_results);
        assert!(result.is_err());
    }
}
