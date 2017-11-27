use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use bson;
use serde_json;
use serde_json::Value;
use uuid::Uuid;

use errors::*;
use cerberus_proto::worker as pb;
use master_interface::MasterInterface;
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

/// ParsedMapResults is a tuple containing a HashMap of parsed map results and a vector
/// of intermediate files created by the worker.
type ParsedMapResults = (HashMap<u64, String>, Vec<PathBuf>);

fn parse_map_results(map_result_string: &str, output_dir: &str) -> Result<ParsedMapResults> {
    let parse_value: Value = serde_json::from_str(map_result_string).chain_err(
        || "Error parsing map response.",
    )?;

    let mut map_results: HashMap<u64, String> = HashMap::new();

    let partition_map = match parse_value["partitions"].as_object() {
        None => return Err("Error parsing partition map.".into()),
        Some(map) => map,
    };

    let mut intermediate_files = Vec::new();
    for (partition_str, pairs) in partition_map.iter() {
        let partition: u64 = partition_str.to_owned().parse().chain_err(
            || "Error parsing map response.",
        )?;

        let file_name = Uuid::new_v4().to_string();
        let mut file_path = PathBuf::new();
        file_path.push(output_dir);
        file_path.push(&file_name);

        let mut file = File::create(file_path.clone()).chain_err(
            || "Failed to create map output file.",
        )?;
        file.write_all(pairs.to_string().as_bytes()).chain_err(
            || "Failed to write to map output file.",
        )?;
        intermediate_files.push(file_path.clone());

        map_results.insert(partition, (*file_path.to_string_lossy()).to_owned());
    }

    Ok((map_results, intermediate_files))
}

/// MapOperationResults is a tuple containing a pb::MapResult object and a vector of intermediate
/// files created by the worker.
type MapOperationResults = (pb::MapResult, Vec<PathBuf>);

fn map_operation_thread_impl(
    map_input_value: &bson::Document,
    output_dir: &str,
    mut child: Child,
) -> Result<MapOperationResults> {
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

    let (map_results, intermediate_files) = parse_map_results(&output_str, output_dir).chain_err(
        || "Error parsing map results.",
    )?;

    let mut response = pb::MapResult::new();
    response.set_status(pb::ResultStatus::SUCCESS);
    response.set_map_results(map_results);

    Ok((response, intermediate_files))
}

pub fn perform_map(
    map_options: &pb::PerformMapRequest,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    master_interface_arc: Arc<MasterInterface>,
    output_dir_uuid: &str,
) -> Result<()> {
    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.initial_cpu_time = operation_handler::get_cpu_time();
    }

    info!(
        "Performing map operation. mapper={} input={}",
        map_options.mapper_file_path,
        map_options.input_file_path
    );

    if operation_handler::get_worker_status(operation_state_arc) == pb::WorkerStatus::BUSY {
        warn!("Map operation requested while worker is busy");
        return Err("Worker is busy.".into());
    }
    operation_handler::set_busy_status(operation_state_arc);

    let result = do_perform_map(
        map_options,
        Arc::clone(operation_state_arc),
        master_interface_arc,
        output_dir_uuid,
    );
    if let Err(err) = result {
        log_map_operation_err(err, operation_state_arc);
        return Err("Error starting map operation.".into());
    }

    Ok(())
}

// Internal implementation for performing a map task.
// TODO: Split this function into smaller pieces (https://github.com/CPSSD/cerberus/issues/281)
fn do_perform_map(
    map_options: &pb::PerformMapRequest,
    operation_state_arc: Arc<Mutex<OperationState>>,
    master_interface_arc: Arc<MasterInterface>,
    output_dir_uuid: &str,
) -> Result<()> {
    let file = File::open(map_options.get_input_file_path()).chain_err(
        || "Couldn't open input file.",
    )?;

    let mut buf_reader = BufReader::new(file);
    let mut map_input_value = String::new();
    buf_reader.read_to_string(&mut map_input_value).chain_err(
        || "Couldn't read map input file.",
    )?;

    let child = Command::new(map_options.get_mapper_file_path())
        .arg("map")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .chain_err(|| "Failed to start map operation process.")?;

    let map_input = MapInput {
        key: map_options.get_input_file_path().to_string(),
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

    let mut output_path = PathBuf::new();
    output_path.push(WORKER_OUTPUT_DIRECTORY);
    output_path.push(output_dir_uuid);
    output_path.push("map");

    fs::create_dir_all(output_path.clone()).chain_err(
        || "Failed to create output directory",
    )?;

    let initial_cpu_time = operation_state_arc.lock().unwrap().initial_cpu_time;

    thread::spawn(move || {
        let result =
            map_operation_thread_impl(&map_input_document, &*output_path.to_string_lossy(), child);

        match result {
            Ok((mut map_result, intermediate_files)) => {
                let mut operation_state = operation_state_arc.lock().unwrap();
                operation_state.add_intermediate_files(intermediate_files);

                map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
                if let Err(err) = send_map_result(&master_interface_arc, map_result) {
                    log_map_operation_err(err, &operation_state_arc);
                } else {
                    info!("Map operation completed sucessfully.");
                    operation_handler::set_complete_status(&operation_state_arc);
                }
            }
            Err(err) => {
                let mut map_result = pb::MapResult::new();
                map_result.set_status(pb::ResultStatus::FAILURE);
                map_result.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
                map_result.set_failure_details(operation_handler::failure_details_from_error(&err));

                if let Err(err) = send_map_result(&master_interface_arc, map_result) {
                    error!("Could not send map operation failed: {}", err);
                }
                log_map_operation_err(err, &operation_state_arc);
            }
        }
    });

    Ok(())
}
