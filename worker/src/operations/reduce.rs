use std::collections::HashMap;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json;

use errors::*;
use communication;
use communication::MasterInterface;
use super::io;
use super::operation_handler;
use super::operation_handler::OperationResources;
use super::state::OperationState;
use util::output_error;
use util::data_layer::AbstractionLayer;

use cerberus_proto::worker as pb;

#[derive(Serialize)]
struct ReduceInput {
    pub key: serde_json::Value,
    pub values: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct ReduceResult {
    pub key: String,
    pub values: serde_json::Value,
}

struct ReduceOptions {
    reducer_file_path: String,
    output_directory: String,
    task_id: String,
    partition: u64,
}

fn run_reducer(
    reduce_options: &ReduceOptions,
    reduce_operations: &[ReduceInput],
    data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
) -> Result<Vec<ReduceResult>> {
    let absolute_path = data_abstraction_layer_arc
        .get_local_file(Path::new(&reduce_options.reducer_file_path))
        .chain_err(|| "Unable to get absolute path")?;
    let mut child = Command::new(absolute_path)
        .arg("reduce")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .chain_err(|| "Failed to start reduce operation process.")?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(json!(reduce_operations).to_string().as_bytes())
            .chain_err(|| "Error writing to payload stdin.")?;
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

    let reduce_output: serde_json::Value = serde_json::from_str(&output_str).chain_err(
        || "Error parsing reduce results.",
    )?;

    let mut reduce_results = Vec::new();
    if let serde_json::Value::Array(ref reduce_outputs) = reduce_output {
        for (i, reduce_output) in reduce_outputs.iter().enumerate() {
            let result_key_string = {
                if let Some(key_str) = reduce_operations[i].key.as_str() {
                    key_str.to_string()
                } else {
                    reduce_operations[i].key.to_string()
                }
            };

            let reduce_result = ReduceResult {
                key: result_key_string,
                values: reduce_output["values"].to_owned(),
            };
            reduce_results.push(reduce_result);
        }
    } else {
        return Err("Error parsing reduce results as Array".into());
    }

    Ok(reduce_results)
}

fn log_reduce_operation_err(
    err: Error,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    task_id: &str,
) {
    output_error(&err.chain_err(|| "Error running reduce operation."));
    operation_handler::set_failed_status(operation_state_arc, task_id);
}

fn send_reduce_result(
    master_interface_arc: &Arc<MasterInterface>,
    result: pb::ReduceResult,
) -> Result<()> {
    master_interface_arc
        .return_reduce_result(result)
        .chain_err(|| "Error sending reduce result to master.")?;
    Ok(())
}

fn create_reduce_input(
    reduce_request: &pb::PerformReduceRequest,
    output_uuid: &str,
    resources: &OperationResources,
) -> Result<Vec<ReduceInput>> {
    let mut reduce_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    let reduce_inputs = communication::fetch_reduce_inputs(
        reduce_request.get_input_file_paths().clone().to_vec(),
        output_uuid.to_string(),
        resources.clone(),
        reduce_request.task_id.to_string(),
    ).chain_err(|| "Error fetching reduce inputs")?;

    for reduce_input in reduce_inputs {
        let parsed_value: serde_json::Value = serde_json::from_str(&reduce_input).chain_err(
            || "Error parsing reduce input",
        )?;

        let parsed_object = parsed_value.as_object().chain_err(
            || "Error parsing reduce input",
        )?;

        for (key, values) in parsed_object.iter() {
            let key = key.to_string();
            if let serde_json::Value::Array(ref values) = *values {
                for value in values {
                    let reduce_array = reduce_map.entry(key.to_owned()).or_insert_with(Vec::new);
                    reduce_array.push(value.to_owned());
                }
            } else {
                return Err("Error parsing reduce input value.".into());
            }
        }
    }

    let mut reduce_operations: Vec<ReduceInput> = Vec::new();
    for (intermediate_key, reduce_array) in reduce_map {
        let key_value: serde_json::Value = serde_json::from_str(&intermediate_key).chain_err(
            || "Error parsing intermediate_key",
        )?;

        let reduce_operation = ReduceInput {
            key: key_value,
            values: reduce_array,
        };

        reduce_operations.push(reduce_operation);
    }
    Ok(reduce_operations)
}

// Public api for performing a reduce task.
pub fn perform_reduce(
    reduce_request: &pb::PerformReduceRequest,
    resources: &OperationResources,
    output_uuid: &str,
) -> Result<()> {
    {
        let mut operation_state = resources.operation_state.lock().unwrap();
        operation_state.initial_cpu_time = operation_handler::get_cpu_time();
    }

    info!(
        "Performing reduce operation. reducer={}",
        reduce_request.reducer_file_path
    );

    {
        let mut state = resources.operation_state.lock().unwrap();
        if state.current_task_id != "" {
            warn!("Reduce operation requested while worker is busy");
            return Err("Worker is busy.".into());
        }

        state.current_task_id = reduce_request.task_id.clone();
        state.operation_status = pb::OperationStatus::IN_PROGRESS;
    }

    let result = internal_perform_reduce(reduce_request, resources, output_uuid);
    if let Err(err) = result {
        log_reduce_operation_err(err, &resources.operation_state, &reduce_request.task_id);
        return Err("Error starting reduce operation.".into());
    }

    Ok(())
}

// Internal implementation for performing a reduce task.
fn internal_perform_reduce(
    reduce_request: &pb::PerformReduceRequest,
    resources: &OperationResources,
    output_uuid: &str,
) -> Result<()> {
    let reduce_operations = create_reduce_input(reduce_request, output_uuid, resources)
        .chain_err(|| "Error creating reduce operations from input")?;

    let reduce_options = ReduceOptions {
        reducer_file_path: reduce_request.get_reducer_file_path().to_owned(),
        output_directory: reduce_request.get_output_directory().to_owned(),
        task_id: reduce_request.get_task_id().to_owned(),
        partition: reduce_request.get_partition().to_owned(),
    };

    run_reduce(reduce_options, resources, reduce_operations);

    Ok(())
}

fn handle_reduce_error(err: Error, resources: &OperationResources, task_id: &str) {
    let mut response = pb::ReduceResult::new();
    response.set_status(pb::ResultStatus::FAILURE);
    response.set_failure_details(operation_handler::failure_details_from_error(&err));
    response.set_task_id(task_id.to_owned());

    let result = send_reduce_result(&resources.master_interface, response);
    if let Err(err) = result {
        error!("Error sending reduce failed: {}", err);
    }

    log_reduce_operation_err(err, &resources.operation_state, task_id);
}

fn handle_reduce_success(resources: &OperationResources, initial_cpu_time: u64, task_id: &str) {
    let mut response = pb::ReduceResult::new();
    response.set_status(pb::ResultStatus::SUCCESS);
    response.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
    response.set_task_id(task_id.to_owned());

    let result = send_reduce_result(&resources.master_interface, response);

    match result {
        Ok(_) => {
            operation_handler::set_complete_status(&resources.operation_state, task_id);
            info!("Reduce operation completed sucessfully.");
        }
        Err(err) => {
            error!("Error sending reduce result: {}", err);
            operation_handler::set_failed_status(&resources.operation_state, task_id);
        }
    }
}

fn write_reduce_output(
    reduce_options: &ReduceOptions,
    reduce_results: &HashMap<String, serde_json::Value>,
    data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
) -> Result<()> {
    data_abstraction_layer_arc
        .create_dir_all(Path::new(&reduce_options.output_directory))
        .chain_err(|| "Failed to create output directory")?;

    let reduce_results_pretty: String = serde_json::to_string_pretty(&reduce_results).chain_err(
        || "Error prettifying reduce results",
    )?;

    let mut file_path = PathBuf::new();
    file_path.push(reduce_options.output_directory.clone());
    file_path.push(reduce_options.partition.to_string());

    io::write(
        data_abstraction_layer_arc,
        file_path,
        reduce_results_pretty.as_bytes(),
    ).chain_err(|| "Failed to write reduce output.")?;

    Ok(())
}

fn run_reduce(
    reduce_options: ReduceOptions,
    resources: &OperationResources,
    reduce_operations: Vec<ReduceInput>,
) {
    let initial_cpu_time = resources.operation_state.lock().unwrap().initial_cpu_time;
    let resources = resources.clone();

    thread::spawn(move || {
        let mut reduce_results: HashMap<String, serde_json::Value> = HashMap::new();

        // Make sure the job hasn't been cancelled before continuing.
        if operation_handler::check_task_cancelled(
            &resources.operation_state,
            &reduce_options.task_id,
        )
        {
            return;
        }

        let result = run_reducer(
            &reduce_options,
            &reduce_operations,
            &resources.data_abstraction_layer,
        );

        match result {
            Ok(reduce_kvs) => {
                for reduce_kv in reduce_kvs {
                    reduce_results.insert(reduce_kv.key, reduce_kv.values);
                }
            }
            Err(err) => {
                handle_reduce_error(err, &resources, &reduce_options.task_id);
                return;
            }
        }

        let write_output_result = write_reduce_output(
            &reduce_options,
            &reduce_results,
            &resources.data_abstraction_layer,
        );

        if let Err(err) = write_output_result {
            handle_reduce_error(err, &resources, &reduce_options.task_id);
            return;
        }

        handle_reduce_success(&resources, initial_cpu_time, &reduce_options.task_id);
    });
}
