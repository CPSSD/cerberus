use cerberus_proto::worker as pb;
use std::thread;
use std::path::PathBuf;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::fs;
use std::sync::{Arc, Mutex};
use std::process::{Child, Command, Stdio};
use serde_json;
use serde_json::Value;
use serde_json::Value::Array;
use protobuf::RepeatedField;
use uuid::Uuid;
use errors::*;
use util::output_error;

const WORKER_OUTPUT_DIRECTORY: &str = "/tmp/cerberus/";

// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
struct OperationState {
    pub worker_status: pb::WorkerStatusResponse_WorkerStatus,
    pub operation_status: pb::WorkerStatusResponse_OperationStatus,
    pub map_result: Option<pb::MapResponse>,
    pub reduce_result: Option<pb::ReduceResponse>,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            worker_status: pb::WorkerStatusResponse_WorkerStatus::AVAILABLE,
            operation_status: pb::WorkerStatusResponse_OperationStatus::UNKNOWN,
            map_result: None,
            reduce_result: None,
        }
    }
}

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
#[derive(Default)]
pub struct OperationHandler {
    operation_state: Arc<Mutex<OperationState>>,
    output_dir_uuid: String,
}

fn set_operation_handler_status(
    operation_state_arc: &Arc<Mutex<OperationState>>,
    worker_status: pb::WorkerStatusResponse_WorkerStatus,
    operation_status: pb::WorkerStatusResponse_OperationStatus,
) -> Result<()> {
    match operation_state_arc.lock() {
        Ok(mut operation_state) => {
            operation_state.worker_status = worker_status;
            operation_state.operation_status = operation_status;
            Ok(())
        }
        Err(err) => {
            Err(
                format!(
                    "Could not lock operation_state to set status: {}",
                    err.to_string()
                ).into(),
            )
        }
    }
}

fn set_complete_status(operation_state_arc: &Arc<Mutex<OperationState>>) -> Result<()> {
    set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatusResponse_WorkerStatus::AVAILABLE,
        pb::WorkerStatusResponse_OperationStatus::COMPLETE,
    ).chain_err(|| "Could not set operation completed status.")
}

fn set_failed_status(operation_state_arc: &Arc<Mutex<OperationState>>) {
    let result = set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatusResponse_WorkerStatus::AVAILABLE,
        pb::WorkerStatusResponse_OperationStatus::FAILED,
    );

    if let Err(err) = result {
        output_error(&err.chain_err(|| "Could not set operation failed status."));
    }
}

fn parse_map_results(
    map_result_string: &str,
    output_dir: &str,
) -> Result<Vec<pb::MapResponse_MapResult>> {
    let parse_value: Value = serde_json::from_str(map_result_string).chain_err(
        || "Error parsing map response.",
    )?;

    let mut map_results: Vec<pb::MapResponse_MapResult> = Vec::new();

    if let Array(ref pairs) = parse_value["pairs"] {
        for value in pairs {
            let key = {
                match value["key"].as_str() {
                    Some(key) => key,
                    None => continue,
                }
            };

            let value = {
                match value["value"].as_str() {
                    Some(value) => value,
                    None => continue,
                }
            };

            let file_name = Uuid::new_v4().to_string();
            let mut file_path = PathBuf::new();
            file_path.push(output_dir);
            file_path.push(&file_name);

            let mut map_result = pb::MapResponse_MapResult::new();
            map_result.set_key(key.to_owned());
            map_result.set_output_file_path((*file_path.to_string_lossy()).to_owned());

            let mut file = File::create(file_path).chain_err(
                || "Failed to create map output file.",
            )?;
            file.write_all(value.as_bytes()).chain_err(
                || "Failed to write to map output file.",
            )?;

            map_results.push(map_result);
        }
    }

    Ok(map_results)
}

fn log_map_operation_err(err: Error, operation_state_arc: &Arc<Mutex<OperationState>>) {
    output_error(&err.chain_err(|| "Error running map operation."));
    set_failed_status(operation_state_arc);
}

fn log_reduce_operation_err(err: Error, operation_state_arc: &Arc<Mutex<OperationState>>) {
    output_error(&err.chain_err(|| "Error running reduce operation."));
    set_failed_status(operation_state_arc);
}

fn map_operation_thread_impl(
    map_input_value: &str,
    output_dir: &str,
    mut child: Child,
    operation_state_arc: &Arc<Mutex<OperationState>>,
) -> Result<()> {
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(map_input_value.as_bytes()).chain_err(
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

    let map_results = parse_map_results(&output_str, output_dir).chain_err(
        || "Error parsing map results.",
    )?;

    let mut response = pb::MapResponse::new();
    response.set_status(pb::OperationStatus::SUCCESS);
    response.set_map_results(RepeatedField::from_vec(map_results));

    match operation_state_arc.lock() {
        Ok(mut operation_state) => {
            operation_state.map_result = Some(response);
            Ok(())
        }
        Err(_) => Err("Could not lock operation_state to write map result.".into()),
    }
}

fn reduce_operation_thread_impl(
    reduce_input: &str,
    reduce_intermediate_key: &str,
    output_dir: &str,
    mut child: Child,
    operation_state_arc: &Arc<Mutex<OperationState>>,
) -> Result<()> {
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(reduce_input.as_bytes()).chain_err(
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

    let reduce_results: Value = serde_json::from_str(&output_str).chain_err(
        || "Error parsing reduce results.",
    )?;
    let reduce_results_pretty: String = serde_json::to_string_pretty(&reduce_results).chain_err(
        || "Error prettifying reduce results",
    )?;

    let mut file_path = PathBuf::new();
    file_path.push(output_dir);
    file_path.push(reduce_intermediate_key);

    let mut response = pb::ReduceResponse::new();
    response.set_status(pb::OperationStatus::SUCCESS);
    response.set_output_file_path((*file_path.to_string_lossy()).to_owned());

    let mut file = File::create(file_path).chain_err(
        || "Failed to create reduce output file.",
    )?;

    file.write_all(reduce_results_pretty.as_bytes()).chain_err(
        || "Failed to write to reduce output file.",
    )?;

    match operation_state_arc.lock() {
        Ok(mut operation_state) => {
            operation_state.reduce_result = Some(response);
            Ok(())
        }
        Err(_) => Err(
            "Could not lock operation_state to write reduce result".into(),
        ),
    }
}

impl OperationHandler {
    pub fn new() -> Self {
        OperationHandler {
            operation_state: Arc::new(Mutex::new(OperationState::new())),
            output_dir_uuid: Uuid::new_v4().to_string(),
        }
    }

    pub fn get_worker_status(&self) -> pb::WorkerStatusResponse_WorkerStatus {
        match self.operation_state.lock() {
            Ok(operation_state) => operation_state.worker_status,
            Err(_) => pb::WorkerStatusResponse_WorkerStatus::BUSY,
        }
    }

    pub fn get_worker_operation_status(&self) -> pb::WorkerStatusResponse_OperationStatus {
        match self.operation_state.lock() {
            Ok(operation_state) => operation_state.operation_status,
            Err(_) => pb::WorkerStatusResponse_OperationStatus::UNKNOWN,
        }
    }

    pub fn set_operation_handler_busy(&self) -> Result<()> {
        match self.operation_state.lock() {
            Ok(mut operation_state) => {
                operation_state.worker_status = pb::WorkerStatusResponse_WorkerStatus::BUSY;
                operation_state.operation_status =
                    pb::WorkerStatusResponse_OperationStatus::IN_PROGRESS;

                Ok(())
            }
            Err(_) => Err("Failed to lock operation_state struct.".into()),
        }
    }

    pub fn perform_map(&mut self, map_options: pb::PerformMapRequest) -> Result<()> {
        if self.get_worker_status() == pb::WorkerStatusResponse_WorkerStatus::BUSY {
            return Err("Worker is busy.".into());
        }

        let file = File::open(map_options.get_input_file_path()).chain_err(
            || "Couldn't open input file.",
        )?;

        let mut buf_reader = BufReader::new(file);
        let mut map_input_value = String::new();
        buf_reader.read_to_string(&mut map_input_value).chain_err(
            || "Couldn't read map input file.",
        )?;

        self.set_operation_handler_busy().chain_err(
            || "Couldn't set operation handler busy.",
        )?;

        let child = Command::new(map_options.get_mapper_file_path())
            .arg("map")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start map operation process.")?;

        let operation_state_arc = Arc::clone(&self.operation_state);

        let map_input = json!({
            "key": map_options.get_input_file_path(),
            "value": map_input_value,
        });

        let map_input_str = map_input.to_string();

        let mut output_path = PathBuf::new();
        output_path.push(WORKER_OUTPUT_DIRECTORY);
        output_path.push(&self.output_dir_uuid);
        output_path.push("map");

        fs::create_dir_all(output_path.clone()).chain_err(
            || "Failed to create output directory",
        )?;

        thread::spawn(move || {
            let result = map_operation_thread_impl(
                &map_input_str,
                &*output_path.to_string_lossy(),
                child,
                &operation_state_arc,
            );

            match result {
                Ok(_) => {
                    let result = set_complete_status(&operation_state_arc);
                    if let Err(err) = result {
                        log_map_operation_err(err, &operation_state_arc)
                    }
                }
                Err(err) => log_map_operation_err(err, &operation_state_arc),
            }
        });

        Ok(())
    }

    pub fn get_map_result(&self) -> Result<pb::MapResponse> {
        match self.operation_state.lock() {
            Ok(operation_state) => {
                if let Some(ref map_response) = operation_state.map_result {
                    Ok(map_response.clone())
                } else {
                    Err("No map result found.".into())
                }
            }
            Err(_) => Err("Could not lock operation_state.".into()),
        }
    }

    pub fn perform_reduce(&mut self, reduce_options: pb::PerformReduceRequest) -> Result<()> {
        if self.get_worker_status() == pb::WorkerStatusResponse_WorkerStatus::BUSY {
            return Err("Worker is busy.".into());
        }

        let mut reduce_input_values: Vec<String> = Vec::new();
        for input_file in reduce_options.get_input_file_paths() {
            let file = File::open(input_file).chain_err(
                || "Couldn't open reduce input file.",
            )?;

            let mut buf_reader = BufReader::new(file);
            let mut file_contents = String::new();
            buf_reader.read_to_string(&mut file_contents).chain_err(
                || "Couldn't read reduce input file.",
            )?;

            reduce_input_values.push(file_contents);
        }

        let reduce_json_input = json!({
            "key": reduce_options.get_intermediate_key(),
            "values": reduce_input_values,
        });

        let reduce_input_str = reduce_json_input.to_string();

        self.set_operation_handler_busy().chain_err(
            || "Couldn't set operation handler busy.",
        )?;

        let child = Command::new(reduce_options.get_reducer_file_path())
            .arg("reduce")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start reduce operation process.")?;

        let operation_state_arc = Arc::clone(&self.operation_state);

        fs::create_dir_all(reduce_options.get_output_directory())
            .chain_err(|| "Failed to create output directory")?;

        thread::spawn(move || {
            let result = reduce_operation_thread_impl(
                &reduce_input_str,
                reduce_options.get_intermediate_key(),
                reduce_options.get_output_directory(),
                child,
                &operation_state_arc,
            );

            match result {
                Ok(_) => {
                    let result = set_complete_status(&operation_state_arc);
                    if let Err(err) = result {
                        log_map_operation_err(err, &operation_state_arc)
                    }
                }
                Err(err) => log_reduce_operation_err(err, &operation_state_arc),
            }
        });
        Ok(())
    }

    pub fn get_reduce_result(&self) -> Result<pb::ReduceResponse> {
        match self.operation_state.lock() {
            Ok(operation_state) => {
                if let Some(ref reduce_response) = operation_state.reduce_result {
                    Ok(reduce_response.clone())
                } else {
                    Err("No reduce result found.".into())
                }
            }
            Err(_) => Err("Could not lock operation_state.".into()),
        }
    }
}
