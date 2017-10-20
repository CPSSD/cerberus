use cerberus_proto::mrworker::*;
use std::thread;
use std::path::PathBuf;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::process::{Child, Command, Stdio};
use serde_json;
use serde_json::Value;
use serde_json::Value::Array;
use protobuf::RepeatedField;
use uuid::Uuid;
use errors::*;

const WORKER_OUTPUT_DIRECTORY: &'static str = "/tmp/";

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
#[derive(Default)]
pub struct OperationHandler {
    worker_status: Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    operation_status: Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
    map_result: Arc<Mutex<Option<MapResponse>>>,
    reduce_result: Arc<Mutex<Option<ReduceResponse>>>,
}

fn set_worker_status(
    worker_status_arc: Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    status: WorkerStatusResponse_WorkerStatus,
) -> Result<()> {
    let worker_status_result = worker_status_arc.lock();
    match worker_status_result {
        Ok(mut worker_status) => {
            *worker_status = status;
            Ok(())
        }
        Err(_) => Err("Failed to lock worker_status.".into()),
    }
}

fn set_operation_status(
    operation_status_arc: Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
    status: WorkerStatusResponse_OperationStatus,
) -> Result<()> {
    let operation_status_result = operation_status_arc.lock();
    match operation_status_result {
        Ok(mut operation_status) => {
            *operation_status = status;
            Ok(())
        }
        Err(_) => Err("Failed to lock operation_status.".into()),
    }
}

fn set_complete_status(
    worker_status_arc: Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    operation_status_arc: Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
) {
    let worker_status_result = set_worker_status(
        worker_status_arc,
        WorkerStatusResponse_WorkerStatus::AVAILABLE,
    );
    let operation_status_result = set_operation_status(
        operation_status_arc,
        WorkerStatusResponse_OperationStatus::COMPLETE,
    );

    if let Err(err) = worker_status_result {
        error!("Error setting complete status for operation: {}", err);
    }
    if let Err(err) = operation_status_result {
        error!("Error setting complete status for operation: {}", err);
    }
}

fn set_failed_status(
    worker_status_arc: Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    operation_status_arc: Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
) {
    let worker_status_result = set_worker_status(
        worker_status_arc,
        WorkerStatusResponse_WorkerStatus::AVAILABLE,
    );
    let operation_status_result = set_operation_status(
        operation_status_arc,
        WorkerStatusResponse_OperationStatus::FAILED,
    );

    if let Err(err) = worker_status_result {
        error!("Error setting failed status for operation: {}", err);
    }
    if let Err(err) = operation_status_result {
        error!("Error setting failed status for operation: {}", err);
    }
}

fn parse_map_results(map_result_string: &str) -> Result<Vec<MapResponse_MapResult>> {
    let parse_value: Value = serde_json::from_str(map_result_string).chain_err(
        || "Error parsing map response.",
    )?;

    let mut map_results: Vec<MapResponse_MapResult> = Vec::new();

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
            file_path.push(WORKER_OUTPUT_DIRECTORY);
            file_path.push(&file_name);

            let mut map_result = MapResponse_MapResult::new();
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

fn log_map_operation_err(
    err: &Error,
    worker_status_arc: &Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    operation_status_arc: &Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
) {
    error!("Map thread error: {}", err);
    self::set_failed_status(
        Arc::clone(worker_status_arc),
        Arc::clone(operation_status_arc),
    );
}

fn log_reduce_operation_err(
    err: &Error,
    worker_status_arc: &Arc<Mutex<WorkerStatusResponse_WorkerStatus>>,
    operation_status_arc: &Arc<Mutex<WorkerStatusResponse_OperationStatus>>,
) {
    error!("Reduce thread error: {}", err);
    self::set_failed_status(
        Arc::clone(worker_status_arc),
        Arc::clone(operation_status_arc),
    );
}

fn map_operation_thread_impl(
    map_input_value: &str,
    mut child: Child,
    map_result_arc: Arc<Mutex<Option<MapResponse>>>,
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

    let map_results = parse_map_results(&output_str).chain_err(
        || "Error parsing map results.",
    )?;

    let mut response = MapResponse::new();
    response.set_status(OperationStatus::SUCCESS);
    response.set_map_results(RepeatedField::from_vec(map_results));

    match map_result_arc.lock() {
        Ok(mut map_result) => {
            *map_result = Some(response);
            Ok(())
        }
        Err(_) => Err("Could not write to map_result_arc.".into()),
    }
}

fn reduce_operation_thread_impl(
    reduce_input: &str,
    mut child: Child,
    reduce_result_arc: Arc<Mutex<Option<ReduceResponse>>>,
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

    let file_name = Uuid::new_v4().to_string();

    let mut file_path = PathBuf::new();
    file_path.push(WORKER_OUTPUT_DIRECTORY);
    file_path.push(&file_name);

    let mut response = ReduceResponse::new();
    response.set_status(OperationStatus::SUCCESS);
    response.set_output_file_path((*file_path.to_string_lossy()).to_owned());

    let mut file = File::create(file_path).chain_err(
        || "Failed to create reduce output file.",
    )?;

    file.write_all(reduce_results_pretty.as_bytes()).chain_err(
        || "Failed to write to reduce output file.",
    )?;

    let reduce_result_res = reduce_result_arc.lock();
    match reduce_result_res {
        Ok(mut reduce_result) => {
            *reduce_result = Some(response);
            Ok(())
        }
        Err(_) => Err("Could not write to reduce_result_arc".into()),
    }
}

impl OperationHandler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_worker_status(&self) -> WorkerStatusResponse_WorkerStatus {
        match self.worker_status.lock() {
            Err(_) => WorkerStatusResponse_WorkerStatus::BUSY,
            Ok(status) => *status,
        }
    }

    pub fn get_worker_operation_status(&self) -> WorkerStatusResponse_OperationStatus {
        match self.operation_status.lock() {
            Err(_) => WorkerStatusResponse_OperationStatus::UNKNOWN,
            Ok(status) => *status,
        }
    }

    pub fn perform_map(&mut self, map_options: PerformMapRequest) -> Result<()> {
        if self.get_worker_status() == WorkerStatusResponse_WorkerStatus::BUSY {
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

        let worker_status_result = self.worker_status.lock();
        let operation_status_result = self.operation_status.lock();

        if let Ok(mut worker_status) = worker_status_result {
            *worker_status = WorkerStatusResponse_WorkerStatus::BUSY;
        } else {
            return Err("Failed to lock internal operation_handler values.".into());
        }

        if let Ok(mut operation_status) = operation_status_result {
            *operation_status = WorkerStatusResponse_OperationStatus::IN_PROGRESS;
        } else {
            return Err("Failed to lock internal operation_handler values.".into());
        }

        let child = Command::new(map_options.get_mapper_file_path())
            .arg("map")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start map operation process.")?;

        let operation_status_arc = Arc::clone(&self.operation_status);
        let worker_status_arc = Arc::clone(&self.worker_status);
        let map_result_arc = Arc::clone(&self.map_result);

        let map_input =
            format!(
            "{{ \"key\": {}, \"value\": {} }}",
            map_options.get_input_file_path(),
            map_input_value,
        );

        thread::spawn(move || {
            let result = map_operation_thread_impl(&map_input, child, map_result_arc);
            match result {
                Ok(_) => self::set_complete_status(worker_status_arc, operation_status_arc),
                Err(err) => log_map_operation_err(&err, &worker_status_arc, &operation_status_arc),
            }
        });

        Ok(())
    }

    pub fn get_map_result(&self) -> Result<MapResponse> {
        match self.map_result.lock() {
            Err(_) => Err("No map result found.".into()),
            Ok(result) => {
                if let Some(ref map_response) = *result {
                    Ok(map_response.clone())
                } else {
                    Err("No map result found.".into())
                }
            }
        }
    }

    pub fn perform_reduce(&mut self, reduce_options: PerformReduceRequest) -> Result<()> {
        if self.get_worker_status() == WorkerStatusResponse_WorkerStatus::BUSY {
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

        let mut values_list_str = String::from("\"");
        values_list_str.push_str(&reduce_input_values.join("\",\n\""));
        values_list_str.push('"');

        let reduce_json_input =
            format!(
            "{{ \"key\": {}, \"values\": [{}] }}",
            reduce_options.get_intermediate_key(),
            values_list_str,
        );

        let worker_status_result = self.worker_status.lock();
        let operation_status_result = self.operation_status.lock();

        if let Ok(mut worker_status) = worker_status_result {
            *worker_status = WorkerStatusResponse_WorkerStatus::BUSY;
        } else {
            return Err("Failed to lock internal operation_handler values.".into());
        }

        if let Ok(mut operation_status) = operation_status_result {
            *operation_status = WorkerStatusResponse_OperationStatus::IN_PROGRESS;
        } else {
            return Err("Failed to lock internal operation_handler values.".into());
        }

        let child = Command::new(reduce_options.get_reducer_file_path())
            .arg("reduce")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start reduce operation process.")?;

        let operation_status_arc = Arc::clone(&self.operation_status);
        let worker_status_arc = Arc::clone(&self.worker_status);
        let reduce_result_arc = Arc::clone(&self.reduce_result);

        thread::spawn(move || {
            let result = reduce_operation_thread_impl(&reduce_json_input, child, reduce_result_arc);
            match result {
                Ok(_) => self::set_complete_status(worker_status_arc, operation_status_arc),
                Err(err) => {
                    log_reduce_operation_err(&err, &worker_status_arc, &operation_status_arc)
                }
            }
        });
        Ok(())
    }

    pub fn get_reduce_result(&self) -> Result<ReduceResponse> {
        match self.reduce_result.lock() {
            Err(_) => Err("No reduce result found.".into()),
            Ok(result) => {
                if let Some(ref reduce_response) = *result {
                    Ok(reduce_response.clone())
                } else {
                    Err("No reduce result found.".into())
                }
            }
        }
    }
}
