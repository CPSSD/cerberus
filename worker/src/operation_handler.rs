use bson;
use cerberus_proto::worker as pb;
use std::collections::HashMap;
use std::thread;
use std::path::PathBuf;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::fs;
use std::sync::{Arc, Mutex};
use std::process::{Child, Command, Stdio};
use libc::_SC_CLK_TCK;
use procinfo::pid::stat_self;
use serde_json;
use serde_json::Value;
use serde_json::Value::Array;
use uuid::Uuid;
use errors::*;
use util::output_error;
use master_interface::MasterInterface;

const WORKER_OUTPUT_DIRECTORY: &str = "/tmp/cerberus/";

/// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
struct OperationState {
    pub worker_status: pb::WorkerStatus,
    pub operation_status: pb::OperationStatus,
}

/// The `MapInput` is a struct used for serialising input data for a map operation.
#[derive(Default, Serialize)]
pub struct MapInput {
    pub key: String,
    pub value: String,
}

/// `ReduceOperation` stores the input for a reduce operation for a single key.
struct ReduceOperation {
    intermediate_key: String,
    input: String,
}

/// `ReduceOperationQueue` is a struct used for storing and executing a queue of `ReduceOperations`.
#[derive(Default)]
pub struct ReduceOperationQueue {
    queue: Vec<ReduceOperation>,
}

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
pub struct OperationHandler {
    operation_state: Arc<Mutex<OperationState>>,
    master_interface: Arc<Mutex<MasterInterface>>,

    reduce_operation_queue: Arc<Mutex<ReduceOperationQueue>>,
    output_dir_uuid: String,

    // Initial CPU time of the operation. This is used to calculate the total cpu time used for an
    // operation
    initial_cpu_time: Mutex<u64>,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            worker_status: pb::WorkerStatus::AVAILABLE,
            operation_status: pb::OperationStatus::UNKNOWN,
        }
    }
}

fn set_operation_handler_status(
    operation_state_arc: &Arc<Mutex<OperationState>>,
    worker_status: pb::WorkerStatus,
    operation_status: pb::OperationStatus,
) -> Result<()> {
    let mut operation_state = operation_state_arc.lock().unwrap();
    operation_state.worker_status = worker_status;
    operation_state.operation_status = operation_status;

    Ok(())
}

fn set_complete_status(operation_state_arc: &Arc<Mutex<OperationState>>) -> Result<()> {
    set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatus::AVAILABLE,
        pb::OperationStatus::COMPLETE,
    ).chain_err(|| "Could not set operation completed status.")
}

fn set_failed_status(operation_state_arc: &Arc<Mutex<OperationState>>) {
    let result = set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatus::AVAILABLE,
        pb::OperationStatus::FAILED,
    );

    if let Err(err) = result {
        output_error(&err.chain_err(|| "Could not set operation failed status."));
    }
}

fn send_reduce_result(
    master_interface_arc: &Arc<Mutex<MasterInterface>>,
    result: pb::ReduceResult,
) -> Result<()> {
    let master_interface = master_interface_arc.lock().unwrap();

    master_interface.return_reduce_result(result).chain_err(
        || "Error sending reduce result to master.",
    )?;
    Ok(())
}

fn send_map_result(
    master_interface_arc: &Arc<Mutex<MasterInterface>>,
    map_result: pb::MapResult,
) -> Result<()> {
    let master_interface = master_interface_arc.lock().unwrap();

    master_interface.return_map_result(map_result).chain_err(
        || "Error sending map result to master.",
    )?;
    Ok(())
}

fn failure_details_from_error(err: &Error) -> String {
    let mut failure_details = format!("{}", err);

    for e in err.iter().skip(1) {
        failure_details.push_str("\n");
        failure_details.push_str(&format!("caused by: {}", e));
    }
    failure_details
}

fn parse_map_results(map_result_string: &str, output_dir: &str) -> Result<HashMap<u64, String>> {
    let parse_value: Value = serde_json::from_str(map_result_string).chain_err(
        || "Error parsing map response.",
    )?;

    let mut map_results: HashMap<u64, String> = HashMap::new();

    let partition_map = match parse_value["partitions"].as_object() {
        None => return Err("Error parsing partition map.".into()),
        Some(map) => map,
    };

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

        map_results.insert(partition, (*file_path.to_string_lossy()).to_owned());
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
    map_input_value: &bson::Document,
    output_dir: &str,
    mut child: Child,
) -> Result<pb::MapResult> {
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

    let map_results = parse_map_results(&output_str, output_dir).chain_err(
        || "Error parsing map results.",
    )?;

    let mut response = pb::MapResult::new();
    response.set_status(pb::ResultStatus::SUCCESS);
    response.set_map_results(map_results);

    Ok(response)
}

fn get_cpu_time() -> u64 {
    // We can panic in this case. This is beyond our control and would mostly be caused by a very
    // critical error.
    let stat = stat_self().unwrap();
    (stat.utime + stat.stime + stat.cstime + stat.cutime) as u64 / (_SC_CLK_TCK as u64)
}

impl ReduceOperationQueue {
    fn new() -> Self {
        Default::default()
    }

    fn run_reducer(
        &self,
        reduce_options: &ReduceOptions,
        reduce_operation: &ReduceOperation,
        mut child: Child,
    ) -> Result<()> {
        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(reduce_operation.input.as_bytes())
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

        let reduce_results: Value = serde_json::from_str(&output_str).chain_err(
            || "Error parsing reduce results.",
        )?;
        let reduce_results_pretty: String = serde_json::to_string_pretty(&reduce_results)
            .chain_err(|| "Error prettifying reduce results")?;

        let mut file_path = PathBuf::new();
        file_path.push(reduce_options.output_directory.clone());
        file_path.push(reduce_operation.intermediate_key.clone());


        let mut file = File::create(file_path).chain_err(
            || "Failed to create reduce output file.",
        )?;

        file.write_all(reduce_results_pretty.as_bytes()).chain_err(
            || "Failed to write to reduce output file.",
        )?;
        Ok(())
    }

    fn perform_reduce_operation(
        &mut self,
        reduce_options: &ReduceOptions,
        reduce_operation: &ReduceOperation,
    ) -> Result<()> {
        let child = Command::new(reduce_options.reducer_file_path.to_owned())
            .arg("reduce")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start reduce operation process.")?;

        fs::create_dir_all(reduce_options.output_directory.to_owned())
            .chain_err(|| "Failed to create output directory")?;

        self.run_reducer(reduce_options, reduce_operation, child)
            .chain_err(|| "Error running reducer.")?;

        Ok(())
    }

    fn perform_next_reduce_operation(&mut self, reduce_options: &ReduceOptions) -> Result<()> {
        if let Some(reduce_operation) = self.queue.pop() {
            self.perform_reduce_operation(reduce_options, &reduce_operation)
                .chain_err(|| "Error performing reduce operation.")?;
        }
        Ok(())
    }

    fn is_queue_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn set_queue(&mut self, new_queue: Vec<ReduceOperation>) {
        self.queue = new_queue;
    }
}

#[derive(Serialize)]
struct ReduceInput {
    pub key: String,
    pub values: Vec<Value>,
}

struct ReduceOptions {
    reducer_file_path: String,
    output_directory: String,
}

impl OperationHandler {
    pub fn new(master_interface: Arc<Mutex<MasterInterface>>) -> Self {
        OperationHandler {
            operation_state: Arc::new(Mutex::new(OperationState::new())),
            reduce_operation_queue: Arc::new(Mutex::new(ReduceOperationQueue::new())),

            master_interface: master_interface,
            output_dir_uuid: Uuid::new_v4().to_string(),

            initial_cpu_time: Mutex::new(0),
        }
    }

    pub fn get_worker_status(&self) -> pb::WorkerStatus {
        let operation_state = self.operation_state.lock().unwrap();

        operation_state.worker_status
    }

    pub fn get_worker_operation_status(&self) -> pb::OperationStatus {
        let operation_state = self.operation_state.lock().unwrap();

        operation_state.operation_status
    }

    pub fn set_operation_handler_busy(&self) -> Result<()> {
        let mut operation_state = self.operation_state.lock().unwrap();

        operation_state.worker_status = pb::WorkerStatus::BUSY;
        operation_state.operation_status = pb::OperationStatus::IN_PROGRESS;

        Ok(())
    }

    pub fn perform_map(&mut self, map_options: &pb::PerformMapRequest) -> Result<()> {
        {
            *self.initial_cpu_time.lock().unwrap() = get_cpu_time();
        }

        info!(
            "Performing map operation. mapper={} input={}",
            map_options.mapper_file_path,
            map_options.input_file_path
        );

        if self.get_worker_status() == pb::WorkerStatus::BUSY {
            return Err("Worker is busy.".into());
        }
        self.set_operation_handler_busy().chain_err(
            || "Couldn't set operation handler busy.",
        )?;

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

        let operation_state_arc = Arc::clone(&self.operation_state);
        let master_interface_arc = Arc::clone(&self.master_interface);

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
        output_path.push(&self.output_dir_uuid);
        output_path.push("map");

        fs::create_dir_all(output_path.clone()).chain_err(
            || "Failed to create output directory",
        )?;

        let initial_cpu_time = *self.initial_cpu_time.lock().unwrap();

        thread::spawn(move || {
            let result = map_operation_thread_impl(
                &map_input_document,
                &*output_path.to_string_lossy(),
                child,
            );

            match result {
                Ok(mut map_result) => {
                    map_result.set_cpu_time(get_cpu_time() - initial_cpu_time);
                    if let Err(err) = send_map_result(&master_interface_arc, map_result) {
                        log_map_operation_err(err, &operation_state_arc)
                    } else {
                        info!("Map operation completed sucessfully.");
                        let result = set_complete_status(&operation_state_arc);
                        if let Err(err) = result {
                            log_map_operation_err(err, &operation_state_arc)
                        }
                    }
                }
                Err(err) => {
                    let mut map_result = pb::MapResult::new();
                    map_result.set_status(pb::ResultStatus::FAILURE);
                    map_result.set_cpu_time(get_cpu_time() - initial_cpu_time);
                    map_result.set_failure_details(failure_details_from_error(&err));

                    if let Err(err) = send_map_result(&master_interface_arc, map_result) {
                        error!("Could not send map operation failed: {}", err);
                    }
                    log_map_operation_err(err, &operation_state_arc);
                }
            }
        });

        Ok(())
    }

    fn create_reduce_operations(
        &self,
        reduce_request: &pb::PerformReduceRequest,
    ) -> Result<Vec<ReduceOperation>> {
        let mut reduce_map: HashMap<String, Vec<Value>> = HashMap::new();

        for reduce_input_file in reduce_request.get_input_file_paths() {
            let file = File::open(reduce_input_file).chain_err(
                || "Couldn't open input file.",
            )?;

            let mut buf_reader = BufReader::new(file);
            let mut reduce_input = String::new();
            buf_reader.read_to_string(&mut reduce_input).chain_err(
                || "Couldn't read map input file.",
            )?;

            let parse_value: Value = serde_json::from_str(&reduce_input).chain_err(
                || "Error parsing map response.",
            )?;

            if let Array(ref pairs) = parse_value {
                for pair in pairs {
                    let key = pair["key"].as_str().chain_err(
                        || "Error parsing reduce input.",
                    )?;

                    let value = pair["value"].clone();
                    if value.is_null() {
                        return Err("Error parsing reduce input.".into());
                    }

                    let reduce_array = reduce_map.entry(key.to_owned()).or_insert_with(Vec::new);
                    reduce_array.push(value);
                }
            }
        }

        let mut reduce_operations: Vec<ReduceOperation> = Vec::new();
        for (intermediate_key, reduce_array) in reduce_map {
            let reduce_input = ReduceInput {
                key: intermediate_key.to_owned(),
                values: reduce_array,
            };
            let reduce_operation = ReduceOperation {
                intermediate_key: intermediate_key.to_owned(),
                input: serde_json::to_string(&reduce_input).chain_err(
                    || "Error seralizing reduce operation input.",
                )?,
            };
            reduce_operations.push(reduce_operation);
        }
        Ok(reduce_operations)
    }

    pub fn perform_reduce(&mut self, reduce_request: &pb::PerformReduceRequest) -> Result<()> {
        {
            *self.initial_cpu_time.lock().unwrap() = get_cpu_time();
        }

        info!(
            "Performing reduce operation. reducer={}",
            reduce_request.reducer_file_path
        );

        if self.get_worker_status() == pb::WorkerStatus::BUSY {
            return Err("Worker is busy.".into());
        }
        self.set_operation_handler_busy().chain_err(
            || "Error performing reduce.",
        )?;

        let reduce_operations = self.create_reduce_operations(reduce_request).chain_err(
            || "Error creating reduce operations from input.",
        )?;

        {
            let mut reduce_queue = self.reduce_operation_queue.lock().unwrap();
            reduce_queue.set_queue(reduce_operations);
        }

        let reduce_options = ReduceOptions {
            reducer_file_path: reduce_request.get_reducer_file_path().to_owned(),
            output_directory: reduce_request.get_output_directory().to_owned(),
        };

        self.run_reduce_queue(reduce_options);

        Ok(())
    }

    fn run_reduce_queue(&self, reduce_options: ReduceOptions) {
        let reduce_queue = Arc::clone(&self.reduce_operation_queue);
        let operation_state_arc = Arc::clone(&self.operation_state);
        let master_interface_arc = Arc::clone(&self.master_interface);

        let initial_cpu_time = *self.initial_cpu_time.lock().unwrap();

        thread::spawn(move || {
            let mut reduce_err = None;
            loop {
                let mut reduce_queue = reduce_queue.lock().unwrap();

                if reduce_queue.is_queue_empty() {
                    break;
                } else {
                    let result = reduce_queue.perform_next_reduce_operation(&reduce_options);
                    if let Err(err) = result {
                        reduce_err = Some(err);
                        break;
                    }
                }
            }

            if let Some(err) = reduce_err {
                let mut response = pb::ReduceResult::new();
                response.set_status(pb::ResultStatus::FAILURE);
                response.set_failure_details(failure_details_from_error(&err));

                let result = send_reduce_result(&master_interface_arc, response);
                if let Err(err) = result {
                    error!("Error sending reduce failed: {}", err);
                }

                log_reduce_operation_err(err, &operation_state_arc);
            } else {
                let mut response = pb::ReduceResult::new();
                response.set_status(pb::ResultStatus::SUCCESS);
                response.set_cpu_time(get_cpu_time() - initial_cpu_time);
                let result = send_reduce_result(&master_interface_arc, response);

                match result {
                    Ok(_) => {
                        let result = set_complete_status(&operation_state_arc);
                        if let Err(err) = result {
                            error!("Error setting reduce complete: {}", err);
                        } else {
                            info!("Reduce operation completed sucessfully.");
                        }
                    }
                    Err(err) => {
                        error!("Error sending reduce result: {}", err);
                        set_failed_status(&operation_state_arc);
                    }
                }
            }
        });
    }

    pub fn update_worker_status(&self) -> Result<()> {
        let worker_status = self.get_worker_status();
        let operation_status = self.get_worker_operation_status();

        let master_interface = self.master_interface.lock().unwrap();
        master_interface.update_worker_status(worker_status, operation_status)
    }
}
