use std::collections::HashMap;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json;

use errors::*;
use master_interface::MasterInterface;
use super::io;
use super::operation_handler;
use super::operation_handler::OperationResources;
use super::state::OperationState;
use util::output_error;
use util::data_layer::AbstractionLayer;
use worker_interface::WorkerInterface;

use cerberus_proto::worker as pb;

#[cfg(test)]
use mocktopus;
#[cfg(test)]
use mocktopus::macros::*;
#[cfg(test)]
use std;

/// `ReduceOperation` stores the input for a reduce operation for a single key.
struct ReduceOperation {
    intermediate_key: String,
    input: String,
}

#[derive(Serialize)]
struct ReduceInput {
    pub key: serde_json::Value,
    pub values: Vec<serde_json::Value>,
}

struct ReduceOptions {
    reducer_file_path: String,
    output_directory: String,
    task_id: String,
}

/// `ReduceOperationQueue` is a struct used for storing and executing a queue of `ReduceOperations`.
#[derive(Default)]
pub struct ReduceOperationQueue {
    queue: Vec<ReduceOperation>,
}

fn run_reducer(
    reduce_options: &ReduceOptions,
    reduce_operation: &ReduceOperation,
    data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
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

    let stderr_str = String::from_utf8(output.stderr).chain_err(
        || "Error accessing payload output.",
    )?;

    if !stderr_str.is_empty() {
        return Err(
            format!("MapReduce binary failed with stderr:\n {}", stderr_str).into(),
        );
    }

    let reduce_results: serde_json::Value = serde_json::from_str(&output_str).chain_err(
        || "Error parsing reduce results.",
    )?;
    let reduce_results_pretty: String = serde_json::to_string_pretty(&reduce_results).chain_err(
        || "Error prettifying reduce results",
    )?;

    let mut file_path = PathBuf::new();
    file_path.push(reduce_options.output_directory.clone());
    file_path.push(reduce_operation.intermediate_key.clone());

    io::write(
        data_abstraction_layer_arc,
        file_path,
        reduce_results_pretty.as_bytes(),
    ).chain_err(|| "Failed to write reduce output.")?;
    Ok(())
}

#[cfg_attr(test, mockable)]
impl ReduceOperationQueue {
    pub fn new() -> Self {
        Default::default()
    }

    fn perform_reduce_operation(
        &mut self,
        reduce_options: &ReduceOptions,
        reduce_operation: &ReduceOperation,
        data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
    ) -> Result<()> {
        let absolute_path = data_abstraction_layer_arc
            .get_local_file(Path::new(&reduce_options.reducer_file_path))
            .chain_err(|| "Unable to get absolute path")?;
        let child = Command::new(absolute_path)
            .arg("reduce")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .chain_err(|| "Failed to start reduce operation process.")?;

        data_abstraction_layer_arc
            .create_dir_all(Path::new(&reduce_options.output_directory))
            .chain_err(|| "Failed to create output directory")?;

        run_reducer(
            reduce_options,
            reduce_operation,
            data_abstraction_layer_arc,
            child,
        ).chain_err(|| "Error running reducer.")?;

        Ok(())
    }

    fn perform_next_reduce_operation(
        &mut self,
        reduce_options: &ReduceOptions,
        data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
    ) -> Result<()> {
        if let Some(reduce_operation) = self.queue.pop() {
            self.perform_reduce_operation(
                reduce_options,
                &reduce_operation,
                data_abstraction_layer_arc,
            ).chain_err(|| "Error performing reduce operation.")?;
        } else {
            return Err("No reduce operations in queue".into());
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

fn log_reduce_operation_err(err: Error, operation_state_arc: &Arc<Mutex<OperationState>>) {
    output_error(&err.chain_err(|| "Error running reduce operation."));
    operation_handler::set_failed_status(operation_state_arc);
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

fn create_reduce_operations(
    reduce_request: &pb::PerformReduceRequest,
    output_uuid: &str,
) -> Result<Vec<ReduceOperation>> {
    let mut reduce_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    for reduce_input_file in reduce_request.get_input_file_paths() {
        // TODO: Run these operations in parallel as networks can be slow
        let reduce_input = WorkerInterface::get_data(reduce_input_file, output_uuid)
            .chain_err(|| "Couldn't read reduce input file")?;

        let parsed_value: serde_json::Value = serde_json::from_str(&reduce_input).chain_err(
            || "Error parsing map response.",
        )?;

        if let serde_json::Value::Array(ref pairs) = parsed_value {
            for pair in pairs {
                let key = pair["key"].to_string();

                let value = pair["value"].clone();
                if value.is_null() {
                    return Err("Error parsing reduce input value.".into());
                }

                let reduce_array = reduce_map.entry(key.to_owned()).or_insert_with(Vec::new);
                reduce_array.push(value);
            }
        }
    }

    let mut reduce_operations: Vec<ReduceOperation> = Vec::new();
    for (intermediate_key, reduce_array) in reduce_map {
        let key_value: serde_json::Value = serde_json::from_str(&intermediate_key).chain_err(
            || "Error parsing intermediate_key",
        )?;

        let reduce_input = ReduceInput {
            key: key_value.clone(),
            values: reduce_array,
        };

        let intermediate_key_string = {
            if let Some(key_str) = key_value.as_str() {
                key_str.to_string()
            } else {
                key_value.to_string()
            }
        };

        let reduce_operation = ReduceOperation {
            intermediate_key: intermediate_key_string,
            input: serde_json::to_string(&reduce_input).chain_err(
                || "Error seralizing reduce operation input.",
            )?,
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

    if operation_handler::get_worker_status(&resources.operation_state) == pb::WorkerStatus::BUSY {
        warn!("Reduce operation requested while worker is busy");
        return Err("Worker is busy.".into());
    }
    operation_handler::set_busy_status(&resources.operation_state);

    let result = internal_perform_reduce(reduce_request, resources, output_uuid);
    if let Err(err) = result {
        log_reduce_operation_err(err, &resources.operation_state);
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
    let reduce_operations = create_reduce_operations(reduce_request, output_uuid)
        .chain_err(|| "Error creating reduce operations from input.")?;

    let reduce_options = ReduceOptions {
        reducer_file_path: reduce_request.get_reducer_file_path().to_owned(),
        output_directory: reduce_request.get_output_directory().to_owned(),
        task_id: reduce_request.get_task_id().to_owned(),
    };

    run_reduce_queue(reduce_options, resources, reduce_operations);

    Ok(())
}

fn handle_reduce_queue_finished(
    reduce_err: Option<Error>,
    resources: &OperationResources,
    initial_cpu_time: u64,
    task_id: &str,
) {
    if let Some(err) = reduce_err {
        let mut response = pb::ReduceResult::new();
        response.set_status(pb::ResultStatus::FAILURE);
        response.set_failure_details(operation_handler::failure_details_from_error(&err));
        response.set_task_id(task_id.to_owned());

        let result = send_reduce_result(&resources.master_interface, response);
        if let Err(err) = result {
            error!("Error sending reduce failed: {}", err);
        }

        log_reduce_operation_err(err, &resources.operation_state);
    } else {
        let mut response = pb::ReduceResult::new();
        response.set_status(pb::ResultStatus::SUCCESS);
        response.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
        response.set_task_id(task_id.to_owned());

        let result = send_reduce_result(&resources.master_interface, response);

        match result {
            Ok(_) => {
                operation_handler::set_complete_status(&resources.operation_state);
                info!("Reduce operation completed sucessfully.");
            }
            Err(err) => {
                error!("Error sending reduce result: {}", err);
                operation_handler::set_failed_status(&resources.operation_state);
            }
        }
    }
}

fn run_reduce_queue(
    reduce_options: ReduceOptions,
    resources: &OperationResources,
    reduce_operations: Vec<ReduceOperation>,
) {
    let initial_cpu_time = resources.operation_state.lock().unwrap().initial_cpu_time;
    let resources = resources.clone();

    thread::spawn(move || {
        let mut reduce_err = None;

        let mut reduce_queue = ReduceOperationQueue::new();
        reduce_queue.set_queue(reduce_operations);

        loop {
            if reduce_queue.is_queue_empty() {
                break;
            } else {

                // Make sure the job hasn't been cancelled before continuing.
                {
                    let cancelled = {
                        let operation_state = resources.operation_state.lock().unwrap();
                        operation_state.task_cancelled(&reduce_options.task_id)
                    };
                    if cancelled {
                        operation_handler::set_cancelled_status(&resources.operation_state);
                        println!(
                            "Succesfully cancelled reduce task: {}",
                            &reduce_options.task_id
                        );
                        return;
                    }
                }

                let result = reduce_queue.perform_next_reduce_operation(
                    &reduce_options,
                    &resources.data_abstraction_layer,
                );
                if let Err(err) = result {
                    reduce_err = Some(err);
                    break;
                }
            }
        }

        handle_reduce_queue_finished(
            reduce_err,
            &resources,
            initial_cpu_time,
            &reduce_options.task_id,
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use mocktopus::mocking::*;

    use util::data_layer::NullAbstractionLayer;

    #[test]
    fn test_reduce_operation_queue() {
        ReduceOperationQueue::perform_reduce_operation.mock_safe(
            |_,
             _,
             _,
             _| {
                return MockResult::Return(Ok(()));
            },
        );

        let reduce_operation = ReduceOperation {
            input: "foo".to_owned(),
            intermediate_key: "bar".to_owned(),
        };
        let mut reduce_operations = Vec::new();
        reduce_operations.push(reduce_operation);

        let mut reduce_queue = ReduceOperationQueue::new();
        assert!(reduce_queue.is_queue_empty());

        reduce_queue.set_queue(reduce_operations);
        assert!(!reduce_queue.is_queue_empty());

        let reduce_options = ReduceOptions {
            output_directory: "foo".to_owned(),
            reducer_file_path: "bar".to_owned(),
            task_id: "task1".to_owned(),
        };

        let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync> =
            Arc::new(NullAbstractionLayer);

        let result =
            reduce_queue.perform_next_reduce_operation(&reduce_options, &data_abstraction_layer);
        assert!(!result.is_err());
        assert!(reduce_queue.is_queue_empty());

        let result =
            reduce_queue.perform_next_reduce_operation(&reduce_options, &data_abstraction_layer);
        assert!(result.is_err());
    }
}
