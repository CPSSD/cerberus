use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json;

use errors::*;
use super::operation_handler::OperationResources;

fn check_has_combine(resources: &OperationResources) -> Result<bool> {
    let absolute_path = resources
        .data_abstraction_layer
        .absolute_path(Path::new(&resources.binary_path))
        .chain_err(|| "Unable to get absolute path")?;
    let output = Command::new(absolute_path)
        .arg("has-combine")
        .output()
        .chain_err(|| "Error running MapReduce binary.")?;
    let output_str = String::from_utf8(output.stdout).unwrap();

    let has_combine = "yes" == output_str.trim();

    Ok(has_combine)
}

fn do_combine_operation(
    resources: &OperationResources,
    key: &str,
    values: Vec<serde_json::Value>,
) -> Result<serde_json::Value> {
    let combine_input = json!({
        "key": key.clone().to_owned(),
        "values": values,
    });
    let combine_input_str = serde_json::to_string(&combine_input).chain_err(
        || "Error seralizing combine operation input.",
    )?;

    let absolute_binary_path = resources
        .data_abstraction_layer
        .absolute_path(Path::new(&resources.binary_path))
        .chain_err(|| "Unable to get absolute path")?;
    let mut child = Command::new(absolute_binary_path)
        .arg("combine")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .chain_err(|| "Failed to start combine operation process.")?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(combine_input_str.as_bytes()).chain_err(
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

    let combine_result: serde_json::Value = serde_json::from_str(&output_str).chain_err(
        || "Error parsing reduce results.",
    )?;

    Ok(combine_result)
}

fn run_combine(resources: &OperationResources) -> Result<()> {
    let partition_map;
    {
        let operation_state = resources.operation_state.lock().unwrap();
        partition_map = operation_state.intermediate_map_results.clone();
    }

    let mut new_partition_map = HashMap::new();
    for (partition, values) in (&partition_map).iter() {
        let mut kv_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        for pair in values {
            let key = pair["key"].as_str().chain_err(
                || "Error parsing map output key to run combine.",
            )?;

            let value = pair["value"].clone();
            if value.is_null() {
                return Err("Error parsing map output value to run combine.".into());
            }

            let vec = kv_map.entry(key.to_string()).or_insert_with(Vec::new);
            vec.push(value);
        }

        let mut partition_results: Vec<serde_json::Value> = Vec::new();

        for (key, values) in (&kv_map).iter() {
            if values.len() > 1 {
                let pair = do_combine_operation(resources, key, values.clone())
                    .chain_err(|| "Failed to run combine operation.")?;

                partition_results.push(pair);
            } else if let Some(value) = values.first() {
                partition_results.push(json!({
                    "key": key,
                    "value": value
                }));
            }
        }

        new_partition_map.insert(*partition, partition_results);
    }

    let mut operation_state = resources.operation_state.lock().unwrap();
    operation_state.intermediate_map_results = new_partition_map;

    Ok(())
}

/// Optionally run a combine operation if it's implemented by the MapReduce binary.
pub fn optional_run_combine(resources: &OperationResources) -> Result<()> {
    let has_combine = check_has_combine(resources).chain_err(
        || "Error running has-combine command.",
    )?;

    if has_combine {
        return run_combine(resources);
    }

    Ok(())
}
