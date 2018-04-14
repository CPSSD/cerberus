use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json;

use super::operation_handler::{OperationResources, PartitionMap};
use errors::*;

#[derive(Serialize)]
struct CombineInput {
    pub key: serde_json::Value,
    pub values: Vec<serde_json::Value>,
}

fn check_has_combine(resources: &OperationResources) -> Result<bool> {
    let absolute_path = resources
        .data_abstraction_layer
        .get_local_file(Path::new(&resources.binary_path))
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
    combine_input: &[CombineInput],
) -> Result<serde_json::Value> {
    let combine_input_str = serde_json::to_string(&combine_input)
        .chain_err(|| "Error seralizing combine operation input.")?;

    let absolute_binary_path = resources
        .data_abstraction_layer
        .get_local_file(Path::new(&resources.binary_path))
        .chain_err(|| "Unable to get absolute path")?;
    let mut child = Command::new(absolute_binary_path)
        .arg("combine")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .chain_err(|| "Failed to start combine operation process.")?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(combine_input_str.as_bytes())
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

    let combine_results: serde_json::Value =
        serde_json::from_str(&output_str).chain_err(|| "Error parsing combine results.")?;

    Ok(combine_results)
}

fn run_combine(resources: &OperationResources, partition_map: &mut PartitionMap) -> Result<()> {
    for (_, kv_map) in partition_map.iter_mut() {
        // Do one combine for each partition.
        let mut combine_keys = Vec::new();
        let mut combine_inputs = Vec::new();
        for (key, values) in kv_map.iter() {
            if values.len() > 1 {
                combine_keys.push(key.to_owned());

                let key_value: serde_json::Value =
                    serde_json::from_str(key).chain_err(|| "Error parsing key")?;
                let combine_input = CombineInput {
                    key: key_value,
                    values: values.to_owned(),
                };
                combine_inputs.push(combine_input);
            }
        }

        if combine_inputs.is_empty() {
            continue;
        }

        let results = do_combine_operation(resources, &combine_inputs)
            .chain_err(|| "Failed to run combine operation.")?;

        // Use results of combine operations.
        if let serde_json::Value::Array(results) = results {
            for (i, result) in results.iter().enumerate() {
                let values = kv_map
                    .get_mut(&combine_keys[i])
                    .chain_err(|| "Error running combine")?;

                values.clear();

                if let serde_json::Value::Array(ref new_values) = *result {
                    for value in new_values {
                        values.push(value.to_owned());
                    }
                } else {
                    values.push(result.to_owned());
                }
            }
        } else {
            return Err("Error parsing combine output as Array".into());
        }
    }

    Ok(())
}

/// Optionally run a combine operation if it's implemented by the `MapReduce` binary.
pub fn optional_run_combine(
    resources: &OperationResources,
    partition_map: &mut PartitionMap,
) -> Result<()> {
    let has_combine =
        check_has_combine(resources).chain_err(|| "Error running has-combine command.")?;

    if has_combine {
        return run_combine(resources, partition_map);
    }

    Ok(())
}
