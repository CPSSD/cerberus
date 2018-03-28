use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json;

use errors::*;
use super::operation_handler::{OperationResources, PartitionMap};

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
    key: &str,
    values: &[serde_json::Value],
) -> Result<serde_json::Value> {
    let combine_input = json!({
        "key": key.to_owned(),
        "values": values,
    });
    let combine_input_str = serde_json::to_string(&combine_input).chain_err(
        || "Error seralizing combine operation input.",
    )?;

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

    let stderr_str = String::from_utf8(output.stderr).chain_err(
        || "Error accessing payload output.",
    )?;

    if !stderr_str.is_empty() {
        return Err(
            format!("MapReduce binary failed with stderr:\n {}", stderr_str).into(),
        );
    }

    let combine_results: serde_json::Value = serde_json::from_str(&output_str).chain_err(
        || "Error parsing combine results.",
    )?;

    Ok(combine_results)
}

fn run_combine(resources: &OperationResources, partition_map: &mut PartitionMap) -> Result<()> {
    for (_, kv_map) in partition_map.iter_mut() {
        for (key, values) in kv_map.iter_mut() {
            if values.len() > 1 {
                let results = do_combine_operation(resources, key, values).chain_err(
                    || "Failed to run combine operation.",
                )?;

                values.clear();

                if let serde_json::Value::Array(new_values) = results {
                    for value in new_values {
                        values.push(value);
                    }
                } else {
                    values.push(results);
                }
            }
        }
    }

    Ok(())
}

/// Optionally run a combine operation if it's implemented by the `MapReduce` binary.
pub fn optional_run_combine(
    resources: &OperationResources,
    partition_map: &mut PartitionMap,
) -> Result<()> {
    let has_combine = check_has_combine(resources).chain_err(
        || "Error running has-combine command.",
    )?;

    if has_combine {
        return run_combine(resources, partition_map);
    }

    Ok(())
}
