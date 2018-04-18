use std::path::Path;

use clap::ArgMatches;
use grpc::RequestOptions;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService;
use common::get_client_id;
use errors::*;

// Default priority applied to jobs.
const DEFAULT_PRIORITY: &str = "3";
const DEFAULT_MAP_SIZE: &str = "64";

fn verify_valid_path(path_str: &str) -> Result<String> {
    let path = Path::new(path_str);

    if !path.is_absolute() {
        return Err("Paths passed to the CLI must be absolute.".into());
    }

    let path_option = path.to_str();
    match path_option {
        Some(res) => Ok(res.to_owned()),
        None => Err("Invalid characters in path.".into()),
    }
}

fn get_priority(matches: &ArgMatches) -> Result<u32> {
    let priority_str = matches.value_of("priority").unwrap_or(DEFAULT_PRIORITY);
    let priority: u32 = match priority_str.parse() {
        Ok(val) => val,
        Err(err) => {
            return Err(format!(
                "Error occured while converting '{}' to a u32: {}",
                priority_str, err
            ).into());
        }
    };

    if priority < 1 || priority > 10 {
        return Err(format!(
            "Priority can only be between 1 and 10. {} is not in this range",
            priority
        ).into());
    }
    Ok(priority)
}

fn get_map_size(matches: &ArgMatches) -> Result<u32> {
    let map_size_str = matches.value_of("map_size").unwrap_or(DEFAULT_MAP_SIZE);
    let map_size: u32 = match map_size_str.parse() {
        Ok(val) => val,
        Err(err) => {
            return Err(format!(
                "Error occured while converting '{}' to a u32: {}",
                map_size_str, err
            ).into());
        }
    };
    if map_size < 1 {
        return Err("Map Size must be greater than or equal to 1 megabyte".into());
    }
    Ok(map_size)
}

pub fn run(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut input = matches
        .value_of("input")
        .chain_err(|| "Input directory cannot be empty")?
        .to_owned();

    input = verify_valid_path(&input).chain_err(|| "Invalid input path.")?;

    let output = matches.value_of("output").unwrap_or("");
    if !output.is_empty() {
        verify_valid_path(output).chain_err(|| "Invalid output path")?;
    }

    let mut binary = matches
        .value_of("binary")
        .chain_err(|| "Binary cannot be empty")?
        .to_owned();

    let map_size: u32 = get_map_size(matches).chain_err(|| "Unable to get map size")?;

    binary = verify_valid_path(&binary).chain_err(|| "Invalid binary path.")?;

    let priority = get_priority(matches)?;

    let mut req = pb::MapReduceRequest::new();
    req.set_binary_path(binary.to_owned());
    req.set_input_directory(input.to_owned());
    req.set_client_id(get_client_id()?);
    req.set_output_directory(output.to_owned());
    req.set_priority(priority);
    req.set_map_size(map_size);

    let res = client
        .perform_map_reduce(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to schedule MapReduce")?
        .1;

    println!("MapReduce {} scheduled", res.get_mapreduce_id().to_owned());

    Ok(())
}
