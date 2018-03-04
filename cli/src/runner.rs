use std::env;
use std::fs;
use std::io::prelude::{Read, Write};
use std::path::Path;

use chrono::Local;
use clap::ArgMatches;
use grpc::RequestOptions;
use uuid::Uuid;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService; // do not use
use errors::*;

// Directory of client ID in the users home directory.
const CLIENT_ID_DIR: &str = ".local/share/";
// Client ID file name.
const CLIENT_ID_FILE: &str = "cerberus";

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

fn create_new_client_id(dir: &str, file_path: &str) -> Result<String> {
    // Create new client id as we do not have one saved.
    fs::create_dir_all(dir).chain_err(
        || "Error creating new client id.",
    )?;

    let client_id = Uuid::new_v4().to_string();
    let mut file = fs::File::create(file_path).chain_err(
        || "Error creating new client id.",
    )?;

    file.write_all(client_id.as_bytes()).chain_err(
        || "Error creating new client id.",
    )?;

    Ok(client_id)
}

fn get_client_id() -> Result<String> {
    let mut path_buf = env::home_dir().chain_err(|| "Error getting client id.")?;

    path_buf.push(CLIENT_ID_DIR);
    let dir = path_buf
        .to_str()
        .chain_err(|| "Error getting client id.")?
        .to_owned();

    path_buf.push(CLIENT_ID_FILE);
    let file_path = path_buf.to_str().chain_err(|| "Error getting client id.")?;

    if fs::metadata(file_path).is_ok() {
        let mut file = fs::File::open(file_path).chain_err(
            || "Error getting client id.",
        )?;

        let mut client_id = String::new();
        file.read_to_string(&mut client_id).chain_err(
            || "Error getting client id.",
        )?;

        return Ok(client_id);
    }

    create_new_client_id(&dir, file_path).chain_err(|| "Error getting client id.")
}

pub fn run(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut input = matches
        .value_of("input")
        .chain_err(|| "Input directory cannot be empty")?
        .to_owned();

    input = verify_valid_path(&input).chain_err(
        || "Invalid input path.",
    )?;

    let output = matches.value_of("output").unwrap_or("");
    if !output.is_empty() {
        verify_valid_path(output).chain_err(
            || "Invalid output path",
        )?;
    }

    let mut binary = matches
        .value_of("binary")
        .chain_err(|| "Binary cannot be empty")?
        .to_owned();

    binary = verify_valid_path(&binary).chain_err(
        || "Invalid binary path.",
    )?;

    let mut req = pb::MapReduceRequest::new();
    req.set_binary_path(binary.to_owned());
    req.set_input_directory(input.to_owned());
    req.set_client_id(get_client_id()?);
    req.set_output_directory(output.to_owned());

    let res = client
        .perform_map_reduce(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to schedule MapReduce")?
        .1;

    println!("MapReduce {} scheduled", res.get_mapreduce_id().to_owned());

    Ok(())
}

pub fn cluster_status(client: &grpc_pb::MapReduceServiceClient) -> Result<()> {
    let res = client
        .cluster_status(RequestOptions::new(), pb::EmptyMessage::new())
        .wait()
        .chain_err(|| "Failed to get cluster status")?
        .1;
    println!(
        "Workers:\t{}\nQueue:\t\t{}",
        res.get_workers(),
        res.get_queue_size()
    );

    Ok(())
}

pub fn cancel(client: &grpc_pb::MapReduceServiceClient, args: Option<&ArgMatches>) -> Result<()> {
    let mut req = pb::MapReduceCancelRequest::new();
    req.set_client_id(get_client_id().chain_err(|| "Error getting client id")?);
    if let Some(sub) = args {
        if let Some(id) = sub.value_of("id") {
            req.set_mapreduce_id(id.to_owned());
        }
    }

    let resp = client
        .cancel_map_reduce(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to cancel MapReduce")?
        .1;

    println!(
        "Succesfully cancelled MapReduce with ID: {}",
        resp.mapreduce_id
    );
    Ok(())
}

pub fn status(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut req = pb::MapReduceStatusRequest::new();
    req.set_client_id(get_client_id()?);

    if let Some(id) = matches.value_of("job_id") {
        req.set_mapreduce_id(id.to_owned());
    }

    let res = client
        .map_reduce_status(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to get MapReduce Status")?
        .1;

    let reports = res.get_reports();
    if reports.is_empty() {
        println!("No jobs for this client on cluster.");
    } else {
        for rep in reports {
            print_table(rep);
        }
    }

    Ok(())
}

fn print_table(rep: &pb::MapReduceReport) {
    let id = rep.get_mapreduce_id();
    let output = rep.get_output_directory();

    let status: String = match rep.get_status() {
        pb::Status::UNKNOWN => "UNKNOWN".to_owned(),
        pb::Status::DONE => {
            let time_taken = rep.get_done_timestamp() - rep.get_started_timestamp();
            format!("DONE ({}s)", time_taken)
        }
        pb::Status::IN_PROGRESS => {
            format!(
                "IN_PROGRESS ({})",
                get_time_offset(rep.get_started_timestamp())
            )
        }
        pb::Status::IN_QUEUE => format!("IN_QUEUE ({})", rep.get_queue_length()),
        pb::Status::FAILED => format!("FAILED\n{}", rep.get_failure_details()).to_owned(),
    };

    table!(["MRID", id], ["Status", status], ["Output", output]).printstd();
}

fn get_time_offset(offset: i64) -> String {
    format!("{}s", Local::now().timestamp() - offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_time_offset_test() {
        assert_eq!(get_time_offset(Local::now().timestamp() - 10), "10s");
    }
}
