use chrono::Local;
use clap::ArgMatches;
use errors::*;
use grpc::RequestOptions;
use std::path::Path;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService; // do not use

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
    // TODO(voy): Replace it with generated ClientID.
    req.set_client_id("abc".to_owned());
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

pub fn status(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut req = pb::MapReduceStatusRequest::new();
    req.set_client_id("abc".to_owned());
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
        pb::Status::FAILED => format!("FAILED ({})", rep.get_failure_details()).to_owned(),
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
