use chrono::Local;
use clap::ArgMatches;
use errors::*;
use grpc::RequestOptions;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService; // do not use

pub fn run(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let input = matches.value_of("input").chain_err(
        || "Input directory cannot be empty",
    )?;

    let binary = matches.value_of("binary").chain_err(
        || "Binary cannot be empty",
    )?;

    let mut req = pb::MapReduceRequest::new();
    req.set_binary_path(binary.to_owned());
    req.set_input_directory(input.to_owned());
    // TODO(voy): Replace it with generated ClientID.
    req.set_client_id("abc".to_owned());
    // TODO(voy): Add the output directory once its implemented in the proto.

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

    for rep in res.get_reports() {
        print_table(rep);
    }

    Ok(())
}

fn print_table(rep: &pb::MapReduceStatusResponse_MapReduceReport) {
    let id = rep.get_mapreduce_id();

    let status: String = match rep.get_status() {
        pb::MapReduceStatusResponse_MapReduceReport_Status::UNKNOWN => "UNKNOWN".to_owned(),
        pb::MapReduceStatusResponse_MapReduceReport_Status::DONE => {
            format!("DONE ({})", get_time_offset(rep.get_done_timestamp()))
        }
        pb::MapReduceStatusResponse_MapReduceReport_Status::IN_PROGRESS => {
            format!(
                "IN_PROGRESS ({})",
                get_time_offset(rep.get_started_timestamp())
            )
        }
        pb::MapReduceStatusResponse_MapReduceReport_Status::IN_QUEUE => {
            format!("IN_QUEUE ({})", rep.get_queue_length())
        }
        pb::MapReduceStatusResponse_MapReduceReport_Status::FAILED => "FAILED".to_owned(),
    };

    table!(["MRID", id], ["Status", status]).printstd();
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
