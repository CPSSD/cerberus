use chrono::Local;
use clap::ArgMatches;
use grpc::RequestOptions;

use common::get_client_id;
use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService;
use errors::*;

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
        pb::Status::CANCELLED => "CANCELLED".to_owned(),
    };

    table!(["MRID", id], ["Status", status], ["Output", output]).printstd();
}

fn get_time_offset(offset: i64) -> String {
    format!("{}s", Local::now().timestamp() - offset)
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
