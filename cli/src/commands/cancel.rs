use clap::ArgMatches;
use grpc::RequestOptions;

use common::get_client_id;
use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService;
use errors::*;

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

    if resp.success {
        println!(
            "Succesfully cancelled MapReduce with ID: {}",
            resp.mapreduce_id
        );
    } else {
        println!("Unable to cancel MapReduce with ID: {}", resp.mapreduce_id);
    }
    Ok(())
}
