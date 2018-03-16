use grpc::RequestOptions;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService;
use errors::*;

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
