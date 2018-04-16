use std::sync::Arc;

use grpc::RequestOptions;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerLogService;
use errors::*;
use worker_management::WorkerManager;

pub fn fetch_worker_log(worker_id: &str, worker_manager: &Arc<WorkerManager>) -> Result<String> {
    let worker_addr = worker_manager
        .get_worker_address(worker_id)
        .chain_err(|| "Failed to get worker address")?;

    let client = grpc_pb::WorkerLogServiceClient::new_plain(
        &worker_addr.ip().to_string(),
        worker_addr.port(),
        Default::default(),
    ).chain_err(|| format!("Error building client for worker {}", worker_addr))?;

    info!("Fetching log file from worker {}", worker_id);
    let response = client
        .get_worker_logs(RequestOptions::new(), pb::EmptyMessage::new())
        .wait();

    match response {
        Ok(res) => Ok(res.1.get_log_contents().to_string()),
        Err(err) => Err(format!(
            "Error retrieving log from worker id {}, address: {:?}, error: {}",
            worker_id,
            worker_addr,
            err.to_string()
        ).into()),
    }
}
