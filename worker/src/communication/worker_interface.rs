use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;

use grpc::RequestOptions;
use std::error::Error;

use errors::*;
use operations::io;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::IntermediateDataService;
use operations::OperationResources; // For pub functions only

const INTERMEDIATE_DATA_RETRIES: u8 = 3;

/// `WorkerInterface` is used to load data from other workers which have completed
/// their map tasks.
pub struct WorkerInterface;

impl WorkerInterface {
    pub fn get_data<P: AsRef<Path>>(
        path: P,
        output_dir_uuid: &str,
        resources: &OperationResources,
        task_id: &str,
    ) -> Result<String> {
        let path_str = path.as_ref().to_string_lossy();
        let split_path: Vec<&str> = path_str.splitn(2, '/').collect();
        let worker_addr =
            SocketAddr::from_str(split_path[0]).chain_err(|| "Unable to parse worker address")?;
        let file = format!("/{}", split_path[1]);
        info!("getting {} from {}", &file, worker_addr);

        if file.contains(output_dir_uuid) {
            info!("file {} is local, loading from disk", file);
            return io::read_local(file).chain_err(|| "Unable to read from local disk");
        }

        let mut req = pb::IntermediateDataRequest::new();
        req.set_path(file.clone());

        let res = WorkerInterface::request_data(worker_addr, req, resources, task_id)
            .chain_err(|| format!("Failed to get {} from {}", file, worker_addr))?;

        String::from_utf8(res.get_data().to_vec())
            .chain_err(|| "Unable to convert returned data to string")
    }

    pub fn request_data(
        worker_addr: SocketAddr,
        req: pb::IntermediateDataRequest,
        resources: &OperationResources,
        task_id: &str,
    ) -> Result<pb::IntermediateData> {
        // TODO: Add client store so we don't need to create a new client every time.
        let client = grpc_pb::IntermediateDataServiceClient::new_plain(
            &worker_addr.ip().to_string(),
            worker_addr.port(),
            Default::default(),
        ).chain_err(|| format!("Error building client for worker {}", worker_addr))?;

        for i in 0..INTERMEDIATE_DATA_RETRIES {
            let response = client
                .get_intermediate_data(RequestOptions::new(), req.clone())
                .wait();

            if let Ok(res) = response {
                return Ok(res.1);
            }

            match response {
                Ok(res) => return Ok(res.1),
                Err(err) => {
                    info!(
                        "Error retrieving intermediate data from {} (Attempts left: {}): {}",
                        worker_addr,
                        INTERMEDIATE_DATA_RETRIES - i - 1,
                        err.description()
                    );
                }
            };
        }

        // At this point we have failed to contact the worker multiple times and should report this
        // to the master.

        resources
            .master_interface
            .report_worker(
                format!("{}:{}", worker_addr.ip().to_string(), worker_addr.port()),
                req.path,
                task_id.to_owned(),
            )
            .chain_err(|| "Unable to report worker")?;

        Err(format!(
            "Unable to get intermediate data after {} attempts",
            INTERMEDIATE_DATA_RETRIES
        ).into())
    }
}
