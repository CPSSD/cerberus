use std::net::SocketAddr;
use errors::*;
use grpc::RequestOptions;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerService;

/// `MasterInterface` is used by the worker to communicate with the master.
/// It is used for sending the results of Map and Reduce operations,
/// registering the worker with the cluster and updating the worker status on a regular interval.
pub struct MasterInterface {
    client: grpc_pb::WorkerServiceClient,
    worker_id: String,
}

impl MasterInterface {
    pub fn new(master_addr: SocketAddr) -> Result<Self> {
        let client = grpc_pb::WorkerServiceClient::new_plain(
            &master_addr.ip().to_string(),
            master_addr.port(),
            Default::default(),
        ).chain_err(|| "Error building WorkerService client.")?;

        Ok(MasterInterface {
            client: client,
            worker_id: String::new(),
        })
    }

    pub fn register_worker(&mut self, address: &SocketAddr) -> Result<()> {
        let worker_addr = address.to_string();

        let mut req = pb::RegisterWorkerRequest::new();
        req.set_worker_address(worker_addr);

        let response = self.client
            .register_worker(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to register worker")?
            .1;

        self.worker_id = response.get_worker_id().to_owned();

        Ok(())
    }

    pub fn update_worker_status(
        &self,
        worker_status: pb::UpdateStatusRequest_WorkerStatus,
        operation_status: pb::UpdateStatusRequest_OperationStatus,
    ) -> Result<()> {
        let mut req = pb::UpdateStatusRequest::new();
        req.set_worker_status(worker_status);
        req.set_operation_status(operation_status);
        req.set_worker_id(self.worker_id.clone());

        self.client
            .update_worker_status(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to update worker status.")?;

        Ok(())
    }

    pub fn return_map_result(&self, mut map_result: pb::MapResult) -> Result<()> {
        map_result.set_worker_id(self.worker_id.clone());

        self.client
            .return_map_result(RequestOptions::new(), map_result)
            .wait()
            .chain_err(|| "Failed to return map result to master.")?;

        Ok(())
    }

    pub fn return_reduce_result(&self, mut reduce_result: pb::ReduceResult) -> Result<()> {
        reduce_result.set_worker_id(self.worker_id.clone());

        self.client
            .return_reduce_result(RequestOptions::new(), reduce_result)
            .wait()
            .chain_err(|| "Failed to return reduce result to master.")?;

        Ok(())
    }
}
