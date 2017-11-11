use errors::*;
use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerService;
use grpc::RequestOptions;
use std::net::SocketAddr;

/// `MasterInterface` is used by the worker to communicate with the master.
/// It is used for sending the results of Map and Reduce operations,
/// registering the worker with the cluster and updating the worker status on a regular interval.
pub struct MasterInterface {
    client: grpc_pb::WorkerServiceClient,
}

impl MasterInterface {
    pub fn new(master_addr: SocketAddr) -> Result<Self> {
        let client = grpc_pb::WorkerServiceClient::new_plain(
            &master_addr.ip().to_string(),
            master_addr.port(),
            Default::default(),
        ).chain_err(|| "Error building WorkerService client.")?;

        Ok(MasterInterface { client: client })
    }

    pub fn register_worker(&self, address: &SocketAddr) -> Result<()> {
        let worker_addr = address.to_string();

        let mut req = pb::RegisterWorkerRequest::new();
        req.set_worker_address(worker_addr);

        self.client
            .register_worker(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to register worker")?;

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

        self.client
            .update_worker_status(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to update worker status.")?;

        Ok(())
    }

    pub fn return_map_result(&self, map_result: pb::MapResult) -> Result<()> {
        self.client
            .return_map_result(RequestOptions::new(), map_result)
            .wait()
            .chain_err(|| "Failed to return map result to master.")?;

        Ok(())
    }

    pub fn return_reduce_result(&self, reduce_result: pb::ReduceResult) -> Result<()> {
        self.client
            .return_reduce_result(RequestOptions::new(), reduce_result)
            .wait()
            .chain_err(|| "Failed to return reduce result to master.")?;

        Ok(())
    }
}
