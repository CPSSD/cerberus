use std::net::SocketAddr;
use std::sync::RwLock;

use grpc::RequestOptions;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::WorkerService;
use errors::*;

/// `MasterInterface` is used by the worker to communicate with the master.
/// It is used for sending the results of Map and Reduce operations,
/// registering the worker with the cluster and updating the worker status on a regular interval.
pub struct MasterInterface {
    client: grpc_pb::WorkerServiceClient,
    worker_id: RwLock<String>,
    master_addr: SocketAddr,
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
            worker_id: RwLock::new(String::new()),
            master_addr: master_addr,
        })
    }

    pub fn get_master_addr(&self) -> SocketAddr {
        self.master_addr
    }

    pub fn register_worker(&self, address: &SocketAddr, worker_id: &str) -> Result<String> {
        let worker_addr = address.to_string();

        let mut req = pb::RegisterWorkerRequest::new();
        req.set_worker_address(worker_addr);

        if worker_id != "" {
            req.set_worker_id(worker_id.to_owned());
        }

        let response = self.client
            .register_worker(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to register worker")?
            .1;

        *self.worker_id.write().unwrap() = response.get_worker_id().to_owned();

        Ok(response.get_worker_id().to_owned())
    }

    pub fn update_worker_status(
        &self,
        worker_status: pb::WorkerStatus,
        operation_status: pb::OperationStatus,
    ) -> Result<()> {
        let mut req = pb::UpdateStatusRequest::new();
        req.set_worker_status(worker_status);
        req.set_operation_status(operation_status);
        req.set_worker_id(self.worker_id.read().unwrap().clone());

        self.client
            .update_worker_status(RequestOptions::new(), req)
            .wait()
            .chain_err(|| "Failed to update worker status.")?;

        Ok(())
    }

    pub fn return_map_result(&self, mut map_result: pb::MapResult) -> Result<()> {
        map_result.set_worker_id(self.worker_id.read().unwrap().clone());

        self.client
            .return_map_result(RequestOptions::new(), map_result)
            .wait()
            .chain_err(|| "Failed to return map result to master.")?;

        Ok(())
    }

    pub fn return_reduce_result(&self, mut reduce_result: pb::ReduceResult) -> Result<()> {
        reduce_result.set_worker_id(self.worker_id.read().unwrap().clone());

        self.client
            .return_reduce_result(RequestOptions::new(), reduce_result)
            .wait()
            .chain_err(|| "Failed to return reduce result to master.")?;

        Ok(())
    }

    pub fn report_worker(
        &self,
        worker_address: String,
        intermediate_data_path: String,
        task_id: String,
    ) -> Result<()> {
        let mut report_request = pb::ReportWorkerRequest::new();
        report_request.set_task_id(task_id);
        report_request.set_worker_id(self.worker_id.read().unwrap().clone());
        report_request.set_report_address(worker_address);
        report_request.set_path(intermediate_data_path);
        self.client
            .report_worker(RequestOptions::new(), report_request)
            .wait()
            .chain_err(|| "Failed to report worker to master.")?;

        Ok(())
    }
}
