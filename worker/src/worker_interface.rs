use std::sync::{Arc, Mutex};

use errors::*;
use grpc::{Server, ServerBuilder};

use cerberus_proto::worker_grpc as grpc_pb;
use schedule_operation_service::ScheduleOperationServiceImpl;
use operation_handler::OperationHandler;

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct WorkerInterface {
    server: Server,
}

/// `WorkerInterface` is the implementation of the interface used by the worker to recieve commands
/// from the master.
/// This will be used by the master to schedule `MapReduce` operations on the worker
impl WorkerInterface {
    pub fn new(operation_handler: Arc<Mutex<OperationHandler>>, serving_port: u16) -> Result<Self> {
        let schedule_operation_service_impl = ScheduleOperationServiceImpl::new(operation_handler);

        let mut server_builder: ServerBuilder = ServerBuilder::new_plain();
        server_builder.http.set_port(serving_port);
        server_builder.add_service(grpc_pb::ScheduleOperationServiceServer::new_service_def(
            schedule_operation_service_impl,
        ));
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        Ok(WorkerInterface {
            server: server_builder.build().chain_err(
                || "Error building ScheduleOperationService server.",
            )?,
        })
    }

    pub fn get_server(&self) -> &Server {
        &self.server
    }
}
