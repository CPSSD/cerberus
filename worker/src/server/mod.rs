/// `filesystem_service` is responsible for handling requests related to the distributed file
/// system.
mod filesystem_service;
/// `intermediate_data_service` is responsible for handing traffic coming from other workers
/// requesting intermediate data created by the map task.
mod intermediate_data_service;
/// `log_service` is responsible for serving the log file of this worker to the master.
mod log_service;
/// `master_service` is responsible for handing data incoming from the master.
mod master_service;

pub use self::filesystem_service::FileSystemService;
pub use self::intermediate_data_service::IntermediateDataService;
pub use self::log_service::WorkerLogService;
pub use self::master_service::ScheduleOperationService;

use cerberus_proto::filesystem_grpc;
use cerberus_proto::worker_grpc;
use errors::*;
use grpc;
use std::net::SocketAddr;

const GRPC_THREAD_POOL_SIZE: usize = 10;

pub struct Server {
    server: grpc::Server,
}

impl Server {
    pub fn new(
        port: u16,
        scheduler_service: ScheduleOperationService,
        interm_data_service: IntermediateDataService,
        logs_service: WorkerLogService,
        filesystem_service: FileSystemService,
    ) -> Result<Self> {
        let mut server_builder = grpc::ServerBuilder::new_plain();
        server_builder.http.set_port(port);
        server_builder
            .http
            .set_cpu_pool_threads(GRPC_THREAD_POOL_SIZE);

        // Register the ScheduleOperationService
        server_builder.add_service(
            worker_grpc::ScheduleOperationServiceServer::new_service_def(scheduler_service),
        );
        // Register IntermediateDataService
        server_builder.add_service(worker_grpc::IntermediateDataServiceServer::new_service_def(
            interm_data_service,
        ));
        // Register WorkerLogService
        server_builder.add_service(worker_grpc::WorkerLogServiceServer::new_service_def(
            logs_service,
        ));

        // Register FileSystemService
        server_builder.add_service(
            filesystem_grpc::FileSystemWorkerServiceServer::new_service_def(filesystem_service),
        );

        Ok(Server {
            server: server_builder
                .build()
                .chain_err(|| "Error building gRPC server")?,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.server.is_alive()
    }

    pub fn addr(&self) -> &SocketAddr {
        self.server.local_addr()
    }
}
