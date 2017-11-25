/// `master_service` is responsible for handing data incoming from the master.
mod master_service;
mod server;

pub use self::master_service::ScheduleOperationServer;
pub use self::server::Server;
