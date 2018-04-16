// The dashboard module contains code used for serving the cluster dashboard web page.
pub mod fetch_logs;
pub mod server;

pub use self::fetch_logs::fetch_worker_log;
pub use self::server::DashboardServer;
