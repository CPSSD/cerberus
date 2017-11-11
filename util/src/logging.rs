use std::env;

use chrono::Utc;
use env_logger::LogBuilder;
use error_chain::ChainedError;
use log::LogRecord;

use errors::*;

// This makes the default logging level info for everything but the grpc modules which will use
// error. This is because grpc modules output an unnecessary level of logging for info.
const DEFAULT_LOG_CONFIG: &str = "info,httpbis=error,grpc=error";

pub fn init_logger() -> Result<()> {
    let format = |record: &LogRecord| {
        format!(
            "{}:{}:{}: {}",
            record.level(),
            record.location().module_path(),
            Utc::now().format("%T%.3f"),
            record.args()
        )
    };
    let log_config = env::var("RUST_LOG").unwrap_or_else(|_| DEFAULT_LOG_CONFIG.to_owned());

    LogBuilder::new()
        .format(format)
        .parse(log_config.as_str())
        .init()
        .chain_err(|| "Failed to build env_logger")?;

    Ok(())
}

pub fn output_error<E: ChainedError>(err: &E) {
    error!("{}", err);

    for e in err.iter().skip(1) {
        error!("caused by: {}", e);
    }

    if let Some(backtrace) = err.backtrace() {
        error!("backtrace: {:?}", backtrace);
    }
}
