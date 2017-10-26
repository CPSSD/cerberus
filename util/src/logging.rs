use env_logger::LogBuilder;
use error_chain::ChainedError;
use errors::*;
use std::env;

// This makes the default logging level info for everything but the grpc modules which will use
// error. This is because grpc modules output an unnecessary level of logging for info.
const DEFAULT_LOG_CONFIG: &str = "info,grpc=error";

pub fn init_logger() -> Result<()> {
    let builder = &mut LogBuilder::new();
    let builder = {
        match env::var("RUST_LOG") {
            Ok(log_config) => builder.parse(&log_config),
            Err(_) => builder.parse(DEFAULT_LOG_CONFIG),
        }
    };

    builder.init().chain_err(|| "Failed to build env_logger")?;
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
