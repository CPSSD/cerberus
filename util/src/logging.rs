use std::fs;
use std::io;
use std::path::Path;

use chrono::Utc;
use error_chain::ChainedError;
use fern;
use log;

use errors::*;

pub fn init_logger(log_file_path: &str, verbose_logging: bool) -> Result<()> {
    let mut log_directory = Path::new(log_file_path).to_path_buf();
    log_directory.pop();
    fs::create_dir_all(log_directory).chain_err(|| "Error creating logs output directory")?;

    let log_file = fern::log_file(log_file_path)
        .chain_err(|| format!("Error creating log file with path: {}", log_file_path))?;

    let mut fern_logger = fern::Dispatch::new().format(|out, _message, record| {
        if let Some(path) = record.module_path() {
            out.finish(format_args!(
                "{}:{}:{}: {}",
                record.level(),
                Utc::now().format("%T%.3f"),
                path,
                record.args(),
            ))
        } else {
            out.finish(format_args!(
                "{}:{}: {}",
                record.level(),
                Utc::now().format("%T%.3f"),
                record.args(),
            ))
        }
    });

    if verbose_logging {
        fern_logger = fern_logger.level(log::LevelFilter::Off);
    } else {
        fern_logger = fern_logger
            .level(log::LevelFilter::Info)
            .level_for("httpbis", log::LevelFilter::Error)
            .level_for("grpc", log::LevelFilter::Error);
    }

    fern_logger
        .chain(io::stdout())
        .chain(log_file)
        .apply()
        .chain_err(|| "Failed to initialize logging")?;

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
