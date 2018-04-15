use grpc::{Error, RequestOptions, SingleResponse};

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use operations::io;
use util;

const FILE_NOT_AVAILABLE: &str = "Log file not available";

pub struct WorkerLogService {
    log_file_path: String,
}

impl WorkerLogService {
    pub fn new(log_file_path: String) -> WorkerLogService {
        WorkerLogService { log_file_path }
    }
}

impl grpc_pb::WorkerLogService for WorkerLogService {
    fn get_worker_logs(
        &self,
        _o: RequestOptions,
        _req: pb::EmptyMessage,
    ) -> SingleResponse<pb::LogsResult> {
        info!("Serving log file {}", self.log_file_path);
        match io::read_local(&self.log_file_path) {
            Ok(log_file_contents) => {
                let mut res = pb::LogsResult::new();
                res.set_log_contents(log_file_contents);

                SingleResponse::completed(res)
            }
            Err(err) => {
                util::output_error(&err.chain_err(|| "Unable to read log file: {:?}"));
                SingleResponse::err(Error::Other(FILE_NOT_AVAILABLE))
            }
        }
    }
}
