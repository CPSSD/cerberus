use std::sync::Arc;

use grpc::{SingleResponse, Error, RequestOptions};

use cerberus_proto::filesystem as pb;
use cerberus_proto::filesystem_grpc as grpc_pb;
use util::distributed_filesystem::LocalFileManager;
use util::output_error;

const NOT_DISTRIBUTED_FILESYSTEM: &str = "Worker is not running in distributed filesytem configuration";
const STORE_FILE_ERROR: &str = "Error processing store file request";
const READ_FILE_ERROR: &str = "Error processing read file request";

/// `FileSystemService` recieves communication from the master and other workers in relation to the
/// distributed file system.
pub struct FileSystemService {
    local_file_manager: Option<Arc<LocalFileManager>>,
}

impl FileSystemService {
    pub fn new(local_file_manager: Option<Arc<LocalFileManager>>) -> Self {
        FileSystemService { local_file_manager: local_file_manager }
    }
}

impl grpc_pb::FileSystemWorkerService for FileSystemService {
    fn store_file(
        &self,
        _: RequestOptions,
        req: pb::StoreFileRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        info!("Processing store file request for {}", req.file_path);

        let local_file_manager = match self.local_file_manager {
            Some(ref local_file_manager) => local_file_manager,
            None => {
                error!("Worker is not running in distributed filesystem mode.");
                return SingleResponse::err(Error::Other(NOT_DISTRIBUTED_FILESYSTEM));
            }
        };

        if let Err(err) = local_file_manager.store_file_chunk(
            &req.file_path,
            req.start_byte,
            &req.data,
        )
        {
            output_error(&err.chain_err(|| "Error processing store file request."));
            return SingleResponse::err(Error::Other(STORE_FILE_ERROR));
        }

        SingleResponse::completed(pb::EmptyMessage::new())
    }

    fn read_file(
        &self,
        _: RequestOptions,
        req: pb::ReadFileRequest,
    ) -> SingleResponse<pb::ReadFileResponse> {
        let local_file_manager = match self.local_file_manager {
            Some(ref local_file_manager) => local_file_manager,
            None => {
                error!("Worker is not running in distributed filesystem mode.");
                return SingleResponse::err(Error::Other(NOT_DISTRIBUTED_FILESYSTEM));
            }
        };

        let read_file_result =
            local_file_manager.read_file_chunk(&req.file_path, req.start_byte, req.end_byte);
        match read_file_result {
            Ok(bytes) => {
                let mut response = pb::ReadFileResponse::new();
                response.data = bytes;
                SingleResponse::completed(response)
            }
            Err(err) => {
                output_error(&err.chain_err(|| "Error processing read file request."));
                SingleResponse::err(Error::Other(READ_FILE_ERROR))
            }
        }
    }
}
