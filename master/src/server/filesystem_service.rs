use std::sync::Arc;

use grpc::{SingleResponse, Error, RequestOptions};

use cerberus_proto::filesystem as pb;
use cerberus_proto::filesystem_grpc as grpc_pb;
use util::distributed_filesystem::FileSystemManager;
use util::output_error;

const NOT_DISTRIBUTED_FILESYSTEM: &str = "Master is not running in distributed filesytem configuration";
const UPLOAD_FILE_ERROR: &str = "Error processing upload file request";
const FILE_LOCATION_ERROR: &str = "Error processing file location request";

/// `FileSystemService` recieves communication from a clients and workers in relation to the
/// distributed file system.
pub struct FileSystemService {
    filesystem_manager: Option<Arc<FileSystemManager>>,
}

impl FileSystemService {
    pub fn new(filesystem_manager: Option<Arc<FileSystemManager>>) -> Self {
        FileSystemService { filesystem_manager: filesystem_manager }
    }
}

impl grpc_pb::FileSystemMasterService for FileSystemService {
    fn upload_file(
        &self,
        _: RequestOptions,
        req: pb::UploadFileRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        info!("Processing upload file request for {}", req.file_path);

        let filesystem_manager = match self.filesystem_manager {
            Some(ref filesystem_manager) => filesystem_manager,
            None => {
                error!("Master is not running in distributed filesystem mode.");
                return SingleResponse::err(Error::Other(NOT_DISTRIBUTED_FILESYSTEM));
            }
        };

        if let Err(err) = filesystem_manager.upload_file_chunk(
            &req.file_path,
            req.start_byte,
            &req.data,
        )
        {
            output_error(&err.chain_err(|| "Error processing upload file request."));
            return SingleResponse::err(Error::Other(UPLOAD_FILE_ERROR));
        }

        SingleResponse::completed(pb::EmptyMessage::new())
    }

    fn get_file_location(
        &self,
        _: RequestOptions,
        req: pb::FileLocationRequest,
    ) -> SingleResponse<pb::FileLocationResponse> {
        let filesystem_manager = match self.filesystem_manager {
            Some(ref filesystem_manager) => filesystem_manager,
            None => {
                error!("Master is not running in distributed filesystem mode.");
                return SingleResponse::err(Error::Other(NOT_DISTRIBUTED_FILESYSTEM));
            }
        };

        let file_location_result =
            filesystem_manager.get_file_location(&req.file_path, req.start_byte, req.end_byte);

        match file_location_result {
            Ok(file_location) => SingleResponse::completed(file_location),
            Err(err) => {
                output_error(&err.chain_err(|| "Error processing file location request."));
                SingleResponse::err(Error::Other(FILE_LOCATION_ERROR))
            }
        }
    }

    fn get_file_info(
        &self,
        _: RequestOptions,
        req: pb::FileInfoRequest,
    ) -> SingleResponse<pb::FileInfoResponse> {
        let filesystem_manager = match self.filesystem_manager {
            Some(ref filesystem_manager) => filesystem_manager,
            None => {
                error!("Master is not running in distributed filesystem mode.");
                return SingleResponse::err(Error::Other(NOT_DISTRIBUTED_FILESYSTEM));
            }
        };

        let file_info_response = filesystem_manager.get_file_info(&req.file_path);
        SingleResponse::completed(file_info_response)
    }
}
