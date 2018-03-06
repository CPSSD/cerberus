use std::sync::Arc;

use grpc::{SingleResponse, RequestOptions};

use util::data_layer::AbstractionLayer;

use cerberus_proto::filesystem as pb;
use cerberus_proto::filesystem_grpc as grpc_pb;

/// `FileSystemService` recieves communication from a clients and workers in relation to the
/// distributed file system.
pub struct FileSystemService {
    // file_system_manager: Arc<FileSystemManager>,
    data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
}

impl FileSystemService {
    pub fn new(data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>) -> Self {
        FileSystemService {
            // file_system_manager: file_system_manager,
            data_abstraction_layer: data_abstraction_layer,
        }
    }
}

impl grpc_pb::FileSystemMasterService for FileSystemService {
    fn upload_file(
        &self,
        _: RequestOptions,
        req: pb::UploadFileRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        SingleResponse::completed(pb::EmptyMessage::new())
    }

    fn download_file(
        &self,
        _: RequestOptions,
        req: pb::DownloadFileRequest,
    ) -> SingleResponse<pb::DownloadFileResponse> {
        let response = pb::DownloadFileResponse::new();

        SingleResponse::completed(response)
    }

    fn get_file_location(
        &self,
        _: RequestOptions,
        req: pb::FileLocationRequest,
    ) -> SingleResponse<pb::FileLocationResponse> {
        let response = pb::FileLocationResponse::new();

        SingleResponse::completed(response)
    }
}
