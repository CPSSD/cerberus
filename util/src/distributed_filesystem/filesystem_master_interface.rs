use std::net::SocketAddr;
use std::sync::Arc;

use errors::*;
use grpc::RequestOptions;

use cerberus_proto::filesystem as pb;
use cerberus_proto::filesystem_grpc as grpc_pb;
use cerberus_proto::filesystem_grpc::FileSystemMasterService; // Importing methods, don't use directly
use distributed_filesystem::FileSystemManager;

pub trait FileSystemMasterInterface {
    fn upload_file_chunk(&self, file_path: &str, start_byte: u64, data: Vec<u8>) -> Result<()>;

    fn get_file_location(
        &self,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<pb::FileLocationResponse>;

    fn get_file_info(&self, file_path: &str) -> Result<pb::FileInfoResponse>;
}

/// `NetworkFileSystemMasterInterface` is used to perform file system operations on the master and get
/// information about the distributed filesystem from the master.
pub struct NetworkFileSystemMasterInterface {
    client: grpc_pb::FileSystemMasterServiceClient,
}

impl NetworkFileSystemMasterInterface {
    pub fn new(master_addr: SocketAddr) -> Result<Self> {
        let client = grpc_pb::FileSystemMasterServiceClient::new_plain(
            &master_addr.ip().to_string(),
            master_addr.port(),
            Default::default(),
        ).chain_err(|| "Error building FileSystemMasterService client.")?;

        Ok(NetworkFileSystemMasterInterface { client: client })
    }
}

impl FileSystemMasterInterface for NetworkFileSystemMasterInterface {
    fn upload_file_chunk(&self, file_path: &str, start_byte: u64, data: Vec<u8>) -> Result<()> {
        let mut upload_file_req = pb::UploadFileRequest::new();
        upload_file_req.file_path = file_path.to_owned();
        upload_file_req.start_byte = start_byte;
        upload_file_req.data = data;

        self.client
            .upload_file(RequestOptions::new(), upload_file_req)
            .wait()
            .chain_err(|| "Failed to upload file chunk")?;

        Ok(())
    }

    // Gets the locations for the entire file if end_byte is 0
    fn get_file_location(
        &self,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<pb::FileLocationResponse> {
        let mut file_location_req = pb::FileLocationRequest::new();
        file_location_req.file_path = file_path.to_owned();
        file_location_req.start_byte = start_byte;
        file_location_req.end_byte = end_byte;

        let response = self.client
            .get_file_location(RequestOptions::new(), file_location_req)
            .wait()
            .chain_err(|| "Failed to get file location")?
            .1;

        Ok(response)
    }

    fn get_file_info(&self, file_path: &str) -> Result<pb::FileInfoResponse> {
        let mut file_info_req = pb::FileInfoRequest::new();
        file_info_req.file_path = file_path.to_owned();

        let response = self.client
            .get_file_info(RequestOptions::new(), file_info_req)
            .wait()
            .chain_err(|| "Failed to get file info")?
            .1;

        Ok(response)
    }
}

/// `LocalFileSystemMasterInterface` is used to perform file system operations on the master.
pub struct LocalFileSystemMasterInterface {
    filesystem_manager: Arc<FileSystemManager>,
}

impl LocalFileSystemMasterInterface {
    pub fn new(filesystem_manager: Arc<FileSystemManager>) -> Self {
        LocalFileSystemMasterInterface { filesystem_manager: filesystem_manager }
    }
}

impl FileSystemMasterInterface for LocalFileSystemMasterInterface {
    fn upload_file_chunk(&self, file_path: &str, start_byte: u64, data: Vec<u8>) -> Result<()> {
        self.filesystem_manager.upload_file_chunk(
            file_path,
            start_byte,
            &data,
        )
    }

    // Gets the locations for the entire file if end_byte is 0
    fn get_file_location(
        &self,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<pb::FileLocationResponse> {
        self.filesystem_manager.get_file_location(
            file_path,
            start_byte,
            end_byte,
        )
    }

    fn get_file_info(&self, file_path: &str) -> Result<pb::FileInfoResponse> {
        Ok(self.filesystem_manager.get_file_info(file_path))
    }
}
