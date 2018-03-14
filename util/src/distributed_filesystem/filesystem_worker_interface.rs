
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::RwLock;

use grpc::RequestOptions;

use cerberus_proto::filesystem as pb;
use cerberus_proto::filesystem_grpc as grpc_pb;
use cerberus_proto::filesystem_grpc::FileSystemWorkerService; // Importing methods, don't use directly
use errors::*;

const NO_CLIENT_FOUND_ERR: &str = "No client found for this worker";

#[derive(Default)]
pub struct FileSystemWorkerInterface {
    clients: RwLock<HashMap<String, grpc_pb::FileSystemWorkerServiceClient>>,
}

/// `FileSystemWorkerInterface` is used to perform file system operations on workers.
impl FileSystemWorkerInterface {
    pub fn new() -> Self {
        Default::default()
    }

    // Creates a client for a given worker address if one does not already exist.
    fn create_client_if_required(&self, worker_addr: &str) -> Result<()> {
        {
            let clients = self.clients.read().unwrap();
            if clients.contains_key(worker_addr) {
                return Ok(());
            }
        }

        let worker_socket_addr = SocketAddr::from_str(worker_addr).chain_err(
            || "Invalid worker address",
        )?;

        let client = grpc_pb::FileSystemWorkerServiceClient::new_plain(
            &worker_socket_addr.ip().to_string(),
            worker_socket_addr.port(),
            Default::default(),
        ).chain_err(|| "Error building client for worker")?;

        let mut clients = self.clients.write().unwrap();

        clients.insert(worker_addr.to_owned(), client);
        Ok(())
    }

    pub fn store_file(
        &self,
        worker_addr: &str,
        file_path: &str,
        start_byte: u64,
        data: Vec<u8>,
    ) -> Result<()> {
        self.create_client_if_required(worker_addr).chain_err(
            || "Error creating client",
        )?;

        let mut request = pb::StoreFileRequest::new();
        request.set_file_path(file_path.to_owned());
        request.set_start_byte(start_byte);
        request.set_data(data);

        let clients = self.clients.read().unwrap();

        if let Some(client) = clients.get(worker_addr) {
            client
                .store_file(RequestOptions::new(), request)
                .wait()
                .chain_err(|| "Failed to store file")?;
            return Ok(());
        } else {
            // This should never happen.
            return Err(NO_CLIENT_FOUND_ERR.into());
        }
    }

    pub fn read_file(
        &self,
        worker_addr: &str,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<Vec<u8>> {
        self.create_client_if_required(worker_addr).chain_err(
            || "Error creating client",
        )?;

        let mut request = pb::ReadFileRequest::new();
        request.set_file_path(file_path.to_owned());
        request.set_start_byte(start_byte);
        request.set_end_byte(end_byte);

        let clients = self.clients.read().unwrap();

        if let Some(client) = clients.get(worker_addr) {
            let response = client
                .read_file(RequestOptions::new(), request)
                .wait()
                .chain_err(|| "Failed to read file")?
                .1;
            return Ok(response.data);
        } else {
            // This should never happen.
            return Err(NO_CLIENT_FOUND_ERR.into());
        }
    }
}
