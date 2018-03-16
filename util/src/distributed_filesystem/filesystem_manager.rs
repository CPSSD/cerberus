use std::collections::HashMap;
use std::cmp::max;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::Receiver;
use std::thread;

use protobuf::repeated::RepeatedField;
use rand::random;

use cerberus_proto::filesystem as pb;
use errors::*;
use distributed_filesystem::{FileChunk, FileSystemWorkerInterface};
use logging::output_error;
use serde_json;
use state::SimpleStateHandling;

const MIN_DISTRIBUTION_LEVEL: usize = 2;
const MAX_DISTRIBUTION_LEVEL: usize = 3;
const MAX_DISTRIBUTION_FAILURES: usize = 3;

#[derive(Deserialize, Serialize)]
struct FileInfo {
    length: u64,
    chunks: Vec<FileChunk>,
}

#[derive(Deserialize, Serialize)]
struct DirInfo {
    // List of files in the directory
    children: Vec<String>,
}

#[derive(PartialEq)]
pub enum WorkerInfoUpdateType {
    Available,
    Unavailable,
}

pub struct WorkerInfoUpdate {
    update_type: WorkerInfoUpdateType,
    worker_id: String,
    address: Option<SocketAddr>,
}

impl WorkerInfoUpdate {
    pub fn new(
        update_type: WorkerInfoUpdateType,
        worker_id: String,
        address: Option<SocketAddr>,
    ) -> WorkerInfoUpdate {
        WorkerInfoUpdate {
            update_type: update_type,
            worker_id: worker_id,
            address: address,
        }
    }
}

pub struct FileSystemManager {
    file_info_map: RwLock<HashMap<String, FileInfo>>,
    dir_info_map: RwLock<HashMap<String, DirInfo>>,
    active_workers: RwLock<HashMap<String, SocketAddr>>,
    worker_interface: FileSystemWorkerInterface,
}

impl FileSystemManager {
    pub fn new() -> Self {
        FileSystemManager {
            file_info_map: RwLock::new(HashMap::new()),
            dir_info_map: RwLock::new(HashMap::new()),
            active_workers: RwLock::new(HashMap::new()),
            worker_interface: FileSystemWorkerInterface::new(),
        }
    }

    fn get_active_workers(&self) -> HashMap<String, SocketAddr> {
        let workers = self.active_workers.read().unwrap();
        workers.clone()
    }

    fn get_distribution_level(&self, worker_count: usize) -> usize {
        let mut distribution_level = worker_count / 2;

        if distribution_level > MAX_DISTRIBUTION_LEVEL {
            distribution_level = MAX_DISTRIBUTION_LEVEL;
        } else if distribution_level < MIN_DISTRIBUTION_LEVEL {
            distribution_level = MIN_DISTRIBUTION_LEVEL;
        }
        distribution_level
    }

    fn get_random_worker(&self, workers: &HashMap<String, SocketAddr>) -> String {
        let workers_vec: Vec<&String> = workers.keys().collect();

        let random_val = random::<usize>() % workers_vec.len();

        workers_vec[random_val].to_owned()
    }

    fn update_file_info(&self, file_path: &str, file_chunk: FileChunk) {
        let mut file_info_map = self.file_info_map.write().unwrap();

        let file_info = file_info_map.entry(file_path.to_owned()).or_insert(
            FileInfo {
                length: 0,
                chunks: Vec::new(),
            },
        );

        file_info.length = max(file_info.length, file_chunk.end_byte);
        file_info.chunks.push(file_chunk);
    }

    fn update_dir_info(&self, file_path: &str) {
        let mut dir_path = PathBuf::from(file_path);
        dir_path.pop();
        let dir_path_str = dir_path.to_string_lossy().to_string() + "/";

        let mut dir_info_map = self.dir_info_map.write().unwrap();

        let dir_info = dir_info_map.entry(dir_path_str).or_insert(DirInfo {
            children: Vec::new(),
        });

        dir_info.children.push(file_path.to_owned());
    }

    fn validate_upload_chunk(&self, file_path: &str, start_byte: u64) -> Result<()> {
        let file_info_map = self.file_info_map.read().unwrap();

        if start_byte == 0 {
            // Verify that file does not exist already
            if file_info_map.contains_key(file_path) {
                return Err("File already exists".into());
            }
        } else {
            // Verify that the chunk being uploaded starts directly after last chunk
            let file_info = match file_info_map.get(file_path) {
                Some(file_info) => file_info,
                None => return Err("File does not exist".into()),
            };

            if file_info.length != start_byte {
                return Err("Upload chunk does not start at correct location".into());
            }
        }

        Ok(())
    }

    pub fn upload_file_chunk(&self, file_path: &str, start_byte: u64, data: &[u8]) -> Result<()> {
        self.validate_upload_chunk(file_path, start_byte)
            .chain_err(|| "Error validating upload chunk request")?;

        let mut active_workers = self.get_active_workers();

        if active_workers.len() < MIN_DISTRIBUTION_LEVEL {
            return Err("Not enough workers in cluster".into());
        }

        let mut used_workers = Vec::new();

        let distribution_level = self.get_distribution_level(active_workers.len());
        let mut distribution_count = 0;
        let mut distribution_failures = 0;
        while distribution_count < distribution_level &&
            distribution_failures < MAX_DISTRIBUTION_FAILURES
        {
            let worker = self.get_random_worker(&active_workers);
            let worker_addr = active_workers
                .get(&worker)
                .chain_err(|| "Error getting worker address")?
                .to_owned();

            let mut data_vec = Vec::new();
            data_vec.extend_from_slice(data);
            let result = self.worker_interface.store_file(
                &format!("{}", worker_addr),
                file_path,
                start_byte,
                data_vec,
            );

            match result {
                Ok(_) => {
                    distribution_count += 1;
                    active_workers.remove(&worker);
                    used_workers.push(worker);
                }
                Err(err) => {
                    output_error(&err.chain_err(
                        || format!("Error storing file on worker {}", worker),
                    ));
                    distribution_failures += 1;
                    if distribution_count + active_workers.len() > distribution_level {
                        active_workers.remove(&worker);
                    }
                }
            }
        }

        let file_chunk = FileChunk {
            start_byte: start_byte,
            end_byte: start_byte + (data.len() as u64),
            workers: used_workers,
        };

        self.update_file_info(file_path, file_chunk);
        // If the file is not already part of the directory.
        if start_byte == 0 {
            self.update_dir_info(file_path);
        }

        Ok(())
    }

    fn convert_file_chunk(
        &self,
        chunk: &FileChunk,
        active_workers: &HashMap<String, SocketAddr>,
    ) -> pb::FileChunk {
        let mut pb_chunk = pb::FileChunk::new();
        pb_chunk.start_byte = chunk.start_byte;
        pb_chunk.end_byte = chunk.end_byte;

        for worker in &chunk.workers {
            if let Some(worker_addr) = active_workers.get(worker) {
                pb_chunk.worker_address.push(format!("{}", worker_addr));
            }
        }

        pb_chunk
    }

    // Gets the locations for the entire file if end_byte is 0
    pub fn get_file_location(
        &self,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<pb::FileLocationResponse> {
        let file_info_map = self.file_info_map.read().unwrap();
        match file_info_map.get(file_path) {
            Some(file_info) => {
                let mut active_workers = self.get_active_workers();

                let mut response = pb::FileLocationResponse::new();
                if end_byte == 0 {
                    for chunk in &file_info.chunks {
                        let pb_chunk = self.convert_file_chunk(chunk, &active_workers);
                        response.chunks.push(pb_chunk);
                    }
                } else {
                    for chunk in &file_info.chunks {
                        // Check if chunk is in the range.
                        if (start_byte >= chunk.start_byte && start_byte < chunk.end_byte) ||
                            (chunk.start_byte >= start_byte && chunk.start_byte < end_byte)
                        {
                            let pb_chunk = self.convert_file_chunk(chunk, &active_workers);
                            response.chunks.push(pb_chunk);
                        }
                    }
                }

                Ok(response)
            }
            None => Err(format!("No file info found for {}", file_path).into()),
        }
    }

    pub fn get_file_chunks(&self, file_path: &str) -> Result<Vec<FileChunk>> {
        let file_info_map = self.file_info_map.read().unwrap();

        match file_info_map.get(file_path) {
            Some(file_info) => Ok(file_info.chunks.clone()),
            None => Err(format!("No file info found for {}", file_path).into()),
        }
    }

    pub fn get_file_info(&self, file_path: &str) -> pb::FileInfoResponse {
        {
            let file_info_map = self.file_info_map.read().unwrap();
            if let Some(file_info) = file_info_map.get(file_path) {
                // Is file
                let mut response = pb::FileInfoResponse::new();
                response.set_exists(true);
                response.set_is_file(true);
                response.set_length(file_info.length);
                return response;
            }
        }

        {
            let dir_info_map = self.dir_info_map.read().unwrap();
            if let Some(dir_info) = dir_info_map.get(file_path) {
                // Is Dir
                // Is file
                let mut response = pb::FileInfoResponse::new();
                response.set_exists(true);
                response.set_is_file(false);
                response.set_length(dir_info.children.len() as u64);
                response.set_children(RepeatedField::from_vec(dir_info.children.clone()));
                return response;
            }

        }

        let mut response = pb::FileInfoResponse::new();
        response.set_exists(false);
        response
    }

    pub fn process_worker_info_update(&self, worker_info_update: &WorkerInfoUpdate) -> Result<()> {
        let mut worker_map = self.active_workers.write().unwrap();
        if worker_info_update.update_type == WorkerInfoUpdateType::Available {
            let address = worker_info_update.address.chain_err(
                || "No address when adding available worker",
            )?;

            worker_map.insert(worker_info_update.worker_id.clone(), address);
        } else {
            worker_map.remove(&worker_info_update.worker_id);
        }

        Ok(())
    }
}

impl SimpleStateHandling<Error> for FileSystemManager {
    fn dump_state(&self) -> Result<serde_json::Value> {
        let file_info_map = self.file_info_map.read().unwrap();
        let dir_info_map = self.dir_info_map.read().unwrap();

        let mut file_info_vec: Vec<serde_json::Value> = Vec::new();
        for (file_path, file_info) in file_info_map.iter() {
            file_info_vec.push(json!({"file_path": file_path, "file_info": file_info}));
        }

        let mut dir_info_vec: Vec<serde_json::Value> = Vec::new();
        for (dir_path, dir_info) in dir_info_map.iter() {
            dir_info_vec.push(json!({"dir_path": dir_path, "dir_info": dir_info}));
        }

        Ok(json!({
            "file_info_array": file_info_vec,
            "dir_info_array": dir_info_vec,
        }))
    }

    // Updates the object to match the JSON state provided.
    fn load_state(&self, data: serde_json::Value) -> Result<()> {
        let mut file_info_map = self.file_info_map.write().unwrap();
        let mut dir_info_map = self.dir_info_map.write().unwrap();

        if let serde_json::Value::Array(ref file_info_array) = data["file_info_array"] {
            for file_json in file_info_array {
                let file_path: String = serde_json::from_value(file_json["file_path"].clone())
                    .chain_err(|| "Unable to convert file path")?;
                let file_info = serde_json::from_value(file_json["file_info"].clone())
                    .chain_err(|| "Unable to convert file info")?;

                file_info_map.insert(file_path, file_info);
            }
        } else {
            return Err("Error processing file info array.".into());
        }

        if let serde_json::Value::Array(ref dir_info_array) = data["dir_info_array"] {
            for dir_json in dir_info_array {
                let dir_path: String = serde_json::from_value(dir_json["dir_path"].clone())
                    .chain_err(|| "Unable to convert dir path")?;
                let dir_info = serde_json::from_value(dir_json["dir_info"].clone())
                    .chain_err(|| "Unable to convert dir info")?;

                dir_info_map.insert(dir_path.to_owned(), dir_info);
            }
        } else {
            return Err("Error processing dir info array.".into());
        }

        Ok(())
    }
}

pub fn run_worker_info_upate_loop(
    file_system_manager_option: &Option<Arc<FileSystemManager>>,
    worker_info_receiver: Receiver<WorkerInfoUpdate>,
) {
    if let &Some(ref file_system_manager) = file_system_manager_option {
        let file_system_manager = Arc::clone(&file_system_manager);

        thread::spawn(move || loop {
            match worker_info_receiver.recv() {
                Err(e) => error!("Error receiving worker info update: {}", e),
                Ok(worker_info_update) => {
                    let result =
                        file_system_manager.process_worker_info_update(&worker_info_update);
                    if let Err(e) = result {
                        output_error(&e);
                    }
                }
            }
        });
    } else {
        thread::spawn(move || loop {
            match worker_info_receiver.recv() {
                Err(e) => error!("Error receiving worker info update: {}", e),
                Ok(_) => {}
            }
        });
    }
}
