use std::collections::HashMap;
use std::cmp::max;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use protobuf::repeated::RepeatedField;
use rand::random;

use cerberus_proto::filesystem as pb;
use errors::*;
use distributed_filesystem::{FileSystemWorkerInterface, WorkerInfoProvider};
use logging::output_error;

const MIN_DISTRIBUTION_LEVEL: usize = 2;
const MAX_DISTRIBUTION_LEVEL: usize = 3;
const MAX_DISTRIBUTION_FAILURES: usize = 3;

struct FileChunk {
    start_byte: u64,
    end_byte: u64,

    // The worker ids of the workers this chunk is stored on.
    workers: Vec<String>,
}

struct FileInfo {
    length: u64,
    chunks: Vec<FileChunk>,
}

struct DirInfo {
    // List of files in the directory
    children: Vec<String>,
}

pub struct FileSystemManager {
    file_info_map: RwLock<HashMap<String, FileInfo>>,
    dir_info_map: RwLock<HashMap<String, DirInfo>>,
    worker_interface: FileSystemWorkerInterface,
    worker_info_provider: Arc<WorkerInfoProvider + Send + Sync>,
}

impl FileSystemManager {
    pub fn new(worker_info_provider: Arc<WorkerInfoProvider + Send + Sync>) -> Self {
        FileSystemManager {
            file_info_map: RwLock::new(HashMap::new()),
            dir_info_map: RwLock::new(HashMap::new()),
            worker_interface: FileSystemWorkerInterface::new(),
            worker_info_provider: worker_info_provider,
        }
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

    pub fn upload_file_chunk(&self, file_path: &str, start_byte: u64, data: &[u8]) -> Result<()> {
        let mut active_workers = self.worker_info_provider.get_workers();

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
        self.update_dir_info(file_path);

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
                let active_workers = self.worker_info_provider.get_workers();

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
}
