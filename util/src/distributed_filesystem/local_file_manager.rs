use std::collections::HashMap;
use std::cmp::{max, min};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::RwLock;
use std::path::PathBuf;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

use uuid::Uuid;

use errors::*;
use serde_json;
use state::SimpleStateHandling;

const COMPLETE_SUB_DIR: &str = "complete";

#[derive(Deserialize, Serialize)]
struct FileChunk {
    local_file_path: PathBuf,
    start_byte: u64,
    end_byte: u64,
}

#[derive(Deserialize, Serialize)]
pub struct ReadFileChunk {
    pub start_byte: u64,
    pub end_byte: u64,
    pub data: Vec<u8>,
}

pub struct LocalFileManager {
    local_file_map: RwLock<HashMap<String, Vec<FileChunk>>>,
    complete_file_map: RwLock<HashMap<String, String>>,
    storage_directory: PathBuf,
}

impl LocalFileManager {
    pub fn new(storage_directory: PathBuf) -> Self {
        LocalFileManager {
            local_file_map: RwLock::new(HashMap::new()),
            complete_file_map: RwLock::new(HashMap::new()),
            storage_directory,
        }
    }

    pub fn store_file_chunk(&self, file_path: &str, start_byte: u64, data: &[u8]) -> Result<()> {
        let mut storage_path = PathBuf::new();
        storage_path.push(self.storage_directory.clone());

        let mut dir_builder = DirBuilder::new();
        let dir_builder = dir_builder.recursive(true).mode(0o777);
        dir_builder.create(&storage_path).chain_err(
            || "Failed to create storage directory",
        )?;

        let file_name = Uuid::new_v4().to_string();
        storage_path.push(file_name);

        info!(
            "Storing file chunk {} ({} -> {}) to {}",
            file_path,
            start_byte,
            start_byte + data.len() as u64,
            storage_path.to_string_lossy()
        );

        let mut file = File::create(storage_path.clone()).chain_err(
            || "Unable to create file",
        )?;
        file.write_all(data).chain_err(|| "Unable to write data")?;

        let mut local_file_map = self.local_file_map.write().unwrap();
        let chunks = local_file_map.entry(file_path.to_owned()).or_insert_with(
            Vec::new,
        );

        let file_chunk = FileChunk {
            local_file_path: storage_path,
            start_byte,
            end_byte: start_byte + (data.len() as u64),
        };
        chunks.push(file_chunk);

        Ok(())
    }

    pub fn get_local_file(&self, file_path: &str) -> Option<String> {
        let complete_file_map = self.complete_file_map.read().unwrap();
        complete_file_map.get(file_path).map(|s| s.to_owned())
    }

    pub fn write_local_file(&self, file_path: &str, data: &[u8]) -> Result<String> {
        let mut storage_path = PathBuf::new();
        storage_path.push(self.storage_directory.clone());
        storage_path.push(COMPLETE_SUB_DIR);

        let mut dir_builder = DirBuilder::new();
        let dir_builder = dir_builder.recursive(true).mode(0o777);
        dir_builder.create(&storage_path).chain_err(
            || "Failed to create storage directory",
        )?;

        let file_name = Uuid::new_v4().to_string();
        storage_path.push(file_name);

        let mut options = OpenOptions::new();
        options.read(true);
        options.write(true);
        options.truncate(true);
        options.create(true);
        options.mode(0o777);

        let mut file = options.open(storage_path.clone()).chain_err(
            || "Unable to create file",
        )?;
        file.write_all(data).chain_err(|| "Unable to write data")?;

        let mut complete_file_map = self.complete_file_map.write().unwrap();
        complete_file_map.insert(
            file_path.to_owned(),
            storage_path.to_string_lossy().to_string(),
        );

        Ok(storage_path.to_string_lossy().to_string())
    }

    /// `read_file_chunk` reads a single file chunk known to exist requested by another worker.
    pub fn read_file_chunk(
        &self,
        file_path: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> Result<Vec<u8>> {
        let local_file_map = self.local_file_map.read().unwrap();
        let stored_chunks = match local_file_map.get(file_path) {
            Some(stored_chunks) => stored_chunks,
            None => {
                return Err(
                    format!("No stored file chunks found for {}", file_path).into(),
                )
            }
        };

        let mut file_chunk = None;
        for chunk in stored_chunks {
            if chunk.start_byte <= start_byte && chunk.end_byte >= end_byte {
                file_chunk = Some(chunk);
                break;
            }
        }

        if let Some(chunk) = file_chunk {
            let bytes_to_read = end_byte - start_byte;
            let mut bytes = vec![0u8; bytes_to_read as usize];

            let mut file = File::open(chunk.local_file_path.clone()).chain_err(|| {
                format!("Error opening file chunk {:?}", chunk.local_file_path)
            })?;

            file.seek(SeekFrom::Start(start_byte - chunk.start_byte))
                .chain_err(|| {
                    format!("Error reading file chunk {:?}", chunk.local_file_path)
                })?;

            file.read_exact(&mut bytes).chain_err(|| {
                format!("Error reading file chunk {:?}", chunk.local_file_path)
            })?;

            return Ok(bytes);
        }
        Err(
            format!("Stored file chunk not found for {}", file_path).into(),
        )
    }

    /// `read_local_file` reads the portion of the requested file that is stored localy and
    /// returns this. Returns a Vector for the unlikely case where the stored file is split across
    /// multiple chunks.
    pub fn read_local_file(
        &self,
        file_path: &str,
        mut start_byte: u64,
        end_byte: u64,
    ) -> Result<Vec<ReadFileChunk>> {
        let mut file_chunks = Vec::new();

        let local_file_map = self.local_file_map.read().unwrap();

        if let Some(stored_chunks) = local_file_map.get(file_path) {
            while start_byte < end_byte {
                let mut best: u64 = end_byte + 1;
                let mut best_chunk = 0;
                for (i, chunk) in stored_chunks.iter().enumerate() {
                    if chunk.end_byte > start_byte && chunk.start_byte < end_byte {
                        // Stored chunk starts at or after the requested section.
                        if chunk.start_byte >= start_byte {
                            let distance = chunk.start_byte - start_byte;
                            if distance < best {
                                best = distance;
                                best_chunk = i;
                            }
                        } else {
                            // Stored chunk starts before the requested section and continues into
                            // the requested section.
                            best = 0;
                            best_chunk = i;
                        }
                    }
                }

                if best >= end_byte {
                    break;
                } else {
                    let chunk = &stored_chunks[best_chunk];

                    start_byte = max(start_byte, chunk.start_byte);
                    let bytes_to_read = min(end_byte - start_byte, chunk.end_byte - start_byte);
                    let mut bytes = vec![0u8; bytes_to_read as usize];

                    let mut file = File::open(chunk.local_file_path.clone()).chain_err(|| {
                        format!("Error opening file chunk {:?}", chunk.local_file_path)
                    })?;
                    file.seek(SeekFrom::Start(start_byte - chunk.start_byte))
                        .chain_err(|| {
                            format!("Error reading file chunk {:?}", chunk.local_file_path)
                        })?;

                    file.read_exact(&mut bytes).chain_err(|| {
                        format!("Error reading file chunk {:?}", chunk.local_file_path)
                    })?;

                    let file_chunk = ReadFileChunk {
                        start_byte,
                        end_byte: start_byte + bytes_to_read,
                        data: bytes,
                    };
                    file_chunks.push(file_chunk);

                    start_byte += bytes_to_read;
                }
            }
        }

        Ok(file_chunks)
    }
}

impl SimpleStateHandling<Error> for LocalFileManager {
    fn dump_state(&self) -> Result<serde_json::Value> {
        let local_file_map = self.local_file_map.read().unwrap();
        let mut local_file_vec: Vec<serde_json::Value> = Vec::new();
        for (file_path, file_info) in local_file_map.iter() {
            local_file_vec.push(json!({"file_path": file_path, "file_info": file_info}));
        }

        let complete_file_map = self.complete_file_map.read().unwrap();
        let mut complete_file_vec: Vec<serde_json::Value> = Vec::new();
        for (remote_path, local_path) in complete_file_map.iter() {
            complete_file_vec.push(
                json!({"remote_path": remote_path, "local_path": local_path}),
            );
        }

        Ok(json!({
            "local_file_array": local_file_vec,
            "complete_file_array": complete_file_vec,
        }))
    }

    // Updates the object to match the JSON state provided.
    fn load_state(&self, data: serde_json::Value) -> Result<()> {
        let mut local_file_map = self.local_file_map.write().unwrap();
        let mut complete_file_map = self.complete_file_map.write().unwrap();

        if let serde_json::Value::Array(ref local_file_array) = data["local_file_array"] {
            for file_json in local_file_array {
                let file_path: String = serde_json::from_value(file_json["file_path"].clone())
                    .chain_err(|| "Unable to convert file path")?;
                let file_info = serde_json::from_value(file_json["file_info"].clone())
                    .chain_err(|| "Unable to convert file info")?;

                local_file_map.insert(file_path, file_info);
            }
        } else {
            return Err("Error processing local file array.".into());
        }

        if let serde_json::Value::Array(ref complete_file_array) = data["complete_file_array"] {
            for complete_file_json in complete_file_array {
                let remote_path: String = serde_json::from_value(
                    complete_file_json["remote_path"].clone(),
                ).chain_err(|| "Unable to convert remote path")?;
                let local_path: String = serde_json::from_value(
                    complete_file_json["local_path"].clone(),
                ).chain_err(|| "Unable to convert local path")?;

                complete_file_map.insert(remote_path, local_path);
            }
        } else {
            return Err("Error processing complete file array.".into());
        }

        Ok(())
    }
}
