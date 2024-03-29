use std::cmp::{max, min};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::random;

use data_layer::AbstractionLayer;
use distributed_filesystem::{FileSystemMasterInterface, FileSystemWorkerInterface,
                             LocalFileManager};
use errors::*;
use logging::output_error;

const MAX_GET_DATA_RETRIES: usize = 3;
const MEGA_BYTE: u64 = 1000 * 1000;
const MAX_LOCAL_FILE_CHUNK: u64 = MEGA_BYTE * 32;

pub struct DFSAbstractionLayer {
    local_file_manager: Arc<LocalFileManager>,
    master_interface: Box<FileSystemMasterInterface + Send + Sync>,
    worker_interface: FileSystemWorkerInterface,
}

impl DFSAbstractionLayer {
    pub fn new(
        local_file_manager: Arc<LocalFileManager>,
        master_interface: Box<FileSystemMasterInterface + Send + Sync>,
    ) -> Self {
        DFSAbstractionLayer {
            local_file_manager,
            master_interface,
            worker_interface: FileSystemWorkerInterface::new(),
        }
    }

    fn get_remote_data(&self, file_path: &str, start_byte: u64, end_byte: u64) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        let file_location = self.master_interface
            .get_file_location(file_path, start_byte, end_byte)
            .chain_err(|| "Error getting remote data.")?;

        for chunk in file_location.chunks.iter() {
            if chunk.worker_address.len() == 0 {
                return Err(
                    "Error getting remote data: Incomplete file location information.".into(),
                );
            }

            let mut retries = MAX_GET_DATA_RETRIES;
            loop {
                let random_val = random::<usize>() % chunk.worker_address.len();
                let worker_addr = &chunk.worker_address[random_val];

                let remote_data_result = self.worker_interface.read_file(
                    worker_addr,
                    file_path,
                    max(start_byte, chunk.start_byte),
                    min(end_byte, chunk.end_byte),
                );

                retries -= 1;

                match remote_data_result {
                    Ok(remote_data) => {
                        data.extend(remote_data);
                        break;
                    }
                    Err(_) => {
                        if retries == 0 {
                            return remote_data_result.chain_err(|| "Error reading remote data");
                        }
                    }
                }
            }
        }

        Ok(data)
    }
}

impl AbstractionLayer for DFSAbstractionLayer {
    fn get_file_length(&self, path: &Path) -> Result<u64> {
        debug!("Getting file length: {:?}", path);

        let file_info = self.master_interface
            .get_file_info(&path.to_string_lossy())
            .chain_err(|| "Error getting file length")?;

        if !file_info.exists || !file_info.is_file {
            return Err("File does not exist".into());
        }

        Ok(file_info.length)
    }

    fn read_file_location(&self, path: &Path, start_byte: u64, end_byte: u64) -> Result<Vec<u8>> {
        debug!("Reading file: {:?}", path);

        let local_file_chunks = self.local_file_manager
            .read_local_file(&path.to_string_lossy(), start_byte, end_byte)
            .chain_err(|| "Error reading local file")?;

        let mut data = Vec::new();
        let mut on_local_chunk = 0;
        let mut on_byte = start_byte;
        while on_byte < end_byte {
            while on_local_chunk < local_file_chunks.len()
                && local_file_chunks[on_local_chunk].start_byte < on_byte
            {
                on_local_chunk += 1;
            }
            if on_local_chunk < local_file_chunks.len() {
                if on_byte == local_file_chunks[on_local_chunk].start_byte {
                    data.extend(local_file_chunks[on_local_chunk].data.clone());
                    on_byte = local_file_chunks[on_local_chunk].end_byte;
                } else {
                    let new_on_byte = local_file_chunks[on_local_chunk].start_byte;
                    let remote_data =
                        self.get_remote_data(&path.to_string_lossy(), on_byte, new_on_byte)
                            .chain_err(|| "Error reading remote data.")?;
                    data.extend(remote_data);
                    on_byte = new_on_byte;
                }
            } else {
                let remote_data = self.get_remote_data(&path.to_string_lossy(), on_byte, end_byte)
                    .chain_err(|| "Error reading remote data.")?;
                data.extend(remote_data);
                on_byte = end_byte;
            }
        }

        Ok(data)
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        debug!("Writing file: {:?}", path);

        let mut data_vec = Vec::new();
        data_vec.extend_from_slice(data);

        self.master_interface
            .upload_file_chunk(&path.to_string_lossy(), 0, data_vec)
            .chain_err(|| "Error writing file")
    }

    fn get_local_file(&self, path: &Path) -> Result<PathBuf> {
        debug!("Getting local file: {:?}", path);

        if let Some(local_file_path) = self.local_file_manager
            .get_local_file(&path.to_string_lossy())
        {
            return Ok(PathBuf::from(local_file_path));
        }

        let mut start_byte = 0;
        let file_length = self.get_file_length(path)
            .chain_err(|| "Error getting file length")?;

        let local_file_path = self.local_file_manager
            .get_new_local_file_path()
            .chain_err(|| "Error getting local file path")?;

        let mut options = OpenOptions::new();
        options.read(true);
        options.write(true);
        options.truncate(true);
        options.create(true);
        options.mode(0o777);

        let mut file = options
            .open(local_file_path.to_owned())
            .chain_err({ || format!("unable to create file {}", local_file_path) })?;

        while start_byte < file_length {
            let end_byte = min(file_length, start_byte + MAX_LOCAL_FILE_CHUNK);
            let data = self.read_file_location(path, start_byte, file_length)
                .chain_err(|| "Error getting local file")?;

            file.write_all(&data)
                .chain_err(|| format!("unable to write content to {}", local_file_path,))?;
            start_byte = end_byte;
        }

        self.local_file_manager
            .complete_local_file(&path.to_string_lossy(), &local_file_path);

        Ok(PathBuf::from(local_file_path))
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        debug!("Reading directory {:?}", path);

        let file_info = self.master_interface
            .get_file_info(&path.to_string_lossy())
            .chain_err(|| "Error reading directory")?;

        if !file_info.exists || file_info.is_file {
            return Err("Directory does not exist".into());
        }

        let mut dir_info = Vec::new();
        for file_path in file_info.children.iter() {
            let mut path_buf = PathBuf::new();
            path_buf.push(file_path);
            dir_info.push(path_buf);
        }

        Ok(dir_info)
    }

    fn is_file(&self, path: &Path) -> Result<(bool)> {
        let file_info = self.master_interface
            .get_file_info(&path.to_string_lossy())
            .chain_err(|| "Error checking is file")?;

        if !file_info.exists {
            return Err("File does not exist".into());
        }

        Ok(file_info.is_file)
    }

    fn is_dir(&self, path: &Path) -> Result<(bool)> {
        let file_info = self.master_interface
            .get_file_info(&path.to_string_lossy())
            .chain_err(|| "Error checking is directory")?;

        if !file_info.exists {
            return Err("Directory does not exist".into());
        }

        Ok(!file_info.is_file)
    }

    fn exists(&self, path: &Path) -> Result<(bool)> {
        let file_info = self.master_interface
            .get_file_info(&path.to_string_lossy())
            .chain_err(|| "Error checking file exists")?;

        Ok(file_info.exists)
    }

    fn create_dir_all(&self, _path: &Path) -> Result<()> {
        // Creating a directory is not needed for the distributed file system, can write to any
        // path.
        Ok(())
    }

    fn get_data_closeness(
        &self,
        path: &Path,
        chunk_start: u64,
        chunk_end: u64,
        worker_id: &str,
    ) -> u64 {
        let file_chunks = match self.master_interface
            .get_file_chunks(&path.to_string_lossy())
        {
            Ok(chunks) => chunks,
            Err(err) => {
                output_error(&err.chain_err(|| {
                    format!("Error getting data closeness score for {:?}", path)
                }));
                return 0;
            }
        };

        let mut score: u64 = 0;

        for chunk in file_chunks {
            if chunk.workers.contains(&worker_id.to_string()) {
                let overlap: i64 = min(chunk_end, chunk.end_byte) as i64
                    - max(chunk_start, chunk.start_byte) as i64;
                if overlap > 0 {
                    score += overlap as u64;
                }
            }
        }

        score
    }
}
