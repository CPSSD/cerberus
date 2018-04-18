use std::cmp::min;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use util::data_layer::AbstractionLayer;
use util::distributed_filesystem::{DFSAbstractionLayer, LocalFileManager,
                                   NetworkFileSystemMasterInterface};

const DFS_FILE_DIRECTORY: &str = "/tmp/cerberus/dfs/";
const MEGA_BYTE: u64 = 1000 * 1000;
const MAX_DOWNLOAD_SIZE: u64 = MEGA_BYTE * 32;

fn get_files_to_download(
    data_layer: &DFSAbstractionLayer,
    remote_path: &str,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    if data_layer
        .is_file(Path::new(remote_path))
        .chain_err(|| "Error checking is file")?
    {
        files.push(remote_path.to_string());
    } else {
        let dir_entries = data_layer
            .read_dir(Path::new(remote_path))
            .chain_err(|| "Error reading directory")?;

        for entry in dir_entries {
            files.push(entry.to_string_lossy().to_string());
        }
    }

    Ok(files)
}

fn download_file(
    data_layer: &DFSAbstractionLayer,
    remote_path: &str,
    local_path: &str,
) -> Result<()> {
    println!("Downloading file {} to {}", remote_path, local_path);

    let mut local_directory = Path::new(local_path).to_path_buf();
    local_directory.pop();
    fs::create_dir_all(local_directory).chain_err(|| "Error creating new local directory")?;

    let mut file =
        File::create(local_path).chain_err(|| format!("unable to create file {}", local_path))?;

    let remote_path = Path::new(remote_path);

    let mut start_byte = 0;
    let file_length = data_layer
        .get_file_length(remote_path)
        .chain_err(|| "Error getting file length")?;

    while start_byte < file_length {
        let end_byte = min(file_length, start_byte + MAX_DOWNLOAD_SIZE);
        let file_data = data_layer
            .read_file_location(remote_path, start_byte, end_byte)
            .chain_err(|| "Error reading file")?;

        file.write_all(&file_data)
            .chain_err(|| format!("unable to write content to {}", local_path,))?;
        start_byte = end_byte;
    }

    Ok(())
}

fn get_download_file_path(local_path_str: &str, remote_path: &str) -> Result<String> {
    let local_path = Path::new(local_path_str);

    if local_path.is_dir() {
        let file_name = Path::new(&remote_path)
            .file_name()
            .chain_err(|| "Error getting file name to download")?;

        let local_path = local_path.join(file_name);
        return Ok(local_path.to_string_lossy().to_string());
    }

    Ok(local_path_str.to_owned())
}

pub fn download(master_addr: &SocketAddr, args: &ArgMatches) -> Result<()> {
    println!("Downloading File(s) from Cluster...");

    let remote_path = args.value_of("remote_path")
        .chain_err(|| "Remote path can not be empty")?;

    let local_path = args.value_of("local_path")
        .chain_err(|| "Local path can not be empty")?;

    let master_interface = NetworkFileSystemMasterInterface::new(*master_addr)
        .chain_err(|| "Error creating distributed filesystem master interface")?;

    let mut path_buf = PathBuf::new();
    path_buf.push(DFS_FILE_DIRECTORY);
    let local_file_manager = Arc::new(LocalFileManager::new(path_buf));
    let data_layer = DFSAbstractionLayer::new(local_file_manager, Box::new(master_interface));

    let files = get_files_to_download(&data_layer, remote_path)
        .chain_err(|| "Error getting files to download")?;

    if files.is_empty() {
        return Err("No files found to download".into());
    } else if files.len() > 1 && Path::new(local_path).is_file() {
        return Err("Local Path must be directory to download multiple files".into());
    }

    for remote_file_path in files {
        let download_path = get_download_file_path(local_path, &remote_file_path)
            .chain_err(|| "Error getting download path")?;

        download_file(&data_layer, &remote_file_path, &download_path)
            .chain_err(|| "Error downloading file")?;
    }

    Ok(())
}
