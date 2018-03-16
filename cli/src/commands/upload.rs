use std::fs;
use std::fs::{DirEntry, File};
use std::io::prelude::Read;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;

use clap::ArgMatches;

use errors::*;
use util::distributed_filesystem::{NetworkFileSystemMasterInterface, FileSystemMasterInterface};

fn get_local_files(path: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();

    if path.is_dir() {
        let entries = fs::read_dir(path).chain_err(
            || "Unable to read local file directroy",
        )?;

        for entry in entries {
            let entry: DirEntry = entry.chain_err(|| "Error reading input directory")?;
            let entry_path = Path::new("/").join(entry.path());
            if entry_path.is_file() {
                files.push(entry_path.to_string_lossy().to_string());
            }
        }
    } else {
        files.push(path.to_string_lossy().to_string());
    }

    Ok(files)
}

fn upload_local_file(
    master_interface: &NetworkFileSystemMasterInterface,
    local_path: &str,
    remote_path: &str,
) -> Result<()> {
    println!("Uploading File {} to Cluster", local_path);
    //TODO(conor): Improve this function to allow for files that can not be kept in memory.

    let file = File::open(local_path).chain_err(|| {
        format!("unable to open file {}", local_path)
    })?;

    let mut buf_reader = BufReader::new(file);
    let mut data = Vec::new();
    buf_reader.read_to_end(&mut data).chain_err(|| {
        format!("unable to read content of {}", local_path)
    })?;

    master_interface
        .upload_file_chunk(remote_path, 0, data)
        .chain_err(|| "Error uploading file chunk.")?;

    Ok(())
}

fn get_upload_file_path(local_path: &str, remote_path: Option<&str>) -> Result<String> {
    let remote_path_str = match remote_path {
        Some(rstr) => rstr,
        None => return Ok(local_path.to_owned()),
    };

    let remote_path = Path::new(remote_path_str);
    if remote_path.is_file() {
        return Ok(remote_path_str.to_owned());
    }

    let local_file_name = Path::new(&local_path).file_name().chain_err(
        || "Error getting file name to upload",
    )?;

    let remote_path = remote_path.join(local_file_name);
    Ok(remote_path.to_string_lossy().to_string())
}

pub fn upload(master_addr: &SocketAddr, args: &ArgMatches) -> Result<()> {
    println!("Uploading File(s) to Cluster...");

    let local_path = args.value_of("local_path").chain_err(
        || "Local path can not be empty",
    )?;

    let remote_path = args.value_of("remote_path");

    let local_files = get_local_files(Path::new(local_path)).chain_err(
        || "Error getting files to uplaod to cluster",
    )?;

    if local_files.is_empty() {
        return Err(
            "No local file found to upload. Is the directory empty?".into(),
        );
    } else if let Some(remote_path_str) = remote_path {
        if local_files.len() > 1 {
            let remote_path = Path::new(remote_path_str);
            if remote_path.is_file() {
                return Err(
                    "Remote path must be directory when uploading more than one file.".into(),
                );
            }
        }
    }

    let master_interface =
        NetworkFileSystemMasterInterface::new(*master_addr)
            .chain_err(|| "Error creating distributed filesystem master interface")?;

    for local_path in local_files {
        let remote_path_str = get_upload_file_path(&local_path, remote_path).chain_err(
            || "Error getting file path to upload",
        )?;

        upload_local_file(&master_interface, &local_path, &remote_path_str)
            .chain_err(|| "Error uploading local file")?;
    }

    Ok(())
}
