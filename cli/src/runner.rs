use std::env;
use std::fs;
use std::fs::{DirEntry, File};
use std::io::prelude::{Read, Write};
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::path::{Path, PathBuf};

use chrono::Local;
use clap::ArgMatches;
use grpc::RequestOptions;
use uuid::Uuid;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;
use cerberus_proto::mapreduce_grpc::MapReduceService; // do not use
use errors::*;
use util::data_layer::AbstractionLayer;
use util::distributed_filesystem::{NetworkFileSystemMasterInterface, FileSystemMasterInterface,
                                   DFSAbstractionLayer, LocalFileManager};

// Directory of client ID in the users home directory.
const CLIENT_ID_DIR: &str = ".local/share/";
// Client ID file name.
const CLIENT_ID_FILE: &str = "cerberus";
// Default priority applied to jobs.
const DEFAULT_PRIORITY: &str = "3";
const DFS_FILE_DIRECTORY: &str = "/tmp/cerberus/dfs/";

fn verify_valid_path(path_str: &str) -> Result<String> {
    let path = Path::new(path_str);

    if !path.is_absolute() {
        return Err("Paths passed to the CLI must be absolute.".into());
    }

    let path_option = path.to_str();
    match path_option {
        Some(res) => Ok(res.to_owned()),
        None => Err("Invalid characters in path.".into()),
    }
}

fn create_new_client_id(dir: &str, file_path: &str) -> Result<String> {
    // Create new client id as we do not have one saved.
    fs::create_dir_all(dir).chain_err(
        || "Error creating new client id.",
    )?;

    let client_id = Uuid::new_v4().to_string();
    let mut file = fs::File::create(file_path).chain_err(
        || "Error creating new client id.",
    )?;

    file.write_all(client_id.as_bytes()).chain_err(
        || "Error creating new client id.",
    )?;

    Ok(client_id)
}

fn get_client_id() -> Result<String> {
    let mut path_buf = env::home_dir().chain_err(|| "Error getting client id.")?;

    path_buf.push(CLIENT_ID_DIR);
    let dir = path_buf
        .to_str()
        .chain_err(|| "Error getting client id.")?
        .to_owned();

    path_buf.push(CLIENT_ID_FILE);
    let file_path = path_buf.to_str().chain_err(|| "Error getting client id.")?;

    if fs::metadata(file_path).is_ok() {
        let mut file = fs::File::open(file_path).chain_err(
            || "Error getting client id.",
        )?;

        let mut client_id = String::new();
        file.read_to_string(&mut client_id).chain_err(
            || "Error getting client id.",
        )?;

        return Ok(client_id);
    }

    create_new_client_id(&dir, file_path).chain_err(|| "Error getting client id.")
}

pub fn run(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut input = matches
        .value_of("input")
        .chain_err(|| "Input directory cannot be empty")?
        .to_owned();

    input = verify_valid_path(&input).chain_err(
        || "Invalid input path.",
    )?;

    let output = matches.value_of("output").unwrap_or("");
    if !output.is_empty() {
        verify_valid_path(output).chain_err(
            || "Invalid output path",
        )?;
    }

    let mut binary = matches
        .value_of("binary")
        .chain_err(|| "Binary cannot be empty")?
        .to_owned();

    binary = verify_valid_path(&binary).chain_err(
        || "Invalid binary path.",
    )?;

    let priority_str = matches.value_of("priority").unwrap_or(DEFAULT_PRIORITY);
    let priority: u32 = match priority_str.parse() {
        Ok(val) => val,
        Err(err) => {
            return Err(
                format!(
                    "Error occured while converting '{}' to a u32: {}",
                    priority_str,
                    err
                ).into(),
            );
        }
    };
    if priority < 1 || priority > 10 {
        return Err(
            format!(
                "Priority can only be between 1 and 10. {} is not in this range",
                priority
            ).into(),
        );
    }

    let mut req = pb::MapReduceRequest::new();
    req.set_binary_path(binary.to_owned());
    req.set_input_directory(input.to_owned());
    req.set_client_id(get_client_id()?);
    req.set_output_directory(output.to_owned());
    req.set_priority(priority);

    let res = client
        .perform_map_reduce(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to schedule MapReduce")?
        .1;

    println!("MapReduce {} scheduled", res.get_mapreduce_id().to_owned());

    Ok(())
}

pub fn cluster_status(client: &grpc_pb::MapReduceServiceClient) -> Result<()> {
    let res = client
        .cluster_status(RequestOptions::new(), pb::EmptyMessage::new())
        .wait()
        .chain_err(|| "Failed to get cluster status")?
        .1;
    println!(
        "Workers:\t{}\nQueue:\t\t{}",
        res.get_workers(),
        res.get_queue_size()
    );

    Ok(())
}

pub fn cancel(client: &grpc_pb::MapReduceServiceClient, args: Option<&ArgMatches>) -> Result<()> {
    let mut req = pb::MapReduceCancelRequest::new();
    req.set_client_id(get_client_id().chain_err(|| "Error getting client id")?);
    if let Some(sub) = args {
        if let Some(id) = sub.value_of("id") {
            req.set_mapreduce_id(id.to_owned());
        }
    }

    let resp = client
        .cancel_map_reduce(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to cancel MapReduce")?
        .1;

    println!(
        "Succesfully cancelled MapReduce with ID: {}",
        resp.mapreduce_id
    );
    Ok(())
}

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

fn get_files_to_download(
    data_layer: &DFSAbstractionLayer,
    remote_path: &str,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    if data_layer.is_file(Path::new(remote_path)).chain_err(
        || "Error checking is file",
    )?
    {
        files.push(remote_path.to_string());
    } else {
        let dir_entries = data_layer.read_dir(Path::new(remote_path)).chain_err(
            || "Error reading directory",
        )?;

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

    let remote_path = Path::new(remote_path);
    let file_length = data_layer.get_file_length(remote_path).chain_err(
        || "Error getting file length",
    )?;

    let file_data = data_layer
        .read_file_location(remote_path, 0 /* Start btye */, file_length)
        .chain_err(|| "Error reading file")?;

    let mut local_directory = Path::new(local_path).to_path_buf();
    local_directory.pop();
    fs::create_dir_all(local_directory).chain_err(
        || "Error creating new local directory",
    )?;

    let mut file = File::create(local_path).chain_err(|| {
        format!("unable to create file {}", local_path)
    })?;

    file.write_all(&file_data).chain_err(|| {
        format!(
            "unable to write content to {}",
            local_path,
        )
    })?;

    Ok(())
}

fn get_download_file_path(local_path_str: &str, remote_path: &str) -> Result<String> {
    let local_path = Path::new(local_path_str);

    if local_path.is_dir() {
        let file_name = Path::new(&remote_path).file_name().chain_err(
            || "Error getting file name to download",
        )?;

        let local_path = local_path.join(file_name);
        return Ok(local_path.to_string_lossy().to_string());
    }

    Ok(local_path_str.to_owned())
}

pub fn download(master_addr: &SocketAddr, args: &ArgMatches) -> Result<()> {
    println!("Downloading File(s) from Cluster...");

    let remote_path = args.value_of("remote_path").chain_err(
        || "Remote path can not be empty",
    )?;

    let local_path = args.value_of("local_path").chain_err(
        || "Local path can not be empty",
    )?;

    let master_interface =
        NetworkFileSystemMasterInterface::new(*master_addr)
            .chain_err(|| "Error creating distributed filesystem master interface")?;

    let mut path_buf = PathBuf::new();
    path_buf.push(DFS_FILE_DIRECTORY);
    let local_file_manager = Arc::new(LocalFileManager::new(path_buf));
    let data_layer = DFSAbstractionLayer::new(local_file_manager, Box::new(master_interface));

    let files = get_files_to_download(&data_layer, remote_path).chain_err(
        || "Error getting files to download",
    )?;

    if files.is_empty() {
        return Err("No files found to download".into());
    } else if files.len() > 1 && Path::new(local_path).is_file() {
        return Err(
            "Local Path must be directory to download multiple files".into(),
        );
    }

    for remote_file_path in files {
        let download_path = get_download_file_path(local_path, &remote_file_path)
            .chain_err(|| "Error getting download path")?;

        download_file(&data_layer, &remote_file_path, &download_path)
            .chain_err(|| "Error downloading file")?;
    }

    Ok(())
}

pub fn status(client: &grpc_pb::MapReduceServiceClient, matches: &ArgMatches) -> Result<()> {
    let mut req = pb::MapReduceStatusRequest::new();
    req.set_client_id(get_client_id()?);

    if let Some(id) = matches.value_of("job_id") {
        req.set_mapreduce_id(id.to_owned());
    }

    let res = client
        .map_reduce_status(RequestOptions::new(), req)
        .wait()
        .chain_err(|| "Failed to get MapReduce Status")?
        .1;

    let reports = res.get_reports();
    if reports.is_empty() {
        println!("No jobs for this client on cluster.");
    } else {
        for rep in reports {
            print_table(rep);
        }
    }

    Ok(())
}

fn print_table(rep: &pb::MapReduceReport) {
    let id = rep.get_mapreduce_id();
    let output = rep.get_output_directory();

    let status: String = match rep.get_status() {
        pb::Status::UNKNOWN => "UNKNOWN".to_owned(),
        pb::Status::DONE => {
            let time_taken = rep.get_done_timestamp() - rep.get_started_timestamp();
            format!("DONE ({}s)", time_taken)
        }
        pb::Status::IN_PROGRESS => {
            format!(
                "IN_PROGRESS ({})",
                get_time_offset(rep.get_started_timestamp())
            )
        }
        pb::Status::IN_QUEUE => format!("IN_QUEUE ({})", rep.get_queue_length()),
        pb::Status::FAILED => format!("FAILED\n{}", rep.get_failure_details()).to_owned(),
        pb::Status::CANCELLED => "CANCELLED".to_owned(),
    };

    table!(["MRID", id], ["Status", status], ["Output", output]).printstd();
}

fn get_time_offset(offset: i64) -> String {
    format!("{}s", Local::now().timestamp() - offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_time_offset_test() {
        assert_eq!(get_time_offset(Local::now().timestamp() - 10), "10s");
    }
}
