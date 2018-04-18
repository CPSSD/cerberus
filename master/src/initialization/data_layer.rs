use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use clap::ArgMatches;

use errors::*;
use util::data_layer::{AbstractionLayer, AmazonS3AbstractionLayer, NFSAbstractionLayer,
                       NullAbstractionLayer};
use util::distributed_filesystem::{run_worker_info_upate_loop, DFSAbstractionLayer,
                                   FileSystemManager, LocalFileManager,
                                   LocalFileSystemMasterInterface, WorkerInfoUpdate};

const DEFAULT_DFS_DIRECTORY: &str = "/tmp/cerberus/dfs/";
const DEFAULT_S3_DIRECTORY: &str = "/tmp/cerberus/s3/";

type AbstractionLayerArc = Arc<AbstractionLayer + Send + Sync>;

fn initialize_dfs(
    storage_location: &Option<&str>,
) -> (AbstractionLayerArc, Arc<FileSystemManager>) {
    let mut storage_dir = PathBuf::new();
    storage_dir.push(storage_location.unwrap_or(DEFAULT_DFS_DIRECTORY));

    let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));
    let file_manager_arc = Arc::new(FileSystemManager::new());

    let master_interface = Box::new(LocalFileSystemMasterInterface::new(Arc::clone(
        &file_manager_arc,
    )));

    let dfs_abstraction_layer = Arc::new(DFSAbstractionLayer::new(
        Arc::clone(&local_file_manager_arc),
        master_interface,
    ));

    (dfs_abstraction_layer, file_manager_arc)
}

fn initialize_s3(storage_location: &Option<&str>, bucket: &str) -> Result<AbstractionLayerArc> {
    let mut storage_dir = PathBuf::new();
    storage_dir.push(storage_location.unwrap_or(DEFAULT_S3_DIRECTORY));

    let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));

    let s3_layer =
        AmazonS3AbstractionLayer::new(bucket.into(), Arc::clone(&local_file_manager_arc))
            .chain_err(|| "Unable to create AmazonS3 abstraction layer")?;

    Ok(Arc::new(s3_layer))
}

pub fn get_data_abstraction_layer(
    matches: &ArgMatches,
    worker_info_receiver: Receiver<WorkerInfoUpdate>,
) -> Result<(AbstractionLayerArc, Option<Arc<FileSystemManager>>)> {
    let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>;
    let mut filesystem_manager: Option<Arc<FileSystemManager>> = None;

    let storage_location = matches.value_of("storage-location");
    if let Some(path) = matches.value_of("nfs") {
        data_abstraction_layer = Arc::new(NFSAbstractionLayer::new(Path::new(path)));
    } else if matches.is_present("dfs") {
        let (abstraction_layer, file_manager_arc) = initialize_dfs(&storage_location);

        data_abstraction_layer = abstraction_layer;
        filesystem_manager = Some(file_manager_arc);
    } else if let Some(bucket) = matches.value_of("s3") {
        data_abstraction_layer = initialize_s3(&storage_location, bucket)
            .chain_err(|| "Error initializing S3 abstraction layer")?;
    } else {
        data_abstraction_layer = Arc::new(NullAbstractionLayer::new());
    }

    run_worker_info_upate_loop(&filesystem_manager, worker_info_receiver);

    Ok((data_abstraction_layer, filesystem_manager))
}
