use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use clap::ArgMatches;

use errors::*;
use util::data_layer::{AbstractionLayer, AmazonS3AbstractionLayer, NullAbstractionLayer,
                       NFSAbstractionLayer};
use util::distributed_filesystem::{LocalFileManager, DFSAbstractionLayer,
                                   LocalFileSystemMasterInterface, FileSystemManager,
                                   run_worker_info_upate_loop, WorkerInfoUpdate};

const DEFAULT_DFS_DIRECTORY: &str = "/tmp/cerberus/dfs/";
const DEFAULT_S3_DIRECTORY: &str = "/tmp/cerberus/s3/";

type AbstractionLayerArc = Arc<AbstractionLayer + Send + Sync>;

pub fn get_data_abstraction_layer(
    matches: &ArgMatches,
    worker_info_receiver: Receiver<WorkerInfoUpdate>,
) -> Result<(AbstractionLayerArc, Option<Arc<FileSystemManager>>)> {
    let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>;
    let filesystem_manager: Option<Arc<FileSystemManager>>;

    let nfs_path = matches.value_of("nfs");
    let dfs = matches.is_present("dfs");
    let s3 = matches.value_of("s3");
    let storage_location = matches.value_of("storage-location");
    if let Some(path) = nfs_path {
        data_abstraction_layer = Arc::new(NFSAbstractionLayer::new(Path::new(path)));
        filesystem_manager = None;
    } else if dfs {
        let mut storage_dir = PathBuf::new();
        storage_dir.push(storage_location.unwrap_or(DEFAULT_DFS_DIRECTORY));

        let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));
        let file_manager_arc = Arc::new(FileSystemManager::new());

        let master_interface = Box::new(LocalFileSystemMasterInterface::new(
            Arc::clone(&file_manager_arc),
        ));

        data_abstraction_layer = Arc::new(DFSAbstractionLayer::new(
            Arc::clone(&local_file_manager_arc),
            master_interface,
        ));

        filesystem_manager = Some(file_manager_arc);
    } else if let Some(bucket) = s3 {
        let mut storage_dir = PathBuf::new();
        storage_dir.push(storage_location.unwrap_or(DEFAULT_S3_DIRECTORY));

        let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));

        let s3_layer =
            AmazonS3AbstractionLayer::new(bucket.into(), Arc::clone(&local_file_manager_arc))
                .chain_err(|| "Unable to create AmazonS3 abstraction layer")?;
        data_abstraction_layer = Arc::new(s3_layer);

        filesystem_manager = None;
    } else {
        data_abstraction_layer = Arc::new(NullAbstractionLayer::new());
        filesystem_manager = None;
    }

    run_worker_info_upate_loop(&filesystem_manager, worker_info_receiver);

    Ok((data_abstraction_layer, filesystem_manager))
}
