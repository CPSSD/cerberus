use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::ArgMatches;

use util::data_layer::{AbstractionLayer, NullAbstractionLayer, NFSAbstractionLayer};
use util::distributed_filesystem::{LocalFileManager, DFSAbstractionLayer,
                                   LocalFileSystemMasterInterface, FileSystemManager,
                                   WorkerInfoProvider};

const DFS_FILE_DIRECTORY: &str = "/tmp/cerberus/dfs/";

pub fn get_data_abstraction_layer(
    matches: &ArgMatches,
    worker_info_provider: &Arc<WorkerInfoProvider + Send + Sync>,
) -> (Arc<AbstractionLayer + Send + Sync>, Option<Arc<FileSystemManager>>) {
    let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>;
    let filesystem_manager: Option<Arc<FileSystemManager>>;

    let nfs_path = matches.value_of("nfs");
    let dfs = matches.is_present("dfs");
    if let Some(path) = nfs_path {
        data_abstraction_layer = Arc::new(NFSAbstractionLayer::new(Path::new(path)));
        filesystem_manager = None;
    } else if dfs {
        let mut storage_dir = PathBuf::new();
        storage_dir.push(DFS_FILE_DIRECTORY);
        let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));
        let file_manager_arc = Arc::new(FileSystemManager::new(Arc::clone(worker_info_provider)));

        let master_interface = Box::new(LocalFileSystemMasterInterface::new(
            Arc::clone(&file_manager_arc),
        ));

        data_abstraction_layer = Arc::new(DFSAbstractionLayer::new(
            Arc::clone(&local_file_manager_arc),
            master_interface,
        ));

        filesystem_manager = Some(file_manager_arc);
    } else {
        data_abstraction_layer = Arc::new(NullAbstractionLayer::new());
        filesystem_manager = None;
    }

    (data_abstraction_layer, filesystem_manager)
}