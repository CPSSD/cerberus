use std::net::SocketAddr;
use std::sync::Arc;
use std::path::{Path, PathBuf};

use clap::ArgMatches;

use errors::*;
use util::data_layer::{AbstractionLayer, NullAbstractionLayer, NFSAbstractionLayer};
use util::distributed_filesystem::{LocalFileManager, DFSAbstractionLayer,
                                   NetworkFileSystemMasterInterface};

const DEFAULT_DFS_DIRECTORY: &str = "/tmp/cerberus/dfs/";

type AbstractionLayerArc = Arc<AbstractionLayer + Send + Sync>;

pub fn get_data_abstraction_layer(
    master_addr: SocketAddr,
    matches: &ArgMatches,
) -> Result<(AbstractionLayerArc, Option<Arc<LocalFileManager>>)> {
    let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>;
    let local_file_manager: Option<Arc<LocalFileManager>>;

    let nfs_path = matches.value_of("nfs");
    let dfs = matches.is_present("dfs");
    if let Some(path) = nfs_path {
        data_abstraction_layer = Arc::new(NFSAbstractionLayer::new(Path::new(path)));
        local_file_manager = None;
    } else if dfs {
        let mut storage_dir = PathBuf::new();
        storage_dir.push(matches.value_of("dfs-location").unwrap_or(
            DEFAULT_DFS_DIRECTORY,
        ));

        let local_file_manager_arc = Arc::new(LocalFileManager::new(storage_dir));

        let master_interface = Box::new(
            NetworkFileSystemMasterInterface::new(master_addr)
                .chain_err(|| "Error creating filesystem master interface.")?,
        );

        data_abstraction_layer = Arc::new(DFSAbstractionLayer::new(
            Arc::clone(&local_file_manager_arc),
            master_interface,
        ));

        local_file_manager = Some(local_file_manager_arc);
    } else {
        data_abstraction_layer = Arc::new(NullAbstractionLayer::new());
        local_file_manager = None;
    }

    Ok((data_abstraction_layer, local_file_manager))
}
