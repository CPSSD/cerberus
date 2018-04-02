use std::path::{Path, PathBuf};

use errors::*;

pub trait AbstractionLayer {
    fn get_file_length(&self, path: &Path) -> Result<u64>;

    fn read_file_location(&self, path: &Path, start_byte: u64, end_byte: u64) -> Result<Vec<u8>>;

    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()>;

    /// `get_local_file` returns a filepath of the given file on the local machine. If the file can
    /// not be accessed on the local machine already, it will retrive the file and create it on the
    /// local machine.
    fn get_local_file(&self, path: &Path) -> Result<PathBuf>;

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    fn exists(&self, path: &Path) -> Result<(bool)>;

    fn is_file(&self, path: &Path) -> Result<(bool)>;

    fn is_dir(&self, path: &Path) -> Result<(bool)>;

    fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// `get_file_closeness` returns how an integer indicating how easy it is for a given
    /// worker to request a given file from the file system. If a file is on the local filesystem
    /// of the worker then the closeness score should be high, if chunks of the file need to be
    /// requested from other workers then the score should be low.
    fn get_file_closeness(&self, path: &Path, worker_id: &str) -> Result<u64>;
}
