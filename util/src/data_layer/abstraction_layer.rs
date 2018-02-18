use std::fs::File;
use std::path::{Path, PathBuf};

use errors::*;

pub trait AbstractionLayer {
    fn open_file(&self, path: &Path) -> Result<File>;

    fn create_file(&self, path: &Path) -> Result<File>;

    fn absolute_path(&self, path: &Path) -> Result<PathBuf>;

    fn abstracted_path(&self, path: &Path) -> Result<PathBuf>;

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    fn exists(&self, path: &Path) -> Result<(bool)>;

    fn is_file(&self, path: &Path) -> Result<(bool)>;

    fn is_dir(&self, path: &Path) -> Result<(bool)>;

    fn create_dir_all(&self, path: &Path) -> Result<()>;
}
