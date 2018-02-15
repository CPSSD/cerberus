use std::fs::{File, DirEntry};
use std::fs;
use std::path::{Path, PathBuf};

use errors::*;

use data_layer::abstraction_layer::AbstractionLayer;

// NullAbstractionLayer handles data interaction on a simple unix file system.
#[derive(Default)]
pub struct NullAbstractionLayer;

impl NullAbstractionLayer {
    pub fn new() -> Self {
        NullAbstractionLayer
    }
}

impl AbstractionLayer for NullAbstractionLayer {
    fn open_file(&self, path: &Path) -> Result<File> {
        debug!("Opening file: {}", path.to_string_lossy());
        File::open(&path).chain_err(|| format!("unable to open file {:?}", path))
    }

    fn create_file(&self, path: &Path) -> Result<File> {
        debug!("Creating file: {}", path.to_string_lossy());
        File::create(&path).chain_err(|| format!("unable to create file {:?}", path))
    }

    fn absolute_path(&self, path: &Path) -> Result<PathBuf> {
        Ok(PathBuf::from(path))
    }

    fn abstracted_path(&self, path: &Path) -> Result<PathBuf> {
        Ok(PathBuf::from(path))
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(path).chain_err(
            || "Unable to read input directroy",
        )?;
        let mut pathbufs: Vec<PathBuf> = vec![];
        for entry in entries {
            let entry: DirEntry = entry.chain_err(|| "Error reading input directory")?;
            pathbufs.push(Path::new("/").join(entry.path()))
        }
        Ok(pathbufs)
    }

    fn is_file(&self, path: &Path) -> Result<bool> {
        Ok(path.is_file())
    }

    fn is_dir(&self, path: &Path) -> Result<bool> {
        Ok(path.is_dir())
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(path.exists())
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).chain_err(|| "Unable to create directories")
    }
}
