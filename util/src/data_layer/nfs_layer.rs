use std::fs::{File, DirEntry};
use std::fs;
use std::path::{Path, PathBuf};

use errors::*;

use data_layer::abstraction_layer::AbstractionLayer;

pub struct NFSAbstractionLayer {
    nfs_path: PathBuf,
}

impl NFSAbstractionLayer {
    pub fn new(nfs_path: &Path) -> Self {
        NFSAbstractionLayer { nfs_path: PathBuf::from(nfs_path) }
    }
}

impl AbstractionLayer for NFSAbstractionLayer {
    fn open_file(&self, path: &Path) -> Result<File> {
        let file_path = self.absolute_path(path).chain_err(|| "Unable to get path")?;
        debug!("Opening file: {}", file_path.to_string_lossy());
        File::open(file_path.clone()).chain_err(|| format!("unable to open file {:?}", file_path))
    }

    fn create_file(&self, path: &Path) -> Result<File> {
        let file_path = self.absolute_path(path).chain_err(|| "Unable to get path")?;
        debug!("Creating file: {}", file_path.to_string_lossy());
        File::create(file_path.clone()).chain_err(|| {
            format!("unable to create file {:?}", file_path)
        })
    }

    fn absolute_path(&self, path: &Path) -> Result<PathBuf> {
        debug!(
            "Attempting to get absolute path: {:?}, {:?}",
            self.nfs_path,
            path
        );

        let relative_path = path.strip_prefix("/").chain_err(
            || "Error occured stripping prefix",
        )?;
        Ok(self.nfs_path.join(relative_path))
    }

    fn abstracted_path(&self, path: &Path) -> Result<PathBuf> {
        if path.starts_with(self.nfs_path.clone()) {
            let abstracted_path = path.strip_prefix(self.nfs_path.as_path()).chain_err(
                || "Unable to strip prefix from path",
            )?;
            return Ok(PathBuf::from(abstracted_path));
        }
        Ok(PathBuf::from(path))
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let absolute_path = self.absolute_path(path).chain_err(|| "Unable to get path")?;
        debug!("Reading from {:?}", absolute_path);
        let entries = fs::read_dir(absolute_path.as_path()).chain_err(
            || "Unable to read input directroy",
        )?;
        let mut abstracted_entries: Vec<PathBuf> = vec![];
        for entry in entries {
            let entry: DirEntry = entry.chain_err(|| "Error reading input directory")?;
            let abstracted_path = self.abstracted_path(&entry.path()).chain_err(
                || "Unable to get abstracted path",
            )?;
            abstracted_entries.push(Path::new("/").join(abstracted_path))
        }
        Ok(abstracted_entries)
    }

    fn is_file(&self, path: &Path) -> Result<(bool)> {
        let absolute_path = self.absolute_path(path).chain_err(|| "")?;
        Ok(absolute_path.is_file())
    }

    fn is_dir(&self, path: &Path) -> Result<(bool)> {
        let absolute_path = self.absolute_path(path).chain_err(|| "")?;
        Ok(absolute_path.is_dir())
    }

    fn exists(&self, path: &Path) -> Result<(bool)> {
        let absolute_path = self.absolute_path(path).chain_err(|| "")?;
        Ok(absolute_path.exists())
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let absolute_path = self.absolute_path(path).chain_err(
            || "Unable to get absolute_path",
        )?;
        debug!("Creating directory: {:?}", absolute_path);
        fs::create_dir_all(&absolute_path.as_path()).chain_err(|| "Unable to create directories")
    }
}
