use std::fs::{File, DirEntry};
use std::fs;
use std::io::{Read, Write, Seek, SeekFrom};
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

    fn open_file(&self, path: &Path) -> Result<File> {
        debug!("Opening file: {}", path.to_string_lossy());
        File::open(&path).chain_err(|| format!("unable to open file {:?}", path))
    }
}

impl AbstractionLayer for NullAbstractionLayer {
    fn get_file_length(&self, path: &Path) -> Result<u64> {
        debug!("Getting file length: {:?}", path);

        let metadata = fs::metadata(path).chain_err(|| "Error getting metadata")?;

        Ok(metadata.len())
    }

    fn read_file_location(&self, path: &Path, start_byte: u64, end_byte: u64) -> Result<Vec<u8>> {
        debug!("Reading file: {:?}", path);

        let mut file = self.open_file(path)?;
        file.seek(SeekFrom::Start(start_byte)).chain_err(|| {
            format!("Error reading file {:?}", path)
        })?;

        let mut bytes = vec![0; (end_byte - start_byte) as usize];
        file.read_exact(&mut bytes).chain_err(|| {
            format!("Error reading file {:?}", path)
        })?;

        Ok(bytes)
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        debug!("Writing file: {}", path.to_string_lossy());
        let mut file = File::create(&path).chain_err(|| {
            format!("unable to create file {:?}", path)
        })?;

        file.write_all(data).chain_err(|| {
            format!("unable to write content to {:?}", path)
        })
    }

    fn get_local_file(&self, path: &Path) -> Result<PathBuf> {
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
        debug!("Creating directory: {:?}", path);
        fs::create_dir_all(path).chain_err(|| "Unable to create directories")
    }
}
