use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use errors::*;

pub fn read<P: AsRef<Path>>(path: P) -> Result<String> {
    let file = File::open(&path).chain_err(
        || format!("unable to open file {}", path.as_ref().to_string_lossy()),
    )?;

    let mut buf_reader = BufReader::new(file);
    let mut value = String::new();
    buf_reader.read_to_string(&mut value).chain_err(
        || format!("unable to read content of {}", path.as_ref().to_string_lossy()),
    )?;

    Ok(value)
}

pub fn write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    let mut file = File::create(&path).chain_err(
        || format!("unable to create file {}", path.as_ref().to_string_lossy()),
    )?;
    file.write_all(data).chain_err(
        || format!("unable to write content to {}", path.as_ref().to_string_lossy()),
    )?;

    Ok(())
}
