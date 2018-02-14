use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str;

use cerberus_proto::worker as pb;
use errors::*;

#[cfg(test)]
use mocktopus;
#[cfg(test)]
use mocktopus::macros::*;

pub fn read<P: AsRef<Path>>(path: P) -> Result<String> {
    let file = File::open(&path).chain_err(|| {
        format!("unable to open file {}", path.as_ref().to_string_lossy())
    })?;

    let mut buf_reader = BufReader::new(file);
    let mut value = String::new();
    buf_reader.read_to_string(&mut value).chain_err(|| {
        format!(
            "unable to read content of {}",
            path.as_ref().to_string_lossy()
        )
    })?;

    Ok(value)
}

pub fn read_location(input_location: &pb::InputLocation) -> Result<String> {
    let mut path = PathBuf::new();
    path.push(input_location.get_input_path());

    let file = File::open(&path).chain_err(|| {
        format!("unable to open file {}", input_location.get_input_path())
    })?;

    let mut buf_reader = BufReader::new(file);
    buf_reader
        .seek(SeekFrom::Start(input_location.start_byte as u64))
        .chain_err(|| {
            format!(
                "could not seek file {} to position {}",
                input_location.get_input_path(),
                input_location.start_byte
            )
        })?;

    let mut buffer = vec![0; (input_location.end_byte - input_location.start_byte) as usize];
    buf_reader.read(&mut buffer[..]).chain_err(|| {
        format!(
            "unable to read content of {}",
            input_location.get_input_path()
        )
    })?;

    let value = str::from_utf8(&buffer).chain_err(|| {
        format!("Invalid string in file {}", input_location.get_input_path())
    })?;

    Ok(value.to_owned())
}

#[cfg_attr(test, mockable)]
pub fn write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    let mut file = File::create(&path).chain_err(|| {
        format!("unable to create file {}", path.as_ref().to_string_lossy())
    })?;
    file.write_all(data).chain_err(|| {
        format!(
            "unable to write content to {}",
            path.as_ref().to_string_lossy()
        )
    })?;

    Ok(())
}
