use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::str;
use std::sync::Arc;

use cerberus_proto::worker as pb;
use errors::*;

use util::data_layer::AbstractionLayer;

#[cfg(test)]
use mocktopus;
#[cfg(test)]
use mocktopus::macros::*;

pub fn read_location(
    data_abstraction_layer: &Arc<AbstractionLayer + Send + Sync>,
    input_location: &pb::InputLocation,
) -> Result<String> {
    let path = Path::new(input_location.get_input_path());

    let buffer: Vec<u8> = data_abstraction_layer
        .read_file_location(path, input_location.start_byte, input_location.end_byte)
        .chain_err(|| "Error reading file location.")?;

    let value = str::from_utf8(&buffer)
        .chain_err(|| format!("Invalid string in file {}", input_location.get_input_path()))?;

    Ok(value.to_owned())
}

pub fn read_local<P: AsRef<Path>>(path: P) -> Result<String> {
    debug!("Attempting to read local file: {:?}", path.as_ref());
    let file = File::open(&path)
        .chain_err(|| format!("unable to open file {}", path.as_ref().to_string_lossy()))?;

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

#[cfg_attr(test, mockable)]
pub fn write<P: AsRef<Path>>(
    data_abstraction_layer_arc: &Arc<AbstractionLayer + Send + Sync>,
    path: P,
    data: &[u8],
) -> Result<()> {
    data_abstraction_layer_arc
        .write_file(path.as_ref(), data)
        .chain_err(|| format!("Unable to write file {}", path.as_ref().to_string_lossy()))?;

    Ok(())
}

#[cfg_attr(test, mockable)]
pub fn write_local<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    debug!("Attempting to write to local file: {:?}", path.as_ref());
    let mut file = File::create(&path)
        .chain_err(|| format!("unable to create file {}", path.as_ref().to_string_lossy()))?;
    file.write_all(data).chain_err(|| {
        format!(
            "unable to write content to {}",
            path.as_ref().to_string_lossy()
        )
    })?;

    Ok(())
}
