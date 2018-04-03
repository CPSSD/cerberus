use std::env;
use std::fs;
use std::io::{Read, Write};

use uuid::Uuid;

use errors::*;

// Directory of client ID in the users home directory.
const CLIENT_ID_DIR: &str = ".local/share/";

// Client ID file name.
const CLIENT_ID_FILE: &str = "cerberus";

fn create_new_client_id(dir: &str, file_path: &str) -> Result<String> {
    // Create new client id as we do not have one saved.
    fs::create_dir_all(dir).chain_err(
        || "Error creating new client id.",
    )?;

    let client_id = Uuid::new_v4().to_string();
    let mut file = fs::File::create(file_path).chain_err(
        || "Error creating new client id.",
    )?;

    file.write_all(client_id.as_bytes()).chain_err(
        || "Error creating new client id.",
    )?;

    Ok(client_id)
}

pub fn get_client_id() -> Result<String> {
    let mut path_buf = env::home_dir().chain_err(|| "Error getting client id.")?;

    path_buf.push(CLIENT_ID_DIR);
    let dir = path_buf
        .to_str()
        .chain_err(|| "Error getting client id.")?
        .to_owned();

    path_buf.push(CLIENT_ID_FILE);
    let file_path = path_buf.to_str().chain_err(|| "Error getting client id.")?;

    if fs::metadata(file_path).is_ok() {
        let mut file = fs::File::open(file_path).chain_err(
            || "Error getting client id.",
        )?;

        let mut client_id = String::new();
        file.read_to_string(&mut client_id).chain_err(
            || "Error getting client id.",
        )?;

        return Ok(client_id);
    }

    create_new_client_id(&dir, file_path).chain_err(|| "Error getting client id.")
}
