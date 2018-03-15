extern crate fs_extra;

use std::env;
use std::fs;
use std::path::PathBuf;

use fs_extra::dir;

fn main() {
    let build_out_dir = env::var("OUT_DIR").unwrap();
    let mut out_path = PathBuf::from(build_out_dir);
    out_path.pop();
    out_path.pop();
    out_path.pop();

    let mut previous_dir = out_path.clone();
    previous_dir.push("content");

    if previous_dir.exists() {
        fs::remove_dir_all(previous_dir).unwrap();
    }

    fs::create_dir_all(out_path.clone()).unwrap();

    let mut options = dir::CopyOptions::new();
    options.overwrite = true;

    let source_dir = PathBuf::from("content");

    dir::copy(source_dir, out_path, &options).unwrap();
}
