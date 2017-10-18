use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const TEST_BIN_NAME: &str = "end-to-end";

// This can't be a one-liner because cargo sometimes runs integration tests from
// `target/debug/deps`.
fn get_bin_path() -> PathBuf {
    let mut path = env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(TEST_BIN_NAME);
    path
}

#[test]
fn run_sanity_check() {
    let output = Command::new(get_bin_path())
        .arg("sanity-check")
        .output()
        .unwrap();
    let output_str = String::from_utf8(output.stdout).unwrap();

    assert_eq!("sanity located\n", output_str);
}

#[test]
fn run_map_valid_input() {
    let json_input = r#"{"key":"foo","value":"bar"}"#;
    let expected_output = r#"{"pairs":[{"key":"bar","value":"test"}]}"#;

    let mut child = Command::new(get_bin_path())
        .arg("map")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(json_input.as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();
    let output_str = String::from_utf8(output.stdout).unwrap();

    assert_eq!(true, output.status.success());
    assert_eq!(expected_output, output_str);
}

#[test]
fn run_map_invalid_input() {
    let bad_input = r#"foo"#;

    let mut child = Command::new(get_bin_path())
        .arg("map")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(bad_input.as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();

    assert_eq!(false, output.status.success());
}

#[test]
fn run_reduce_valid_input() {
    let json_input = r#"{"key":"foo","values":["bar","baz"]}"#;
    let expected_output = r#"{"values":["barbaz"]}"#;

    let mut child = Command::new(get_bin_path())
        .arg("reduce")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(json_input.as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();
    let output_str = String::from_utf8(output.stdout).unwrap();

    assert_eq!(true, output.status.success());
    assert_eq!(expected_output, output_str);
}

#[test]
fn run_reduce_invalid_input() {
    let json_input = r#"foo"#;

    let mut child = Command::new(get_bin_path())
        .arg("reduce")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(json_input.as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();

    assert_eq!(false, output.status.success());
}
