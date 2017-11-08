/// This is a set of integration tests which run against a dummy payload binary living in
/// `libcerberus/src/bin/end-to-end.rs`.

use std::collections::HashSet;
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
    let json_input = r#"{"key":"foo","value":"bar zar"}"#;
    // The ordering of partitions is not guarenteed.
    let mut output_set = HashSet::new();
    let expected_output1 =
        r#"{"partitions":{"0":[{"key":"bar","value":"test"}],"1":[{"key":"zar","value":"test"}]}}"#;
    let expected_output2 =
        r#"{"partitions":{"1":[{"key":"zar","value":"test"}],"0":[{"key":"bar","value":"test"}]}}"#;
    output_set.insert(expected_output1.to_owned());
    output_set.insert(expected_output2.to_owned());

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

    println!("Output: {}", output_str.to_owned());

    assert!(output.status.success());
    assert!(output_set.contains(&output_str.to_owned()));
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

    assert!(output.status.success());
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
