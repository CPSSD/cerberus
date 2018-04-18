/// This is a set of tests which is ran against the CLI. It spawns a test server to which the CLI
/// connects to.
use std::env;
use std::path::PathBuf;
use std::process::Command;

const CLI_BIN_NAME: &str = "cli";

fn get_bin_path() -> PathBuf {
    let mut path = env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(CLI_BIN_NAME);
    path
}

#[test]
fn empty() {
    let output = Command::new(get_bin_path()).output().unwrap();
    let output_str = String::from_utf8(output.stderr).unwrap();

    let expected = r#"error: The following required arguments were not provided:
    --master <master>

USAGE:
    cli --master <master> [SUBCOMMAND]

For more information try --help
"#;

    assert_eq!(output_str, expected);
}

#[test]
fn bad_master() {
    let output = Command::new(get_bin_path())
        .arg("--master=abc")
        .output()
        .unwrap();
    let output_str = String::from_utf8(output.stderr).unwrap();

    assert_eq!(
        output_str,
        "Error: Error parsing master address\nCaused by: invalid IP address syntax\n"
    );
}

#[test]
fn missing_subcommand() {
    let output = Command::new(get_bin_path())
        .arg("--master=127.0.0.1:3456")
        .output()
        .unwrap();
    let output_str = String::from_utf8(output.stderr).unwrap();

    assert_eq!(
        output_str,
        "Error: USAGE:\n    cli --master <master> [SUBCOMMAND]\n"
    );
}
