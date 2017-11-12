use std::process::Command;

#[test]
fn integration_test() {
    let status = Command::new("bash")
        .arg("-c")
        .arg("tests/integration")
        .status();

    assert!(status.unwrap().success());
}
