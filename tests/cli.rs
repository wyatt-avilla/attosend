use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_attosend"))
        .args(args)
        .output()
        .expect("attosend should start")
}

#[test]
fn subcommand_required() {
    let output = run(&[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr).unwrap().contains("Usage:"));
}

#[test]
fn help() {
    for flag in ["-h", "--help"] {
        let output = run(&[flag]);
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("Usage: attosend"));
        assert!(stdout.contains("--help"));
        assert!(stdout.contains("--version"));
        assert!(!stdout.contains("Hello, world!"));
        assert_eq!(output.stderr, [] as [u8; 0]);
    }
}

#[test]
fn version() {
    for flag in ["-V", "--version"] {
        let output = run(&[flag]);
        assert!(output.status.success());
        assert_eq!(
            output.stdout,
            format!("attosend {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
        );
        assert_eq!(output.stderr, [] as [u8; 0]);
    }
}

#[test]
fn unexpected_arguments() {
    for arg in ["--unknown", "unexpected"] {
        let output = run(&[arg]);
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stdout, [] as [u8; 0]);
        assert!(String::from_utf8(output.stderr).unwrap().contains("error:"));
    }
}

#[test]
fn retry_requires_a_message_id_before_reading_configuration() {
    let output = run(&[
        "--config",
        "/nonexistent/attosend-config.json",
        "retry",
        "invalid",
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("invalid message ID")
    );
}
