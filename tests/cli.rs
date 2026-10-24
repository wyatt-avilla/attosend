use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_attosend"))
        .args(args)
        .output()
        .expect("attosend should start")
}

#[test]
fn hello_world() {
    let output = run(&[]);
    assert!(output.status.success());
    assert_eq!(output.stdout, b"Hello, world!\n");
    assert!(output.stderr.is_empty());
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
        assert!(output.stderr.is_empty());
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
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn unexpected_arguments() {
    for arg in ["--unknown", "unexpected"] {
        let output = run(&[arg]);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr).unwrap().contains("error:"));
    }
}
