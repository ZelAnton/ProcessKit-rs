//! One-shot capture verbs: output_string/bytes, run, stdin, timeouts, probe,
//! first_line, and the top-level free functions.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_string_captures_stdout() {
    let result = two_line_echo().output_string().await.expect("run echo");
    assert!(result.is_success(), "exit was {:?}", result.code());
    assert!(
        result.stdout().contains("first"),
        "stdout: {:?}",
        result.stdout()
    );
    assert!(
        result.stdout().contains("second"),
        "stdout: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn run_trims_and_requires_success() {
    // `cargo --version` is reliably present in this workspace.
    let out = Command::new("cargo")
        .arg("--version")
        .run()
        .await
        .expect("cargo --version");
    assert!(out.to_lowercase().contains("cargo"), "unexpected: {out}");
    // `run` trims trailing newlines.
    assert_eq!(out, out.trim_end());
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_bytes_returns_raw_stdout() {
    let result = two_line_echo().output_bytes().await.expect("run echo");
    assert!(result.is_success());
    let text = String::from_utf8_lossy(result.stdout());
    assert!(text.contains("first") && text.contains("second"));
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdin_is_fed_to_the_child() {
    // `cat` (Unix) / `findstr` echo of stdin (Windows `sort` reads stdin).
    let result = if cfg!(windows) {
        Command::new("cmd")
            .args(["/c", "sort"])
            .stdin(processkit::Stdin::from_string("delta\nalpha\n"))
            .output_string()
            .await
            .expect("run sort")
    } else {
        Command::new("cat")
            .stdin(processkit::Stdin::from_string("hello stdin\n"))
            .output_string()
            .await
            .expect("run cat")
    };
    assert!(result.is_success());
    let expected = if cfg!(windows) {
        "alpha"
    } else {
        "hello stdin"
    };
    assert!(
        result.stdout().contains(expected),
        "stdout: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for the timeout"]
async fn timeout_kills_and_flags() {
    let result = sleeper()
        .timeout(Duration::from_millis(300))
        .output_string()
        .await
        .expect("timed run still returns a result");
    assert!(result.timed_out(), "should be flagged as timed out");
    assert!(!result.is_success());
}

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for the timeout"]
async fn exit_code_surfaces_timeout_as_error() {
    // `Command::exit_code` must report a timeout as `Error::Timeout`, not the
    // synthetic `-1` — consistent with the runner/CliClient code paths.
    let err = sleeper()
        .timeout(Duration::from_millis(300))
        .exit_code()
        .await
        .expect_err("a timed-out run has no meaningful exit code");
    assert!(
        matches!(err, processkit::Error::Timeout { .. }),
        "expected Error::Timeout, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess that stalls; must not hang past the timeout"]
async fn first_line_honors_timeout_instead_of_hanging() {
    // A long-running command that emits NO stdout: without a timeout `first_line`
    // would block forever waiting for a line. With a deadline it must give up and
    // surface `Error::Timeout` promptly — never hang.
    let silent = if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
    } else {
        Command::new("sleep").arg("30")
    };
    let start = Instant::now();
    let err = silent
        .timeout(Duration::from_millis(300))
        .first_line(|_| true)
        .await
        .expect_err("a stalled run should time out, not return Ok(None)");
    assert!(
        matches!(err, processkit::Error::Timeout { .. }),
        "expected Error::Timeout, got {err:?}"
    );
    // Generous anti-hang bound (the sleeper runs ~30s if the timeout is
    // broken): under full-suite load PowerShell's cold start alone has been
    // seen to push a 300ms-timeout run past 5s.
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "first_line did not honor the timeout (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses"]
async fn probe_reads_real_exit_codes() {
    // Exit 0 -> Ok(true), exit 1 -> Ok(false), exit 2 -> Err.
    let exits = |code: i32| {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "exit", &code.to_string()])
        } else {
            Command::new("sh").args(["-c", &format!("exit {code}")])
        }
    };
    assert!(exits(0).probe().await.expect("exit 0 is a clean true"));
    assert!(!exits(1).probe().await.expect("exit 1 is a clean false"));
    assert!(
        exits(2).probe().await.is_err(),
        "any code other than 0/1 must be an error, not a silent bool"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess via the top-level free functions"]
async fn top_level_run_and_output() {
    let v = processkit::run("cargo", ["--version"])
        .await
        .expect("run cargo --version");
    assert!(v.to_lowercase().contains("cargo"), "unexpected: {v}");

    let result = processkit::output("cargo", ["--version"])
        .await
        .expect("output cargo --version");
    assert!(result.is_success());
    assert!(result.stdout().to_lowercase().contains("cargo"));
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn first_line_returns_none_when_the_stream_ends_without_a_match() {
    // stdout closing without a matching line is Ok(None) — not a hang and not
    // an error (the timeout path is covered separately).
    let found = tokio::time::timeout(
        Duration::from_secs(15),
        two_line_echo().first_line(|l| l.contains("never-printed")),
    )
    .await
    .expect("first_line must end when stdout closes, not hang")
    .expect("run succeeds");
    assert_eq!(found, None);
}
