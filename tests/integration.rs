//! Real-subprocess integration tests for `processkit`.
//!
//! These spawn actual child processes (and create OS jobs / cgroups), so they
//! are `#[ignore]`d to keep `cargo test` hermetic on CI. Run them locally with:
//!
//! ```text
//! cargo test -- --ignored
//! ```
//!
//! The no-orphan kernel guarantee can only be proven against a real process
//! tree, which is exactly what these cover.

use std::time::{Duration, Instant};

use processkit::{Command, Mechanism, ProcessGroup};

/// A command that prints two known lines and exits 0, per platform.
fn two_line_echo() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo first& echo second"])
    } else {
        Command::new("sh").args(["-c", "printf 'first\\nsecond\\n'"])
    }
}

/// A command that runs ~30s with no output, per platform.
fn sleeper() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping", "-n", "30", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("30")
    }
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_string_captures_stdout() {
    let result = two_line_echo().output_string().await.expect("run echo");
    assert!(result.is_success(), "exit was {}", result.exit_code());
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
#[ignore = "creates an OS job/cgroup"]
async fn group_reports_a_known_mechanism() {
    let group = ProcessGroup::new().expect("create group");
    assert!(matches!(
        group.mechanism(),
        Mechanism::JobObject | Mechanism::CgroupV2 | Mechanism::ProcessGroup | Mechanism::None
    ));
}

#[tokio::test]
#[ignore = "spawns a long-lived subprocess and asserts kill-on-drop"]
async fn dropping_group_kills_children() {
    // Kill-on-close only exists on the containment platforms; the `other` path
    // (macOS/BSD) has no job to kill, so the guarantee can't be asserted there.
    if cfg!(not(any(windows, target_os = "linux"))) {
        return;
    }

    // Start the sleeper into a *shared* group: the returned handle does not own
    // the group, so we can drop the group out from under it.
    let group = ProcessGroup::new().expect("create group");
    let process = group.start(&sleeper()).await.expect("spawn sleeper");
    let pid = process.pid();
    assert!(
        pid.is_some(),
        "sleeper should report a pid right after spawn"
    );

    drop(group); // kill-on-close should reap the child promptly

    // The kill releases the child's pipes and forces exit, so `wait` returns
    // far sooner than the sleeper's own ~30s runtime. A hang past the timeout
    // (or an elapsed time near 30s) would mean the child outlived its group.
    // The exit code of a job-killed process is platform-dependent (Windows can
    // report 0), so promptness — not the code — is the guarantee under test.
    let start = Instant::now();
    let _exit = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("child outlived its group — kill-on-close did not fire")
        .expect("wait completed");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child was not reaped promptly (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and streams its stdout"]
async fn stdout_lines_streams_incrementally() {
    use tokio_stream::StreamExt;

    let mut process = two_line_echo().start().await.expect("start echo");
    let mut lines = process.stdout_lines();
    let mut collected = Vec::new();
    while let Some(line) = lines.next().await {
        collected.push(line.expect("read line"));
    }
    assert!(
        collected.iter().any(|l| l.contains("first")),
        "lines: {collected:?}"
    );
    assert!(
        collected.iter().any(|l| l.contains("second")),
        "lines: {collected:?}"
    );
}
