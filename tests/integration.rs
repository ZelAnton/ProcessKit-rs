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

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use processkit::{Command, Mechanism, OutputBufferPolicy, ProcessGroup};

/// A command that prints five numbered lines and exits 0, per platform.
fn five_lines() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo 1& echo 2& echo 3& echo 4& echo 5"])
    } else {
        Command::new("sh").args(["-c", "printf '1\\n2\\n3\\n4\\n5\\n'"])
    }
}

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
#[ignore = "spawns a real subprocess"]
async fn stdout_line_handler_sees_every_line() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = seen.clone();
    let result = five_lines()
        .on_stdout_line(move |line| captured.lock().unwrap().push(line.to_owned()))
        .output_string()
        .await
        .expect("run");
    assert!(result.is_success());
    let lines = seen.lock().unwrap();
    assert_eq!(lines.len(), 5, "handler saw: {lines:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_buffer_drops_oldest_lines() {
    // Keep only the last two lines; the rest are dropped from the buffer.
    let result = five_lines()
        .output_buffer(OutputBufferPolicy::bounded(2))
        .output_string()
        .await
        .expect("run");
    let kept: Vec<&str> = result.stdout().lines().collect();
    assert_eq!(kept.len(), 2, "retained: {:?}", result.stdout());
    assert!(kept.iter().all(|l| l.trim() == "4" || l.trim() == "5"));
}

#[tokio::test]
#[ignore = "spawns a real subprocess driven via interactive stdin"]
async fn interactive_stdin_round_trips() {
    // `sort` reads stdin until EOF, then writes the sorted lines.
    let program = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("sort")
    };
    let mut process = program.keep_stdin_open().start().await.expect("start sort");
    let mut stdin = process.standard_input().expect("stdin kept open");
    stdin.write_line("banana").await.expect("write");
    stdin.write_line("apple").await.expect("write");
    stdin.finish().await.expect("eof");

    let result = process.output_string().await.expect("collect");
    assert!(result.is_success());
    let first = result
        .stdout()
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    assert_eq!(first, "apple", "sorted output: {:?}", result.stdout());
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup and reads accounting"]
async fn group_stats_report_active_processes() {
    let group = ProcessGroup::new().expect("create group");
    let _process = group.start(&sleeper()).await.expect("spawn sleeper");
    let stats = group.stats().expect("stats");
    assert!(
        stats.active_process_count >= 1,
        "expected a live process, got {stats:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and reads per-process metrics"]
async fn process_diagnostics_are_available() {
    // On the containment platforms CPU/memory are reported; elsewhere they may be
    // None, so only assert the pid/elapsed basics universally.
    let mut process = sleeper().start().await.expect("start sleeper");
    assert!(process.pid().is_some());
    assert!(process.elapsed() < Duration::from_secs(5));
    if cfg!(any(windows, target_os = "linux")) {
        // Give the child a moment to accrue something measurable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            process.peak_memory_bytes().is_some(),
            "peak memory should be readable on this platform"
        );
    }
    let _ = process.standard_input(); // no-op (stdin not kept open)
    drop(process);
}

#[tokio::test]
#[ignore = "spawns a real subprocess and streams its stdout"]
async fn stdout_lines_streams_incrementally() {
    use tokio_stream::StreamExt;

    let mut process = two_line_echo().start().await.expect("start echo");
    let mut lines = process.stdout_lines();
    let mut collected: Vec<String> = Vec::new();
    while let Some(line) = lines.next().await {
        collected.push(line);
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
