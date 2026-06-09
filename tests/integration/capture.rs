//! One-shot capture verbs: output_string/bytes, run, stdin, timeouts, probe,
//! first_line, and the top-level free functions.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn missing_working_directory_errors_clearly() {
    // A cwd that doesn't exist must surface as a clear, not-found error that
    // names the working directory — not the opaque ENOENT that looks like the
    // program is missing. The check fires before any child is spawned.
    let err = Command::new("echo")
        .arg("hi")
        .current_dir("does-not-exist-processkit-xyz")
        .output_string()
        .await
        .expect_err("a missing cwd must error");
    assert!(err.is_not_found(), "got {err:?}");
    assert!(
        format!("{err}").contains("working directory does not exist"),
        "message should name the cwd: {err}"
    );
}

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn working_directory_that_is_a_file_errors_as_not_a_directory() {
    // A cwd that exists but is a regular file is reported distinctly — it is
    // *found*, just not a directory, so `is_not_found()` must be false. (Cargo.toml
    // exists at the package root, which is the test process's working directory.)
    let err = Command::new("echo")
        .arg("hi")
        .current_dir("Cargo.toml")
        .output_string()
        .await
        .expect_err("a file as cwd must error");
    assert!(!err.is_not_found(), "a file is found, not missing: {err:?}");
    assert!(
        format!("{err}").contains("is not a directory"),
        "message should say not-a-directory: {err}"
    );
}

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
#[ignore = "spawns a real subprocess fed stdin it never reads"]
async fn early_exiting_child_does_not_fail_a_large_stdin_feed() {
    // duct's gotcha #2 as a spec: the child exits without reading stdin while
    // the writer still has ~1 MiB to push — the resulting broken-pipe write
    // (EPIPE / Windows pipe error) must not fail the run or hang the feed.
    let big = "x".repeat(1024 * 1024);
    let exits_zero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };
    let result = exits_zero
        .stdin(processkit::Stdin::from_string(big))
        .output_string()
        .await
        .expect("the stdin writer's broken pipe must not surface as Err");
    assert!(result.is_success(), "result: {result:?}");
}

#[tokio::test]
#[ignore = "spawns a real stdin-reading subprocess on the bulk path"]
async fn untaken_keep_stdin_open_pipe_is_closed_by_bulk_verbs() {
    // Regression (audit-found hang): `keep_stdin_open` + a bulk verb used to
    // leave the stdin pipe open forever — a stdin-reading child (`sort`)
    // blocked to its timeout instead of seeing EOF. The consuming verb must
    // close an untaken pipe, so this returns promptly and cleanly.
    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    let start = std::time::Instant::now();
    let result = reads_stdin
        .keep_stdin_open()
        .timeout(std::time::Duration::from_secs(20)) // tripwire, must not be hit
        .output_string()
        .await
        .expect("run completes");
    assert!(result.is_success(), "result: {result:?}");
    assert!(
        !result.timed_out(),
        "the child must see EOF, not hang to the deadline: {result:?}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(15),
        "bulk verb did not close the untaken stdin pipe (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed a failing stdin source"]
async fn failing_stdin_source_does_not_fail_the_run() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source whose read fails immediately with a non-broken-pipe
    /// error — the stdin writer's failure must stay diagnostics-only (a
    /// `tracing` warn when enabled), never the run's result.
    struct FailingReader;
    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("stdin source failed")))
        }
    }

    // `sort` (Windows) / `cat` (Unix): both read stdin and exit 0 on EOF.
    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    let result = reads_stdin
        .stdin(processkit::Stdin::from_reader(FailingReader))
        .output_string()
        .await
        .expect("a failed stdin writer must not surface as Err");
    // The child saw immediate EOF (the sink is dropped on the writer's
    // error) and exited cleanly with empty output.
    assert!(result.is_success(), "result: {result:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess echoing 256 KiB through both pipes"]
async fn large_stdin_and_large_output_do_not_deadlock() {
    // duct's gotcha #10 as a spec: stdin is fed and both outputs are drained
    // concurrently, so a child echoing more than a pipe buffer (~64 KiB) in
    // each direction can neither stall writing (full stdout pipe nobody
    // drains) nor starve reading (stdin writer blocked behind us).
    let line = "0123456789abcdef".repeat(64); // 1 KiB
    let big = format!("{line}\n").repeat(256); // 256 KiB + newlines
    let echo_all = if cfg!(windows) {
        // findstr "^" passes every line through.
        Command::new("cmd").args(["/c", "findstr", "^^"])
    } else {
        Command::new("cat")
    };
    let result = echo_all
        .stdin(processkit::Stdin::from_string(big.clone()))
        .timeout(std::time::Duration::from_secs(60)) // deadlock tripwire
        .output_string()
        .await
        .expect("echo run");
    assert!(result.is_success(), "result: {result:?}");
    assert_eq!(
        result.stdout().lines().count(),
        256,
        "every line must round-trip"
    );
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
#[ignore = "spawns a real stdin-reading subprocess via first_line"]
async fn first_line_closes_an_untaken_keep_stdin_open_pipe() {
    // `first_line` gives no way to write stdin, so a `keep_stdin_open` filter
    // must still see EOF — otherwise `cat` blocks reading stdin forever and
    // the stream never yields. The streaming verb closes the untaken pipe.
    let filter = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    let start = Instant::now();
    // No timeout: a regression would hang indefinitely, caught by the outer
    // test harness — the assertion below documents the intent.
    let found = filter
        .keep_stdin_open()
        .first_line(|_| true)
        .await
        .expect("first_line completes");
    assert_eq!(
        found, None,
        "an empty stdin filter emits nothing: {found:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "first_line hung on an untaken stdin pipe (took {:?})",
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
