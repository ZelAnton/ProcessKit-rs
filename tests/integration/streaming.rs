//! Line streaming and incremental output: stdout_lines, finish,
//! line handlers, bounded buffers, and interactive stdin.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use processkit::{Command, Finished, Outcome, OutputBufferPolicy};

use crate::common::*;

#[tokio::test]
#[ignore = "spawns a real subprocess that outlives its timeout"]
async fn streaming_honors_timeout() {
    use tokio_stream::StreamExt;

    // Emit one line, then idle well past the timeout. The deadline must end the
    // stream (kill the tree) rather than hang.
    let cmd = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo one& ping -n 30 127.0.0.1 >NUL"])
    } else {
        Command::new("sh").args(["-c", "echo one; sleep 30"])
    }
    .timeout(Duration::from_millis(500));

    let start = Instant::now();
    let mut run = cmd.start().await.expect("start");
    let mut lines = run.stdout_lines().unwrap();
    let mut seen = Vec::new();
    while let Some(line) = lines.next().await {
        seen.push(line);
    }
    drop(lines);
    let Finished { outcome, .. } = run.finish().await.expect("finish");

    // Generous anti-hang bound (the sleeper runs ~30s if the deadline is
    // broken): under full-suite load cold spawns have been seen to push a
    // 500ms-timeout run past 5s.
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "stream did not end at the deadline (took {:?})",
        start.elapsed()
    );
    // B1: a timed-out streamed run reports `Outcome::TimedOut` deterministically
    // on every platform — the watchdog sets the shared `timed_out` flag before
    // killing the tree, even on this no-grace path — so we assert it exactly
    // rather than just "not a clean success".
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "a timed-out streamed run must report TimedOut (got {outcome:?})"
    );
    assert!(seen.iter().any(|l| l.contains("one")), "saw: {seen:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess in a shared group that outlives its timeout"]
async fn shared_group_streaming_honors_timeout() {
    use processkit::ProcessGroup;
    use tokio_stream::StreamExt;

    // A1: `Command::timeout` must bound a stream on a SHARED-group handle too.
    // Before the fix the deadline watchdog armed only for own-group handles, so a
    // quiet never-exiting child left the stream pending forever. A single-process
    // idle keeps the shared-group pid-only kill sufficient (a forking tree is the
    // separate teardown gap).
    let group = ProcessGroup::new().expect("group");
    let cmd = if cfg!(windows) {
        // ping is a single process that emits lines then idles.
        Command::new("ping").args(["-n", "30", "127.0.0.1"])
    } else {
        // `exec` replaces the shell so the idle is one process (fork-free).
        Command::new("sh").args(["-c", "echo one; exec sleep 30"])
    }
    .timeout(Duration::from_millis(500));

    let start = Instant::now();
    let mut run = group.start(&cmd).await.expect("start");
    let mut lines = run.stdout_lines().unwrap();
    // Bound the drain so a broken deadline fails loud instead of hanging forever.
    let seen = tokio::time::timeout(Duration::from_secs(15), async {
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        seen
    })
    .await
    .expect("the deadline must end the stream, not hang");
    drop(lines);
    let Finished { outcome, .. } = run.finish().await.expect("finish");

    assert!(
        start.elapsed() < Duration::from_secs(15),
        "shared-group stream did not end at the deadline (took {:?})",
        start.elapsed()
    );
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "a timed-out shared-group streamed run must report TimedOut (got {outcome:?})"
    );
    assert!(
        !seen.is_empty(),
        "the stream should have seen some output first"
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
async fn stdout_line_handler_panic_is_isolated_on_a_real_subprocess() {
    // F: the real-subprocess analogue of `pump::panicking_handler_is_isolated_
    // and_capture_completes` (unit-covered only until now) — a handler that
    // panics on the second line must not fail the run, hang the pump, or lose
    // any already-produced output. Called `#[ignore]`'d suite-wide; this test
    // proves the contract holds against the real pump wiring, not a hand-fed
    // in-memory byte stream.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let result = five_lines()
        .on_stdout_line(move |_line| {
            if counted.fetch_add(1, Ordering::SeqCst) == 1 {
                panic!("boom on the second line");
            }
        })
        .output_string()
        .await
        .expect("a panicking handler must not fail the run");
    assert!(result.is_success(), "result: {result:?}");
    assert_eq!(
        result.stdout().lines().count(),
        5,
        "every line is still captured despite the panic: {:?}",
        result.stdout()
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the handler is disabled after its panic (called for lines 1 and 2 only)"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed stdin from a file"]
async fn stdin_from_file_round_trips_end_to_end() {
    // F: `Stdin::from_file` end-to-end — write a real file, feed it as stdin,
    // and confirm the child sees the exact bytes.
    let path = std::env::temp_dir().join(format!(
        "processkit_t052_stdin_from_file_{}.txt",
        std::process::id()
    ));
    std::fs::write(&path, "banana\napple\n").expect("write fixture file");

    let program = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("sort")
    };
    let result = program
        .stdin(processkit::Stdin::from_file(&path))
        .output_string()
        .await
        .expect("run sort fed from a file");
    let _ = std::fs::remove_file(&path);

    assert!(result.is_success(), "result: {result:?}");
    let first = result
        .stdout()
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    assert_eq!(first, "apple", "sorted output: {:?}", result.stdout());
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and writes to its stdin after it exits"]
async fn write_after_child_exit_reports_the_windows_pipe_error_code() {
    // F: the Windows broken-pipe error-code gap — a write to a pipe whose
    // reader is gone commonly surfaces here as raw OS error 109
    // (`ERROR_BROKEN_PIPE`) or 232 (`ERROR_NO_DATA`), which don't always map
    // to `ErrorKind::BrokenPipe` (see `is_broken_pipe` in
    // `src/running/mod.rs`). The POSIX EPIPE-equivalent forgiveness is already
    // covered cross-platform by
    // `capture::early_exiting_child_does_not_fail_a_large_stdin_feed`; this
    // pins the raw Windows code on the interactive writer, which hands the
    // caller a bare `io::Error` instead of forgiving it internally.
    let exits_zero = Command::new("cmd").args(["/c", "exit", "0"]);
    let mut process = exits_zero.keep_stdin_open().start().await.expect("start");
    let mut stdin = process.take_stdin().expect("stdin kept open");

    let outcome = completes_within(Duration::from_secs(10), "child exit", process.wait())
        .await
        .expect("wait");
    assert_eq!(outcome, Outcome::Exited(0));

    let err = stdin
        .write_line("data")
        .await
        .expect_err("a write after the child exited must fail");
    assert!(
        err.kind() == std::io::ErrorKind::BrokenPipe
            || matches!(err.raw_os_error(), Some(109 | 232)),
        "expected a broken-pipe-shaped error, got kind={:?} raw={:?}",
        err.kind(),
        err.raw_os_error()
    );
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
    let mut stdin = process.take_stdin().expect("stdin kept open");
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
#[ignore = "spawns a real subprocess and streams its stdout"]
async fn stdout_lines_streams_incrementally() {
    use tokio_stream::StreamExt;

    let mut process = two_line_echo().start().await.expect("start echo");
    let mut lines = process.stdout_lines().unwrap();
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

#[tokio::test]
#[ignore = "spawns a real subprocess: stream stdout, then collect exit + stderr"]
async fn finish_returns_code_and_stderr() {
    use tokio_stream::StreamExt;

    // Emit one stdout line and one stderr line, exit 0, per platform.
    let cmd = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo out& echo err 1>&2"])
    } else {
        Command::new("sh").args(["-c", "echo out; echo err 1>&2"])
    };
    let mut process = cmd.start().await.expect("start");
    let mut lines = process.stdout_lines().unwrap();
    let mut out = Vec::new();
    while let Some(line) = lines.next().await {
        out.push(line);
    }
    drop(lines);
    let Finished {
        outcome, stderr, ..
    } = process.finish().await.expect("finish");
    assert_eq!(outcome, Outcome::Exited(0));
    assert!(out.iter().any(|l| l.contains("out")), "stdout: {out:?}");
    assert!(stderr.contains("err"), "stderr: {stderr:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn second_stdout_lines_call_is_a_loud_error() {
    use tokio_stream::StreamExt;

    let mut process = five_lines().start().await.expect("start");
    let mut first = process.stdout_lines().expect("first stdout_lines");
    let mut seen = 0;
    while tokio::time::timeout(Duration::from_secs(10), first.next())
        .await
        .expect("first stream ends")
        .is_some()
    {
        seen += 1;
    }
    assert_eq!(seen, 5);

    // D2: "Call this once." A second call is a LOUD error (stdout streams once),
    // not a silently-empty stream.
    let err = process
        .stdout_lines()
        .expect_err("a second stdout_lines must be a loud error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Io(_)),
        "expected ErrorReason::Io, got {err:?}"
    );

    let _ = process.finish().await;
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn finish_without_streaming_first_drains_and_exits() {
    // Skipping stdout_lines() leaves both pipes untaken — finish must
    // drain them itself or a chatty child would block forever.
    let process = two_line_echo().start().await.expect("start");
    let finish = tokio::time::timeout(Duration::from_secs(15), process.finish())
        .await
        .expect("finish must not hang without a prior stdout_lines")
        .expect("finish");
    assert_eq!(finish.outcome, Outcome::Exited(0));
}
