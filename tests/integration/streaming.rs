//! Line streaming and incremental output: stdout_lines, finish_streamed,
//! line handlers, bounded buffers, and interactive stdin.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use processkit::{Command, Outcome, OutputBufferPolicy, StreamedFinish};

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
    let mut lines = run.stdout_lines();
    let mut seen = Vec::new();
    while let Some(line) = lines.next().await {
        seen.push(line);
    }
    drop(lines);
    let StreamedFinish { outcome, .. } = run.finish_streamed().await.expect("finish");

    // Generous anti-hang bound (the sleeper runs ~30s if the deadline is
    // broken): under full-suite load cold spawns have been seen to push a
    // 500ms-timeout run past 5s.
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "stream did not end at the deadline (took {:?})",
        start.elapsed()
    );
    // Б1: a timed-out streamed run reports `Outcome::TimedOut` deterministically
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

#[tokio::test]
#[ignore = "spawns a real subprocess: stream stdout, then collect exit + stderr"]
async fn finish_streamed_returns_code_and_stderr() {
    use tokio_stream::StreamExt;

    // Emit one stdout line and one stderr line, exit 0, per platform.
    let cmd = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo out& echo err 1>&2"])
    } else {
        Command::new("sh").args(["-c", "echo out; echo err 1>&2"])
    };
    let mut process = cmd.start().await.expect("start");
    let mut lines = process.stdout_lines();
    let mut out = Vec::new();
    while let Some(line) = lines.next().await {
        out.push(line);
    }
    drop(lines);
    let StreamedFinish { outcome, stderr } = process.finish_streamed().await.expect("finish");
    assert_eq!(outcome, Outcome::Exited(0));
    assert!(out.iter().any(|l| l.contains("out")), "stdout: {out:?}");
    assert!(stderr.contains("err"), "stderr: {stderr:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn second_stdout_lines_call_ends_immediately() {
    use tokio_stream::StreamExt;

    let mut process = five_lines().start().await.expect("start");
    let mut first = process.stdout_lines();
    let mut seen = 0;
    while tokio::time::timeout(Duration::from_secs(10), first.next())
        .await
        .expect("first stream ends")
        .is_some()
    {
        seen += 1;
    }
    assert_eq!(seen, 5);

    // Documented: "Call this once." A second call must hand back an
    // immediately-finished stream, not hang or panic.
    let mut second = process.stdout_lines();
    let next = tokio::time::timeout(Duration::from_secs(5), second.next())
        .await
        .expect("the second stream must end immediately");
    assert!(next.is_none(), "second stream yields nothing: {next:?}");

    let _ = process.finish_streamed().await;
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn finish_streamed_without_streaming_first_drains_and_exits() {
    // Skipping stdout_lines() leaves both pipes untaken — finish_streamed must
    // drain them itself or a chatty child would block forever.
    let process = two_line_echo().start().await.expect("start");
    let finish = tokio::time::timeout(Duration::from_secs(15), process.finish_streamed())
        .await
        .expect("finish_streamed must not hang without a prior stdout_lines")
        .expect("finish");
    assert_eq!(finish.outcome, Outcome::Exited(0));
}
