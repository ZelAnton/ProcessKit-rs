//! One-shot capture verbs: output_string/bytes, run, stdin, timeouts, probe,
//! first_line, and the top-level free functions.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn missing_working_directory_errors_clearly() {
    // A cwd that doesn't exist must surface as a clear error that names the
    // working directory — not the opaque ENOENT that looks like the program is
    // missing. The check fires before any child is spawned. D11: a bad cwd is
    // NOT a missing program, so `is_not_found()` must be false — the
    // "not installed?" hint would mislead.
    let err = Command::new("echo")
        .arg("hi")
        .current_dir("does-not-exist-processkit-xyz")
        .output_string()
        .await
        .expect_err("a missing cwd must error");
    assert!(
        !err.is_not_found(),
        "a missing cwd is not a missing program: {err:?}"
    );
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
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn missing_program_surfaces_not_found_with_searched_path() {
    // A bare program name that isn't on PATH must produce Error::NotFound
    // with a message that names the searched directories — not the opaque
    // OS ENOENT that would otherwise be indistinguishable from a missing cwd.
    let err = Command::new("processkit-definitely-not-installed-424242")
        .output_string()
        .await
        .expect_err("an unknown program must error");
    assert!(
        matches!(err, processkit::Error::NotFound { .. }),
        "expected Error::NotFound, got {err:?}"
    );
    assert!(err.is_not_found(), "is_not_found() must be true: {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("not found on PATH"),
        "message should name the PATH search: {msg}"
    );
}

// Regression: a bare program name that the OS resolves by a route we don't model
// must NOT be falsely rejected as `NotFound`. On Windows std also searches the
// *application directory* (the running exe's dir), not just PATH — a helper
// shipped beside the binary spawns fine. An earlier PATH-only pre-check rejected
// it before ever attempting the spawn; the rich error is now enriched from the
// OS's actual not-found, so the OS stays the source of truth. (Unix `execvp`
// searches PATH only, so there is no equivalent app-dir case to regress there.)
#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the real spawn path; writes a temp exe beside the test binary"]
async fn bare_name_in_the_application_directory_is_not_falsely_not_found() {
    // Place a uniquely-named copy of a real system exe next to THIS test binary
    // (the application directory) — deliberately *not* on PATH and *not* in cwd.
    let app_dir = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("exe has a parent dir")
        .to_path_buf();
    let unique = "pk_appdir_regression_probe_77321";
    let dst = app_dir.join(format!("{unique}.exe"));
    std::fs::copy(r"C:\Windows\System32\where.exe", &dst).expect("copy where.exe beside test exe");

    // `where /?` prints help and exits 0 — so a clean success proves the OS both
    // *found* (via the app dir) and *ran* the bare-named program.
    let result = Command::new(unique).arg("/?").output_string().await;
    let _ = std::fs::remove_file(&dst); // best-effort cleanup before asserting

    let result = result.expect("a program in the application dir must spawn, not error");
    assert!(
        result.is_success(),
        "`where /?` exits 0; got {:?}",
        result.code()
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

// StdioMode::Null on stdout: D5 makes the bulk capture verbs ERROR (there is no
// pipe to read, so returning silently-empty output was a footgun). To run a
// command with a suppressed stdout, use a discard verb (`wait`), which captures
// nothing by design.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_null_makes_capture_verbs_error_but_discard_verbs_run() {
    // D5: a capture verb on a non-piped stdout errors loudly.
    let err = two_line_echo()
        .stdout(processkit::StdioMode::Null)
        .output_string()
        .await
        .expect_err("output_string on a non-piped stdout must error (D5)");
    match err {
        processkit::Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    // A discard verb still runs the command to completion (nothing to capture is
    // fine there) — the exit code is real.
    let outcome = two_line_echo()
        .stdout(processkit::StdioMode::Null)
        .start()
        .await
        .expect("start")
        .wait()
        .await
        .expect("wait() runs a stdout(Null) command fine");
    assert_eq!(
        outcome,
        processkit::Outcome::Exited(0),
        "the command still ran"
    );
}

// stdout_tee: each decoded line is written to the sink *and* still captured.
// A shared in-memory sink lets the test read back what the tee received without
// depending on file-handle flush/close timing.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_tee_writes_to_the_sink_while_capturing() {
    // An in-memory `tokio::io::AsyncWrite` sink shared with the test, so it can
    // read back what the tee received without depending on file flush/close.
    #[derive(Clone)]
    struct SharedSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl tokio::io::AsyncWrite for SharedSink {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().expect("sink mutex").extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let result = two_line_echo()
        .stdout_tee(sink.clone())
        .output_string()
        .await
        .expect("a stdout_tee run completes");

    // Capture still happens alongside the tee.
    assert!(
        result.stdout().contains("first") && result.stdout().contains("second"),
        "capture must still see both lines: {:?}",
        result.stdout()
    );
    // The tee received the same lines (each decoded line + '\n').
    let teed = String::from_utf8(sink.0.lock().expect("sink mutex").clone()).expect("tee is utf-8");
    assert!(
        teed.contains("first") && teed.contains("second"),
        "tee sink must receive both lines: {teed:?}"
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
async fn failing_stdin_source_surfaces_as_error_stdin_on_a_successful_run() {
    use processkit::Error;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source whose read fails immediately with a non-broken-pipe error.
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
    // B3 (Decision 2): the child sees EOF (the sink is dropped on the writer's
    // error) and exits 0 — a success — so the stashed non-broken-pipe stdin
    // failure now surfaces as `Error::Stdin` instead of being swallowed.
    let err = reads_stdin
        .stdin(processkit::Stdin::from_reader(FailingReader))
        .output_string()
        .await
        .expect_err("a failed stdin writer on a successful run must surface as Error::Stdin");
    assert!(matches!(err, Error::Stdin { .. }), "got: {err:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed a panicking stdin source"]
async fn panicking_stdin_source_surfaces_as_error_stdin_not_silent_success() {
    use processkit::Error;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source whose read PANICS — a bug in a user-supplied reader.
    struct PanickingReader;
    impl tokio::io::AsyncRead for PanickingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            panic!("stdin source panicked");
        }
    }

    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    // L1: the writer task panics; its `JoinError` must be surfaced as
    // `Error::Stdin` on an otherwise-successful run, not swallowed into a clean
    // success (a panicking source is a real failure the caller must see).
    let err = reads_stdin
        .stdin(processkit::Stdin::from_reader(PanickingReader))
        .output_string()
        .await
        .expect_err("a panicking stdin writer on a successful run must surface as Error::Stdin");
    assert!(matches!(err, Error::Stdin { .. }), "got: {err:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess that exits non-zero while its stdin source fails"]
async fn nonzero_exit_wins_over_a_failing_stdin_source() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Yields one line, then fails (non-broken-pipe) — the child consumes the
    /// line and still exits non-zero.
    struct OneLineThenFail(bool);
    impl tokio::io::AsyncRead for OneLineThenFail {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.0 {
                Poll::Ready(Err(std::io::Error::other("stdin source failed")))
            } else {
                self.0 = true;
                buf.put_slice(b"x\n");
                Poll::Ready(Ok(()))
            }
        }
    }

    // grep/findstr of an absent pattern reads stdin, then exits 1 (no match).
    let nonzero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "findstr", "zzz-no-match"])
    } else {
        Command::new("grep").arg("zzz-no-match")
    };
    // B3 (Decision 2): exit 1 is outside `ok_codes`, so the run is NOT a success
    // — the stdin failure is dropped, not surfaced. `output_string` returns the
    // result carrying the real exit code rather than `Err(Error::Stdin)`.
    let result = nonzero
        .stdin(processkit::Stdin::from_reader(OneLineThenFail(false)))
        .output_string()
        .await
        .expect("a non-zero exit is a result on output_string, not Err(Stdin)");
    assert_eq!(
        result.code(),
        Some(1),
        "the real exit code is preserved: {result:?}"
    );
    assert!(!result.is_success());
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
#[ignore = "spawns real subprocesses; ok_codes through the real verbs"]
async fn ok_codes_widens_success_through_output_string_and_bytes() {
    // A non-zero exit the caller declares OK must flow through the REAL spawn
    // path to is_success() — for BOTH output_string and output_bytes (the bytes
    // verb carries ok_codes too, and had no test at any layer).
    let s = failing_exit(1)
        .ok_codes([0, 1])
        .output_string()
        .await
        .expect("run completes");
    assert!(
        s.is_success(),
        "exit 1 is success under ok_codes([0,1]) (output_string)"
    );
    assert_eq!(s.code(), Some(1), "the raw code is still reported");

    let b = failing_exit(1)
        .ok_codes([0, 1])
        .output_bytes()
        .await
        .expect("run completes");
    assert!(
        b.is_success(),
        "exit 1 is success under ok_codes([0,1]) (output_bytes)"
    );
    assert_eq!(b.code(), Some(1));

    // The widening is bounded: a code outside the set is still a failure.
    let outside = failing_exit(2)
        .ok_codes([0, 1])
        .output_string()
        .await
        .expect("run completes");
    assert!(
        !outside.is_success(),
        "exit 2 is outside ok_codes([0,1]) — still a failure"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess that overflows a fail-loud buffer"]
async fn fail_loud_buffer_surfaces_output_too_large() {
    // A child that prints more lines than a fail-loud ceiling must surface
    // Error::OutputTooLarge through the real spawn+pump path — the fail-loud
    // DoS guard, end-to-end. `five_lines` prints 5 lines; the cap is 2, so the
    // run errors even though the child exited 0. (The pipe is still drained, so
    // the child never blocks.)
    use processkit::OutputBufferPolicy;
    let err = five_lines()
        .output_buffer(OutputBufferPolicy::fail_loud(2))
        .output_string()
        .await
        .expect_err("5 lines over a 2-line fail-loud cap must error");
    match err {
        processkit::Error::OutputTooLarge {
            line_limit,
            total_lines,
            ..
        } => {
            assert_eq!(
                line_limit,
                Some(2),
                "the configured line cap is reported: {err:?}"
            );
            assert!(total_lines >= 5, "every line is counted: {total_lines}");
        }
        other => panic!("expected Error::OutputTooLarge, got {other:?}"),
    }

    // A run that stays under the cap must NOT error (control case).
    let ok = two_line_echo()
        .output_buffer(OutputBufferPolicy::fail_loud(10))
        .output_string()
        .await
        .expect("2 lines under a 10-line cap is fine");
    assert!(ok.is_success());
}

#[tokio::test]
#[ignore = "spawns a real subprocess whose output a bounded drop-policy truncates"]
async fn checking_verbs_reject_truncated_output_e2e() {
    // B12: a bounded *drop* policy silently discards lines. The lenient capture
    // verb (`output_string`) returns the partial result with `truncated()` set;
    // the checking verbs that hand back stdout as if complete (`run`/`parse`)
    // must instead fail loud with `OutputTooLarge` rather than feed a caller a
    // truncated tail. `five_lines` prints 5 lines; the cap keeps 2.
    use processkit::OutputBufferPolicy;

    // Lenient: output_string keeps the (partial) result and flags truncation.
    let lenient = five_lines()
        .output_buffer(OutputBufferPolicy::bounded(2))
        .output_string()
        .await
        .expect("output_string stays lenient under a bounded drop policy");
    assert!(lenient.is_success());
    assert!(lenient.truncated(), "the bounded policy dropped lines");

    // Strict: run() refuses the silently-truncated stdout.
    let err = five_lines()
        .output_buffer(OutputBufferPolicy::bounded(2))
        .run()
        .await
        .expect_err("run must reject truncated stdout (B12)");
    assert!(
        matches!(
            err,
            processkit::Error::OutputTooLarge {
                line_limit: Some(2),
                ..
            }
        ),
        "expected OutputTooLarge with the configured cap, got {err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "Windows has no signal tier: timeout_grace must degrade to a prompt atomic kill"]
async fn graceful_timeout_degrades_to_a_prompt_kill_on_windows() {
    // Windows has no SIGTERM tier, so timeout_grace/timeout_signal degrade to the
    // atomic Job kill at the deadline — it must NOT wait out a phantom grace.
    let start = Instant::now();
    let result = sleeper() // ~30s child
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(30))
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out(), "the deadline fired");
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "Windows must hard-kill promptly at the deadline, not wait the 30s grace (took {:?})",
        start.elapsed()
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
