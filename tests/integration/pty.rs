//! Real pseudo-terminal (`Command::use_pty`) integration tests.
//!
//! Like the rest of this binary they spawn actual child processes (here under a
//! real `openpty` / ConPTY pseudo-terminal) and are `#[ignore]`d to keep
//! `cargo test` hermetic; run them with:
//!
//! ```text
//! cargo test --all-features -- --ignored
//! ```
//!
//! The whole module is gated on the `pty` feature (via the `mod` declaration in
//! `main.rs`). These prove the four things a PTY run must get right: the child
//! sees a terminal (`isatty`), a prompt/response round-trips over the single
//! master, terminal echo is disabled for secret entry (Unix), and the PTY child
//! stays contained so kill-on-drop reaps it.

use std::time::Duration;

use processkit::{Command, JobRunner, ProcessRunner};

use crate::common::{completes_within, poll_until};

/// A child that prints `TTY` when its stdin is a terminal and `PIPE` otherwise.
fn isatty_probe() -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "if ([Console]::IsInputRedirected) { 'PIPE' } else { 'TTY' }",
        ])
    } else {
        Command::new("sh").args(["-c", "if [ -t 0 ]; then echo TTY; else echo PIPE; fi"])
    }
}

/// A child that reads one line and echoes `reply:<line>`.
fn prompt_responder() -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "$l = [Console]::In.ReadLine(); Write-Output \"reply:$l\"",
        ])
    } else {
        Command::new("sh").args(["-c", "read line; printf 'reply:%s\\n' \"$line\""])
    }
}

/// A long-running, output-free sleeper, per platform.
fn sleeper() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping", "-n", "30", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("30")
    }
}

/// Whether `pid` still names a live (or un-reaped) process.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // `kill(pid, 0)` succeeds (0) while the pid exists; `ESRCH` once it is gone
    // and reaped.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    crate::common::windows_pid_alive(pid)
}

#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_child_sees_a_tty() {
    // Under `use_pty` the child's stdin is a terminal, so an `isatty()`-gated tool
    // behaves as if run interactively.
    let out = completes_within(
        Duration::from_secs(20),
        "pty isatty run",
        JobRunner::new().output_string(&isatty_probe().use_pty()),
    )
    .await
    .expect("pty run");
    assert!(
        out.stdout().contains("TTY"),
        "an isatty child must see a terminal under PTY, got {:?}",
        out.stdout()
    );

    // The same child WITHOUT `use_pty` sees a plain (non-tty) pipe — proving the
    // difference is the PTY, not the tool.
    let plain = completes_within(
        Duration::from_secs(20),
        "pipe isatty run",
        JobRunner::new().output_string(&isatty_probe()),
    )
    .await
    .expect("pipe run");
    assert!(
        plain.stdout().contains("PIPE"),
        "without PTY the child sees a pipe, got {:?}",
        plain.stdout()
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_unused_stdin_does_not_close_the_conpty_session() {
    // Closing the ConPTY host-input pipe is a console-close request, not an EOF
    // for the child. Keep it alive while a child that never reads stdin runs.
    let child = Command::new("powershell").args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "Start-Sleep -Milliseconds 750; Write-Output READY",
    ]);
    let result = completes_within(
        Duration::from_secs(20),
        "ConPTY child with unused stdin",
        JobRunner::new().output_string(&child.use_pty()),
    )
    .await
    .expect("pty run");

    assert_eq!(
        result.code(),
        Some(0),
        "closing the public stdin writer must not close the ConPTY session: {result:?}"
    );
    assert!(
        result.stdout().contains("READY"),
        "the child must reach its command body: {:?}",
        result.stdout()
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_finish_sends_console_eof_without_closing_the_session() {
    let child = Command::new("powershell").args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "$count = 0; while ($null -ne [Console]::In.ReadLine()) { $count++ }; Write-Output \"lines:$count\"",
    ]);
    let mut process = JobRunner::new()
        .start(&child.use_pty().keep_stdin_open())
        .await
        .expect("start pty child");
    let mut stdin = process.take_stdin().expect("pty stdin writer");
    stdin.write_line("one").await.expect("write input");
    stdin.finish().await.expect("finish input");

    let result = completes_within(
        Duration::from_secs(20),
        "ConPTY console EOF",
        process.output_string(),
    )
    .await
    .expect("output");
    assert_eq!(
        result.code(),
        Some(0),
        "console EOF must be graceful: {result:?}"
    );
    assert!(
        result.stdout().contains("lines:1"),
        "the child must consume one line and then observe EOF: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_prompt_response_round_trips_over_the_master() {
    // Write a prompt to the master's input side and read the child's reply back
    // from the merged master output — the rexpect-style dialog the PTY mode exists
    // for.
    let mut proc = JobRunner::new()
        .start(&prompt_responder().use_pty().keep_stdin_open())
        .await
        .expect("start pty child");
    let mut stdin = proc.take_stdin().expect("pty stdin writer");
    // `write_line` maps Enter to CR for ConPTY (LF remains correct elsewhere),
    // so the same interactive API completes a cooked line read on both families.
    stdin.write_line("hello").await.expect("write the prompt");
    drop(stdin);
    let result = completes_within(
        Duration::from_secs(20),
        "pty prompt/response",
        proc.output_string(),
    )
    .await
    .expect("output");
    assert!(
        result.stdout().contains("reply:hello"),
        "the master must carry the child's reply, got {:?}",
        result.stdout()
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_disables_echo_so_a_written_secret_is_not_echoed() {
    // With terminal echo disabled, a secret written to stdin is consumed by the
    // child but never echoed back into the merged output — the passphrase-entry
    // guarantee. The child reads the secret and prints only a fixed marker.
    let child = Command::new("sh").args(["-c", "read secret; echo done"]);
    let mut proc = JobRunner::new()
        .start(&child.use_pty().keep_stdin_open())
        .await
        .expect("start pty child");
    let mut stdin = proc.take_stdin().expect("pty stdin writer");
    stdin
        .write_line("s3cr3t-passphrase")
        .await
        .expect("write the secret");
    drop(stdin);
    let result = completes_within(
        Duration::from_secs(20),
        "pty echo-off",
        proc.output_string(),
    )
    .await
    .expect("output");
    assert!(
        !result.stdout().contains("s3cr3t-passphrase"),
        "echo must be disabled — the secret must not appear in the merged output, got {:?}",
        result.stdout()
    );
    assert!(
        result.stdout().contains("done"),
        "the child still ran to completion, got {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_child_stays_contained_and_is_reaped_on_drop() {
    // The PTY child lives in the same job/cgroup/process group as any other run,
    // so an own-group handle tears its whole tree down on drop — the kill-on-drop
    // guarantee is unchanged by the PTY wiring.
    let proc = JobRunner::new()
        .start(&sleeper().use_pty())
        .await
        .expect("start pty sleeper");
    let pid = proc.pid().expect("a live pty child has a pid");
    assert!(
        proc.kills_tree_on_drop(),
        "an own-group PTY handle must tear its tree down on drop"
    );
    assert!(pid_alive(pid), "the pty child is alive before drop");
    drop(proc);
    // Dropping the handle drops its owned group, whose kill-on-close reaps the
    // PTY child (Job Object / cgroup / killpg) — poll until the pid is gone.
    poll_until(
        Duration::from_secs(15),
        Duration::from_millis(100),
        "the dropped PTY child's pid is reaped",
        || !pid_alive(pid),
    )
    .await;
}

#[tokio::test]
#[ignore = "spawns a real pseudo-terminal"]
async fn pty_events_stream_carries_the_full_lifecycle() {
    use processkit::{Outcome, ProcessEvent};
    use tokio_stream::StreamExt;

    // The lifecycle `events()` stream runs identically on the `Backend::Pty`
    // pump: `Started` leads, the child's output arrives as `Stdout` events over
    // the single merged master (so there are no separate `Stderr` events), and
    // the terminal `Exited` carries the run's outcome. Driven the documented way
    // (the stream and `finish` polled together).
    let child = if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Write-Output pty-1; Write-Output pty-2",
        ])
    } else {
        Command::new("sh").args(["-c", "echo pty-1; echo pty-2"])
    };

    let mut run = child.use_pty().start().await.expect("start pty events run");
    let pid = run.pid();
    let mut events = run.events().expect("events");
    let collect = async {
        let mut names = Vec::new();
        let mut started_pid = None;
        let mut lines = Vec::new();
        let mut exit_outcome = None;
        while let Some(ev) = events.next().await {
            names.push(ev.name());
            match ev {
                ProcessEvent::Started { pid } => started_pid = Some(pid),
                ProcessEvent::Stdout(l) => lines.push(l.text().to_owned()),
                ProcessEvent::Exited(o) => exit_outcome = Some(o),
                _ => {}
            }
        }
        (names, started_pid, lines, exit_outcome)
    };
    let outcome = completes_within(Duration::from_secs(20), "pty events lifecycle run", async {
        tokio::join!(collect, run.finish())
    })
    .await;
    let ((names, started_pid, lines, exit_outcome), finished) = outcome;
    let finished = finished.expect("finish");

    assert_eq!(names.first(), Some(&"started"), "Started leads the stream");
    assert_eq!(names.last(), Some(&"exited"), "Exited ends the stream");
    assert_eq!(
        started_pid,
        Some(pid),
        "the Started event mirrors the handle's pid"
    );
    assert!(
        !names.contains(&"stderr"),
        "a merged PTY stream produces no separate stderr events, got {names:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("pty-1")) && lines.iter().any(|l| l.contains("pty-2")),
        "both stdout lines arrive as events over the master, got {lines:?}"
    );
    assert_eq!(
        exit_outcome,
        Some(Outcome::Exited(0)),
        "the terminal Exited carries the run's outcome"
    );
    assert_eq!(finished.outcome, Outcome::Exited(0));
}
