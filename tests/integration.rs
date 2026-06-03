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
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "first_line did not honor the timeout (took {:?})",
        start.elapsed()
    );
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
    // Kill-on-close exists on Windows (Job Object), Linux (cgroup/process group)
    // and other unix (macOS/BSD process group). Only targets with no containment
    // at all (non-unix, non-Windows — `Mechanism::None`) can't assert it.
    if cfg!(not(any(windows, unix))) {
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

/// Whether a process with `pid` is still alive (Windows): `OpenProcess` with
/// limited-query access succeeds while it lives; once reaped the pid is invalid.
#[cfg(windows)]
fn windows_pid_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
    // SAFETY: limited-information access; returns null when the pid is gone.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return false;
    }
    // SAFETY: handle came from OpenProcess; closed exactly once.
    unsafe { CloseHandle(handle) };
    true
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real process tree; proves a grandchild is contained (race fix)"]
async fn windows_grandchild_is_contained() {
    // A parent that launches a detached grandchild which records its own PID and
    // then sleeps ~30s; the parent exits as soon as the grandchild is launched.
    // Before the CREATE_SUSPENDED fix the grandchild could be created in the
    // spawn→assign window and escape the job; now the parent runs suspended until
    // it is in the job, so whatever it spawns is contained too. Dropping the
    // group must therefore reap the grandchild, not just the parent.
    //
    // Two small .ps1 files avoid nested-quoting fragility: parent.ps1 launches
    // grandchild.ps1 via Start-Process (which returns immediately).
    let tmp = std::env::temp_dir();
    let tag = std::process::id();
    let pidfile = tmp.join(format!("processkit_gc_{tag}.pid"));
    let grandchild_ps1 = tmp.join(format!("processkit_gc_{tag}.ps1"));
    let parent_ps1 = tmp.join(format!("processkit_parent_{tag}.ps1"));
    let _ = std::fs::remove_file(&pidfile);

    std::fs::write(
        &grandchild_ps1,
        format!(
            "$PID | Set-Content -Encoding ascii '{}'\nStart-Sleep -Seconds 30\n",
            pidfile.display()
        ),
    )
    .expect("write grandchild script");
    std::fs::write(
        &parent_ps1,
        format!(
            "Start-Process -WindowStyle Hidden -FilePath powershell \
             -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','{}'\n",
            grandchild_ps1.display()
        ),
    )
    .expect("write parent script");

    let group = ProcessGroup::new().expect("create group");
    group
        .start(&Command::new("powershell").args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &parent_ps1.to_string_lossy(),
        ]))
        .await
        .expect("spawn parent")
        .wait()
        .await
        .expect("parent waits"); // parent exits promptly after launching grandchild

    // Wait for the grandchild to publish its PID.
    let mut grandchild_pid = None;
    for _ in 0..50 {
        if let Ok(text) = std::fs::read_to_string(&pidfile)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            grandchild_pid = Some(pid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let pid = grandchild_pid.expect("grandchild never recorded its PID");
    assert!(
        windows_pid_alive(pid),
        "grandchild should be alive before drop"
    );

    drop(group); // kill-on-close must reap the whole tree, grandchild included

    // Give the job a moment to tear the tree down.
    let mut reaped = false;
    for _ in 0..50 {
        if !windows_pid_alive(pid) {
            reaped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_file(&pidfile);
    let _ = std::fs::remove_file(&grandchild_ps1);
    let _ = std::fs::remove_file(&parent_ps1);
    assert!(
        reaped,
        "grandchild {pid} outlived its job — containment leaked"
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
    let (code, _stderr) = run.finish_streamed().await.expect("finish");

    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stream did not end at the deadline (took {:?})",
        start.elapsed()
    );
    // The tree was killed at the deadline. The exact code is platform-dependent
    // (None on a Unix signal-kill, a nonzero code on a Windows Job kill), so the
    // guarantee under test is "ended promptly and not a clean success".
    assert!(
        !matches!(code, Some(0)),
        "a timed-out streamed run must not look successful (got {code:?})"
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
    let (code, stderr) = process.finish_streamed().await.expect("finish");
    assert_eq!(code, Some(0));
    assert!(out.iter().any(|l| l.contains("out")), "stdout: {out:?}");
    assert!(stderr.contains("err"), "stderr: {stderr:?}");
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
#[ignore = "spawns a long-lived subprocess and kills it early"]
async fn start_kill_terminates_a_running_process() {
    let mut process = sleeper().start().await.expect("start sleeper");
    assert!(process.pid().is_some());
    process.start_kill().expect("start_kill");
    // After an explicit kill, waiting returns far sooner than the sleeper's ~30s
    // runtime. The exit code of a killed process is platform-dependent, so
    // promptness is the guarantee under test.
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("killed process should be reaped promptly")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "kill was not prompt (took {:?})",
        start.elapsed()
    );
}
