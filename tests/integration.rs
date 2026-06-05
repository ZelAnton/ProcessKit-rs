//! Real-subprocess integration tests for `processkit`.
//!
//! These spawn actual child processes (and create OS jobs / cgroups), so they
//! are `#[ignore]`d to keep `cargo test` hermetic on CI. Run them locally with:
//!
//! ```text
//! cargo test --all-features -- --ignored
//! ```
//!
//! (`--all-features` because the `limits` tests are compiled out by default.)
//!
//! The no-orphan kernel guarantee can only be proven against a real process
//! tree, which is exactly what these cover.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use processkit::{Command, Mechanism, OutputBufferPolicy, ProcessGroup, Signal, wait_any};
// Imported only by the `limits` tests; the other tests name `processkit::Error`
// variants via their full path.
#[cfg(feature = "limits")]
use processkit::{Error, ProcessGroupOptions};

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

/// A command that sleeps ~`secs` seconds then exits 0, per platform.
fn sleep_secs(secs: u32) -> Command {
    if cfg!(windows) {
        // ping waits ~1s between echoes, so n+1 echoes ≈ n seconds.
        Command::new("ping").args([
            "-n".to_string(),
            (secs + 1).to_string(),
            "127.0.0.1".to_string(),
        ])
    } else {
        Command::new("sleep").arg(secs.to_string())
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

#[cfg(feature = "stats")]
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

#[cfg(feature = "stats")]
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

// ----- Resource limits (memory / process count / CPU) -----

#[cfg(feature = "limits")]
#[tokio::test]
#[ignore = "creates an OS job/cgroup with a resource limit"]
async fn limits_are_enforced_or_rejected_per_platform() {
    // Setting a limit must either be honored by a real container (Windows Job
    // Object / Linux cgroup) or fail fast with `Error::ResourceLimit` — never
    // silently hand back an unbounded group.
    let res =
        ProcessGroup::with_options(ProcessGroupOptions::default().memory_max(64 * 1024 * 1024));
    if cfg!(windows) {
        let group = res.expect("Windows Job Objects enforce a memory cap");
        assert!(matches!(group.mechanism(), Mechanism::JobObject));
    } else if cfg!(target_os = "linux") {
        match res {
            Ok(group) => assert!(matches!(group.mechanism(), Mechanism::CgroupV2)),
            // Common on dev boxes / CI without cgroup delegation — the fail-fast path.
            Err(Error::ResourceLimit(_)) => {
                eprintln!("skipping cgroup enforcement: controller delegation unavailable");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    } else {
        // macOS/BSD and the no-containment target have no whole-tree cap.
        assert!(
            matches!(res, Err(Error::ResourceLimit(_))),
            "a limit on a container-less target must be rejected, not silently dropped"
        );
    }
}

#[cfg(all(windows, feature = "limits"))]
#[tokio::test]
#[ignore = "spawns real subprocesses to prove the active-process cap is enforced"]
async fn windows_process_count_limit_is_enforced() {
    // A single-process sleeper keeps the accounting unambiguous (one process per
    // start), so `max_processes(1)` admits the first and must refuse the second.
    let one_proc_sleeper = || Command::new("ping").args(["-n", "30", "127.0.0.1"]);

    let group = ProcessGroup::with_options(ProcessGroupOptions::default().max_processes(1))
        .expect("create capped group");
    assert!(matches!(group.mechanism(), Mechanism::JobObject));

    let _first = group
        .start(&one_proc_sleeper())
        .await
        .expect("first child fits the cap");
    let second = group.start(&one_proc_sleeper()).await;
    assert!(
        second.is_err(),
        "a second process must not be admitted past max_processes(1)"
    );
}

#[cfg(all(windows, feature = "limits"))]
#[tokio::test]
#[ignore = "creates a capped Job Object and runs a small child within it"]
async fn windows_memory_and_cpu_limits_accept_and_run() {
    // A generous memory cap plus a half-core CPU cap must be accepted by the job
    // (both SetInformationJobObject calls succeed) and must not break an ordinary
    // short-lived child.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .memory_max(512 * 1024 * 1024)
            .cpu_quota(0.5),
    )
    .expect("create capped group");
    assert!(matches!(group.mechanism(), Mechanism::JobObject));

    let out = group
        .start(&Command::new("cmd").args(["/c", "echo hi"]))
        .await
        .expect("spawn small child")
        .output_string()
        .await
        .expect("collect");
    assert!(out.is_success(), "exit {:?}", out.code());
    assert!(out.stdout().contains("hi"), "stdout: {:?}", out.stdout());
}

// ----- Whole-tree signals and suspend/resume -----

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and signals it"]
async fn unix_signal_reaches_the_tree() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // Print a readiness marker once the trap is installed, then idle; on SIGHUP
    // the trap fires after the current `sleep` returns (it dies to the HUP too).
    let cmd = Command::new("sh").args([
        "-c",
        "trap 'echo got-hup' HUP; echo ready; while :; do sleep 0.1; done",
    ]);
    let mut process = group.start(&cmd).await.expect("start trap child");
    let mut lines = process.stdout_lines();

    let ready = tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("readiness line in time")
        .expect("readiness line");
    assert!(ready.contains("ready"), "line: {ready:?}");

    group.signal(Signal::Hup).expect("broadcast SIGHUP");
    let got = tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("trap line in time")
        .expect("trap line");
    assert!(got.contains("got-hup"), "line: {got:?}");
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and freezes it"]
async fn unix_suspend_freezes_progress() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // A ticker: one line every ~50ms.
    let cmd = Command::new("sh").args([
        "-c",
        "i=0; while :; do i=$((i+1)); echo $i; sleep 0.05; done",
    ]);
    let mut process = group.start(&cmd).await.expect("start ticker");
    let mut lines = process.stdout_lines();

    // Prove it is producing output, then freeze.
    tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("first tick in time")
        .expect("first tick");
    group.suspend().expect("suspend");

    // Drain lines emitted before the freeze landed (pipe buffering), then
    // require silence for a window several ticks long.
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), lines.next()).await {}
    let stalled = tokio::time::timeout(Duration::from_millis(400), lines.next()).await;
    assert!(stalled.is_err(), "frozen tree kept producing output");

    group.resume().expect("resume");
    let resumed = tokio::time::timeout(Duration::from_secs(10), lines.next()).await;
    assert!(
        resumed.is_ok_and(|line| line.is_some()),
        "tree did not resume ticking"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "creates an OS job/cgroup"]
fn signal_on_empty_group_is_ok() {
    // An empty group is trivially signalled/suspended/resumed — load-bearing
    // for callers that broadcast before (or after) any member is alive.
    let group = ProcessGroup::new().expect("create group");
    group.signal(Signal::Term).expect("signal on empty group");
    group.suspend().expect("suspend on empty group");
    group.resume().expect("resume on empty group");
}

#[cfg(windows)]
#[test]
#[ignore = "creates an OS job"]
fn windows_signal_non_kill_is_unsupported() {
    // Job Objects have no POSIX signals: everything except Kill must surface as
    // the typed Unsupported error, never a silent no-op.
    let group = ProcessGroup::new().expect("create group");
    for sig in [Signal::Term, Signal::Hup, Signal::Other(9)] {
        let err = group
            .signal(sig)
            .expect_err("a non-Kill signal must be rejected on Windows");
        assert!(
            matches!(err, processkit::Error::Unsupported { .. }),
            "expected Error::Unsupported for {sig:?}, got {err:?}"
        );
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and kills it via Signal::Kill"]
async fn windows_signal_kill_kills_tree() {
    let group = ProcessGroup::new().expect("create group");
    let process = group.start(&sleeper()).await.expect("start sleeper");
    assert!(process.pid().is_some());

    group
        .signal(Signal::Kill)
        .expect("Signal::Kill maps to job terminate");

    // The ~30s sleeper waiting out promptly proves the whole tree was killed
    // (pid liveness can't be probed here: our own RunningProcess still holds the
    // child handle, which keeps the terminated process object around).
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("killed tree should be reaped promptly")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "Signal::Kill was not prompt (took {:?})",
        start.elapsed()
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and suspends/resumes its threads"]
async fn windows_suspend_resume_stalls_output() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // ping prints one line per second — a slow ticker.
    let cmd = Command::new("ping").args(["-n", "30", "127.0.0.1"]);
    let mut process = group.start(&cmd).await.expect("start ping");
    let mut lines = process.stdout_lines();

    tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("first ping line in time")
        .expect("first ping line");
    group.suspend().expect("suspend");

    // Drain pre-freeze buffered lines, then require silence across what would
    // be two ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), lines.next()).await {}
    let stalled = tokio::time::timeout(Duration::from_secs(2), lines.next()).await;
    assert!(stalled.is_err(), "suspended tree kept producing output");

    group.resume().expect("resume");
    let resumed = tokio::time::timeout(Duration::from_secs(10), lines.next()).await;
    assert!(
        resumed.is_ok_and(|line| line.is_some()),
        "tree did not resume output"
    );
}

// ----- Stats sampling: sample_stats() and profile() -----

#[cfg(feature = "stats")]
#[tokio::test]
#[ignore = "spawns a real subprocess and samples the group's stats"]
async fn sample_stats_yields_a_live_series() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }
    let _child = group.start(&sleeper()).await.expect("start sleeper");

    let mut samples = group.sample_stats(Duration::from_millis(50));
    for n in 0..3 {
        let snapshot = tokio::time::timeout(Duration::from_secs(5), samples.next())
            .await
            .expect("sample in time")
            .expect("series still live");
        assert!(
            snapshot.active_process_count >= 1,
            "sample #{n} saw no live process: {snapshot:?}"
        );
    }
}

#[cfg(feature = "stats")]
#[tokio::test]
#[ignore = "spawns a real subprocess and profiles its run"]
async fn profile_summarizes_a_run() {
    let profile = group_started_short_run()
        .await
        .profile(Duration::from_millis(50))
        .await
        .expect("profile");

    assert_eq!(profile.exit_code, Some(0), "profile: {profile:?}");
    assert!(
        profile.duration >= Duration::from_millis(500),
        "a ~1s child reported {:?}",
        profile.duration
    );
    assert!(profile.samples >= 1, "profile never sampled: {profile:?}");
    if cfg!(any(windows, target_os = "linux")) {
        assert!(
            profile.peak_memory_bytes.is_some(),
            "peak RSS should be readable on this platform: {profile:?}"
        );
    }
}

/// Start a ~1s single-process child directly (its own private group).
#[cfg(feature = "stats")]
async fn group_started_short_run() -> processkit::RunningProcess {
    sleep_secs(1).start().await.expect("start short child")
}

// ----- Readiness probes: wait_for_line / wait_for_port / wait_for -----

/// A child that prints `ready` after ~1s, then idles ~30s, per platform.
fn banner_then_idle() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args([
            "/c",
            "ping -n 2 127.0.0.1 >nul & echo ready & ping -n 30 127.0.0.1 >nul",
        ])
    } else {
        Command::new("sh").args(["-c", "sleep 0.5; echo ready; sleep 30"])
    }
}

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for its readiness banner"]
async fn wait_for_line_matches_banner_and_leaves_child_running() {
    let mut process = banner_then_idle().start().await.expect("start");
    let line = tokio::time::timeout(
        Duration::from_secs(15),
        process.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10)),
    )
    .await
    .expect("probe finished in time")
    .expect("banner matched");
    assert!(line.contains("ready"), "line: {line:?}");

    // The probe must not have killed the still-idling child.
    assert!(process.pid().is_some());
    process.start_kill().expect("kill");
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("reaped promptly");
}

#[tokio::test]
#[ignore = "spawns a silent subprocess; the probe must give up at its deadline"]
async fn wait_for_line_not_ready_when_silent() {
    // Genuinely silent: the plain `sleeper()` ping prints lines on Windows.
    let silent = if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping -n 30 127.0.0.1 >nul"])
    } else {
        Command::new("sleep").arg("30")
    };
    let mut process = silent.start().await.expect("start sleeper");
    let start = Instant::now();
    let err = process
        .wait_for_line(|_| true, Duration::from_millis(300))
        .await
        .expect_err("a silent child never becomes ready");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "probe gave up before its deadline ({:?})",
        start.elapsed()
    );
    // The probe does not kill: the sleeper is still there to reap ourselves.
    assert!(process.pid().is_some());
    process.start_kill().expect("kill");
}

#[tokio::test]
#[ignore = "spawns a short subprocess; the probe must fail fast once stdout closes"]
async fn wait_for_line_not_ready_fast_when_child_exits_silently() {
    let mut process = two_line_echo().start().await.expect("start echo");
    let start = Instant::now();
    let err = process
        .wait_for_line(|l| l.contains("never-printed"), Duration::from_secs(30))
        .await
        .expect_err("the banner never appears");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stdout closed — the probe should not wait out the 30s deadline ({:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and probes a TCP port that opens late"]
async fn wait_for_port_succeeds_against_a_late_listener() {
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
    // The "server" socket opens only after a delay — the probe must poll, not
    // one-shot.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral listener");
        let addr = listener.local_addr().expect("local addr");
        let _ = addr_tx.send(addr);
        // Keep the listener alive long enough for the probe to connect.
        tokio::time::sleep(Duration::from_secs(15)).await;
        drop(listener);
    });

    let mut process = sleeper().start().await.expect("start context child");
    let addr = addr_rx.await.expect("listener address");
    tokio::time::timeout(
        Duration::from_secs(15),
        process.wait_for_port(addr, Duration::from_secs(10)),
    )
    .await
    .expect("probe finished in time")
    .expect("port became ready");
}

#[tokio::test]
#[ignore = "spawns a real subprocess and polls an async readiness check"]
async fn wait_for_passes_once_the_check_turns_true() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let mut process = sleeper().start().await.expect("start sleeper");
    let attempts = std::sync::Arc::new(AtomicU32::new(0));
    let seen = std::sync::Arc::clone(&attempts);
    process
        .wait_for(
            move || {
                let n = seen.fetch_add(1, Ordering::SeqCst);
                async move { n >= 2 }
            },
            Duration::from_secs(10),
        )
        .await
        .expect("third attempt passes");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "the check should have been re-invoked across ticks"
    );
}

#[tokio::test]
#[ignore = "spawns a short subprocess; the probe must fail fast once it exits"]
async fn wait_for_fails_fast_when_child_exits() {
    let mut process = two_line_echo().start().await.expect("start echo");
    let start = Instant::now();
    let err = process
        .wait_for(|| async { false }, Duration::from_secs(30))
        .await
        .expect_err("an exited child never becomes ready");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child exited — the probe should not wait out the 30s deadline ({:?})",
        start.elapsed()
    );
}

// ----- Supervisor -----

#[tokio::test]
#[ignore = "spawns real subprocesses repeatedly under supervision"]
async fn supervisor_exhausts_restarts_on_a_crashing_child() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    let always_fails = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "1"])
    } else {
        Command::new("sh").args(["-c", "exit 1"])
    };

    let outcome = Supervisor::new(always_fails)
        .restart(RestartPolicy::OnCrash)
        .max_restarts(2)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .run()
        .await
        .expect("supervision completes with a result");

    assert_eq!(outcome.restarts, 2, "two restarts = three real runs");
    assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
    assert_eq!(outcome.final_result.code(), Some(1));
}

// ----- Tree inspection: members() and wait_any -----

#[tokio::test]
#[ignore = "spawns real subprocesses and lists the group's members"]
async fn members_lists_live_children() {
    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }
    let _a = group.start(&sleeper()).await.expect("start first sleeper");
    let _b = group.start(&sleeper()).await.expect("start second sleeper");

    // Windows/cgroup list the whole tree (a started child may be a shell plus
    // its own child); the pgroup backends list one leader per started child.
    // Either way, two started children mean at least two live pids.
    let members = group.members().expect("members");
    assert!(members.len() >= 2, "members: {members:?}");
}

#[tokio::test]
#[ignore = "spawns real subprocesses and watches the member list shrink"]
async fn members_shrinks_when_a_child_dies() {
    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }
    let _keep = group.start(&sleeper()).await.expect("start survivor");
    let mut dying = group.start(&sleeper()).await.expect("start victim");
    let before = group.members().expect("members").len();
    assert!(before >= 2, "expected at least two members, got {before}");

    dying.start_kill().expect("kill victim");
    // Reap it (wait consumes the handle) so the kill is visible everywhere —
    // an unreaped zombie still probes as alive on the pgroup backends.
    let _ = tokio::time::timeout(Duration::from_secs(10), dying.wait())
        .await
        .expect("victim reaped in time");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let now = group.members().expect("members").len();
        if now < before {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "member count never dropped below {before}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn members_on_empty_group_is_empty() {
    let group = ProcessGroup::new().expect("create group");
    let members = group.members().expect("members");
    assert!(members.is_empty(), "fresh group has members: {members:?}");
}

#[tokio::test]
#[ignore = "spawns real subprocesses and races their exits"]
async fn wait_any_returns_first_finisher() {
    let group = ProcessGroup::new().expect("create group");
    let mut slow = group.start(&sleep_secs(15)).await.expect("start slow");
    let mut fast = group.start(&sleep_secs(1)).await.expect("start fast");

    let (idx, code) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_any(&mut [&mut slow, &mut fast]),
    )
    .await
    .expect("race finished in time")
    .expect("race");
    assert_eq!(idx, 1, "the 1-second sleeper should finish first");
    assert_eq!(code, Some(0), "the fast sleeper exits cleanly");
}

#[tokio::test]
#[ignore = "spawns real subprocesses; proves the race loser stays usable"]
async fn wait_any_losers_still_waitable() {
    let group = ProcessGroup::new().expect("create group");
    // A single-process sleeper: `start_kill` must hit the process holding the
    // pipes, or `wait` idles out the pump-teardown grace for an orphaned child.
    let mut slow = group.start(&sleep_secs(30)).await.expect("start slow");
    let mut fast = group.start(&sleep_secs(1)).await.expect("start fast");

    let (idx, _code) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_any(&mut [&mut slow, &mut fast]),
    )
    .await
    .expect("race finished in time")
    .expect("race");
    assert_eq!(idx, 1);

    // The loser was only borrowed by the race — kill it and reap it promptly to
    // prove the handle still works end-to-end.
    slow.start_kill().expect("kill the loser");
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), slow.wait())
        .await
        .expect("loser reaped in time")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "loser wait was not prompt (took {:?})",
        start.elapsed()
    );
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
