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

#[cfg(feature = "process-control")]
use processkit::Signal;
use processkit::{Command, Mechanism, OutputBufferPolicy, ProcessGroup, wait_any};
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
async fn group_reports_the_platforms_mechanism() {
    let group = ProcessGroup::new().expect("create group");
    let mechanism = group.mechanism();
    // Tightened per platform: a silently-degraded backend (e.g. JobObject
    // creation failing over to nothing) must not pass as "known".
    #[cfg(windows)]
    assert_eq!(mechanism, Mechanism::JobObject);
    #[cfg(target_os = "linux")]
    assert!(
        matches!(mechanism, Mechanism::CgroupV2 | Mechanism::ProcessGroup),
        "linux is cgroup v2 or its pgroup fallback, got {mechanism:?}"
    );
    #[cfg(all(unix, not(target_os = "linux")))]
    assert_eq!(mechanism, Mechanism::ProcessGroup);
    #[cfg(not(any(unix, windows)))]
    assert_eq!(mechanism, Mechanism::None);
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

    // Generous anti-hang bound (the sleeper runs ~30s if the deadline is
    // broken): under full-suite load cold spawns have been seen to push a
    // 500ms-timeout run past 5s.
    assert!(
        start.elapsed() < Duration::from_secs(15),
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
#[cfg(feature = "process-control")]
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
#[cfg(feature = "process-control")]
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
#[cfg(feature = "process-control")]
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
#[cfg(feature = "process-control")]
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
#[cfg(feature = "process-control")]
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
#[cfg(feature = "process-control")]
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

// ----- Environment and privilege builders -----

/// A child that prints its whole environment, per platform.
fn print_env() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "set"])
    } else {
        Command::new("sh").args(["-c", "env"])
    }
}

#[tokio::test]
#[ignore = "spawns real subprocesses to compare environments"]
async fn inherit_env_whitelists_parent_env() {
    // Without a whitelist, an explicit marker (and the inherited env) shows up.
    let with_marker = print_env()
        .env("PK_ITEM8_MARKER", "present")
        .output_string()
        .await
        .expect("run env printer");
    assert!(with_marker.is_success());
    assert!(
        with_marker.stdout().contains("PK_ITEM8_MARKER"),
        "explicit env should reach the child"
    );

    // With an allow-list, only the named vars survive: PATH present (needed to
    // even find the shell on unix), the marker absent (never set explicitly,
    // and the inherited env was cleared).
    let whitelisted = print_env()
        .inherit_env(if cfg!(windows) {
            // cmd.exe needs SystemRoot to run at all.
            vec!["PATH", "SystemRoot"]
        } else {
            vec!["PATH"]
        })
        .output_string()
        .await
        .expect("run env printer");
    assert!(whitelisted.is_success(), "result: {whitelisted:?}");
    assert!(
        whitelisted.stdout().to_uppercase().contains("PATH="),
        "whitelisted PATH should be present: {:?}",
        whitelisted.stdout()
    );
    assert!(
        !whitelisted.stdout().contains("PK_ITEM8_MARKER"),
        "non-whitelisted vars must not leak"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess in a new session"]
async fn setsid_spawns_and_stays_contained() {
    // THE regression test for the setsid × process-group coordination: with
    // setpgid applied before pre_exec hooks, setsid would fail EPERM and the
    // spawn would error. It must succeed on every unix mechanism…
    let group = ProcessGroup::new().expect("create group");
    let process = group
        .start(&sleep_secs(30).setsid())
        .await
        .expect("setsid child spawns (EPERM would mean the pgroup coordination broke)");
    let pid = process.pid().expect("pid") as i32;

    // …and the new session's process group must still be contained: dropping
    // the group reaps the child.
    drop(group);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // SAFETY: signal 0 is a sound liveness probe.
        let alive = unsafe { libc::kill(pid, 0) == 0 };
        if !alive {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "setsid child survived the group drop — containment broke"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "drops privileges; meaningful only as root"]
async fn uid_gid_drop_privileges() {
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    let result = Command::new("id").arg("-u").uid(1).gid(1).run().await;
    match ProcessGroup::new().expect("probe group").mechanism() {
        // Documented caveat: under the cgroup mechanism the cgroup join runs
        // after the uid drop and fails with a permission error — the spawn
        // must error, never hand back an uncontained or wrongly-privileged
        // child.
        Mechanism::CgroupV2 => {
            assert!(
                result.is_err(),
                "uid drop on the cgroup mechanism is documented to fail the \
                 spawn, got {result:?}"
            );
        }
        _ => {
            let out = result.expect("run id -u as uid 1");
            assert_eq!(out.trim(), "1", "child should report the dropped uid");
        }
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the non-unix unsupported gate"]
async fn windows_unix_only_builders_are_unsupported() {
    for (command, what) in [
        (Command::new("cmd").args(["/c", "exit 0"]).uid(1000), "uid"),
        (Command::new("cmd").args(["/c", "exit 0"]).gid(1000), "gid"),
        (
            Command::new("cmd").args(["/c", "exit 0"]).setsid(),
            "setsid",
        ),
    ] {
        let err = command
            .output_string()
            .await
            .expect_err("a privilege request must not be silently skipped");
        assert!(
            matches!(err, processkit::Error::Unsupported { .. }),
            "expected Unsupported for {what}, got {err:?}"
        );
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess with CREATE_NO_WINDOW under a job"]
async fn windows_create_no_window_spawns_in_group() {
    // Window absence isn't assertable headlessly; what this proves is that the
    // extra flag is OR'd with (not clobbering) CREATE_SUSPENDED containment.
    let group = ProcessGroup::new().expect("create group");
    let process = group
        .start(&two_line_echo().create_no_window())
        .await
        .expect("spawn with CREATE_NO_WINDOW");
    let result = process.output_string().await.expect("collect");
    assert!(result.is_success(), "result: {result:?}");
    assert!(result.stdout().contains("first"));
}

// ----- Pipelines -----

/// A stage that copies stdin to stdout, per platform (`sort` keeps order-free
/// assertions simple on Windows; `cat` on Unix).
fn sort_stage() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("sort")
    }
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline"]
async fn pipeline_flows_data_between_stages() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo delta& echo alpha"])
    } else {
        Command::new("sh").args(["-c", "printf 'delta\\nalpha\\n'"])
    };

    let result = producer
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    let stdout = result.stdout();
    let alpha = stdout.find("alpha").expect("alpha in output");
    let delta = stdout.find("delta").expect("delta in output");
    assert!(alpha < delta, "sort should reorder: {stdout:?}");
}

#[tokio::test]
#[ignore = "spawns a real three-stage pipeline"]
async fn pipeline_three_stages_end_to_end() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo bb& echo aa& echo bb"])
    } else {
        Command::new("sh").args(["-c", "printf 'bb\\naa\\nbb\\n'"])
    };
    let filter = if cfg!(windows) {
        Command::new("findstr").arg("bb")
    } else {
        Command::new("grep").arg("bb")
    };

    let result = producer
        .pipe(sort_stage())
        .pipe(filter)
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains("bb"),
        "stdout: {:?}",
        result.stdout()
    );
    assert!(
        !result.stdout().contains("aa"),
        "filter stage should drop aa: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline with a failing inner stage"]
async fn pipeline_pipefail_attributes_the_first_failure() {
    // A tiny producer exits 0 (its few bytes fit the pipe buffer even though
    // the next stage never reads); the middle stage fails with a distinctive
    // code; the final stage succeeds reading EOF.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "3"])
    } else {
        Command::new("sh").args(["-c", "exit 3"])
    };

    let result = producer
        .pipe(failing)
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("pipeline completes with a result");
    assert_eq!(result.code(), Some(3), "pipefail code: {result:?}");
    assert!(!result.is_success());

    // run() surfaces the same attribution as a typed error.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "3"])
    } else {
        Command::new("sh").args(["-c", "exit 3"])
    };
    let err = producer
        .pipe(failing)
        .pipe(sort_stage())
        .run()
        .await
        .expect_err("a failing stage must fail run()");
    assert!(
        matches!(err, processkit::Error::Exit { code: 3, .. }),
        "expected Exit with code 3, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline and kills it at the deadline"]
async fn pipeline_timeout_kills_the_whole_chain() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };

    let start = Instant::now();
    let result = producer
        .pipe(sleep_secs(30))
        .timeout(Duration::from_millis(300))
        .output_string()
        .await
        .expect("a timed-out pipeline still reports a result");
    assert!(result.timed_out(), "result: {result:?}");
    assert!(!result.is_success());
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "pipeline did not honor its timeout (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline fed from a string stdin"]
async fn pipeline_honors_first_stage_stdin() {
    let result = sort_stage()
        .stdin(processkit::Stdin::from_string("delta\nalpha\n"))
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains("alpha") && result.stdout().contains("delta"),
        "stdin should flow through both stages: {:?}",
        result.stdout()
    );
}

// ----- Supervisor -----

#[tokio::test]
#[ignore = "spawns real subprocesses under supervision in a shared group"]
async fn supervisor_runs_incarnations_in_a_shared_group() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    let exits_zero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };

    // The headline `with_runner(&group)` path: every incarnation runs inside
    // one caller-owned kill-on-drop group, and the group stays usable after.
    let group = ProcessGroup::new().expect("create group");
    let outcome = Supervisor::new(exits_zero)
        .with_runner(&group)
        .restart(RestartPolicy::OnCrash)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .run()
        .await
        .expect("supervision completes");
    assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    assert!(outcome.final_result.is_success());

    // The shared group survived supervision and still works.
    let _after = group
        .start(&sleep_secs(1))
        .await
        .expect("group still usable");
}

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess outside the group and adopts it"]
async fn adopt_brings_an_external_child_under_containment() {
    // Spawn OUTSIDE any processkit group, adopt, then prove the group's
    // teardown reaps it — the adopt() containment claim, end-to-end.
    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("ping");
        c.args(["-n", "30", "127.0.0.1"]);
        c
    } else {
        let mut c = tokio::process::Command::new("sleep");
        c.arg("30");
        c
    };
    cmd.stdout(std::process::Stdio::null());
    let mut child = cmd.spawn().expect("spawn external child");

    let group = ProcessGroup::new().expect("create group");
    group.adopt(&child).expect("adopt external child");
    group.terminate_all().expect("terminate the adopted tree");

    // The adopted child must die promptly — well under its ~30s natural run.
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("adopted child reaped in time");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "adopted child was not contained (took {:?})",
        start.elapsed()
    );
}

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

#[cfg(feature = "process-control")]
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

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns real subprocesses and watches the member list shrink"]
async fn members_shrinks_when_a_child_dies() {
    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }
    // Single-process sleepers, deliberately: the cmd-wrapped `sleeper()` is two
    // processes whose second member spawns asynchronously, so a `before`
    // snapshot can race it — and `start_kill` would hit only the wrapper,
    // leaving its orphan in the job and the count above the threshold forever
    // (seen on a cold CI runner).
    let _keep = group.start(&sleep_secs(30)).await.expect("start survivor");
    let mut dying = group.start(&sleep_secs(30)).await.expect("start victim");
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

#[cfg(feature = "process-control")]
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

// ----- Cancellation: Command::cancel_on (feature `cancellation`) -----

#[cfg(feature = "cancellation")]
mod cancellation {
    use super::*;
    use processkit::CancellationToken;

    /// Whether a process with `pid` is still alive, per platform.
    fn pid_alive(pid: u32) -> bool {
        #[cfg(windows)]
        return super::windows_pid_alive(pid);
        #[cfg(unix)]
        // SAFETY: signal 0 is a sound liveness probe.
        return unsafe { libc::kill(pid as i32, 0) == 0 };
        #[cfg(not(any(windows, unix)))]
        {
            let _ = pid;
            false
        }
    }

    #[tokio::test]
    #[ignore = "spawns real subprocesses and cancels one mid-run"]
    async fn cancel_mid_run_errors_and_kills_only_the_cancelled_child() {
        let group = ProcessGroup::new().expect("create group");
        let token = CancellationToken::new();

        // A sibling in the same shared group: cancellation must not touch it
        // (same child-only scope as a timeout on a shared-group handle).
        let sibling = group.start(&sleep_secs(30)).await.expect("start sibling");
        let sibling_pid = sibling.pid().expect("sibling pid");

        // Single-process sleeper, deliberately: the cmd-wrapped `sleeper()` is
        // two processes on Windows, and the child-only cancel kill would leave
        // the grandchild holding the stdout pipe — stalling teardown for the
        // full pump grace instead of ending promptly.
        let run = group
            .start(&sleep_secs(30).cancel_on(token.clone()))
            .await
            .expect("start cancellable sleeper");
        let pid = run.pid().expect("pid");

        let canceller = tokio::spawn({
            let token = token.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                token.cancel();
            }
        });

        let start = Instant::now();
        let err = run
            .output_string()
            .await
            .expect_err("a cancelled run must error, not produce a result");
        assert!(
            matches!(err, processkit::Error::Cancelled { .. }),
            "expected Error::Cancelled, got {err:?}"
        );
        // Promptness: the sleeper runs ~30s if cancellation is broken. Generous
        // headroom for full-suite load (cf. the widened timeout-test bounds).
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "cancel was not prompt (took {:?})",
            start.elapsed()
        );
        canceller.await.expect("canceller task");

        // The cancelled child is killed and reaped...
        let deadline = Instant::now() + Duration::from_secs(5);
        while pid_alive(pid) {
            assert!(
                Instant::now() < deadline,
                "cancelled child survived (pid {pid})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // ...while the shared group's sibling is untouched.
        assert!(
            pid_alive(sibling_pid),
            "cancel must kill the child only, not shared-group siblings"
        );
        drop(sibling);
    }

    #[tokio::test]
    #[ignore = "exercises the pre-spawn short-circuit (no real subprocess)"]
    async fn pre_cancelled_token_short_circuits_before_spawning() {
        let token = CancellationToken::new();
        token.cancel();

        let start = Instant::now();
        // A program that doesn't exist: reaching the OS spawn would fail with
        // an Io error, so getting Cancelled proves the short-circuit fired
        // before any spawn was attempted.
        let err = Command::new("processkit-no-such-program-424242")
            .cancel_on(token)
            .run()
            .await
            .expect_err("a pre-cancelled run must not start");
        assert!(
            matches!(err, processkit::Error::Cancelled { .. }),
            "expected Error::Cancelled, got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "short-circuit was not immediate (took {:?})",
            start.elapsed()
        );
    }

    #[tokio::test]
    #[ignore = "spawns a real subprocess and cancels it mid-stream"]
    async fn cancel_ends_the_stream_and_finish_streamed_reports_it() {
        use tokio_stream::StreamExt;

        let token = CancellationToken::new();
        let mut run = banner_then_idle()
            .cancel_on(token.clone())
            .start()
            .await
            .expect("start banner child");

        let mut lines = run.stdout_lines();
        // Wait for the banner so the cancel provably lands mid-stream.
        let first = tokio::time::timeout(Duration::from_secs(15), lines.next())
            .await
            .expect("banner in time")
            .expect("banner line");
        assert!(first.contains("ready"), "line: {first:?}");

        token.cancel();

        // The cancel tears the (handle-owned) tree down, the pipes close, and
        // the stream ends — the child would otherwise idle ~30s.
        let start = Instant::now();
        while tokio::time::timeout(Duration::from_secs(10), lines.next())
            .await
            .expect("stream should end promptly after cancel")
            .is_some()
        {}
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "stream did not end promptly (took {:?})",
            start.elapsed()
        );

        let err = run
            .finish_streamed()
            .await
            .expect_err("finishing a cancelled streamed run must error");
        assert!(
            matches!(err, processkit::Error::Cancelled { .. }),
            "expected Error::Cancelled, got {err:?}"
        );
    }
}

// ----- Coverage-gap sweeps: stream edges, group idempotency, platform semantics -----

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
    let (code, _stderr) = tokio::time::timeout(Duration::from_secs(15), process.finish_streamed())
        .await
        .expect("finish_streamed must not hang without a prior stdout_lines")
        .expect("finish");
    assert_eq!(code, Some(0));
}

#[tokio::test]
#[ignore = "spawns a real subprocess and kills it twice"]
async fn terminate_all_is_idempotent() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleep_secs(30)).await.expect("start sleeper");

    group.terminate_all().expect("first terminate");
    group
        .terminate_all()
        .expect("second terminate must be a no-op success, not an error");

    // The group stays usable after teardown: a fresh spawn still lands in it.
    let again = group
        .start(&sleep_secs(1))
        .await
        .expect("group usable after terminate");
    drop(again);
    let _ = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child reaped");
}

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a short subprocess and adopts it after reaping"]
async fn adopt_of_a_reaped_child_errors_instead_of_tracking_nothing() {
    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }

    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/c", "exit", "0"]);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.args(["-c", "exit 0"]);
        c
    };
    let mut child = cmd.spawn().expect("spawn short child");
    let _ = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("short child exits");

    // A reaped child has no pid/handle left — adopting it must say so loudly
    // rather than silently tracking nothing.
    let err = group
        .adopt(&child)
        .expect_err("adopting a reaped child must error");
    assert!(
        matches!(err, processkit::Error::Io(_)),
        "expected the no-pid Io error, got {err:?}"
    );
}

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn empty_group_accepts_lifecycle_calls() {
    let group = ProcessGroup::new().expect("create group");
    if matches!(group.mechanism(), Mechanism::None) {
        eprintln!("skipping: no containment on this target");
        return;
    }

    // Signalling, freezing, and thawing nobody must succeed trivially…
    group.signal(Signal::Kill).expect("Kill on an empty group");
    if cfg!(windows) {
        // …except non-Kill signals on Windows, which are typed as unsupported
        // (a Job Object has no POSIX signals) even with no members.
        let err = group
            .signal(Signal::Term)
            .expect_err("only Kill is deliverable on Windows");
        assert!(
            matches!(err, processkit::Error::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    } else {
        group.signal(Signal::Term).expect("Term on an empty group");
    }
    group.suspend().expect("suspend an empty group");
    group.resume().expect("resume an empty group");

    #[cfg(feature = "stats")]
    {
        let stats = group.stats().expect("stats on an empty group");
        assert_eq!(stats.active_process_count, 0);
    }
}

#[cfg(windows)]
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess and nests suspend/resume"]
async fn windows_nested_suspend_needs_matching_resumes() {
    use tokio_stream::StreamExt;

    // Documented Windows semantics: suspend/resume are per-thread *counts*, so
    // two suspends need two resumes. A bare ping prints ~one line per second —
    // line flow is the freeze probe.
    let group = ProcessGroup::new().expect("create group");
    let mut run = group
        .start(&Command::new("ping").args(["-n", "31", "127.0.0.1"]))
        .await
        .expect("start ticker");
    let mut lines = run.stdout_lines();
    tokio::time::timeout(Duration::from_secs(15), lines.next())
        .await
        .expect("ticker prints")
        .expect("first line");

    group.suspend().expect("suspend #1");
    group.suspend().expect("suspend #2");
    group.resume().expect("resume #1 of 2");

    // Drain lines emitted before the freeze landed; 2s of silence (double the
    // ticker period) means the tree is genuinely frozen.
    loop {
        match tokio::time::timeout(Duration::from_secs(2), lines.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("ticker exited while suspended"),
            Err(_) => break,
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(3), lines.next())
            .await
            .is_err(),
        "one resume must not thaw two suspends"
    );

    group.resume().expect("resume #2 of 2");
    let line = tokio::time::timeout(Duration::from_secs(15), lines.next())
        .await
        .expect("a balanced resume thaws the tree");
    assert!(line.is_some(), "ticker resumed output");
}

#[cfg(target_os = "linux")]
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "adopts a real subprocess into a suspended cgroup"]
async fn linux_cgroup_adopt_into_suspended_group_freezes_the_child() {
    use tokio::io::AsyncBufReadExt;

    // Documented cgroup divergence: the freeze is *group state*, so a child
    // joining while the group is suspended freezes on attach. (Windows/pgroup
    // freeze only the members present at the call.) The join is exercised via
    // `adopt` — the parent writes the pid itself. `group.start()` would test
    // the same kernel behavior but can BLOCK here: the pre-exec cgroup join
    // freezes the child before the spawn handshake completes (see the
    // `suspend` rustdoc), which would hang this very test.
    let group = ProcessGroup::new().expect("create group");
    if !matches!(group.mechanism(), Mechanism::CgroupV2) {
        eprintln!("skipping: needs the cgroup mechanism");
        return;
    }

    // A free-running ticker, spawned OUTSIDE the group.
    let mut ticker = tokio::process::Command::new("sh")
        .args(["-c", "while :; do echo tick; sleep 0.25; done"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn ticker");
    let stdout = ticker.stdout.take().expect("ticker stdout");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("ticker prints")
        .expect("read line")
        .expect("a tick before adoption");

    group.suspend().expect("suspend the empty group");
    group
        .adopt(&ticker)
        .expect("adopt the ticker into the frozen cgroup");

    // Drain ticks emitted before the freeze landed; 1s of silence (4× the
    // tick period) means the child is genuinely frozen…
    loop {
        match tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await {
            Ok(Ok(Some(_))) => continue,
            Ok(_) => panic!("ticker exited while frozen"),
            Err(_) => break,
        }
    }
    // …and stays frozen.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .is_err(),
        "a child adopted into a suspended cgroup must freeze on attach"
    );

    group.resume().expect("resume");
    let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("thawed ticker resumes output")
        .expect("read line");
    assert_eq!(line.as_deref(), Some("tick"));

    let _ = ticker.kill().await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and shuts it down gracefully"]
async fn shutdown_lets_a_term_handling_child_end_the_grace_early() {
    // The struct update covers the `limits`-gated field; without that feature
    // every field is already named, which clippy would otherwise flag.
    #[allow(clippy::needless_update)]
    let group = ProcessGroup::with_options(processkit::ProcessGroupOptions {
        shutdown_timeout: Duration::from_secs(10),
        ..Default::default()
    })
    .expect("create group");

    // Exits 0 on SIGTERM. The sleep runs in the background (`wait` is
    // interruptible; a foreground sleep would delay the trap until it ends).
    let run = group
        .start(&Command::new("sh").args(["-c", "trap 'exit 0' TERM; sleep 30 & wait"]))
        .await
        .expect("start");
    // Reap concurrently: the graceful path's liveness probe sees a zombie as
    // alive, so the child must actually be collected for the early return.
    let waiter = tokio::spawn(run.wait());

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(20), group.shutdown())
        .await
        .expect("shutdown bounded")
        .expect("shutdown ok");
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        start.elapsed()
    );

    let code = waiter.await.expect("join").expect("wait");
    assert_eq!(code, Some(0), "the child exited via its TERM trap");
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a TERM-ignoring subprocess and escalates to SIGKILL"]
async fn shutdown_escalates_to_kill_after_the_grace_window() {
    // See above: the struct update exists for the `limits`-gated field.
    #[allow(clippy::needless_update)]
    let group = ProcessGroup::with_options(processkit::ProcessGroupOptions {
        shutdown_timeout: Duration::from_millis(500),
        escalate_to_kill: true,
        ..Default::default()
    })
    .expect("create group");

    // Ignores SIGTERM and busy-waits (a foreground `sleep` would itself die to
    // the broadcast TERM and end the script cleanly — defeating the test).
    let run = group
        .start(&Command::new("sh").args(["-c", "trap '' TERM; while :; do :; done"]))
        .await
        .expect("start");
    let waiter = tokio::spawn(run.wait());

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(15), group.shutdown())
        .await
        .expect("escalation keeps shutdown bounded")
        .expect("shutdown ok");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300),
        "the grace window must be waited out before escalating ({elapsed:?})"
    );

    let code = waiter.await.expect("join").expect("wait");
    assert_eq!(code, None, "SIGKILL leaves no exit code, got {code:?}");
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
