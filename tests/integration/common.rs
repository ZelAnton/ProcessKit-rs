//! Shared per-platform helpers: canned child commands, liveness probes, and
//! timing/poll utilities for the real-subprocess tests.

use std::future::Future;
use std::time::{Duration, Instant};

use processkit::Command;

/// Awaits `fut`, panicking if it does not finish within `max`; returns its
/// output. Prefer this over a bare `Instant::now()` / `elapsed()` pair for
/// promptness checks: it expresses the deadline in one place *and* bounds a
/// hang, so a regression fails the one test instead of stalling the suite.
pub(crate) async fn completes_within<T>(
    max: Duration,
    what: &str,
    fut: impl Future<Output = T>,
) -> T {
    match tokio::time::timeout(max, fut).await {
        Ok(out) => out,
        Err(_) => panic!("{what} did not finish within {max:?}"),
    }
}

/// Polls `cond` every `interval` until it returns `true`, panicking if `max`
/// elapses first. Replaces hand-rolled `loop { … deadline … sleep }` and
/// `for _ in 0..N { … sleep }` waits, centralising the deadline arithmetic that
/// is easy to get subtly wrong (off-by-one iteration counts, a missing final
/// check after the loop). `cond` is `FnMut`, so it may capture a value found on
/// the satisfying poll. Don't use it where cleanup must run on timeout — the
/// panic skips any teardown after the call site.
// All current callers sit behind a feature/platform gate, so a minimal feature
// set can legitimately compile this in with no user.
#[allow(dead_code)]
pub(crate) async fn poll_until(
    max: Duration,
    interval: Duration,
    what: &str,
    mut cond: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + max;
    loop {
        if cond() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {max:?}"
        );
        tokio::time::sleep(interval).await;
    }
}

/// A command that prints five numbered lines and exits 0, per platform.
pub(crate) fn five_lines() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo 1& echo 2& echo 3& echo 4& echo 5"])
    } else {
        Command::new("sh").args(["-c", "printf '1\\n2\\n3\\n4\\n5\\n'"])
    }
}

/// A command that prints two known lines and exits 0, per platform.
pub(crate) fn two_line_echo() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo first& echo second"])
    } else {
        Command::new("sh").args(["-c", "printf 'first\\nsecond\\n'"])
    }
}

/// A command that runs ~30s with no output, per platform.
pub(crate) fn sleeper() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping", "-n", "30", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("30")
    }
}

/// A command that sleeps ~`secs` seconds then exits 0, per platform.
pub(crate) fn sleep_secs(secs: u32) -> Command {
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

/// A child that prints `ready` after ~1s, then idles ~30s, per platform.
pub(crate) fn banner_then_idle() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args([
            "/c",
            "ping -n 2 127.0.0.1 >nul & echo ready & ping -n 30 127.0.0.1 >nul",
        ])
    } else {
        Command::new("sh").args(["-c", "sleep 0.5; echo ready; sleep 30"])
    }
}

/// An endless line producer, per platform: emits `y` lines until its consumer
/// goes away. On Unix it then dies of `SIGPIPE` promptly; on Windows
/// PowerShell may keep buffering without observing the closed pipe, so pair
/// it with a per-stage `timeout` — that kill is the load-bearing teardown
/// there (and is equally "unclean" to pipefail).
pub(crate) fn endless_yes() -> Command {
    if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", "while($true){'y'}"])
    } else {
        Command::new("yes")
    }
}

/// A consumer that reads one line, prints it, and exits 0, per platform — the
/// `| head -1` shape that kills its producer by closing the pipe early.
pub(crate) fn first_line_consumer() -> Command {
    if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", "[Console]::In.ReadLine()"])
    } else {
        Command::new("head").args(["-n", "1"])
    }
}

/// A command that exits with `code` and no output, per platform.
pub(crate) fn failing_exit(code: i32) -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", &code.to_string()])
    } else {
        Command::new("sh").args(["-c", &format!("exit {code}")])
    }
}

/// A child that copies its stdin to stdout byte-for-byte (no line buffering,
/// no text-encoding translation), per platform — used to assert that a
/// specific raw byte (e.g. a control byte from `send_control`) reaches the
/// child unmodified. `cat` already does this on Unix; on Windows there is no
/// standard line-oriented tool that passes arbitrary bytes through untouched,
/// so a short PowerShell one-liner copies the raw stdin/stdout streams.
pub(crate) fn raw_stdin_echo() -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "$in=[Console]::OpenStandardInput(); $out=[Console]::OpenStandardOutput(); \
             $in.CopyTo($out); $out.Flush()",
        ])
    } else {
        Command::new("cat")
    }
}

/// A child that writes `bytes` bytes to stdout in one burst, then creates an
/// empty marker file at `marker` and exits 0, per platform. `bytes` should be
/// well past the OS pipe buffer (~64 KiB on Linux, similarly small elsewhere):
/// if nothing drains piped stdout while the child writes, it stalls in
/// `write()` and never reaches (creates) the marker — the shape a readiness
/// check can poll for without touching the child's stdout itself.
pub(crate) fn big_stdout_then_marker(bytes: usize, marker: &std::path::Path) -> Command {
    let marker = marker.display();
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!(
                "[Console]::Out.Write(('x' * {bytes})); \
                 New-Item -ItemType File -Path '{marker}' -Force | Out-Null"
            ),
        ])
    } else {
        Command::new("sh").args(["-c", &format!("yes | head -c {bytes}; touch '{marker}'")])
    }
}

/// A child that writes `bytes` bytes to **stderr** in one burst, then creates an
/// empty marker file at `marker` and exits 0, per platform — the stderr analogue
/// of [`big_stdout_then_marker`]. `bytes` should be well past the OS pipe buffer
/// (~64 KiB on Linux, similarly small elsewhere): if nothing drains piped stderr
/// while the child writes, it stalls in `write()` and never reaches (creates) the
/// marker. Pair it with `stdout(StdioMode::Null)` to reproduce the readiness-probe
/// case where stdout is not piped and only stderr needs a background drain.
pub(crate) fn big_stderr_then_marker(bytes: usize, marker: &std::path::Path) -> Command {
    let marker = marker.display();
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!(
                "[Console]::Error.Write(('x' * {bytes})); \
                 New-Item -ItemType File -Path '{marker}' -Force | Out-Null"
            ),
        ])
    } else {
        // `1>&2` sends head's byte burst to stderr; the marker follows only once
        // that write completes.
        Command::new("sh").args([
            "-c",
            &format!("yes | head -c {bytes} 1>&2; touch '{marker}'"),
        ])
    }
}

/// A child that prints its whole environment, per platform.
pub(crate) fn print_env() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "set"])
    } else {
        Command::new("sh").args(["-c", "env"])
    }
}

/// Whether a process with `pid` is still alive (Windows): `OpenProcess` with
/// limited-query access succeeds while it lives; once reaped the pid is invalid.
#[cfg(windows)]
pub(crate) fn windows_pid_alive(pid: u32) -> bool {
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
