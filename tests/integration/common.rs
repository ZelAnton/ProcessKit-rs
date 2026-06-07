//! Shared per-platform helpers: canned child commands and liveness probes.

use processkit::Command;

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
/// goes away, then dies of `SIGPIPE` (Unix) or a broken-pipe write error
/// (PowerShell terminates on the failed write).
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
