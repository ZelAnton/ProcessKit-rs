//! Stress-tier helpers: the opt-in gate and per-platform child commands.
//!
//! This is a *separate* test binary from `tests/integration`, so it can't share
//! that crate's `common` module — the small overlap (a few child builders) is
//! deliberate.

use processkit::Command;

/// The stress tier is **opt-in**: every scenario early-returns unless
/// `PROCESSKIT_STRESS` is set, so the normal PR matrix compiles the tier (free
/// breakage-checking) but doesn't pay its cost. The nightly workflow sets it.
///
/// Returns `true` (and prints why) when the caller should skip. Use as the
/// first line of each scenario: `if skip_unless_enabled("name") { return; }`.
pub(crate) fn skip_unless_enabled(scenario: &str) -> bool {
    if std::env::var_os("PROCESSKIT_STRESS").is_some() {
        return false;
    }
    eprintln!("[stress] skipping {scenario}: set PROCESSKIT_STRESS=1 to run");
    true
}

/// A command that exits 0 immediately, per platform.
pub(crate) fn quick_exit() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("true")
    }
}

/// A command that exits non-zero immediately, per platform — feeds the
/// supervisor storm guard.
pub(crate) fn always_fail() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "1"])
    } else {
        Command::new("sh").args(["-c", "exit 1"])
    }
}

/// A command that emits `n` numbered lines then exits 0, per platform — drives
/// the output pump at volume.
///
/// The Windows form **streams** via a `for` loop rather than `1..n` (which
/// materializes the whole range and pushes it through the host formatter at
/// once); streaming both starts faster and better models a flooding producer
/// the pump has to keep draining under sustained back-pressure.
pub(crate) fn line_emitter(n: u32) -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!("for ($i = 1; $i -le {n}; $i++) {{ $i }}"),
        ])
    } else {
        Command::new("seq").arg(n.to_string())
    }
}

/// A long-lived (~90s) silent child, per platform — a process to kill or tear
/// down rather than wait out. The lifetime is long on purpose: the teardown
/// scenario proves children are reaped *promptly* (far inside a grace shorter
/// than this), so a survivor that ran its full ~90s would miss the grace and
/// fail the test.
pub(crate) fn long_sleeper() -> Command {
    if cfg!(windows) {
        Command::new("ping").args(["-n", "91", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("90")
    }
}

/// A child for the concurrent-PTY stress: floods `n` numbered lines then prints
/// the unique `marker` and exits 0 — per platform. The flood drives the PTY
/// master's *merged, non-blocking* reader under sustained output while `marker`
/// gives each concurrent session a distinct end-of-stream sentinel, so a session
/// that drained all the way to EOF is provable per-session. `marker` must be
/// `[A-Za-z0-9-]` only (the call sites pass `PTY-DONE-<n>`), safe unquoted in
/// `sh` and inside a PowerShell single-quoted string.
#[cfg(feature = "pty")]
pub(crate) fn pty_flood_then_marker(n: u32, marker: &str) -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!("for ($i = 1; $i -le {n}; $i++) {{ $i }}; Write-Output '{marker}'"),
        ])
    } else {
        Command::new("sh").args(["-c", &format!("seq {n}; echo {marker}")])
    }
}

/// A child for the concurrent-PTY round-trip stress: reads one line from its
/// (terminal) stdin and echoes `reply:<line>`, then exits — per platform. Drives
/// the master's write side (the prompt) and read side (the reply) concurrently,
/// the exact interleaving the Unix non-blocking (`AsyncFd`) rewrite must keep
/// correct across many simultaneous sessions.
#[cfg(feature = "pty")]
pub(crate) fn pty_prompt_responder() -> Command {
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

/// Best-effort count of this process's open file descriptors/handles, used to
/// assert spawn/reap churn doesn't leak them. `None` means the platform
/// mechanism is unavailable or failed (no `/proc`, no `lsof` on `$PATH`, an
/// unsupported target, …) — the caller skips the leak assertion rather than
/// let a missing mechanism read as a false pass.
#[cfg(target_os = "linux")]
pub(crate) fn open_handle_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|dir| dir.count())
}

/// macOS/BSD have no `/proc`; shell out to `lsof -p <pid>` and count the rows.
/// `lsof` lists one row per open descriptor plus a fixed handful of non-fd
/// rows (cwd, txt, mapped libraries) that don't grow with spawn/reap churn, so
/// a before/after diff still isolates a real leak without needing to filter
/// them out.
#[cfg(target_os = "macos")]
pub(crate) fn open_handle_count() -> Option<usize> {
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("lsof")
        .args(["-p", &pid])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    // The first line is the column header, not an fd row.
    Some(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .count()
            .saturating_sub(1),
    )
}

/// Windows counter of open handles (`GetProcessHandleCount`, kernel32) — the
/// closest analogue to a Unix fd count, covering files/pipes/events/etc. a
/// leaking child-process pipeline could otherwise accumulate in this process.
#[cfg(windows)]
pub(crate) fn open_handle_count() -> Option<usize> {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};

    let mut count: u32 = 0;
    // SAFETY: `GetCurrentProcess` returns a pseudo-handle that needs no
    // closing; `count` is a valid, uniquely-owned out-param for the call.
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    (ok != 0).then_some(count as usize)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn open_handle_count() -> Option<usize> {
    None
}

/// The cgroup v2 (unified) mount root this process's own cgroup lives under,
/// mirroring the discovery `sys/linux.rs`'s `Cgroup::create` performs (root
/// candidates, then `/proc/self/cgroup`'s `0::` line) — needed here only so
/// the drop-cleanup stress test can look in the same parent directory a
/// [`processkit::ProcessGroup`] creates its cgroup under.
#[cfg(target_os = "linux")]
pub(crate) fn own_cgroup_v2_parent() -> Option<std::path::PathBuf> {
    for candidate in ["/sys/fs/cgroup", "/sys/fs/cgroup/unified"] {
        let root = std::path::Path::new(candidate);
        if !root.join("cgroup.controllers").exists() {
            continue;
        }
        let self_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let rel = self_cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap_or("/")
            .trim();
        return Some(root.join(rel.trim_start_matches('/')));
    }
    None
}

/// Every `processkit-<own pid>-*` child directory currently under `parent` —
/// the naming pattern `Cgroup::create` (`sys/linux.rs`) uses, scoped to our
/// own pid so a concurrently-running, unrelated processkit instance's dirs
/// are never mistaken for ours.
#[cfg(target_os = "linux")]
pub(crate) fn own_processkit_cgroup_dirs(parent: &std::path::Path) -> Vec<std::path::PathBuf> {
    let prefix = format!("processkit-{}-", std::process::id());
    std::fs::read_dir(parent)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(&prefix))
                })
                .collect()
        })
        .unwrap_or_default()
}
