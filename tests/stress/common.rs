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

/// Process file-descriptor count for the current process (Linux only, via
/// `/proc/self/fd`). Used to assert churn doesn't leak fds. macOS/BSD lack
/// `/proc`, so the fd-stability assertion there is skipped by the caller.
#[cfg(target_os = "linux")]
pub(crate) fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|dir| dir.count())
        .unwrap_or(0)
}
