//! Free-standing identity & reuse-safe liveness queries for an **arbitrary** pid
//! — one the caller holds *outside* any [`ProcessGroup`](crate::ProcessGroup).
//!
//! [`process_info`] answers "does this pid name a process, and what is it?" with
//! the same best-effort fields a group member carries in a
//! [`MemberInfo`](crate::MemberInfo); [`process_is_alive`] answers "is the *same*
//! process I saw earlier still running?" — reuse-safe, by pairing the pid with the
//! start-time token, so a recycled number is not mistaken for the original.
//!
//! Both reuse the crate's existing per-platform readers rather than a second
//! implementation, and both keep the crate's standing rules: **never** read a
//! process's argv/environment, and honestly tell **"no such process"** (a negative
//! answer) apart from **"not allowed to look"** (an error).

use crate::member::MemberInfo;
use crate::{Error, Result};

/// Look up the identity and best-effort metadata of an **arbitrary** process by
/// pid — the standalone companion to
/// [`ProcessGroup::members_info`](crate::ProcessGroup::members_info), for a pid the
/// caller holds *outside* any group (a pid saved to disk across runs, a launch
/// registry, an e2e probe watching a process from outside its container).
///
/// Returns the very fields a group member's [`MemberInfo`] carries — parent pid,
/// image name, and the start-time identity token — read through the **same**
/// per-platform readers (`/proc/<pid>/stat` on Linux, `proc_pidinfo` on macOS,
/// `Toolhelp32` + the creation `FILETIME` on Windows), with the same honest
/// `Option` policy: a field the platform can't report is `None`, never fabricated.
///
/// # The three outcomes
///
/// - **`Ok(Some(info))`** — the process exists; inspect it via
///   [`MemberInfo::ppid`], [`exe_name`](MemberInfo::exe_name), and
///   [`start_time`](MemberInfo::start_time) (each `None` where unavailable).
/// - **`Ok(None)`** — the pid names **no** process. An honest negative, *not* an
///   error: this is the "it's gone" answer a liveness check wants.
/// - **`Err`** — the process may well exist, but its state couldn't be determined:
///   the caller lacks permission to inspect it, or the OS read failed. **Never**
///   read this as "dead" — that is the whole reason it is an error rather than
///   `Ok(None)`.
///
/// # No command line
///
/// The raw argv / environment is **deliberately never** read, on any platform — a
/// command line routinely carries secrets, and redaction is the consumer's policy
/// to own (the crate's standing "never argv/env" stance, the same one
/// [`MemberInfo`] documents).
///
/// # Point-in-time
///
/// A snapshot taken now: the process may exit immediately afterwards, and the pid
/// is only as stable as the OS's reuse policy. To tell a *recycled* number apart
/// from the original process later, pair the returned
/// [`start_time`](MemberInfo::start_time) with the pid and use
/// [`process_is_alive`] — do not trust the bare number.
///
/// # Platform notes
///
/// - **Linux / Android** — one `/proc/<pid>/stat` read; world-readable for other
///   users' processes on a default mount, so a foreign process is reported. A
///   `hidepid` mount that denies the read surfaces as `Err`, not a false "gone".
/// - **Windows** — `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)` is the
///   existence/permission oracle (the least-privilege query right, grantable across
///   sessions and integrity levels for ordinary processes); ppid and image name
///   come from one system-wide `Toolhelp32` snapshot. A **protected / higher-integrity
///   process** (an anti-malware PPL, the `System` process) the caller may not query
///   yields `Err` (access denied), distinct from a non-existent pid's `Ok(None)`.
/// - **macOS** — one `proc_pidinfo(PROC_PIDTBSDINFO)` fill; a process the caller may
///   not inspect yields `Err`, a gone pid `Ok(None)`.
/// - **the bare BSDs** — no per-process reader is wired up, so existence is probed
///   with a zero-signal `kill(pid, 0)` and the pid is reported with every enriching
///   field `None` (`Ok(Some(_))`). That is a correct best-effort result, not an
///   error, and never a false "gone".
///
/// # Errors
///
/// [`Error::Io`] when the process may exist but couldn't be inspected — a
/// permission denial (a Windows protected process, a Linux `hidepid` mount, a macOS
/// restricted process) or another OS read failure.
///
/// # Examples
///
/// ```no_run
/// # fn main() -> processkit::Result<()> {
/// let pid = 4321;
/// match processkit::process_info(pid)? {
///     Some(info) => println!(
///         "pid={} ppid={:?} exe={:?} start={:?}",
///         info.pid(),
///         info.ppid(),
///         info.exe_name(),
///         info.start_time(),
///     ),
///     None => println!("pid {pid} is not running"),
/// }
/// # Ok(())
/// # }
/// ```
pub fn process_info(pid: u32) -> Result<Option<MemberInfo>> {
    crate::sys::process_info(pid).map_err(Error::Io)
}

/// Reuse-safe liveness: is the process at `pid` **still the same instance** you saw
/// earlier — the one whose [`start_time`](MemberInfo::start_time) you saved?
///
/// Pass the pid together with the `start_time` token from an earlier
/// [`process_info`] (or [`MemberInfo::start_time`]). Because the OS reuses pid
/// *numbers*, a bare pid check would answer "alive" for a stranger that recycled
/// the number after your process exited; pairing it with the start time — fixed at
/// creation and distinct for a later occupant — tells the original apart from a
/// recycled number. This is the same anti-reuse discipline the crate applies
/// internally to its own kills and stats reads, exposed for a pid you hold.
///
/// # Result
///
/// - **`Ok(true)`** — the process at `pid` exists **and** its current start time
///   matches the one you saved: your process is still running.
/// - **`Ok(false)`** — the process is gone: either the pid names nothing, or it
///   names a **different** process now (a recycled number — the start times
///   differ), which means *your* process is no longer alive.
/// - **`Err`** — the pid may name a live process but it couldn't be inspected
///   (permission denied, or an OS read failure). As with [`process_info`], never
///   read this as "dead".
///
/// # Reuse protection degrades honestly
///
/// The recycle check needs a start-time token on **both** sides. When one is
/// missing it can't *prove* a recycle, so it degrades to bare-pid liveness — a live
/// process at the number reads as `Ok(true)`:
/// - `start_time` is `None` (you saved no token — e.g. it originated on a
///   [bare BSD](process_info#platform-notes), which reports none), or
/// - the platform can't report a current token for the live process at `pid` (the
///   same structural `None`).
///
/// So on platforms that *do* report a start time (Windows, Linux, macOS), passing
/// the saved `Some(token)` gives full reuse protection; on a platform that reports
/// none, this is exactly the number-only liveness a caller would otherwise write by
/// hand — no weaker, and never a false "dead".
///
/// # Errors
///
/// [`Error::Io`] when the pid may name a live process but couldn't be inspected —
/// the same permission/OS-error surface as [`process_info`].
///
/// # Examples
///
/// ```no_run
/// # fn main() -> processkit::Result<()> {
/// // Earlier: record a process's identity.
/// let pid = 4321;
/// let saved_start = processkit::process_info(pid)?.and_then(|i| i.start_time());
///
/// // Later (perhaps after a restart): is that same process still running?
/// if processkit::process_is_alive(pid, saved_start)? {
///     println!("the original process {pid} is still alive");
/// } else {
///     println!("process {pid} is gone (exited, or its number was recycled)");
/// }
/// # Ok(())
/// # }
/// ```
pub fn process_is_alive(pid: u32, start_time: Option<u64>) -> Result<bool> {
    match process_info(pid)? {
        // No process at the number — the honest "gone" answer.
        None => Ok(false),
        // A process is present: it is the same instance iff the start-time tokens
        // agree (or the check degrades to bare-pid liveness when a token is
        // missing — see `same_process_instance`).
        Some(info) => Ok(same_process_instance(start_time, info.start_time())),
    }
}

/// Reuse-safe identity comparison for a process known to be **present** at the pid:
/// decide whether the live process is the *same instance* the caller saved.
///
/// - Both tokens known → the instances match iff they are **equal** (a difference
///   is positive proof the number was recycled by a different process).
/// - Either token `None` → a recycle cannot be *proven*, so degrade to bare-pid
///   liveness: the process at the number is live, so report `true`. This mirrors
///   the crate's internal `is_recycled` stance (`None` is never proof) and keeps
///   the bare-BSD / number-only path exactly as strong as a hand-written liveness
///   check — never a false "dead".
///
/// Pure and platform-agnostic, so the reuse discipline is unit-tested directly
/// (the modelled "same pid, different start time" case) without waiting on a real
/// pid recycle.
fn same_process_instance(expected: Option<u64>, current: Option<u64>) -> bool {
    match (expected, current) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::same_process_instance;

    #[test]
    fn matching_tokens_are_the_same_live_instance() {
        // Same pid, same start time → the original process, still alive.
        assert!(same_process_instance(Some(987_654_321), Some(987_654_321)));
    }

    #[test]
    fn differing_tokens_model_a_recycled_number() {
        // The modelled pid-reuse case (no real recycle needed): the number is live
        // but its current start time differs from the saved one, so a *different*
        // process holds it now — the saved instance is gone.
        assert!(!same_process_instance(Some(987_654_321), Some(123_456_789)));
        // Symmetric: order of the two tokens must not matter to the verdict.
        assert!(!same_process_instance(Some(123_456_789), Some(987_654_321)));
    }

    #[test]
    fn a_missing_token_degrades_to_bare_pid_liveness() {
        // No saved token (e.g. a bare-BSD origin), or the platform reports none for
        // the live process now: a recycle can't be proven, so a live process at the
        // number reads as "same/alive" — never a false "dead".
        assert!(same_process_instance(None, Some(987_654_321)));
        assert!(same_process_instance(Some(987_654_321), None));
        assert!(same_process_instance(None, None));
    }
}
