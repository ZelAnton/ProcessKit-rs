//! Shared POSIX process-group job.
//!
//! Each spawned child becomes the leader of its own process group, so signalling
//! the negative group id (`killpg`) reaps the child *and* every descendant it
//! forked. This backs three callers:
//!
//! - **Linux** — the fallback when no writable cgroup is available (e.g. a CI
//!   runner without cgroup delegation).
//! - **macOS / the BSDs other than FreeBSD** — the primary mechanism, since those
//!   targets have neither cgroups nor Job Objects.
//! - **FreeBSD** — the spawn coordination, tracked-id bookkeeping and signalling
//!   substrate underneath the `procctl` process reaper, which layers whole-tree
//!   containment on top of it (see `sys::freebsd`).
//!
//! Weaker than a cgroup or Job Object: a child that calls `setsid` starts a new
//! session and escapes the group. Wherever this module *is* the mechanism, callers
//! surface it as [`Mechanism::ProcessGroup`](crate::Mechanism::ProcessGroup) so it
//! is never a silent downgrade; on FreeBSD the reaper layer closes that escape and
//! reports [`Mechanism::ProcessReaper`](crate::Mechanism::ProcessReaper) instead.

use std::io;
use std::os::unix::process::CommandExt;
use std::sync::Mutex;
use std::time::Duration;

use tokio::process::{Child, Command};

#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

/// Best-effort read of `pid`'s OS **start-time identity token** — a value that
/// changes when the same pid/pgid *number* is reused for a different process, so
/// a recycled number can be told apart from the original process a tracked
/// [`Entry`] was bound to. Captured once at track time and re-read on every
/// probe; a live number whose current token differs from the captured one is a
/// *stranger* that recycled the number, and is treated as gone (never signalled).
///
/// `None` means "identity unknown" and is *never* treated as proof of anything —
/// a target or a read that can't produce a token degrades to the number-only
/// liveness behavior with no weakening. Availability by platform:
///
/// - **Linux / Android** — `/proc/<pid>/stat` field 22 (process start time in
///   clock ticks since boot; set at creation, stable across `exec`).
/// - **macOS / the other Apple targets** — `proc_pidinfo(PROC_PIDTBSDINFO)`'s
///   `pbi_start_tvsec`/`pbi_start_tvusec` (process creation time).
/// - **the BSDs** — *not wired up*: the start time lives in `kinfo_proc`, reached
///   only through per-OS `sysctl(KERN_PROC)` MIBs with divergent layouts
///   (FreeBSD/DragonFly `kp_start`, NetBSD's separate `kinfo_proc2`, OpenBSD's
///   element-size/count MIB) and no hosted CI runner to verify any of them.
///   Shipping an unverifiable reader whose silent miscompute would *break*
///   kill-on-drop is worse than not having one, so identity reads return `None`
///   here and every entry keeps the pre-existing number-only `group_seen`
///   behavior. The residual recycled-number window this leaves is exactly the
///   one that existed before identity tracking — no BSD regression.
///
/// Residual even where available: start-time granularity (a clock tick on Linux,
/// a microsecond on macOS) makes two processes that occupy the same number within
/// one tick indistinguishable — astronomically unlikely for a group leader (its
/// pgid is reserved by POSIX until the whole group drains, so reuse requires the
/// group to fully die first) and negligible for a solo pid.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_identity(pid: i32) -> Option<u64> {
    // `/proc/<pid>/stat` field 22 is the start time in clock ticks since boot. The
    // parse (skip past the comm's last ')', then field 22 = whitespace index 19)
    // lives in `sys::procfs`, shared with the `process_metrics` identity gate in
    // `sys/linux.rs` so the two can never disagree. Pids are always positive here,
    // so the `as u32` cast is value-preserving.
    super::procfs::read_starttime(pid as u32)
}

/// The Apple reader — see the identity-token doc above the Linux `read_identity`.
#[cfg(target_vendor = "apple")]
fn read_identity(pid: i32) -> Option<u64> {
    // `proc_pidinfo(PROC_PIDTBSDINFO)` fills a `proc_bsdinfo` whose
    // `pbi_start_tvsec`/`pbi_start_tvusec` is the process creation time (stable
    // across `exec`, distinct for a recycled pid). Fold it into microseconds.
    // SAFETY: `proc_bsdinfo` is plain-old-data (integers and byte arrays), for
    // which an all-zero bit pattern is a valid initialized value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let want = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `proc_pidinfo` writes at most `want` bytes into `info`; a valid
    // pointer and a matching buffer size are its only preconditions.
    let got = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::addr_of_mut!(info).cast::<libc::c_void>(),
            want,
        )
    };
    // A full-size fill is success; 0 / -1 (gone, EPERM) or a short read is not a
    // usable identity — report `None` so the caller defers to the liveness probe.
    if got != want {
        return None;
    }
    Some(
        info.pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    )
}

/// The BSDs (and any other unix): no wired-up reader, so identity is always
/// unknown — see the identity-token doc above the Linux `read_identity`.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn read_identity(_pid: i32) -> Option<u64> {
    None
}

/// Positive proof that the number behind a tracked entry was recycled: the
/// identity captured at track time and the one read now are *both* known and
/// they *differ*. A `None` on either side is never proof — the caller then
/// defers to the liveness probe, so a target without an identity reader (the
/// BSDs) is not weakened.
///
/// `pub(crate)` for one caller outside this module: the Linux cgroup backend's
/// bare-pid adoption, which brackets its `cgroup.procs` write with the same
/// start-time token and must decide "recycled?" by exactly this rule rather than
/// a second, subtly different comparison of its own.
pub(crate) fn is_recycled(tracked: Option<u64>, current: Option<u64>) -> bool {
    matches!((tracked, current), (Some(a), Some(b)) if a != b)
}

/// Capture the start-time identity anchor of the live process at `pid` for a
/// **bare-pid adoption** ([`ProcessGroup::adopt_external`]) — the targets that
/// have a reader ([`read_identity`]).
///
/// Unlike the best-effort [`read_identity`] this is *required* to succeed: an
/// entry created from a bare number, with no `Child` the caller keeps un-reaped
/// behind it, has nothing else that could tell the tracked process apart from a
/// later occupant of the number. So "no token" is refused here rather than
/// degraded to number-only tracking, and the two ways it can happen are told
/// apart for the caller:
///
/// - the pid names **no process** (`ESRCH` from a null-signal probe, which
///   delivers nothing) → `NotFound`, the honest negative;
/// - the process is there but its identity could not be read (a `hidepid` `/proc`
///   mount; a `proc_pidinfo` denial for another uid's process on macOS) → a plain
///   error, never mistaken for "dead".
#[cfg(all(
    feature = "process-control",
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
fn capture_adoption_anchor(pid: i32) -> io::Result<u64> {
    if let Some(token) = read_identity(pid) {
        return Ok(token);
    }
    // SAFETY: signal 0 is a sound existence probe and delivers nothing.
    if unsafe { libc::kill(pid, 0) } != 0
        && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    {
        return Err(no_such_process(pid));
    }
    Err(io::Error::other(format!(
        "cannot adopt pid {pid}: its start-time identity could not be read (a hidepid /proc \
         mount, or a proc_pidinfo denial for another uid's process on macOS), and this group \
         will not track an external process by number alone"
    )))
}

/// The BSDs (and any other unix): no start-time reader is wired up here — see the
/// identity-token doc above the Linux [`read_identity`] for why none is shipped —
/// so there is no anchor to capture and a bare-pid adoption is refused outright.
///
/// This is the deliberate line: [`adopt`](ProcessGroup::adopt) still works on
/// these targets, because the caller's own un-reaped [`Child`] is what keeps the
/// number from being recycled. A bare number has no such backing, so accepting it
/// here would mean tracking — and eventually `SIGKILL`ing — whatever process holds
/// the number at teardown time.
#[cfg(all(
    feature = "process-control",
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
fn capture_adoption_anchor(pid: i32) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "adopting pid {pid} by number needs a start-time identity reader, and none is wired \
             up on this target (the BSDs other than macOS); adopt a Child you hold instead"
        ),
    ))
}

/// The honest negative for a bare-pid adoption: the number names no process.
/// Gated with its only caller, [`capture_adoption_anchor`]'s reader-bearing arm —
/// the targets without a reader refuse before they ever look for the process.
#[cfg(all(
    feature = "process-control",
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
fn no_such_process(pid: i32) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no process with pid {pid} to adopt"),
    )
}

/// The verdict of **this backend's** bare-pid adoption when its closing identity
/// re-read shows the number was recycled while the call ran.
///
/// It can state the after-error condition flatly, which is what makes this arm the
/// fail-safe one: everything the call added is an [`Entry`] carrying the token
/// captured at the start, and [`Tracked::probe_entry`] reports any entry whose
/// number now answers with a different token as gone — so it is pruned at the next
/// sweep without ever being signalled. The one kernel-visible thing this arm can
/// have done is a `setpgid`, which the kernel permits only against a not-yet-
/// `exec`'d child of this process; where it does apply it changes which process
/// group leads the number and nothing else.
///
/// The other two backends deliberately do **not** share this wording, because their
/// states differ: the Linux cgroup arm has already moved a task between cgroups and
/// reports what its undo achieved (`sys::imp::recycled_during_cgroup_adoption`),
/// and the Windows backend cannot reach this state at all — it uses the number once,
/// for `OpenProcess`, and everything after is that kernel object.
#[cfg(feature = "process-control")]
fn recycled_during_adoption(pid: i32) -> io::Error {
    io::Error::other(format!(
        "pid {pid} was recycled while it was being adopted: its start-time identity differs \
         from the one captured at the start of the call, so the process the caller named is \
         not the one this call acted on; the entry this call left behind carries that identity, \
         so this group prunes it unsignalled rather than tearing it down"
    ))
}

/// Positive proof that `pid` names a **live, non-zombie** process — the sole state
/// for which a delivery `EPERM` in [`signal_all`](Tracked::signal_all) is surfaced
/// as a genuine "couldn't kill a live tree" failure rather than swallowed.
///
/// This discrimination is what lets the process-group teardown raise `EPERM`
/// *honestly*: on macOS/BSD `killpg` returns `EPERM` **both** for a genuinely-alive
/// uid-changed member (a `sudo`/setuid child that rejects the signal — a real
/// containment gap worth reporting) *and* for a group whose only member is an
/// unreaped **zombie** (dead, harmless — the false positive that reverted the first
/// attempt at this fix, breaking a normal shutdown of a group with unreaped
/// children). The errno alone cannot tell them apart, so we check the target's
/// actual run state after the `EPERM`. Only a *positive* live/non-zombie answer
/// surfaces the error; a zombie, a since-reaped/gone pid, or a target without a
/// state reader all report `false`, so a normal teardown is never falsely failed.
///
/// Availability mirrors [`read_identity`]:
/// - **Linux / Android** — `/proc/<pid>/stat` field 3 (state); live is any state
///   other than `Z` (zombie) or `X`/`x` (dead).
/// - **macOS / the other Apple targets** — `proc_pidinfo(PROC_PIDTBSDINFO)`'s
///   `pbi_status`; live is `SIDL`/`SRUN`/`SSLEEP`/`SSTOP`, never `SZOMB`.
/// - **the BSDs (and any other unix)** — no wired-up state reader (the same
///   per-OS `sysctl(KERN_PROC)` divergence that blocks `read_identity` there), so
///   the answer is always `false`: delivery `EPERM` keeps its pre-existing
///   swallowed behavior on those targets, exactly as before this change — no
///   regression and, crucially, no new false positive.
///
/// It classifies the tracked id itself (a group *leader* pid, or a solo pid). A
/// live uid-changed *descendant* hidden behind an already-reaped/zombie leader is
/// therefore not detected — the fail-safe direction (a missed report, never a
/// false one); the common case the first attempt tripped over — the tracked leader
/// *being* the zombie — is what this closes.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn is_live_non_zombie(pid: i32) -> bool {
    // `/proc/<pid>/stat` field 3 is the state char. A successful read that is
    // neither a zombie (`Z`) nor dead (`X`/`x`) is a live process; a failed read
    // (the pid is gone) yields `None` → `false`. Pids are positive here, so the
    // `as u32` cast is value-preserving.
    super::procfs::read_state(pid as u32).is_some_and(|s| !matches!(s, 'Z' | 'X' | 'x'))
}

/// The Apple reader — see the doc above the Linux `is_live_non_zombie`.
#[cfg(target_vendor = "apple")]
fn is_live_non_zombie(pid: i32) -> bool {
    // `proc_pidinfo(PROC_PIDTBSDINFO)` fills a `proc_bsdinfo` whose `pbi_status` is
    // the BSD run state. A full-size fill whose status is a live value
    // (SIDL/SRUN/SSLEEP/SSTOP — never SZOMB) is a genuinely-alive process; a
    // short/failed read (the pid is gone or unreadable) or SZOMB reports `false`.
    // SAFETY: `proc_bsdinfo` is plain-old-data (integers and byte arrays), for
    // which an all-zero bit pattern is a valid initialized value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let want = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `proc_pidinfo` writes at most `want` bytes into `info`; a valid
    // pointer and a matching buffer size are its only preconditions.
    let got = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::addr_of_mut!(info).cast::<libc::c_void>(),
            want,
        )
    };
    got == want
        && matches!(
            info.pbi_status,
            libc::SIDL | libc::SRUN | libc::SSLEEP | libc::SSTOP
        )
}

/// The BSDs (and any other unix): no wired-up state reader, so a delivery `EPERM`
/// is never classified as a live containment gap — see the doc above the Linux
/// `is_live_non_zombie`.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn is_live_non_zombie(_pid: i32) -> bool {
    false
}

/// Best-effort enriching metadata for one tracked leader `pid` — its ppid, short
/// image name, and start-time token — for [`ProcessGroup::members_info`]. `None`
/// means the process is gone (skip the record, never fabricate one); a `Some`
/// carries whatever fields the platform can report (each independently `Option`).
/// Availability mirrors [`read_identity`]/[`is_live_non_zombie`].
///
/// **Linux / Android** — one `/proc/<pid>/stat` read via the shared `sys::procfs`
/// parser (ppid = field 4, `comm` = field 2, start time = field 22), so the
/// fallback backend reports the *same* fields the cgroup backend does.
#[cfg(all(
    feature = "process-control",
    any(target_os = "linux", target_os = "android")
))]
fn read_member_info(pid: i32) -> Option<MemberInfo> {
    // Pids are positive here, so the `as u32` cast is value-preserving. `None` (the
    // stat read failed) means the leader is gone — skipped by the caller.
    let m = super::procfs::read_stat_meta(pid as u32)?;
    Some(MemberInfo::new(pid as u32, m.ppid, m.comm, m.starttime))
}

/// One `proc_pidinfo(PROC_PIDTBSDINFO)` fill for `pid`, distinguishing **gone**
/// from **can't look** — the single `proc_bsdinfo` read shared by the group
/// member snapshot ([`read_member_info`]) and the standalone
/// [`process_info`](crate::process_info) query, so neither carries an independent
/// copy of the syscall dance.
///
/// `Ok(Some(info))` on a full-size fill (the process exists and is readable),
/// `Ok(None)` when the errno is `ESRCH` (no such process — an honest negative),
/// and `Err` on any other failure (notably `EPERM` on a process this caller may
/// not inspect — so the standalone query never reads "not allowed to look" as
/// "dead"; the group-snapshot caller collapses both `Ok(None)` and `Err` back to
/// "skip this pid").
#[cfg(all(feature = "process-control", target_vendor = "apple"))]
fn fill_bsdinfo(pid: i32) -> io::Result<Option<libc::proc_bsdinfo>> {
    // SAFETY: `proc_bsdinfo` is plain-old-data (integers and byte arrays), for
    // which an all-zero bit pattern is a valid initialized value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let want = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `proc_pidinfo` writes at most `want` bytes into `info`; a valid
    // pointer and a matching buffer size are its only preconditions.
    let got = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::addr_of_mut!(info).cast::<libc::c_void>(),
            want,
        )
    };
    if got == want {
        return Ok(Some(info));
    }
    // A gone pid reports `ESRCH` → an honest "no such process"; `EPERM` (a
    // process we may not inspect) or any other errno is a genuine "couldn't look"
    // error, so the standalone query never mistakes an existing process for dead.
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        Ok(None)
    } else {
        Err(err)
    }
}

/// Assemble a [`MemberInfo`] from a filled `proc_bsdinfo`: `pbi_ppid`, the short
/// `pbi_comm` image name, and the creation time folded to microseconds since the
/// Unix epoch — shared by [`read_member_info`] and [`process_info`] so the field
/// mapping exists once.
#[cfg(all(feature = "process-control", target_vendor = "apple"))]
fn build_member_info(pid: u32, info: &libc::proc_bsdinfo) -> MemberInfo {
    let start_time = info
        .pbi_start_tvsec
        .saturating_mul(1_000_000)
        .saturating_add(info.pbi_start_tvusec);
    MemberInfo::new(
        pid,
        Some(info.pbi_ppid),
        comm_to_string(&info.pbi_comm),
        Some(start_time),
    )
}

/// The Apple reader — see the doc above the Linux `read_member_info`.
///
/// **macOS / the other Apple targets** — one [`fill_bsdinfo`] read (the same
/// `proc_bsdinfo` `read_identity`/`is_live_non_zombie` fill), mapped to a
/// [`MemberInfo`] by [`build_member_info`]. A gone/unreadable pid (either arm of
/// `fill_bsdinfo`'s `Ok(None)`/`Err`) collapses to `None` — a vanished leader is
/// skipped, never a fabricated record.
#[cfg(all(feature = "process-control", target_vendor = "apple"))]
fn read_member_info(pid: i32) -> Option<MemberInfo> {
    fill_bsdinfo(pid)
        .ok()
        .flatten()
        .map(|info| build_member_info(pid as u32, &info))
}

/// Identity + best-effort metadata for an **arbitrary** pid — the Apple backend of
/// the standalone [`process_info`](crate::process_info) query. Reuses the same
/// [`fill_bsdinfo`] read the group snapshot uses, but preserves its "gone vs can't
/// look" distinction: `Ok(None)` for `ESRCH`, `Err` for `EPERM`/other, `Ok(Some)`
/// otherwise. A pid that does not fit `pid_t` (`i32`) cannot name a real process,
/// so it is an honest `Ok(None)`.
#[cfg(all(feature = "process-control", target_vendor = "apple"))]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    let Ok(spid) = i32::try_from(pid) else {
        return Ok(None);
    };
    Ok(fill_bsdinfo(spid)?.map(|info| build_member_info(pid, &info)))
}

/// Identity + best-effort metadata for an **arbitrary** pid — the bare-BSD backend
/// of the standalone [`process_info`](crate::process_info) query.
///
/// No per-process introspection is wired up on the BSDs (the same
/// `sysctl(KERN_PROC)` divergence that leaves [`read_identity`] `None` there), so
/// existence is probed with a **zero-signal** `kill(pid, 0)` — which delivers
/// nothing — and the pid is reported with every enriching field honestly `None`:
/// - `0` → the process exists (and is signalable): `Ok(Some(pid, None…))`.
/// - `EPERM` → the process exists but is not ours to signal; for a read-only
///   existence query that is still a positive answer, and no restricted field is
///   being read anyway, so `Ok(Some(pid, None…))` — never a permission `Err` here
///   (there is nothing to be denied *looking* at).
/// - `ESRCH` → no such process: `Ok(None)`.
/// - any other errno → a genuine `Err`.
///
/// A pid that does not fit `pid_t` (`i32`) cannot name a real process — and casting
/// it could turn `kill` into a *process-group* signal — so it is an honest
/// `Ok(None)` before any syscall.
#[cfg(all(
    feature = "process-control",
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    let Ok(spid) = i32::try_from(pid) else {
        return Ok(None);
    };
    // SAFETY: the null signal (`0`) delivers nothing; it only probes whether the
    // pid names a live process the caller could signal.
    let rc = unsafe { libc::kill(spid, 0) };
    if rc == 0 {
        return Ok(Some(MemberInfo::new(pid, None, None, None)));
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // Exists but not ours to signal — still a positive existence answer.
        Some(libc::EPERM) => Ok(Some(MemberInfo::new(pid, None, None, None))),
        // No such process — the honest negative.
        Some(libc::ESRCH) => Ok(None),
        _ => Err(err),
    }
}

/// Decode a NUL-terminated `c_char` `comm` array (`proc_bsdinfo::pbi_comm`) into a
/// `String`, or `None` when it is empty. The kernel truncates `comm`, so this is a
/// short image name, not a path.
#[cfg(all(feature = "process-control", target_vendor = "apple"))]
fn comm_to_string(comm: &[libc::c_char]) -> Option<String> {
    let bytes: Vec<u8> = comm
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    if bytes.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// The BSDs (and any other unix): no wired-up per-process reader (the same per-OS
/// `sysctl(KERN_PROC)` divergence that blocks [`read_identity`] there), so the
/// leader — just probed live by [`Tracked::live_snapshot`] — is reported with the
/// pid known and every enriching field honestly `None`. That is a correct
/// best-effort result on a bare BSD, not an error, and never drops a live member.
#[cfg(all(
    feature = "process-control",
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
fn read_member_info(pid: i32) -> Option<MemberInfo> {
    Some(MemberInfo::new(pid as u32, None, None, None))
}

/// One tracked id (a group leader pid or a solo pid), its liveness latch, and the
/// start-time identity captured when it was first tracked (see `read_identity`).
struct Entry {
    id: i32,
    /// Latched `true` once the group probe (`kill(-id, 0)`) has succeeded — the
    /// child has called `setpgid` and the fork→exec window is closed. After that,
    /// an `ESRCH` on the group probe means the group is *genuinely gone*, so the
    /// direct-pid fallback is disabled: a reaped-and-recycled pid is pruned (and
    /// never signalled) instead of being kept alive forever, which would let
    /// `Drop`/`kill_all` SIGKILL an unrelated process that recycled the pid.
    /// Unused for solo (non-group) sets, whose probe is always a direct pid.
    group_seen: bool,
    /// Start-time identity of the tracked process captured at track time (see
    /// `read_identity`). Re-read on every probe: a live number whose current
    /// identity differs is a recycled *stranger* and is reported gone — the
    /// fail-safe that stops a signal reaching an unrelated process/group that
    /// reused the pid/pgid *without* an intervening `ESRCH` for the `group_seen`
    /// latch to catch. `None` (the BSDs, or a failed read) defers to the
    /// number-only liveness behavior, so no platform is weakened.
    identity: Option<u64>,
}

/// Which POSIX primitive one sweep send uses: `killpg(2)` for a whole process
/// group (the leader and every descendant) or `kill(2)` for a single pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalTarget {
    Group,
    Pid,
}

/// The single boundary every real signal **delivery** of the tracked sweep
/// ([`Tracked::signal_all`]) passes through — the backend's one "actually send it"
/// primitive.
///
/// Behaviorally it is the bare `killpg`/`kill` plus this backend's usual
/// `-1`→[`io::Error::last_os_error`] conversion, so the caller reads the errno from
/// the returned error instead of a separate `last_os_error()` read. Funnelling the
/// sweep's three sends through one place is what lets a `cfg(test)` rule order a
/// specific delivery to fail with a specific errno — the `EPERM` a live,
/// uid-changed member raises is otherwise reachable only on a host built to have
/// one. A faulted call never reaches the kernel, so such a test can also name a
/// signal it must not actually deliver. See the `sys::fault_injection` module (test
/// builds only, hence the bare reference — an intra-doc link to a `cfg(test)` item
/// breaks the rustdoc build).
///
/// The existence **probes** ([`Tracked::probe_raw`]) deliberately stay raw: they are
/// signal-`0` queries, not deliveries, and keeping them real means a fault-injected
/// delivery test still exercises the genuine liveness/identity gate ahead of it.
/// [`UntrackedChildGuard`]'s emergency `SIGKILL` stays raw for the same reason in
/// reverse — it is the leak backstop for a child nothing else owns yet, so it must
/// not be interposable at all.
fn deliver_signal(id: i32, sig: i32, target: SignalTarget) -> io::Result<()> {
    #[cfg(test)]
    if let Some(injected) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::PgroupSignalDelivery,
        match target {
            SignalTarget::Group => "killpg",
            SignalTarget::Pid => "kill",
        },
    ) {
        return Err(injected);
    }
    // SAFETY: killpg/kill to a probed-existing id; an exit between the probe and
    // here just yields ESRCH, which the caller classifies.
    let rc = unsafe {
        match target {
            SignalTarget::Group => libc::killpg(id, sig),
            SignalTarget::Pid => libc::kill(id, sig),
        }
    };
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// One tracked id-set with its probe/signal primitives — either process
/// **groups** (each id is a leader child's pid, probed and signalled
/// negatively: `kill(-id, 0)` / `killpg`) or **solo** pids (adopted children
/// that could not be re-grouped, probed and signalled directly).
///
/// This is the single place the recycled-pid hazard is reasoned about. A
/// stale id whose process was reaped and whose pid got recycled could address
/// an unrelated process: for a group entry the alias additionally requires
/// the recycled pid to become a group *leader*, while a solo entry is a plain
/// pid — any reuse aliases it (likelier on macOS's small pid space). The
/// mitigations are uniform for both kinds:
///
/// - bind each entry to the tracked process's start-time identity (see
///   `read_identity`) captured at track time and re-checked on every probe: a
///   live number whose current identity differs was recycled by a *stranger*, so
///   it is reported gone and never signalled ([`probe_entry`](Self::probe_entry)).
///   This is the load-bearing fail-safe — it catches a reuse even when no
///   intervening `ESRCH` was ever observed (the case the `group_seen` latch alone
///   misses: a drained group whose pgid a new leader takes, or a solo pid reused,
///   between two sweeps). Where identity is unreadable (the BSDs) the entry falls
///   back to the number-only checks below with no weakening;
/// - probe existence immediately before signalling, so the in-sweep window is
///   a few instructions wide;
/// - prune on `ESRCH` and never re-add a pruned id — an empty group can never
///   regain members (new members only fork from existing ones), so the probe
///   is terminal and a recyclable dead id is forgotten promptly (and, once the
///   group has been seen alive, the [`group_seen`](Entry::group_seen) latch
///   disables the direct-pid fallback so a recycled pid is never revived);
/// - treat `EPERM` as **exists**: the process/group is alive but may not be
///   signalled (e.g. after a third-party uid change) — pruning it would
///   silently orphan a live tree, so it is kept and signalled best-effort.
///
/// A tracked id stays until its process is *reaped* — an unreaped zombie
/// probes alive (relevant for adopted children, which the caller reaps).
struct Tracked {
    ids: Mutex<Vec<Entry>>,
    /// Probe/signal the whole process group (negative id) instead of one pid.
    group: bool,
}

impl Tracked {
    const fn new(group: bool) -> Self {
        Tracked {
            ids: Mutex::new(Vec::new()),
            group,
        }
    }

    /// Core liveness probe for `id` given the entry's latch state `group_seen`.
    /// Returns `(alive, group_seen_after)`. See [`Entry::group_seen`] and the
    /// type doc for the direct-pid fallback rule and why the latch disables it.
    fn probe_raw(&self, id: i32, group_seen: bool) -> (bool, bool) {
        let probe = if self.group { -id } else { id };
        // SAFETY: signal 0 is a sound existence probe (a negative target
        // probes the process group).
        if unsafe { libc::kill(probe, 0) } == 0 {
            // Alive. For a group, latch: the leader exists, so it has `setpgid`'d
            // and the fork→exec window is closed.
            return (true, group_seen || self.group);
        }
        let err = std::io::Error::last_os_error().raw_os_error();
        if err == Some(libc::EPERM) {
            // Alive but unsignallable — keep it (pruning would orphan a live tree).
            return (true, group_seen || self.group);
        }
        // Group-mode ESRCH on the negative group-id does not prove the process is
        // gone *while the group has never been seen alive*: a just-forked child
        // may not have called setpgid(0,0)/setsid yet (the between-fork-and-exec
        // window, reachable right after *any* spawn until the first successful
        // group probe — every spawn seeds the latch `false`), so fall back to a
        // direct pid probe rather than permanently prune a still-live entry. ONCE
        // `group_seen` has latched, the child long since `setpgid`'d, so an ESRCH
        // means the group genuinely drained — do NOT fall back, or a direct probe
        // would keep a reaped-and-recycled pid alive forever. `signal_all` mirrors
        // this latch-gated fallback.
        if self.group && !group_seen && err == Some(libc::ESRCH) {
            // SAFETY: probing pid directly; EPERM means alive-but-unsignallable.
            if unsafe { libc::kill(id, 0) } == 0 {
                return (true, false);
            }
            let alive = std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
            return (alive, false);
        }
        (false, group_seen)
    }

    /// Probe a stored entry, updating its [`group_seen`](Entry::group_seen) latch
    /// and gating the liveness verdict through the entry's start-time identity.
    ///
    /// The identity gate is the fail-safe that closes the recycled-number hazard
    /// the `group_seen` latch alone cannot: the latch only catches a reuse the
    /// code observed an `ESRCH` for *before* the number was recycled, but a group
    /// that drained and whose pgid an unrelated new leader then took (or a solo
    /// pid recycled to any process) between two sweeps still probes alive.
    /// Re-reading the identity and comparing it to the one captured at track time
    /// detects that positively — a live number with a *different* identity is a
    /// stranger, so we report it gone and it is pruned (never signalled). Only a
    /// positive mismatch prunes; an unknown identity on either side (the BSDs, or
    /// a group whose leader was reaped while its descendants live and keep the
    /// group — and its pgid — alive) defers to the liveness verdict, preserving
    /// descendant containment and not weakening any platform.
    ///
    /// Placed here, at the single probe choke point every sweep funnels through
    /// (`track` / `signal_all` / `any_alive` / `live_snapshot` / `count_alive`),
    /// the check runs a few instructions before the matching `kill`/`killpg` in
    /// `signal_all`, so cross-sweep reuse is closed and only the irreducible
    /// in-sweep instruction window remains (POSIX offers no atomic
    /// probe-and-signal for a whole *group*; that narrow window is unchanged from
    /// before this hardening).
    fn probe_entry(&self, entry: &mut Entry) -> bool {
        let (alive, group_seen) = self.probe_raw(entry.id, entry.group_seen);
        entry.group_seen = group_seen;
        if alive && entry.identity.is_some() && is_recycled(entry.identity, read_identity(entry.id))
        {
            // Positively recycled: the number is alive but names a different
            // process than the one tracked — fail-safe, report gone so it is
            // pruned and never signalled. (The `is_some` guard skips the identity
            // read entirely when there is no captured token to compare against —
            // the BSDs, or a track-time read that failed.)
            return false;
        }
        alive
    }

    /// Whether this set already tracks **the very process instance** `id` names,
    /// `identity` being the start-time token just read for it — the de-dup gate
    /// [`ProcessGroup::adopt`] asks before adding a solo entry for a pid that may
    /// already be tracked here as a group leader. Identity-anchored external
    /// adoption uses [`prepare_identity_adoption`](Self::prepare_identity_adoption)
    /// instead, because it must also remove stale same-pid group entries before
    /// publishing the solo entry.
    ///
    /// Two properties carry the weight, and a plain membership scan has neither:
    ///
    /// - **It sweeps before it answers.** Entries leave this set only during a sweep
    ///   ([`probe_entry`](Self::probe_entry)); nothing deregisters one when the
    ///   process behind it is reaped, so from that reap until the next sweep the set
    ///   still holds the bare *number*. Answering from that stale entry would let it
    ///   speak for a number the OS has since handed to somebody else, and the
    ///   adoption of the new holder would be skipped — reporting containment while
    ///   tracking nothing at all. Sweeping first is the same prune-then-decide order
    ///   [`track_with`](Self::track_with) uses, and the sweep's identity gate is
    ///   what drops the stale entry.
    /// - **It compares identities, not numbers.** A surviving entry de-dups only
    ///   when its token is the one just captured for this pid. That closes the
    ///   residue the sweep cannot: an entry carrying *no* token (a track-time read
    ///   that failed) is un-prunable by identity, probes alive on the number alone,
    ///   and would otherwise silently answer for whoever holds the number now.
    ///   Two entries that cannot be proven to be the same instance are therefore
    ///   treated as different — over-tracking (a pid listed twice, a signal
    ///   delivered twice, both harmless) rather than reporting containment that was
    ///   never established. Where neither side has a token at all (the BSDs), the
    ///   comparison degrades to the number-only de-dup that has always applied
    ///   there, with no change.
    #[cfg(feature = "process-control")]
    fn holds_same_process(&self, id: i32, identity: Option<u64>) -> bool {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));
        ids.iter().any(|e| e.id == id && e.identity == identity)
    }

    /// Reconcile an identity-anchored external adoption without adding an entry.
    ///
    /// The failed-`setpgid` path moves a pid from the group table's semantics to
    /// the solo table's semantics. A stale group entry with no identity cannot be
    /// pruned by [`probe_entry`](Self::probe_entry), so merely asking
    /// [`holds_same_process`](Self::holds_same_process) whether the group already
    /// owns this process would leave that bare pid in the table beside the fresh
    /// solo entry. Remove every same-pid entry that does not carry this anchor,
    /// retain one matching entry for the same-process fast path, and report whether
    /// that matching entry was already present. The caller publishes the solo entry
    /// only after this cleanup, so a broadcast cannot observe both representations.
    #[cfg(feature = "process-control")]
    fn prepare_identity_adoption(&self, id: i32, identity: u64) -> bool {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));

        let mut same_process = false;
        ids.retain(|entry| {
            if entry.id != id {
                return true;
            }
            if entry.identity == Some(identity) {
                if same_process {
                    false
                } else {
                    same_process = true;
                    true
                }
            } else {
                false
            }
        });
        same_process
    }

    /// Track `id`, pruning drained entries and de-duplicating (re-adopting a
    /// child this set already tracks must not make `members()`/`stats()`
    /// over-report). `group_seen` seeds the latch: `true` only when *this process
    /// itself* created the group synchronously — a successful `adopt`, whose
    /// `setpgid` the parent ran before this call. Every `spawn` seeds `false`: the
    /// child runs its own `setpgid`/`setsid` after fork, so the group is not proven
    /// to exist until the first successful probe latches it, and the direct-pid
    /// fallback must stay armed across the not-yet-`setpgid`'d window (for the
    /// non-`setsid` fork path too — see `ProcessGroup::spawn`).
    fn track(&self, id: i32, group_seen: bool) {
        // Capture the start-time identity now, while `id` is freshly live, so a
        // later probe can tell the tracked process apart from any process that
        // recycles the number.
        self.track_with(id, group_seen, read_identity(id));
    }

    /// [`track`](Self::track) with the start-time identity supplied by the caller
    /// instead of read here.
    ///
    /// One body serves both, so the pruning and de-dup rules cannot drift between
    /// them. The caller-supplied form exists for
    /// [`adopt_external`](ProcessGroup::adopt_external), whose whole contract is
    /// that the token was captured *before* the adoption's own syscalls rather than
    /// after them: re-reading it here could hand back `None` for a process that
    /// exited in between, leaving a number-only entry on a path that promises an
    /// anchored one. A matching anchor preserves the existing entry; a different
    /// anchor replaces same-pid entries, including an unanchored stale record that
    /// cannot be pruned by the identity gate. With no anchor, the existing
    /// number-only de-dup remains in effect for targets without an identity reader.
    fn track_with(&self, id: i32, group_seen: bool, identity: Option<u64>) {
        // Recover a poisoned lock instead of dropping the child from tracking,
        // which would void the kill-on-drop guarantee.
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));

        if let Some(identity) = identity {
            // A number-only entry cannot prove that it is this identity: it may
            // be the residue of a process that was reaped before the number was
            // recycled. Replace every same-pid entry that is not this anchor so a
            // stale solo record cannot mask a fresh adoption or keep a bare pid
            // alive beside its identity-anchored replacement.
            let mut same_process = false;
            ids.retain(|entry| {
                if entry.id != id {
                    return true;
                }
                if entry.identity == Some(identity) {
                    if same_process {
                        false
                    } else {
                        same_process = true;
                        true
                    }
                } else {
                    false
                }
            });
            if same_process {
                return;
            }
        } else if ids.iter().any(|entry| entry.id == id) {
            // Preserve the long-standing number-only de-dup on targets without
            // an identity reader. If an anchored entry already exists but this
            // best-effort read failed, retaining the anchored entry also avoids
            // downgrading the protection it already provides.
            return;
        }

        ids.push(Entry {
            id,
            group_seen,
            identity,
        });
    }

    /// Send `sig` to every still-existing entry, pruning the drained ones.
    ///
    /// Each entry is identity-gated by [`probe_entry`](Self::probe_entry) a few
    /// instructions before its `kill`/`killpg`, so a number recycled by a stranger
    /// since it was tracked is pruned here rather than signalled — the delivery
    /// only ever reaches an id whose identity was just re-verified (or, on a
    /// target/path without a readable identity, whose bare liveness was).
    ///
    /// Returns `Err` when a send **honestly failed**: an `EINVAL` (a bad signal
    /// number — the request itself is malformed, so it is surfaced whatever the
    /// target's state; symmetric with the cgroup backend's `signal`) or a delivery
    /// `EPERM` that hit a positively **live, non-zombie** member
    /// ([`is_live_non_zombie`]) — the genuine containment gap (a `sudo`/setuid child
    /// that rejects the signal). Every other outcome is `Ok`, including an `ESRCH`
    /// (the target already exited) and the ambiguous `EPERM` this backend used to
    /// swallow wholesale:
    /// on macOS/BSD `killpg` returns `EPERM` for a group whose only member is an
    /// unreaped **zombie** (dead, harmless) too, and surfacing *that* is what
    /// reverted the first attempt at this fix (it falsely failed a normal
    /// `kill_all`/`shutdown` of a group with unreaped children). By checking the
    /// target's run state after the `EPERM`, the harmless zombie case — and a
    /// since-reaped pid, and every target without a state reader (the BSDs) — stays
    /// `Ok`, while a genuinely-alive rejecting member is reported. The sweep always
    /// visits every entry before returning, so one member's live-`EPERM` never
    /// skips signalling the rest of the tree. The best-effort callers (`Drop` and
    /// `GracefulTarget::signal_all`) consume the result without returning an I/O
    /// error; explicit `kill_all`/`hard_kill`/`signal`/`suspend`/`resume` calls
    /// propagate it.
    fn signal_all(&self, sig: i32) -> io::Result<()> {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        // The first *surfaceable* send error seen this sweep, returned after every
        // entry has been signalled (a partial failure must not skip the rest): an
        // `EINVAL` (a bad signal number — the request is malformed, so it surfaces
        // whatever the target's state) or a live-non-zombie `EPERM` (a uid-changed
        // member that genuinely rejects the signal). Every other outcome — `ESRCH`,
        // a harmless zombie-only `EPERM`, a since-reaped pid, a target without a
        // state reader (the BSDs) — stays swallowed.
        let mut surfaced: Option<io::Error> = None;
        ids.retain_mut(|e| {
            if !self.probe_entry(e) {
                return false; // gone — forget it.
            }
            let id = e.id;
            // Every send goes through `deliver_signal` (the sweep's one delivery
            // primitive); an exit between the probe and here just yields ESRCH and
            // the sweep continues.
            let delivery = if self.group {
                // killpg reaches the leader and every descendant. While the
                // group has never been seen alive (a forked-but-not-yet-
                // `setpgid`'d child), killpg yields ESRCH; fall back to a
                // direct pid signal so the entry drains. ONCE `group_seen`
                // latched (`probe_entry` set it above), an ESRCH means the
                // group is genuinely gone — do NOT direct-signal, or that
                // would SIGKILL a process that recycled the pid.
                match deliver_signal(id, sig, SignalTarget::Group) {
                    Ok(()) => None,
                    Err(err) if err.raw_os_error() == Some(libc::ESRCH) && !e.group_seen => {
                        // Direct-pid fallback: report its own failure, if any.
                        deliver_signal(id, sig, SignalTarget::Pid).err()
                    }
                    Err(err) => Some(err),
                }
            } else {
                deliver_signal(id, sig, SignalTarget::Pid).err()
            };
            // Surface a real send failure — an `EINVAL` (a malformed request: a bad
            // signal number, which fails uniformly for every target) or an `EPERM`
            // against a positively live, non-zombie process (the genuine "couldn't
            // signal it" case). A zombie-only group's `killpg` `EPERM`, an `EPERM`
            // against a since-reaped pid, or a target without a state reader (the
            // BSDs) all classify as not-live and are swallowed — the fail-safe that
            // keeps a normal teardown succeeding — and an `ESRCH` (the target is
            // already gone) is likewise swallowed. The `is_live_non_zombie` probe
            // runs only on the rare `EPERM` path (an `EINVAL` short-circuits before
            // it).
            if let Some(err) = delivery
                && surfaced.is_none()
            {
                let code = err.raw_os_error();
                if code == Some(libc::EINVAL)
                    || (code == Some(libc::EPERM) && is_live_non_zombie(id))
                {
                    surfaced = Some(err);
                }
            }
            true
        });
        match surfaced {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Whether any tracked entry still exists.
    fn any_alive(&self) -> bool {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.iter_mut().any(|e| self.probe_entry(e))
    }

    /// The still-existing entries, pruning the drained ones on the way.
    #[cfg(feature = "process-control")]
    fn live_snapshot(&self) -> Vec<i32> {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));
        ids.iter().map(|e| e.id).collect()
    }

    /// Drop a just-spawned entry whose child a higher-level constructor is
    /// rolling back, **after** that child's tree has already been killed (see
    /// [`ProcessGroup::rollback_pty_spawn`]). Bookkeeping only, deliberately: the
    /// tracked id names a *shared* process group, so anything broader here would
    /// broadcast to members this failed spawn does not own.
    #[cfg(feature = "pty")]
    fn remove(&self, id: i32) {
        // Rollback runs from a synchronous `Drop`, where a poisoned tracker is
        // unactionable — recover it rather than panic mid-teardown.
        self.ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|entry| entry.id != id);
    }

    /// How many tracked entries still exist (probe-only; no pruning — `stats` and
    /// the graceful teardown report's before/after member counts must not mutate the
    /// *set* of tracked ids, though it may refresh the `group_seen` latch, which is a
    /// benign monotonic cache). Un-gated: the always-available graceful driver reads
    /// it through [`ProcessGroup`]'s
    /// [`GracefulTarget::alive_count`](crate::sys::graceful::GracefulTarget::alive_count)
    /// as well as the
    /// `stats`-gated `stats()`.
    fn count_alive(&self) -> usize {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        let mut alive = 0;
        for e in ids.iter_mut() {
            if self.probe_entry(e) {
                alive += 1;
            }
        }
        alive
    }
}

/// A set of process groups, one per spawned (or adopted) child.
///
/// Tracks the group ids (each == its leader child's pid) so teardown can signal
/// them. Its [`Drop`] hard-kills every still-live group, so an exiting or
/// panicking owner never leaks subprocesses.
pub(crate) struct ProcessGroup {
    /// Group ids we own. A group id is the leader child's pid.
    groups: Tracked,
    /// Adopted children that could not be re-grouped: POSIX forbids
    /// `setpgid` on a child that has already `exec`'d (`EACCES`) — the common
    /// case for [`adopt`](Self::adopt). These are tracked and signalled
    /// *individually*: the child itself is contained, but unlike a group
    /// leader, descendants it forks are not.
    solos: Tracked,
    /// Set by `graceful_shutdown(escalate=false)` to tell `Drop` not to
    /// hard-kill survivors (the caller deliberately chose not to escalate).
    skip_drop_kill: super::SkipDropKill,
}

impl ProcessGroup {
    pub(crate) fn new() -> Self {
        ProcessGroup {
            groups: Tracked::new(true),
            solos: Tracked::new(false),
            skip_drop_kill: super::SkipDropKill::new(),
        }
    }

    /// Undo the registration [`spawn`](Self::spawn) made, when the PTY setup that
    /// follows it fails before the child can be handed to a `RunningProcess`.
    ///
    /// **Kill first, forget second.** [`hard_kill_fresh_spawn`] reaches this spawn's
    /// whole process group (the pty child is a session leader, so its pgid is its
    /// pid) while the entry is still tracked, and only then is the pid dropped. The
    /// kill itself is aimed by the pid passed in rather than by the tracked set, so
    /// the order is not what makes it land; what the order preserves is the state
    /// this group can still act on if the kill does *not* land, since a tracked id
    /// is what [`Tracked::signal_all`] later sweeps.
    ///
    /// Dropping the id afterwards is right *here* and would be wrong on the FreeBSD
    /// reaper (see `freebsd::Job::rollback_pty_spawn`), because the two ids are not
    /// alike: after `killpg` this one can only still reach a member that refused the
    /// signal, while a stale **pgid** is this platform's sharpest recycling hazard —
    /// it can come to name a process group of an unrelated process, which a reaper
    /// root (always within this process's own tree) never can.
    ///
    /// The reach is `killpg`'s — this mechanism's own whole-tree maximum, and a
    /// superset of the per-child teardown a successful run would get. A descendant
    /// that calls `setsid` itself escapes it, exactly as it escapes `kill_all` /
    /// `shutdown` / `signal` on this backend (the documented
    /// [`Mechanism::ProcessGroup`](crate::Mechanism::ProcessGroup) limit).
    ///
    /// The caller still owns the child's un-reaped `Child`, so `pid` cannot have
    /// been recycled between the spawn and this call.
    ///
    /// **Then the kill-on-drop backstop.** `displaced` is the spare this spawn's own
    /// re-arm took away (see [`spawn_displacing_spare`](Self::spawn_displacing_spare)):
    /// putting it back returns the latch to the state a
    /// `graceful_shutdown(escalate = false)` had left it in, so a launch that failed
    /// after its child existed does not hand `Drop` a licence to kill survivors the
    /// caller deliberately spared. It takes only while no other `spawn`/`adopt` has
    /// re-armed the backstop since — that newcomer wins and stays killable (see
    /// [`SkipDropKill::restore`](super::SkipDropKill::restore)).
    #[cfg(feature = "pty")]
    pub(crate) fn rollback_pty_spawn(&self, pid: u32, displaced: super::DisplacedSpare) {
        hard_kill_fresh_spawn(pid as i32);
        self.groups.remove(pid as i32);
        self.skip_drop_kill.restore(displaced);
    }

    // The plain shape, for a backend that only wants the child: `sys::unix`
    // (macOS/the other BSDs) and `sys::freebsd` call it. Linux does not — its `Job`
    // routes both of its arms through `spawn_displacing_spare`, so the cgroup arm
    // and this fallback re-arm the backstop through one body — which leaves this
    // method dead on the Linux `--lib` build alone (the K-092 asymmetric-backend
    // shape). Allowed on exactly that target rather than pushing a tuple onto every
    // caller that has no use for one.
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        // The spare the re-arm below displaces interests only a launch that can
        // still be undone; a spawn that hands back a live child never puts it back.
        self.spawn_displacing_spare(cmd, opts)
            .map(|(child, _displaced)| child)
    }

    /// [`spawn`](Self::spawn), also handing back the [`DisplacedSpare`](super::DisplacedSpare)
    /// its kill-on-drop re-arm took away — the token
    /// [`rollback_pty_spawn`](Self::rollback_pty_spawn) needs if the PTY setup that
    /// follows this spawn fails. One body serves both, so the re-arm cannot drift
    /// between the plain and the undoable launch path.
    pub(crate) fn spawn_displacing_spare(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<(Child, super::DisplacedSpare)> {
        // Own process group per child → killpg reaps it and its descendants.
        // `process_group(0)` == setpgid(0, 0): the child becomes its own group
        // leader. EXCEPT when the command carries a `setsid()` pre-exec hook:
        // std applies setpgid *before* pre-exec hooks, and setsid fails EPERM
        // for a process that is already a group leader — so skip setpgid and
        // let setsid create the session + group (pgid == pid). The tracking
        // below is identical either way.
        if !opts.setsid {
            cmd.as_std_mut().process_group(0);
        }
        // Guard the window between a live child and its registration in `groups`:
        // until `track` records it, nothing owns its teardown, so an early return
        // or panic here would leak a live self-grouped child — a silent
        // kill-on-drop violation. The guard hard-kills the not-yet-tracked child
        // on unwind and is disarmed once tracking succeeds; it is the pgroup
        // analogue of the Windows backend's `UncontainedChildGuard`. Today the
        // steps between spawn and `track` are infallible, but the guard keeps that
        // fragile invariant from silently regressing if a fallible step is ever
        // inserted here.
        let guard = UntrackedChildGuard::arm(cmd.spawn()?);
        if let Some(pid) = guard.child().id() {
            // Seed the liveness latch `false` on *every* spawn — the child runs
            // its own `setpgid(0, 0)` (or `setsid`) after fork, so the group is
            // not proven to exist until the first successful group probe latches
            // it. Seeding `true` for a non-`setsid` spawn would be safe only on
            // the posix_spawn fast path (setpgid applied atomically before the pid
            // is returned); with a `pre_exec` hook std falls back to
            // fork→setpgid→exec, and a group probe in the not-yet-`setpgid`'d
            // window would ESRCH and — with the latch wrongly seeded `true` —
            // wrongly prune (and never signal) the live child. `false` keeps the
            // direct-pid fallback armed until the group is first seen, matching
            // the `setsid` path. `adopt`, whose `setpgid` the parent itself runs
            // synchronously before tracking, still seeds `true`.
            self.groups.track(pid as i32, false);
        }
        // Re-arm the kill-on-drop backstop now that a child has actually joined
        // and been tracked: a prior graceful_shutdown(escalate=false) latched
        // skip_drop_kill to spare survivors; a fresh member must not be spared by
        // that stale latch. Done *after* tracking (and after spawn) so a failed
        // spawn — whose guard reaps the child, adding no member — leaves the
        // spared survivors untouched. What the re-arm displaced travels back with
        // the child, for the one caller that may still have to undo this spawn.
        let displaced = self.skip_drop_kill.clear();
        Ok((guard.disarm(), displaced))
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("child has no pid (already exited?)"))?
            as i32;
        // Try to make the external child its own group leader. Only the child
        // itself is moved — already running descendants keep their group.
        // SAFETY: setpgid on a live pid is a sound call.
        let rc = unsafe { libc::setpgid(pid, 0) };
        if rc == 0 {
            // It now leads group `pid` — track the group; future forks inherit
            // it and are reaped with it. The group exists (setpgid succeeded), so
            // seed the latch true. `track` de-duplicates a re-adopt.
            // A new killable member joined — re-arm Drop's backstop so a prior
            // graceful_shutdown(escalate=false) latch doesn't spare it.
            self.skip_drop_kill.clear();
            self.groups.track(pid, true);
            return Ok(());
        }

        let err = io::Error::last_os_error();
        match err.raw_os_error().unwrap_or(0) {
            // The child already exited — nothing to contain.
            code if code == libc::ESRCH => Ok(()),
            // POSIX forbids re-grouping a child once it has `exec`'d (EACCES) —
            // the NORMAL case for adopting a running process — and a session
            // leader / cross-session child can't be moved either (EPERM).
            // Recording `pid` as a *group* id would make teardown a silent
            // no-op (no group `pid` exists); track it individually instead:
            // the child is contained, its future forks are not.
            code if code == libc::EACCES || code == libc::EPERM => {
                // A child THIS group already spawned is already tracked as a group
                // leader; its `setpgid` fails EACCES because it has exec'd. Don't
                // also solo-track it (that would double-count in `members()`/
                // `stats()` and double-deliver every broadcast) — only solo-track a
                // genuinely external child.
                //
                // The de-dup asks whether the tracked entry is this very process
                // instance ([`Tracked::holds_same_process`]), not merely whether the
                // number is on the books. The caller's un-reaped `Child` keeps *this*
                // number from being recycled during the call, but it says nothing
                // about an entry left over from an *earlier* process that held the
                // same number and was reaped — precisely how the number became free
                // for this child. That stale entry answering the de-dup would skip
                // the tracking and report containment the group does not have, so
                // the identity read here (once, and handed to both the check and the
                // entry, so they cannot disagree) is what it is decided on.
                let identity = read_identity(pid);
                if !self.groups.holds_same_process(pid, identity) {
                    // A new killable solo member joined — re-arm Drop's backstop.
                    self.skip_drop_kill.clear();
                    self.solos.track_with(pid, false, identity);
                }
                Ok(())
            }
            _ => Err(err),
        }
    }

    /// Adopt an **external** process named only by `pid` — the backend of
    /// [`ProcessGroup::adopt_external`](crate::ProcessGroup::adopt_external).
    ///
    /// Same three steps as [`adopt`](Self::adopt), with the identity work a bare
    /// number needs wrapped around them:
    ///
    /// 1. **Anchor first.** [`capture_adoption_anchor`] reads the process's
    ///    start-time token *before* anything is tracked. It doubles as the
    ///    existence check — a token can only be read for a process that is there —
    ///    so a number that names nothing is refused here, not later, and a target
    ///    without a readable token is refused rather than tracked by number alone.
    /// 2. **`setpgid`, then track.** Unchanged from [`adopt`](Self::adopt) except
    ///    for how `ESRCH` is read. For a `Child` an `ESRCH` means "already exited";
    ///    for a bare pid it is the *ordinary* answer for a process that is not this
    ///    process's own child at all (the kernel reports `ESRCH` for a `setpgid`
    ///    target that is neither the caller nor one of its children), which is the
    ///    normal case here — and step 1 has just proved the process exists. So it
    ///    joins `EACCES`/`EPERM` on the solo-tracking path instead of being read as
    ///    "gone".
    /// 3. **Re-read the anchor.** A token that has positively changed means the
    ///    number was recycled inside this call's own window, so no claim on the
    ///    process the caller named was established; that is reported rather than
    ///    passed off as containment. The entry pushed in step 2 is deliberately
    ///    *left in place*: its captured token no longer matches the number, so
    ///    [`Tracked::probe_entry`] reports it gone and prunes it at the next sweep
    ///    without ever signalling it — dropping bookkeeping on an unproven guess is
    ///    the direction that loses a live member, not this one.
    ///
    /// From step 2 on, every probe, signal and teardown for the entry runs through
    /// the same identity gate the rest of this module uses ([`Tracked::probe_entry`]),
    /// and — unlike an [`adopt`](Self::adopt)ed entry, whose token is best-effort —
    /// this entry's token is always present, because step 1 refuses the adoption
    /// otherwise.
    #[cfg(feature = "process-control")]
    pub(crate) fn adopt_external(&self, pid: u32) -> io::Result<()> {
        // A number that does not fit `pid_t` cannot name a process — and casting it
        // would turn the `kill` probes below into *process-group* signals.
        let Ok(pid) = i32::try_from(pid) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no process with pid {pid} to adopt"),
            ));
        };
        let anchor = capture_adoption_anchor(pid)?;
        // Try to make the external process its own group leader. Only the process
        // itself is moved — already running descendants keep their group.
        // SAFETY: setpgid on a pid is a sound call, and it can never re-group *us*:
        // `ProcessGroup::adopt_external` (src/group.rs) refuses pid 0 and this
        // process's own pid before any backend is reached — the one guard for that,
        // and the reason this line does not repeat the check.
        let rc = unsafe { libc::setpgid(pid, 0) };
        if rc == 0 {
            // It now leads group `pid` — track the group; future forks inherit it
            // and are reaped with it. The group exists (setpgid succeeded), so seed
            // the latch true. A new killable member joined — re-arm Drop's backstop
            // so a prior graceful_shutdown(escalate=false) latch doesn't spare it.
            self.skip_drop_kill.clear();
            self.groups.track_with(pid, true, Some(anchor));
        } else {
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                // ESRCH: not this process's child (the normal answer for a truly
                // foreign process — and NOT proof it is gone, which the anchor
                // above has just disproved). EACCES: it has already `exec`'d.
                // EPERM: it is a session leader, or lives in another session.
                // Recording `pid` as a *group* id would make teardown a silent
                // no-op (no group `pid` exists); track it individually instead:
                // the process is contained, its future forks are not.
                code if code == libc::EACCES || code == libc::EPERM || code == libc::ESRCH => {
                    // A process this group already spawned is tracked as a group
                    // leader; re-adopting it by number must not also solo-track it
                    // (that would double-count in `members()`/`stats()` and
                    // double-deliver every broadcast). The de-dup is decided on the
                    // anchor this call just captured, against the tracked entry's own
                    // token and only after a sweep. The reconciliation also removes
                    // same-pid group entries that cannot prove this identity,
                    // including a stale bare-pid entry, before the fresh solo entry
                    // is published. A bare "is the number on the books" test would
                    // otherwise let an entry whose process was reaped, and whose
                    // number the OS has since been reassigned, skip the tracking and
                    // return an `Ok` that contains nothing
                    // ([`Tracked::prepare_identity_adoption`]).
                    if !self.groups.prepare_identity_adoption(pid, anchor) {
                        self.skip_drop_kill.clear();
                        self.solos.track_with(pid, false, Some(anchor));
                    }
                }
                _ => return Err(err),
            }
        }
        if is_recycled(Some(anchor), read_identity(pid)) {
            return Err(recycled_during_adoption(pid));
        }
        Ok(())
    }

    /// Hard-kill every tracked group and solo child. Surfaces a genuine delivery
    /// `EPERM` against a live, non-zombie member (a `sudo`/setuid child that
    /// rejects `SIGKILL`) — see [`Tracked::signal_all`] — while a harmless
    /// zombie-only group's `EPERM` stays `Ok`.
    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.broadcast(libc::SIGKILL)
    }

    /// Broadcast `sig` to every tracked process group and solo-adopted child,
    /// reporting an **honest** send failure — an `EINVAL` (a bad signal number) or
    /// an `EPERM` against a live, non-zombie member that rejects it — as `Err`,
    /// symmetric with the cgroup backend (see [`Tracked::signal_all`]). An entry
    /// that already drained (`ESRCH`) is skipped and pruned, a harmless zombie-only
    /// `EPERM` stays swallowed, and an empty / all-drained set is a no-op `Ok`.
    ///
    /// Signal `0` (`Signal::Other(0)`) is the POSIX existence probe: it delivers
    /// **nothing**, so a returned `Ok` here means "the probe reached a signalable
    /// live target", not "a signal was delivered". (See the honesty contract on
    /// [`ProcessGroup::signal`](crate::ProcessGroup::signal).)
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: i32) -> io::Result<()> {
        self.broadcast(sig)
    }

    /// Freeze every tracked group (`SIGSTOP` — unblockable, idempotent).
    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.broadcast(libc::SIGSTOP)
    }

    /// Thaw every tracked group (`SIGCONT`).
    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.broadcast(libc::SIGCONT)
    }

    /// One signal sweep over both tracking sets. Both sets are always signalled;
    /// the first surfaceable send error either raises — an `EINVAL` (a bad signal
    /// number) or a live-non-zombie `EPERM` — is returned (see
    /// [`Tracked::signal_all`]). The best-effort callers (`Drop` and
    /// `GracefulTarget::signal_all`) consume the result; explicit control operations
    /// propagate it.
    fn broadcast(&self, sig: i32) -> io::Result<()> {
        let groups = self.groups.signal_all(sig);
        let solos = self.solos.signal_all(sig);
        // `and` keeps the groups error if present, else the solos one — both sweeps
        // ran regardless, so no set is skipped by the other's failure.
        groups.and(solos)
    }

    /// Whether anything tracked is still alive.
    fn any_alive(&self) -> bool {
        self.groups.any_alive() || self.solos.any_alive()
    }

    /// The live tracked group **leaders** (one pid per spawned child) plus the
    /// solo-adopted pids — descendants inside the groups are not enumerated
    /// here. Dead entries are pruned on the way.
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> Vec<i32> {
        let mut members = self.groups.live_snapshot();
        members.extend_from_slice(&self.solos.live_snapshot());
        members
    }

    /// The tracked leaders (plus solo-adopted pids) of [`members`](Self::members),
    /// enriched with best-effort per-platform metadata via [`read_member_info`]. A
    /// leader that vanished between the live probe and its metadata read is skipped
    /// where the platform can tell (Linux `/proc`, Apple `proc_pidinfo`); on the
    /// bare BSDs — no reader wired up — each live leader is reported with the pid
    /// known and every enriching field `None`.
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> Vec<MemberInfo> {
        self.members()
            .into_iter()
            .filter_map(read_member_info)
            .collect()
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<super::graceful::GracefulOutcome> {
        super::graceful::run(self, &self.skip_drop_kill, signal, timeout, escalate).await
    }

    /// This group's "don't kill on `Drop`" latch, for a backend that **wraps**
    /// `ProcessGroup` and drives the shared graceful loop against its own
    /// [`GracefulTarget`](super::graceful::GracefulTarget) rather than this one —
    /// today only the FreeBSD reaper backend (`sys::freebsd`), whose teardown must
    /// reach descendants a `killpg` cannot see.
    ///
    /// Handing back the *same* latch (rather than the wrapper owning a second one)
    /// is what keeps the spare coherent: a `graceful_shutdown(escalate = false)`
    /// driven by the wrapper must suppress **both** the wrapper's reaper kill and
    /// this `ProcessGroup`'s own `Drop` backstop, and a later `spawn`/`adopt` here
    /// must re-arm both at once. Read-only — the caller only observes
    /// ([`is_set`](super::SkipDropKill::is_set)) or hands it to
    /// [`graceful::run`](super::graceful::run), which owns the epoch protocol.
    #[cfg(target_os = "freebsd")]
    pub(crate) fn skip_drop_kill(&self) -> &super::SkipDropKill {
        &self.skip_drop_kill
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        // We track group ids (plus solo-adopted pids), not every individual
        // process, so report the number of live entries and leave cpu/memory
        // absent.
        Ok(ProcessGroupStats {
            active_process_count: self.groups.count_alive() + self.solos.count_alive(),
            total_cpu_time: None,
            peak_memory_bytes: None,
        })
    }
}

impl super::graceful::GracefulTarget for ProcessGroup {
    fn signal_all(&self, signal: i32) -> super::graceful::SoftDelivery {
        // The graceful soft signal is best-effort by trait contract (the driver
        // polls regardless), so a delivery failure never stops the teardown — the
        // genuine live-`EPERM` is still reported from `hard_kill` at escalation. The
        // send verdict is recorded only for the report: an `Ok` sweep (including an
        // empty group) is `Sent`; a surfaced send failure (an `EINVAL`, or a
        // live-non-zombie `EPERM` a uid-changed member raised) is `Failed`.
        match self.broadcast(signal) {
            Ok(()) => super::graceful::SoftDelivery::Sent,
            Err(_) => super::graceful::SoftDelivery::Failed,
        }
    }

    fn is_drained(&self) -> bool {
        !self.any_alive()
    }

    fn alive_count(&self) -> Option<usize> {
        // The tracked group leaders plus solo-adopted pids still alive — the same
        // member set `members()` reports (descendants inside the groups are not
        // enumerated). Probe-only (no pruning), and infallible for this in-memory
        // tracked set, so always `Some`.
        Some(self.groups.count_alive() + self.solos.count_alive())
    }

    fn hard_kill(&self) -> io::Result<()> {
        // `SIGKILL` sweep. A delivery `EPERM` against a live, non-zombie member (a
        // `sudo`/setuid child that rejects the signal) is surfaced as the genuine
        // containment gap; a harmless zombie-only group's `EPERM` — the false
        // positive that reverted the first attempt — stays `Ok` because the sweep
        // checks the target's run state first (see `Tracked::signal_all`). The
        // contract is documented on `ProcessGroup::kill_all`.
        self.broadcast(libc::SIGKILL)
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.skip_drop_kill.is_set() {
            // Best-effort backstop; `Drop` cannot surface a result, so a
            // live-`EPERM` here is swallowed (the same tree would have surfaced it
            // from an explicit `kill_all`/`shutdown` had the caller made one).
            let _ = self.broadcast(libc::SIGKILL);
        }
    }
}

/// Reaps a freshly-spawned, not-yet-tracked child if [`ProcessGroup::spawn`]
/// unwinds (an early `Err` or a panic) before the child is registered in
/// `groups`. Until `track` records it the child is owned by nothing that would
/// tear it down, so dropping it un-disarmed would leak a live self-grouped child
/// — a silent kill-on-drop violation. [`disarm`](Self::disarm) hands the child
/// back once it is tracked, after which `groups`/`Drop` own teardown.
///
/// The pgroup analogue of the Windows backend's `UncontainedChildGuard`: same
/// arm/disarm shape, but it hard-kills the child's process *group* (with a
/// direct-pid fallback for the not-yet-`setpgid`'d window) rather than the lone
/// process, so any descendant the child managed to fork in the window is reaped
/// too.
struct UntrackedChildGuard {
    /// `None` only after [`disarm`](Self::disarm) has taken the child.
    child: Option<Child>,
}

impl UntrackedChildGuard {
    fn arm(child: Child) -> Self {
        Self { child: Some(child) }
    }

    /// Borrow the guarded child (present from `arm` until `disarm`) to read its
    /// `id()` while the reaper is armed.
    fn child(&self) -> &Child {
        self.child
            .as_ref()
            .expect("the guarded child is present until disarm")
    }

    /// Tracking succeeded: stop guarding and return the child unharmed.
    fn disarm(mut self) -> Child {
        self.child
            .take()
            .expect("the guarded child is taken exactly once")
    }
}

impl Drop for UntrackedChildGuard {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return; // disarmed — the child is tracked, teardown is owned elsewhere.
        };
        if let Some(pid) = child.id() {
            hard_kill_fresh_spawn(pid as i32);
        }
        // Dropping the tokio `Child` hands the killed process to tokio's orphan
        // reaper, so it is waited (no zombie leak) without this guard blocking.
        drop(child);
    }
}

/// Best-effort `SIGKILL` of a **freshly-spawned, still-owned** child's whole
/// process group, with a direct-pid fallback for the window in which it may not
/// have run its `setpgid`/`setsid` yet (`killpg` → `ESRCH`, because that group id
/// does not exist). The child is its own group leader (or, on the `setsid` path, a
/// session leader), so the `killpg` reaps it *and* any descendant it forked in
/// that window.
///
/// Both callers hold the child's `Child` un-reaped across the call, so `pid` can
/// never be a recycled alias and the kill needs no identity gate: the
/// [`UntrackedChildGuard`] leak backstop, and the PTY rollback
/// ([`ProcessGroup::rollback_pty_spawn`]), which must land *before* the tracked id
/// is dropped.
///
/// Deliberately raw rather than routed through [`deliver_signal`], for the reason
/// documented there: these are the last-resort backstops for a child nothing else
/// owns yet, so they must not be interposable by a fault-injection rule.
pub(crate) fn hard_kill_fresh_spawn(pid: i32) {
    // SAFETY: killpg/kill delivering SIGKILL to a freshly-spawned id the caller
    // still owns; `ESRCH` (nothing under that id) is the classified benign case.
    unsafe {
        if libc::killpg(pid, libc::SIGKILL) == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::process::Command;

    use super::*;

    /// A signal number well past any real or real-time signal (`SIGRTMAX` is ~64 on
    /// Linux, and macOS/BSD have no RT signals), so `kill`/`killpg` reject it with
    /// `EINVAL` on every POSIX target — the malformed-request case the honesty fix
    /// must surface rather than swallow.
    const BOGUS_SIGNAL: i32 = 4096;

    /// `graceful_shutdown(escalate=false)` must not kill survivors — neither
    /// during the call nor when the `ProcessGroup` itself drops.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn escalate_false_does_not_kill_survivors() {
        let pg = ProcessGroup::new();
        let opts = crate::sys::SpawnOptions::default();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("trap '' TERM; while :; do :; done");
        // Reap the child on any early panic path so the test never orphans it.
        cmd.kill_on_drop(true);
        let mut child = pg.spawn(&mut cmd, &opts).unwrap();
        let pid = child.id().unwrap() as i32;
        tokio::time::sleep(Duration::from_millis(50)).await;

        pg.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .unwrap();
        // Drop the group explicitly — this is where the bug fires.
        drop(pg);

        let alive = unsafe { libc::kill(pid, 0) } == 0;
        // Cleanup the orphaned child regardless.
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(alive, "child must survive when escalate_to_kill=false");
    }

    /// A child of this group that outlives the test, plus the un-reaped handle that
    /// keeps its pid pinned: `sh` ignoring `SIGTERM`, so a non-escalating shutdown
    /// really does leave it running.
    ///
    /// It publishes a marker file only *after* installing the trap, and this waits
    /// for that rather than sleeping a guessed interval: until the trap exists the
    /// child would die of the graceful `SIGTERM` like any other, which would make
    /// the caller's "the survivor was spared" reading meaningless.
    #[cfg(feature = "pty")]
    async fn spawn_survivor(pg: &ProcessGroup, tag: &str) -> (Child, i32) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let marker = std::env::temp_dir().join(format!(
            "processkit_pgroup_survivor_{tag}_{}_{nanos}.ready",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&marker);
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("trap '' TERM; echo ready > \"$PK_READY\"; while :; do sleep 60; done")
            .env("PK_READY", &marker);
        // Reap the child on any early panic path so the test never orphans it.
        cmd.kill_on_drop(true);
        let child = pg
            .spawn(&mut cmd, &crate::sys::SpawnOptions::default())
            .expect("spawn a group member");
        let pid = child.id().expect("the member reports a pid") as i32;
        for _ in 0..600 {
            if marker.exists() {
                let _ = std::fs::remove_file(&marker);
                return (child, pid);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the survivor never reported that its SIGTERM trap was installed");
    }

    /// Whether `pid` still names a **live** process, reaping it first if it is one of
    /// ours that already exited — an un-reaped corpse answers a bare `kill(pid, 0)`
    /// as "alive", which would let a group that wrongly killed its survivor pass.
    #[cfg(feature = "pty")]
    fn is_live_child(pid: i32) -> bool {
        let mut status = 0;
        // SAFETY: a non-blocking wait for one specific pid, then the signal-`0`
        // existence probe; neither touches memory beyond `status`.
        unsafe {
            if libc::waitpid(pid, &raw mut status, libc::WNOHANG) == pid {
                return false; // just reaped by us — definitively gone
            }
            libc::kill(pid, 0) == 0
        }
    }

    /// T-270: a PTY launch that fails *after* its child exists must leave a
    /// `graceful_shutdown(escalate = false)` decision standing. Its spawn re-armed
    /// the kill-on-drop backstop (every spawn does), so without a restore the
    /// group's `Drop` would `SIGKILL` survivors the caller chose to leave running —
    /// a launch failure silently overriding the caller's stop policy.
    ///
    /// The spawn/rollback pair is driven directly here (the shared PTY seam wires
    /// the same two calls together — see `Job::spawn_pty` on each Unix backend, and
    /// `sys::pty::imp::tests` for the end-to-end run through the real guard), which
    /// is what lets this pin the ProcessGroup backend specifically on any host.
    #[cfg(feature = "pty")]
    #[tokio::test]
    #[ignore = "spawns real subprocesses"]
    async fn a_rolled_back_pty_spawn_restores_the_spare_it_displaced() {
        let pg = ProcessGroup::new();
        let (mut survivor, survivor_pid) = spawn_survivor(&pg, "restore").await;

        pg.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .unwrap();
        assert!(
            pg.skip_drop_kill.is_set(),
            "precondition: a non-escalating shutdown spares the survivors"
        );

        // The PTY launch: its spawn joins the group and re-arms the backstop…
        let mut pty_cmd = Command::new("sh");
        pty_cmd.arg("-c").arg("sleep 60");
        pty_cmd.kill_on_drop(true);
        let (mut pty_child, displaced) = pg
            .spawn_displacing_spare(&mut pty_cmd, &crate::sys::SpawnOptions::default())
            .unwrap();
        let pty_pid = pty_child.id().expect("the pty child reports a pid");
        assert!(
            !pg.skip_drop_kill.is_set(),
            "precondition: the spawn re-arms the backstop for its new member"
        );
        // …and the master wiring then fails, so the whole spawn is undone.
        pg.rollback_pty_spawn(pty_pid, displaced);
        assert!(
            pg.skip_drop_kill.is_set(),
            "the rollback must restore the spare its own spawn displaced"
        );

        drop(pg);
        // A `Drop` that killed the survivor issued an unblockable `SIGKILL` before
        // returning; give the kernel a moment to retire it before reading liveness.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let spared = is_live_child(survivor_pid);

        // Whichever way the assertion goes, leave nothing behind.
        let _ = unsafe { libc::kill(survivor_pid, libc::SIGKILL) };
        let _ = survivor.wait().await;
        let _ = pty_child.wait().await;

        assert!(
            spared,
            "the survivor a non-escalating shutdown spared must outlive a failed \
             PTY launch and the group's Drop"
        );
    }

    /// The transactional half of the same fix: a `spawn` landed in the same group
    /// between the rolled-back spawn's re-arm and its rollback. That newcomer is a
    /// live member nothing chose to spare, so the restore must lose and `Drop` must
    /// still kill — restoring the older spare here would re-open exactly the
    /// orphan-leak class the latch's generation guard exists for (T-079).
    #[cfg(feature = "pty")]
    #[tokio::test]
    #[ignore = "spawns real subprocesses"]
    async fn a_spawn_between_the_pty_spawn_and_its_rollback_keeps_the_backstop_armed() {
        let pg = ProcessGroup::new();
        let (mut survivor, survivor_pid) = spawn_survivor(&pg, "raced").await;

        pg.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .unwrap();

        let mut pty_cmd = Command::new("sh");
        pty_cmd.arg("-c").arg("sleep 60");
        pty_cmd.kill_on_drop(true);
        let (mut pty_child, displaced) = pg
            .spawn_displacing_spare(&mut pty_cmd, &crate::sys::SpawnOptions::default())
            .unwrap();
        let pty_pid = pty_child.id().expect("the pty child reports a pid");

        // A fresh member joins before the failed launch is undone.
        let (mut newcomer, newcomer_pid) = spawn_survivor(&pg, "newcomer").await;
        pg.rollback_pty_spawn(pty_pid, displaced);
        assert!(
            !pg.skip_drop_kill.is_set(),
            "a spawn after the rolled-back one must keep the backstop armed"
        );

        drop(pg);
        // `Drop`'s `SIGKILL` is asynchronous; poll for the newcomer's death rather
        // than assuming it has already landed.
        let mut killed = false;
        for _ in 0..100 {
            if !is_live_child(newcomer_pid) {
                killed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let _ = unsafe { libc::kill(newcomer_pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(survivor_pid, libc::SIGKILL) };
        let _ = newcomer.wait().await;
        let _ = survivor.wait().await;
        let _ = pty_child.wait().await;

        assert!(
            killed,
            "a member that joined after the rolled-back spawn must keep its \
             kill-on-drop backstop — the restore must not spare it"
        );
    }

    /// T-079 (pgroup re-arm race): a `spawn`/`adopt` that re-arms the backstop
    /// while a `graceful_shutdown(escalate=false)` is mid-poll must win — the
    /// shutdown's final (stale) `request` must not re-spare the fresh child.
    ///
    /// Deterministic on the paused clock (no real subprocess): a fake
    /// [`GracefulTarget`](crate::sys::graceful::GracefulTarget) re-arms the
    /// ProcessGroup's **own** latch during the drain wait, standing in for the
    /// concurrent spawn/adopt, and the real [`graceful::run`](crate::sys::graceful::run)
    /// driver — the exact call `ProcessGroup::graceful_shutdown` makes — is exercised
    /// against that latch. The final `is_set() == false` is the load-bearing
    /// outcome: `ProcessGroup::drop` then SIGKILLs the tracked groups rather than
    /// sparing the newcomer.
    #[tokio::test(start_paused = true)]
    async fn shutdown_request_does_not_override_a_concurrent_rearm() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct RacingRearm<'a> {
            latch: &'a crate::sys::SkipDropKill,
            polls: AtomicUsize,
        }
        impl crate::sys::graceful::GracefulTarget for RacingRearm<'_> {
            fn signal_all(&self, _signal: i32) -> crate::sys::graceful::SoftDelivery {
                crate::sys::graceful::SoftDelivery::Sent
            }
            fn is_drained(&self) -> bool {
                // Re-arm on the second poll (the concurrent spawn/adopt landing
                // mid-shutdown), then keep reporting "not drained" so the driver
                // runs to the deadline and issues its stale request.
                if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                    self.latch.clear();
                }
                false
            }
            fn alive_count(&self) -> Option<usize> {
                None
            }
            fn hard_kill(&self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let pg = ProcessGroup::new();
        // A live reused group: its backstop is already armed by an earlier spawn.
        pg.skip_drop_kill.clear();
        let target = RacingRearm {
            latch: &pg.skip_drop_kill,
            polls: AtomicUsize::new(0),
        };
        crate::sys::graceful::run(
            &target,
            &pg.skip_drop_kill,
            libc::SIGTERM,
            Duration::from_millis(100),
            false,
        )
        .await
        .expect("graceful run");
        assert!(
            !pg.skip_drop_kill.is_set(),
            "a child spawned/adopted mid-shutdown must keep the group's Drop-kill \
             backstop — the stale request must not re-spare it"
        );
    }

    /// A pid that exists as a process but not as a process-group leader must
    /// not be pruned from a group-mode `Tracked` set — ESRCH on the group probe
    /// does not mean the process is gone.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn esrch_on_group_probe_does_not_prune_a_live_pid() {
        let tracked = Tracked::new(true);

        // Spawn without `process_group(0)` so the child inherits the current
        // process group and is NOT its own leader — kill(-pid,0) is ESRCH.
        // `kill_on_drop` reaps it on any early panic path (e.g. the `pid_ok`
        // assert) so the test never orphans the `sleep 60`.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Verify precondition: group probe is ESRCH, pid probe is alive.
        let group_ok = unsafe { libc::kill(-pid, 0) } == 0;
        let pid_ok = unsafe { libc::kill(pid, 0) } == 0;
        if group_ok {
            // Pid happened to become a group leader (process_group set elsewhere).
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }
        assert!(pid_ok, "spawned child must be alive");

        // The probe (no latch → fallback applies) must return true — the pid is
        // alive as a process even though it is not a group leader.
        let exists = tracked.probe_raw(pid, false).0;

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            exists,
            "a process that exists as a pid but not as a group leader \
             must be considered alive (L6 fallback, pre-latch)"
        );
    }

    /// Once the group has been seen alive (the `group_seen` latch), the
    /// direct-pid fallback is disabled — a not-a-group-leader pid (standing in
    /// for a reaped-and-recycled pid) is treated as GONE, instead of being kept
    /// alive (and later signalled) forever, which would SIGKILL an innocent
    /// process that recycled the pid.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn group_seen_latch_disables_l6_fallback() {
        let tracked = Tracked::new(true);
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Skip if the pid happens to be a group leader (then kill(-pid,0) would
        // succeed and there is no fallback case to exercise).
        if unsafe { libc::kill(-pid, 0) } == 0 {
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }

        // Before the group was seen, the fallback keeps a live-but-not-a-leader
        // pid alive (the fork→exec window semantics).
        assert!(
            tracked.probe_raw(pid, false).0,
            "pre-latch: L6 keeps a live pid"
        );
        // After the latch the same pid is GONE: the fallback is disabled, so a
        // recycled pid is pruned rather than kept and signalled.
        assert!(
            !tracked.probe_raw(pid, true).0,
            "post-latch: L6 disabled — a not-a-group-leader pid is treated as gone (B5)"
        );

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;
    }

    /// Adopting a child this group already spawned must not double-track it.
    /// The child has exec'd, so its `setpgid` fails `EACCES`; without the dedup it
    /// would land in `solos` while still in `groups`, double-counting in
    /// `members()`/`stats()` and double-delivering every broadcast.
    #[cfg(feature = "process-control")]
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn adopt_of_an_already_spawned_child_does_not_double_track() {
        let pg = ProcessGroup::new();
        let opts = crate::sys::SpawnOptions::default();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        cmd.kill_on_drop(true);
        let mut child = pg.spawn(&mut cmd, &opts).unwrap();
        let pid = child.id().unwrap() as i32;

        // Re-adopt the same child: its `setpgid` fails EACCES (it has exec'd).
        pg.adopt(&child).unwrap();

        let members = pg.members();
        assert_eq!(
            members.iter().filter(|&&m| m == pid).count(),
            1,
            "an already-spawned child must be tracked once, not double-tracked"
        );

        drop(pg);
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;
    }

    /// A bare-pid adoption must leave an entry that is **identity-anchored**, not a
    /// number-only one: that token is the whole of what stands between a later
    /// recycle of the number and a `SIGKILL` aimed at whoever holds it, since no
    /// `Child` is kept un-reaped behind an external process.
    ///
    /// The load-bearing assertion is `identity.is_some()`. Neutralize the anchor
    /// capture — have `adopt_external` track through the plain `track` (whose read
    /// is best-effort and can legitimately answer `None`) or push `identity: None`
    /// — and this fails on exactly the platforms where the protection is claimed.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn adopt_external_anchors_the_tracked_entry_on_an_identity() {
        let pg = ProcessGroup::new();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;
        let real = read_identity(pid).expect("this target reports a start-time identity");

        pg.adopt_external(pid as u32)
            .expect("a live process is adoptable by pid on this target");

        // Wherever it landed (solo is the ordinary outcome — this process forked it
        // and it has exec'd, so `setpgid` is refused), the entry carries the token.
        let anchored = {
            let solos = pg.solos.ids.lock().unwrap_or_else(|e| e.into_inner());
            let groups = pg.groups.ids.lock().unwrap_or_else(|e| e.into_inner());
            solos
                .iter()
                .chain(groups.iter())
                .find(|e| e.id == pid)
                .map(|e| e.identity)
        };

        drop(pg);
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert_eq!(
            anchored,
            Some(Some(real)),
            "a bare-pid adoption must track the pid, anchored on the identity read \
             during the call — never a number-only entry"
        );
    }

    /// A stale solo entry without an identity must not mask a fresh anchored
    /// entry for the same number. This uses the current test process as the live
    /// holder, so it models the recycled-pid table state without depending on pid
    /// allocation or subprocess timing; the old entry is never signalled because
    /// only the replacement remains in the solo table.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[test]
    fn solo_track_with_replaces_a_stale_unanchored_entry() {
        let solos = Tracked::new(false);
        let pid = std::process::id() as i32;
        let anchor = read_identity(pid).expect("this target reports a start-time identity");

        solos.track_with(pid, false, None);
        solos.track_with(pid, false, Some(anchor));
        solos.track_with(pid, false, Some(anchor));

        let ids = solos.ids.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            ids.len(),
            1,
            "the same anchored process must remain deduplicated"
        );
        assert_eq!(ids[0].id, pid);
        assert_eq!(ids[0].identity, Some(anchor));
    }

    /// The same shared `Tracked` path keeps its historical numeric de-dup when
    /// the target has no identity reader (the BSD fallback).
    #[cfg(all(
        feature = "process-control",
        not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
    ))]
    #[test]
    fn solo_track_with_keeps_number_only_dedup_without_identity_reader() {
        let solos = Tracked::new(false);
        let pid = std::process::id() as i32;

        solos.track_with(pid, false, None);
        solos.track_with(pid, false, None);

        let ids = solos.ids.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            ids.len(),
            1,
            "number-only targets must retain numeric de-dup"
        );
        assert_eq!(ids[0].id, pid);
        assert_eq!(ids[0].identity, None);
    }

    /// The negative control for the entry above: once the number no longer names
    /// the process the anchor was taken from, the group must not kill whoever holds
    /// it now. Modelled by poisoning the stored token — the same shape
    /// `solo_pid_reuse_without_esrch_is_not_signalled` uses, but driven through
    /// `adopt_external` end-to-end and asserting on the *process*, not on pruning.
    ///
    /// Neutralize the identity gate (make `is_recycled` return `false`, or drop the
    /// `probe_entry` check) and this test's stand-in is SIGKILLed by `kill_all` —
    /// the exact defect the anchor exists to prevent.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn a_recycled_number_is_not_killed_by_the_group_that_adopted_it() {
        let pg = ProcessGroup::new();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        pg.adopt_external(pid as u32).expect("adopt by pid");

        // Stand in for "this number was reaped and handed to a stranger": the live
        // process at `pid` is now a different instance than the anchored one. A
        // real recycle needs the pid space to wrap; the entry's view of it is
        // identical, and this is the state the gate must act on.
        for set in [&pg.solos, &pg.groups] {
            let mut ids = set.ids.lock().unwrap_or_else(|e| e.into_inner());
            for entry in ids.iter_mut().filter(|e| e.id == pid) {
                entry.identity = entry.identity.map(|token| token ^ 1);
            }
        }

        pg.kill_all().expect("kill_all over a recycled entry");
        // A `SIGKILL` that must never have been sent — and liveness has to be read
        // as *live and non-zombie*, not as `kill(pid, 0) == 0`: the stand-in is a
        // direct child this test has not awaited, so a delivered `SIGKILL` would
        // leave a zombie that still answers the bare existence probe. Poll across a
        // window several times longer than delivery takes, so the neutralized-gate
        // case fails here instead of racing.
        let mut survived = true;
        for _ in 0..50 {
            if !is_live_non_zombie(pid) {
                survived = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        drop(pg);
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            survived,
            "kill_all must not signal a number whose identity no longer matches the \
             one captured when it was adopted"
        );
    }

    /// A **leftover entry for a number that has since changed hands** must not
    /// answer the adoption's de-dup: `adopt_external` has to track the process it
    /// was given, not skip it and hand back an `Ok` that contains nothing.
    ///
    /// The sequence this models is reachable and not micro-second wide: this group
    /// spawned a child, the child exited and was reaped (nothing deregisters an
    /// entry at reap — only a sweep prunes, and a passive group may not sweep for
    /// minutes), the OS handed the number on, and *that* number is what arrives
    /// here. Both shapes the leftover entry can have are exercised, because only the
    /// first is prunable:
    ///
    /// - it carries a token that no longer matches the number's occupant — the
    ///   sweep's own identity gate drops it;
    /// - it carries **no** token (a track-time identity read that failed), so no
    ///   sweep can drop it: it probes alive on the number alone. Only comparing it
    ///   against the anchor this call captured tells it apart from the real process.
    ///
    /// The load-bearing assertion is that the pid ends up tracked *with the token
    /// read during the call*. Neutralize the fix — de-dup on a bare "is this number
    /// on the books" scan, as `Tracked::contains` did — and both shapes fail here:
    /// the call returns `Ok` having tracked nothing at all.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real subprocess"]
    async fn adopt_external_is_not_silenced_by_a_stale_entry_for_the_same_number() {
        for (case, stale_carries_a_token) in [("anchored", true), ("bare", false)] {
            let pg = ProcessGroup::new();
            let ready = std::env::temp_dir().join(format!(
                "processkit_pgroup_adopt_external_{case}_{}.ready",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&ready);
            let mut child = Command::new("sh")
                .arg("-c")
                .arg("echo ready > \"$PK_ADOPT_READY\"; exec sleep 60")
                .env("PK_ADOPT_READY", &ready)
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let pid = child.id().unwrap() as i32;
            for _ in 0..200 {
                if ready.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert!(
                ready.exists(),
                "the child must reach the post-exec adoption point"
            );
            let _ = std::fs::remove_file(&ready);
            let real = read_identity(pid).expect("this target reports a start-time identity");

            // The books as the reaped child left them: the number tracked as a group
            // leader, seeded `false` exactly as `spawn` seeds it (so the direct-pid
            // fallback keeps the entry alive on the number alone), with either the
            // dead process's token or none at all.
            pg.groups
                .track_with(pid, false, stale_carries_a_token.then_some(real ^ 1));

            pg.adopt_external(pid as u32)
                .expect("a live process is adoptable by pid on this target");

            // The child has already exec'd, so setpgid is refused and the adoption
            // must move the pid from the group table to the solo table. In
            // particular, the stale group entry must not remain beside the anchor.
            let (group_count, solo_count, solo_identity) = {
                let solos = pg.solos.ids.lock().unwrap_or_else(|e| e.into_inner());
                let groups = pg.groups.ids.lock().unwrap_or_else(|e| e.into_inner());
                (
                    groups.iter().filter(|e| e.id == pid).count(),
                    solos.iter().filter(|e| e.id == pid).count(),
                    solos.iter().find(|e| e.id == pid).map(|e| e.identity),
                )
            };

            assert_eq!(group_count, 0, "the stale group entry must be removed");
            assert_eq!(
                solo_count, 1,
                "the adopted pid must be tracked once as a solo"
            );
            assert_eq!(solo_identity, Some(Some(real)));

            // Fault every delivery so this regression can observe the number of
            // targets without signalling the live holder. A clean cross-table move
            // has one solo delivery; leaving the stale group entry would make the
            // broadcast attempt both killpg and kill.
            let faults = crate::sys::fault_injection::Faults::new()
                .fail_every(
                    crate::sys::fault_injection::Site::PgroupSignalDelivery,
                    None,
                    libc::EINVAL,
                )
                .arm();
            let outcome = pg.kill_all();
            assert_eq!(
                faults.fired(crate::sys::fault_injection::Site::PgroupSignalDelivery),
                1
            );
            assert_eq!(
                outcome
                    .expect_err("the injected delivery failure must reach kill_all")
                    .raw_os_error(),
                Some(libc::EINVAL)
            );
            assert_eq!(
                unsafe { libc::kill(pid, 0) },
                0,
                "the holder was not signalled"
            );
            drop(faults);

            drop(pg);
            // SAFETY: a best-effort kill of a child this test started.
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
        }
    }

    /// A number that names no process is refused by the anchor capture — the bare-pid
    /// counterpart of `adopt`'s "an already-reaped child errors instead of tracking
    /// nothing", which a bare number carries no evidence for on its own.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[tokio::test]
    async fn adopt_external_of_a_pid_that_names_nothing_is_not_found() {
        let pg = ProcessGroup::new();
        // Far above any pid_max, still inside `pid_t`.
        let err = pg
            .adopt_external(2_000_000_000)
            .expect_err("a pid that names nothing is not adoptable");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err:?}");
        assert!(
            pg.members().is_empty(),
            "a refused adoption must track nothing"
        );
    }

    /// The BSD arm of the same call: no start-time reader, so the refusal is
    /// `Unsupported` and comes before any syscall against the target.
    #[cfg(all(
        feature = "process-control",
        not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
    ))]
    #[tokio::test]
    async fn adopt_external_is_unsupported_without_an_identity_reader() {
        let pg = ProcessGroup::new();
        let err = pg
            .adopt_external(std::process::id())
            .expect_err("no identity reader here, so nothing is adoptable by pid");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err:?}");
        assert!(
            pg.members().is_empty(),
            "a refused adoption must track nothing"
        );
    }

    /// Every `spawn` seeds the group-liveness latch `false` — on the non-`setsid`
    /// path too (it used to seed `true`). The child runs its own
    /// `setpgid`/`setsid` after fork, so the group is not proven to exist until
    /// the first successful probe; seeding `false` keeps the direct-pid fallback
    /// armed across the not-yet-`setpgid`'d window for BOTH paths, so a fast
    /// probe/sweep right after spawn (before the child is scheduled) never wrongly
    /// prunes the still-live child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn spawn_seeds_group_seen_false_on_both_paths() {
        for setsid in [false, true] {
            let pg = ProcessGroup::new();
            let opts = crate::sys::SpawnOptions {
                setsid,
                ..Default::default()
            };
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg("sleep 60");
            cmd.kill_on_drop(true);
            let mut child = pg.spawn(&mut cmd, &opts).unwrap();
            let pid = child.id().unwrap() as i32;

            // Inspect the freshly-pushed entry *before* any probe runs on it:
            // `track` pushes without probing the new id, so the seeded latch is
            // observable directly.
            let seeded_false = {
                let ids = pg.groups.ids.lock().unwrap_or_else(|e| e.into_inner());
                ids.iter()
                    .find(|e| e.id == pid)
                    .map(|e| !e.group_seen)
                    .unwrap_or(false)
            };

            drop(pg);
            let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;

            assert!(
                seeded_false,
                "spawn (setsid={setsid}) must seed group_seen=false so the fallback \
                 window stays open until the first successful group probe",
            );
        }
    }

    /// A freshly-spawned child seeded `group_seen = false` that is a live process
    /// but not yet its own group leader (the not-yet-`setpgid`'d window) must be
    /// KEPT and SIGNALLED by a teardown sweep — via the direct-pid fallback — not
    /// pruned as a drained group and silently left unsignalled. Seeding the latch
    /// `false` on every spawn is what keeps this window covered for the
    /// non-`setsid` fork path too.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn signal_all_keeps_and_signals_a_not_yet_grouped_child() {
        let tracked = Tracked::new(true);
        // Spawn WITHOUT process_group(0): the child inherits the parent's group
        // and is not its own leader, so kill(-pid, 0) is ESRCH — the exact shape
        // of the not-yet-`setpgid`'d window a spawn seeds `group_seen=false` to
        // survive. The child traps TERM, so a delivered SIGTERM does NOT kill it:
        // we assert it stayed alive (signalled, kept), then SIGKILL it.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("trap '' TERM; while :; do :; done")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Skip if the pid happens to already be a group leader — then kill(-pid,0)
        // succeeds and there is no not-yet-grouped window to exercise.
        if unsafe { libc::kill(-pid, 0) } == 0 {
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }

        // Seed exactly as a spawn does now: a group entry with the latch `false`.
        tracked.track(pid, false);
        // A teardown sweep must keep and signal it via the direct-pid fallback.
        let _ = tracked.signal_all(libc::SIGTERM);

        let still_tracked = {
            let ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter().any(|e| e.id == pid)
        };
        let alive = unsafe { libc::kill(pid, 0) } == 0;

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            still_tracked,
            "a not-yet-grouped live child must not be pruned by a teardown sweep"
        );
        assert!(
            alive,
            "the child must survive a trapped SIGTERM — it was signalled, not lost"
        );
    }

    /// An armed [`UntrackedChildGuard`] dropped without `disarm` (the spawn→track
    /// unwind path) must hard-kill the still-untracked child, so a panic or early
    /// error there never leaks a live self-grouped child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn untracked_guard_reaps_the_child_on_an_armed_drop() {
        use std::os::unix::process::CommandExt as _;

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        // Own group leader, so the guard's killpg reaches it (the primary path,
        // not just the ESRCH direct-pid fallback).
        cmd.as_std_mut().process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "the child is alive right after spawn"
        );

        drop(UntrackedChildGuard::arm(child)); // armed → reaps on drop

        // A zombie still probes alive via kill(pid,0), so death is only observable
        // once the exited child is *waited*: reap it with a WNOHANG loop,
        // cooperating with tokio's orphan reaper (an ECHILD means it already
        // waited the child — also dead).
        let mut dead = false;
        for _ in 0..200 {
            // SAFETY: waitpid on a pid we spawned; a null status pointer is valid
            // (we don't inspect the exit status) and WNOHANG never blocks.
            let r = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
            if r == pid
                || (r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD))
            {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Cleanup regardless of the outcome.
        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };

        assert!(
            dead,
            "an armed guard drop must terminate the untracked child"
        );
    }

    /// `disarm` hands back the same child, still running, for `groups` to own —
    /// the guard must not kill a tracked (disarmed) child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn untracked_guard_disarm_hands_back_a_live_child() {
        use std::os::unix::process::CommandExt as _;

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        cmd.as_std_mut().process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;

        let mut kept = UntrackedChildGuard::arm(child).disarm();
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "disarm must leave the child running"
        );

        // Clean up the child the guard handed back.
        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = kept.wait().await;
    }

    /// Reuse of a group's pgid **without** an intervening `ESRCH` — the hazard the
    /// `group_seen` latch alone misses. A tracked group drains, its pgid number is
    /// freed, and a *different* leader takes it before the next sweep: `kill(-id,0)`
    /// reports the stranger group alive, so without the identity gate a teardown
    /// sweep would `killpg` an unrelated group. The gate must instead prune the
    /// entry (its captured identity no longer matches the number's current one)
    /// and signal nothing.
    ///
    /// Deterministic on a real subprocess: a genuinely-alive group leader stands
    /// in for the stranger that recycled the number, and the entry is tracked with
    /// a deliberately *stale* identity (as if captured from the original,
    /// since-reaped leader). Pruning inside `signal_all`'s sweep — before any
    /// `killpg` for that entry — is the load-bearing outcome: it is structurally
    /// impossible for the stranger to have been signalled.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn group_pgid_reuse_without_esrch_is_not_signalled() {
        use std::os::unix::process::CommandExt as _;

        let tracked = Tracked::new(true);

        // A real child that leads its own group, so `kill(-pid, 0)` succeeds — the
        // stranger group that reused our old pgid number. It traps TERM so an
        // (erroneous) signal would not even reap it, keeping the test orphan-free;
        // the load-bearing assertion is the prune, not liveness.
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("trap '' TERM; while :; do :; done");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(-pid, 0) } == 0,
            "the stand-in must lead its own group"
        );

        let Some(real) = read_identity(pid) else {
            // No identity reader on this target (the BSDs): the strengthening
            // degrades to the documented number-only behavior — nothing to assert.
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        };

        // Track the *number* with a stale identity (`real ^ 1` ≠ `real`) and
        // `group_seen = true`: the original group was seen alive before it drained,
        // so absent the identity check the sweep would happily `killpg` the
        // stranger that now holds the pgid.
        {
            let mut ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.push(Entry {
                id: pid,
                group_seen: true,
                identity: Some(real ^ 1),
            });
        }

        let _ = tracked.signal_all(libc::SIGTERM);

        let still_tracked = {
            let ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter().any(|e| e.id == pid)
        };

        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            !still_tracked,
            "a recycled-pgid entry (identity mismatch, no intervening ESRCH) must \
             be pruned by the sweep, so the stranger group is never signalled"
        );
    }

    /// The solo counterpart of `group_pgid_reuse_without_esrch_is_not_signalled`:
    /// an adopted (solo) pid recycled to an unrelated process between two sweeps.
    /// A solo entry is a bare pid — `kill(pid, 0)` reports the recycled stranger
    /// alive — so its protection must be no weaker than a group's: the identity
    /// gate must prune the entry and signal nothing.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn solo_pid_reuse_without_esrch_is_not_signalled() {
        let tracked = Tracked::new(false); // solo: direct-pid probe/signal

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("trap '' TERM; while :; do :; done")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "the stand-in solo pid must be alive"
        );

        let Some(real) = read_identity(pid) else {
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        };

        // Track the number with a stale identity: the original adopted child was
        // reaped and the pid recycled to this unrelated process.
        {
            let mut ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.push(Entry {
                id: pid,
                group_seen: false,
                identity: Some(real ^ 1),
            });
        }

        let _ = tracked.signal_all(libc::SIGTERM);

        let still_tracked = {
            let ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter().any(|e| e.id == pid)
        };

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            !still_tracked,
            "a recycled solo pid (identity mismatch) must be pruned by the sweep, \
             so the stranger process is never signalled"
        );
    }

    /// The identity gate must not over-prune: a genuinely-alive entry whose
    /// captured identity still *matches* the number's current one is kept and
    /// signalled, so a normal spawn/adopt/Drop does not regress. `track` captures
    /// the real identity here, exercising the actual capture-then-match path (on
    /// every platform, including the BSDs where both sides are `None` and the gate
    /// is a no-op).
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn matching_identity_group_is_kept_and_signalled() {
        use std::os::unix::process::CommandExt as _;

        let tracked = Tracked::new(true);

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("trap '' TERM; while :; do :; done");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(-pid, 0) } == 0,
            "the child must lead its own group"
        );
        // Let the shell finish executing `trap '' TERM` before we signal it —
        // without this settle window a SIGTERM can race the trap installation
        // and kill the child under its still-default disposition, exactly as
        // `escalate_false_does_not_kill_survivors` above already guards against
        // for the same spawn pattern.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Real capture: `track` reads the live leader's identity itself.
        tracked.track(pid, true);
        // Traps TERM, so a delivered SIGTERM does not reap it — we assert it stayed
        // tracked (kept) and alive (signalled, not lost to the gate).
        let _ = tracked.signal_all(libc::SIGTERM);

        let still_tracked = {
            let ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter().any(|e| e.id == pid)
        };
        let alive = unsafe { libc::kill(-pid, 0) } == 0;

        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            still_tracked,
            "a live, identity-matching group must be kept by the sweep"
        );
        assert!(
            alive,
            "a matching-identity group must be signalled (trapped TERM) — the gate \
             must not prune it"
        );
    }

    /// `is_live_non_zombie` positively confirms a genuinely-running process — the
    /// only state for which a delivery `EPERM` is surfaced as a real containment
    /// gap. A running sleeper must classify as live wherever a state reader exists
    /// (Linux/Android `/proc`, Apple `proc_pidinfo`); on the BSDs there is no
    /// reader, so the answer is always `false` and the assertion is skipped — the
    /// same "defer where the platform can't read it" shape the identity tests use.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn is_live_non_zombie_is_true_for_a_running_process() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        let verdict = is_live_non_zombie(pid);

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        assert!(
            verdict,
            "a running child must classify as live/non-zombie (the state that \
             surfaces a genuine SIGKILL EPERM)"
        );
        #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
        assert!(
            !verdict,
            "targets without a state reader (the BSDs) always classify as not-live, \
             so a delivery EPERM stays swallowed there"
        );
    }

    /// THE regression test for the reverted first attempt (macOS/BSD zombie-EPERM
    /// false positive): a process **group** whose only member is an unreaped
    /// **zombie** must NOT surface an error from a `signal_all(SIGKILL)` teardown.
    /// On macOS `killpg` returns `EPERM` for such a group — the exact false positive
    /// that broke a normal shutdown of a group with unreaped children before — and
    /// the run-state check must classify the zombie leader as not-live and swallow
    /// it, so the sweep returns `Ok`. On Linux `killpg` to a zombie group returns
    /// `0`, so this also guards that the invariant holds identically there.
    ///
    /// Gated to the targets that have a state reader: on the BSDs the discrimination
    /// degrades to the documented "swallow every EPERM" behavior (no zombie/live
    /// telling-apart), so there is nothing platform-specific to exercise, and the
    /// zombie oracle this test uses (`is_live_non_zombie` flipping to `false`) is
    /// unavailable there.
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    #[tokio::test]
    #[ignore = "spawns a real subprocess and leaves it an unreaped zombie"]
    async fn zombie_only_group_teardown_reports_success() {
        use std::os::unix::process::CommandExt as _;

        let tracked = Tracked::new(true);
        // A child that exits at once, in its own process group so `killpg`
        // addresses exactly it. `kill_on_drop` reaps it on any early panic path.
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("exit 0");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;

        // Let the child exit into an unreaped zombie. We deliberately do NOT `wait`
        // it — the tokio `Child` handle is held and unpolled, so nothing reaps it and
        // the exited process lingers as a zombie the group still tracks. Poll until
        // its state reads not-live: since nothing here reaps it, that flip can only
        // mean "exited into a zombie" (never "gone"), a deterministic oracle that
        // needs no fixed sleep.
        let mut became_zombie = false;
        for _ in 0..500 {
            if !is_live_non_zombie(pid) {
                became_zombie = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(became_zombie, "the child never became an observable zombie");
        // It is a zombie, not already gone: a zombie still answers signal 0.
        // SAFETY: signal 0 is a sound liveness probe.
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "the exited-but-unreaped child must still exist as a zombie"
        );

        // Track the zombie leader and tear the group down. The identity captured
        // here still matches the zombie's (its proc entry lingers), so the entry is
        // not pruned by the recycle gate — the `killpg` really fires.
        tracked.track(pid, true);
        let outcome = tracked.signal_all(libc::SIGKILL);

        // Reap the zombie regardless of the assertion below.
        let _ = child.wait().await;

        outcome.expect(
            "a zombie-only group's killpg EPERM must be swallowed, not surfaced — \
             surfacing it is the false positive that reverted the first attempt",
        );
    }

    /// `suspend` and `resume` share teardown's zombie-EPERM discrimination. A
    /// group whose only member is an unreaped zombie has nothing left to freeze
    /// or thaw, so both operations must remain successful even on kernels where
    /// `killpg(SIGSTOP/SIGCONT)` reports `EPERM` for that group.
    #[cfg(all(
        feature = "process-control",
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    #[tokio::test]
    #[ignore = "spawns a real subprocess and leaves it an unreaped zombie"]
    async fn suspend_resume_zombie_only_group_reports_success() {
        use std::os::unix::process::CommandExt as _;

        let pg = ProcessGroup::new();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("exit 0");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;

        // Hold the Child unpolled and use the same state oracle as the teardown
        // regression: a not-live process that still answers signal 0 is an
        // unreaped zombie, not a process that has disappeared.
        let mut became_zombie = false;
        for _ in 0..500 {
            if !is_live_non_zombie(pid) {
                became_zombie = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(became_zombie, "the child never became an observable zombie");
        // SAFETY: signal 0 is a sound existence probe and delivers no signal.
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "the exited-but-unreaped child must still exist as a zombie"
        );

        // Seed the group as already observed so both calls exercise killpg and
        // its zombie-only EPERM handling rather than the pre-setpgid pid fallback.
        pg.groups.track(pid, true);
        let suspend_outcome = pg.suspend();
        let resume_outcome = pg.resume();

        // Reap before asserting so a failure never leaks the zombie.
        let _ = child.wait().await;

        suspend_outcome.expect("suspending a zombie-only group must remain a no-op success");
        resume_outcome.expect("resuming a zombie-only group must remain a no-op success");
    }

    /// Positive pin for the public control path: successful `SIGSTOP` and
    /// `SIGCONT` broadcasts must reach a live tracked group, not merely return
    /// `Ok`. `waitpid` stop/continue notifications observe both state changes
    /// without reaping the child.
    #[cfg(feature = "process-control")]
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn suspend_resume_on_live_group_succeeds() {
        let pg = ProcessGroup::new();
        let opts = crate::sys::SpawnOptions::default();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("while :; do :; done");
        cmd.kill_on_drop(true);
        let mut child = pg.spawn(&mut cmd, &opts).unwrap();
        let pid = child.id().unwrap() as i32;
        // Let the child complete setpgid/exec before exercising the group path.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let suspend_outcome = pg.suspend();
        let mut observed_stop = false;
        for _ in 0..500 {
            let mut status = 0;
            // SAFETY: `pid` is our live child and `status` is valid writable
            // storage. WNOHANG keeps this poll non-blocking; WUNTRACED reports
            // the stop without reaping the process.
            let waited = unsafe {
                libc::waitpid(
                    pid,
                    std::ptr::addr_of_mut!(status),
                    libc::WNOHANG | libc::WUNTRACED,
                )
            };
            if waited == pid && libc::WIFSTOPPED(status) {
                observed_stop = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let resume_outcome = pg.resume();
        let mut observed_continue = false;
        for _ in 0..500 {
            let mut status = 0;
            // SAFETY: as above; WCONTINUED observes the resume transition
            // without reaping the still-live child.
            let waited = unsafe {
                libc::waitpid(
                    pid,
                    std::ptr::addr_of_mut!(status),
                    libc::WNOHANG | libc::WCONTINUED,
                )
            };
            if waited == pid && libc::WIFCONTINUED(status) {
                observed_continue = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clean up before asserting so every failure path reaps the subprocess.
        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        suspend_outcome.expect("SIGSTOP delivery to a live group must return Ok");
        assert!(
            observed_stop,
            "the live group never entered a waitpid-observable stopped state"
        );
        resume_outcome.expect("SIGCONT delivery to a live group must return Ok");
        assert!(
            observed_continue,
            "the live group never produced a waitpid-observable continued state"
        );
    }

    /// The honesty fix: an out-of-range signal number (`EINVAL`) sent to a group
    /// with a live member must now be **surfaced** as `Err`, symmetric with the
    /// cgroup backend, instead of the old blanket swallow that returned a false
    /// `Ok`. `EINVAL` is the malformed-request case, independent of the target's
    /// run state, so it fires whatever the platform (no `is_live_non_zombie` gate,
    /// hence no BSD carve-out — the assertion holds on every POSIX target).
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn signal_all_surfaces_einval_for_a_live_group() {
        use std::os::unix::process::CommandExt as _;

        let tracked = Tracked::new(true);
        let mut cmd = Command::new("sh");
        // Traps TERM so a stray real signal cannot reap it; the load-bearing check
        // is the EINVAL return, and `BOGUS_SIGNAL` delivers nothing anyway.
        cmd.arg("-c").arg("trap '' TERM; while :; do :; done");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(-pid, 0) } == 0,
            "the child must lead its own group"
        );
        // Real capture of the live leader's identity, then a bogus-signal sweep.
        tracked.track(pid, true);

        let outcome = tracked.signal_all(BOGUS_SIGNAL);

        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        let err = outcome.expect_err(
            "an out-of-range signal number must surface EINVAL, not be swallowed as \
             a false success",
        );
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EINVAL),
            "the surfaced error must be the EINVAL from the malformed send"
        );
    }

    /// `Signal::Other(0)` maps to signal `0`, the POSIX existence probe: it delivers
    /// **nothing**. A `signal_all(0)` over a group with a live member must return
    /// `Ok` — but that `Ok` reports "a signalable target was reached", not "a signal
    /// was delivered": the child must still be alive afterwards. This pins the
    /// documented "success here does not mean delivery" contract of
    /// `ProcessGroup::signal`.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn other_zero_probes_without_delivering_and_returns_ok() {
        use std::os::unix::process::CommandExt as _;

        let tracked = Tracked::new(true);
        let mut cmd = Command::new("sh");
        // No signal handler needed: signal 0 is never delivered, so a plain idler
        // that would die to any real signal proves nothing was delivered.
        cmd.arg("-c").arg("while :; do :; done");
        cmd.kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(-pid, 0) } == 0,
            "the child must lead its own group"
        );
        tracked.track(pid, true);

        let outcome = tracked.signal_all(0);
        // Nothing was delivered, so the group leader is untouched.
        let alive = unsafe { libc::kill(-pid, 0) } == 0;

        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        outcome.expect(
            "signal 0 is a probe over a live target — it returns Ok having delivered \
             nothing, never an error",
        );
        assert!(
            alive,
            "signal 0 delivers nothing (POSIX existence probe), so the child must \
             survive: an Ok here is not proof of delivery"
        );
    }

    /// Regression for "nobody to signal": an empty tracked set is a trivial `Ok`
    /// even for a bogus signal number, because there is no live member for the bad
    /// number to reach a syscall against — the honesty fix must not turn an empty /
    /// all-drained group into a new error. Matches the cgroup backend, whose
    /// empty-membership broadcast is likewise a no-op `Ok`. Deterministic (no
    /// subprocess), so it runs in CI on every POSIX target.
    #[test]
    fn signal_all_on_an_empty_set_is_ok_even_for_a_bogus_signal() {
        Tracked::new(true)
            .signal_all(BOGUS_SIGNAL)
            .expect("an empty group signals nothing, so a bogus number is a no-op Ok");
        Tracked::new(false)
            .signal_all(BOGUS_SIGNAL)
            .expect("an empty solo set signals nothing either — still a no-op Ok");
    }
}

/// Error paths of the tracked sweep's **delivery** primitive, driven with one
/// `killpg`/`kill` made to fail on demand (`crate::sys::fault_injection`).
///
/// The scenario these exist for — a member that is genuinely alive and not a zombie
/// yet rejects our signal (the `sudo`/setuid child that changed uid) — is the one
/// case this backend must report rather than swallow, and the one that previously
/// needed a privileged host to reproduce at all: every existing test of that
/// discrimination either spawns a real child and settles for the *swallowed*
/// direction, or is `#[ignore]`d behind a real subprocess.
///
/// The tracked member here is **this very process**, so the existence probe, the
/// recycled-pid identity gate and the live/zombie classification of the `EPERM` all
/// run for real against a real, live, identity-stable process; only the delivery
/// itself is injected — and an injected delivery never reaches the kernel, which is
/// what makes it safe to name `SIGSTOP` against our own pid.
///
/// Gated to the targets that *have* a run-state reader: on the BSDs
/// `is_live_non_zombie` is always `false` by design, so a delivery `EPERM` keeps its
/// documented swallowed behavior and there is no discrimination to exercise — the
/// same gating shape the zombie-`EPERM` regression test uses.
#[cfg(all(
    test,
    feature = "process-control",
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
mod delivery_error_paths {
    use super::ProcessGroup;
    use crate::sys::fault_injection::{Faults, Site};

    const SITE: Site = Site::PgroupSignalDelivery;

    /// Arm the faults, build a group whose only tracked member is this process, and
    /// disarm its kill-on-drop backstop.
    ///
    /// Ordering is load-bearing twice over. The faults are armed **first** so they
    /// are still armed when the group drops (locals drop in reverse order), and the
    /// backstop is disarmed **before** anything can panic — between them, no path
    /// out of these tests can deliver a real signal to this process's own group.
    ///
    /// For the same reason every rule passed here must name **no** target, so it
    /// covers both delivery syscalls: a `killpg` that fails `ESRCH` falls back to a
    /// direct `kill`, and a rule narrowed to `killpg` alone would let that fallback
    /// send a real `SIGSTOP` to the test runner.
    fn group_tracking_this_process(
        faults: Faults,
    ) -> (super::super::fault_injection::Armed, ProcessGroup) {
        let armed = faults.arm();
        let group = ProcessGroup::new();
        let epoch = group.skip_drop_kill.begin_shutdown();
        group.skip_drop_kill.request(epoch);
        group
            .groups
            .track(std::process::id() as i32, /* group_seen */ false);
        (armed, group)
    }

    /// A live, non-zombie member that refuses the broadcast is a genuine containment
    /// gap: `suspend` must report it, not answer `Ok` for a tree it never froze.
    #[test]
    fn a_live_member_rejecting_the_broadcast_makes_suspend_fail() {
        let (faults, group) =
            group_tracking_this_process(Faults::new().fail_every(SITE, None, libc::EPERM));

        let err = group
            .suspend()
            .expect_err("a live member that rejects SIGSTOP is a gap, not a success");

        assert!(faults.fired(SITE) >= 1, "the delivery really was refused");
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EPERM),
            "the refusal reaches the caller as itself"
        );

        // What `ProcessGroup::suspend` publishes for it, through the same mapping
        // the public verb applies.
        let public = crate::group::map_unsupported(err, "suspend");
        assert_eq!(
            public.kind(),
            crate::ErrorKind::PermissionDenied,
            "a refused signal is a permission problem — never `Unsupported`, which \
             would claim the mechanism cannot suspend at all"
        );
        match public.reason() {
            crate::ErrorReason::Io(source) => assert_eq!(
                source.raw_os_error(),
                Some(libc::EPERM),
                "the errno survives the public mapping"
            ),
            other => panic!("expected a plain Io failure, got {other:?}"),
        }
    }

    /// `resume` is the twin of `suspend` and must be exactly as honest: the
    /// concurrently-developed backend that quietly returned `Ok` from both is the
    /// regression this pins.
    #[test]
    fn a_live_member_rejecting_the_broadcast_makes_resume_fail() {
        let (faults, group) =
            group_tracking_this_process(Faults::new().fail_every(SITE, None, libc::EPERM));

        let err = group
            .resume()
            .expect_err("a live member that rejects SIGCONT is a gap, not a success");

        assert!(faults.fired(SITE) >= 1);
        assert_eq!(err.raw_os_error(), Some(libc::EPERM));
    }

    /// The fail-safe direction, and why the `EPERM` above cannot simply be surfaced
    /// on errno alone: a member that has already exited (`ESRCH`) is **not** a
    /// failure — a normal teardown of a drained tree must stay `Ok`.
    #[test]
    fn an_already_exited_member_keeps_the_broadcast_successful() {
        let (faults, group) =
            group_tracking_this_process(Faults::new().fail_every(SITE, None, libc::ESRCH));

        group
            .suspend()
            .expect("a target that is already gone is nothing to report");

        assert!(
            faults.fired(SITE) >= 1,
            "the delivery was attempted and refused — the Ok is the classification"
        );
    }

    /// A malformed request (a bad signal number, `EINVAL`) surfaces whatever the
    /// target's state is — the asymmetry with `EPERM`, which is only surfaced
    /// against a positively live member.
    #[test]
    fn a_malformed_request_surfaces_regardless_of_the_targets_state() {
        let (faults, group) =
            group_tracking_this_process(Faults::new().fail_every(SITE, None, libc::EINVAL));

        let err = group
            .suspend()
            .expect_err("a bad signal number is a malformed request, always reported");

        assert!(faults.fired(SITE) >= 1);
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    }
}
