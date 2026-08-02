//! Shared POSIX process-group job.
//!
//! Each spawned child becomes the leader of its own process group, so signalling
//! the negative group id (`killpg`) reaps the child *and* every descendant it
//! forked. This backs two callers:
//!
//! - **Linux** — the fallback when no writable cgroup is available (e.g. a CI
//!   runner without cgroup delegation).
//! - **macOS / the BSDs** — the primary mechanism, since those targets have
//!   neither cgroups nor Job Objects.
//!
//! Weaker than a cgroup or Job Object: a child that calls `setsid` starts a new
//! session and escapes the group. Callers surface this as
//! [`Mechanism::ProcessGroup`](crate::Mechanism::ProcessGroup) so it is never a
//! silent downgrade.

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
fn is_recycled(tracked: Option<u64>, current: Option<u64>) -> bool {
    matches!((tracked, current), (Some(a), Some(b)) if a != b)
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

    /// Whether `id` is currently tracked (cheap membership check — no probe/prune).
    /// Only the `process-control`-gated `adopt` de-dup uses this.
    #[cfg(feature = "process-control")]
    fn contains(&self, id: i32) -> bool {
        self.ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|e| e.id == id)
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
        // Recover a poisoned lock instead of dropping the child from tracking,
        // which would void the kill-on-drop guarantee.
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));
        if !ids.iter().any(|e| e.id == id) {
            // Capture the start-time identity now, while `id` is freshly live, so
            // a later probe can tell the tracked process apart from any process
            // that recycles the number. A de-dup (re-adopt of an id still present
            // above) keeps the existing entry's identity untouched; had the number
            // been recycled since, `probe_entry` would already have pruned the
            // stale entry in the `retain` above, so this pushes a fresh entry
            // carrying the new identity.
            ids.push(Entry {
                id,
                group_seen,
                identity: read_identity(id),
            });
        }
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
            // SAFETY: killpg/kill to a probed-existing id; an exit between the
            // probe and here just yields ESRCH and the sweep continues.
            let delivery = unsafe {
                if self.group {
                    // killpg reaches the leader and every descendant. While the
                    // group has never been seen alive (a forked-but-not-yet-
                    // `setpgid`'d child), killpg yields ESRCH; fall back to a
                    // direct pid signal so the entry drains. ONCE `group_seen`
                    // latched (`probe_entry` set it above), an ESRCH means the
                    // group is genuinely gone — do NOT direct-signal, or that
                    // would SIGKILL a process that recycled the pid.
                    if libc::killpg(id, sig) == -1 {
                        let err = io::Error::last_os_error();
                        if err.raw_os_error() == Some(libc::ESRCH) && !e.group_seen {
                            // Direct-pid fallback: report its own failure, if any.
                            if libc::kill(id, sig) == -1 {
                                Some(io::Error::last_os_error())
                            } else {
                                None
                            }
                        } else {
                            Some(err)
                        }
                    } else {
                        None
                    }
                } else if libc::kill(id, sig) == -1 {
                    Some(io::Error::last_os_error())
                } else {
                    None
                }
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

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
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
        // spared survivors untouched.
        self.skip_drop_kill.clear();
        Ok(guard.disarm())
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
                if !self.groups.contains(pid) {
                    // A new killable solo member joined — re-arm Drop's backstop.
                    self.skip_drop_kill.clear();
                    self.solos.track(pid, false);
                }
                Ok(())
            }
            _ => Err(err),
        }
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
            let pid = pid as i32;
            // Best-effort hard kill of the still-untracked child. It is its own
            // group leader (or, on the `setsid` path, a session leader), so killpg
            // reaps it *and* any descendant it forked in this tiny window; if it
            // has not `setpgid`'d yet the group id does not exist (killpg → ESRCH),
            // so fall back to a direct pid kill. `pid` was just spawned — never a
            // recycled alias — so the kill is safe.
            // SAFETY: killpg/kill delivering SIGKILL to a freshly-spawned id.
            unsafe {
                if libc::killpg(pid, libc::SIGKILL) == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
        // Dropping the tokio `Child` hands the killed process to tokio's orphan
        // reaper, so it is waited (no zombie leak) without this guard blocking.
        drop(child);
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
