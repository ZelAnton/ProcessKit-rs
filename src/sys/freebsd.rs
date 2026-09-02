//! Implementation for FreeBSD: the kernel **process reaper** — `procctl(2)`'s
//! `PROC_REAP_ACQUIRE` — layered over the shared POSIX process-group backend.
//!
//! FreeBSD is the one unix outside Linux with a real whole-tree containment
//! primitive. Acquiring reaper status makes this process the *reaper* of its
//! entire descendant tree: every descendant, however deeply forked and whether or
//! not it called `setsid`, stays inside that tree, can be enumerated
//! (`PROC_REAP_GETPIDS`) and can be signalled in one call (`PROC_REAP_KILL`).
//! That closes the documented escape hatch of the plain process-group backend (a
//! child that `setsid`s out of the group `killpg` addresses) and is surfaced as
//! [`Mechanism::ProcessReaper`], never as a silent upgrade.
//!
//! # What this layer adds, and what it reuses
//!
//! Everything about *starting* a child stays with [`ProcessGroup`] (`sys::pgroup`):
//! the `setpgid`/`setsid` coordination, the untracked-child spawn guard, the
//! liveness/identity bookkeeping, the kill-on-drop backstop, the graceful-shutdown
//! `SkipDropKill` latch. This module adds exactly the whole-tree semantics on top:
//!
//! - **membership** comes from `PROC_REAP_GETPIDS` (the real tree) instead of the
//!   tracked group leaders,
//! - **delivery** (`kill_all`, `signal`, `suspend`/`resume`, the graceful tiers)
//!   goes through `PROC_REAP_KILL`, which reaches the escapees `killpg` cannot.
//!
//! # Scope: one reaper per *process*, many jobs
//!
//! Reaper status is a property of a **process**, not of a container object: there
//! is no way to open several independent reaper scopes inside one process. So this
//! backend acquires it once, process-wide ([`acquire_reaper_status`]), and scopes
//! each [`Job`] with the kernel's own subtree tag instead. Every process forked by
//! this process roots a *subtree* identified by its own pid (`pi_subtree ==
//! pi_pid`), and every descendant of that child carries that pid in `pi_subtree`
//! for life. A `Job` therefore records the pids of the children **it** started
//! ([`Reaper::roots`]) and addresses only those subtrees — `PROC_REAP_KILL` with
//! `REAPER_KILL_SUBTREE`, membership filtered by `pi_subtree`. One job can never
//! kill or count another job's tree, nor a child the embedding application spawned
//! for itself.
//!
//! Reaper status is acquired lazily (first [`Job::new`]) and **never released**.
//! Releasing it would be actively wrong: the flag is shared by every live `Job` in
//! the process — and possibly by the application, which may have acquired it before
//! us (`EBUSY`, treated as success) — so dropping one job must not strip containment
//! from the others. It also owns no resource; it is a process flag whose only
//! effect is the re-parenting described next.
//!
//! # The obligation this takes on: re-parented orphans
//!
//! Being the reaper means an orphaned descendant — one whose parent dies first, the
//! classic daemonising double-fork — is re-parented to **us** instead of to `init`.
//! That is precisely the containment we want (the orphan stays in the tree), but it
//! transfers `init`'s duty along with it: when such a process exits it becomes a
//! zombie *of this process* and someone must `wait` for it. Nothing else will —
//! tokio only reaps the children it spawned itself.
//!
//! [`reap_stray_zombies`] discharges that duty, and does so without ever touching a
//! process some `Child` handle owns: it reaps only entries with `pi_subtree !=
//! pi_pid`, i.e. processes this process did **not** fork itself. (The kernel's
//! `REAPER_PIDINFO_CHILD` flag is *not* the right test — it means "is currently a
//! direct child", which a re-parented orphan also is.) The sweep runs on every
//! reaper read — membership, delivery, teardown — so an ordinary job discharges the
//! duty as a side effect of being used. `Drop` adds a short bounded drain
//! ([`Reaper::drain_dead`]) on top, because it is the one moment after which no
//! later read will ever come.
//!
//! # What is still *not* provided
//!
//! - **Resource accounting/limits.** A reaper is a process-tree facility, not a
//!   container: there is no aggregate memory/CPU/pids counter, so `limits` stays
//!   `Unsupported` exactly as on the plain process-group backend.
//! - **Per-process metrics and member metadata.** No `/proc` and no wired-up
//!   `kinfo_proc` reader (see `pgroup::read_identity` for why), so
//!   [`process_metrics`] returns defaults and a member's ppid/image/start time are
//!   honestly `None`.

use std::io;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

// The dispatcher in `sys/mod.rs` picks one backend per target through a stack of
// mutually-exclusive `#[cfg_attr(..., path = ...)]` arms. That exclusivity is an
// argument about `cfg` predicates, not something the compiler otherwise checks —
// so each of the two arms this file's introduction had to disambiguate asserts
// its own target here (see the twin guard in `unix.rs`). A cross-target `cargo
// check` then *proves* the routing rather than merely tolerating it: were this
// module ever selected for macOS or another BSD, that build would fail loudly
// instead of silently issuing FreeBSD-only syscalls there.
#[cfg(not(target_os = "freebsd"))]
compile_error!(
    "sys::freebsd is the FreeBSD-only backend (it calls procctl(2)); the platform \
     dispatcher in sys/mod.rs must not select it for any other target"
);

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::{LimitEvidence, ResourceLimits};
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
use crate::sys::pgroup::ProcessGroup;
#[cfg(feature = "stats")]
use crate::sys::{ProcIdentity, ProcMetrics};

// ---------------------------------------------------------------------------
// `procctl(2)` reaper ABI
//
// `libc` declares the `procctl` entry point and the `PROC_REAP_*` command
// numbers, but none of the four data structures the reaper commands exchange nor
// their flag bits. They are mirrored here from `<sys/procctl.h>`; the layout is
// pinned by `reaper_abi_layout_matches_the_kernel_headers` below so a silent
// mismatch (which would hand the kernel a wrongly-sized buffer) fails loudly in
// the FreeBSD test job rather than corrupting memory.
// ---------------------------------------------------------------------------

/// `REAPER_STATUS_OWNED` — the queried process holds reaper status itself
/// (as opposed to merely belonging to some other process's reaper tree).
const REAPER_STATUS_OWNED: libc::c_uint = 0x0000_0001;

/// `REAPER_PIDINFO_VALID` — this array slot was filled in by the kernel. The
/// `PROC_REAP_GETPIDS` call reports no element count, so a zeroed slot is how the
/// filled prefix ends.
const REAPER_PIDINFO_VALID: libc::c_uint = 0x0000_0001;

/// `REAPER_PIDINFO_ZOMBIE` — the descendant has exited and is waiting to be
/// `wait`ed for. Reported since FreeBSD 12.1; on an older kernel the bit is simply
/// never set, which costs the zombie sweep a candidate but can never mislabel a
/// live process as dead.
const REAPER_PIDINFO_ZOMBIE: libc::c_uint = 0x0000_0008;

/// `REAPER_KILL_SUBTREE` — deliver only to the descendants of `rk_subtree`
/// (the single direct child that roots the subtree), not to the whole tree.
const REAPER_KILL_SUBTREE: libc::c_uint = 0x0000_0002;

/// `struct procctl_reaper_status` (`PROC_REAP_STATUS`).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
// `rs_children`/`rs_pid`/`rs_pad0` are part of the kernel's ABI but are not read
// by this backend; they must still occupy their exact slots in the struct.
#[allow(dead_code)]
struct ReaperStatus {
    rs_flags: libc::c_uint,
    rs_children: libc::c_uint,
    rs_descendants: libc::c_uint,
    rs_reaper: libc::pid_t,
    rs_pid: libc::pid_t,
    rs_pad0: [libc::c_uint; 15],
}

impl ReaperStatus {
    const ZERO: Self = Self {
        rs_flags: 0,
        rs_children: 0,
        rs_descendants: 0,
        rs_reaper: 0,
        rs_pid: 0,
        rs_pad0: [0; 15],
    };
}

/// `struct procctl_reaper_pidinfo` — one descendant, as reported by
/// `PROC_REAP_GETPIDS`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// `pi_pad0` is ABI padding the kernel owns.
#[allow(dead_code)]
struct ReaperPidInfo {
    pi_pid: libc::pid_t,
    /// The pid of the direct child of this reaper that roots the subtree this
    /// process belongs to. Fixed at fork and **never** rewritten, not even when the
    /// process is later re-parented — which is what makes it, and not the
    /// `REAPER_PIDINFO_CHILD` flag, the reliable "did we fork this ourselves?" test.
    pi_subtree: libc::pid_t,
    pi_flags: libc::c_uint,
    pi_pad0: [libc::c_uint; 15],
}

impl ReaperPidInfo {
    const ZERO: Self = Self {
        pi_pid: 0,
        pi_subtree: 0,
        pi_flags: 0,
        pi_pad0: [0; 15],
    };

    /// Whether the kernel actually filled this slot (see [`REAPER_PIDINFO_VALID`]).
    fn is_valid(&self) -> bool {
        self.pi_flags & REAPER_PIDINFO_VALID != 0
    }

    /// Whether the descendant has already exited and awaits a `wait(2)`.
    fn is_zombie(&self) -> bool {
        self.pi_flags & REAPER_PIDINFO_ZOMBIE != 0
    }

    /// Whether **this process forked it directly** — the subtree it belongs to is
    /// the one it roots. Such a process is owned by some `Child` handle (a tokio
    /// child of a job, or a detached/foreign child of the embedding application),
    /// so the zombie sweep must never `wait` for it: that would steal the exit
    /// status its owner is waiting for and free the pid for reuse behind its back.
    fn is_own_fork(&self) -> bool {
        self.pi_pid == self.pi_subtree
    }
}

/// `struct procctl_reaper_pids` — the `PROC_REAP_GETPIDS` request header.
#[repr(C)]
#[derive(Debug)]
struct ReaperPids {
    rp_count: libc::c_uint,
    rp_pad0: [libc::c_uint; 15],
    rp_pids: *mut ReaperPidInfo,
}

/// `struct procctl_reaper_kill` — the `PROC_REAP_KILL` request/response.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
// `rk_killed` (how many processes were signalled) is an output this backend does
// not need; it still occupies its ABI slot.
#[allow(dead_code)]
struct ReaperKill {
    rk_sig: libc::c_int,
    rk_flags: libc::c_uint,
    rk_subtree: libc::pid_t,
    rk_killed: libc::c_uint,
    /// The first pid delivery failed for — the kernel copies this back **even on
    /// error**, which is what lets the `EPERM` classification below name the
    /// offending member instead of guessing.
    rk_fpid: libc::pid_t,
    rk_pad0: [libc::c_uint; 15],
}

/// This process's pid, as the `pid_t` the reaper ABI speaks.
fn self_pid() -> libc::pid_t {
    // SAFETY: `getpid` takes no arguments, touches no memory and cannot fail.
    unsafe { libc::getpid() }
}

/// Issue a `procctl(2)` reaper command against **this** process.
///
/// `data` must be null for the commands that take none (the kernel rejects a
/// non-null pointer there with `EINVAL`), and otherwise a valid, fully-initialized
/// instance of that command's struct.
fn procctl_self(cmd: libc::c_int, data: *mut libc::c_void) -> io::Result<()> {
    // SAFETY: `cmd` is one of the `PROC_REAP_*` commands and `data` upholds the
    // contract above — every caller below passes either `null_mut()` or a pointer
    // to a live, correctly-typed local that outlives the call. The reaper commands
    // are addressed to this very process (`P_PID` + our own pid), which is the only
    // target `PROC_REAP_ACQUIRE` accepts at all.
    let rc = unsafe { libc::procctl(libc::P_PID, self_pid() as libc::id_t, cmd, data) };
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Read this process's reaper status (`PROC_REAP_STATUS`).
fn reap_status() -> io::Result<ReaperStatus> {
    let mut status = ReaperStatus::ZERO;
    procctl_self(
        libc::PROC_REAP_STATUS,
        std::ptr::addr_of_mut!(status).cast::<libc::c_void>(),
    )?;
    Ok(status)
}

/// Make this process the reaper of its descendant tree, **once** per process, and
/// report whether it now genuinely holds that status.
///
/// Two non-failures are folded into success: `EBUSY` means the process is *already*
/// a reaper (a second `Job`, or an application that acquired the status before this
/// crate did), and being `init` in a jail has the same effect from birth. Either way
/// the containment this backend needs is in place, which is why the outcome is
/// confirmed by reading `PROC_REAP_STATUS` back rather than inferred from the return
/// code. A genuine failure is not fatal: the caller degrades to the plain
/// process-group backend and reports [`Mechanism::ProcessGroup`], so the mechanism
/// query never overstates the containment actually in force.
fn acquire_reaper_status() -> bool {
    static ACQUIRED: OnceLock<bool> = OnceLock::new();
    *ACQUIRED.get_or_init(|| {
        match procctl_self(libc::PROC_REAP_ACQUIRE, std::ptr::null_mut()) {
            Ok(()) => {}
            Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {}
            Err(_) => return false,
        }
        reap_status()
            .is_ok_and(|s| s.rs_flags & REAPER_STATUS_OWNED != 0 || s.rs_reaper == self_pid())
    })
}

/// Extra slots requested over the descendant count `PROC_REAP_STATUS` just
/// reported, so the common case of a child forked between the two calls is
/// absorbed without a second round trip.
const GETPIDS_SLACK: usize = 16;

/// How many times the descendant listing may double its buffer before settling for
/// what it got. A tree that keeps outgrowing four doublings is being forked into
/// faster than it can be read, so a best-effort answer beats failing the read — but
/// that answer is **marked** ([`Listing::truncated`]) rather than passed off as
/// complete: under-reporting membership is harmless, while letting a partial list
/// drive [`Reaper::prune`] would read "no entry names this root" as "that subtree is
/// empty" and forget a live subtree the listing never reached.
const GETPIDS_GROW_ATTEMPTS: u32 = 4;

/// Ceiling on a single listing, so a runaway `rs_descendants` cannot turn into an
/// unbounded allocation. Far above any realistic tree — FreeBSD's default
/// system-wide process limit is well under this.
const GETPIDS_MAX: usize = 1 << 20;

/// One `PROC_REAP_GETPIDS` answer: the descendants the kernel reported, plus whether
/// it may have had more to say than the buffer could hold.
///
/// The flag is load-bearing, not diagnostic — see [`GETPIDS_GROW_ATTEMPTS`] and
/// [`Reaper::prune`], which is a no-op on a truncated listing. Every *reader* of the
/// entries (membership, the zombie sweep, the liveness probe behind the `EPERM`
/// discrimination) is safe with a partial list: each of them fails towards
/// "fewer members / cannot prove liveness", never towards forgetting containment.
struct Listing {
    entries: Vec<ReaperPidInfo>,
    /// The kernel filled the buffer exactly, with no growth attempts left, so there
    /// may be descendants it could not report.
    truncated: bool,
}

impl Listing {
    /// The listing of a process the kernel says has no descendants at all — complete
    /// by construction.
    const EMPTY: Self = Self {
        entries: Vec::new(),
        truncated: false,
    };
}

/// Every descendant of this process, as the kernel's reaper tree sees it —
/// including ones that `setsid`ed away, ones re-parented to us, and zombies.
///
/// `PROC_REAP_GETPIDS` reports no element count, so the returned prefix is
/// delimited by [`REAPER_PIDINFO_VALID`]: the buffer is zeroed up front and the
/// kernel sets that bit on every slot it writes.
fn descendants() -> io::Result<Listing> {
    // The status read is one cheap syscall that both sizes the buffer and lets a
    // childless process skip the listing (and its allocation) entirely.
    let status = reap_status()?;
    if status.rs_descendants == 0 {
        return Ok(Listing::EMPTY);
    }
    let mut capacity = (status.rs_descendants as usize)
        .saturating_add(GETPIDS_SLACK)
        .min(GETPIDS_MAX);
    let mut attempt = 0;
    loop {
        let mut buf = vec![ReaperPidInfo::ZERO; capacity];
        let mut request = ReaperPids {
            rp_count: capacity as libc::c_uint,
            rp_pad0: [0; 15],
            rp_pids: buf.as_mut_ptr(),
        };
        procctl_self(
            libc::PROC_REAP_GETPIDS,
            std::ptr::addr_of_mut!(request).cast::<libc::c_void>(),
        )?;
        let filled = buf.iter().take_while(|entry| entry.is_valid()).count();
        buf.truncate(filled);
        // A filled prefix shorter than the buffer proves the listing is complete;
        // an exactly-full buffer may have been truncated, so grow and re-read —
        // and, once the attempts are spent, say so instead of pretending.
        attempt += 1;
        if filled < capacity {
            return Ok(Listing {
                entries: buf,
                truncated: false,
            });
        }
        if attempt >= GETPIDS_GROW_ATTEMPTS || capacity >= GETPIDS_MAX {
            return Ok(Listing {
                entries: buf,
                truncated: true,
            });
        }
        capacity = capacity.saturating_mul(2).min(GETPIDS_MAX);
    }
}

/// Whether `entry` is a live member of the tree rooted at one of `roots`.
///
/// A zombie is excluded: it is a dead process awaiting a `wait`, and reporting it
/// as a member would make `members()`/`is_drained` claim a tree is still up after
/// everything in it has exited.
fn is_member(entry: &ReaperPidInfo, roots: &[libc::pid_t]) -> bool {
    entry.is_valid() && !entry.is_zombie() && roots.contains(&entry.pi_subtree)
}

/// Whether `entry` is a zombie this process must `wait` for — a descendant it did
/// **not** fork itself, which therefore reached us only by being re-parented when
/// its own parent died (see the module docs). Deliberately *not* restricted to any
/// one job's roots: the re-parenting happens because this process is the reaper at
/// all, so the duty spans the whole process, and a corpse left by a job that has
/// since been dropped must still be collected by whoever sweeps next.
fn is_stray_zombie(entry: &ReaperPidInfo) -> bool {
    entry.is_valid() && entry.is_zombie() && !entry.is_own_fork()
}

/// Whether any process in `roots`' subtrees is still alive **below** its root —
/// the condition [`Reaper::drain_dead`] waits out. Roots themselves are excluded:
/// they are owned by a `Child` handle that does its own reaping, so waiting for one
/// to disappear would be waiting on someone else's `wait`.
fn has_live_descendant(all: &[ReaperPidInfo], roots: &[libc::pid_t]) -> bool {
    all.iter()
        .any(|entry| !entry.is_own_fork() && is_member(entry, roots))
}

/// `wait` for every re-parented corpse in `all` (see [`is_stray_zombie`]).
///
/// `WNOHANG` keeps this non-blocking, and a pid that is not (or is no longer) our
/// child simply answers `ECHILD` — so the sweep is safe to run from any thread, as
/// often as convenient, and two jobs sweeping concurrently just race to a harmless
/// no-op.
fn reap_stray_zombies(all: &[ReaperPidInfo]) {
    for entry in all.iter().filter(|entry| is_stray_zombie(entry)) {
        let mut status: libc::c_int = 0;
        // SAFETY: `waitpid` with `WNOHANG` never blocks and writes at most one
        // `c_int` through the status pointer, which points at a live local.
        let _ = unsafe { libc::waitpid(entry.pi_pid, &mut status, libc::WNOHANG) };
    }
}

/// Whether `pid` is a positively **live, non-zombie** descendant right now — the
/// same discrimination `pgroup::is_live_non_zombie` makes on the platforms that can
/// (see K-055): only against such a target is a delivery `EPERM` a genuine
/// containment gap worth surfacing, rather than the harmless "the target was
/// already dead" case that must stay `Ok`.
///
/// The reaper listing supplies here what a bare BSD cannot: the kernel's own
/// zombie flag for a process we can positively identify as ours. Anything less than
/// a positive live answer — an unreadable listing, a pid the tree no longer knows,
/// a pid a truncated listing did not reach, a kernel too old to report
/// `REAPER_PIDINFO_ZOMBIE` for a corpse — reports
/// `false` and the error stays swallowed, which is the fail-safe direction and
/// exactly the pre-existing behavior on this target.
fn is_live_descendant(pid: libc::pid_t) -> bool {
    if pid <= 0 {
        return false;
    }
    descendants().is_ok_and(|listing| {
        listing
            .entries
            .iter()
            .any(|entry| entry.pi_pid == pid && entry.is_valid() && !entry.is_zombie())
    })
}

/// How long [`Drop`](Job::drop) may block waiting for the tree it just `SIGKILL`ed
/// to actually die, so the re-parented corpses can be collected before the last
/// sweeper — this `Job` — is gone.
///
/// This wait is what makes "the tree is gone once the job is dropped" true on this
/// backend rather than nearly true: an orphan the reaper inherited stays visible to
/// `kill(pid, 0)` until *this* process `wait`s for it, and after a `Drop` there is
/// no later call to sweep it. A killed process has no handler to run and dies as
/// soon as it is scheduled, so the loop normally ends within a poll or two; the
/// budget caps only pathological cases (a member wedged in uninterruptible sleep,
/// or one that genuinely rejects the kill), and it is not entered at all unless the
/// job really has descendants below its roots, which the ordinary one-child job
/// does not.
///
/// **The number is the project's accepted ceiling, not a fresh judgement.** `Drop`
/// cannot await, so this runs synchronously wherever the job is dropped — often a
/// tokio worker thread. The Linux cgroup backend blocks the same way and for the
/// same reason (`src/sys/linux.rs`: 50 polls × 2 ms while `cgroup.procs` drains),
/// and ~100 ms is the bound the crate documents wherever a teardown may stall a
/// worker thread. A
/// larger budget here would buy nothing measurable — a `SIGKILL`ed tree dies within
/// a poll or two, and anything that outlasts 100 ms of polling is a tree that will
/// not die within 500 ms either — while costing five times the worst-case stall on
/// an executor thread. The verbs reachable from async code (`kill_all`,
/// `GracefulTarget::hard_kill`) deliberately do **not** drain at all: they leave a
/// live `Job` behind, and every later reaper read sweeps the corpses anyway.
const DRAIN_BUDGET: Duration = Duration::from_millis(100);

/// Poll interval for that drain — mirroring the Linux loop's 2 ms rather than
/// spinning twice as fast, since each poll here costs two syscalls and a killed
/// tree is gone long before the difference shows.
const DRAIN_POLL: Duration = Duration::from_millis(2);

/// One subtree root: the pid of a child this job started, plus the order in which
/// it was recorded. See [`Reaper::prune`] for what the sequence number is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Root {
    pid: libc::pid_t,
    seq: u64,
}

/// This job's subtree roots and the counter that stamps them.
#[derive(Debug, Default)]
struct RootSet {
    items: Vec<Root>,
    next_seq: u64,
}

/// The reaper scope of one [`Job`]: whether this process holds reaper status at
/// all, plus the subtree roots this job owns.
struct Reaper {
    /// Whether `procctl(PROC_REAP_ACQUIRE)` succeeded for this process. `false`
    /// degrades every method here to the plain process-group backend.
    active: bool,
    /// Latched when this job took on a member the reaper provably cannot see, so
    /// the process-group layer must stay engaged alongside it (see
    /// [`Job::adopt`]).
    ///
    /// Only an `adopt` can do this, and only in one specific way. `PROC_REAP_ACQUIRE`
    /// attaches *future* forks to the new reaper and deliberately leaves the
    /// existing ones where they were ("we do not reattach existing children", says
    /// the kernel), so a child spawned by the embedding application **before** this
    /// crate created its first `ProcessGroup` still belongs to the previous reaper —
    /// usually `init`. Everything a job spawns itself is forked after acquisition
    /// and is therefore always inside.
    outside_members: std::sync::atomic::AtomicBool,
    /// The pids of the children this job started or adopted; each roots one reaper
    /// subtree (see the module docs).
    ///
    /// Deliberately **not** the process group backend's tracked-id list, which is
    /// pruned as soon as its *process group* drains: that is precisely the moment a
    /// `setsid` escapee stops being reachable through `killpg`, so pruning on it
    /// would forget the subtree exactly when the reaper is the only thing that can
    /// still reach into it. A root is dropped only on the kernel's own positive
    /// answer that its subtree holds nothing — an empty descendant listing
    /// ([`Reaper::prune`]) or an `ESRCH` from `PROC_REAP_KILL`
    /// ([`Reaper::signal_tree`]) — and never merely because time passed.
    roots: Mutex<RootSet>,
}

impl Reaper {
    fn new() -> Self {
        Self {
            active: acquire_reaper_status(),
            outside_members: std::sync::atomic::AtomicBool::new(false),
            roots: Mutex::new(RootSet::default()),
        }
    }

    /// Whether this job holds a member outside the reaper's tree (see
    /// [`outside_members`](Self::outside_members)). Read on every whole-tree verb,
    /// so `Relaxed` is deliberate: the latch is one-way and the only ordering that
    /// matters — "the adopt that set it happened before this call" — is already
    /// established by the caller's own `&self` sequencing.
    fn has_outside_members(&self) -> bool {
        self.outside_members
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Latch that this job took on a member the reaper cannot reach.
    #[cfg(feature = "process-control")]
    fn mark_outside_member(&self) {
        self.outside_members
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether the reaper's tree currently contains `pid` at all — the test that
    /// tells an adopted child forked *after* acquisition (contained) from one
    /// forked before it (not ours to reach).
    #[cfg(feature = "process-control")]
    fn covers(&self, pid: libc::pid_t) -> bool {
        descendants().is_ok_and(|listing| {
            listing
                .entries
                .iter()
                .any(|entry| entry.pi_pid == pid && entry.is_valid())
        })
    }

    /// Record a freshly-started child as a subtree root, **after** forgetting the
    /// roots the kernel reports as empty — the "prune, then track" order
    /// `pgroup::Tracked::track` uses, and for the same reason. A spawn is precisely
    /// the moment a pid this job still remembers can be handed out again, so it is
    /// the moment a stale root is most expensive to keep (see [`prune`](Self::prune)).
    ///
    /// De-duplicated by pid — re-adopting a child this job already owns cannot
    /// double-count it — with the *fresh* entry winning; see
    /// [`insert_root`](Self::insert_root).
    fn record(&self, pid: libc::pid_t) {
        if !self.active || pid <= 0 {
            return;
        }
        // Stamp first, then read, then lock — the ordering every pruning caller
        // here uses, so a root recorded by another thread meanwhile is never judged
        // by a listing that could not have contained it.
        let since = self.seq_mark();
        let listing = descendants().ok();
        self.insert_root(pid, listing.as_ref(), since);
    }

    /// The bookkeeping half of [`record`](Self::record), split out so the unit tests
    /// can drive it with a synthetic listing instead of the live kernel.
    ///
    /// `listing` is `None` when the tree could not be read at all; nothing is pruned
    /// then, because under-pruning merely keeps a stale root while over-pruning
    /// drops containment.
    ///
    /// A new entry always **replaces** an existing one for the same pid instead of
    /// deferring to it, which matters for one specific race: if this job still holds
    /// a stale root for a number the OS has just recycled into this very child, the
    /// old entry would keep its old stamp — and a concurrent membership read whose
    /// [`seq_mark`](Self::seq_mark) predates this call would then prune that root as
    /// "empty", forgetting the child that was just started. A fresh stamp is exactly
    /// what tells such a reader "recorded after your listing; keep it".
    fn insert_root(&self, pid: libc::pid_t, listing: Option<&Listing>, since: u64) {
        let mut roots = self.lock_roots();
        if let Some(listing) = listing {
            Self::prune_locked(&mut roots, listing, since);
        }
        roots.items.retain(|root| root.pid != pid);
        let seq = roots.next_seq;
        roots.next_seq += 1;
        roots.items.push(Root { pid, seq });
    }

    /// Recover a poisoned lock rather than dropping the roots, which would void
    /// this job's share of the kill-on-drop guarantee.
    fn lock_roots(&self) -> std::sync::MutexGuard<'_, RootSet> {
        self.roots.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn roots(&self) -> Vec<libc::pid_t> {
        self.lock_roots()
            .items
            .iter()
            .map(|root| root.pid)
            .collect()
    }

    /// The roots *with* their stamps — what a delivery sweep iterates, so that a
    /// root it drops afterwards can be matched by identity and not merely by number
    /// (see [`forget`](Self::forget)).
    fn root_snapshot(&self) -> Vec<Root> {
        self.lock_roots().items.clone()
    }

    /// Forget one specific root, matched by its stamp as well as its pid: if a
    /// concurrent [`record`](Self::record) replaced the entry in between — a new
    /// child that inherited the recycled number — the replacement stays.
    fn forget(&self, root: Root) {
        self.lock_roots().items.retain(|item| *item != root);
    }

    /// `SIGKILL` everything under one root — the containment-scoped teardown a
    /// failed PTY setup rolls back with, i.e. [`signal_tree`](Self::signal_tree)'s
    /// `PROC_REAP_KILL`/[`REAPER_KILL_SUBTREE`] delivery aimed at a single subtree
    /// instead of every root this job owns (a failed spawn must not touch the rest
    /// of the job).
    ///
    /// This is what makes the rollback's reach the mechanism's maximum rather than
    /// `killpg`'s: a descendant that forked and called `setsid` inside the setup
    /// window has left the process group but not the reaper's subtree, and the
    /// kernel walks it here.
    ///
    /// # A root that is already dead, and why nothing is forgotten afterwards
    ///
    /// The rollback runs because the *master wiring* failed, not because the child
    /// misbehaved, so nothing guarantees the child is still alive when it runs: if
    /// it exited on its own first, this call aims at a zombie root (its number
    /// pinned by the un-reaped `Child` the guard still owns). That does **not**
    /// narrow the walk. A subtree is not looked up through its root process — the
    /// kernel tags every descendant with the pid of the reaper's direct child that
    /// began its subtree (`p_reapsubtree`), a tag that outlives the root: an orphan
    /// simply re-parents onto this process, *its reaper*, carrying the tag with it.
    /// This is executed on every FreeBSD CI run, not merely read out of
    /// `kern_procctl.c` —
    /// `groups::freebsd_reaper::a_setsid_escapee_stays_contained` orphans a `setsid`
    /// escapee, **reaps** its parent (a stronger state than the zombie here: the
    /// number is fully released), and then asserts both that `PROC_REAP_GETPIDS`
    /// still reports the escapee under that dead root and that a `PROC_REAP_KILL`
    /// aimed at it still kills the escapee.
    ///
    /// Even so, this call's result decides **no bookkeeping**: it forgets no root,
    /// and neither does its caller. The reason is the outcomes it cannot rule out —
    /// an `EPERM` from a member this job may not signal, an unexpected refusal, or
    /// an `ESRCH` that means "the subtree already drained" and cannot be told apart
    /// from a hypothetical "the walk found nothing to walk". A root is instead
    /// released only by the ordinary [`prune`](Self::prune), i.e. only once the
    /// kernel itself reports nothing left under it — which is the rule the `roots`
    /// field states for every root, and which a `forget` on this path would have
    /// been the sole exception to. The cost is a root that may linger until the next
    /// prune (every spawn, every membership read, every delivery sweep); the
    /// alternative — dropping a root while a `setsid` escapee still lives under it —
    /// would put that escapee permanently beyond `kill_all`/`Drop`, and no
    /// error-path tidiness is worth a leaked live process.
    ///
    /// What is left is therefore bounded on both sides: the walk reaches this
    /// spawn's whole subtree, and anything it *fails* to kill stays reachable to the
    /// job's own teardown. An ordinary (non-`setsid`) descendant has a third layer
    /// besides: the process-group layer's `killpg` runs right after this, and a
    /// process group outlives its leader.
    ///
    /// Deliberately unbracketed by the pruning [`signal_tree`](Self::signal_tree)
    /// pays for: the caller holds the child's un-reaped `Child`, so this root
    /// cannot have been recycled and the recycled-number hazard that prune defends
    /// against does not exist on this path.
    #[cfg(feature = "pty")]
    fn hard_kill_subtree(&self, root: libc::pid_t) {
        if !self.active {
            return; // no reaper status: the process-group layer is the mechanism.
        }
        let mut request = ReaperKill {
            rk_sig: libc::SIGKILL,
            rk_flags: REAPER_KILL_SUBTREE,
            rk_subtree: root,
            rk_killed: 0,
            rk_fpid: 0,
            rk_pad0: [0; 15],
        };
        // Every outcome is swallowed *because* none of them is actionable here, not
        // for convenience: an `ESRCH` means the subtree held nothing to signal, and
        // an `EPERM` (a member this job may not signal) or any other refusal leaves
        // survivors — and in every case the response is the one this method already
        // commits to, namely keeping the root so the job's own teardown still
        // reaches whatever is left.
        let _ = procctl_self(
            libc::PROC_REAP_KILL,
            std::ptr::addr_of_mut!(request).cast::<libc::c_void>(),
        );
    }

    /// The stamp a root recorded from now on will carry — read **before** a
    /// descendant listing so [`prune`](Self::prune) can tell which roots that
    /// listing had a chance to see.
    fn seq_mark(&self) -> u64 {
        self.lock_roots().next_seq
    }

    /// Forget every root whose subtree the kernel no longer knows anything about —
    /// no live member, no zombie, nothing.
    ///
    /// This is the recycled-number defence, and it is the *only* one this platform
    /// offers: `pgroup::Tracked` can additionally identity-gate an id before
    /// signalling it, but `read_identity` is `None` on the BSDs, so here promptness
    /// is the whole mitigation. A subtree is named by a pid; once everything under
    /// that pid is gone and the number is reaped, the OS can hand it to a new child
    /// of this same process — another job's, or one the embedding application forked
    /// for itself — and a root kept past that point would alias the newcomer's
    /// subtree, so a later `PROC_REAP_KILL` would walk into a tree that is not this
    /// job's.
    ///
    /// The defence is therefore run at every point that could make a root stale or
    /// act on one: on every `spawn`/`adopt` ([`record`](Self::record)), on every
    /// membership read ([`tree`](Self::tree)), immediately before every delivery
    /// sweep and again on its `ESRCH`es ([`signal_tree`](Self::signal_tree)), and
    /// throughout the teardown drain ([`drain_dead`](Self::drain_dead)). What
    /// remains is a genuine but narrow window: the root's subtree must drain **and**
    /// its number be recycled between two of this job's reaper calls, with no spawn,
    /// membership read or delivery in between. Even then the mistake is confined to
    /// another tree of this same process (a `PROC_REAP_KILL` can only ever reach our
    /// own descendants), never to an unrelated process the way a recycled *pgid*
    /// could.
    ///
    /// Two things deliberately do **not** prune:
    ///
    /// - a **truncated** listing ([`Listing::truncated`]) — it proves nothing about
    ///   the roots it never reached, and treating its silence as "empty" would drop
    ///   live subtrees precisely when the tree is forking fastest;
    /// - a root stamped at or after `since`, the [`seq_mark`](Self::seq_mark) taken
    ///   before `listing` was read. Such a root was recorded by a concurrent
    ///   `spawn`/`adopt` the listing could not possibly contain, so pruning it would
    ///   drop the brand-new child's subtree and silently narrow this job's teardown
    ///   to what `killpg` can reach.
    fn prune(&self, listing: &Listing, since: u64) {
        Self::prune_locked(&mut self.lock_roots(), listing, since);
    }

    /// [`prune`](Self::prune)'s body, for the callers that already hold the lock.
    fn prune_locked(roots: &mut RootSet, listing: &Listing, since: u64) {
        if listing.truncated {
            return;
        }
        roots.items.retain(|root| {
            root.seq >= since
                || listing
                    .entries
                    .iter()
                    .any(|entry| entry.pi_subtree == root.pid)
        });
    }

    /// The live members of this job's subtrees, pruning emptied roots and
    /// collecting re-parented corpses on the way.
    ///
    /// Gated on the union of the features whose verbs read membership
    /// (`members`/`members_info` and `stats`), not on either one alone — the same
    /// "gate an internal on the union of its callers" rule the Windows backend's
    /// shared member-snapshot helpers follow. Without the gate a
    /// `--no-default-features` build warns this method is never used.
    #[cfg(any(feature = "process-control", feature = "stats"))]
    fn tree(&self) -> io::Result<Vec<ReaperPidInfo>> {
        self.read_tree(true)
    }

    /// [`tree`](Self::tree) without the root pruning — for the graceful driver's
    /// probe-only reads, whose contract forbids mutating the tracked set. The
    /// zombie sweep still runs: it collects corpses from the OS and touches no
    /// tracked state.
    fn tree_probe(&self) -> io::Result<Vec<ReaperPidInfo>> {
        self.read_tree(false)
    }

    fn read_tree(&self, prune: bool) -> io::Result<Vec<ReaperPidInfo>> {
        let listing = self.read_listing(prune)?;
        let roots = self.roots();
        Ok(listing
            .entries
            .into_iter()
            .filter(|entry| is_member(entry, &roots))
            .collect())
    }

    /// One reaper read: collect the re-parented corpses it exposes and, unless the
    /// caller is bound by the probe-only contract, prune the roots it proves empty.
    fn read_listing(&self, prune: bool) -> io::Result<Listing> {
        let since = self.seq_mark();
        let listing = descendants()?;
        reap_stray_zombies(&listing.entries);
        if prune {
            self.prune(&listing, since);
        }
        Ok(listing)
    }

    /// Drop the roots the kernel has forgotten, discarding the listing — the
    /// pre-delivery half of the defence described on [`prune`](Self::prune).
    /// Best-effort: an unreadable (or truncated) listing prunes nothing.
    fn prune_stale_roots(&self) {
        let _ = self.read_listing(true);
    }

    /// Deliver `sig` to every process in this job's subtrees — the whole tree, each
    /// process exactly once, `setsid` escapees included.
    ///
    /// Honest failure reporting mirrors the process-group backend's contract
    /// (K-055) so every Unix mechanism answers alike: an `ESRCH` (nothing left in
    /// the subtree) is success, an `EPERM` is surfaced only when it hit a positively
    /// live member, and every sweep visits **all** roots before returning so one
    /// failing subtree never leaves another unsignalled.
    ///
    /// Pruning brackets the delivery, exactly as `pgroup::Tracked::signal_all`'s
    /// does: the roots are refreshed against the kernel immediately *before* the
    /// sweep, and a root the kernel answers `ESRCH` for during it is dropped there
    /// and then. Delivery is the one operation where a stale root is actually
    /// dangerous — a recycled number would aim `PROC_REAP_KILL` at a subtree this
    /// job does not own (see [`prune`](Self::prune)) — so it is the one operation
    /// that pays for a fresh read.
    fn signal_tree(&self, sig: libc::c_int) -> io::Result<()> {
        self.prune_stale_roots();
        let mut surfaced: Option<io::Error> = None;
        for root in self.root_snapshot() {
            let mut request = ReaperKill {
                rk_sig: sig,
                rk_flags: REAPER_KILL_SUBTREE,
                rk_subtree: root.pid,
                rk_killed: 0,
                rk_fpid: 0,
                rk_pad0: [0; 15],
            };
            let Err(err) = procctl_self(
                libc::PROC_REAP_KILL,
                std::ptr::addr_of_mut!(request).cast::<libc::c_void>(),
            ) else {
                continue;
            };
            if err.raw_os_error() == Some(libc::ESRCH) {
                // The kernel matched nothing in this subtree: it drained between the
                // prune above and this call. Forget it now rather than at the next
                // read — the same terminal `ESRCH` pruning `Tracked::signal_all`
                // does — and by identity, so a root re-recorded meanwhile survives.
                self.forget(root);
            }
            if surfaced.is_none() && is_honest_failure(&err, request.rk_fpid) {
                surfaced = Some(err);
            }
        }
        match surfaced {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// After a hard kill: collect the corpses the kill produced, waiting (briefly,
    /// and only while something is actually still dying) for the tree to finish
    /// going down. See [`DRAIN_BUDGET`].
    fn drain_dead(&self, budget: Duration) {
        // `std::time::Instant` deliberately, not `tokio::time::Instant`: this is a
        // synchronous blocking loop reachable from `Drop`, where the tokio clock may
        // be paused (a hermetic test) and would never advance past the deadline.
        let deadline = Instant::now() + budget;
        loop {
            let Ok(listing) = self.read_listing(true) else {
                return;
            };
            // Zombies are excluded on purpose: the read above already `wait`ed for
            // the ones that are ours, and the rest belong to a live parent still
            // counted here in its own right.
            if !has_live_descendant(&listing.entries, &self.roots()) || Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(DRAIN_POLL);
        }
    }
}

/// Classify a `PROC_REAP_KILL` failure: is it a real containment failure worth
/// surfacing, or one of the benign outcomes the process-group backend has always
/// swallowed?
///
/// - `ESRCH` — nothing in the subtree matched (it already drained). Success.
/// - `EPERM` — a member refused the signal. Surfaced only against a positively
///   live, non-zombie member (see [`is_live_descendant`]): the genuine
///   `sudo`/setuid containment gap. Against a corpse, or when liveness cannot be
///   established, it stays swallowed — the fail-safe that keeps an ordinary
///   teardown of a tree with unreaped children from failing spuriously (K-055).
/// - `EINVAL` — the *request* is malformed (an out-of-range signal number), which
///   is wrong whatever the target's state, so it surfaces like the process-group
///   backend's `EINVAL`.
/// - anything else (`ECAPMODE` in a Capsicum sandbox, say) — an unexpected refusal
///   that means the tree was **not** signalled. Surfaced rather than hidden.
fn is_honest_failure(err: &io::Error, first_failing_pid: libc::pid_t) -> bool {
    match err.raw_os_error() {
        Some(libc::ESRCH) => false,
        Some(libc::EPERM) => is_live_descendant(first_failing_pid),
        _ => true,
    }
}

pub(crate) struct Job {
    /// The shared POSIX process-group backend: spawn coordination, tracked-id
    /// bookkeeping, the `SkipDropKill` latch, and the fallback for a process that
    /// could not become a reaper.
    group: ProcessGroup,
    /// The whole-tree layer on top.
    reaper: Reaper,
}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // A reaper accounts for no resources (see the module docs), so a requested
        // limit can't be honored — fail rather than hand back an unbounded tree.
        #[cfg(feature = "limits")]
        if limits.any() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "resource limits require a cgroup or Job Object; unavailable on this target",
            ));
        }
        Ok(Job {
            group: ProcessGroup::new(),
            reaper: Reaper::new(),
        })
    }

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        let child = self.group.spawn(cmd, opts)?;
        self.record_root(&child);
        Ok(child)
    }

    /// Spawn `cmd` under a pseudo-terminal, reusing this backend's normal spawn path
    /// (and therefore both containment layers). The Unix pty child keeps the
    /// tokio `Command`'s environment.
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<crate::sys::pty::PtySpawn> {
        // Carries the spare the spawn's kill-on-drop re-arm displaced over to the
        // rollback. Both closures run inside this one call, on this thread, and the
        // rollback only ever runs after the spawn returned — so a `Cell` is the
        // whole hand-off, and an untouched one ("nothing to restore") is exactly
        // right when the spawn never ran.
        let displaced = std::cell::Cell::new(crate::sys::DisplacedSpare::default());
        crate::sys::pty::spawn_pty(
            cmd,
            opts,
            |c, o| {
                let (child, spare) = self.group.spawn_displacing_spare(c, o)?;
                self.record_root(&child);
                displaced.set(spare);
                Ok(child)
            },
            |pid| self.rollback_pty_spawn(pid, displaced.take()),
        )
    }

    /// Undo a PTY spawn whose master setup failed — both containment layers:
    /// **kill within the containment, and forget nothing this mechanism still needs
    /// afterwards**.
    ///
    /// The subtree kill goes first, and its reach is this mechanism's whole-subtree
    /// maximum: `setsid` escapees included, the same reach `kill_all` has for this
    /// root. Note what carries that reach, because the wrong reading of it is easy
    /// and expensive: `PROC_REAP_KILL` is aimed by the pid handed to
    /// [`Reaper::hard_kill_subtree`](Reaper), *not* by a lookup in this job's root
    /// set, so no bookkeeping order could have made this one call miss. What the
    /// bookkeeping decides is every kill *after* it — `kill_all`/`Drop` sweep the
    /// recorded roots, and a subtree this job has forgotten is one nothing it owns
    /// can ever aim at again. That, not this call, is what a `forget` here would
    /// have cost: a descendant that forked and `setsid`'d inside the setup window
    /// and survived the kill for any reason would be beyond `killpg` *and* beyond
    /// every later reaper sweep.
    ///
    /// Then the process-group layer runs its own kill-then-forget — load-bearing,
    /// not redundant, for a job whose `PROC_REAP_ACQUIRE` failed: the reaper is
    /// inactive there and `killpg` is the whole mechanism. A doubled `SIGKILL` when
    /// both layers are live is harmless for the same reason
    /// [`kill_all`](Self::kill_all) accepts it — the signal cannot be handled,
    /// blocked or counted.
    ///
    /// The reaper root is deliberately **not** dropped afterwards, which is the
    /// asymmetry between the two layers. The process-group layer's tracked id is a
    /// *pgid*, and the only thing it could still reach after `killpg` is a member
    /// that refused the signal — while a stale pgid is this platform's sharpest
    /// recycling hazard, able to alias a process group of an unrelated process. The
    /// reaper root aliases only another tree of *this* process, and it is the sole
    /// handle by which this job can ever reach a `setsid` escapee again. So the
    /// first is released here and the second is left to the ordinary pruning, on the
    /// kernel's own positive answer that nothing is left under it.
    ///
    /// The process-group layer also restores `displaced` — the spare this spawn's
    /// own kill-on-drop re-arm took away — since both layers read the one latch this
    /// `Job` shares with its embedded [`ProcessGroup`].
    #[cfg(feature = "pty")]
    pub(crate) fn rollback_pty_spawn(&self, pid: u32, displaced: crate::sys::DisplacedSpare) {
        self.reaper.hard_kill_subtree(pid as libc::pid_t);
        self.group.rollback_pty_spawn(pid, displaced);
    }

    /// Register a just-started child as one of this job's subtree roots. Called
    /// immediately after the process-group backend has tracked it, with no fallible
    /// step in between: the window in which a child exists but belongs to no
    /// subtree is the same instruction-wide one `ProcessGroup::spawn`'s own guard
    /// already covers.
    fn record_root(&self, child: &Child) {
        if let Some(pid) = child.id() {
            self.reaper.record(pid as libc::pid_t);
        }
    }

    /// Adopt an already-started child of this process.
    ///
    /// An adopted child normally roots a reaper subtree exactly like one this job
    /// started — and gains more than it does on the process-group path, whose
    /// `setpgid` an already-`exec`ed child rejects (leaving its own descendants
    /// uncontained): the reaper follows the whole subtree regardless.
    ///
    /// The exception is a child forked **before** this process acquired reaper
    /// status, which the kernel deliberately leaves attached to the previous reaper
    /// (see [`Reaper::outside_members`]). Such a child cannot be reached by any
    /// `PROC_REAP_*` call of ours, so instead of recording a subtree root that names
    /// nothing, this latches the job into hybrid mode: the process-group layer —
    /// which *can* still `kill` a tracked pid directly — stays engaged alongside the
    /// reaper for every whole-tree verb. Membership is then the union of the two
    /// views, and delivery goes through both (the only place this backend accepts
    /// a doubled signal, because the alternative is not delivering at all).
    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.group.adopt(child)?;
        let Some(pid) = child.id().map(|pid| pid as libc::pid_t) else {
            return Ok(());
        };
        if !self.reaper.active {
            return Ok(());
        }
        if self.reaper.covers(pid) {
            self.reaper.record(pid);
        } else {
            // Either pre-acquisition (genuinely unreachable) or already exited
            // between the adopt and this probe. Latching for the second case only
            // costs a redundant process-group sweep on later verbs; failing to
            // latch for the first would silently drop the member.
            self.reaper.mark_outside_member();
        }
        Ok(())
    }

    /// Adopting an external process by **bare pid** is refused on FreeBSD, and the
    /// refusal comes straight from the shared process-group layer, whose
    /// [`read_identity`](crate::sys::pgroup) is `None` on this target.
    ///
    /// The reaper does not change that answer, because it is not an identity
    /// mechanism: `procctl(PROC_REAP_*)` gives *membership* of this process's own
    /// descendant subtree, kernel-maintained and precise for a process this job
    /// started — but a process an outside supervisor started is not a descendant at
    /// all ([`Reaper::covers`] is false for it), so no `PROC_REAP_*` call of ours
    /// can reach it and the reaper contributes nothing to a bare-pid adoption. What
    /// would be left is the process-group layer tracking a bare number with no
    /// start-time token behind it, which is exactly what
    /// [`adopt_external`](ProcessGroup::adopt_external) refuses to do — see its
    /// `capture_adoption_anchor`.
    ///
    /// [`adopt`](Self::adopt) is unaffected: there the caller's own un-reaped
    /// [`Child`] is what keeps the number from being recycled, and an adopted child
    /// of this process *does* root a reaper subtree.
    #[cfg(feature = "process-control")]
    pub(crate) fn adopt_external(&self, pid: u32) -> io::Result<()> {
        self.group.adopt_external(pid)
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        if !self.reaper.active {
            return self.group.kill_all();
        }
        // Both sweeps run, deliberately. The reaper kill is the one that reaches
        // the whole tree; the process-group sweep keeps the shared `Tracked`
        // bookkeeping (liveness latches, pruning) that `adopt` and the fallback
        // path depend on, and preserves its own honest-`EPERM` verdict. Doubling
        // delivery is harmless *here* and only here: `SIGKILL` cannot be handled,
        // blocked or counted, so a process the first sweep killed simply is not
        // there for the second. `signal` — whose signal a child can observe — never
        // does this.
        //
        // No corpse drain (unlike `Drop`, see `DRAIN_BUDGET`): this verb is called
        // from cancel/deadline watchdogs and from `RunningProcess`/`Pipeline`
        // teardown, i.e. from tokio worker threads, and it leaves a live `Job`
        // behind — the very next reaper read (`members`, `stats`, `is_drained`, the
        // job's own `Drop`) collects what the kill produced. Blocking an executor
        // thread to do it a few hundred microseconds earlier is not worth it.
        let reaper = self.reaper.signal_tree(libc::SIGKILL);
        let group = self.group.kill_all();
        reaper.and(group)
    }

    /// A reaper has no resource accounting, so a request carrying any cap is refused
    /// with `ErrorKind::Unsupported` — the exact typed refusal creation gives
    /// ([`Job::new`](Self::new) rejects a limited job the same way). An empty set
    /// (all `None`) is a trivially-satisfiable no-op.
    #[cfg(feature = "limits")]
    pub(crate) fn update_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        if limits.any() {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "resource limits require a cgroup or Job Object; unavailable on this target",
            ))
        } else {
            Ok(())
        }
    }

    /// A reaper carries no whole-tree resource accounting whatsoever — no
    /// counterpart to cgroup v2's event counters or a Job Object's limit accounting
    /// — so every axis is honestly `Unknown`. That is the correct answer here, not a
    /// degraded one: this mechanism also refuses to carry a cap in the first place,
    /// so `Unknown` means "no evidence apparatus exists on this mechanism", never
    /// "a cap may have fired unseen".
    #[cfg(feature = "limits")]
    pub(crate) fn limit_evidence(&self, _capped: crate::limits::CappedAxes) -> LimitEvidence {
        LimitEvidence::unknown()
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        let raw = sig.raw();
        // `Signal::Other(0)` is the POSIX *existence probe*: it delivers nothing and
        // must answer `Ok` when it reaches a signalable live target. `PROC_REAP_KILL`
        // has no such mode — the kernel rejects any signal number below 1 with
        // `EINVAL` — so the probe (and any other non-positive request, which this
        // job's two delivery paths reject identically) stays on the process-group
        // path. What that keeps identical to every other Unix backend is the `Ok`
        // having delivered nothing; it does NOT carry the reaper's `EPERM`
        // discrimination over, since `pgroup::is_live_non_zombie` has no state
        // reader on this target — a live member that rejects even the null signal
        // answers `Ok` here, exactly as on the bare BSDs. Documented as the one
        // probe-path exception on `ProcessGroup::signal`; do not restate the
        // reaper's `PROC_REAP_GETPIDS` discrimination as covering this path.
        if !self.reaper.active || raw <= 0 {
            return self.group.signal(raw);
        }
        // Reaper only, *not* also the process-group sweep: a soft `Term`/`Int` is a
        // signal the child observes, and delivering it twice would make a child that
        // treats the second request as "force quit now" skip its own graceful path.
        // The reaper already covers every process `killpg` would reach, plus the
        // escapees, each exactly once. The one exception is a job holding a member
        // the reaper cannot see (see `adopt`), where a doubled signal to the rest of
        // the tree is the price of reaching that member at all.
        let reaped = self.reaper.signal_tree(raw);
        if self.reaper.has_outside_members() {
            return reaped.and(self.group.signal(raw));
        }
        reaped
    }

    /// Both containment layers deliver a soft `Int`/`Term` to the whole tracked
    /// tree, so the scope is always `WholeTree` — with the reaper active it is the
    /// strongest form of that promise available on any Unix: `PROC_REAP_KILL`
    /// reaches even a member that `setsid`ed out of its process group. There is no
    /// opt-in subset or `Unsupported` case here (`signal(Int/Term)` never returns
    /// `Unsupported` on this backend).
    #[cfg(feature = "process-control")]
    pub(crate) fn soft_stop_scope(&self) -> crate::SoftStopScope {
        crate::SoftStopScope::WholeTree
    }

    /// Freeze the whole tree, reporting the delivery **honestly** — the same verdict
    /// [`signal`](Self::signal) and [`kill_all`](Self::kill_all) return, and for the
    /// same reason: `signal_tree` has already classified the failure, so discarding
    /// it here would throw away the one thing the caller cannot recompute. A live,
    /// non-zombie member's `EPERM` (a uid-changed child that rejects `SIGSTOP`) and a
    /// malformed/refused request (`EINVAL`, `ECAPMODE`) surface as `Err`; an `ESRCH`
    /// and a zombie-only `EPERM` stay `Ok` inside that classification, so an ordinary
    /// drained or half-reaped tree is still a no-op success.
    ///
    /// This matches the process-group backend this job also drives: its
    /// `Tracked::suspend` propagates its own sweep verdict too, so both of a hybrid
    /// job's layers — and the `!reaper.active` fallback path above — answer alike.
    ///
    /// A doubled `SIGSTOP`/`SIGCONT` is a no-op (both are level-triggered), so the
    /// hybrid case needs no special care beyond running both layers; `and` keeps the
    /// reaper's error while still running the process-group sweep.
    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        if !self.reaper.active {
            return self.group.suspend();
        }
        let reaped = self.reaper.signal_tree(libc::SIGSTOP);
        if self.reaper.has_outside_members() {
            return reaped.and(self.group.suspend());
        }
        reaped
    }

    /// Thaw a tree frozen by [`suspend`](Self::suspend) — its exact mirror, including
    /// the honest delivery verdict described there.
    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        if !self.reaper.active {
            return self.group.resume();
        }
        let reaped = self.reaper.signal_tree(libc::SIGCONT);
        if self.reaper.has_outside_members() {
            return reaped.and(self.group.resume());
        }
        reaped
    }

    /// The **whole tree**, not just the tracked group leaders: every live descendant
    /// of every child this job started, read from the kernel's reaper listing. This
    /// is the headline difference from the process-group backend, which can only
    /// report the leaders it tracks and is blind to anything that `setsid`ed away.
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        if !self.reaper.active {
            return Ok(self
                .group
                .members()
                .into_iter()
                .map(|pid| pid as u32)
                .collect());
        }
        Ok(self.member_pids(&self.reaper.tree()?))
    }

    /// The reaper tree's pids, plus — only for a job in hybrid mode (see
    /// [`adopt`](Self::adopt)) — the process-group layer's tracked pids that the
    /// reaper does not already list. De-duplicated, so a member both layers can see
    /// (every child this job spawned itself) is counted exactly once.
    ///
    /// The process-group read is taken only in hybrid mode, which matters for the
    /// one caller bound by the graceful driver's probe-only contract
    /// (`alive_count`): that read prunes the process group's tracked set, but only
    /// of ids that just probed **dead**, so it cannot forget the survivor the
    /// contract is protecting — and in the ordinary, non-hybrid case it is not taken
    /// at all.
    #[cfg(feature = "process-control")]
    fn member_pids(&self, tree: &[ReaperPidInfo]) -> Vec<u32> {
        let mut pids: Vec<u32> = tree.iter().map(|entry| entry.pi_pid as u32).collect();
        if self.reaper.has_outside_members() {
            for pid in self.group.members() {
                let pid = pid as u32;
                if !pids.contains(&pid) {
                    pids.push(pid);
                }
            }
        }
        pids
    }

    /// The same member set as [`members`](Self::members), with every enriching field
    /// honestly `None`: FreeBSD keeps ppid/image name/start time in `kinfo_proc`,
    /// reachable only through a `sysctl(KERN_PROC)` MIB this crate deliberately does
    /// not carry (see `pgroup::read_identity` for the reasoning). Reporting the pids
    /// of the *whole* tree with no metadata is strictly more than the process-group
    /// backend can say, and nothing here is fabricated.
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        if !self.reaper.active {
            return Ok(self.group.members_info());
        }
        Ok(self
            .member_pids(&self.reaper.tree()?)
            .into_iter()
            .map(|pid| MemberInfo::new(pid, None, None, None))
            .collect())
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<super::graceful::GracefulOutcome> {
        if !self.reaper.active {
            return self
                .group
                .graceful_shutdown(signal, timeout, escalate)
                .await;
        }
        // The shared signal → poll → escalate driver, run against *this* job's
        // whole-tree target rather than the process group's. The latch is the
        // process group's own, so an `escalate = false` spare suppresses both this
        // backend's `Drop` kill and the wrapped `ProcessGroup`'s, and a later
        // `spawn`/`adopt` re-arms both at once.
        super::graceful::run(self, self.group.skip_drop_kill(), signal, timeout, escalate).await
    }

    /// The count is the **whole tree**, not one entry per contained child: with the
    /// reaper active this is the exact process count a cgroup or Job Object would
    /// report, not the process-group backend's live-group tally.
    ///
    /// Every *measurement* stays absent, and the reaper changes nothing about that:
    /// it is a containment relationship — the kernel remembers which process
    /// inherits the tree's orphans, so `PROC_REAP_GETPIDS` can enumerate it — and
    /// carries no accounting whatsoever. There is no CPU or memory accumulator to
    /// read, no I/O byte counter, and no record of how many processes the tree held
    /// at its peak; `procctl` offers none of them. So this reports the one thing the
    /// reaper genuinely knows — who is in the tree, right now — and leaves the rest
    /// `None` rather than filling it with zeroes or with a walk that would be a
    /// different measurement wearing the same name.
    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        if !self.reaper.active {
            return self.group.stats();
        }
        let tree = self.reaper.tree()?;
        // The hybrid case's extra members can only exist under `process-control`
        // (only `adopt` creates them, and `adopt` is gated on it), so without that
        // feature the tree is the whole answer by construction.
        #[cfg(feature = "process-control")]
        let active_process_count = self.member_pids(&tree).len();
        #[cfg(not(feature = "process-control"))]
        let active_process_count = tree.len();
        Ok(ProcessGroupStats {
            active_process_count,
            total_cpu_time: None,
            peak_memory_bytes: None,
            io_read_bytes: None,
            io_write_bytes: None,
            peak_process_count: None,
        })
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        // Honest: a process that could not acquire reaper status has exactly the
        // containment of the plain POSIX backend, `setsid` hole included, and says
        // so rather than claiming the stronger mechanism.
        if self.reaper.active {
            Mechanism::ProcessReaper
        } else {
            Mechanism::ProcessGroup
        }
    }
}

impl super::graceful::GracefulTarget for Job {
    fn signal_all(&self, signal: i32) -> super::graceful::SoftDelivery {
        // Best-effort by trait contract — the driver polls regardless; the verdict
        // is recorded for the shutdown report only. The hybrid case adds the
        // process-group sweep for the same reason `Job::signal` does.
        let mut sent = self.reaper.signal_tree(signal).is_ok();
        if self.reaper.has_outside_members() {
            sent &= super::graceful::GracefulTarget::signal_all(&self.group, signal)
                == super::graceful::SoftDelivery::Sent;
        }
        if sent {
            super::graceful::SoftDelivery::Sent
        } else {
            super::graceful::SoftDelivery::Failed
        }
    }

    fn is_drained(&self) -> bool {
        // Probe-only (no root pruning), per the trait contract. An unreadable
        // listing falls back to the process group's own answer rather than
        // guessing "drained", which would end the grace early.
        let Ok(tree) = self.reaper.tree_probe() else {
            return super::graceful::GracefulTarget::is_drained(&self.group);
        };
        if !tree.is_empty() {
            return false;
        }
        // In hybrid mode an empty reaper tree is only half the answer: the member
        // the reaper cannot see lives entirely on the process-group side.
        !self.reaper.has_outside_members()
            || super::graceful::GracefulTarget::is_drained(&self.group)
    }

    fn alive_count(&self) -> Option<usize> {
        // `None` on an unreadable listing — the report says "could not read", never
        // an invented count.
        let tree = self.reaper.tree_probe().ok()?;
        // Only `adopt` (itself `process-control`-gated) can create a member outside
        // the tree, so without that feature the tree's length is the whole count.
        #[cfg(feature = "process-control")]
        {
            Some(self.member_pids(&tree).len())
        }
        #[cfg(not(feature = "process-control"))]
        {
            Some(tree.len())
        }
    }

    fn hard_kill(&self) -> io::Result<()> {
        // Both sweeps, for the same reason `kill_all` runs both — and with no drain:
        // this runs inside the async driver, which polls `is_drained` (and so sweeps
        // corpses) on its own schedule instead of blocking a runtime thread here.
        let reaper = self.reaper.signal_tree(libc::SIGKILL);
        let group = super::graceful::GracefulTarget::hard_kill(&self.group);
        reaper.and(group)
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // The wrapped `ProcessGroup`'s own `Drop` runs right after this one and
        // handles the `killpg` backstop (and the whole job when there is no reaper),
        // so this adds only the part `killpg` cannot do.
        if !self.reaper.active {
            return;
        }
        if self.group.skip_drop_kill().is_set() {
            // Survivors were deliberately spared by a `graceful_shutdown(escalate =
            // false)`. Kill nothing — but still collect any corpse already
            // re-parented to us, which sparing the living says nothing about.
            if let Ok(listing) = descendants() {
                reap_stray_zombies(&listing.entries);
            }
            return;
        }
        // Best-effort backstop; `Drop` cannot surface a result, so a live-`EPERM`
        // here is swallowed (an explicit `kill_all`/`shutdown` would have reported
        // it). The drain then collects what the kill produced.
        let _ = self.reaper.signal_tree(libc::SIGKILL);
        self.reaper.drain_dead(DRAIN_BUDGET);
    }
}

/// Read-only prediction of the [`Mechanism`] a fresh [`Job`] would use on this host,
/// computed **without creating any OS object or spawning anything**.
///
/// A fixed constant on FreeBSD, like [`Mechanism::JobObject`] on Windows: acquiring
/// reaper status is available on every supported FreeBSD kernel and needs no
/// privilege (`procctl(PROC_REAP_ACQUIRE)` only ever fails for a target that is not
/// the calling process, or with `EBUSY` when the status is already held — which this
/// backend treats as success). Crucially the prediction must **not** probe by
/// acquiring: that is a real, permanent side effect on the process, and this query
/// backs the spawn-free, side-effect-free `host_containment()`. So the constant is an
/// assumption, exactly as documented — should acquisition nonetheless fail, a real
/// [`Job`] degrades to [`Mechanism::ProcessGroup`] and reports that from
/// [`Job::mechanism`], which stays the final word.
pub(crate) fn detect_mechanism() -> Mechanism {
    Mechanism::ProcessReaper
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(_pid: u32, _expected: Option<ProcIdentity>) -> ProcMetrics {
    // Not *implemented* on this target (returns the empty default), rather than
    // impossible: FreeBSD has no `/proc/<pid>/stat` by default, and its per-process
    // CPU/memory lives in `kinfo_proc` behind a `sysctl(KERN_PROC)` MIB that is not
    // wired up here. The `expected` identity is irrelevant while no metrics are
    // reported — an all-`None` default can never misattribute a recycled pid's
    // counters, so it is honestly ignored.
    ProcMetrics::default()
}

/// Identity + best-effort metadata for an **arbitrary** pid — the standalone
/// [`process_info`](crate::process_info) query. Delegates to the shared POSIX
/// module's bare-BSD reader, which probes existence with a zero-signal
/// `kill(pid, 0)` and keeps the "no such process" (`Ok(None)`) vs "can't look"
/// (`Err`) distinction. Deliberately *not* routed through the reaper listing: this
/// query answers for any pid on the host, most of which are not our descendants at
/// all, so the reaper tree is the wrong source of truth for it.
#[cfg(feature = "process-control")]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    crate::sys::pgroup::process_info(pid)
}

#[cfg(feature = "stats")]
pub(crate) fn process_identity(_pid: u32) -> Option<ProcIdentity> {
    // No wired-up per-process metrics here (see `process_metrics`), so there is no
    // reading to identity-gate and thus no anchor to capture. Honest `None` — never
    // a fabricated token.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a descendant listing entry the way the kernel would.
    fn entry(pid: libc::pid_t, subtree: libc::pid_t, zombie: bool) -> ReaperPidInfo {
        ReaperPidInfo {
            pi_pid: pid,
            pi_subtree: subtree,
            pi_flags: REAPER_PIDINFO_VALID | if zombie { REAPER_PIDINFO_ZOMBIE } else { 0 },
            pi_pad0: [0; 15],
        }
    }

    /// A listing the kernel reported in full.
    fn listing(entries: &[ReaperPidInfo]) -> Listing {
        Listing {
            entries: entries.to_vec(),
            truncated: false,
        }
    }

    /// A listing that ran out of buffer — the entries are real, their *absence* is
    /// not evidence of anything.
    fn cut_short(entries: &[ReaperPidInfo]) -> Listing {
        Listing {
            entries: entries.to_vec(),
            truncated: true,
        }
    }

    /// The bookkeeping half of a [`Reaper::record`], with no kernel read. The real
    /// `record` is exactly this plus one `PROC_REAP_GETPIDS`, whose pruning is
    /// covered by `recording_forgets_the_roots_the_kernel_has_forgotten` below;
    /// stubbing the read out keeps these tests hermetic (a live listing on the test
    /// host would prune the synthetic roots they are built from).
    fn record(reaper: &Reaper, pid: libc::pid_t) {
        reaper.insert_root(pid, None, reaper.seq_mark());
    }

    /// The four `<sys/procctl.h>` structures are hand-mirrored (`libc` declares only
    /// the command numbers), so their size and — for the one with a pointer after a
    /// fixed prefix — their field offsets are pinned here. A drift would hand the
    /// kernel a wrongly-shaped buffer, which is exactly the class of bug that never
    /// announces itself.
    #[test]
    fn reaper_abi_layout_matches_the_kernel_headers() {
        use std::mem::{align_of, offset_of, size_of};

        // 5 × 4-byte fields + 15 × u_int of padding.
        assert_eq!(size_of::<ReaperStatus>(), 80);
        assert_eq!(size_of::<ReaperKill>(), 80);
        // 3 × 4-byte fields + 15 × u_int of padding.
        assert_eq!(size_of::<ReaperPidInfo>(), 72);
        // `rp_count` + 15 × u_int, then the array pointer — which the C layout
        // places at exactly that offset, with no implicit padding on any FreeBSD
        // architecture this crate builds for.
        assert_eq!(offset_of!(ReaperPids, rp_pids), 64);
        assert_eq!(
            size_of::<ReaperPids>(),
            64 + size_of::<*mut ReaperPidInfo>()
        );
        assert_eq!(align_of::<ReaperPidInfo>(), align_of::<libc::c_uint>());

        // The flag bits themselves, pinned against a typo.
        assert_eq!(REAPER_STATUS_OWNED, 0x1);
        assert_eq!(REAPER_PIDINFO_VALID, 0x1);
        assert_eq!(REAPER_PIDINFO_ZOMBIE, 0x8);
        assert_eq!(REAPER_KILL_SUBTREE, 0x2);
    }

    #[test]
    fn membership_is_scoped_to_this_jobs_subtrees() {
        let roots = vec![100, 200];
        // A direct child (roots its own subtree), a grandchild that inherited the
        // subtree tag, and a deep descendant that `setsid`ed away — the escapee is
        // still tagged, which is the whole point.
        assert!(is_member(&entry(100, 100, false), &roots));
        assert!(is_member(&entry(101, 100, false), &roots));
        assert!(is_member(&entry(102, 100, false), &roots));
        // Another job's subtree, and a child the embedding application forked for
        // itself, are both invisible to this job.
        assert!(!is_member(&entry(300, 300, false), &roots));
        assert!(!is_member(&entry(301, 300, false), &roots));
    }

    #[test]
    fn a_zombie_is_not_a_live_member() {
        let roots = vec![100];
        assert!(!is_member(&entry(101, 100, true), &roots));
        // An unfilled array slot is not a member either, whatever it happens to hold.
        assert!(!is_member(&ReaperPidInfo::ZERO, &roots));
    }

    #[test]
    fn only_reparented_corpses_are_swept() {
        // A grandchild's corpse reached us by re-parenting: nothing else will ever
        // `wait` for it, so the sweep must.
        assert!(is_stray_zombie(&entry(101, 100, true)));
        // A direct child's corpse belongs to the `Child` handle that spawned it —
        // `wait`ing for it here would steal that owner's exit status. `pi_subtree ==
        // pi_pid` is the test, and it keeps holding after re-parenting, unlike the
        // kernel's "is a direct child right now" flag.
        assert!(!is_stray_zombie(&entry(100, 100, true)));
        // The living are never swept.
        assert!(!is_stray_zombie(&entry(101, 100, false)));
    }

    #[test]
    fn the_drain_waits_only_for_processes_below_a_root() {
        let roots = vec![100];
        // A live grandchild keeps the drain going...
        assert!(has_live_descendant(&[entry(101, 100, false)], &roots));
        // ...but its corpse does not (the sweep already collected it), and neither
        // does the root itself, whose reaping belongs to its `Child` handle.
        assert!(!has_live_descendant(&[entry(101, 100, true)], &roots));
        assert!(!has_live_descendant(&[entry(100, 100, false)], &roots));
        // Nor does another job's live tree.
        assert!(!has_live_descendant(&[entry(301, 300, false)], &roots));
    }

    #[test]
    fn a_kill_failure_surfaces_only_when_it_is_a_real_one() {
        // Nothing left to signal is success, not a failure to contain.
        assert!(!is_honest_failure(
            &io::Error::from_raw_os_error(libc::ESRCH),
            0
        ));
        // A malformed request (an out-of-range signal number) is wrong whatever the
        // target's state.
        assert!(is_honest_failure(
            &io::Error::from_raw_os_error(libc::EINVAL),
            0
        ));
        // An unexpected refusal means the tree was not signalled — surfaced, not
        // hidden.
        assert!(is_honest_failure(
            &io::Error::from_raw_os_error(libc::ECAPMODE),
            0
        ));
        // `EPERM` without an identifiable live target stays swallowed: the kernel
        // reports pid 0 when it never got as far as naming one, and a pid the reaper
        // tree does not know cannot be proven alive. (The positive case needs a real
        // uid-changed descendant and is covered by the FreeBSD integration job.)
        assert!(!is_honest_failure(
            &io::Error::from_raw_os_error(libc::EPERM),
            0
        ));
        assert!(!is_honest_failure(
            &io::Error::from_raw_os_error(libc::EPERM),
            -1
        ));
    }

    fn reaper(active: bool) -> Reaper {
        Reaper {
            active,
            outside_members: std::sync::atomic::AtomicBool::new(false),
            roots: Mutex::new(RootSet::default()),
        }
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn the_outside_member_latch_is_off_until_an_adopt_needs_it_and_then_stays_on() {
        // Nothing a job *spawns* can be outside the reaper's tree — those forks all
        // happen after acquisition — so the hybrid path stays off by default and the
        // whole-tree verbs never pay for the process-group sweep.
        let reaper = reaper(true);
        assert!(!reaper.has_outside_members());
        reaper.mark_outside_member();
        assert!(reaper.has_outside_members());
        // One-way: a later ordinary adopt must not switch the process-group layer
        // back off while the unreachable member is still there.
        record(&reaper, 100);
        assert!(reaper.has_outside_members());
    }

    #[test]
    fn roots_are_recorded_once_and_pruned_only_when_the_subtree_is_empty() {
        let reaper = reaper(true);
        record(&reaper, 100);
        record(&reaper, 100); // a re-adopt must not double-count
        record(&reaper, 200);
        assert_eq!(reaper.roots(), vec![100, 200]);

        let mark = reaper.seq_mark();
        // Subtree 100 still holds an escapee (its own root is long reaped); subtree
        // 200 holds nothing at all, so only 200 is forgotten.
        reaper.prune(&listing(&[entry(101, 100, false)]), mark);
        assert_eq!(reaper.roots(), vec![100]);
        // Even a lone corpse keeps a subtree occupied — it has not been collected
        // yet, so the root must stay addressable.
        reaper.prune(&listing(&[entry(101, 100, true)]), mark);
        assert_eq!(reaper.roots(), vec![100]);
        // Empty at last.
        reaper.prune(&listing(&[]), mark);
        assert!(reaper.roots().is_empty());
    }

    #[test]
    fn a_pid_that_cannot_name_a_process_is_never_recorded() {
        // The real entry point, guards included — both rejections short-circuit
        // before any kernel read, so this stays hermetic.
        let reaper = reaper(true);
        reaper.record(0);
        reaper.record(-5);
        assert!(reaper.roots().is_empty());
    }

    #[test]
    fn pruning_never_forgets_a_root_recorded_after_the_listing_was_taken() {
        // The race a membership read on one thread runs against a `spawn` on
        // another: the listing is taken, then the new child is recorded, then the
        // prune runs against the now-stale listing. Without the sequence guard the
        // brand-new subtree would be dropped on the floor and this job's teardown
        // would silently narrow to whatever `killpg` can still reach.
        let reaper = reaper(true);
        record(&reaper, 100);
        let mark = reaper.seq_mark(); // taken before the (empty) listing below
        record(&reaper, 200); // the concurrent spawn
        reaper.prune(&listing(&[]), mark);
        assert_eq!(reaper.roots(), vec![200]);
    }

    #[test]
    fn a_truncated_listing_prunes_nothing() {
        // A listing cut short by the buffer says nothing about the subtrees it never
        // reached — and the kernel prepends new processes, so what it drops first is
        // the *oldest* subtrees. Treating that silence as "empty" would let a job
        // that is forking hard make a quiet neighbour job forget its roots, after
        // which its teardown reaches nothing and still reports success.
        let reaper = reaper(true);
        record(&reaper, 100);
        record(&reaper, 200);
        let mark = reaper.seq_mark();
        reaper.prune(&cut_short(&[entry(301, 300, false)]), mark);
        assert_eq!(reaper.roots(), vec![100, 200]);
        // The very same listing, known complete, does prune: the difference is the
        // flag, not the entries.
        reaper.prune(&listing(&[entry(301, 300, false)]), mark);
        assert!(reaper.roots().is_empty());
    }

    #[test]
    fn recording_forgets_the_roots_the_kernel_has_forgotten() {
        // "Prune, then track", like `pgroup::Tracked::track`: a spawn is the moment
        // a number this job still remembers can be handed to a new process, so the
        // stale roots go first.
        let reaper = reaper(true);
        record(&reaper, 100);
        record(&reaper, 200);
        let mark = reaper.seq_mark();
        // Only subtree 200 is still populated when the next child is recorded.
        reaper.insert_root(300, Some(&listing(&[entry(201, 200, false)])), mark);
        assert_eq!(reaper.roots(), vec![200, 300]);
    }

    #[test]
    fn recording_a_recycled_number_replaces_the_stale_root_rather_than_keeping_it() {
        // The job holds a stale root for pid 100, and the OS hands that very number
        // to its next child — whose fork also makes subtree 100 look occupied again,
        // so the prune above cannot tell the two apart. Keeping the old entry would
        // keep its old stamp, and a membership read already in flight (its mark taken
        // before this spawn) would then prune the root as "empty" and forget the
        // child that was just started. The fresh stamp is what survives that read.
        let reaper = reaper(true);
        record(&reaper, 100);
        let in_flight = reaper.seq_mark(); // a concurrent read's mark, taken now
        record(&reaper, 100); // the recycled number, recorded after it
        assert_eq!(
            reaper.roots(),
            vec![100],
            "still exactly one root for the pid"
        );
        reaper.prune(&listing(&[]), in_flight);
        assert_eq!(
            reaper.roots(),
            vec![100],
            "the re-recorded root outranks the stale listing"
        );
    }

    #[test]
    fn forgetting_a_root_matches_the_stamp_not_just_the_number() {
        // `signal_tree` drops a root the kernel answered `ESRCH` for. If a concurrent
        // `record` replaced that entry in between — a new child on the recycled
        // number — the replacement must survive: the `ESRCH` was about the subtree
        // that died, not the one that just started.
        let reaper = reaper(true);
        record(&reaper, 100);
        let stale = reaper.root_snapshot()[0];
        record(&reaper, 100); // the concurrent re-record
        reaper.forget(stale);
        assert_eq!(reaper.roots(), vec![100]);
        // The current entry, on the other hand, is forgettable.
        let current = reaper.root_snapshot()[0];
        reaper.forget(current);
        assert!(reaper.roots().is_empty());
    }

    #[test]
    fn an_inactive_reaper_records_nothing() {
        // Without reaper status the subtree list would be meaningless — every
        // whole-tree method falls back to the process-group backend, so nothing is
        // tracked here at all.
        let reaper = reaper(false);
        reaper.record(100);
        assert!(reaper.roots().is_empty());
    }
}
