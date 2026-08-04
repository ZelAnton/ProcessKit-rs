//! Implementation for unix targets with no whole-tree containment primitive of
//! their own (macOS and the BSDs other than FreeBSD, which has the `procctl`
//! process reaper — see `sys::freebsd`): a [`ProcessGroup`] per the shared POSIX
//! backend. Every child leads its own process group, so dropping the job
//! `killpg`s the whole tree — a real kill-on-close guarantee, weaker only
//! against children that `setsid` away. Surfaced as [`Mechanism::ProcessGroup`].
//!
//! These targets have no `/proc`, so per-process CPU/memory metrics are not
//! available; [`process_metrics`] returns defaults.

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};

// The twin of `sys::freebsd`'s guard: this module is the *catch-all* unix arm of
// the platform dispatcher in `sys/mod.rs`, so it must stay out of the two unix
// targets that have a backend of their own. Asserting that here turns a
// cross-target `cargo check` into a proof that adding the FreeBSD arm did not
// re-route macOS or the other BSDs — and that FreeBSD did not silently keep the
// weaker, `setsid`-escapable fallback.
#[cfg(any(not(unix), target_os = "linux", target_os = "freebsd"))]
compile_error!(
    "sys::unix is the catch-all POSIX process-group backend; Linux (sys::linux) \
     and FreeBSD (sys::freebsd) have their own, and non-unix targets have none"
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

pub(crate) struct Job {
    group: ProcessGroup,
}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // A POSIX process group has no resource accounting, so a requested limit
        // can't be honored — fail rather than hand back an unbounded tree.
        #[cfg(feature = "limits")]
        if limits.any() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "resource limits require a cgroup or Job Object; unavailable on this target",
            ));
        }
        Ok(Job {
            group: ProcessGroup::new(),
        })
    }

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        self.group.spawn(cmd, opts)
    }

    /// Spawn `cmd` under a pseudo-terminal, reusing this backend's normal
    /// process-group containment for the actual spawn (K-032). `env` is unused
    /// here — the Unix pty child keeps the tokio `Command`'s env (`std` applies
    /// it); it exists only to match the cross-platform seam.
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
        _env: Option<Vec<(std::ffi::OsString, std::ffi::OsString)>>,
    ) -> io::Result<crate::sys::pty::PtySpawn> {
        crate::sys::pty::spawn_pty(
            cmd,
            opts,
            |c, o| self.group.spawn(c, o),
            |pid| self.rollback_pty_spawn(pid),
        )
    }

    /// Undo a PTY spawn whose master setup failed: this backend *is* the process
    /// group, so the whole rollback is the group's own kill-then-forget (see
    /// [`ProcessGroup::rollback_pty_spawn`](crate::sys::pgroup::ProcessGroup::rollback_pty_spawn)) —
    /// `killpg` over the child's session while it is still tracked, then the
    /// tracked id. A descendant that `setsid`s away is outside `killpg`'s reach
    /// here, the standing [`Mechanism::ProcessGroup`] limit rather than anything
    /// this path adds.
    #[cfg(feature = "pty")]
    pub(crate) fn rollback_pty_spawn(&self, pid: u32) {
        self.group.rollback_pty_spawn(pid);
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.group.adopt(child)
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.group.kill_all()
    }

    /// A POSIX process group has no resource accounting, so a request carrying any
    /// cap is refused with `ErrorKind::Unsupported` — the exact typed refusal
    /// creation gives ([`Job::new`](Self::new) rejects a limited group the same
    /// way). An empty set (all `None`) is a trivially-satisfiable no-op: the tree is
    /// already unbounded here, so "remove all limits" needs nothing done and must
    /// not spuriously fail.
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

    /// A POSIX process group has no whole-tree resource accounting whatsoever — no
    /// counterpart to cgroup v2's event counters or a Job Object's limit accounting
    /// — so there is nothing post-mortem to read and every axis is honestly
    /// `Unknown`. That is the correct answer here, not a degraded one: this
    /// mechanism also refuses to carry a cap in the first place ([`Job::new`](Self::new)
    /// fails fast when `limits.any()`), so `Unknown` means "no evidence apparatus
    /// exists on this mechanism", never "a cap may have fired unseen".
    #[cfg(feature = "limits")]
    pub(crate) fn limit_evidence(&self, _capped: crate::limits::CappedAxes) -> LimitEvidence {
        LimitEvidence::unknown()
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        self.group.signal(sig.raw())
    }

    /// The POSIX process-group backend delivers a soft `Int`/`Term` to the whole
    /// tracked tree (`killpg` over every tracked leader — see the pgroup backend),
    /// matching `signal`'s reach. There is no opt-in subset or `Unsupported` case
    /// here (`signal(Int/Term)` never returns `Unsupported` on this backend), so
    /// the scope is always `WholeTree`.
    #[cfg(feature = "process-control")]
    pub(crate) fn soft_stop_scope(&self) -> crate::SoftStopScope {
        crate::SoftStopScope::WholeTree
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.group.suspend()
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.group.resume()
    }

    /// Tracked group leaders only — see the pgroup backend.
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        Ok(self
            .group
            .members()
            .into_iter()
            .map(|pid| pid as u32)
            .collect())
    }

    /// Tracked group leaders enriched with best-effort metadata — see the pgroup
    /// backend. Infallible enumeration (an in-memory tracked list), so always
    /// `Ok`; on macOS the fields are read via `proc_pidinfo`, on the bare BSDs they
    /// are honestly `None`.
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        Ok(self.group.members_info())
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<super::graceful::GracefulOutcome> {
        self.group
            .graceful_shutdown(signal, timeout, escalate)
            .await
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        self.group.stats()
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        Mechanism::ProcessGroup
    }
}

/// Read-only prediction of the [`Mechanism`] a fresh [`Job`] would use on this host,
/// computed **without creating any OS object or spawning anything** — always the
/// POSIX [`Mechanism::ProcessGroup`] backend on macOS/BSD (no cgroups or Job Objects
/// exist here), so there is nothing to probe. Mirrors [`Job::mechanism`]; backs the
/// public `host_containment()` query.
pub(crate) fn detect_mechanism() -> Mechanism {
    Mechanism::ProcessGroup
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(_pid: u32, _expected: Option<ProcIdentity>) -> ProcMetrics {
    // Not *implemented* on these targets (returns the empty default), rather than
    // impossible: macOS/BSD have no `/proc`, so the Linux `/proc/<pid>/stat` path
    // doesn't apply, but per-process CPU/memory IS obtainable here via
    // `libproc`/`proc_pidinfo` (macOS) or `kvm`/`sysctl` (BSD) — just not wired up
    // (C12). Group-level `stats()` is likewise unavailable on the process-group
    // mechanism; the count is all it can report. The `expected` identity is
    // irrelevant while no metrics are reported — an all-`None` default can never
    // misattribute a recycled pid's counters, so it is honestly ignored.
    ProcMetrics::default()
}

/// Identity + best-effort metadata for an **arbitrary** pid — the macOS/BSD
/// backend of the standalone [`process_info`](crate::process_info) query.
/// Delegates to the shared POSIX process-group module, which reuses the very
/// `proc_pidinfo` reader (macOS) or `kill(pid, 0)` existence probe (the bare BSDs)
/// the group tracking already relies on, and keeps its "no such process" (`Ok(None)`)
/// vs "can't look" (`Err`) distinction. Works for any pid the caller holds, not
/// only tracked group leaders.
#[cfg(feature = "process-control")]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    crate::sys::pgroup::process_info(pid)
}

#[cfg(feature = "stats")]
pub(crate) fn process_identity(_pid: u32) -> Option<ProcIdentity> {
    // No wired-up per-process metrics here (see `process_metrics`), so there is no
    // reading to identity-gate and thus no anchor to capture. Honest `None` — never
    // a fabricated token — degrades callers to the number-only path, which on this
    // backend already reports no metrics. (The pgroup backend that actually tracks
    // liveness DOES read a start-time identity where the platform allows it — see
    // `pgroup::read_identity`; this is only the metrics-side stub.)
    None
}
