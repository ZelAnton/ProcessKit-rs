//! [`ProcessGroup`] — a kill-on-drop container for a tree of child processes.

use std::time::Duration;

use tokio::process::{Child, Command};

use crate::error::{Error, Result};
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
use crate::mechanism::Mechanism;
#[cfg(feature = "process-control")]
use crate::signal::Signal;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
use crate::sys::Job;

/// Tuning for a [`ProcessGroup`] — graceful-shutdown timing and (with the
/// `limits` feature) resource limits.
///
/// On the Unix graceful path ([`ProcessGroup::shutdown`]): give the tree
/// `shutdown_timeout` to exit after `SIGTERM`, then `SIGKILL` survivors if
/// `escalate_to_kill` is set. On Windows the job kill is atomic, so
/// `shutdown_timeout` is ignored; `escalate_to_kill` is still honored — `false`
/// preserves survivors (the handle closes without `KILL_ON_JOB_CLOSE`).
#[cfg_attr(
    feature = "limits",
    doc = "",
    doc = "[`limits`](Self::limits) caps the whole tree's memory, process count, and CPU;",
    doc = "it is applied at group creation and only where a real container exists (Windows",
    doc = "Job Object or Linux cgroup v2) — see [`ResourceLimits`]."
)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProcessGroupOptions {
    /// How long to wait after `SIGTERM` before escalating. Default: 2 seconds.
    pub shutdown_timeout: Duration,
    /// Whether to `SIGKILL` processes that outlive `shutdown_timeout`.
    /// Default: `true`.
    pub escalate_to_kill: bool,
    /// Whole-tree resource caps applied at creation. Default: no limits.
    #[cfg(feature = "limits")]
    pub limits: ResourceLimits,
}

impl Default for ProcessGroupOptions {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(2),
            escalate_to_kill: true,
            #[cfg(feature = "limits")]
            limits: ResourceLimits::default(),
        }
    }
}

impl ProcessGroupOptions {
    /// How long to wait after `SIGTERM` before escalating to `SIGKILL` (Unix;
    /// default 2 seconds). See [`shutdown`](ProcessGroup::shutdown).
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Whether to `SIGKILL` processes that outlive the shutdown grace window
    /// (default `true`).
    #[must_use]
    pub fn escalate_to_kill(mut self, escalate: bool) -> Self {
        self.escalate_to_kill = escalate;
        self
    }
}

#[cfg(feature = "limits")]
impl ProcessGroupOptions {
    /// Cap the tree's total memory at `bytes`. See [`ResourceLimits`] for platform
    /// support.
    #[must_use]
    pub fn memory_max(mut self, bytes: u64) -> Self {
        self.limits.memory_max = Some(bytes);
        self
    }

    /// Cap the number of live processes in the tree at `n`.
    #[must_use]
    pub fn max_processes(mut self, n: u32) -> Self {
        self.limits.max_processes = Some(n);
        self
    }

    /// Cap the tree's CPU at `cores` cores' worth (`0.5` = half a core, `2.0` = two
    /// cores). See [`ResourceLimits::cpu_quota`] for the Windows approximation.
    #[must_use]
    pub fn cpu_quota(mut self, cores: f64) -> Self {
        self.limits.cpu_quota = Some(cores);
        self
    }
}

/// A container that ties the lifetime of a child-process tree to its own.
///
/// Every process spawned into the group — and everything *those* processes
/// spawn — is killed when the group is dropped (kill-on-close), so an exiting or
/// panicking owner never leaks subprocesses. The containment mechanism is
/// platform-specific and observable via [`mechanism`](Self::mechanism).
///
/// Dropping the group performs an immediate **hard** kill. For a graceful
/// `SIGTERM` → wait → `SIGKILL` teardown (Unix), call
/// [`shutdown`](Self::shutdown) instead — `Drop` cannot `await`, so the graceful
/// tier lives in that async method.
///
/// The drop guarantee covers every exit that runs destructors (returns,
/// panics with unwinding). If the owner dies **abruptly** — `SIGKILL`,
/// `std::process::abort` — `Drop` never runs: on Windows the kernel still
/// kills the tree (the job handle closes with the process), elsewhere that
/// hardening is the opt-in
/// [`Command::kill_on_parent_death`](crate::Command::kill_on_parent_death)
/// (Linux, direct child only; unavailable on macOS/BSD).
pub struct ProcessGroup {
    job: Job,
    options: ProcessGroupOptions,
}

// Manual: the platform `Job` is an opaque OS handle.
impl std::fmt::Debug for ProcessGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessGroup")
            .field("mechanism", &self.mechanism())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl ProcessGroup {
    /// Create an empty group with [default options](ProcessGroupOptions).
    pub fn new() -> Result<Self> {
        Self::with_options(ProcessGroupOptions::default())
    }

    /// Create an empty group with the given options.
    #[cfg_attr(
        feature = "limits",
        doc = "",
        doc = "If `options.limits` sets any cap, it is enforced now. When the active",
        doc = "mechanism can't honor a requested limit (no cgroup/Job Object, or a Linux",
        doc = "cgroup without controller delegation) this returns",
        doc = "[`Error::ResourceLimit`] rather than handing back an unbounded group."
    )]
    pub fn with_options(options: ProcessGroupOptions) -> Result<Self> {
        #[cfg(feature = "limits")]
        let job = {
            validate_limits(&options.limits)?;
            Job::new(&options.limits).map_err(|source| {
                // A failure while limits were requested means we could not enforce
                // them — surface that distinctly so the caller never assumes a cap
                // is live.
                if options.limits.any() {
                    Error::ResourceLimit(source.to_string())
                } else {
                    Error::Io(source)
                }
            })?
        };
        #[cfg(not(feature = "limits"))]
        let job = Job::new()?;
        Ok(Self { job, options })
    }

    /// Spawn `cmd` as a member of this group.
    ///
    /// The returned [`Child`] — and any process it later spawns — belongs to the
    /// group and is reaped when the group is killed or dropped. The caller is
    /// responsible for configuring `cmd`'s stdio; the group only handles
    /// containment.
    ///
    /// **Windows:** to make containment race-free the child is created
    /// `CREATE_SUSPENDED`, assigned to the job, then resumed. This **overwrites**
    /// any process-creation flags the caller set on `cmd` (e.g.
    /// `CREATE_NO_WINDOW`) — Win32 exposes no way to read them back and OR the
    /// suspend bit in. The `Command`-driven launch paths (run helpers,
    /// [`start`](Self::start), pipelines) don't have this limitation: their
    /// [`Command::create_no_window`](crate::Command::create_no_window) flag
    /// travels alongside the OS command and is OR'd in. Only this raw escape
    /// hatch forces `CREATE_SUSPENDED` alone.
    /// **Unix:** the group likewise installs a `pre_exec` hook on `cmd` to join
    /// the cgroup / process group.
    ///
    /// These mutations make `cmd` **single-use**: each call appends another
    /// `pre_exec` hook (Unix) and re-sets the creation flags (Windows), so reusing
    /// the same [`Command`] across spawns stacks them. Build a fresh `cmd` per
    /// spawn. (The crate's own run helpers do this — every run rebuilds the OS
    /// command — so this only concerns direct `spawn` callers.)
    pub fn spawn(&self, cmd: &mut Command) -> Result<Child> {
        self.spawn_with_options(cmd, &crate::sys::SpawnOptions::default())
    }

    /// `spawn`, carrying the per-spawn knobs a raw `tokio::process::Command`
    /// can't (extra Windows creation flags; the setsid/process-group
    /// coordination). The `Command`-driven launch path.
    pub(crate) fn spawn_with_options(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> Result<Child> {
        let child = self.job.spawn(cmd, opts).map_err(|source| Error::Spawn {
            program: program_name(cmd),
            source,
        })?;
        Ok(child)
    }

    /// Attach an already-started [`Child`] to this group.
    ///
    /// Only the child itself is moved into the group; processes it has *already*
    /// spawned keep their original containment (future forks are captured). On
    /// targets without a job mechanism (non-unix, non-Windows) this is a no-op.
    ///
    /// On the POSIX process-group mechanism, a child that has already `exec`'d
    /// cannot be re-grouped (POSIX forbids it), so it is tracked
    /// *individually*: the child itself is signalled/killed with the group,
    /// but — unlike on Windows/cgroup — its future forks are not captured.
    /// The caller keeps the [`Child`] handle and is responsible for reaping:
    /// an adopted child that exited but was never awaited probes as alive, so
    /// a graceful [`shutdown`](Self::shutdown) can wait out its full timeout
    /// on the zombie before escalating.
    #[cfg(feature = "process-control")]
    pub fn adopt(&self, child: &Child) -> Result<()> {
        self.job.adopt(child)?;
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: "processkit",
            mechanism = ?self.mechanism(),
            pid = ?child.id(),
            "adopted an externally spawned child"
        );
        Ok(())
    }

    /// Immediately hard-kill every process currently in the group. Idempotent;
    /// the group remains usable for further spawns afterwards.
    pub fn terminate_all(&self) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            mechanism = ?self.mechanism(),
            "terminating every process in the group"
        );
        self.job.kill_all()?;
        Ok(())
    }

    /// Broadcast `sig` to every process in the group.
    ///
    /// Best-effort: a member that has already exited is skipped, and an empty
    /// group succeeds trivially.
    ///
    /// # Platform support
    ///
    /// - **Linux (cgroup or process-group fallback), macOS/BSD** — any signal,
    ///   delivered to every live member of the tree.
    /// - **Windows** — only [`Signal::Kill`]; any other signal — including
    ///   [`Signal::Other`] — returns [`Error::Unsupported`].
    /// - **No-containment target** — always [`Error::Unsupported`].
    ///
    /// `SIGKILL` ([`Signal::Kill`], or `Other(libc::SIGKILL)`) is routed through
    /// the same whole-tree hard kill as [`terminate_all`](Self::terminate_all)
    /// on every backend (`cgroup.kill` / `killpg` / Job Object terminate), so it
    /// cannot miss a process forked mid-broadcast. Other signals are a per-member
    /// broadcast.
    #[cfg(feature = "process-control")]
    pub fn signal(&self, sig: Signal) -> Result<()> {
        self.job
            .signal(sig)
            .map_err(|source| map_unsupported(source, format!("signal({sig:?})")))
    }

    /// Suspend (freeze) every process in the group.
    ///
    /// # Platform support
    ///
    /// - **Linux cgroup** — one `cgroup.freeze` write covering the whole subtree
    ///   (kernel ≥ 5.2; older kernels fall back to per-process `SIGSTOP`). The
    ///   freeze is applied by the kernel shortly after the write returns, not
    ///   instantaneously.
    /// - **Linux process-group fallback, macOS/BSD** — `SIGSTOP` to every
    ///   group; an individually-tracked adopted child (see
    ///   [`adopt`](Self::adopt)) is frozen alone — its own descendants keep
    ///   running.
    /// - **Windows** — suspends every thread of every member process. Best-effort
    ///   and not atomic: threads spawned mid-walk can be missed, and Windows keeps
    ///   per-thread suspend *counts*, so nested `suspend` calls stack — N suspends
    ///   need N [`resume`](Self::resume)s. On Unix suspend/resume are idempotent
    ///   (level-triggered).
    /// - **No-containment target** — [`Error::Unsupported`].
    ///
    /// A suspended tree can still be hard-killed
    /// ([`terminate_all`](Self::terminate_all), or dropping the group) — SIGKILL,
    /// `cgroup.kill`, and `TerminateJobObject` all act on frozen processes. The
    /// graceful [`shutdown`](Self::shutdown), however, starts with a `SIGTERM`
    /// that a frozen tree cannot act on until thawed, so it waits out
    /// `shutdown_timeout` and then escalates; call [`resume`](Self::resume) first
    /// for a clean graceful shutdown.
    ///
    /// **Spawning into a suspended group is platform-divergent.** Under the
    /// Linux cgroup mechanism the freeze is *group state*: a child spawned (or
    /// adopted) while the group is suspended joins the frozen cgroup and
    /// **starts frozen** — it does not run until [`resume`](Self::resume).
    /// Worse, the *spawn call itself* can block until then: the child joins
    /// the cgroup before `exec`, so it can freeze before the spawn handshake
    /// completes and [`start`](Self::start) never returns. The Windows and
    /// POSIX process-group mechanisms freeze only the members present at the
    /// call, so a later spawn runs normally. Don't start new work in a
    /// suspended group (e.g. a
    /// [`Supervisor::with_runner(&group)`](crate::Supervisor::with_runner)
    /// restarting into it) — resume first.
    #[cfg(feature = "process-control")]
    pub fn suspend(&self) -> Result<()> {
        self.job
            .suspend()
            .map_err(|source| map_unsupported(source, "suspend"))
    }

    /// Resume a tree suspended by [`suspend`](Self::suspend).
    ///
    /// See [`suspend`](Self::suspend) for the platform matrix and the Windows
    /// suspend-count nesting caveat.
    #[cfg(feature = "process-control")]
    pub fn resume(&self) -> Result<()> {
        self.job
            .resume()
            .map_err(|source| map_unsupported(source, "resume"))
    }

    /// The pids of the processes currently in the group.
    ///
    /// A point-in-time snapshot: a returned pid may belong to a process that
    /// exits (or is reaped) immediately afterwards, and a process spawned during
    /// the call may be missing. Useful for diagnostics, dashboards, and targeted
    /// per-pid action.
    ///
    /// # Platform support
    ///
    /// - **Windows** — every pid assigned to the Job Object (the whole tree).
    /// - **Linux cgroup** — every pid in the cgroup (`cgroup.procs`, whole tree).
    /// - **Linux process-group fallback, macOS/BSD** — the tracked **group
    ///   leaders**, plus any individually-tracked adopted child (one pid per
    ///   spawned/adopted child); descendants inside the groups are contained
    ///   but not enumerated. An exited child still counts as a member until it
    ///   is reaped (awaited): the liveness probe sees the not-yet-collected
    ///   process.
    /// - **No-containment target** — always empty: nothing is tracked.
    ///   [`Mechanism::None`](crate::Mechanism::None) (via
    ///   [`mechanism`](Self::mechanism)) is the cue that children are
    ///   *unmanaged*, not absent.
    #[cfg(feature = "process-control")]
    pub fn members(&self) -> Result<Vec<u32>> {
        let pids = self.job.members()?;
        Ok(pids)
    }

    /// Gracefully tear the group down, consuming it.
    ///
    /// On Unix: `SIGTERM` the tree, wait up to `shutdown_timeout`, then `SIGKILL`
    /// survivors when `escalate_to_kill` is set. On Windows the kill is atomic.
    /// Dropping the group instead (without calling this) performs only the hard
    /// kill.
    pub async fn shutdown(self) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            mechanism = ?self.mechanism(),
            timeout_ms = self.options.shutdown_timeout.as_millis() as u64,
            escalate = self.options.escalate_to_kill,
            "graceful shutdown: TERM, grace, then KILL"
        );
        self.job
            .graceful_shutdown(
                crate::sys::SIGTERM_RAW,
                self.options.shutdown_timeout,
                self.options.escalate_to_kill,
            )
            .await?;
        // `self` drops here; the job's Drop hard-kills any straggler (a no-op
        // after a successful graceful shutdown) and frees the OS handle/cgroup.
        Ok(())
    }

    /// Gracefully tear the tree down **without consuming** the group — the
    /// run-level graceful-timeout path holds an `Arc`/`Weak`, not an owned group.
    /// Sends `signal`, waits up to `grace`, then `SIGKILL`s survivors. On Windows
    /// the signal/grace are ignored (atomic job kill). Best-effort: the caller
    /// reaps the child and the group's `Drop` backstops any straggler.
    pub(crate) async fn graceful_terminate(&self, grace: Duration, signal: i32) -> Result<()> {
        self.job
            .graceful_shutdown(signal, grace, /* escalate */ true)
            .await?;
        Ok(())
    }

    /// Snapshot the group's resource usage (active process count and, where the
    /// platform supports it, total CPU time and peak memory). See
    /// [`ProcessGroupStats`].
    #[cfg(feature = "stats")]
    pub fn stats(&self) -> Result<ProcessGroupStats> {
        let stats = self.job.stats()?;
        Ok(stats)
    }

    /// Sample [`stats`](Self::stats) on an interval, as a
    /// [`Stream`](tokio_stream::Stream) of snapshots — a time-series of the
    /// group's CPU/memory/process-count for benchmarking and diagnostics.
    ///
    /// The first sample is taken immediately; the series ends on the first
    /// snapshot the group fails to report. The sampler borrows the group, so it
    /// never keeps the group (or its kill-on-drop guarantee) alive. What each
    /// snapshot can report per platform is exactly [`stats`](Self::stats)'s
    /// matrix. A zero `every` is clamped to 1 ms.
    ///
    /// ```no_run
    /// # async fn demo() -> processkit::Result<()> {
    /// use processkit::{Command, ProcessGroup, StreamExt};
    /// use std::time::Duration;
    ///
    /// let group = ProcessGroup::new()?;
    /// let _worker = group.start(&Command::new("worker")).await?;
    /// let mut samples = group.sample_stats(Duration::from_millis(250));
    /// while let Some(s) = samples.next().await {
    ///     println!("procs={} rss={:?}", s.active_process_count, s.peak_memory_bytes);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "stats")]
    pub fn sample_stats(&self, every: Duration) -> crate::stats::StatsSampler<'_> {
        crate::stats::StatsSampler::new(self, every)
    }

    /// The containment mechanism actually in effect (see [`Mechanism`]).
    pub fn mechanism(&self) -> Mechanism {
        self.job.mechanism()
    }
}

/// Best-effort program name for error messages.
fn program_name(cmd: &Command) -> String {
    cmd.as_std().get_program().to_string_lossy().into_owned()
}

/// Map a backend `ErrorKind::Unsupported` to the typed [`Error::Unsupported`],
/// passing every other IO failure through unchanged. Unambiguous here: on the
/// signal/suspend/resume paths the only producer of `Unsupported` is the
/// backends' own "this platform can't do that" reporting.
#[cfg(feature = "process-control")]
fn map_unsupported(source: std::io::Error, operation: impl Into<String>) -> Error {
    if source.kind() == std::io::ErrorKind::Unsupported {
        Error::Unsupported {
            operation: operation.into(),
        }
    } else {
        Error::Io(source)
    }
}

/// Reject nonsensical limit values before touching the OS, so a typo surfaces as a
/// clear [`Error::ResourceLimit`] rather than an opaque kernel error.
#[cfg(feature = "limits")]
fn validate_limits(limits: &ResourceLimits) -> Result<()> {
    if limits.memory_max == Some(0) {
        return Err(Error::ResourceLimit(
            "memory_max must be greater than 0".into(),
        ));
    }
    if limits.max_processes == Some(0) {
        return Err(Error::ResourceLimit(
            "max_processes must be greater than 0".into(),
        ));
    }
    if let Some(cores) = limits.cpu_quota
        && !(cores.is_finite() && cores > 0.0)
    {
        return Err(Error::ResourceLimit(
            "cpu_quota must be a finite value greater than 0".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, feature = "limits"))]
mod tests {
    use super::*;

    #[test]
    fn builders_set_limits() {
        let opts = ProcessGroupOptions::default()
            .memory_max(1024)
            .max_processes(8)
            .cpu_quota(0.5);
        assert_eq!(opts.limits.memory_max, Some(1024));
        assert_eq!(opts.limits.max_processes, Some(8));
        assert_eq!(opts.limits.cpu_quota, Some(0.5));
        assert!(opts.limits.any());
    }

    #[test]
    fn default_options_have_no_limits() {
        let opts = ProcessGroupOptions::default();
        assert!(!opts.limits.any());
    }

    #[test]
    fn validate_rejects_nonsense() {
        for opts in [
            ProcessGroupOptions::default().memory_max(0),
            ProcessGroupOptions::default().max_processes(0),
            ProcessGroupOptions::default().cpu_quota(0.0),
            ProcessGroupOptions::default().cpu_quota(-1.0),
            ProcessGroupOptions::default().cpu_quota(f64::NAN),
            ProcessGroupOptions::default().cpu_quota(f64::INFINITY),
        ] {
            assert!(matches!(
                validate_limits(&opts.limits),
                Err(Error::ResourceLimit(_))
            ));
            // The public entry point rejects them too, before any OS work.
            assert!(matches!(
                ProcessGroup::with_options(opts),
                Err(Error::ResourceLimit(_))
            ));
        }
    }
}
