//! [`ProcessGroup`] — a kill-on-drop container for a tree of child processes.

use std::time::Duration;

use tokio::process::{Child, Command};

#[cfg(any(feature = "limits", feature = "process-control"))]
use crate::error::ErrorReason;
use crate::error::{Error, Result};
#[cfg(feature = "limits")]
use crate::limits::{CappedAxes, LimitEvidence, LimitKind, LimitReason, ResourceLimits};
use crate::mechanism::Mechanism;
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "process-control")]
use crate::shutdown_report::ShutdownReport;
#[cfg(feature = "process-control")]
use crate::signal::Signal;
#[cfg(feature = "process-control")]
use crate::soft_stop::SoftStopScope;
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
    pub fn max_memory(mut self, bytes: u64) -> Self {
        self.limits.max_memory = Some(bytes);
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
    /// Every limit axis this group has carried a cap on, at any point — sticky, so
    /// [`limit_evidence`](Self::limit_evidence) still reports a cap that
    /// [`update_limits`](Self::update_limits) has since lifted (it was in force, and
    /// it may well have fired), and reads nothing for an axis never capped.
    #[cfg(feature = "limits")]
    capped: CappedAxes,
}

// Manual: `Job` is an opaque OS handle.
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
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the OS rejects creating the group's containment primitive
    /// (a Job Object on Windows, a cgroup on Linux). The default options set no
    /// resource caps, so no limit-enforcement failure can arise.
    pub fn new() -> Result<Self> {
        Self::with_options(ProcessGroupOptions::default())
    }

    /// Create an empty group with the given options.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the OS rejects creating the group's containment primitive.
    #[cfg_attr(
        feature = "limits",
        doc = "",
        doc = "With the `limits` feature, if `options.limits` sets any cap it is enforced",
        doc = "now. When the active mechanism can't honor a requested limit (no",
        doc = "cgroup/Job Object, or a Linux cgroup whose controllers can't be enabled —",
        doc = "see [`ResourceLimits`] for the cgroup-v2 real-root requirement) this",
        doc = "returns [`crate::ErrorReason::ResourceLimit`] — rather than handing back an unbounded",
        doc = "group — and an invalid cap value returns it too, with",
        doc = "[`LimitReason::Invalid`]."
    )]
    pub fn with_options(options: ProcessGroupOptions) -> Result<Self> {
        #[cfg(feature = "limits")]
        let job = {
            validate_limits(&options.limits)?;
            Job::new(&options.limits).map_err(|source| {
                if options.limits.any() {
                    // A real signal from the backend, not a guess: every
                    // backend reports `ErrorKind::Unsupported` exactly when no
                    // whole-tree container mechanism exists at all on this
                    // platform (macOS/BSD's POSIX-only fallback, a Linux host
                    // with no cgroup v2 mounted) — the same convention
                    // `map_unsupported` relies on for the signal/suspend/resume
                    // paths. Every other failure (Linux delegation/
                    // subtree_control rejected, a Windows Job Object call
                    // failing) means a capable mechanism exists but this
                    // request could not be applied to it.
                    let reason = if source.kind() == std::io::ErrorKind::Unsupported {
                        LimitReason::Unsupported
                    } else {
                        LimitReason::Unenforceable
                    };
                    ErrorReason::ResourceLimit {
                        kind: first_requested_kind(&options.limits),
                        reason,
                        detail: source.to_string(),
                    }
                    .into()
                } else {
                    Error::io(source)
                }
            })?
        };
        #[cfg(not(feature = "limits"))]
        let job = Job::new().map_err(Error::io)?;
        // Record the axes this group starts out capped on, for the post-run
        // `limit_evidence` report. Only reached once creation SUCCEEDED, so a
        // rejected cap (`ErrorReason::ResourceLimit`) never leaves a phantom axis
        // recorded on a group that was never created.
        #[cfg(feature = "limits")]
        let capped = {
            let mut capped = CappedAxes::default();
            capped.record(&options.limits);
            capped
        };
        Ok(Self {
            job,
            options,
            #[cfg(feature = "limits")]
            capped,
        })
    }

    /// Spawn `cmd` as a member of this group.
    ///
    /// The returned [`Child`] — and any process it later spawns — belongs to the
    /// group and is reaped when the group is killed or dropped. The caller is
    /// responsible for configuring `cmd`'s stdio; the group only handles
    /// containment. To build a capture-wired `tokio::process::Command` from a
    /// [`Command`](crate::Command), use its
    /// [`to_tokio_command()`](crate::Command::to_tokio_command) escape-hatch
    /// bridge, or construct the `tokio::process::Command` directly.
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
    /// These mutations make `cmd` **single-use**: the spawn appends a `pre_exec`
    /// hook (Unix) and re-sets the creation flags (Windows), which would stack if
    /// the same command were spawned twice. **`spawn` takes `cmd` by value** so
    /// that reuse is a compile error rather than a silent hook-stacking footgun —
    /// build a fresh `Command` per spawn. (The crate's own run helpers
    /// already rebuild the OS command per run, so this only ever concerned direct
    /// `spawn` callers.)
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Spawn`] if the OS refuses to start `cmd` — the working directory
    /// is bad, permission is denied, and so on. (This raw path reports every
    /// launch failure as [`crate::ErrorReason::Spawn`]; the `Command`-driven run helpers, by
    /// contrast, translate a not-found program into [`crate::ErrorReason::NotFound`].)
    pub fn spawn(&self, mut cmd: Command) -> Result<Child> {
        self.spawn_with_options(&mut cmd, &crate::sys::SpawnOptions::default())
    }

    /// `spawn`, carrying the per-spawn knobs a raw `tokio::process::Command`
    /// can't (extra Windows creation flags; the setsid/process-group
    /// coordination). The `Command`-driven launch path.
    pub(crate) fn spawn_with_options(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> Result<Child> {
        let child = self
            .job
            .spawn(cmd, opts)
            .map_err(|source| Error::spawn(program_name(cmd), source))?;
        Ok(child)
    }

    /// `spawn_with_options` under a pseudo-terminal (the
    /// [`Command::use_pty`](crate::Command::use_pty) launch path): the child joins
    /// this group exactly as [`spawn_with_options`](Self::spawn_with_options)'s
    /// does, but over a single PTY master instead of three pipes. `env` is the
    /// child's resolved environment for the Windows raw-spawn path.
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty_with_options(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
        env: Option<Vec<(std::ffi::OsString, std::ffi::OsString)>>,
    ) -> Result<crate::sys::pty::PtySpawn> {
        self.job
            .spawn_pty(cmd, opts, env)
            .map_err(|source| Error::spawn(program_name(cmd), source))
    }

    /// Attach an already-started [`Child`] to this group.
    ///
    /// Only the child itself is moved into the group; processes it has *already*
    /// spawned keep their original containment (future forks are captured).
    ///
    /// On the POSIX process-group mechanism, a child that has already `exec`'d
    /// cannot be re-grouped (POSIX forbids it), so it is tracked
    /// *individually*: the child itself is signalled/killed with the group,
    /// but — unlike on Windows/cgroup — its future forks are not captured.
    /// The caller keeps the [`Child`] handle and is responsible for reaping:
    /// an adopted child that exited but was never awaited probes as alive, so
    /// a graceful [`shutdown`](Self::shutdown) can wait out its full timeout
    /// on the zombie before escalating.
    ///
    /// **Reap promptly (pid-reuse hazard).** An individually-tracked adopted child
    /// is remembered by **pid**. If you let it exit *and be reaped* elsewhere
    /// without dropping/tearing down this group, that pid can be recycled by the OS
    /// to an **unrelated** process — and this group's later teardown would then
    /// signal that stranger. The crate has no start-time identity to detect the
    /// reuse (and macOS's small pid space makes it likelier). Reap an adopted child
    /// through this group's lifetime, or tear the group down when done, so a stale
    /// pid is never carried across a reuse.
    ///
    /// On the containment backends, adopting a child that has already **exited
    /// but not yet been reaped** is a successful no-op (`Ok`) — there is nothing
    /// left to contain — while an **already-reaped** child (one that was
    /// `wait`ed, so its handle/pid is gone) errors, since there is no longer
    /// anything to reference.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if `child` has already been reaped (awaited), leaving no
    /// live handle/pid to reference. Adopting an exited-but-unreaped child is a
    /// successful no-op.
    #[cfg(feature = "process-control")]
    pub fn adopt(&self, child: &Child) -> Result<()> {
        self.job.adopt(child).map_err(Error::io)?;
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
    ///
    /// This is an unconditional **hard** kill (`SIGKILL` / `cgroup.kill` /
    /// `TerminateJobObject`), not a graceful `SIGTERM` — for a `SIGTERM` → grace →
    /// `SIGKILL` teardown use [`shutdown`](Self::shutdown) /
    /// [`shutdown_ref`](Self::shutdown_ref). The name mirrors the underlying
    /// `Job::kill_all` it delegates to.
    ///
    /// On the legacy per-pid kill fallback (a Linux kernel without `cgroup.kill`,
    /// pre-5.14), a tree that won't drain within the bounded sweep — a fork bomb
    /// still out-spawning, or un-reapable `D`-state zombies — surfaces as an `Err`
    /// rather than a false success; the atomic backends (`cgroup.kill`, Windows
    /// Job Object) don't need to.
    ///
    /// **Process-group mechanism (macOS/BSD, Linux process-group fallback).** A
    /// member that changed its real/saved uid (a `sudo`/setuid child) and rejects
    /// `SIGKILL` with `EPERM` while still **alive** is surfaced as an `Err` — the
    /// containment gap is reported, not hidden. The one `EPERM` that is *not*
    /// surfaced is the harmless one: on those platforms `killpg` also returns
    /// `EPERM` for a group whose only member is an unreaped **zombie** (dead), and
    /// the two are indistinguishable from the errno alone, so the target's actual
    /// run state is checked after the `EPERM` (`proc_pidinfo` on macOS, the
    /// `/proc/<pid>/stat` state field on the Linux fallback) and only a
    /// genuinely-alive, non-zombie member fails the call. On the **BSDs**, where no
    /// process-state reader is wired up, a delivery `EPERM` stays swallowed
    /// (best-effort), so a privileged child can still outlive `kill_all` there — the
    /// atomic mechanisms (`cgroup.kill`, Job Object) have no such gap.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] in two cases, both on the non-atomic Unix backends: the legacy
    /// per-pid kill fallback (a pre-5.14 Linux kernel without `cgroup.kill`) when
    /// the tree won't drain within the bounded sweep, and the process-group
    /// mechanism (macOS/Linux fallback) when a live, non-zombie member rejects
    /// `SIGKILL` with `EPERM` (a uid-changed child — see above). The atomic backends
    /// (`cgroup.kill`, Windows Job Object) never fail here.
    pub fn kill_all(&self) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            mechanism = ?self.mechanism(),
            "hard-killing every process in the group"
        );
        self.job.kill_all().map_err(Error::io)?;
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
    ///   attempted for every live member of the tree.
    /// - **Windows** — [`Signal::Kill`] (the atomic Job Object terminate) always;
    ///   [`Signal::Int`] / [`Signal::Term`] as a best-effort **soft close** —
    ///   `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)` to any child spawned with
    ///   [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break),
    ///   plus `WM_CLOSE` to every top-level window owned by a live member (an
    ///   Electron app, a desktop tool, a windowed service). This TRIGGERS a clean
    ///   exit without waiting or escalating. It returns [`crate::ErrorReason::Unsupported`] only
    ///   when the group has **neither** a console-CTRL leader **nor** a windowed
    ///   member (nothing a soft close could reach); every other signal
    ///   ([`Signal::Hup`], [`Signal::Quit`], [`Signal::Usr1`], [`Signal::Usr2`],
    ///   [`Signal::Other`]) is always [`crate::ErrorReason::Unsupported`].
    ///
    /// `SIGKILL` ([`Signal::Kill`], or `Other(libc::SIGKILL)`) is routed through
    /// the same whole-tree hard kill as [`kill_all`](Self::kill_all)
    /// on every backend (`cgroup.kill` / `killpg` / Job Object terminate), so it
    /// cannot miss a process forked mid-broadcast. Other signals are a per-member
    /// broadcast.
    ///
    /// **Honest send failures (every Unix backend).** A genuinely failed send is
    /// reported as [`crate::ErrorReason::Io`], not hidden behind a false `Ok`, and the two POSIX
    /// mechanisms agree on which failures those are:
    /// - an **`EINVAL`** (an out-of-range [`Signal::Other`] number) always surfaces;
    /// - an **`EPERM`** surfaces when it hit a **live, non-zombie** member (a
    ///   `sudo`/setuid child that rejects the signal — the genuine containment gap),
    ///   on both the cgroup mechanism and the process-group mechanism (which checks
    ///   the target's run state — `proc_pidinfo` on macOS, the `/proc/<pid>/stat`
    ///   state field on the Linux fallback — after the `EPERM`, exactly as
    ///   [`kill_all`](Self::kill_all) does). The one `EPERM` deliberately swallowed
    ///   is the harmless zombie-only case;
    /// - an **`ESRCH`** (the member already exited) is always a benign no-op success.
    ///
    /// On the **BSDs**, where no process-state reader is wired up, a delivery
    /// `EPERM` stays swallowed (best-effort) — the same residual gap
    /// [`kill_all`](Self::kill_all) documents. `SIGKILL` here goes through the same
    /// whole-tree hard kill as [`kill_all`](Self::kill_all), so a rejected hard kill
    /// surfaces identically whichever verb you call.
    ///
    /// **[`Signal::Other(0)`](Signal::Other) is an existence probe, not a delivery.**
    /// Signal `0` checks whether targets exist and delivers nothing; a
    /// `signal(Other(0))` over a group with live members therefore returns `Ok`
    /// **having sent no signal** — the `Ok` means "a signalable target was reached",
    /// not "a signal was delivered". It can still surface an `EPERM` against a live
    /// target that rejects even the null signal, identically on both POSIX backends.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Unsupported`] on Windows for [`Signal::Int`] / [`Signal::Term`]
    /// only when the group has no console-CTRL leader and no windowed member (see
    /// Platform support above), and for every other non-[`Kill`](Signal::Kill)
    /// signal unconditionally (a Job Object has no POSIX signals). On **every** Unix
    /// backend (cgroup and process-group alike), [`crate::ErrorReason::Io`] if the OS honestly
    /// rejects the send — an `EINVAL` (a bad [`Signal::Other`] number) or an `EPERM`
    /// against a live, non-zombie member (see above); an `ESRCH` (member already
    /// gone) and a harmless zombie-only `EPERM` are not errors. The Windows soft
    /// close is likewise best-effort (an enumeration / post failure never fails a
    /// call that reached a target).
    #[cfg(feature = "process-control")]
    pub fn signal(&self, sig: Signal) -> Result<()> {
        self.job
            .signal(sig)
            .map_err(|source| map_unsupported(source, format!("signal({sig:?})")))
    }

    /// The reach of a **soft stop** on this group *right now* — the honest
    /// capability answer to "if I ask this group to stop gracefully
    /// ([`signal(Signal::Term)`](Self::signal) / [`Signal::Int`]),
    /// which of its members will actually receive it?" — queried **before** the
    /// attempt so a caller need not fire a `signal`, catch an
    /// [`crate::ErrorReason::Unsupported`], and reverse-engineer the
    /// scope.
    ///
    /// The group-axis analogue of
    /// [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope):
    /// where that reports the abrupt-owner-death cleanup reach fixed per platform,
    /// this reports the *deliberate soft stop* reach read from this group's **live
    /// membership**, so the same build can answer differently for different groups
    /// (most visibly on Windows). See [`SoftStopScope`] for the full contract.
    ///
    /// # Side-effect-free
    ///
    /// A pure read: it delivers **no** signal, posts **no** `WM_CLOSE`, spawns
    /// nothing, creates no container, and does not mutate the group — asking never
    /// changes what a subsequent [`signal`](Self::signal) does. It is read from
    /// the *same* live-membership primitives `signal(Int/Term)` acts on, so its
    /// answer is consistent with the outcome a real soft stop would then have.
    ///
    /// # Platform reach
    ///
    /// - **Linux cgroup v2, macOS/BSD, Linux process-group fallback** —
    ///   [`SoftStopScope::WholeTree`]: `signal(Int/Term)` reaches every member of
    ///   the tree (the cgroup, or every tracked process group via `killpg`), so a
    ///   soft stop is always available and never `Unsupported` here.
    /// - **Windows** — [`SoftStopScope::OptInMembers`] when the group holds a live
    ///   console-CTRL leader (a child spawned with
    ///   [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break))
    ///   or a live windowed member (reachable by `WM_CLOSE`);
    ///   [`SoftStopScope::Unsupported`] when it holds **neither** (an empty group,
    ///   or plain windowless children with no console opt-in), which is exactly
    ///   when [`signal(Signal::Term)`](Self::signal) would return
    ///   [`crate::ErrorReason::Unsupported`].
    ///
    /// This describes the *soft* tier only: the unconditional hard kill
    /// ([`Signal::Kill`], [`kill_all`](Self::kill_all),
    /// dropping the group) always tears the whole tree down regardless of this
    /// value.
    #[cfg(feature = "process-control")]
    pub fn soft_stop_scope(&self) -> SoftStopScope {
        self.job.soft_stop_scope()
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
    ///
    /// A suspended tree can still be hard-killed
    /// ([`kill_all`](Self::kill_all), or dropping the group) — SIGKILL,
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
    ///
    /// On **Unix**, a graceful [`shutdown`](Self::shutdown) of a suspended group
    /// cannot drain (C7): frozen members don't run their `SIGTERM` handlers (and a
    /// `SIGSTOP`'d member keeps the signal pending), so the graceful phase burns
    /// its full `shutdown_timeout` and then hard-kills — or, under
    /// `escalate_to_kill = false`, spares the still-frozen survivors.
    /// [`resume`](Self::resume) before a graceful shutdown if you want the members
    /// to actually handle the signal. (On **Windows** the point is moot: a graceful
    /// shutdown is a prompt hard kill regardless — there's no soft-signal tier and
    /// no grace wait — so a suspended group is torn down at once, not after the
    /// timeout.)
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Unsupported`] if the active mechanism cannot freeze the tree;
    /// otherwise [`crate::ErrorReason::Io`] if the OS rejects the freeze / `SIGSTOP`.
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
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Unsupported`] if the active mechanism cannot thaw the tree;
    /// otherwise [`crate::ErrorReason::Io`] if the OS rejects the resume / `SIGCONT`.
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
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the group's membership cannot be read (e.g. a failed
    /// `cgroup.procs` read or Job Object query).
    #[cfg(feature = "process-control")]
    pub fn members(&self) -> Result<Vec<u32>> {
        let pids = self.job.members().map_err(Error::io)?;
        Ok(pids)
    }

    /// An enriched, point-in-time snapshot of the group's members — the same set
    /// as [`members`](Self::members), but each pid carried in a [`MemberInfo`]
    /// alongside best-effort parent pid, image name, and start time.
    ///
    /// The metadata-carrying companion to [`members`](Self::members): use it for
    /// diagnostics that want more than bare pids (a `members_snapshot` event, a
    /// process tree view). *Which* processes appear is identical to
    /// [`members`](Self::members) — see its platform matrix — and each enriching
    /// field is `None` wherever the platform can't report it, never a fabricated
    /// value (full per-field matrix on [`MemberInfo`]).
    ///
    /// # Platform support
    ///
    /// - **Windows** — the whole tree (every pid in the Job Object); ppid and image
    ///   name from one `Toolhelp32` process snapshot, start time (creation
    ///   `FILETIME`) per pid.
    /// - **Linux cgroup** — the whole tree (`cgroup.procs`); ppid, `comm` image
    ///   name, and start time from one `/proc/<pid>/stat` read each.
    /// - **Linux process-group fallback** — the tracked group **leaders** (as
    ///   [`members`](Self::members)), enriched from `/proc` the same way.
    /// - **macOS** — the tracked leaders; ppid / image name / start time via
    ///   `proc_pidinfo`.
    /// - **the BSDs** — the tracked leaders with every enriching field `None` (no
    ///   wired-up per-process reader — see [`MemberInfo::start_time`]); the pid is
    ///   still reported, which is a correct result, not an error.
    ///
    /// # Racing a member that exits
    ///
    /// A point-in-time snapshot taken per pid: if a member exits **between** its
    /// pid being enumerated and its metadata being read, that pid is **skipped**
    /// (omitted from the `Vec`) rather than reported with fabricated fields — one
    /// vanished member never fails the whole call. A member that is still present
    /// but for which only some finer field can't be read (e.g. its start-time
    /// handle just closed) is kept, with that field `None`.
    ///
    /// # No command line
    ///
    /// The raw argv / environment is **deliberately never** included, on any
    /// platform — a command line routinely carries secrets, and redaction is the
    /// consumer's policy to own (the crate's standing "never argv/env" stance).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] only if the group's membership cannot be read (the same
    /// failure as [`members`](Self::members) — a failed `cgroup.procs` read or Job
    /// Object query) or, on Windows, if the process-metadata snapshot cannot be
    /// created at all. A single member vanishing is not an error (it is skipped).
    #[cfg(feature = "process-control")]
    pub fn members_info(&self) -> Result<Vec<MemberInfo>> {
        let infos = self.job.members_info().map_err(Error::io)?;
        Ok(infos)
    }

    /// Gracefully tear the group down, consuming it.
    ///
    /// On Unix: `SIGTERM` the tree, wait up to `shutdown_timeout`, then `SIGKILL`
    /// survivors when `escalate_to_kill` is set. On Windows the kill is atomic.
    /// Dropping the group instead (without calling this) performs only the hard
    /// kill.
    ///
    /// **Reap your children, or the grace is wasted (POSIX process-group
    /// mechanism only).** On the [`Mechanism::ProcessGroup`](crate::Mechanism)
    /// fallback (macOS/the BSDs, and Linux without a usable cgroup), liveness is
    /// probed by signalling the group id, and an **unreaped zombie still answers**
    /// — its process-group entry survives until the child is `wait`ed. So a child
    /// that exits promptly on `SIGTERM` but whose [`RunningProcess`](crate::RunningProcess)
    /// handle was dropped without being awaited (or is still held un-awaited) reads
    /// as alive for the full `shutdown_timeout`, and `shutdown` then burns the
    /// whole grace plus a pointless `SIGKILL` escalation. Await each child you
    /// start into the group (any consuming verb, or `wait`) so its handle reaps it.
    /// The Windows Job Object and Linux cgroup mechanisms are immune (a process
    /// leaves `cgroup.procs` / the job on *exit*, before reaping).
    ///
    /// When `escalate_to_kill` is set, the final hard kill can surface the same
    /// errors as [`kill_all`](Self::kill_all): the undrained-tree `Err` on the
    /// legacy pre-5.14 per-pid fallback, and — on the process-group mechanism — a
    /// live, non-zombie member that rejects `SIGKILL` with `EPERM` (a uid-changed
    /// child). A harmless zombie-only group is not reported (see
    /// [`kill_all`](Self::kill_all)).
    ///
    /// Holding the group behind a shared handle (an `Arc`, a long-lived
    /// supervisor) that can't be moved out by value? Use the borrowing twin
    /// [`shutdown_ref`](Self::shutdown_ref) — same teardown, `&self`.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the graceful teardown fails — including, when
    /// `escalate_to_kill` performs the final hard kill, the same failures as
    /// [`kill_all`](Self::kill_all): the undrained-tree failure on the legacy
    /// pre-5.14 per-pid fallback, and a process-group member that rejects `SIGKILL`
    /// with `EPERM` while still alive.
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_ref().await
    }

    /// Gracefully tear the group down **without consuming it** — the borrowing
    /// twin of [`shutdown`](Self::shutdown), for a group held behind a shared
    /// handle (an `Arc`, a supervisor) that cannot be moved out by value to call
    /// the consuming form.
    ///
    /// Identical teardown to [`shutdown`](Self::shutdown): on Unix, `SIGTERM` the
    /// tree, wait up to the configured
    /// [`shutdown_timeout`](ProcessGroupOptions::shutdown_timeout), then `SIGKILL`
    /// survivors when [`escalate_to_kill`](ProcessGroupOptions::escalate_to_kill)
    /// is set; on Windows the kill is atomic and the timeout is ignored. The group
    /// stays usable afterwards (a re-`shutdown_ref` on an already-drained tree is a
    /// near no-op). Spawning or adopting a new child **re-arms** `Drop`'s kill
    /// backstop for the whole group, so a straggler started after — or
    /// *concurrently with* — this shutdown is still torn down on `Drop`: a
    /// non-escalating shutdown that is still in flight when the child joins cannot
    /// silently strip the newcomer of its backstop (its spare is keyed to a
    /// generation the join bumps). A group left untouched keeps the survivors an
    /// [`escalate_to_kill`](ProcessGroupOptions::escalate_to_kill)` = false`
    /// shutdown chose to spare.
    ///
    /// The same reaping caveat as [`shutdown`](Self::shutdown) applies on the
    /// POSIX process-group mechanism: await each child you started into the group,
    /// or an unreaped zombie reads as alive for the whole grace.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the graceful teardown fails (see
    /// [`shutdown`](Self::shutdown) — the same undrained-tree failure on the legacy
    /// per-pid fallback and the process-group live-`EPERM` on the final hard kill
    /// apply).
    pub async fn shutdown_ref(&self) -> Result<()> {
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
            .await
            .map_err(Error::io)?;
        Ok(())
    }

    /// Gracefully stop the group with an explicit `grace` and escalation, returning
    /// a [`ShutdownReport`] of what the teardown **actually observed** — the
    /// introspective, parameterized sibling of
    /// [`shutdown_ref`](Self::shutdown_ref).
    ///
    /// Like [`shutdown_ref`](Self::shutdown_ref) it borrows the group (`&self`, so
    /// it works behind an `Arc` / a supervisor and the group stays usable
    /// afterwards) and drives the same teardown: send the graceful signal
    /// (`SIGTERM`; a `CTRL_BREAK`/`WM_CLOSE` trigger on the Windows soft tier), wait
    /// up to `grace` for the tree to drain, then `SIGKILL` (/ `cgroup.kill` /
    /// `TerminateJobObject`) survivors when `escalate` is set, or spare them when it
    /// is not. Unlike `shutdown_ref` — which reads
    /// [`shutdown_timeout`](ProcessGroupOptions::shutdown_timeout) /
    /// [`escalate_to_kill`](ProcessGroupOptions::escalate_to_kill) from the group's
    /// options and returns only success-or-error — this takes `grace` / `escalate`
    /// explicitly and returns the observed [`ShutdownReport`]: the attempted soft
    /// signal and whether it landed, the member counts before and after, whether the
    /// tree drained within the grace or was escalated to a hard kill, and the actual
    /// elapsed time.
    ///
    /// A consumer that owns its own end-of-run race (its deadline is a
    /// timeout ⨯ Ctrl-C ⨯ control-socket race, not a
    /// [`Command::timeout`](crate::Command::timeout)) can use the report to stop the
    /// instant the tree is empty — rather than always spending the whole grace — and
    /// to report the tier the kernel *observed* rather than what it *tried*.
    ///
    /// # Kill and wait for drainage
    ///
    /// Call `stop(Duration::ZERO, true)` for a **confirmed** hard kill: it kills the
    /// tree at once (a zero grace elapses immediately) and the returned report tells
    /// you what was still live at the end via [`members_after`](ShutdownReport::members_after)
    /// — the "kill and wait until the members actually vanish" path that bare
    /// [`kill_all`](Self::kill_all) (which returns as soon as the kill is *issued*)
    /// does not offer. (`kill_all` itself is unchanged.)
    ///
    /// # Backward compatibility
    ///
    /// Purely additive: [`shutdown`](Self::shutdown) /
    /// [`shutdown_ref`](Self::shutdown_ref) and their behavior are unchanged, and the
    /// unconditional guarantees hold — dropping the group is still an immediate
    /// `SIGKILL` backstop, a straggler spawned during a non-escalating stop keeps
    /// that backstop (its spare is keyed to a generation the join bumps), and no
    /// extra wait is introduced beyond `grace` (a `Duration::ZERO` grace waits not at
    /// all).
    ///
    /// # Reaping caveat (POSIX process-group mechanism)
    ///
    /// The same caveat as [`shutdown`](Self::shutdown): on the
    /// [`Mechanism::ProcessGroup`](crate::Mechanism) fallback an unreaped **zombie**
    /// still reads as alive, so a child that exits on `SIGTERM` but whose handle was
    /// never awaited reads live for the full `grace` and inflates
    /// [`members_after`](ShutdownReport::members_after). Await each child you start
    /// into the group. The Windows Job Object and Linux cgroup mechanisms are immune.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the teardown fails — the same surface as
    /// [`shutdown`](Self::shutdown): when `escalate` performs the final hard kill,
    /// the undrained-tree failure on the legacy pre-5.14 per-pid fallback and a
    /// process-group member that rejects `SIGKILL` with `EPERM` while still alive. A
    /// best-effort **soft**-signal failure is **not** an error — it is reported as
    /// [`SoftSignal::Failed`](crate::SoftSignal::Failed) in the returned report and
    /// the teardown proceeds.
    #[cfg(feature = "process-control")]
    pub async fn stop(&self, grace: Duration, escalate: bool) -> Result<ShutdownReport> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            mechanism = ?self.mechanism(),
            grace_ms = grace.as_millis() as u64,
            escalate,
            "graceful stop (reporting): TERM, grace, then KILL"
        );
        let outcome = self
            .job
            .graceful_shutdown(crate::sys::SIGTERM_RAW, grace, escalate)
            .await
            .map_err(Error::io)?;
        Ok(ShutdownReport::from_outcome(outcome, Signal::Term))
    }

    /// Gracefully tear the tree down **without consuming** the group — the
    /// run-level graceful-timeout path holds an `Arc`/`Weak`, not an owned group.
    /// Sends `signal`, waits up to `grace`, then `SIGKILL`s survivors. On Windows
    /// the signal/grace are ignored (atomic job kill). Best-effort: the caller
    /// reaps the child and the group's `Drop` backstops any straggler.
    pub(crate) async fn graceful_terminate(&self, grace: Duration, signal: i32) -> Result<()> {
        self.job
            .graceful_shutdown(signal, grace, true)
            .await
            .map_err(Error::io)?;
        Ok(())
    }

    /// Snapshot the group's resource usage (active process count and, where the
    /// platform supports it, total CPU time and peak memory). See
    /// [`ProcessGroupStats`].
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::Io`] if the platform's resource query fails.
    #[cfg(feature = "stats")]
    pub fn stats(&self) -> Result<ProcessGroupStats> {
        let stats = self.job.stats().map_err(Error::io)?;
        Ok(stats)
    }

    /// Sample [`stats`](Self::stats) on an interval as a
    /// [`Stream`](tokio_stream::Stream) of snapshots. The first sample is taken
    /// immediately; the series ends on the first failure. A zero `every` is
    /// clamped to 1 ms.
    #[cfg(feature = "stats")]
    pub fn sample_stats(&self, every: Duration) -> crate::stats::StatsSampler<'_> {
        crate::stats::StatsSampler::new(self, every)
    }

    /// Replace the group's resource limits on the **live** container, without
    /// recreating it or restarting its children — adaptive resource management
    /// (tighten a slumping batch's memory, widen a long-lived worker pool's CPU
    /// quota) after the group is already running.
    ///
    /// # Full replacement
    ///
    /// The new [`ResourceLimits`] **wholly replaces** the active set — it is not
    /// merged with the old one. An axis left `None` becomes **unbounded again**
    /// (its cap is lifted), exactly as if the group had been created with that axis
    /// unset — *not* "keep the previous value". So `update_limits` always describes
    /// the complete desired state of all three caps.
    ///
    /// # Platform support
    ///
    /// The same matrix as creation ([`with_options`](Self::with_options) — see
    /// [`ResourceLimits`]): a real container is required. On **Windows** the live
    /// Job Object's memory / process / CPU caps are reissued; on **Linux cgroup v2**
    /// the `memory.max` / `pids.max` / `cpu.max` files are rewritten (a removed axis
    /// written back to `max`). On the **POSIX process-group** mechanism (macOS, the
    /// BSDs, and the Linux fallback with no usable cgroup) there is no whole-tree
    /// cap primitive, so a request carrying **any** cap is refused with
    /// [`crate::ErrorReason::ResourceLimit`] — never silently dropped — while an all-`None`
    /// request (lift every cap) is a trivial success, since the tree is already
    /// unbounded there.
    ///
    /// This routes through the same live handle / cgroup the tree-control verbs
    /// ([`kill_all`](Self::kill_all), [`signal`](Self::signal),
    /// [`suspend`](Self::suspend)) use — it never re-derives the container — so once
    /// the group is torn down by the consuming [`shutdown`](Self::shutdown) or
    /// `Drop`, the group is gone by ownership and cannot be reconfigured at all.
    ///
    /// On success the group's stored options reflect the new set (observable via the
    /// group's [`Debug`](std::fmt::Debug)).
    ///
    /// # A failure is not a rollback
    ///
    /// A failed call leaves the group's *reflected* options describing the previous
    /// set — but that is this handle's bookkeeping, not a statement about the OS
    /// container. Neither backend applies the set atomically: Windows writes the
    /// memory and process caps in one `SetInformationJobObject` call and the CPU cap
    /// in a second, and the cgroup v2 backend writes `memory.max`, `pids.max` and
    /// `cpu.max` in turn. A call that fails part-way can therefore leave the
    /// container carrying a **mix** of old and new caps — including an axis this
    /// request meant to *lift*, already lifted. Nothing is undone, and the error does
    /// not say how far the write got. Recover by re-issuing the complete desired set
    /// (this is a full replacement, so the retry is idempotent) or by tearing the
    /// group down. Only a value rejected by validation
    /// ([`LimitReason::Invalid`]) is guaranteed to have changed nothing: it fails
    /// before the OS is touched at all.
    ///
    /// What stays exact through this is the post-run evidence.
    /// [`limit_evidence`](Self::limit_evidence) is the authoritative answer to what
    /// actually *fired*, and a partial update cannot fool it: every axis this call
    /// requests joins the group's sticky cap record as soon as the request reaches
    /// the OS — on failure exactly as on success — so an axis that did land before
    /// the failure is still answered from the kernel's own counters instead of being
    /// reported [`NotTripped`](crate::LimitVerdict::NotTripped) with nothing read.
    /// The cost of that conservatism is at most an extra counter read, or an honest
    /// [`Unknown`](crate::LimitVerdict::Unknown) where a cap was in fact never
    /// applied — never an invented verdict.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorReason::ResourceLimit`] — with [`LimitReason::Invalid`] for a nonsensical
    /// value (rejected by the shared `validate_limits` before the OS is touched,
    /// exactly as at creation), [`LimitReason::Unsupported`] when the active
    /// mechanism has no whole-tree accounting at all (a process-group mechanism),
    /// or [`LimitReason::Unenforceable`] when a capable mechanism exists but this
    /// request could not be applied (a Linux cgroup whose controllers can't be
    /// enabled off the real hierarchy root, or a Job Object call the OS rejected).
    #[cfg(feature = "limits")]
    pub fn update_limits(&mut self, limits: ResourceLimits) -> Result<()> {
        // Destructured for disjoint field borrows: the shared core takes the
        // evidence record and the reflected options mutably while the apply closure
        // still borrows the live job handle.
        let Self {
            job,
            options,
            capped,
        } = self;
        update_limits_with(capped, &mut options.limits, limits, |limits| {
            job.update_limits(limits)
        })
    }

    /// Post-run evidence about this group's resource caps: **did a cap that was in
    /// force actually fire?** One [`LimitVerdict`](crate::LimitVerdict) per axis, read from the kernel /
    /// OS container this crate owns.
    ///
    /// This closes the gap a plain exit status leaves. A child killed by its memory
    /// cap and a child that crashed on its own both surface as an ordinary non-zero
    /// exit (or a `SIGKILL`), and [`stats`](Self::stats) reports only peak/cumulative
    /// samples, never a verdict. Call this once the run you care about has finished
    /// and ask the axis you capped:
    /// [`memory`](LimitEvidence::memory) / [`processes`](LimitEvidence::processes) /
    /// [`cpu`](LimitEvidence::cpu), or [`verdict(kind)`](LimitEvidence::verdict).
    ///
    /// # Not the same question as [`crate::ErrorReason::ResourceLimit`]
    ///
    /// That error is **pre-spawn admission**: "the cap you requested could not be
    /// *applied*" ([`LimitReason::Invalid`] / [`LimitReason::Unsupported`] /
    /// [`LimitReason::Unenforceable`]), raised by
    /// [`with_options`](Self::with_options) / [`update_limits`](Self::update_limits)
    /// before or instead of running anything. This report is the other side: the cap
    /// **was** applied — did it then *engage*? The two never overlap, and this
    /// addition changes nothing about that error's behavior.
    ///
    /// # Three-valued, and never a guess
    ///
    /// [`Tripped`](crate::LimitVerdict::Tripped) is returned only on authoritative
    /// kernel/OS evidence recorded by this group's own container.
    /// [`NotTripped`](crate::LimitVerdict::NotTripped) means the evidence says it did not
    /// fire — or that the axis never carried a cap at all, so nothing could.
    /// [`Unknown`](crate::LimitVerdict::Unknown) means **no evidence is available**, and is
    /// deliberately not folded into "no". Exit codes and signals are never
    /// consulted: they cannot distinguish a cap-driven kill from a self-inflicted
    /// one, so reading them would be the guess this report exists to avoid.
    ///
    /// # What each mechanism can prove
    ///
    /// - **Linux cgroup v2** — all three axes, from the kernel's own counters:
    ///   `memory.events`' `oom` (this cgroup hit *its own* memory cap and had to
    ///   OOM), `pids.events`' `max` (a fork was refused by the process cap),
    ///   `cpu.stat`'s `nr_throttled` (the quota throttled the tree). Note the memory
    ///   axis keys on `oom` and **not** `oom_kill`: the latter also counts a *global*
    ///   host OOM kill of our child, which would misattribute a system-wide event to
    ///   your cap.
    /// - **Windows Job Object** — [`Unknown`](crate::LimitVerdict::Unknown) on every capped
    ///   axis. Not an omission: a Job Object keeps no post-mortem record that any of
    ///   these caps fired. Its process cap refuses the offending process *without*
    ///   ever counting it as an accounted member (the job accounting's
    ///   "terminated for a limit violation" tally is measurably unmoved by a real
    ///   violation); its memory cap fails a commit rather than killing, and is
    ///   surfaced only as a live IO-completion-port notification; its CPU hard cap
    ///   throttles with no counter at all. Reading any of those would require
    ///   attaching a completion port and a drain thread to every group — new
    ///   machinery on the containment object itself, for reporting alone — which
    ///   this crate deliberately does not do. Inferring from `PeakJobMemoryUsed` or
    ///   an exit code is refused as a guess. The containers guide records the full
    ///   reasoning.
    /// - **POSIX process group** (macOS, the BSDs, and the Linux fallback with no
    ///   usable cgroup v2) — [`Unknown`](crate::LimitVerdict::Unknown) on every axis: the
    ///   mechanism has no whole-tree resource accounting to read. It also cannot
    ///   carry a cap at all (creation fails fast with
    ///   [`crate::ErrorReason::ResourceLimit`]), so this is "no evidence apparatus
    ///   here", not "a cap may have fired unseen".
    ///
    /// # Lifetime and cost
    ///
    /// The evidence lives in the container, so read it **before the group is
    /// dropped** (or before the consuming [`shutdown`](Self::shutdown)): dropping
    /// removes the cgroup / closes the job handle and takes the counters with it.
    /// Any number of reads is fine — the counters are cumulative and are not reset
    /// by reading, by a teardown, or by [`update_limits`](Self::update_limits); an
    /// axis whose cap was later lifted still reports the fact that it fired while it
    /// was in force. An axis named by an `update_limits` call that *failed* counts as
    /// capped too: that call is not a rollback, so the axis may have been applied
    /// before the failure, and it is read from the counters rather than assumed
    /// innocent.
    ///
    /// This is a pure read — no signal, no kill, no write — so it cannot perturb
    /// teardown, kill-on-drop, or the order the container is removed in, whenever it
    /// is called. It also costs nothing on runs that asked for no caps: an axis that
    /// never carried one is answered without touching the OS at all, so a group
    /// created without [`ResourceLimits`] performs no evidence I/O whatsoever.
    #[cfg(feature = "limits")]
    pub fn limit_evidence(&self) -> LimitEvidence {
        self.job.limit_evidence(self.capped)
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

/// Map a backend `ErrorKind::Unsupported` to the typed [`crate::ErrorReason::Unsupported`],
/// passing every other IO failure through unchanged. Unambiguous here: on the
/// signal/suspend/resume paths the only producer of `Unsupported` is the
/// backends' own "this platform can't do that" reporting.
#[cfg(feature = "process-control")]
fn map_unsupported(source: std::io::Error, operation: impl Into<String>) -> Error {
    if source.kind() == std::io::ErrorKind::Unsupported {
        ErrorReason::Unsupported {
            operation: operation.into(),
        }
        .into()
    } else {
        Error::io(source)
    }
}

/// Reject nonsensical limit values before touching the OS, so a typo surfaces as a
/// clear [`crate::ErrorReason::ResourceLimit`] (`reason: Invalid`) rather than an opaque
/// kernel error.
#[cfg(feature = "limits")]
fn validate_limits(limits: &ResourceLimits) -> Result<()> {
    if limits.max_memory == Some(0) {
        return Err(ErrorReason::ResourceLimit {
            kind: LimitKind::Memory,
            reason: LimitReason::Invalid,
            detail: "max_memory must be greater than 0".into(),
        }
        .into());
    }
    if limits.max_processes == Some(0) {
        return Err(ErrorReason::ResourceLimit {
            kind: LimitKind::Processes,
            reason: LimitReason::Invalid,
            detail: "max_processes must be greater than 0".into(),
        }
        .into());
    }
    if let Some(cores) = limits.cpu_quota
        && !(cores.is_finite() && cores > 0.0)
    {
        return Err(ErrorReason::ResourceLimit {
            kind: LimitKind::Cpu,
            reason: LimitReason::Invalid,
            detail: "cpu_quota must be a finite value greater than 0".into(),
        }
        .into());
    }
    Ok(())
}

/// The shared core of [`ProcessGroup::update_limits`], parametrized over the
/// backend call — the injectable seam that lets tests drive a *failed* application
/// without an OS primitive that can be made to fail on demand, in the same style as
/// the cgroup backend's `limit_evidence_with`.
///
/// The order of the three steps is the contract, not an implementation detail:
///
/// 1. **Validate first.** A value rejected here never reaches the OS, so nothing is
///    recorded for it — mirroring `with_options`, which records only for a group
///    that was actually created, and leaving no phantom axis behind a typo.
/// 2. **Record before applying**, so the record happens whatever the outcome.
///    Neither backend applies the set atomically (Windows writes the memory and
///    process caps in one `SetInformationJobObject` call and the CPU cap in a
///    second; the cgroup backend writes `memory.max`, `pids.max` and `cpu.max` in
///    turn), so a failure part-way through can leave an axis of *this* request
///    already in force. Recording only on success would let `limit_evidence` answer
///    `NotTripped` for such an axis **without reading any counter** — precisely the
///    fabricated "it did not fire" that `LimitVerdict` exists to rule out. Over-
///    recording is the safe direction: at worst it costs one extra counter read
///    (Linux) or turns a would-be `NotTripped` into an honest `Unknown` (Windows),
///    neither of which can invent a verdict.
/// 3. **Reflect only on success.** The whole new set demonstrably did not take
///    effect, so updating the group's `Debug`-visible options would be the opposite
///    lie — claiming caps that may never have been written.
#[cfg(feature = "limits")]
fn update_limits_with(
    capped: &mut CappedAxes,
    reflected: &mut ResourceLimits,
    limits: ResourceLimits,
    apply: impl FnOnce(&ResourceLimits) -> std::io::Result<()>,
) -> Result<()> {
    // Same validation the creation path runs — an invalid value is rejected
    // before the OS is touched, with the specific offending axis.
    validate_limits(&limits)?;
    // Sticky, unlike `reflected`: an axis capped here stays on the evidence record
    // even after a later replacement lifts it, so a cap that fired while it was in
    // force is never reported as "did not fire". Deliberately recorded up front
    // rather than on success — see step 2 above.
    capped.record(&limits);
    apply(&limits).map_err(|source| {
        if limits.any() {
            // Mirror `with_options`'s classification exactly: the backends
            // report `ErrorKind::Unsupported` precisely when no whole-tree
            // container mechanism exists at all; every other failure means a
            // capable mechanism exists but this request could not be applied.
            let reason = if source.kind() == std::io::ErrorKind::Unsupported {
                LimitReason::Unsupported
            } else {
                LimitReason::Unenforceable
            };
            ErrorReason::ResourceLimit {
                kind: first_requested_kind(&limits),
                reason,
                detail: source.to_string(),
            }
            .into()
        } else {
            // No cap requested (a pure "lift everything") that still failed —
            // a plain I/O failure on the reset write, not a limit-capability
            // problem.
            Error::io(source)
        }
    })?;
    // Reflect the applied set so the group's public view (Debug, any future
    // getter) stays honest.
    *reflected = limits;
    Ok(())
}

/// Which limit an enforcement failure (as opposed to a `validate_limits`
/// rejection) should be attributed to, when the backend's error can't be
/// pinned to a single one: the **first** requested limit in `max_memory`,
/// `max_processes`, `cpu_quota` order — see [`LimitKind`]'s doc for why this
/// fixed tie-break is honest rather than arbitrary. `limits.any()` is a
/// precondition (checked by the caller), so at least one arm always matches.
#[cfg(feature = "limits")]
fn first_requested_kind(limits: &ResourceLimits) -> LimitKind {
    if limits.max_memory.is_some() {
        LimitKind::Memory
    } else if limits.max_processes.is_some() {
        LimitKind::Processes
    } else {
        LimitKind::Cpu
    }
}

#[cfg(all(test, feature = "limits"))]
mod tests {
    use super::*;

    #[test]
    fn builders_set_limits() {
        let opts = ProcessGroupOptions::default()
            .max_memory(1024)
            .max_processes(8)
            .cpu_quota(0.5);
        assert_eq!(opts.limits.max_memory, Some(1024));
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
        for (opts, expected_kind) in [
            (
                ProcessGroupOptions::default().max_memory(0),
                LimitKind::Memory,
            ),
            (
                ProcessGroupOptions::default().max_processes(0),
                LimitKind::Processes,
            ),
            (
                ProcessGroupOptions::default().cpu_quota(0.0),
                LimitKind::Cpu,
            ),
            (
                ProcessGroupOptions::default().cpu_quota(-1.0),
                LimitKind::Cpu,
            ),
            (
                ProcessGroupOptions::default().cpu_quota(f64::NAN),
                LimitKind::Cpu,
            ),
            (
                ProcessGroupOptions::default().cpu_quota(f64::INFINITY),
                LimitKind::Cpu,
            ),
        ] {
            // `validate_limits` classifies as `Invalid` with the specific
            // field that failed — never a guess, and never touching the OS.
            let err = validate_limits(&opts.limits).unwrap_err();
            match err.reason() {
                ErrorReason::ResourceLimit { kind, reason, .. } => {
                    assert_eq!(*kind, expected_kind);
                    assert_eq!(*reason, LimitReason::Invalid);
                }
                other => panic!("expected ResourceLimit, got {other:?}"),
            }
            let err = ProcessGroup::with_options(opts).unwrap_err();
            match err.reason() {
                ErrorReason::ResourceLimit { kind, reason, .. } => {
                    assert_eq!(*kind, expected_kind);
                    assert_eq!(*reason, LimitReason::Invalid);
                }
                other => panic!("expected ResourceLimit, got {other:?}"),
            }
        }
    }

    /// Every axis, for the "and nothing else was touched" half of each assertion.
    const ALL_KINDS: [LimitKind; 3] = [LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu];

    fn resource_limit_of(err: &Error) -> (LimitKind, LimitReason) {
        match err.reason() {
            ErrorReason::ResourceLimit { kind, reason, .. } => (*kind, *reason),
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_update_still_records_every_requested_axis() {
        // The regression this pins. Both backends write the axes one at a time
        // (Windows: extended-limit struct, then CPU rate; cgroup v2: memory.max,
        // pids.max, cpu.max), so a failure part-way through can leave an axis of
        // *this* request already in force. Recording only on success used to skip
        // that axis, and `limit_evidence` would then answer `NotTripped` for it
        // without reading a single counter — the fabricated "it did not fire" that
        // `LimitVerdict` exists to rule out, on an axis whose cap may really have
        // killed the tree.
        //
        // A genuine mid-sequence OS failure can't be provoked from a unit test (no
        // fault-injection seam exists in the backends), so the backend call is
        // stubbed at `update_limits_with`'s seam — which is exactly where the
        // ordering under test lives.
        let mut capped = CappedAxes::default();
        let mut reflected = ResourceLimits::default();
        let requested = ResourceLimits {
            max_memory: Some(64 * 1024 * 1024),
            max_processes: Some(4),
            cpu_quota: None,
        };

        let err = update_limits_with(&mut capped, &mut reflected, requested, |_| {
            // The shape of a real Windows failure here: the extended-limit write
            // (memory + processes) landed, the separate CPU-rate write did not.
            Err(std::io::Error::other("cpu-rate reissue: boom"))
        })
        .unwrap_err();

        // Both requested axes may have landed before the failure, so both must be on
        // the record even though the call returned `Err`.
        assert!(capped.has(LimitKind::Memory));
        assert!(capped.has(LimitKind::Processes));
        // Honest in the other direction too: an axis the caller never named stays
        // off the record, so evidence still costs nothing on axes that had no cap.
        assert!(!capped.has(LimitKind::Cpu));
        // Unchanged behaviour, deliberately: the whole new set demonstrably did not
        // take effect, so the group's Debug-visible options must not claim it did.
        assert_eq!(reflected, ResourceLimits::default());
        assert_eq!(
            resource_limit_of(&err),
            (LimitKind::Memory, LimitReason::Unenforceable)
        );
    }

    #[test]
    fn a_failed_update_records_even_where_no_container_exists() {
        // A process-group mechanism refuses any cap outright, so nothing was applied
        // — yet the record is still written, because "how far did the backend get?"
        // is precisely what the caller cannot know. Over-recording is the safe
        // direction: this mechanism reports `Unknown` for every axis anyway, and a
        // capable one reads a counter rather than guessing.
        let mut capped = CappedAxes::default();
        let mut reflected = ResourceLimits::default();
        let requested = ResourceLimits {
            max_memory: None,
            max_processes: None,
            cpu_quota: Some(0.5),
        };

        let err = update_limits_with(&mut capped, &mut reflected, requested, |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "no whole-tree limit mechanism",
            ))
        })
        .unwrap_err();

        assert!(capped.has(LimitKind::Cpu));
        assert!(!capped.has(LimitKind::Memory));
        assert!(!capped.has(LimitKind::Processes));
        // Classified exactly as `with_options` classifies the same backend signal.
        assert_eq!(
            resource_limit_of(&err),
            (LimitKind::Cpu, LimitReason::Unsupported)
        );
    }

    #[test]
    fn an_invalid_request_records_nothing_and_never_reaches_the_backend() {
        // The one failure that *is* a guarantee of "nothing changed": validation runs
        // before the OS is touched. Recording there would leave a phantom axis behind
        // a typo, exactly what `with_options` avoids by recording only for a group
        // that was really created.
        let mut capped = CappedAxes::default();
        let mut reflected = ResourceLimits::default();
        let mut reached_backend = false;
        let err = update_limits_with(
            &mut capped,
            &mut reflected,
            ResourceLimits {
                max_memory: Some(0),
                max_processes: Some(4),
                cpu_quota: None,
            },
            |_| {
                reached_backend = true;
                Ok(())
            },
        )
        .unwrap_err();

        assert!(!reached_backend, "an invalid value must not reach the OS");
        for k in ALL_KINDS {
            assert!(!capped.has(k), "axis {k:?} must stay unrecorded");
        }
        assert_eq!(reflected, ResourceLimits::default());
        assert_eq!(
            resource_limit_of(&err),
            (LimitKind::Memory, LimitReason::Invalid)
        );
    }

    #[test]
    fn a_failed_lift_everything_is_a_plain_io_error_with_nothing_to_record() {
        // No cap requested at all: there is no axis to record, and the failure is an
        // I/O problem on the reset write rather than a limit-capability verdict. The
        // previously-reflected set stays — those caps may well still be in force.
        let mut capped = CappedAxes::default();
        let previous = ResourceLimits {
            max_memory: Some(1024),
            ..ResourceLimits::default()
        };
        capped.record(&previous);
        let mut reflected = previous;

        let err = update_limits_with(
            &mut capped,
            &mut reflected,
            ResourceLimits::default(),
            |_| Err(std::io::Error::other("reset write failed")),
        )
        .unwrap_err();

        assert!(matches!(err.reason(), ErrorReason::Io(_)), "{err:?}");
        // Sticky: the earlier cap stays on the record, and no new axis appears.
        assert!(capped.has(LimitKind::Memory));
        assert!(!capped.has(LimitKind::Processes));
        assert!(!capped.has(LimitKind::Cpu));
        assert_eq!(reflected, previous);
    }

    #[test]
    fn a_successful_update_records_the_new_axes_and_reflects_the_whole_set() {
        // The success path is unchanged, and stickiness still holds through the seam:
        // a replacement that LIFTS memory must not erase it from the record.
        let mut capped = CappedAxes::default();
        let previous = ResourceLimits {
            max_memory: Some(64 * 1024 * 1024),
            ..ResourceLimits::default()
        };
        capped.record(&previous); // what `with_options` does at creation
        let mut reflected = previous;
        let requested = ResourceLimits {
            max_memory: None,
            max_processes: Some(4),
            cpu_quota: Some(0.5),
        };

        update_limits_with(&mut capped, &mut reflected, requested, |limits| {
            // The backend is handed the caller's whole set, verbatim — a full
            // replacement, never a merge with what was reflected before.
            assert_eq!(*limits, requested);
            Ok(())
        })
        .unwrap();

        assert_eq!(reflected, requested);
        for k in ALL_KINDS {
            assert!(capped.has(k), "axis {k:?}");
        }
    }

    #[test]
    fn first_requested_kind_follows_the_documented_tie_break_order() {
        // max_memory wins over the others when several are set...
        let mut limits = ResourceLimits {
            max_memory: Some(1),
            max_processes: Some(1),
            cpu_quota: Some(1.0),
        };
        assert_eq!(first_requested_kind(&limits), LimitKind::Memory);

        // ...then max_processes, when max_memory is unset...
        limits.max_memory = None;
        assert_eq!(first_requested_kind(&limits), LimitKind::Processes);

        // ...and cpu_quota is the last resort.
        limits.max_processes = None;
        assert_eq!(first_requested_kind(&limits), LimitKind::Cpu);
    }
}
