//! Diagnostic counters for a [`ProcessGroup`], plus the
//! time-series samplers ([`StatsSampler`] and its owning `'static` twin
//! [`OwnedStatsSampler`]) and the per-run profile summary ([`RunProfile`]).

use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(feature = "report-serde")]
use serde::ser::{Serialize, SerializeStruct as _, Serializer};

use crate::group::ProcessGroup;
use crate::result::Outcome;

/// A snapshot of a process group's resource usage.
///
/// `total_cpu_time` and `peak_memory_bytes` are `None` when the platform can't
/// report them — notably the POSIX process-group mechanism (no cgroup
/// accounting), i.e. macOS/BSD and the Linux fallback, and the FreeBSD process
/// reaper, which contains a tree without accounting for it.
///
/// Non-exhaustive: a read-only snapshot the crate produces — new metrics can
/// be added without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessGroupStats {
    /// Number of live processes currently in the group.
    ///
    /// Under the POSIX process-group mechanism ([`Mechanism::ProcessGroup`]
    /// — macOS/the non-FreeBSD BSDs and the Linux fallback) this counts live
    /// process *groups* rather than individual processes: a contained child that
    /// itself forks helpers still counts once. With a cgroup, a Job Object or the
    /// FreeBSD process reaper ([`Mechanism::ProcessReaper`]) it is the exact
    /// process count.
    ///
    /// [`Mechanism::ProcessGroup`]: crate::Mechanism::ProcessGroup
    /// [`Mechanism::ProcessReaper`]: crate::Mechanism::ProcessReaper
    pub active_process_count: usize,
    /// Total CPU time (user + kernel) accumulated by the group, if available.
    ///
    /// **Semantic divergence by backend:**
    /// - **Windows Job Object** — cumulative across all processes that have ever
    ///   been part of the job, including already-terminated ones. Reflects the
    ///   full historical cost of the tree.
    /// - **Linux cgroup v2** — sum of `/proc/<pid>/stat` times for *currently
    ///   live* members only; terminated processes are not accounted once they
    ///   leave the cgroup.
    /// - **POSIX process-group / macOS, and the FreeBSD process reaper** — always
    ///   `None`; no kernel accumulator is available without a cgroup or Job Object,
    ///   and a reaper contains a tree without accounting for it.
    pub total_cpu_time: Option<Duration>,
    /// Peak memory used by the group in bytes, if available. This is the OS's
    /// own group-wide measure; its exact meaning differs by platform and it is
    /// **not directly comparable across platforms**, nor equal to the sum of the
    /// per-process [`RunningProcess::peak_memory_bytes`](crate::RunningProcess::peak_memory_bytes)
    /// (which is a resident-set peak):
    /// - **Windows** — the Job Object's `PeakJobMemoryUsed`: peak *committed*
    ///   memory (commit charge) charged to the job, not a working-set figure.
    /// - **Linux cgroup v2** — the sum of currently-live members' peak resident
    ///   sets (`VmHWM`); members that already exited are not counted.
    /// - **POSIX process-group / macOS, and the FreeBSD process reaper** — always
    ///   `None`; no kernel accumulator.
    pub peak_memory_bytes: Option<u64>,
}

/// *(feature `report-serde`)* The snapshot, field for field — a sampler tick as
/// one report line:
///
/// ```json
/// {"active_process_count": 3, "total_cpu_time_secs": 1.5, "peak_memory_bytes": 65536}
/// ```
///
/// Both measurements stay `null` on a mechanism that keeps no whole-tree
/// accounting (the POSIX process group and the FreeBSD reaper), never a
/// plausible-looking `0` — the `Option`'s honesty carried onto the wire. The
/// per-backend meaning of each number is unchanged and still documented on the
/// fields themselves; a consumer comparing series across platforms must read
/// those caveats, the wire form cannot make them comparable.
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for ProcessGroupStats {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Destructured rather than read field by field: a metric added to this
        // snapshot is a compile error here, so it can never silently miss the
        // wire — the mechanical counterpart of the exhaustive `match` the enum
        // impls in this feature use.
        let Self {
            active_process_count,
            total_cpu_time,
            peak_memory_bytes,
        } = self;
        let mut state = serializer.serialize_struct("ProcessGroupStats", 3)?;
        state.serialize_field("active_process_count", active_process_count)?;
        state.serialize_field(
            "total_cpu_time_secs",
            &crate::report_serde::secs_opt(*total_cpu_time),
        )?;
        state.serialize_field("peak_memory_bytes", peak_memory_bytes)?;
        state.end()
    }
}

/// The shared cadence-and-fuse engine behind both stats samplers.
///
/// The polling contract — clamp a zero period, take the first sample
/// immediately, skip missed ticks rather than burst to catch up, and latch the
/// series *done* on the first tick that can't produce a snapshot — lives here
/// exactly once. Both the borrowing [`StatsSampler`] and the owning
/// [`OwnedStatsSampler`] drive their [`Stream`](tokio_stream::Stream) through
/// it, so the two never fork the sampling semantics.
struct SamplerCore {
    interval: tokio::time::Interval,
    /// Latched once a snapshot can't be produced: the series has ended for
    /// good, and further polls keep returning `None` (a well-behaved, fused
    /// stream) instead of resuming if the group recovers.
    done: bool,
}

impl SamplerCore {
    fn new(every: Duration) -> Self {
        // tokio panics on a zero period; clamp rather than make the constructor fallible.
        let every = every.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(every);
        // Each tick wants the *current* state; replaying missed ticks would
        // fabricate identical samples.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        SamplerCore {
            interval,
            done: false,
        }
    }

    /// The configured sampling period (after the zero-clamp) — for `Debug`.
    fn period(&self) -> Duration {
        self.interval.period()
    }

    /// Poll the next tick, then give `take_snapshot` the chance to produce it.
    ///
    /// `take_snapshot` returns `None` for **either** end-of-series cause — the
    /// group's container can no longer report (a failed `stats()`), or the
    /// owning sampler's group has been dropped entirely (its last `Arc`
    /// released, so the `Weak` no longer upgrades). Both latch `done` and fuse
    /// the stream to `None`: the series never silently repeats its last sample
    /// and never resumes.
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        take_snapshot: impl FnOnce() -> Option<ProcessGroupStats>,
    ) -> Poll<Option<ProcessGroupStats>> {
        if self.done {
            return Poll::Ready(None);
        }
        std::task::ready!(self.interval.poll_tick(cx));
        match take_snapshot() {
            Some(snapshot) => Poll::Ready(Some(snapshot)),
            None => {
                self.done = true;
                Poll::Ready(None)
            }
        }
    }
}

/// A periodic [`ProcessGroupStats`] series — created by
/// [`ProcessGroup::sample_stats`].
///
/// Implements [`Stream`](tokio_stream::Stream): each tick yields a fresh
/// snapshot. The first sample is taken immediately, then one per interval (a
/// delayed poll skips missed ticks rather than bursting to catch up). The
/// series ends — the stream yields `None` — on the first snapshot the group
/// fails to report, e.g. after its container is torn down.
///
/// The sampler *borrows* the group, so it can neither outlive it nor keep it
/// (and its kill-on-drop guarantee) alive. When the group is held behind a
/// shared [`Arc`] and you need a sampler that isn't tied to that borrow — one
/// that is `Send + 'static` and can move between tasks or across an FFI
/// boundary — use the owning twin [`OwnedStatsSampler`].
pub struct StatsSampler<'a> {
    group: &'a ProcessGroup,
    core: SamplerCore,
}

impl<'a> StatsSampler<'a> {
    pub(crate) fn new(group: &'a ProcessGroup, every: Duration) -> Self {
        StatsSampler {
            group,
            core: SamplerCore::new(every),
        }
    }
}

impl std::fmt::Debug for StatsSampler<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsSampler")
            .field("period", &self.core.period())
            .field("done", &self.core.done)
            .finish_non_exhaustive()
    }
}

impl tokio_stream::Stream for StatsSampler<'_> {
    type Item = ProcessGroupStats;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let group = this.group;
        // A failed `stats()` (torn-down container) ends the borrowed series,
        // exactly as before — `.ok()` collapses the error into the shared
        // `None`-means-done contract.
        this.core.poll_next(cx, || group.stats().ok())
    }
}

/// A periodic [`ProcessGroupStats`] series that does **not** borrow the group by
/// lifetime — the owning, `'static` twin of [`StatsSampler`], for a group held
/// behind a shared [`Arc`].
///
/// Built from an `&Arc<ProcessGroup>` (via [`new`](Self::new)), it is
/// `Send + 'static`, so — unlike [`StatsSampler`], which is pinned to the
/// group's lifetime — it can be moved into a [`tokio::spawn`]ed task or across
/// an FFI boundary and sampled there. It shares the exact
/// [`Stream`](tokio_stream::Stream) contract of [`StatsSampler`]: first sample
/// immediate, then one per interval, missed ticks skipped rather than burst
/// (the cadence is the same `SamplerCore`, not a second implementation).
///
/// # It holds the group *weakly*
///
/// The sampler keeps only a [`Weak`] handle, so — like the borrowing
/// [`StatsSampler`] — it neither keeps the group nor its kill-on-drop guarantee
/// alive: a lingering sampler (e.g. one left running in a detached task) can
/// never pin a process tree that should have been torn down. That property is
/// what makes the end-of-series contract below possible.
///
/// # End of series
///
/// The stream yields `None` — for good, it is fused — on the **first** tick
/// that can't produce a snapshot, for either reason:
///
/// - the group is still alive but its container was torn down, so
///   [`stats()`](ProcessGroup::stats) fails (identical to [`StatsSampler`]); or
/// - the group has been **released entirely** — every strong [`Arc`] dropped —
///   while the sampler was running, so the [`Weak`] no longer upgrades.
///
/// In both cases the series ends **honestly**: it never silently repeats the
/// last snapshot, never fabricates one, and never leaves the caller awaiting a
/// tick that will never come.
pub struct OwnedStatsSampler {
    group: Weak<ProcessGroup>,
    core: SamplerCore,
}

impl OwnedStatsSampler {
    /// Start an owning stats series over a group held behind a shared [`Arc`].
    ///
    /// Takes the group by shared reference and downgrades it to a [`Weak`]
    /// handle: the caller keeps their `Arc`, and this sampler does **not**
    /// extend the group's life (see the type's [end-of-series](Self#end-of-series)
    /// contract). A zero `every` is clamped to 1 ms, matching
    /// [`ProcessGroup::sample_stats`].
    ///
    /// The `'static`, `Send` counterpart of [`ProcessGroup::sample_stats`]:
    /// reach for it when the group lives under an `Arc` and the sampler must
    /// outlive the borrow (move into a spawned task, cross an FFI boundary);
    /// reach for `sample_stats` when a plain borrow suffices.
    pub fn new(group: &Arc<ProcessGroup>, every: Duration) -> Self {
        OwnedStatsSampler {
            group: Arc::downgrade(group),
            core: SamplerCore::new(every),
        }
    }
}

impl std::fmt::Debug for OwnedStatsSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedStatsSampler")
            .field("period", &self.core.period())
            .field("done", &self.core.done)
            // Whether the group is still reachable — a released group reads
            // `false`, which is exactly when the next tick ends the series.
            .field("group_alive", &(self.group.strong_count() > 0))
            .finish_non_exhaustive()
    }
}

impl tokio_stream::Stream for OwnedStatsSampler {
    type Item = ProcessGroupStats;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let OwnedStatsSampler { group, core } = this;
        // Upgrade the weak handle per tick: `None` if the group was released
        // entirely, else a failed `stats()` (torn-down container) also collapses
        // to `None` — both end the series through the shared `SamplerCore`.
        core.poll_next(cx, || group.upgrade().and_then(|g| g.stats().ok()))
    }
}

/// Resource summary of one finished run — produced by
/// [`RunningProcess::profile`](crate::RunningProcess::profile).
///
/// CPU and memory are sampled from the started child *process* (the same
/// source as [`RunningProcess::cpu_time`](crate::RunningProcess::cpu_time) /
/// [`peak_memory_bytes`](crate::RunningProcess::peak_memory_bytes)), so they
/// are `None` where per-process metrics are unavailable (macOS/BSD) or when
/// the run exited before the first sample landed.
///
/// Non-exhaustive: a read-only summary the crate produces — new metrics can
/// be added without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunProfile {
    /// How the run ended — the full [`Outcome`], so a profile can
    /// distinguish a clean exit from a signal kill from a timeout (all three of
    /// which leave [`code`](Self::code) `None`). Read it directly, or
    /// via the [`code`](Self::code) / [`signal`](Self::signal) /
    /// [`timed_out`](Self::timed_out) convenience accessors. The profile is
    /// therefore a superset of
    /// [`RunningProcess::wait`](crate::RunningProcess::wait): one call yields both
    /// the resource telemetry and the run's actual outcome.
    pub outcome: Outcome,
    /// Wall-clock time from process start until the run finished (exit reaped
    /// and output drained).
    pub duration: Duration,
    /// Cumulative CPU time (user + kernel) at the last successful sample.
    pub cpu_time: Option<Duration>,
    /// Peak resident memory observed across the samples, in bytes.
    pub peak_memory_bytes: Option<u64>,
    /// How many sampling ticks ran (including ones that found no data).
    pub samples: usize,
}

impl RunProfile {
    /// Average CPU utilisation over the run, in **cores** (`0.5` = half a core
    /// busy on average; can exceed `1.0` for multi-threaded children).
    /// `None` when CPU time was never observed or the run had no duration.
    pub fn avg_cpu_cores(&self) -> Option<f64> {
        let cpu = self.cpu_time?;
        if self.duration.is_zero() {
            return None;
        }
        Some(cpu.as_secs_f64() / self.duration.as_secs_f64())
    }

    /// The exit code if the run [exited](crate::Outcome::Exited), else `None`
    /// (a signal kill or a timeout). Equals
    /// [`outcome.code()`](crate::Outcome::code); the method form completes the
    /// `code()` / [`signal()`](Self::signal) / [`timed_out()`](Self::timed_out)
    /// accessor trio that mirrors [`ProcessResult`](crate::ProcessResult) and
    /// [`Outcome`].
    pub fn code(&self) -> Option<i32> {
        self.outcome.code()
    }

    /// The signal that killed the run, if it was
    /// [signalled](crate::Outcome::Signalled) with a known number (`None` on a
    /// clean exit, a timeout, or a signal kill the platform didn't number).
    /// Shorthand for [`outcome.signal()`](crate::Outcome::signal).
    pub fn signal(&self) -> Option<i32> {
        self.outcome.signal()
    }

    /// Whether the run was killed by its
    /// [timeout](crate::Outcome::TimedOut). Shorthand for
    /// [`outcome.timed_out()`](crate::Outcome::timed_out) — distinguishes a
    /// deadline kill from a signal kill, which [`code`](Self::code) alone
    /// (both `None`) cannot.
    pub fn timed_out(&self) -> bool {
        self.outcome.timed_out()
    }

    /// Whether the run was killed specifically by its output-inactivity
    /// watchdog. Shorthand for
    /// [`outcome.inactivity_timed_out()`](crate::Outcome::inactivity_timed_out).
    pub fn inactivity_timed_out(&self) -> bool {
        self.outcome.inactivity_timed_out()
    }

    /// Build a `RunProfile` from its fields — a `#[doc(hidden)]` insulated
    /// constructor for a wrapper/serialization layer to reconstruct a value
    /// directly, by the same "one insulated constructor instead of a struct
    /// literal" rationale as [`Error::exit`](crate::Error::exit) —
    /// `RunProfile`'s own `#[non_exhaustive]` already rejects a struct literal
    /// from outside this crate even though every field is `pub` (see the
    /// type's own doc for why). Off the documented surface, but `pub` so
    /// downstream code can call it; semver-covered like any public item.
    ///
    /// Mirrors every field, so a value round-trips through this constructor and
    /// reading the fields back (or the [`code`](Self::code) /
    /// [`signal`](Self::signal) / [`timed_out`](Self::timed_out) /
    /// [`avg_cpu_cores`](Self::avg_cpu_cores) accessors) byte-for-byte. No
    /// combination of these fields can be internally contradictory: `outcome`
    /// is this crate's own [`Outcome`], already mutually exclusive by
    /// construction (an exit code and a signal can never both be present), and
    /// every other field is independent telemetry with no cross-field
    /// invariant to violate.
    #[doc(hidden)]
    pub fn from_parts(
        outcome: Outcome,
        duration: Duration,
        cpu_time: Option<Duration>,
        peak_memory_bytes: Option<u64>,
        samples: usize,
    ) -> Self {
        RunProfile {
            outcome,
            duration,
            cpu_time,
            peak_memory_bytes,
            samples,
        }
    }
}

/// *(feature `report-serde`)* The run summary, field for field:
///
/// ```json
/// {
///   "outcome": {"kind": "exited", "code": 0, "signal_number": null},
///   "duration_secs": 2.0,
///   "cpu_time_secs": 1.0,
///   "peak_memory_bytes": 4096,
///   "samples": 8
/// }
/// ```
///
/// `cpu_time_secs` / `peak_memory_bytes` are `null` wherever the platform could
/// not measure them or the run ended before the first sample landed — the same
/// honest gap the `Option` fields carry. [`avg_cpu_cores`](Self::avg_cpu_cores)
/// is deliberately **not** a key: it is arithmetic over two fields already
/// here, and this schema reports facts rather than restating derivations (the
/// one exception, `ProcessResult`'s `success`, exists because accepted-exit
/// policy is the *crate's*, not the consumer's).
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for RunProfile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Destructured rather than read field by field: a fact added to this
        // summary is a compile error here, so it can never silently miss the
        // wire — the mechanical counterpart of the exhaustive `match` the enum
        // impls in this feature use. (`avg_cpu_cores` is not a field and stays
        // off the wire deliberately — see the doc above.)
        let Self {
            outcome,
            duration,
            cpu_time,
            peak_memory_bytes,
            samples,
        } = self;
        let mut state = serializer.serialize_struct("RunProfile", 5)?;
        state.serialize_field("outcome", outcome)?;
        state.serialize_field("duration_secs", &crate::report_serde::secs(*duration))?;
        state.serialize_field("cpu_time_secs", &crate::report_serde::secs_opt(*cpu_time))?;
        state.serialize_field("peak_memory_bytes", peak_memory_bytes)?;
        state.serialize_field("samples", samples)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, OwnedStatsSampler, RunProfile};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn zero_interval_sampler_does_not_panic() {
        // tokio's interval panics on a zero period; the constructor must clamp.
        let group = crate::ProcessGroup::new().expect("create group");
        let _sampler = group.sample_stats(Duration::ZERO);
    }

    /// T-180: the owning sampler exists precisely to move between tasks / across
    /// an FFI boundary, so it must be `Send + 'static`. A compile-time pin — if
    /// the type ever stops being `Send + 'static` (e.g. someone swaps the `Weak`
    /// for a borrow), this stops compiling.
    #[test]
    fn owned_sampler_is_send_and_static() {
        fn assert_send_static<T: Send + 'static>() {}
        assert_send_static::<OwnedStatsSampler>();
    }

    #[tokio::test]
    async fn owned_sampler_zero_interval_does_not_panic() {
        // Same zero-period clamp as the borrowing sampler — the shared
        // `SamplerCore` owns it, so the owning constructor must not panic either.
        let group = Arc::new(crate::ProcessGroup::new().expect("create group"));
        let _sampler = OwnedStatsSampler::new(&group, Duration::ZERO);
    }

    /// T-180: releasing the group entirely while the owning sampler runs must
    /// end the series **honestly** — `None`, fused — not hang the caller or
    /// repeat a stale snapshot. Here the only strong handle is dropped before
    /// the first tick, so the weak upgrade fails and the series ends at once.
    #[tokio::test]
    async fn owned_sampler_ends_when_group_released() {
        use tokio_stream::StreamExt;

        let group = Arc::new(crate::ProcessGroup::new().expect("create group"));
        let mut sampler = OwnedStatsSampler::new(&group, Duration::from_millis(1));
        // Drop the last strong `Arc`: the group is torn down and the sampler's
        // `Weak` can no longer upgrade.
        drop(group);
        assert!(
            sampler.next().await.is_none(),
            "a released group must end the owning sampler's series"
        );
        // Fused: it stays ended, never resuming.
        assert!(
            sampler.next().await.is_none(),
            "the series must stay ended (fused), not resume"
        );
    }

    #[test]
    fn avg_cpu_cores_is_cpu_time_over_duration() {
        let profile = RunProfile {
            outcome: Outcome::Exited(0),
            duration: Duration::from_secs(2),
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 8,
        };
        assert_eq!(profile.avg_cpu_cores(), Some(0.5));
    }

    #[test]
    fn avg_cpu_cores_is_none_without_cpu_or_duration() {
        let no_cpu = RunProfile {
            outcome: Outcome::Exited(0),
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert_eq!(no_cpu.avg_cpu_cores(), None);

        let no_duration = RunProfile {
            outcome: Outcome::Exited(0),
            duration: Duration::ZERO,
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 1,
        };
        assert_eq!(no_duration.avg_cpu_cores(), None);
    }

    #[test]
    fn outcome_distinguishes_timeout_from_signal_when_code_is_none() {
        // The whole point of carrying `outcome`: a timeout and a signal kill both
        // leave `code() == None`, yet the profile must tell them apart.
        let timed_out = RunProfile {
            outcome: Outcome::TimedOut,
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert!(timed_out.timed_out());
        assert_eq!(timed_out.signal(), None);

        let signalled = RunProfile {
            outcome: Outcome::Signalled(Some(9)),
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert!(!signalled.timed_out());
        assert_eq!(signalled.signal(), Some(9));
        // Both leave `code()` empty — only `outcome` separates them.
        assert_eq!(timed_out.code(), signalled.code());
    }

    /// T-179: a `RunProfile` built by the `#[doc(hidden)]` `from_parts`
    /// constructor and read back through its (public) fields/accessors
    /// reproduces the original, field for field.
    #[test]
    fn run_profile_from_parts_round_trips_every_field() {
        let original = RunProfile::from_parts(
            Outcome::Exited(0),
            Duration::from_secs(2),
            Some(Duration::from_secs(1)),
            Some(4096),
            8,
        );
        assert_eq!(original.outcome, Outcome::Exited(0));
        assert_eq!(original.duration, Duration::from_secs(2));
        assert_eq!(original.cpu_time, Some(Duration::from_secs(1)));
        assert_eq!(original.peak_memory_bytes, Some(4096));
        assert_eq!(original.samples, 8);
        assert_eq!(original.avg_cpu_cores(), Some(0.5));

        let rebuilt = RunProfile::from_parts(
            original.outcome,
            original.duration,
            original.cpu_time,
            original.peak_memory_bytes,
            original.samples,
        );
        assert_eq!(original, rebuilt);
    }
}
