//! Diagnostic counters for a [`ProcessGroup`](crate::ProcessGroup), plus the
//! time-series sampler ([`StatsSampler`]) and the per-run profile summary
//! ([`RunProfile`]).

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::group::ProcessGroup;
use crate::result::Outcome;

/// A snapshot of a process group's resource usage.
///
/// `total_cpu_time` and `peak_memory_bytes` are `None` when the platform can't
/// report them — notably the POSIX process-group mechanism (no cgroup
/// accounting), i.e. macOS/BSD and the Linux fallback.
///
/// Non-exhaustive: a read-only snapshot the crate produces — new metrics can
/// be added without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessGroupStats {
    /// Number of live processes currently in the group.
    ///
    /// Under the POSIX process-group mechanism ([`Mechanism::ProcessGroup`]
    /// — macOS/BSD and the Linux fallback) this counts live process *groups*
    /// rather than individual processes: a contained child that itself forks
    /// helpers still counts once. With a cgroup or Job Object it is the exact
    /// process count.
    ///
    /// [`Mechanism::ProcessGroup`]: crate::Mechanism::ProcessGroup
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
    /// - **POSIX process-group / macOS** — always `None`; no kernel accumulator
    ///   is available without a cgroup or Job Object.
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
    /// - **POSIX process-group / macOS** — always `None`; no kernel accumulator.
    pub peak_memory_bytes: Option<u64>,
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
/// (and its kill-on-drop guarantee) alive.
pub struct StatsSampler<'a> {
    group: &'a ProcessGroup,
    interval: tokio::time::Interval,
    /// Latched once a snapshot fails: the series has ended for good, and
    /// further polls keep returning `None` (a well-behaved, fused stream)
    /// instead of resuming if the group recovers.
    done: bool,
}

impl<'a> StatsSampler<'a> {
    pub(crate) fn new(group: &'a ProcessGroup, every: Duration) -> Self {
        // tokio panics on a zero period; clamp rather than make the constructor fallible.
        let every = every.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(every);
        // Each tick wants the *current* state; replaying missed ticks would
        // fabricate identical samples.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        StatsSampler {
            group,
            interval,
            done: false,
        }
    }
}

impl std::fmt::Debug for StatsSampler<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsSampler")
            .field("period", &self.interval.period())
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl tokio_stream::Stream for StatsSampler<'_> {
    type Item = ProcessGroupStats;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        std::task::ready!(this.interval.poll_tick(cx));
        match this.group.stats() {
            Ok(snapshot) => Poll::Ready(Some(snapshot)),
            Err(_) => {
                this.done = true;
                Poll::Ready(None)
            }
        }
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
    /// How the run ended — the full [`Outcome`](crate::Outcome), so a profile can
    /// distinguish a clean exit from a signal kill from a timeout (all three of
    /// which leave [`exit_code`](Self::exit_code) `None`). Read it directly, or
    /// via the [`signal`](Self::signal) / [`timed_out`](Self::timed_out)
    /// convenience accessors. The profile is therefore a superset of
    /// [`RunningProcess::wait`](crate::RunningProcess::wait): one call yields both
    /// the resource telemetry and the run's actual outcome.
    pub outcome: Outcome,
    /// The exit code if the run [exited](crate::Outcome::Exited); `None` for a run
    /// killed by its timeout or a signal. Equals
    /// [`outcome.code()`](crate::Outcome::code) — a convenience for the common
    /// case; consult [`outcome`](Self::outcome) to tell a timeout from a signal.
    pub exit_code: Option<i32>,
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

    /// Renamed to [`avg_cpu_cores`](Self::avg_cpu_cores): the value is in CPU
    /// **cores**, and the suffix makes the unit self-documenting. A thin
    /// forwarding shim kept for one minor; **removed in 2.0**.
    #[deprecated(
        since = "1.1.0",
        note = "renamed to `avg_cpu_cores` (the unit is cores); removed in 2.0"
    )]
    pub fn avg_cpu(&self) -> Option<f64> {
        self.avg_cpu_cores()
    }

    /// The exit code if the run [exited](crate::Outcome::Exited), else `None`
    /// (a signal kill or a timeout). Equals the [`exit_code`](Self::exit_code)
    /// field and [`outcome.code()`](crate::Outcome::code); the method form
    /// completes the `code()` / [`signal()`](Self::signal) /
    /// [`timed_out()`](Self::timed_out) accessor trio that mirrors
    /// [`ProcessResult`](crate::ProcessResult) and [`Outcome`](crate::Outcome).
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
    /// deadline kill from a signal kill, which [`exit_code`](Self::exit_code)
    /// alone (both `None`) cannot.
    pub fn timed_out(&self) -> bool {
        self.outcome.timed_out()
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, RunProfile};
    use std::time::Duration;

    #[tokio::test]
    async fn zero_interval_sampler_does_not_panic() {
        // tokio's interval panics on a zero period; the constructor must clamp.
        let group = crate::ProcessGroup::new().expect("create group");
        let _sampler = group.sample_stats(Duration::ZERO);
    }

    #[test]
    fn avg_cpu_cores_is_cpu_time_over_duration() {
        let profile = RunProfile {
            outcome: Outcome::Exited(0),
            exit_code: Some(0),
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
            exit_code: Some(0),
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert_eq!(no_cpu.avg_cpu_cores(), None);

        let no_duration = RunProfile {
            outcome: Outcome::Exited(0),
            exit_code: Some(0),
            duration: Duration::ZERO,
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 1,
        };
        assert_eq!(no_duration.avg_cpu_cores(), None);
    }

    #[test]
    #[allow(deprecated)]
    fn avg_cpu_forwards_to_avg_cpu_cores() {
        // The deprecated alias must keep returning exactly what its replacement does
        // until it is removed in 2.0.
        let profile = RunProfile {
            outcome: Outcome::Exited(0),
            exit_code: Some(0),
            duration: Duration::from_secs(2),
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 4,
        };
        assert_eq!(profile.avg_cpu(), profile.avg_cpu_cores());
        assert_eq!(profile.avg_cpu(), Some(0.5));
    }

    #[test]
    fn outcome_distinguishes_timeout_from_signal_when_exit_code_is_none() {
        // The whole point of carrying `outcome`: a timeout and a signal kill both
        // leave `exit_code == None`, yet the profile must tell them apart.
        let timed_out = RunProfile {
            outcome: Outcome::TimedOut,
            exit_code: None,
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert!(timed_out.timed_out());
        assert_eq!(timed_out.signal(), None);

        let signalled = RunProfile {
            outcome: Outcome::Signalled(Some(9)),
            exit_code: None,
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert!(!signalled.timed_out());
        assert_eq!(signalled.signal(), Some(9));
        // Both leave `exit_code` empty — only `outcome` separates them.
        assert_eq!(timed_out.exit_code, signalled.exit_code);
    }
}
