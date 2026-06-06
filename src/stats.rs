//! Diagnostic counters for a [`ProcessGroup`](crate::ProcessGroup), plus the
//! time-series sampler ([`StatsSampler`]) and the per-run profile summary
//! ([`RunProfile`]).

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::group::ProcessGroup;

/// A snapshot of a process group's resource usage.
///
/// `total_cpu_time` and `peak_memory_bytes` are `None` when the platform can't
/// report them — notably the POSIX process-group mechanism (no cgroup
/// accounting), i.e. macOS/BSD and the Linux fallback, and the no-containment
/// `other` target.
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
    pub total_cpu_time: Option<Duration>,
    /// Peak memory used by the group in bytes, if available.
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
    // (Debug: manual impl below.)
    group: &'a ProcessGroup,
    interval: tokio::time::Interval,
    /// Latched once a snapshot fails: the series has ended for good, and
    /// further polls keep returning `None` (a well-behaved, fused stream)
    /// instead of resuming if the group recovers.
    done: bool,
}

impl<'a> StatsSampler<'a> {
    pub(crate) fn new(group: &'a ProcessGroup, every: Duration) -> Self {
        // tokio panics on a zero interval period; clamp instead of imposing a
        // Result on an otherwise-infallible constructor.
        let every = every.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(every);
        // Sampling wants the *current* state on each tick; replaying a backlog
        // of missed ticks would fabricate identical samples.
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunProfile {
    /// The exit code; `None` for a run killed by its timeout or a signal
    /// (matching [`RunningProcess::wait`](crate::RunningProcess::wait)).
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
    /// Average CPU utilisation over the run, in cores (`0.5` = half a core
    /// busy on average; can exceed `1.0` for multi-threaded children).
    /// `None` when CPU time was never observed or the run had no duration.
    pub fn avg_cpu(&self) -> Option<f64> {
        let cpu = self.cpu_time?;
        if self.duration.is_zero() {
            return None;
        }
        Some(cpu.as_secs_f64() / self.duration.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::RunProfile;
    use std::time::Duration;

    #[tokio::test]
    async fn zero_interval_sampler_does_not_panic() {
        // tokio's interval panics on a zero period; the constructor must clamp.
        let group = crate::ProcessGroup::new().expect("create group");
        let _sampler = group.sample_stats(Duration::ZERO);
    }

    #[test]
    fn avg_cpu_is_cpu_time_over_duration() {
        let profile = RunProfile {
            exit_code: Some(0),
            duration: Duration::from_secs(2),
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 8,
        };
        assert_eq!(profile.avg_cpu(), Some(0.5));
    }

    #[test]
    fn avg_cpu_is_none_without_cpu_or_duration() {
        let no_cpu = RunProfile {
            exit_code: Some(0),
            duration: Duration::from_secs(1),
            cpu_time: None,
            peak_memory_bytes: None,
            samples: 0,
        };
        assert_eq!(no_cpu.avg_cpu(), None);

        let no_duration = RunProfile {
            exit_code: Some(0),
            duration: Duration::ZERO,
            cpu_time: Some(Duration::from_secs(1)),
            peak_memory_bytes: None,
            samples: 1,
        };
        assert_eq!(no_duration.avg_cpu(), None);
    }
}
