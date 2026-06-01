//! Diagnostic counters for a [`ProcessGroup`](crate::ProcessGroup).

use std::time::Duration;

/// A snapshot of a process group's resource usage.
///
/// `total_cpu_time` and `peak_memory_bytes` are `None` when the platform can't
/// report them — notably the POSIX process-group mechanism (no cgroup
/// accounting), i.e. macOS/BSD and the Linux fallback, and the no-containment
/// `other` target.
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
