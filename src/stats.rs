//! Diagnostic counters for a [`ProcessGroup`](crate::ProcessGroup).

use std::time::Duration;

/// A snapshot of a process group's resource usage.
///
/// `total_cpu_time` and `peak_memory_bytes` are `None` when the platform can't
/// report them — notably the Linux POSIX process-group fallback (no cgroup
/// accounting) and the no-containment `other` target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessGroupStats {
    /// Number of live processes currently in the group.
    pub active_process_count: usize,
    /// Total CPU time (user + kernel) accumulated by the group, if available.
    pub total_cpu_time: Option<Duration>,
    /// Peak memory used by the group in bytes, if available.
    pub peak_memory_bytes: Option<u64>,
}
