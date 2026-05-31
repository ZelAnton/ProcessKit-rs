//! Which OS mechanism a [`ProcessGroup`](crate::ProcessGroup) is using to
//! contain its child processes.

/// The containment mechanism actually in effect for a process group.
///
/// Surfaced so callers can tell *how* the no-orphan guarantee is enforced — in
/// particular when Linux silently falls back from a cgroup to a POSIX process
/// group (e.g. on a CI runner without cgroup delegation), which weakens the
/// guarantee against children that call `setsid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mechanism {
    /// Windows Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
    JobObject,
    /// Linux cgroup v2, torn down via `cgroup.kill`.
    CgroupV2,
    /// POSIX process group — the Linux fallback when no cgroup is writable.
    ProcessGroup,
    /// No kernel containment: the child is spawned directly (non-Windows,
    /// non-Linux targets such as macOS/BSD).
    None,
}
