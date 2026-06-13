//! Which OS mechanism a [`ProcessGroup`](crate::ProcessGroup) is using to
//! contain its child processes.

/// The containment mechanism actually in effect for a process group.
///
/// Surfaced so callers can tell *how* the no-orphan guarantee is enforced — in
/// particular when the mechanism is a POSIX process group rather than a cgroup or
/// Job Object (the primary mechanism on macOS/BSD, and the Linux fallback when no
/// cgroup is writable), which weakens the guarantee against children that call
/// `setsid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mechanism {
    /// Windows Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
    JobObject,
    /// Linux cgroup v2, torn down via `cgroup.kill`.
    CgroupV2,
    /// POSIX process group, torn down via `killpg`. The primary mechanism on
    /// macOS and the BSDs, and the Linux fallback when no cgroup is writable.
    /// Weaker than a cgroup/Job Object: a child that calls `setsid` escapes it.
    ProcessGroup,
}
