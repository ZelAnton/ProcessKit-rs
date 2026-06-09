//! Resource caps applied to a [`ProcessGroup`](crate::ProcessGroup).

/// Resource limits enforced on a process group as a whole.
///
/// Set these via [`ProcessGroupOptions`](crate::ProcessGroupOptions) (the
/// `memory_max` / `max_processes` / `cpu_quota` builders, or by setting the
/// public fields on a `ResourceLimits::default()` value) before creating the
/// group. Every limit bounds the **whole tree**, not a single process, and is
/// applied to the kernel container at creation time.
///
/// # Platform support
///
/// Enforcement needs a real container — a **Windows Job Object** or a **Linux
/// cgroup v2**. On macOS/the BSDs, the Linux process-group fallback, and the
/// no-containment `other` target there is no whole-tree limit primitive, so
/// requesting *any* limit there fails fast with
/// [`Error::ResourceLimit`](crate::Error::ResourceLimit) rather than silently
/// leaving the tree unbounded.
///
/// On Linux the cgroup must permit controller delegation (typically running as
/// root, inside a container, or under a systemd unit with `Delegate=yes`). When the
/// surrounding cgroup can't carry the controllers, creation fails fast with the same
/// error — an unenforced limit is no protection, so it is never silently dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[non_exhaustive]
pub struct ResourceLimits {
    /// Maximum total memory for the tree, in bytes. `None` leaves memory
    /// unbounded.
    pub memory_max: Option<u64>,
    /// Maximum number of live processes in the tree. `None` leaves the count
    /// unbounded.
    pub max_processes: Option<u32>,
    /// CPU quota as a fraction of a **single** core: `0.5` is half a core, `2.0`
    /// is two cores' worth. `None` leaves CPU unbounded.
    ///
    /// On Windows the underlying hard cap is expressed against *total* system CPU
    /// capacity, so this is converted using the host's processor count and is
    /// therefore approximate; a quota at or above the core count saturates at 100%.
    pub cpu_quota: Option<f64>,
}

impl ResourceLimits {
    /// Whether any limit is set (i.e. the group needs a limit-capable mechanism).
    pub(crate) fn any(&self) -> bool {
        self.memory_max.is_some() || self.max_processes.is_some() || self.cpu_quota.is_some()
    }
}
