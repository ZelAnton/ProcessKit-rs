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
/// **Linux (cgroup v2): limits need this process at the *real* cgroup root.**
/// The crate creates the limit cgroup as a **child of this process's own cgroup**
/// and enables the controllers in *that* cgroup's `cgroup.subtree_control`. cgroup
/// v2's "no internal processes" rule permits enabling controllers in a cgroup that
/// holds member processes only for the **root of the real hierarchy** — the one
/// exempt cgroup. A cgroup *namespace* root does **not** qualify: it only
/// virtualizes the view (`/proc/self/cgroup` reads `0::/`), but the cgroup still
/// isn't the real root, so a container with a private cgroup namespace (the
/// Docker/Kubernetes default) hits `EBUSY` exactly like a systemd scope. So in
/// practice these limits apply only when this process is a direct member of the
/// real hierarchy root (a minimal init not managed by systemd) — **not** under any
/// systemd session/scope/service, and **not** in an ordinary container. When the
/// controllers can't be enabled, group creation **fails fast**
/// ([`Error::ResourceLimit`](crate::Error::ResourceLimit)) rather than silently
/// leaving the tree unbounded — an unenforced limit is no protection. The crate
/// deliberately does **not** migrate your process into a sub-cgroup to make limits
/// work elsewhere (the create-leaf→migrate-self→enable dance); do that externally
/// if you need them. (When the controllers are already enabled — at the root — no
/// `subtree_control` write is attempted.)
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[non_exhaustive]
pub struct ResourceLimits {
    /// Maximum total memory for the tree, in bytes. `None` leaves memory
    /// unbounded.
    pub memory_max: Option<u64>,
    /// Maximum number of live processes in the tree. `None` leaves the count
    /// unbounded.
    ///
    /// **B14 — cross-platform enforcement differs for direct `spawn`s.** On
    /// **Windows** the Job Object's `ActiveProcessLimit` rejects the *(n+1)*th
    /// process assigned to the job, so `max_processes(n)` caps even repeated
    /// [`ProcessGroup::start`](crate::ProcessGroup::start) calls into one group.
    /// On **Linux** the kernel checks `pids.max` only when a process forks
    /// *inside* the cgroup; the crate's children fork in the parent cgroup and
    /// migrate in during pre-exec, so the cap reliably bounds the **descendants**
    /// a contained child forks, but does **not** reject additional `start()` calls
    /// that each place one more top-level child into the group. Treat
    /// `max_processes` as a bound on a tree's own fork bomb, not as an exact
    /// admission limit on how many children *you* start into a shared group on
    /// Linux. (Memory and CPU caps are whole-cgroup and do not have this caveat.)
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
