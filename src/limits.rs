//! Resource caps applied to a [`ProcessGroup`](crate::ProcessGroup).

/// Resource limits enforced on a process group as a whole.
///
/// Set these via [`ProcessGroupOptions`](crate::ProcessGroupOptions) (the
/// `max_memory` / `max_processes` / `cpu_quota` builders, or by setting the
/// public fields on a `ResourceLimits::default()` value) before creating the
/// group. Every limit bounds the **whole tree**, not a single process, and is
/// applied to the kernel container at creation time.
///
/// # Updating a live group (full replacement)
///
/// [`ProcessGroup::update_limits`](crate::ProcessGroup::update_limits) applies a
/// fresh `ResourceLimits` to an already-running group without recreating the
/// container or restarting its children. Its semantics are a **full replacement**,
/// not a merge: the value passed becomes the complete set of active caps, so an
/// axis left `None` is lifted back to **unbounded** — it does *not* retain whatever
/// value was previously in force. Build the whole desired state each time (e.g.
/// start from [`ResourceLimits::default`] and set the axes you want capped).
///
/// # Platform support
///
/// Enforcement needs a real container — a **Windows Job Object** or a **Linux
/// cgroup v2**. On macOS/the BSDs and the Linux process-group fallback there is
/// no whole-tree limit primitive, so
/// requesting *any* limit there fails fast with
/// [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit) rather than silently
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
/// ([`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit)) rather than silently
/// leaving the tree unbounded — an unenforced limit is no protection. The crate
/// deliberately does **not** migrate your process into a sub-cgroup to make limits
/// work elsewhere (the create-leaf→migrate-self→enable dance); do that externally
/// if you need them. (When the controllers are already enabled — at the root — no
/// `subtree_control` write is attempted.)
///
/// Derives `PartialEq` but **not** `Eq` (unlike the sibling config/stats types):
/// [`cpu_quota`](Self::cpu_quota) is an `Option<f64>`, and `f64` is not `Eq`. So
/// `ResourceLimits` compares with `==` but can't be a `HashMap`/`BTreeSet` key.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[non_exhaustive]
pub struct ResourceLimits {
    /// Maximum total memory for the tree, in bytes. `None` leaves memory
    /// unbounded.
    pub max_memory: Option<u64>,
    /// Maximum number of live processes in the tree. `None` leaves the count
    /// unbounded.
    ///
    /// **Cross-platform enforcement differs for direct `spawn`s.** On
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
        self.max_memory.is_some() || self.max_processes.is_some() || self.cpu_quota.is_some()
    }
}

/// Which [`ResourceLimits`] field an
/// [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit) failure is about —
/// [`Memory`](Self::Memory) for [`max_memory`](ResourceLimits::max_memory),
/// [`Processes`](Self::Processes) for
/// [`max_processes`](ResourceLimits::max_processes), [`Cpu`](Self::Cpu) for
/// [`cpu_quota`](ResourceLimits::cpu_quota).
///
/// When a caller requests more than one limit at once and the failure can't be
/// pinned to a single one (e.g. a Linux cgroup that can't enable any controller
/// because this process isn't at the real hierarchy root), `kind` names the
/// **first** requested limit in `max_memory`, `max_processes`, `cpu_quota` order
/// — a fixed, documented tie-break rather than an arbitrary one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitKind {
    /// [`ResourceLimits::max_memory`].
    Memory,
    /// [`ResourceLimits::max_processes`].
    Processes,
    /// [`ResourceLimits::cpu_quota`].
    Cpu,
}

impl LimitKind {
    /// This kind's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"memory"`, `"processes"`, `"cpu"`) that is part of
    /// the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            LimitKind::Memory => "memory",
            LimitKind::Processes => "processes",
            LimitKind::Cpu => "cpu",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `LimitKind`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `LimitKind::from_name(k.name()) == Some(k)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "memory" => Some(LimitKind::Memory),
            "processes" => Some(LimitKind::Processes),
            "cpu" => Some(LimitKind::Cpu),
            _ => None,
        }
    }
}

/// Why a requested resource limit could not be applied — the classification an
/// [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit) failure carries so a
/// caller (e.g. the `processkit-py` binding) can branch on the *kind* of failure
/// without parsing the English `detail` text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitReason {
    /// The requested value itself is nonsensical (e.g. `max_memory(0)`,
    /// a non-finite or non-positive `cpu_quota`) — rejected before the OS is
    /// ever touched.
    Invalid,
    /// The active containment mechanism has **no whole-tree resource
    /// accounting at all** on this platform — macOS/the BSDs (a POSIX process
    /// group only), or a Linux host with no cgroup v2 mounted. No container
    /// capable of carrying the cap exists here, full stop.
    Unsupported,
    /// A capable mechanism **exists**, but this particular request could not
    /// be applied to it — e.g. a Linux cgroup whose controllers can't be
    /// enabled (this process isn't at the real cgroup-v2 hierarchy root — see
    /// [`ResourceLimits`] for the "real root only" requirement), or a Windows
    /// Job Object that rejected the limit.
    Unenforceable,
}

impl LimitReason {
    /// This reason's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"invalid"`, `"unsupported"`, `"unenforceable"`)
    /// that is part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            LimitReason::Invalid => "invalid",
            LimitReason::Unsupported => "unsupported",
            LimitReason::Unenforceable => "unenforceable",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `LimitReason`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `LimitReason::from_name(r.name()) == Some(r)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "invalid" => Some(LimitReason::Invalid),
            "unsupported" => Some(LimitReason::Unsupported),
            "unenforceable" => Some(LimitReason::Unenforceable),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LimitKind, LimitReason};

    const ALL_KINDS: &[LimitKind] = &[LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu];
    const ALL_REASONS: &[LimitReason] = &[
        LimitReason::Invalid,
        LimitReason::Unsupported,
        LimitReason::Unenforceable,
    ];

    #[test]
    fn limit_kind_name_pins_each_variant() {
        assert_eq!(LimitKind::Memory.name(), "memory");
        assert_eq!(LimitKind::Processes.name(), "processes");
        assert_eq!(LimitKind::Cpu.name(), "cpu");
    }

    #[test]
    fn limit_reason_name_pins_each_variant() {
        assert_eq!(LimitReason::Invalid.name(), "invalid");
        assert_eq!(LimitReason::Unsupported.name(), "unsupported");
        assert_eq!(LimitReason::Unenforceable.name(), "unenforceable");
    }

    #[test]
    fn name_from_name_round_trips_every_variant() {
        for &k in ALL_KINDS {
            assert_eq!(LimitKind::from_name(k.name()), Some(k));
        }
        for &r in ALL_REASONS {
            assert_eq!(LimitReason::from_name(r.name()), Some(r));
        }
    }

    #[test]
    fn from_name_rejects_unknown_without_defaulting() {
        assert_eq!(LimitKind::from_name("Memory"), None);
        assert_eq!(LimitKind::from_name("ram"), None);
        assert_eq!(LimitReason::from_name(""), None);
        assert_eq!(LimitReason::from_name("unenforced"), None);
    }
}
