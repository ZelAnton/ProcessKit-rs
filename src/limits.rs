//! Resource caps applied to a [`ProcessGroup`](crate::ProcessGroup).

#[cfg(feature = "limits")]
use std::{fmt, io};

#[cfg(feature = "report-serde")]
use serde::ser::{Serialize, SerializeStruct as _, Serializer};

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
/// On Windows, memory and process caps share one extended-limit operation; if
/// that operation is rejected for a combined request, `Memory` is reported when
/// requested, otherwise `Processes`.
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

/// An internal error wrapper used while a backend applies resource limits.
///
/// The public [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit)
/// keeps its existing shape, but the backend must tell the shared error mapping
/// which axis actually failed. Keeping the original `io::Error` as the source is
/// important: the wrapper adds classification without replacing the OS error's
/// kind, errno, or source chain.
#[cfg(feature = "limits")]
#[derive(Debug)]
pub(crate) struct LimitApplicationError {
    kind: LimitKind,
    source: io::Error,
    context: Option<LimitApplicationContext>,
}

#[cfg(feature = "limits")]
#[derive(Debug)]
struct LimitApplicationContext {
    prefix: String,
    suffix: String,
}

#[cfg(feature = "limits")]
impl LimitApplicationError {
    #[cfg(any(windows, target_os = "linux"))]
    fn new(kind: LimitKind, source: io::Error, context: Option<LimitApplicationContext>) -> Self {
        Self {
            kind,
            source,
            context,
        }
    }

    fn kind(&self) -> LimitKind {
        self.kind
    }
}

#[cfg(feature = "limits")]
impl fmt::Display for LimitApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(context) = &self.context {
            write!(f, "{}: {}{}", context.prefix, self.source, context.suffix)
        } else {
            self.source.fmt(f)
        }
    }
}

#[cfg(feature = "limits")]
impl std::error::Error for LimitApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Attach the axis that a limit-capable backend could identify as the failing
/// one while retaining the original OS error for display and source-chain
/// inspection.
#[cfg(all(feature = "limits", any(windows, target_os = "linux")))]
pub(crate) fn limit_application_error(kind: LimitKind, source: io::Error) -> io::Error {
    io::Error::new(
        source.kind(),
        LimitApplicationError::new(kind, source, None),
    )
}

/// Attach an axis and a short backend context while retaining the original OS
/// error as the wrapper's source.
#[cfg(all(feature = "limits", windows))]
pub(crate) fn limit_application_error_with_context(
    kind: LimitKind,
    source: io::Error,
    context: impl Into<String>,
) -> io::Error {
    let error_kind = source.kind();
    io::Error::new(
        error_kind,
        LimitApplicationError::new(
            kind,
            source,
            Some(LimitApplicationContext {
                prefix: context.into(),
                suffix: String::new(),
            }),
        ),
    )
}

/// Attach an axis and preserve a backend's existing error layout around the
/// original OS error. This is useful for long diagnostics whose explanation
/// historically followed the errno text.
#[cfg(all(feature = "limits", target_os = "linux"))]
pub(crate) fn limit_application_error_with_context_parts(
    kind: LimitKind,
    source: io::Error,
    prefix: impl Into<String>,
    suffix: impl Into<String>,
) -> io::Error {
    let error_kind = source.kind();
    io::Error::new(
        error_kind,
        LimitApplicationError::new(
            kind,
            source,
            Some(LimitApplicationContext {
                prefix: prefix.into(),
                suffix: suffix.into(),
            }),
        ),
    )
}

/// Recover a backend-provided axis from an `io::Error`, if the backend could
/// identify one. Unwrapped errors intentionally return `None`, preserving the
/// shared first-requested-axis fallback for unsupported or otherwise ambiguous
/// failures.
#[cfg(feature = "limits")]
pub(crate) fn limit_application_kind(source: &io::Error) -> Option<LimitKind> {
    source
        .get_ref()
        .and_then(|source| source.downcast_ref::<LimitApplicationError>())
        .map(LimitApplicationError::kind)
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
    /// accounting at all** on this platform — macOS/the other BSDs (a POSIX
    /// process group only), FreeBSD (a process reaper: it contains a tree without
    /// accounting for it), or a Linux host with no cgroup v2 mounted. No
    /// mechanism capable of carrying the cap exists here, full stop.
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

/// The post-run verdict for **one** limit axis — did a cap this group carried
/// actually engage while the tree ran?
///
/// This is the **other side** of [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit)
/// and its [`LimitReason`]. That error answers an *admission* question —
/// "why could the cap you asked for not be **applied**?" ([`Invalid`](LimitReason::Invalid) /
/// [`Unsupported`](LimitReason::Unsupported) / [`Unenforceable`](LimitReason::Unenforceable)).
/// This verdict answers the *post-run* question that only the container itself can
/// answer — "did a cap on this axis then actually **fire**?" — read from
/// [`ProcessGroup::limit_evidence`](crate::ProcessGroup::limit_evidence). Neither
/// replaces the other, and the error's semantics are unchanged by this type's
/// existence.
///
/// Different questions, but on a *live* group they can meet on the same axis. A
/// failed [`ProcessGroup::update_limits`](crate::ProcessGroup::update_limits) is
/// not a rollback — the backends write the axes one at a time — so an axis of a
/// rejected request may well be in force, and it stays on the group's cap record
/// either way. The error then says "the set you asked for could not be applied
/// whole"; this verdict still answers "and what actually fired?" from the kernel's
/// own counters, rather than assuming the axis innocent. Only
/// [`ProcessGroup::with_options`](crate::ProcessGroup::with_options) fails strictly
/// before anything runs: it hands back no group at all, so there is nothing left to
/// ask.
///
/// # Never a guess
///
/// A [`Tripped`](Self::Tripped) verdict is only ever returned on **authoritative
/// kernel/OS evidence** that the crate's *own* container recorded. Exit codes and
/// signals are deliberately **not** consulted: a `SIGKILL`-looking death is exactly
/// what a cap-driven kill and a self-inflicted crash have in common, so inferring
/// from them would manufacture the false verdict this type exists to avoid. Where
/// no such evidence exists the answer is [`Unknown`](Self::Unknown) — an explicit
/// gap, never a silent "no".
///
/// # What "tripped" means per axis
///
/// | Axis | `Tripped` means | Evidence |
/// |---|---|---|
/// | [`Memory`](LimitKind::Memory) | the container hit **its own** memory cap and the kernel had to OOM inside it (reclaim could not save the allocation) | Linux cgroup v2 `memory.events`' `oom` counter |
/// | [`Processes`](LimitKind::Processes) | a fork inside the container was **refused** by the cap | Linux cgroup v2 `pids.events`' `max` counter |
/// | [`Cpu`](LimitKind::Cpu) | the tree was **throttled** by the quota at least once | Linux cgroup v2 `cpu.stat`'s `nr_throttled` counter |
///
/// Only the Linux cgroup v2 mechanism keeps such records. A Windows Job Object
/// reports every cap it carries as [`Unknown`](Self::Unknown) — not an oversight but
/// a measured conclusion about what that mechanism preserves after the fact; see
/// [`ProcessGroup::limit_evidence`](crate::ProcessGroup::limit_evidence).
///
/// Note the asymmetry, which is the OS's, not this crate's: a memory or process cap
/// stops work outright, while a CPU quota only *slows* it — a throttled tree is a
/// CPU cap working exactly as asked, not a failure. Read a `Tripped` CPU verdict as
/// "the quota bound this workload", not "the quota broke it".
///
/// # A `NotTripped` memory verdict on a host with swap
///
/// cgroup v2's `memory.max` caps **memory, not memory + swap**. On a host where
/// swap is available to the tree (`memory.swap.max` is `max` by default, and this
/// crate sets no swap cap), the kernel may page a tree out rather than OOM-kill it:
/// the cap is doing its job, nothing is killed, and the honest verdict is
/// [`NotTripped`](Self::NotTripped). It is the *kernel* that never fired, not this
/// report that missed it. Where a hard "over the cap means death" boundary matters
/// — the usual case for untrusted workloads — take swap off the tree externally
/// (`memory.swap.max`, or a swapless host/container), which is also the environment
/// most containers already run in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitVerdict {
    /// The kernel/OS recorded that this cap engaged: the tree was OOM-killed under
    /// its memory cap, denied a fork by its process cap, or throttled by its CPU
    /// quota (see the per-axis table on [`LimitVerdict`]).
    Tripped,
    /// This cap did **not** engage. Either authoritative evidence exists and its
    /// counters are all zero, or no cap was ever in force on this axis for this
    /// group — nothing was capped, so nothing could fire. (Both are the same honest
    /// "no" to "did a cap stop this tree on this axis?"; neither is a fallback for
    /// missing evidence, which is [`Unknown`](Self::Unknown).)
    NotTripped,
    /// **No authoritative evidence is available**, so the crate refuses to answer.
    /// Not a "no": the cap may or may not have fired. Reported when the group runs
    /// on a mechanism with no whole-tree resource accounting at all (the POSIX
    /// process-group mechanism — macOS, the other BSDs, and the Linux fallback with
    /// no usable cgroup v2 — or the FreeBSD process reaper, which contains a tree
    /// without accounting for it), when the platform's container records nothing post-mortem
    /// for a cap it does enforce (**every** cap on a Windows Job Object — see
    /// [`ProcessGroup::limit_evidence`](crate::ProcessGroup::limit_evidence)), or
    /// when the evidence file/counter could not be read.
    Unknown,
}

impl LimitVerdict {
    /// This verdict's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"tripped"`, `"not_tripped"`, `"unknown"`) that is part
    /// of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a cross-language
    /// binding, a structured log field — where a consumer needs one canonical
    /// spelling per variant instead of hand-maintaining its own mapping table. It is
    /// a *diagnostic* name — a stable **vocabulary** rather than a frozen record
    /// schema — and the exact string the opt-in `report-serde` feature serializes a
    /// verdict as (each axis of a [`LimitEvidence`] report). It is held
    /// stable either way: a **new** variant gets a **new** identifier, and an
    /// existing identifier is **never renamed** without a major release.
    /// [`from_name`](Self::from_name) parses it back.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            LimitVerdict::Tripped => "tripped",
            LimitVerdict::NotTripped => "not_tripped",
            LimitVerdict::Unknown => "unknown",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `LimitVerdict`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so a consumer that
    /// reads an unknown name (for example one minted by a newer version of this
    /// crate) must handle the gap rather than mis-decode it. Round-trips with
    /// [`name`](Self::name): `LimitVerdict::from_name(v.name()) == Some(v)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "tripped" => Some(LimitVerdict::Tripped),
            "not_tripped" => Some(LimitVerdict::NotTripped),
            "unknown" => Some(LimitVerdict::Unknown),
            _ => None,
        }
    }
}

/// *(feature `report-serde`)* Serialized as the bare stable
/// [`name()`](LimitVerdict::name) identifier — `"tripped"`, `"not_tripped"`,
/// `"unknown"` — with no wrapping object, since the verdict carries no payload
/// beside it. [`from_name`](LimitVerdict::from_name) parses the same string
/// back, so a recorded verdict round-trips through the identifier rather than
/// through a `Deserialize` this feature deliberately does not ship.
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for LimitVerdict {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name())
    }
}

/// Post-run evidence about a group's resource caps: one [`LimitVerdict`] per
/// [`LimitKind`] axis, read from the container the crate itself owns.
///
/// Obtained from [`ProcessGroup::limit_evidence`](crate::ProcessGroup::limit_evidence)
/// after a run finishes (and before the group is dropped, which takes the container
/// — and with it the evidence — away). It exists to answer the one question a plain
/// exit status cannot: *was my child killed by the cap I set, or did it fail on its
/// own?*
///
/// # Per axis, deliberately
///
/// There is no single whole-group "did anything trip?" answer here, and that is a
/// design decision rather than an omission: collapsing three honest three-valued
/// verdicts into one would have to fold [`NotTripped`](LimitVerdict::NotTripped) and
/// [`Unknown`](LimitVerdict::Unknown) together, turning "we have no evidence" into
/// "no". Ask the axis you capped — [`memory`](Self::memory),
/// [`processes`](Self::processes), [`cpu`](Self::cpu), or
/// [`verdict`](Self::verdict) for a [`LimitKind`] held in a variable — and each
/// answer stays honest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LimitEvidence {
    memory: LimitVerdict,
    processes: LimitVerdict,
    cpu: LimitVerdict,
}

impl LimitEvidence {
    /// Build a report from per-axis verdicts (platform backends only).
    ///
    /// Built only by the backends that have a container to read a post-mortem
    /// from — the Linux cgroup v2 backend and the Windows Job Object backend.
    /// The POSIX process group (macOS/the other BSDs) and the FreeBSD process
    /// reaper have no whole-tree accounting at all and answer with `unknown`, which
    /// assembles the struct literally, leaving this constructor unused on exactly
    /// those targets; allow it there rather than deleting a constructor the other
    /// two backends need (mirrors the `unknown` allow below and
    /// `sys::ProcIdentity`'s per-target allow).
    #[cfg_attr(all(unix, not(target_os = "linux")), allow(dead_code))]
    pub(crate) const fn new(
        memory: LimitVerdict,
        processes: LimitVerdict,
        cpu: LimitVerdict,
    ) -> Self {
        Self {
            memory,
            processes,
            cpu,
        }
    }

    /// The all-[`Unknown`](LimitVerdict::Unknown) report — the honest answer from a
    /// mechanism with no whole-tree resource accounting at all.
    ///
    /// Built only by the backends that *have* such a mechanism to fall back from —
    /// the POSIX process group (macOS/the other BSDs, and the Linux cgroup-less
    /// fallback) and the FreeBSD process reaper, which contains a tree without
    /// accounting for it. Windows always has a Job Object and answers per axis,
    /// leaving this unused there; allow it on exactly that target rather than
    /// deleting a constructor the other backends need (mirrors
    /// `sys::ProcIdentity`'s per-target allow).
    #[cfg_attr(windows, allow(dead_code))]
    pub(crate) const fn unknown() -> Self {
        Self {
            memory: LimitVerdict::Unknown,
            processes: LimitVerdict::Unknown,
            cpu: LimitVerdict::Unknown,
        }
    }

    /// The verdict for [`ResourceLimits::max_memory`].
    pub fn memory(&self) -> LimitVerdict {
        self.memory
    }

    /// The verdict for [`ResourceLimits::max_processes`].
    pub fn processes(&self) -> LimitVerdict {
        self.processes
    }

    /// The verdict for [`ResourceLimits::cpu_quota`].
    pub fn cpu(&self) -> LimitVerdict {
        self.cpu
    }

    /// The verdict for `kind` — the same values [`memory`](Self::memory),
    /// [`processes`](Self::processes) and [`cpu`](Self::cpu) return, addressed by a
    /// [`LimitKind`] held in a variable (for example the `kind` carried by an
    /// [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit), or while
    /// iterating the axes a caller capped).
    pub fn verdict(&self, kind: LimitKind) -> LimitVerdict {
        // Exhaustive (no `_` arm) though `LimitKind` is `#[non_exhaustive]`: within
        // the defining crate a new axis is a compile error here, so it can never
        // silently fall through to a wrong verdict.
        match kind {
            LimitKind::Memory => self.memory,
            LimitKind::Processes => self.processes,
            LimitKind::Cpu => self.cpu,
        }
    }
}

/// *(feature `report-serde`)* One verdict identifier per axis, keyed by the
/// accessor of the same name:
///
/// ```json
/// {"memory": "tripped", "processes": "not_tripped", "cpu": "unknown"}
/// ```
///
/// Per axis deliberately, exactly as the type itself is: there is no
/// whole-report "did anything trip?" key, because producing one would have to
/// fold [`Unknown`](LimitVerdict::Unknown) ("no evidence") together with
/// [`NotTripped`](LimitVerdict::NotTripped) ("evidence of no") — the very
/// collapse this type exists to avoid.
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for LimitEvidence {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Destructured rather than read accessor by accessor: a new limit axis
        // on this report is a compile error here, so it can never silently miss
        // the wire — the same mechanical link `verdict`'s exhaustive `match`
        // over `LimitKind` gives the axis set above.
        let Self {
            memory,
            processes,
            cpu,
        } = self;
        let mut state = serializer.serialize_struct("LimitEvidence", 3)?;
        state.serialize_field("memory", memory)?;
        state.serialize_field("processes", processes)?;
        state.serialize_field("cpu", cpu)?;
        state.end()
    }
}

/// Which limit axes have carried a cap at **any** point in a group's life — the
/// sticky record `ProcessGroup` keeps so post-run evidence stays honest across
/// `update_limits`.
///
/// Sticky rather than a read of the *current* [`ResourceLimits`] on purpose: a
/// caller may cap memory, run a tree that gets OOM-killed, then lift the cap with
/// [`ProcessGroup::update_limits`](crate::ProcessGroup::update_limits). The cap is
/// no longer in force, but it did fire, and reporting `NotTripped` there would be a
/// lie. It also keeps the evidence read off the axes that were never capped, so a
/// group created without limits performs **no** evidence I/O at all.
///
/// Recorded **conservatively** for the same reason: every axis an `update_limits`
/// request names goes on the record once the request reaches the OS, whether that
/// call then succeeds or fails. A failed update is not a rollback — the backends
/// write the axes one at a time — so an axis of a failed request may well be in
/// force, and leaving it off the record would make `limit_evidence` answer
/// `NotTripped` for it without reading a single counter. Erring towards a read (or,
/// where the mechanism keeps no record, towards `Unknown`) can only cost an extra
/// file read; erring the other way manufactures a verdict.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CappedAxes {
    memory: bool,
    processes: bool,
    cpu: bool,
}

impl CappedAxes {
    /// Record every axis `limits` caps, keeping axes already recorded.
    pub(crate) fn record(&mut self, limits: &ResourceLimits) {
        self.memory |= limits.max_memory.is_some();
        self.processes |= limits.max_processes.is_some();
        self.cpu |= limits.cpu_quota.is_some();
    }

    /// Whether `kind` has ever carried a cap.
    ///
    /// Read only by the backends that gather per-axis evidence and so need to
    /// know which axes are worth reading — the Linux cgroup v2 backend and the
    /// Windows Job Object backend. The POSIX process group (macOS/the other BSDs)
    /// and the FreeBSD process reaper report every axis `Unknown` and ignore the
    /// record entirely, leaving this method unused on exactly those targets; allow
    /// it there rather than deleting a method the other two backends need (mirrors
    /// `LimitEvidence::new` above and `sys::ProcIdentity`'s per-target allow).
    #[cfg_attr(all(unix, not(target_os = "linux")), allow(dead_code))]
    pub(crate) fn has(&self, kind: LimitKind) -> bool {
        match kind {
            LimitKind::Memory => self.memory,
            LimitKind::Processes => self.processes,
            LimitKind::Cpu => self.cpu,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CappedAxes, LimitEvidence, LimitKind, LimitReason, LimitVerdict, ResourceLimits};

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

    #[test]
    fn limit_verdict_name_pins_each_variant_and_round_trips() {
        assert_eq!(LimitVerdict::Tripped.name(), "tripped");
        assert_eq!(LimitVerdict::NotTripped.name(), "not_tripped");
        assert_eq!(LimitVerdict::Unknown.name(), "unknown");
        for v in [
            LimitVerdict::Tripped,
            LimitVerdict::NotTripped,
            LimitVerdict::Unknown,
        ] {
            assert_eq!(LimitVerdict::from_name(v.name()), Some(v));
        }
        // An honest miss, never a silent default — including the near-miss spelling
        // a hand-written consumer is most likely to try.
        assert_eq!(LimitVerdict::from_name("Tripped"), None);
        assert_eq!(LimitVerdict::from_name("nottripped"), None);
        assert_eq!(LimitVerdict::from_name(""), None);
    }

    #[test]
    fn limit_evidence_addresses_each_axis_by_kind_and_by_name() {
        let ev = LimitEvidence::new(
            LimitVerdict::Tripped,
            LimitVerdict::NotTripped,
            LimitVerdict::Unknown,
        );
        assert_eq!(ev.memory(), LimitVerdict::Tripped);
        assert_eq!(ev.processes(), LimitVerdict::NotTripped);
        assert_eq!(ev.cpu(), LimitVerdict::Unknown);
        // `verdict(kind)` must agree with the named accessor for every axis — the
        // two spellings can never drift into disagreeing about the same axis.
        for &k in ALL_KINDS {
            let named = match k {
                LimitKind::Memory => ev.memory(),
                LimitKind::Processes => ev.processes(),
                LimitKind::Cpu => ev.cpu(),
            };
            assert_eq!(ev.verdict(k), named, "axis {k:?}");
        }
    }

    #[test]
    fn unknown_evidence_is_unknown_on_every_axis() {
        // The report a mechanism without whole-tree accounting hands back: no axis
        // may silently degrade to a "no".
        let ev = LimitEvidence::unknown();
        for &k in ALL_KINDS {
            assert_eq!(ev.verdict(k), LimitVerdict::Unknown, "axis {k:?}");
        }
    }

    #[test]
    fn capped_axes_are_sticky_across_a_lifted_cap() {
        let mut axes = CappedAxes::default();
        for &k in ALL_KINDS {
            assert!(!axes.has(k));
        }

        axes.record(&ResourceLimits {
            max_memory: Some(64 * 1024 * 1024),
            ..ResourceLimits::default()
        });
        assert!(axes.has(LimitKind::Memory));
        assert!(!axes.has(LimitKind::Processes));
        assert!(!axes.has(LimitKind::Cpu));

        // A later full replacement that LIFTS memory and caps processes must not
        // erase the memory axis: that cap was in force and may well have fired.
        axes.record(&ResourceLimits {
            max_processes: Some(4),
            ..ResourceLimits::default()
        });
        assert!(axes.has(LimitKind::Memory), "a lifted cap stays recorded");
        assert!(axes.has(LimitKind::Processes));
        assert!(
            !axes.has(LimitKind::Cpu),
            "an untouched axis stays unrecorded"
        );

        axes.record(&ResourceLimits {
            cpu_quota: Some(0.5),
            ..ResourceLimits::default()
        });
        for &k in ALL_KINDS {
            assert!(axes.has(k));
        }
    }

    #[test]
    fn any_detects_each_limit_axis_independently() {
        assert!(!ResourceLimits::default().any());
        assert!(
            ResourceLimits {
                max_memory: Some(1),
                ..ResourceLimits::default()
            }
            .any()
        );
        assert!(
            ResourceLimits {
                max_processes: Some(1),
                ..ResourceLimits::default()
            }
            .any()
        );
        assert!(
            ResourceLimits {
                cpu_quota: Some(0.5),
                ..ResourceLimits::default()
            }
            .any()
        );
    }
}
