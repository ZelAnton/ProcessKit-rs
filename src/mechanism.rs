//! Which OS mechanism a [`ProcessGroup`](crate::ProcessGroup) is using to
//! contain its child processes, plus [`HostContainment`] — the spawn-free host
//! capability report ([`host_containment`](crate::host_containment)) that predicts
//! that mechanism (and the reach of soft stop / parent-death cleanup) *before* any
//! group exists.

use crate::parent_death::ParentDeathCleanup;
#[cfg(feature = "process-control")]
use crate::soft_stop::SoftStopScope;

/// The containment mechanism actually in effect for a process group.
///
/// Surfaced so callers can tell *how* the no-orphan guarantee is enforced — in
/// particular when the mechanism is a POSIX process group rather than a cgroup,
/// Job Object or FreeBSD process reaper (the primary mechanism on macOS and the
/// non-FreeBSD BSDs, and the Linux fallback when no cgroup is writable), which
/// weakens the guarantee against children that call `setsid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mechanism {
    /// Windows Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
    JobObject,
    /// Linux cgroup v2, torn down via `cgroup.kill` where available (Linux ≥ 5.14);
    /// on an older kernel without `cgroup.kill`, or if that write fails, it falls
    /// back to sweeping `cgroup.procs` with per-pid `SIGKILL`.
    CgroupV2,
    /// POSIX process group, torn down via `killpg`. The primary mechanism on
    /// macOS and the BSDs other than FreeBSD, and the Linux fallback when no
    /// cgroup is writable. Weaker than a cgroup/Job Object: a child that calls
    /// `setsid` escapes it.
    ProcessGroup,
    /// FreeBSD **process reaper** — `procctl(2)`'s `PROC_REAP_ACQUIRE`, which makes
    /// the owning process the reaper of its whole descendant tree. Unlike a POSIX
    /// process group, a descendant that calls `setsid` (or double-forks) stays in
    /// the reaper's subtree: it remains visible to `PROC_REAP_GETPIDS` and is torn
    /// down by `PROC_REAP_KILL`, so the classic `setsid` escape is closed. Whole-tree
    /// *kill* and *membership*, like a cgroup or Job Object; whole-tree *resource
    /// accounting* is still absent (the reaper is a process-tree facility, not a
    /// container), so the `limits` feature stays unsupported here.
    ProcessReaper,
}

impl Mechanism {
    /// This mechanism's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"job_object"`, `"cgroup_v2"`, `"process_group"`,
    /// `"process_reaper"`) that is part of the crate's compatibility surface.
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
            Mechanism::JobObject => "job_object",
            Mechanism::CgroupV2 => "cgroup_v2",
            Mechanism::ProcessGroup => "process_group",
            Mechanism::ProcessReaper => "process_reaper",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `Mechanism`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so a consumer that
    /// reads an unknown name (for example one minted by a newer version of this
    /// crate) must handle the gap rather than mis-decode it. Round-trips with
    /// [`name`](Self::name): `Mechanism::from_name(m.name()) == Some(m)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "job_object" => Some(Mechanism::JobObject),
            "cgroup_v2" => Some(Mechanism::CgroupV2),
            "process_group" => Some(Mechanism::ProcessGroup),
            "process_reaper" => Some(Mechanism::ProcessReaper),
            _ => None,
        }
    }

    /// The mechanism a [`ProcessGroup`](crate::ProcessGroup) created **here and now**
    /// would use — determined by a **read-only** probe that creates no container and
    /// spawns no process. This is the detection lifted out of the group-creation
    /// path (`ProcessGroup::new`) so the public
    /// [`host_containment`](crate::host_containment) query and the real selection
    /// share one source of truth.
    ///
    /// A fixed per-target constant on Windows ([`JobObject`](Self::JobObject)),
    /// FreeBSD ([`ProcessReaper`](Self::ProcessReaper)) and the other unix targets
    /// ([`ProcessGroup`](Self::ProcessGroup)); on Linux a cheap read-only probe of
    /// cgroup v2 availability and writability. Two answers are **best-effort**, and
    /// for the same reason — the prediction must create nothing:
    ///
    /// - **Linux** inspects whether a leaf cgroup *could* be created rather than
    ///   creating one, so in the rare window where a writable-looking cgroup then
    ///   rejects creation it can report [`CgroupV2`](Self::CgroupV2) where a real
    ///   `ProcessGroup::new` falls back to [`ProcessGroup`](Self::ProcessGroup).
    /// - **FreeBSD** reports [`ProcessReaper`](Self::ProcessReaper) without calling
    ///   `procctl(PROC_REAP_ACQUIRE)` (acquiring reaper status is a side effect a
    ///   spawn-free query must not have). Acquisition is available on every
    ///   supported FreeBSD kernel and takes no privilege, so the prediction holds in
    ///   practice; should it nonetheless fail, a real group degrades to
    ///   [`ProcessGroup`](Self::ProcessGroup) and says so through
    ///   [`ProcessGroup::mechanism`](crate::ProcessGroup::mechanism).
    pub(crate) fn detect() -> Mechanism {
        crate::sys::detect_mechanism()
    }
}

/// A spawn-free, side-effect-free report of how process containment behaves on
/// **this** host — the answer to a consumer's preflight "what will I get here?"
/// without paying to create a real [`ProcessGroup`](crate::ProcessGroup).
///
/// Produced by [`host_containment`](crate::host_containment). A
/// [`ProcessGroup`](crate::ProcessGroup) exposes the same facts only *after* it
/// exists ([`mechanism`](crate::ProcessGroup::mechanism), `soft_stop_scope`), which forces a
/// caller whose contract is "no side effects" (a doctor / host-check command that
/// must not spawn a child, open a run registry, or create a container) to either
/// skip the report or violate that contract. This type closes that gap: every field
/// is derived from cheap read-only queries, so a preflight can state the real
/// containment story up front.
///
/// # What each field means
///
/// - [`mechanism`](Self::mechanism) — the [`Mechanism`] a group created here and now
///   would use (`Mechanism::detect`). Two answers are **best-effort**, both because
///   the query creates nothing: on Linux a read-only probe of cgroup v2
///   availability/writability (it does not create the cgroup it would use), and on
///   FreeBSD the process reaper reported without acquiring reaper status. In a rare
///   window either can differ from the mechanism a real `ProcessGroup::new` falls
///   back to.
// The `soft_stop_scope` bullet is split by feature: under `process-control` it keeps
// the full intra-doc links; without it those items (`SoftStopScope`, `Signal`,
// `ProcessGroup::signal`/`::soft_stop_scope`, `Self::soft_stop_scope`) are `cfg`-ed out
// and linking to them would break `cargo doc --no-default-features` (K-026).
#[cfg_attr(
    feature = "process-control",
    doc = "- [`soft_stop_scope`](Self::soft_stop_scope) *(needs `process-control`)* — how far",
    doc = "  a **soft stop** ([`ProcessGroup::signal`](crate::ProcessGroup::signal) with",
    doc = "  [`Signal::Term`](crate::Signal::Term) / [`Int`](crate::Signal::Int)) can reach",
    doc = "  on this host's mechanism: [`WholeTree`](crate::SoftStopScope::WholeTree) on the",
    doc = "  Unix backends, [`OptInMembers`](crate::SoftStopScope::OptInMembers) on Windows",
    doc = "  (a Job Object soft-stops only a console-CTRL leader or a windowed member). This",
    doc = "  is the **host-level maximum** the mechanism can achieve; a *specific* group's",
    doc = "  actual reach is still read per-group from",
    doc = "  [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope), which",
    doc = "  on Windows narrows to [`Unsupported`](crate::SoftStopScope::Unsupported) for a",
    doc = "  group holding no such member."
)]
#[cfg_attr(
    not(feature = "process-control"),
    doc = "- `soft_stop_scope` *(needs the `process-control` feature)* — how far a **soft",
    doc = "  stop** (a `Term` / `Int` signal) can reach on this host's mechanism: the whole",
    doc = "  tree on the Unix backends, only opt-in members on Windows (a Job Object",
    doc = "  soft-stops only a console-CTRL leader or a windowed member). This is the",
    doc = "  **host-level maximum** the mechanism can achieve; a *specific* group's actual",
    doc = "  reach is still read per-group from its own `soft_stop_scope`, which on Windows",
    doc = "  narrows to `Unsupported` for a group holding no such member."
)]
/// - [`parent_death_cleanup`](Self::parent_death_cleanup) — what the OS guarantees
///   when the **owner dies abruptly** (`SIGKILL`/crash, so `Drop` never runs): the
///   very same [`ParentDeathCleanup`] that
///   [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope)
///   reports, reused here rather than recomputed.
/// - [`crate_version`](Self::crate_version) — this `processkit` build's version, so
///   a report logged by a preflight is attributable to the exact crate that produced
///   it.
///
/// # Side-effect-free
///
/// Building this report creates no container, spawns no process, opens no run
/// registry, and (on Linux) creates no cgroup directory — it is exactly the
/// spawn-free preflight the sibling [`which`](crate::which) /
/// [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope)
/// queries are, extended to the whole-host containment picture.
///
/// Non-exhaustive and accessor-only: a read-only snapshot the crate produces, so new
/// host facts can be added without a breaking change, and each field is read through
/// a method (documenting its own caveats) rather than a public struct field.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostContainment {
    mechanism: Mechanism,
    #[cfg(feature = "process-control")]
    soft_stop_scope: SoftStopScope,
    parent_death_cleanup: ParentDeathCleanup,
    crate_version: &'static str,
}

impl HostContainment {
    /// Assemble the host report from cheap read-only queries. Crate-internal; the
    /// public entry point is [`host_containment`](crate::host_containment).
    pub(crate) fn probe() -> Self {
        let mechanism = Mechanism::detect();
        Self {
            mechanism,
            #[cfg(feature = "process-control")]
            soft_stop_scope: host_soft_stop_scope(mechanism),
            parent_death_cleanup: crate::Command::kill_on_parent_death_scope(),
            crate_version: env!("CARGO_PKG_VERSION"),
        }
    }

    /// The containment [`Mechanism`] a [`ProcessGroup`](crate::ProcessGroup) created
    /// here and now would use — see the `mechanism` field note above (via the shared
    /// `Mechanism::detect` probe) for the per-platform detail and the two
    /// best-effort caveats (Linux, FreeBSD).
    pub fn mechanism(&self) -> Mechanism {
        self.mechanism
    }

    /// The host-level reach of a **soft stop** on this mechanism (needs
    /// `process-control`): [`WholeTree`](crate::SoftStopScope::WholeTree) on the Unix
    /// backends, [`OptInMembers`](crate::SoftStopScope::OptInMembers) on Windows.
    ///
    /// This is the *maximum* a group on this host can achieve; the actual reach of a
    /// **specific** group is read per-group from
    /// [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope), which
    /// on Windows reports the narrower
    /// [`Unsupported`](crate::SoftStopScope::Unsupported) for a group with neither a
    /// console-CTRL leader nor a windowed member. See [`SoftStopScope`] for the full
    /// contract.
    #[cfg(feature = "process-control")]
    pub fn soft_stop_scope(&self) -> SoftStopScope {
        self.soft_stop_scope
    }

    /// What the OS cleans up when the **owner dies abruptly** — the same
    /// [`ParentDeathCleanup`]
    /// [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope)
    /// reports, fixed per target: [`WholeTree`](ParentDeathCleanup::WholeTree) on
    /// Windows, [`DirectChildOnly`](ParentDeathCleanup::DirectChildOnly) on Linux,
    /// [`Unsupported`](ParentDeathCleanup::Unsupported) on macOS/BSD.
    pub fn parent_death_cleanup(&self) -> ParentDeathCleanup {
        self.parent_death_cleanup
    }

    /// This `processkit` build's crate version (`CARGO_PKG_VERSION`), so a logged
    /// report is attributable to the exact crate that produced it.
    pub fn crate_version(&self) -> &'static str {
        self.crate_version
    }
}

/// The host-level [`SoftStopScope`] a group would achieve on `mechanism` — the
/// *maximum* reach of a soft `Int`/`Term` stop for that backend. Every Unix backend
/// (cgroup v2, the FreeBSD process reaper, and the POSIX process-group fallback)
/// reaches the [`WholeTree`](SoftStopScope::WholeTree); a Windows Job Object can only
/// ever soft-stop its [`OptInMembers`](SoftStopScope::OptInMembers) (a console-CTRL
/// leader or a windowed member). Kept consistent with
/// [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope) by
/// construction: a real group only ever reports this scope or (on Windows, with no
/// reachable member) the narrower `Unsupported`.
///
/// The FreeBSD reaper reaches the whole tree in the strongest sense of the three:
/// `PROC_REAP_KILL` delivers the soft signal to every descendant the reaper sees,
/// which — unlike `killpg` — still includes one that `setsid`ed out of its process
/// group.
#[cfg(feature = "process-control")]
fn host_soft_stop_scope(mechanism: Mechanism) -> SoftStopScope {
    // Exhaustive (no `_` arm) though `Mechanism` is `#[non_exhaustive]`: within the
    // defining crate a new variant is a compile error here, so a new mechanism can
    // never silently ship without deciding its host-level soft-stop reach.
    match mechanism {
        Mechanism::CgroupV2 | Mechanism::ProcessGroup | Mechanism::ProcessReaper => {
            SoftStopScope::WholeTree
        }
        Mechanism::JobObject => SoftStopScope::OptInMembers,
    }
}

#[cfg(test)]
mod tests {
    use super::Mechanism;

    const ALL: &[Mechanism] = &[
        Mechanism::JobObject,
        Mechanism::CgroupV2,
        Mechanism::ProcessGroup,
        Mechanism::ProcessReaper,
    ];

    #[test]
    fn name_pins_each_variant() {
        // Pin every identifier: these strings are a compatibility surface and
        // must not drift.
        assert_eq!(Mechanism::JobObject.name(), "job_object");
        assert_eq!(Mechanism::CgroupV2.name(), "cgroup_v2");
        assert_eq!(Mechanism::ProcessGroup.name(), "process_group");
        assert_eq!(Mechanism::ProcessReaper.name(), "process_reaper");
    }

    #[test]
    fn name_from_name_round_trips_every_variant() {
        for &m in ALL {
            assert_eq!(Mechanism::from_name(m.name()), Some(m));
        }
    }

    #[test]
    fn from_name_rejects_unknown_without_defaulting() {
        assert_eq!(Mechanism::from_name("JobObject"), None);
        assert_eq!(Mechanism::from_name("jobobject"), None);
        assert_eq!(Mechanism::from_name(""), None);
        assert_eq!(Mechanism::from_name("cgroup"), None);
        // A near-miss on the newest identifier must not resolve either: the
        // dictionary is exact, never a prefix/fuzzy match.
        assert_eq!(Mechanism::from_name("reaper"), None);
        assert_eq!(Mechanism::from_name("ProcessReaper"), None);
    }

    #[test]
    fn host_report_mechanism_matches_the_read_only_detection() {
        // The report's mechanism is exactly what `Mechanism::detect` returns — no
        // second, independently-drifting derivation.
        let report = super::HostContainment::probe();
        assert_eq!(report.mechanism(), Mechanism::detect());
        assert!(matches!(
            report.mechanism(),
            Mechanism::JobObject
                | Mechanism::CgroupV2
                | Mechanism::ProcessGroup
                | Mechanism::ProcessReaper
        ));
    }

    #[test]
    fn host_report_parent_death_reuses_the_command_query() {
        // The parent-death field is the very same `ParentDeathCleanup` the existing
        // capability query reports — reused, not recomputed.
        assert_eq!(
            super::HostContainment::probe().parent_death_cleanup(),
            crate::Command::kill_on_parent_death_scope(),
        );
    }

    #[test]
    fn host_report_carries_this_crate_version() {
        assert_eq!(
            super::HostContainment::probe().crate_version(),
            env!("CARGO_PKG_VERSION"),
        );
    }

    #[test]
    fn host_report_is_clonable_and_comparable() {
        let report = super::HostContainment::probe();
        assert_eq!(report.clone(), report);
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn host_soft_stop_scope_maps_each_mechanism() {
        use crate::SoftStopScope;
        // The Unix backends reach the whole tree; a Windows Job Object only its
        // opt-in members — the host-level maximum a real group can then achieve.
        assert_eq!(
            super::host_soft_stop_scope(Mechanism::CgroupV2),
            SoftStopScope::WholeTree
        );
        assert_eq!(
            super::host_soft_stop_scope(Mechanism::ProcessGroup),
            SoftStopScope::WholeTree
        );
        assert_eq!(
            super::host_soft_stop_scope(Mechanism::ProcessReaper),
            SoftStopScope::WholeTree
        );
        assert_eq!(
            super::host_soft_stop_scope(Mechanism::JobObject),
            SoftStopScope::OptInMembers
        );
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn host_report_soft_stop_scope_is_consistent_with_its_mechanism() {
        let report = super::HostContainment::probe();
        assert_eq!(
            report.soft_stop_scope(),
            super::host_soft_stop_scope(report.mechanism()),
        );
    }
}
