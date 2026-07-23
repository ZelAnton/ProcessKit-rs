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
/// particular when the mechanism is a POSIX process group rather than a cgroup or
/// Job Object (the primary mechanism on macOS/BSD, and the Linux fallback when no
/// cgroup is writable), which weakens the guarantee against children that call
/// `setsid`.
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
    /// macOS and the BSDs, and the Linux fallback when no cgroup is writable.
    /// Weaker than a cgroup/Job Object: a child that calls `setsid` escapes it.
    ProcessGroup,
}

impl Mechanism {
    /// This mechanism's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"job_object"`, `"cgroup_v2"`, `"process_group"`)
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
            Mechanism::JobObject => "job_object",
            Mechanism::CgroupV2 => "cgroup_v2",
            Mechanism::ProcessGroup => "process_group",
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
    /// A fixed per-target constant on Windows ([`JobObject`](Self::JobObject)) and
    /// macOS/BSD ([`ProcessGroup`](Self::ProcessGroup)); on Linux a cheap read-only
    /// probe of cgroup v2 availability and writability. The Linux cgroup answer is
    /// **best-effort**: it inspects whether a leaf cgroup *could* be created rather
    /// than creating one, so in the rare window where a writable-looking cgroup then
    /// rejects creation it can report [`CgroupV2`](Self::CgroupV2) where a real
    /// `ProcessGroup::new` falls back to [`ProcessGroup`](Self::ProcessGroup).
    pub(crate) fn detect() -> Mechanism {
        crate::sys::detect_mechanism()
    }
}

/// A spawn-free, side-effect-free report of how process containment behaves on
/// **this** host — the answer to a consumer's preflight "what will I get here?"
/// without paying to create a real [`ProcessGroup`](crate::ProcessGroup).
///
/// Produced by [`host_containment`](crate::host_containment). A [`ProcessGroup`]
/// exposes the same facts only *after* it exists ([`mechanism`](crate::ProcessGroup::mechanism),
/// [`soft_stop_scope`](crate::ProcessGroup::soft_stop_scope)), which forces a
/// caller whose contract is "no side effects" (a doctor / host-check command that
/// must not spawn a child, open a run registry, or create a container) to either
/// skip the report or violate that contract. This type closes that gap: every field
/// is derived from cheap read-only queries, so a preflight can state the real
/// containment story up front.
///
/// # What each field means
///
/// - [`mechanism`](Self::mechanism) — the [`Mechanism`] a group created here and now
///   would use ([`Mechanism::detect`]). On Linux this is a **best-effort** read-only
///   probe of cgroup v2 availability/writability (it does not create the cgroup it
///   would use), so in a rare window it can differ from the mechanism a real
///   `ProcessGroup::new` falls back to — see [`Mechanism::detect`].
/// - [`soft_stop_scope`](Self::soft_stop_scope) *(needs `process-control`)* — how far
///   a **soft stop** ([`ProcessGroup::signal`](crate::ProcessGroup::signal) with
///   [`Signal::Term`](crate::Signal::Term) / [`Int`](crate::Signal::Int)) can reach
///   on this host's mechanism: [`WholeTree`](crate::SoftStopScope::WholeTree) on the
///   Unix backends, [`OptInMembers`](crate::SoftStopScope::OptInMembers) on Windows
///   (a Job Object soft-stops only a console-CTRL leader or a windowed member). This
///   is the **host-level maximum** the mechanism can achieve; a *specific* group's
///   actual reach is still read per-group from
///   [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope), which
///   on Windows narrows to [`Unsupported`](crate::SoftStopScope::Unsupported) for a
///   group holding no such member.
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
    /// here and now would use — see [`Mechanism::detect`] for the per-platform detail
    /// and the Linux best-effort caveat.
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
/// *maximum* reach of a soft `Int`/`Term` stop for that backend. The Unix backends
/// (cgroup v2 and the POSIX process-group fallback) reach the
/// [`WholeTree`](SoftStopScope::WholeTree); a Windows Job Object can only ever
/// soft-stop its [`OptInMembers`](SoftStopScope::OptInMembers) (a console-CTRL leader
/// or a windowed member). Kept consistent with
/// [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope) by
/// construction: a real group only ever reports this scope or (on Windows, with no
/// reachable member) the narrower `Unsupported`.
#[cfg(feature = "process-control")]
fn host_soft_stop_scope(mechanism: Mechanism) -> SoftStopScope {
    // Exhaustive (no `_` arm) though `Mechanism` is `#[non_exhaustive]`: within the
    // defining crate a new variant is a compile error here, so a new mechanism can
    // never silently ship without deciding its host-level soft-stop reach.
    match mechanism {
        Mechanism::CgroupV2 | Mechanism::ProcessGroup => SoftStopScope::WholeTree,
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
    ];

    #[test]
    fn name_pins_each_variant() {
        // Pin every identifier: these strings are a compatibility surface and
        // must not drift.
        assert_eq!(Mechanism::JobObject.name(), "job_object");
        assert_eq!(Mechanism::CgroupV2.name(), "cgroup_v2");
        assert_eq!(Mechanism::ProcessGroup.name(), "process_group");
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
    }

    #[test]
    fn host_report_mechanism_matches_the_read_only_detection() {
        // The report's mechanism is exactly what `Mechanism::detect` returns — no
        // second, independently-drifting derivation.
        let report = super::HostContainment::probe();
        assert_eq!(report.mechanism(), Mechanism::detect());
        assert!(matches!(
            report.mechanism(),
            Mechanism::JobObject | Mechanism::CgroupV2 | Mechanism::ProcessGroup
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
