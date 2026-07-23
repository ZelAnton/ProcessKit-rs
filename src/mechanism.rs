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
}
