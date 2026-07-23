//! [`SoftStopScope`] — the reach of a **soft stop** (a `SIGTERM`/`SIGINT`-class
//! request that asks a tree to exit cleanly rather than hard-killing it) on the
//! **group axis**, queried by
//! [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope)
//! *before* a [`signal`](crate::ProcessGroup::signal) is attempted.

/// How far a **soft stop** on the group axis reaches on *this* group, right now
/// — the honest answer to "if I ask this group to stop gracefully
/// ([`signal(Signal::Term)`](crate::ProcessGroup::signal) /
/// [`Signal::Int`](crate::Signal::Int)), which of its members will actually
/// receive that request?"
///
/// This is a **capability report**, not an action: query it with
/// [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope) so a
/// caller (for example a CLI wrapping this crate) can decide *up front* whether a
/// soft stop is worth attempting — and state the real reach to its own user —
/// instead of firing a `signal`, parsing an [`Error::Unsupported`](crate::Error::Unsupported)
/// back, and guessing at the scope from a hard-coded platform assumption. It is
/// the group-axis analogue of
/// [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope)'s
/// [`ParentDeathCleanup`](crate::ParentDeathCleanup) report, but for the
/// deliberate-stop axis rather than the abrupt-owner-death axis.
///
/// # It reports the *soft* tier only
///
/// This describes only the graceful `Int`/`Term` soft-stop tier. The
/// unconditional **hard** kill — [`Signal::Kill`](crate::Signal::Kill),
/// [`kill_all`](crate::ProcessGroup::kill_all), and dropping the group — always
/// tears the whole tree down on every platform regardless of this value; that
/// guarantee is unchanged and never `Unsupported`.
///
/// # Runtime, per-group — not a fixed platform constant
///
/// Unlike [`ParentDeathCleanup`](crate::ParentDeathCleanup) (fixed per target at
/// build time), this is read from the group's **live membership** each call, so
/// the *same* build can report different scopes for different groups — most
/// visibly on Windows, where a group with an opt-in console-CTRL leader or a
/// windowed member reports [`OptInMembers`](Self::OptInMembers) while a group
/// with neither reports [`Unsupported`](Self::Unsupported). The read is
/// **side-effect-free**: it delivers no signal, posts no `WM_CLOSE`, spawns
/// nothing, and does not mutate the group — asking never changes the answer to a
/// later [`signal`](crate::ProcessGroup::signal).
///
/// | Mechanism | Scope | Why |
/// |---|---|---|
/// | Linux cgroup v2 | [`WholeTree`](Self::WholeTree) | `signal(Int/Term)` writes to every process in the cgroup — the whole tree receives it. |
/// | POSIX process group (macOS / the BSDs / Linux cgroup-less fallback) | [`WholeTree`](Self::WholeTree) | `killpg` reaches every tracked group leader **and its descendants** — the whole tracked tree (a child that `setsid`s away escapes, exactly as it does the kill-on-drop guarantee). |
/// | Windows Job Object | [`OptInMembers`](Self::OptInMembers) or [`Unsupported`](Self::Unsupported) | A Job Object has no POSIX signal; a soft stop reaches only members it can *trigger* — a console-CTRL leader (opted in via [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break)) or any live windowed member (`WM_CLOSE`). [`OptInMembers`](Self::OptInMembers) when at least one such member is live, else [`Unsupported`](Self::Unsupported). |
///
/// Consistent with [`signal`](crate::ProcessGroup::signal) by construction: it is
/// read from the very same live-membership primitives `signal(Int/Term)` acts on,
/// so what it reports matches what a real soft stop would then reach.
///
/// Non-exhaustive: a future platform — or a future soft-stop mechanism — may
/// report a scope not listed here, so match it with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SoftStopScope {
    /// The soft stop reaches **every** process in the tree. The Unix backends:
    /// the Linux cgroup v2 mechanism (the signal is delivered to every member of
    /// the cgroup) and the POSIX process-group mechanism (`killpg` reaches each
    /// tracked leader and every descendant in its group). The one documented
    /// escapee is a child that `setsid`s away from its process group — the same
    /// weakening that already applies to the kill-on-drop guarantee, not new to
    /// the soft stop.
    WholeTree,
    /// The soft stop reaches only the members that can *receive* it — a curated
    /// **subset** of the tree, not all of it. Windows: a live console-CTRL
    /// process-group leader (a direct child spawned with
    /// [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break),
    /// addressed by `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)`) and/or any live
    /// member that owns a top-level window (addressed by `WM_CLOSE`). A member
    /// that is neither — a windowless child spawned without the console opt-in, an
    /// off-console child that cannot receive the CTRL event — is **not** reached
    /// by the soft stop and rides to the hard-kill fallback instead.
    OptInMembers,
    /// **No** soft stop is available on this group: not one member can receive a
    /// graceful `Int`/`Term`, so [`signal(Signal::Term)`](crate::ProcessGroup::signal)
    /// / [`Signal::Int`](crate::Signal::Int) would return
    /// [`Error::Unsupported`](crate::Error::Unsupported). Windows, when the group
    /// has neither a console-CTRL leader nor a windowed member (an empty group, or
    /// a tree of plain windowless children with no console opt-in). The
    /// unconditional hard kill still works — this reports only the soft tier's
    /// absence.
    Unsupported,
}

impl SoftStopScope {
    /// This scope's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"whole_tree"`, `"opt_in_members"`, `"none"`) that is
    /// part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    ///
    /// [`Unsupported`](Self::Unsupported) reports `"none"` — "no soft stop" —
    /// matching the spelling the sibling
    /// [`ParentDeathCleanup::name`](crate::ParentDeathCleanup::name) already
    /// publishes for its own unsupported case, so a consumer reading both reports
    /// sees one consistent "none" spelling.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            SoftStopScope::WholeTree => "whole_tree",
            SoftStopScope::OptInMembers => "opt_in_members",
            SoftStopScope::Unsupported => "none",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `SoftStopScope`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so a consumer that
    /// reads an unknown name (for example one minted by a newer version of this
    /// crate) must handle the gap rather than mis-decode it. Round-trips with
    /// [`name`](Self::name): `SoftStopScope::from_name(s.name()) == Some(s)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "whole_tree" => Some(SoftStopScope::WholeTree),
            "opt_in_members" => Some(SoftStopScope::OptInMembers),
            "none" => Some(SoftStopScope::Unsupported),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SoftStopScope;

    const ALL: &[SoftStopScope] = &[
        SoftStopScope::WholeTree,
        SoftStopScope::OptInMembers,
        SoftStopScope::Unsupported,
    ];

    #[test]
    fn name_pins_each_variant() {
        assert_eq!(SoftStopScope::WholeTree.name(), "whole_tree");
        assert_eq!(SoftStopScope::OptInMembers.name(), "opt_in_members");
        // `Unsupported` reports `none`, matching `ParentDeathCleanup`'s spelling.
        assert_eq!(SoftStopScope::Unsupported.name(), "none");
    }

    #[test]
    fn name_from_name_round_trips_every_variant() {
        for &s in ALL {
            assert_eq!(SoftStopScope::from_name(s.name()), Some(s));
        }
    }

    #[test]
    fn from_name_rejects_unknown_without_defaulting() {
        assert_eq!(SoftStopScope::from_name("opt_in"), None);
        assert_eq!(SoftStopScope::from_name("OptInMembers"), None);
        assert_eq!(SoftStopScope::from_name("unsupported"), None);
        assert_eq!(SoftStopScope::from_name(""), None);
    }
}
