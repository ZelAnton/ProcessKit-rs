//! [`ParentDeathCleanup`] — the scope of process-tree teardown that
//! [`Command::kill_on_parent_death`](crate::Command::kill_on_parent_death)
//! achieves when the **owning** process dies *abruptly* on the current platform.

/// The reach of the parent-death hardening
/// ([`Command::kill_on_parent_death`](crate::Command::kill_on_parent_death))
/// when the owning process dies **abruptly** — a `SIGKILL` of the owner or a
/// crash, where `Drop` never runs to tear the containment group down.
///
/// This is an **honest capability report**, not a request: query it with
/// [`Command::kill_on_parent_death_scope`](crate::Command::kill_on_parent_death_scope)
/// so a caller (for example a CLI wrapping this crate) can state the *actual*
/// reach of the hardening on the platform it runs on instead of overpromising a
/// whole-tree guarantee the OS cannot keep. The kernel offers no portable
/// "kill the tree when its creator dies" primitive on Unix — only Windows Job
/// Objects give it for free — so this value tells the truth per platform rather
/// than papering over the gap.
///
/// It describes **only the abrupt-death path**. The ordinary graceful teardown —
/// the owner exits or panics, so the [`ProcessGroup`](crate::ProcessGroup)'s
/// `Drop` runs — kills the whole tree on every supported platform regardless of
/// this value; that unconditional kill-on-drop guarantee is unchanged.
///
/// The variant is fixed per target at build time: it reflects which kernel
/// primitive backs the hardening, not any runtime state (nor whether
/// [`kill_on_parent_death`](crate::Command::kill_on_parent_death) was actually
/// called).
///
/// | Platform | Scope | Mechanism |
/// |---|---|---|
/// | Windows | [`WholeTree`](Self::WholeTree) | The kernel closes the Job Object handle when the owner dies and `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` reaps the whole tree — guaranteed regardless of the knob. |
/// | Linux | [`DirectChildOnly`](Self::DirectChildOnly) | `PR_SET_PDEATHSIG(SIGKILL)` reaches only the **direct child**; with the owner gone nothing triggers `cgroup.kill`, so grandchildren survive. |
/// | macOS / the BSDs | [`Unsupported`](Self::Unsupported) | No `pdeathsig` equivalent exists, so an abrupt owner death triggers no cleanup at all. |
///
/// Non-exhaustive: a future platform — or a future whole-tree Linux mechanism —
/// may report a scope not listed here, so match it with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParentDeathCleanup {
    /// The **entire** process tree — the direct child and every descendant — is
    /// torn down when the owner dies abruptly. Windows, via Job Object
    /// kill-on-close (the kernel closes the last job handle on owner death).
    WholeTree,
    /// Only the **direct child** is killed; grandchildren and deeper
    /// descendants survive the owner's abrupt death. Linux, via
    /// `PR_SET_PDEATHSIG` — it reaches the direct child alone, and with the
    /// owner gone nothing tears the surviving cgroup/process group down.
    DirectChildOnly,
    /// **No** parent-death cleanup is available: an abrupt owner death leaves
    /// the whole tree running. macOS and the BSDs, which have no `pdeathsig`
    /// equivalent — [`kill_on_parent_death`](crate::Command::kill_on_parent_death)
    /// is accepted there but is a documented no-op. (The graceful-exit
    /// guarantee via `Drop` still holds when the owner exits normally.)
    Unsupported,
}

impl ParentDeathCleanup {
    /// This scope's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"whole_tree"`, `"direct_child_only"`, `"none"`)
    /// that is part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    ///
    /// [`Unsupported`](Self::Unsupported) reports `"none"` — "no parent-death
    /// cleanup" — matching the spelling downstream tools already publish, so
    /// adopting these identifiers needs no migration on their side.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            ParentDeathCleanup::WholeTree => "whole_tree",
            ParentDeathCleanup::DirectChildOnly => "direct_child_only",
            ParentDeathCleanup::Unsupported => "none",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `ParentDeathCleanup`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so a consumer that
    /// reads an unknown name (for example one minted by a newer version of this
    /// crate) must handle the gap rather than mis-decode it. Round-trips with
    /// [`name`](Self::name): `ParentDeathCleanup::from_name(c.name()) ==
    /// Some(c)` for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "whole_tree" => Some(ParentDeathCleanup::WholeTree),
            "direct_child_only" => Some(ParentDeathCleanup::DirectChildOnly),
            "none" => Some(ParentDeathCleanup::Unsupported),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ParentDeathCleanup;

    const ALL: &[ParentDeathCleanup] = &[
        ParentDeathCleanup::WholeTree,
        ParentDeathCleanup::DirectChildOnly,
        ParentDeathCleanup::Unsupported,
    ];

    #[test]
    fn name_pins_each_variant() {
        assert_eq!(ParentDeathCleanup::WholeTree.name(), "whole_tree");
        assert_eq!(
            ParentDeathCleanup::DirectChildOnly.name(),
            "direct_child_only"
        );
        // `Unsupported` reports `none`, matching the downstream spelling.
        assert_eq!(ParentDeathCleanup::Unsupported.name(), "none");
    }

    #[test]
    fn name_from_name_round_trips_every_variant() {
        for &c in ALL {
            assert_eq!(ParentDeathCleanup::from_name(c.name()), Some(c));
        }
    }

    #[test]
    fn from_name_rejects_unknown_without_defaulting() {
        assert_eq!(ParentDeathCleanup::from_name("unsupported"), None);
        assert_eq!(ParentDeathCleanup::from_name("WholeTree"), None);
        assert_eq!(ParentDeathCleanup::from_name(""), None);
    }
}
