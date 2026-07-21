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
