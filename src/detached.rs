//! [`DetachedChild`] — the handle returned by
//! [`Command::spawn_detached`](crate::Command::spawn_detached) for a child
//! deliberately released from this crate's kill-on-drop containment.

/// A minimal handle to a child spawned **outside** this crate's kill-on-drop
/// containment via [`Command::spawn_detached`](crate::Command::spawn_detached).
///
/// # Warning — this inverts the crate's headline guarantee
///
/// Everywhere else, `processkit` guarantees a child — and everything it spawns —
/// dies with its owner (kill-on-drop). A `DetachedChild` is the crate's **one**
/// deliberate exception: the child was launched into its own session (Unix
/// `setsid`) and is **not** assigned to this crate's containment (no Windows Job
/// Object / cgroup / process-group tracking), so **dropping this handle does
/// nothing to the child** — it keeps running, and its lifetime is now entirely
/// yours to manage.
///
/// That is why this is a **separate, non-interchangeable type**, not a
/// [`RunningProcess`](crate::RunningProcess): it deliberately offers **no**
/// `kill`, no `wait`, no timeout, no output capture, and no teardown verbs. All
/// it carries is the child's [`pid`](Self::pid). If you need any of those, you
/// need containment — use [`Command::start`](crate::Command::start) instead and
/// keep the handle.
///
/// **Not contained by *this crate* — but a *host* container may still bind it.**
/// [`spawn_detached`](crate::Command::spawn_detached) deliberately does **not**
/// break out of an external Job Object / cgroup that already contains *your*
/// process (a CI runner, a `systemd` scope, this crate's own supervisor); doing
/// so would be hostile to whoever set that containment up. So a detached child
/// escapes only *this crate's* per-run containment, not a broader host one it
/// inherits.
#[derive(Debug)]
pub struct DetachedChild {
    pid: u32,
}

impl DetachedChild {
    /// Wrap the pid of a just-spawned detached child. Constructed only by
    /// [`Command::spawn_detached`](crate::Command::spawn_detached).
    pub(crate) fn new(pid: u32) -> Self {
        Self { pid }
    }

    /// The OS process id of the detached child, as reported at spawn.
    ///
    /// Because a detached child is no longer reaped by this crate, this pid can
    /// be recycled by the OS once the child exits and is reaped elsewhere (on
    /// Unix, `init` reaps it after your process exits). Treat it as a spawn-time
    /// identifier, not a durable handle to a still-live process.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }
}
