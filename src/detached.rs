//! [`DetachedChild`] — the handle returned by
//! [`Command::spawn_detached`](crate::Command::spawn_detached) for a child
//! deliberately released from this crate's kill-on-drop containment.

#[cfg(unix)]
mod reaper {
    use std::io;
    use std::process::Child;
    use std::sync::OnceLock;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    // One manager owns every detached `Child`. Keeping the `Child` in this
    // process is what makes it the only path allowed to consume the child's
    // wait status; the caller only ever receives the stable spawn-time pid.
    static SENDER: OnceLock<Result<Sender<Child>, String>> = OnceLock::new();

    fn sender() -> io::Result<&'static Sender<Child>> {
        match SENDER.get_or_init(|| {
            let (sender, receiver) = mpsc::channel();
            std::thread::Builder::new()
                .name("processkit-detached-reaper".into())
                .spawn(move || reap_loop(receiver))
                .map(|_| sender)
                .map_err(|source| format!("could not start detached reaper: {source}"))
        }) {
            Ok(sender) => Ok(sender),
            Err(message) => Err(io::Error::other(message.clone())),
        }
    }

    /// Start the manager before spawning the child, so a thread-resource failure
    /// cannot leave an already-launched child without a reaping owner.
    pub(super) fn prepare() -> io::Result<()> {
        sender().map(|_| ())
    }

    /// Wait for a child without ever dropping it after a failed wait attempt.
    ///
    /// `Child::wait` normally retries interruption internally, but retaining the
    /// handle here also covers a handoff failure that leaves this thread as the
    /// last possible owner. The child is dropped only after a successful wait.
    fn wait_until_reaped(mut child: Child) -> Option<io::Error> {
        let mut first_error = None;
        loop {
            match child.wait() {
                Ok(_) => return first_error,
                Err(source) => {
                    first_error.get_or_insert(source);
                    std::thread::sleep(POLL_INTERVAL);
                }
            }
        }
    }

    /// Preserve a handoff error after synchronously reclaiming the wait owner.
    ///
    /// The fallback intentionally does not return until `child.wait()` succeeds:
    /// returning while retaining no `Child` would make Unix process reaping a
    /// best-effort operation and could leave a zombie behind. A transient wait
    /// error is included in the returned error after the child is eventually
    /// reaped, so callers do not mistake a failed handoff for a successful spawn.
    fn finish_failed_handoff(child: Child, handoff_error: io::Error) -> io::Error {
        match wait_until_reaped(child) {
            None => handoff_error,
            Some(wait_error) => io::Error::other(format!(
                "{handoff_error}; fallback wait initially failed: {wait_error}"
            )),
        }
    }

    /// Transfer the only `Child` handle to the manager. The synchronous fallback
    /// is defensive: `prepare` makes this path unreachable unless the manager
    /// unexpectedly stops after initialization, but it still prevents a zombie
    /// if that invariant is ever broken.
    pub(super) fn handoff(child: Child) -> io::Result<()> {
        let sender = match sender() {
            Ok(sender) => sender,
            Err(source) => {
                return Err(finish_failed_handoff(child, source));
            }
        };
        match sender.send(child) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(child)) => Err(finish_failed_handoff(
                child,
                io::Error::other("detached reaper stopped unexpectedly"),
            )),
        }
    }

    fn reap_loop(receiver: Receiver<Child>) {
        let mut children = Vec::new();
        loop {
            if children.is_empty() {
                match receiver.recv() {
                    Ok(child) => children.push(child),
                    Err(_) => return,
                }
            } else {
                match receiver.recv_timeout(POLL_INTERVAL) {
                    Ok(child) => children.push(child),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // The sender is process-global and normally never drops;
                        // if that invariant changes, finish every owned child
                        // before the manager exits.
                        for child in children {
                            let _ = wait_until_reaped(child);
                        }
                        return;
                    }
                }
            }

            let mut index = 0;
            while index < children.len() {
                match children[index].try_wait() {
                    Ok(Some(_)) => {
                        // `try_wait` has already reaped the child on Unix. Calling
                        // `wait` on this same owner observes that completed status
                        // and makes the ownership handoff explicit to Clippy.
                        let _ = children.swap_remove(index).wait();
                    }
                    Ok(None) | Err(_) => {
                        // A transient wait error must not transfer ownership or
                        // drop the handle; retrying keeps this manager the sole
                        // reaping path.
                        index += 1;
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
pub(crate) fn prepare_reaper() -> std::io::Result<()> {
    reaper::prepare()
}

#[cfg(unix)]
pub(crate) fn handoff_reaper(child: std::process::Child) -> std::io::Result<()> {
    reaper::handoff(child)
}

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
/// [`RunningProcess`](crate::RunningProcess): it deliberately exposes **no**
/// public `kill`, `wait`, timeout, output capture, or teardown/control verbs. All
/// it carries is the child's [`pid`](Self::pid). On Unix, a private background
/// reaper owns the OS child handle and collects its exit status; this public
/// handle still exposes none of those operations. If you need any of those, you
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
    /// The pid can be recycled by the OS once the child exits and the Unix
    /// background reaper (or the platform's normal process cleanup) has
    /// collected it. Treat it as a spawn-time identifier, not a durable handle
    /// to a still-live process.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }
}
