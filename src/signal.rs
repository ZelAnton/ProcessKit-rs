//! [`Signal`] — a portable signal to broadcast to a whole process tree.

/// A signal to broadcast to every process in a
/// [`ProcessGroup`](crate::ProcessGroup) via
/// [`signal`](crate::ProcessGroup::signal).
///
/// The curated variants map to the POSIX signal of the same name on Unix. On
/// Windows only [`Kill`](Signal::Kill) is deliverable (it maps to the Job
/// Object terminate, the same hard kill as
/// [`kill_all`](crate::ProcessGroup::kill_all)); every other variant
/// yields [`Error::Unsupported`](crate::Error::Unsupported).
///
/// [`Other`](Signal::Other) is an escape hatch carrying a raw signal number on
/// Unix (e.g. `libc::SIGWINCH`); it is always unsupported on Windows.
///
/// `SIGSTOP`/`SIGCONT` are deliberately absent from the curated set — pause and
/// resume the whole tree with [`suspend`](crate::ProcessGroup::suspend) /
/// [`resume`](crate::ProcessGroup::resume), which are portable (Windows
/// included); `Signal::Other(libc::SIGSTOP)` remains available when the raw
/// signal is specifically wanted on Unix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Signal {
    /// `SIGTERM` — polite request to exit.
    Term,
    /// `SIGKILL` — unblockable kill. On Windows: terminate the Job Object.
    Kill,
    /// `SIGINT` — keyboard interrupt.
    Int,
    /// `SIGHUP` — hangup; conventionally "reload configuration".
    Hup,
    /// `SIGQUIT` — quit, typically with a core dump.
    Quit,
    /// `SIGUSR1` — user-defined.
    Usr1,
    /// `SIGUSR2` — user-defined.
    Usr2,
    /// A raw signal number, passed through verbatim (Unix only). It lands in the
    /// *signal* argument of `kill(pid, sig)` (never the pid/target), so it can only
    /// ever send a signal — it cannot retarget one at a process group.
    ///
    /// It should be a valid signal (`1..=SIGRTMAX`). Two caveats on an unusual
    /// value:
    /// - **`Other(0)` is not an error.** Signal `0` is the POSIX *existence probe*
    ///   (`kill(pid, 0)` checks whether the target exists and delivers nothing), so
    ///   it is a no-op "is it alive?" send, not `EINVAL`.
    /// - An **out-of-range** number makes the underlying `kill`/`killpg` fail
    ///   `EINVAL`, but whether that **surfaces** is backend-dependent: the Linux
    ///   **cgroup** mechanism propagates the send error, while the **process-group**
    ///   mechanism (macOS/BSD and the Linux fallback) is best-effort and *swallows*
    ///   it (the send appears to succeed, having delivered nothing). Pass a real
    ///   signal number rather than relying on either behavior.
    Other(i32),
}

#[cfg(unix)]
impl Signal {
    /// The raw POSIX signal number for this signal.
    pub(crate) fn raw(self) -> i32 {
        match self {
            Signal::Term => libc::SIGTERM,
            Signal::Kill => libc::SIGKILL,
            Signal::Int => libc::SIGINT,
            Signal::Hup => libc::SIGHUP,
            Signal::Quit => libc::SIGQUIT,
            Signal::Usr1 => libc::SIGUSR1,
            Signal::Usr2 => libc::SIGUSR2,
            Signal::Other(n) => n,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Signal;

    #[cfg(unix)]
    #[test]
    fn raw_maps_curated_variants_and_passes_other_through() {
        assert_eq!(Signal::Term.raw(), libc::SIGTERM);
        assert_eq!(Signal::Kill.raw(), libc::SIGKILL);
        assert_eq!(Signal::Int.raw(), libc::SIGINT);
        assert_eq!(Signal::Hup.raw(), libc::SIGHUP);
        assert_eq!(Signal::Quit.raw(), libc::SIGQUIT);
        assert_eq!(Signal::Usr1.raw(), libc::SIGUSR1);
        assert_eq!(Signal::Usr2.raw(), libc::SIGUSR2);
        // The escape hatch is verbatim — no mapping, no validation here.
        assert_eq!(Signal::Other(64).raw(), 64);
    }

    #[test]
    fn signal_is_copy_eq_and_hashable() {
        let sig = Signal::Hup;
        let copy = sig; // Copy
        assert_eq!(sig, copy);
        assert_ne!(Signal::Other(1), Signal::Other(2));
        let mut set = std::collections::HashSet::new();
        set.insert(Signal::Term);
        set.insert(Signal::Term);
        assert_eq!(set.len(), 1);
    }
}
