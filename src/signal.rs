//! [`Signal`] — a portable signal to broadcast to a whole process tree.

/// A signal to broadcast to every process in a
/// [`ProcessGroup`](crate::ProcessGroup) via
/// [`signal`](crate::ProcessGroup::signal).
///
/// The curated variants map to the POSIX signal of the same name on Unix. On
/// Windows, [`Kill`](Signal::Kill) maps to the atomic Job Object terminate (the
/// same hard kill as [`kill_all`](crate::ProcessGroup::kill_all)), and
/// [`Int`](Signal::Int) / [`Term`](Signal::Term) are a best-effort **soft close**:
/// a console `CTRL_BREAK` to a child opted into
/// [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break),
/// plus `WM_CLOSE` to every top-level window a live member owns. Those two yield
/// [`Error::Unsupported`](crate::Error::Unsupported) only when the group has no such
/// target; every other variant is always unsupported on Windows.
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
    ///   it is a no-op "is it alive?" send, not `EINVAL`. On **both** POSIX backends
    ///   a [`signal(Other(0))`](crate::ProcessGroup::signal) over a group with live
    ///   members returns `Ok` **having delivered nothing** — the `Ok` reports "the
    ///   probe reached a signalable target", never "a signal was delivered".
    /// - An **out-of-range** number makes the underlying `kill`/`killpg` fail
    ///   `EINVAL`, and that failure is now surfaced as
    ///   [`Error::Io`](crate::Error::Io) on **every** Unix backend — the Linux
    ///   **cgroup** mechanism and the **process-group** mechanism (macOS/BSD and the
    ///   Linux fallback) agree, so a bad number no longer silently "succeeds" on one
    ///   and fails on the other. Still, pass a real signal number rather than
    ///   leaning on the `EINVAL`.
    Other(i32),
}

impl Signal {
    /// This signal's **stable machine identifier** for a curated variant: a
    /// short, lowercase `snake_case` string (`"term"`, `"kill"`, `"int"`,
    /// `"hup"`, `"quit"`, `"usr1"`, `"usr2"`), part of the crate's
    /// compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** curated variant
    /// gets a **new** identifier, and an existing identifier is **never
    /// renamed** without a major release.
    ///
    /// Returns `None` for the [`Other`](Signal::Other) escape hatch: a raw
    /// signal number has no curated name, so render (and parse) its inner `i32`
    /// directly rather than inventing a placeholder that could not round-trip.
    /// [`from_name`](Self::from_name) parses the curated identifiers back.
    pub fn name(&self) -> Option<&'static str> {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a decision about its identifier.
        match self {
            Signal::Term => Some("term"),
            Signal::Kill => Some("kill"),
            Signal::Int => Some("int"),
            Signal::Hup => Some("hup"),
            Signal::Quit => Some("quit"),
            Signal::Usr1 => Some("usr1"),
            Signal::Usr2 => Some("usr2"),
            Signal::Other(_) => None,
        }
    }

    /// Parse a curated [`name`](Self::name) identifier back into a `Signal` —
    /// the direction a config value or CLI flag naming a signal needs.
    ///
    /// Covers the curated variants only; returns `None` for any other string —
    /// an honest miss, never a silent default. The [`Other`](Signal::Other)
    /// escape hatch is *not* reached through a name (it has none): parse a raw
    /// signal number into an `i32` and construct `Signal::Other(n)` yourself.
    /// Round-trips with [`name`](Self::name): for every curated variant,
    /// `Signal::from_name(s.name().unwrap()) == Some(s)`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "term" => Some(Signal::Term),
            "kill" => Some(Signal::Kill),
            "int" => Some(Signal::Int),
            "hup" => Some(Signal::Hup),
            "quit" => Some(Signal::Quit),
            "usr1" => Some(Signal::Usr1),
            "usr2" => Some(Signal::Usr2),
            _ => None,
        }
    }
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

    /// Every curated (named) variant, for round-trip coverage. `Other` is
    /// excluded deliberately — it carries a raw number and has no curated name.
    const CURATED: &[Signal] = &[
        Signal::Term,
        Signal::Kill,
        Signal::Int,
        Signal::Hup,
        Signal::Quit,
        Signal::Usr1,
        Signal::Usr2,
    ];

    #[test]
    fn name_pins_each_curated_variant() {
        assert_eq!(Signal::Term.name(), Some("term"));
        assert_eq!(Signal::Kill.name(), Some("kill"));
        assert_eq!(Signal::Int.name(), Some("int"));
        assert_eq!(Signal::Hup.name(), Some("hup"));
        assert_eq!(Signal::Quit.name(), Some("quit"));
        assert_eq!(Signal::Usr1.name(), Some("usr1"));
        assert_eq!(Signal::Usr2.name(), Some("usr2"));
    }

    #[test]
    fn other_has_no_curated_name() {
        // The raw-number escape hatch reports no name (render its i32 instead),
        // and no name parses back to it.
        assert_eq!(Signal::Other(9).name(), None);
        assert_eq!(Signal::Other(0).name(), None);
    }

    #[test]
    fn name_from_name_round_trips_every_curated_variant() {
        for &s in CURATED {
            let name = s.name().expect("curated variant has a name");
            assert_eq!(Signal::from_name(name), Some(s));
        }
    }

    #[test]
    fn from_name_rejects_unknown_and_other_without_defaulting() {
        assert_eq!(Signal::from_name("Term"), None);
        assert_eq!(Signal::from_name("sigterm"), None);
        assert_eq!(Signal::from_name("other"), None);
        assert_eq!(Signal::from_name("9"), None);
        assert_eq!(Signal::from_name(""), None);
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
