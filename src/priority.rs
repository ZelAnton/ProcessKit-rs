//! [`Priority`] — a portable process CPU-scheduling priority.

/// A portable CPU-scheduling priority for a child process (see
/// [`Command::priority`](crate::Command::priority)), mapped onto the native
/// primitive at spawn time: Unix `setpriority`/`nice` (applied through the
/// same `pre_exec` seam as [`uid`](crate::Command::uid)/[`gid`](crate::Command::gid)),
/// Windows priority class (OR'd into `creation_flags`, the same seam as
/// [`create_no_window`](crate::Command::create_no_window)).
///
/// Unlike the privilege builders, every variant here is supported on **both**
/// platform families — `setpriority` is plain POSIX (Linux, macOS, the BSDs
/// alike) and every Windows edition has all five priority classes — so
/// [`Command::priority`](crate::Command::priority) never yields
/// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Priority {
    /// Lowest scheduling priority — runs only when the system is otherwise
    /// idle. Unix: `nice(19)`. Windows: `IDLE_PRIORITY_CLASS`.
    Idle,
    /// Below the default priority — polite background work that still makes
    /// steady progress. Unix: `nice(10)`. Windows: `BELOW_NORMAL_PRIORITY_CLASS`.
    BelowNormal,
    /// The default OS priority. Unix: `nice(0)`. Windows: `NORMAL_PRIORITY_CLASS`.
    ///
    /// **Unix caveat:** this is a no-op *only* when the spawning process
    /// itself has `nice == 0`. A child normally inherits its parent's `nice`
    /// value, so setting `Normal` explicitly under a positively-niced parent
    /// (e.g. a process launched via `nice(1)` in a CI/batch scheduler) asks
    /// the kernel to *lower* the child's `nice` back to `0` — the same kind
    /// of decrease as [`AboveNormal`](Priority::AboveNormal)/[`High`](Priority::High),
    /// and subject to the same `CAP_SYS_NICE`/root requirement and
    /// [`ErrorKind::Spawn`](crate::ErrorKind::Spawn) failure mode described there.
    /// Windows is unaffected: `NORMAL_PRIORITY_CLASS` never needs a privilege
    /// check.
    Normal,
    /// Above the default priority. Unix: `nice(-5)`. Windows:
    /// `ABOVE_NORMAL_PRIORITY_CLASS`.
    ///
    /// **Unix caveat:** same privilege requirement as
    /// [`High`](Priority::High) — lowering `nice` below its inherited value
    /// needs `CAP_SYS_NICE`/root, and without it the spawn fails as
    /// [`ErrorKind::Spawn`](crate::ErrorKind::Spawn) rather than silently applying a
    /// smaller increase.
    AboveNormal,
    /// Highest ordinary (non-real-time) priority.
    ///
    /// **Unix caveat:** lowering `nice` below its inherited value needs
    /// `CAP_SYS_NICE` (Linux) or an equivalent privilege elsewhere; without it
    /// the OS refuses the change and the spawn fails as
    /// [`ErrorKind::Spawn`](crate::ErrorKind::Spawn), exactly like any other rejected
    /// `pre_exec` hook — it is never silently downgraded to a lower priority.
    /// Windows needs no special privilege for `HIGH_PRIORITY_CLASS`.
    /// Unix: `nice(-10)`. Windows: `HIGH_PRIORITY_CLASS`.
    High,
}

#[cfg(unix)]
impl Priority {
    /// The `nice` value applied via `setpriority(PRIO_PROCESS, 0, _)` in the
    /// child's `pre_exec` hook.
    pub(crate) fn nice_value(self) -> i32 {
        match self {
            Priority::Idle => 19,
            Priority::BelowNormal => 10,
            Priority::Normal => 0,
            Priority::AboveNormal => -5,
            Priority::High => -10,
        }
    }
}

#[cfg(windows)]
impl Priority {
    /// The Windows process-creation priority-class flag, OR'd into the
    /// command's `creation_flags`.
    pub(crate) fn creation_flag(self) -> u32 {
        use windows_sys::Win32::System::Threading::{
            ABOVE_NORMAL_PRIORITY_CLASS, BELOW_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS,
            IDLE_PRIORITY_CLASS, NORMAL_PRIORITY_CLASS,
        };
        match self {
            Priority::Idle => IDLE_PRIORITY_CLASS,
            Priority::BelowNormal => BELOW_NORMAL_PRIORITY_CLASS,
            Priority::Normal => NORMAL_PRIORITY_CLASS,
            Priority::AboveNormal => ABOVE_NORMAL_PRIORITY_CLASS,
            Priority::High => HIGH_PRIORITY_CLASS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Priority;

    #[cfg(unix)]
    #[test]
    fn nice_value_maps_every_variant() {
        assert_eq!(Priority::Idle.nice_value(), 19);
        assert_eq!(Priority::BelowNormal.nice_value(), 10);
        assert_eq!(Priority::Normal.nice_value(), 0);
        assert_eq!(Priority::AboveNormal.nice_value(), -5);
        assert_eq!(Priority::High.nice_value(), -10);
    }

    #[cfg(windows)]
    #[test]
    fn creation_flag_maps_every_variant() {
        use windows_sys::Win32::System::Threading::{
            ABOVE_NORMAL_PRIORITY_CLASS, BELOW_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS,
            IDLE_PRIORITY_CLASS, NORMAL_PRIORITY_CLASS,
        };
        assert_eq!(Priority::Idle.creation_flag(), IDLE_PRIORITY_CLASS);
        assert_eq!(
            Priority::BelowNormal.creation_flag(),
            BELOW_NORMAL_PRIORITY_CLASS
        );
        assert_eq!(Priority::Normal.creation_flag(), NORMAL_PRIORITY_CLASS);
        assert_eq!(
            Priority::AboveNormal.creation_flag(),
            ABOVE_NORMAL_PRIORITY_CLASS
        );
        assert_eq!(Priority::High.creation_flag(), HIGH_PRIORITY_CLASS);
    }

    #[test]
    fn priority_is_copy_eq_and_debug() {
        let p = Priority::Idle;
        let copy = p; // Copy
        assert_eq!(p, copy);
        assert_ne!(Priority::Idle, Priority::High);
        assert!(format!("{:?}", Priority::Normal).contains("Normal"));
    }
}
