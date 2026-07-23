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
/// [`Error::Unsupported`](crate::Error::Unsupported).
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
    /// [`Error::Spawn`](crate::Error::Spawn) failure mode described there.
    /// Windows is unaffected: `NORMAL_PRIORITY_CLASS` never needs a privilege
    /// check.
    Normal,
    /// Above the default priority. Unix: `nice(-5)`. Windows:
    /// `ABOVE_NORMAL_PRIORITY_CLASS`.
    ///
    /// **Unix caveat:** same privilege requirement as
    /// [`High`](Priority::High) — lowering `nice` below its inherited value
    /// needs `CAP_SYS_NICE`/root, and without it the spawn fails as
    /// [`Error::Spawn`](crate::Error::Spawn) rather than silently applying a
    /// smaller increase.
    AboveNormal,
    /// Highest ordinary (non-real-time) priority.
    ///
    /// **Unix caveat:** lowering `nice` below its inherited value needs
    /// `CAP_SYS_NICE` (Linux) or an equivalent privilege elsewhere; without it
    /// the OS refuses the change and the spawn fails as
    /// [`Error::Spawn`](crate::Error::Spawn), exactly like any other rejected
    /// `pre_exec` hook — it is never silently downgraded to a lower priority.
    /// Windows needs no special privilege for `HIGH_PRIORITY_CLASS`.
    /// Unix: `nice(-10)`. Windows: `HIGH_PRIORITY_CLASS`.
    High,
}

impl Priority {
    /// This priority's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"idle"`, `"below_normal"`, `"normal"`,
    /// `"above_normal"`, `"high"`) that is part of the crate's compatibility
    /// surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back —
    /// the direction a config file or CLI flag choosing a priority needs.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            Priority::Idle => "idle",
            Priority::BelowNormal => "below_normal",
            Priority::Normal => "normal",
            Priority::AboveNormal => "above_normal",
            Priority::High => "high",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `Priority` — the
    /// direction a config value or CLI flag selecting a priority needs.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so an unknown
    /// value fails loudly rather than defaulting to some priority the caller
    /// never asked for. Round-trips with [`name`](Self::name):
    /// `Priority::from_name(p.name()) == Some(p)` for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "idle" => Some(Priority::Idle),
            "below_normal" => Some(Priority::BelowNormal),
            "normal" => Some(Priority::Normal),
            "above_normal" => Some(Priority::AboveNormal),
            "high" => Some(Priority::High),
            _ => None,
        }
    }
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

    const ALL: &[Priority] = &[
        Priority::Idle,
        Priority::BelowNormal,
        Priority::Normal,
        Priority::AboveNormal,
        Priority::High,
    ];

    #[test]
    fn name_pins_each_variant() {
        assert_eq!(Priority::Idle.name(), "idle");
        assert_eq!(Priority::BelowNormal.name(), "below_normal");
        assert_eq!(Priority::Normal.name(), "normal");
        assert_eq!(Priority::AboveNormal.name(), "above_normal");
        assert_eq!(Priority::High.name(), "high");
    }

    #[test]
    fn name_from_name_round_trips_every_variant() {
        for &p in ALL {
            assert_eq!(Priority::from_name(p.name()), Some(p));
        }
    }

    #[test]
    fn from_name_rejects_unknown_without_defaulting() {
        assert_eq!(Priority::from_name("BelowNormal"), None);
        assert_eq!(Priority::from_name("below-normal"), None);
        assert_eq!(Priority::from_name(""), None);
        assert_eq!(Priority::from_name("highest"), None);
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
