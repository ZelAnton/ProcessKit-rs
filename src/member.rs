//! [`MemberInfo`] — an enriched, point-in-time snapshot of one process in a
//! [`ProcessGroup`](crate::ProcessGroup)'s tree.

/// An enriched snapshot of one member of a [`ProcessGroup`](crate::ProcessGroup)
/// — its pid plus best-effort metadata (parent pid, image name, start time).
///
/// Produced two ways, both filling the same fields the same way: by
/// [`ProcessGroup::members_info`](crate::ProcessGroup::members_info) — the
/// metadata-carrying companion to [`members`](crate::ProcessGroup::members) (which
/// returns bare pids) — for a *member* of a group, and by the free-standing
/// [`process_info`](crate::process_info) query for an **arbitrary** pid the caller
/// holds *outside* any group. From `members_info`, *which* processes appear follows
/// the **same** platform matrix as `members` — the whole tree on Windows and
/// Linux-cgroup, the tracked group *leaders* on the POSIX process-group fallback
/// (macOS/BSD and Linux without a usable cgroup). The enriching fields beyond
/// [`pid`](Self::pid) are each independently `Option` and are `None` wherever the
/// platform can't report them — never a fabricated value.
///
/// # Field availability by platform
///
/// | field                        | Windows | Linux (cgroup / fallback) | macOS  | the BSDs |
/// |------------------------------|---------|---------------------------|--------|----------|
/// | [`pid`](Self::pid)           | yes     | yes                       | yes    | yes      |
/// | [`ppid`](Self::ppid)         | yes     | yes                       | yes    | `None`   |
/// | [`exe_name`](Self::exe_name) | yes     | yes                       | yes    | `None`   |
/// | [`start_time`](Self::start_time) | yes | yes                    | yes    | `None`   |
///
/// On the "bare" BSDs the crate wires up no per-process introspection (see the
/// note on [`start_time`](Self::start_time) for why), so every enriching field is
/// honestly `None` while the pid is still reported — that is a correct result, not
/// an error.
///
/// # No command line
///
/// The raw argv / environment of a member is **deliberately never** included, on
/// any platform: a command line routinely carries secrets, and redaction or
/// hashing is a policy the *consumer* must own — the same "never log argv/env"
/// stance the crate takes in its `tracing` output. This will not change.
///
/// # Racing a member that exits
///
/// The list is a point-in-time snapshot taken per pid: if a process exits between
/// when its pid is enumerated and when its metadata is read, that pid is simply
/// **omitted** from the returned `Vec` — a vanished member is never reported with
/// fabricated fields, and its disappearance never fails the whole call. See
/// [`ProcessGroup::members_info`](crate::ProcessGroup::members_info) for the exact
/// error contract.
///
/// Non-exhaustive and accessor-only: a read-only snapshot the crate produces, so
/// new metadata can be added without a breaking change, and each field is exposed
/// through a method — documenting its own platform caveats — rather than a public
/// struct field.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberInfo {
    pid: u32,
    ppid: Option<u32>,
    exe_name: Option<String>,
    start_time: Option<u64>,
}

impl MemberInfo {
    /// Assemble one snapshot record. Called only by the platform backends, which
    /// fill each field to whatever the OS could report (`None` where it can't).
    pub(crate) fn new(
        pid: u32,
        ppid: Option<u32>,
        exe_name: Option<String>,
        start_time: Option<u64>,
    ) -> Self {
        Self {
            pid,
            ppid,
            exe_name,
            start_time,
        }
    }

    /// The member's process id.
    ///
    /// Always present (it is the key the record is built around). Point-in-time,
    /// exactly like a pid from [`members`](crate::ProcessGroup::members): the
    /// process may exit immediately afterwards, and the number is only as stable
    /// as the OS's reuse policy — pair it with [`start_time`](Self::start_time) to
    /// tell a recycled number apart from the original process.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// The member's parent process id, or `None` where the platform can't report
    /// one.
    ///
    /// - **Windows** — `th32ParentProcessID` from a `Toolhelp32` process snapshot.
    ///   Windows does not reparent orphans, so if the parent already exited this
    ///   pid may name nothing (or, after reuse, an unrelated process).
    /// - **Linux** — field 4 of `/proc/<pid>/stat`.
    /// - **macOS** — `proc_pidinfo(PROC_PIDTBSDINFO)`'s `pbi_ppid`.
    /// - **the BSDs** — always `None` (no wired-up reader).
    pub fn ppid(&self) -> Option<u32> {
        self.ppid
    }

    /// The member's image (executable) name, or `None` where the platform can't
    /// report one.
    ///
    /// A short **base name** — never a full path, and never a command line (see
    /// the type-level "No command line" note):
    /// - **Windows** — the executable file name from the `Toolhelp32` snapshot
    ///   (`szExeFile`, e.g. `worker.exe`).
    /// - **Linux** — the kernel `comm` (field 2 of `/proc/<pid>/stat`): truncated
    ///   to 15 bytes and mutable via `prctl(PR_SET_NAME)`, so it is the process's
    ///   current name (usually, but not guaranteed to be, the exec base name)
    ///   rather than a canonical path. It is read from the *same single*
    ///   `/proc/<pid>/stat` line as [`ppid`](Self::ppid) and
    ///   [`start_time`](Self::start_time), so the three describe one consistent
    ///   instant. (The full path via a `/proc/<pid>/exe` `readlink` is deliberately
    ///   not used — it needs a second syscall and is denied without ptrace-class
    ///   access after a uid change.)
    /// - **macOS** — `proc_bsdinfo::pbi_comm`, likewise a short truncated `comm`.
    /// - **the BSDs** — always `None`.
    pub fn exe_name(&self) -> Option<&str> {
        self.exe_name.as_deref()
    }

    /// The member's start-time token, or `None` where the platform can't report
    /// one — an **opaque identity anchor, not a wall-clock timestamp**.
    ///
    /// Its sole purpose is telling a recycled pid apart from the original: it is
    /// fixed at process creation and differs for a later process that reuses the
    /// number, so two snapshots whose [`pid`](Self::pid) **and** `start_time` both
    /// match name the same process instance. **Do not interpret the number, and do
    /// not compare it across platforms** — the unit and epoch are platform-specific:
    /// - **Windows** — the process-creation `FILETIME`: 100-nanosecond intervals
    ///   since 1601-01-01 UTC.
    /// - **Linux** — `/proc/<pid>/stat` field 22 (`starttime`): clock ticks
    ///   (`sysconf(_SC_CLK_TCK)`, typically 100 Hz) since system boot.
    /// - **macOS** — start time in **microseconds since the Unix epoch**
    ///   (`proc_bsdinfo`'s `pbi_start_tvsec`·10⁶ + `pbi_start_tvusec`).
    /// - **the BSDs** — always `None`: the start time lives in `kinfo_proc`,
    ///   reachable only through per-OS `sysctl(KERN_PROC)` layouts with no hosted
    ///   CI runner to verify a reader, so the crate ships none rather than an
    ///   unverifiable one (the same reason the pgroup backend reads no identity
    ///   token there).
    pub fn start_time(&self) -> Option<u64> {
        self.start_time
    }
}
