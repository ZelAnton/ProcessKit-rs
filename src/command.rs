//! [`Command`] — a builder describing a process to run.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use encoding_rs::Encoding;

use crate::buffer::{
    CapturePolicy, LineTerminator, OutputBufferPolicy, OutputStream, SharedCapturePolicy, StdioMode,
};
use crate::detached::DetachedChild;
use crate::error::{Error, ErrorReason, Result};
use crate::parent_death::ParentDeathCleanup;
use crate::pump::StreamConfig;
use crate::result::ProcessResult;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::runner::{JobRunner, ProcessRunnerExt};
use crate::running::RunningProcess;
use crate::stdin::Stdin;

/// A command's timeout as three type-level cases, so the "explicitly unbounded"
/// state is a variant rather than a `bool` maintained next to an
/// `Option<Duration>` (the old pair could otherwise encode the nonsensical
/// "bounded *and* explicitly unbounded"). Private — the surface is the
/// [`timeout`](Command::timeout) / [`no_timeout`](Command::no_timeout) /
/// [`timeout_opt`](Command::timeout_opt) verbs and the
/// [`configured_timeout`](Command::configured_timeout) accessor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Timeout {
    /// No timeout set — a client-wide
    /// [`default_timeout`](crate::CliClient::default_timeout) may gap-fill a
    /// deadline (see [`Command::accepts_default_timeout`]).
    Unset,
    /// Explicitly unbounded ([`Command::no_timeout`]): no deadline, and opts out
    /// of a client `default_timeout` gap-fill.
    Unbounded,
    /// A deadline of this duration ([`Command::timeout`]).
    After(Duration),
}

/// A child-owned stdout/stderr file destination. This is deliberately separate
/// from [`StdioMode`]: paths make the connection state non-`Copy`, while the
/// mode enum is a small, copyable choice used throughout the streaming API.
#[derive(Clone, Debug)]
struct FileRedirect {
    path: PathBuf,
    append: bool,
}

impl FileRedirect {
    fn truncate(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            append: false,
        }
    }

    fn append(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            append: true,
        }
    }

    fn open(&self) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(self.append)
            .truncate(!self.append)
            .open(&self.path)
    }
}

impl Timeout {
    /// The deadline duration, if one is set (`After`). `None` for both `Unset`
    /// and the explicitly `Unbounded` state — neither imposes a deadline.
    fn as_duration(self) -> Option<Duration> {
        match self {
            Timeout::After(d) => Some(d),
            Timeout::Unset | Timeout::Unbounded => None,
        }
    }
}

/// A description of a child process to launch: program, arguments, working
/// directory, environment, stdin source, and an optional timeout.
///
/// A single builder for everything a run needs. Build it, then either drive it
/// to completion with a
/// helper ([`output_string`](Self::output_string), [`run`](Self::run), …) or
/// start it via a [`ProcessRunner`](crate::ProcessRunner) for streaming/shared
/// groups.
#[derive(Clone)]
#[must_use = "a Command does nothing until it is run or started"]
pub struct Command {
    program: OsString,
    args: Vec<OsString>,
    cwd: Option<OsString>,
    /// Directories to probe (in priority order) before the system `PATH` when
    /// `program` is a bare name — see [`Self::prefer_local`].
    prefer_local: Vec<PathBuf>,
    envs: Vec<(OsString, Option<OsString>)>,
    env_clear: bool,
    stdin: Option<Stdin>,
    keep_stdin_open: bool,
    /// Hand the child the parent's own standard input (`Stdio::inherit`) instead
    /// of a pipe — see [`Self::inherit_stdin`]. Mutually exclusive with
    /// [`keep_stdin_open`](Self::keep_stdin_open) and a configured
    /// [`stdin`](Self::stdin) source; the conflict is rejected at the launch
    /// boundary (see `runner::take_stdin_for_run`).
    stdin_inherit: bool,
    /// Exempt this stage from pipefail attribution (see [`Self::unchecked_in_pipe`]).
    unchecked: bool,
    /// The timeout state — unset, explicitly unbounded, or a deadline (see
    /// [`Timeout`]). This three-case type replaces the old `Option<Duration>` +
    /// `no_timeout: bool` pair, so "explicitly unbounded" is modeled at the type
    /// level instead of via a setter-maintained invariant.
    timeout: Timeout,
    /// Maximum silence between stdout/stderr reads before the run is torn down.
    inactivity_timeout: Option<Duration>,
    /// Grace window after the deadline before `SIGKILL`; its presence makes the
    /// timeout graceful (see [`Self::timeout_grace`]).
    timeout_grace: Option<Duration>,
    /// Signal sent at the start of a graceful timeout (default `SIGTERM`).
    #[cfg(feature = "process-control")]
    timeout_signal: Option<crate::Signal>,
    /// Exit codes treated as success by the checking verbs (`run`/`run_unit`/
    /// `checked` via [`ProcessResult::ensure_success`]). `None` accepts only `0`.
    ok_codes: Option<Vec<i32>>,
    /// Per-stream pump config — decode encoding, optional per-line handler,
    /// optional tee sink, and line-terminator mode — one value per stream instead
    /// of four parallel `stdout_*`/`stderr_*` field pairs. See [`StreamConfig`].
    /// The connection mode (`Piped`/`Inherit`/`Null`) stays separate below.
    stdout_config: StreamConfig,
    stderr_config: StreamConfig,
    stdout_mode: StdioMode,
    stderr_mode: StdioMode,
    /// Child-owned file destinations stay separate from `StdioMode` so that
    /// enum remains `Copy`; their paths are opened only at the launch boundary.
    stdout_file: Option<FileRedirect>,
    stderr_file: Option<FileRedirect>,
    output_buffer: OutputBufferPolicy,
    /// Optional consumer [`CapturePolicy`] redaction/transform seam, applied to
    /// each decoded line of **both** streams just before it enters the capture
    /// backlog. A whole-command knob (like [`output_buffer`](Self::output_buffer)),
    /// injected into each stream's [`StreamConfig`] by
    /// [`stdout_config`](Self::stdout_config)/[`stderr_config`](Self::stderr_config)
    /// with the matching [`OutputStream`] tag. `None` leaves capture verbatim.
    capture_policy: Option<SharedCapturePolicy>,
    retry: Option<RetryConfig>,
    /// `Some` once `inherit_env` was called (even with an empty list): clear
    /// the inherited environment and copy only these parent vars.
    inherit_env: Option<Vec<OsString>>,
    uid: Option<u32>,
    gid: Option<u32>,
    /// Supplementary group ids to set (Unix privilege drop); `Some` replaces the
    /// inherited set. See [`Self::groups`].
    groups: Option<Vec<u32>>,
    setsid: bool,
    /// CPU-scheduling priority (see [`Self::priority`]). Supported on both
    /// Unix (`nice`/`setpriority`) and Windows (priority class) — never
    /// gated as `Unsupported`.
    priority: Option<crate::Priority>,
    /// Logical CPU indices the child and its descendants initially inherit (see
    /// [`Self::cpu_affinity`]). Linux/Windows only.
    cpu_affinity: Option<Vec<usize>>,
    /// Linux I/O-scheduling priority (see [`Self::io_priority`]). Unsupported
    /// on every other target.
    io_priority: Option<crate::IoPriority>,
    /// File-mode creation mask for the child (Unix `umask(2)`, see
    /// [`Self::umask`]). Unix-only — `Unsupported` elsewhere.
    umask: Option<u32>,
    /// Kill the direct child if this process dies abruptly (see
    /// [`Self::kill_on_parent_death`]).
    kill_on_parent_death: bool,
    /// Extra Windows process-creation flags (e.g. `CREATE_NO_WINDOW`), OR'd
    /// into the spawn by the Command-driven launch paths.
    creation_flags_extra: u32,
    /// Opt in to the Windows graceful-teardown console-CTRL path (see
    /// [`Self::windows_graceful_ctrl_break`]): spawn the direct child
    /// `CREATE_NEW_PROCESS_GROUP` and send it `CTRL_BREAK` before the grace
    /// window. A no-op off Windows (Unix already has a real signal tier).
    windows_graceful_ctrl_break: bool,
    /// Spawn the child under a pseudo-terminal instead of three pipes (see
    /// [`Self::use_pty`]). Off by default; only present with the `pty` feature.
    #[cfg(feature = "pty")]
    use_pty: bool,
    /// The initial pseudo-terminal window size `(cols, rows)` for a
    /// [`use_pty`](Self::use_pty) spawn (see [`Self::pty_size`]). `None` falls back
    /// to the backend default (80×24). Read only on the PTY launch path — a
    /// documented no-op without `use_pty`.
    #[cfg(feature = "pty")]
    pty_size: Option<(u16, u16)>,
    /// Whether the caller explicitly chose stdout's / stderr's
    /// [`LineTerminator`] (via `line_terminator`/`stdout_line_terminator`/
    /// `stderr_line_terminator`). Only consulted by the PTY auto-default in
    /// [`stdout_config`](Self::stdout_config)/[`stderr_config`](Self::stderr_config):
    /// [`use_pty`](Self::use_pty) makes the *effective* terminator default to
    /// [`LineTerminator::CarriageReturn`] — but only while this is `false`, so an
    /// explicit choice (including `Newline`) always wins, order-independently.
    /// Feature-gated because it exists solely for that PTY resolution.
    #[cfg(feature = "pty")]
    stdout_terminator_explicit: bool,
    #[cfg(feature = "pty")]
    stderr_terminator_explicit: bool,
    /// When cancelled, the run's tree is killed and every consuming path
    /// resolves to `ErrorReason::Cancelled`. Cheap to clone (internally `Arc`'d), so
    /// a `Command` clone — including each `Pipeline` stage and each
    /// `Supervisor` incarnation — shares the same cancel state.
    cancel_token: Option<tokio_util::sync::CancellationToken>,
}

impl Command {
    /// Start a command for `program` (resolved on `PATH`).
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            args: Vec::new(),
            cwd: None,
            prefer_local: Vec::new(),
            envs: Vec::new(),
            env_clear: false,
            stdin: None,
            keep_stdin_open: false,
            stdin_inherit: false,
            unchecked: false,
            timeout: Timeout::Unset,
            inactivity_timeout: None,
            timeout_grace: None,
            #[cfg(feature = "process-control")]
            timeout_signal: None,
            ok_codes: None,
            stdout_config: StreamConfig::new(),
            stderr_config: StreamConfig::new(),
            stdout_mode: StdioMode::Piped,
            stderr_mode: StdioMode::Piped,
            stdout_file: None,
            stderr_file: None,
            output_buffer: OutputBufferPolicy::unbounded(),
            capture_policy: None,
            retry: None,
            inherit_env: None,
            uid: None,
            gid: None,
            groups: None,
            setsid: false,
            priority: None,
            cpu_affinity: None,
            io_priority: None,
            umask: None,
            kill_on_parent_death: false,
            creation_flags_extra: 0,
            windows_graceful_ctrl_break: false,
            #[cfg(feature = "pty")]
            use_pty: false,
            #[cfg(feature = "pty")]
            pty_size: None,
            #[cfg(feature = "pty")]
            stdout_terminator_explicit: false,
            #[cfg(feature = "pty")]
            stderr_terminator_explicit: false,
            cancel_token: None,
        }
    }

    /// Append a single argument.
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Append several arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|a| a.as_ref().to_os_string()));
        self
    }

    /// Set the working directory for the child process.
    ///
    /// **Relative-path programs and `current_dir`:** if the program passed to
    /// [`Command::new`] is a relative path (e.g. `"./tool"` or `"../bin/x"`),
    /// it is resolved against the *caller's* current directory at spawn time —
    /// not against the directory set here. Use an absolute path for the program
    /// when combining `current_dir` with a relative-path executable.
    /// A bare-name program resolved via [`prefer_local`](Self::prefer_local)
    /// doesn't share this footgun: a relative `prefer_local` directory is
    /// always turned into an absolute path before being handed to the OS, so
    /// it can't be reinterpreted against the directory set here.
    pub fn current_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.cwd = Some(dir.as_ref().as_os_str().to_os_string());
        self
    }

    /// Probe `dir` for the program **before** the system `PATH` — for a
    /// locally-installed tool (a project's `node_modules/.bin`, `target/debug`,
    /// a vendored toolchain) that a caller wants to run by bare name without
    /// hand-rolling a `PATH` override.
    ///
    /// Repeated calls **accumulate**, in priority order: the directory from the
    /// first call is probed first, then the second, and so on, with the system
    /// `PATH` tried last as the final fallback. Resolution reuses the exact
    /// same PATHEXT-aware lookup as the `PATH` search (the same `probe_dir`
    /// helper — no separate copy), so a `.exe`/`.cmd`/`.bat` on Windows is found
    /// exactly as it would be on `PATH`.
    ///
    /// **Only affects a bare-name `program`.** If the program passed to
    /// [`Command::new`] is a path — absolute, or relative with a separator
    /// (`"./tool"`, `"../bin/x"`) — `prefer_local` has no effect and the
    /// existing contract holds unchanged: such a program is never looked up
    /// here or on `PATH`.
    ///
    /// **Does not touch the child's own `PATH`.** This only changes *where the
    /// parent looks* to resolve the program for this one launch — the `PATH`
    /// the child sees in its own environment (via inheritance, [`env`](Self::env),
    /// or [`inherit_env`](Self::inherit_env)) is neither rewritten nor extended.
    /// When the program is found under one of these directories, the child is
    /// simply spawned via that resolved absolute path instead of the bare name
    /// (so the OS never has to search anything); a grandchild the program itself
    /// spawns does not inherit this reach.
    ///
    /// A relative `dir` here (e.g. `"./node_modules/.bin"`) is probed against
    /// the *process's* actual current directory, not against whatever is set
    /// via [`current_dir`](Self::current_dir) — and the resulting match is
    /// always made absolute (by joining it onto that same current directory)
    /// before being handed to the OS, so it can never be reinterpreted against
    /// the child's own working directory once [`current_dir`](Self::current_dir)
    /// is also set.
    ///
    /// If resolution fails everywhere, [`ErrorReason::NotFound`]'s
    /// `searched` includes these directories — first, in priority order — ahead
    /// of the `PATH` directories, so the diagnostic doesn't hide that they were
    /// checked too.
    pub fn prefer_local(mut self, dir: impl Into<PathBuf>) -> Self {
        self.prefer_local.push(dir.into());
        self
    }

    /// Set an environment variable for the child. To *remove* an inherited
    /// variable, use [`env_remove`](Self::env_remove) — `value` here is always a
    /// value, never `None`.
    ///
    /// **Secrets.** env is the right channel for a token or password: env *values*
    /// are redacted from this command's `Debug` (only names appear) and are never
    /// emitted via `tracing` or in cassette recordings, and the child receives the
    /// value intact. Prefer it — or [`stdin`](Self::stdin), the strongest — over a
    /// command-line [`arg`](Self::arg): argv is reduced to a count in `Debug` too,
    /// but is world-readable through the OS process table (`/proc/<pid>/cmdline`,
    /// `ps`) and is exposed verbatim by [`command_line`](Self::command_line) and
    /// cassette recording. An env value is *not* world-readable, but is still
    /// visible to the same user and root via `/proc/<pid>/environ` and is inherited
    /// by every descendant process; `stdin` exposes the secret to neither.
    ///
    /// processkit deliberately ships **no** `Secret` wrapper type — pair `env` with
    /// the [`secrecy`]/[`zeroize`] crates for a typed, memory-scrubbed secret at
    /// your own call sites and pass the exposed value here. Scrubbing only covers
    /// *your* copy: once passed, processkit holds a plain `OsString` for the
    /// command's lifetime and the child receives cleartext (a core dump can expose
    /// either). For a secret recomputed per *operation* — resolved when each command
    /// is built and reused across that command's retries, **not** regenerated per
    /// attempt — use
    /// [`CliClient::default_env_fn`](crate::CliClient::default_env_fn).
    ///
    /// [`secrecy`]: https://crates.io/crates/secrecy
    /// [`zeroize`]: https://crates.io/crates/zeroize
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.envs.push((
            key.as_ref().to_os_string(),
            Some(value.as_ref().to_os_string()),
        ));
        self
    }

    /// Remove an environment variable inherited from the parent.
    pub fn env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.envs.push((key.as_ref().to_os_string(), None));
        self
    }

    /// Set multiple environment variables at once. Order is preserved; later
    /// entries win on a duplicated key.
    ///
    /// ```
    /// use processkit::Command;
    /// Command::new("tool").envs([("FOO", "1"), ("BAR", "2")]);
    /// ```
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.envs.extend(
            vars.into_iter()
                .map(|(k, v)| (k.as_ref().to_os_string(), Some(v.as_ref().to_os_string()))),
        );
        self
    }

    /// Clear all inherited environment variables before applying any set here.
    ///
    /// **Opts out of client env defaults:** a command that clears its environment
    /// is treated as having taken full control of it, so a
    /// [`CliClient`](crate::CliClient)'s [`default_env`](crate::CliClient::default_env)/
    /// [`default_env_fn`](crate::CliClient::default_env_fn) is **not** gap-filled
    /// into it (a client default would otherwise pierce the clean slate). Set any
    /// var you still want with an explicit [`env`](Self::env).
    pub fn env_clear(mut self) -> Self {
        self.env_clear = true;
        self
    }

    /// Inherit **only** the named variables from the parent environment —
    /// an allow-list on top of an implied [`env_clear`](Self::env_clear).
    ///
    /// The named vars are copied from the parent environment at each spawn
    /// (vars the parent lacks are skipped); explicit [`env`](Self::env) /
    /// [`env_remove`](Self::env_remove) overrides still apply afterwards.
    /// Repeated calls extend the allow-list. Works on every platform.
    ///
    /// A client [`default_env`](crate::CliClient::default_env) for an
    /// **allow-listed** key is **not** applied — the command chose to inherit that
    /// key from the parent, and a client default must not override it. A client
    /// default for a key *not* in the list still fills (an explicit override
    /// layered on top, orthogonal to parent inheritance) — so a client-wide safety
    /// default reaches the command. Use [`env_clear`](Self::env_clear) instead to
    /// opt out of client env defaults entirely.
    pub fn inherit_env<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inherit_env
            .get_or_insert_with(Vec::new)
            .extend(names.into_iter().map(|n| n.as_ref().to_os_string()));
        self
    }

    /// Run the child as this user id (Unix privilege drop).
    ///
    /// Applied by the OS between fork and exec; combine with
    /// [`gid`](Self::gid) — the group id is set **before** the user id (once
    /// the uid drops, changing gid is no longer permitted), an ordering the
    /// standard library guarantees. On non-Unix targets the run fails with
    /// [`ErrorReason::Unsupported`] — a requested
    /// privilege drop is never silently skipped.
    ///
    /// **Linux cgroup caveat:** under the cgroup v2 mechanism
    /// ([`Mechanism::CgroupV2`](crate::Mechanism::CgroupV2)) the child joins
    /// its cgroup *after* the OS has dropped the uid, by writing the
    /// auto-created (and therefore not target-uid-writable) `cgroup.procs` —
    /// so the spawn currently fails with a permission error rather than
    /// producing an uncontained child. Privilege drop composes cleanly with
    /// the POSIX process-group mechanism (macOS/BSD, or Linux without cgroup
    /// delegation); making it compose with cgroups (e.g. chowning the cgroup
    /// to the target uid) is tracked future work.
    pub fn uid(mut self, uid: u32) -> Self {
        self.uid = Some(uid);
        self
    }

    /// Run the child under this group id (Unix privilege drop) — see
    /// [`uid`](Self::uid) for ordering and platform notes.
    pub fn gid(mut self, gid: u32) -> Self {
        self.gid = Some(gid);
        self
    }

    /// Set the child's **supplementary groups** (Unix privilege drop),
    /// *replacing* the inherited set.
    ///
    /// This is the missing third leg of a correct privilege drop: dropping the
    /// [`uid`](Self::uid)/[`gid`](Self::gid) alone leaves the child holding the
    /// **parent's** supplementary groups (often root's), so it could still reach
    /// group-owned resources the target user shouldn't. Pass the target user's
    /// groups (or `[]` to drop all extras) alongside `uid`/`gid`.
    ///
    /// Ordering is handled for you: the OS applies `setgroups` → `setgid` →
    /// `setuid` (groups and gid must be set while still privileged, before the
    /// uid drops). On non-Unix targets the run fails with
    /// [`ErrorReason::Unsupported`] — never silently
    /// skipped. The Linux cgroup-v2 caveat from [`uid`](Self::uid) applies
    /// unchanged.
    pub fn groups(mut self, gids: impl AsRef<[u32]>) -> Self {
        self.groups = Some(gids.as_ref().to_vec());
        self
    }

    /// Detach the child into a **new session** (Unix `setsid()`): no
    /// controlling terminal, its own session and process group.
    ///
    /// Containment is preserved: the group tracks the new session's process
    /// group (whose id is the child's pid), so kill-on-drop and the teardown
    /// verbs still reach it. On non-Unix targets the run fails with
    /// [`ErrorReason::Unsupported`].
    ///
    /// Honored by the `Command`-driven launch paths (`run`/`output_*`/
    /// `start`, [`ProcessGroup::start`](crate::ProcessGroup::start),
    /// pipelines); the low-level raw-command
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape hatch
    /// bypasses these builders.
    pub fn setsid(mut self) -> Self {
        self.setsid = true;
        self
    }

    /// Launch this child at a lower (or higher) CPU-scheduling priority — for
    /// background/batch work that shouldn't starve the foreground, or a task
    /// that should win over it.
    ///
    /// Applied on **both** platforms via the existing spawn seams: Unix
    /// `setpriority` in the same `pre_exec` hook that carries
    /// [`uid`](Self::uid)/[`gid`](Self::gid)/[`setsid`](Self::setsid); Windows
    /// a priority-class flag OR'd into `creation_flags`, the same seam as
    /// [`create_no_window`](Self::create_no_window). Unlike the privilege
    /// builders this never yields
    /// [`ErrorReason::Unsupported`] — see
    /// [`Priority`](crate::Priority) for why both platforms cover every
    /// variant, and the Unix caveat that lowering `nice` below its inherited
    /// value — [`Priority::AboveNormal`](crate::Priority::AboveNormal)/
    /// [`Priority::High`](crate::Priority::High) always, and even
    /// [`Priority::Normal`](crate::Priority::Normal) under a positively-niced
    /// parent — needs `CAP_SYS_NICE`/root there.
    ///
    /// Last-write-wins with an earlier call, like [`timeout`](Self::timeout).
    pub fn priority(mut self, priority: crate::Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Restrict the child to the given logical CPU indices. Descendants inherit
    /// the mask, so this initially constrains the whole process tree (a child may
    /// still change its own affinity if the OS permits it).
    ///
    /// On Linux the mask is applied with `sched_setaffinity(2)` in the pre-exec
    /// child, before user code runs. On Windows it is applied with
    /// `SetProcessAffinityMask` while the child is still suspended between job
    /// assignment and resume; the ConPTY path uses the same ordering. macOS,
    /// BSD, and other targets return [`ErrorReason::Unsupported`] rather than
    /// silently inheriting the parent's mask.
    ///
    /// An empty set, a Linux index beyond `cpu_set_t`, or a Windows index beyond
    /// the process mask width fails before the child runs. Windows' mask API is
    /// limited to one processor group; indices are therefore bounded by the
    /// native pointer width, and the OS rejects processors unavailable to the
    /// current group. Repeated calls are last-write-wins; duplicates are removed
    /// and indices are stored in ascending order.
    ///
    /// This owner-dependent configuration is refused by
    /// [`spawn_detached`](Self::spawn_detached). On Windows it is also unavailable
    /// through [`to_tokio_command`](Self::to_tokio_command), because a raw command
    /// has no post-spawn suspended-child configuration seam; use a high-level run
    /// verb instead.
    pub fn cpu_affinity(mut self, cpus: impl IntoIterator<Item = usize>) -> Self {
        self.cpu_affinity = Some(crate::cpu_affinity::normalize(cpus));
        self
    }

    /// Set the Linux I/O-scheduling priority for this child, so background disk
    /// work can yield to foreground users.
    ///
    /// Applied with `ioprio_set(2)` in the child before `exec`, on the same
    /// `pre_exec` seam as [`priority`](Self::priority) and
    /// [`umask`](Self::umask). Linux is the only supported platform: Windows,
    /// macOS, BSD, and other Unix targets fail with
    /// [`ErrorReason::Unsupported`] rather than
    /// silently inheriting the caller's I/O priority. See [`IoPriority`](crate::IoPriority)
    /// for the Linux classes, data range, and privilege caveat.
    ///
    /// This configuration is owner-dependent and is therefore refused by
    /// [`spawn_detached`](Self::spawn_detached). Last-write-wins with an earlier
    /// call, like [`timeout`](Self::timeout).
    pub fn io_priority(mut self, priority: crate::IoPriority) -> Self {
        self.io_priority = Some(priority);
        self
    }

    /// Set the file-mode creation mask for the child (Unix `umask(2)`),
    /// controlling the default permissions of files it creates.
    ///
    /// Applied via `pre_exec`, alongside [`setsid`](Self::setsid)/
    /// [`groups`](Self::groups) — another knob on that same seam. On
    /// non-Unix targets the run fails with
    /// [`ErrorReason::Unsupported`] rather than
    /// silently ignoring the requested mask. Only the low permission bits are
    /// meaningful (as with the `umask(2)` syscall itself); pass the value you
    /// would give the `umask` shell builtin, e.g. `0o022`.
    pub fn umask(mut self, mask: u32) -> Self {
        self.umask = Some(mask);
        self
    }

    /// Kill the **direct child** if *this* process dies abruptly — including
    /// a `SIGKILL` of the parent, where `Drop` never runs to tear the group
    /// down. An opt-in hardening **on top of** the unconditional kill-on-drop
    /// containment, best-effort by design:
    ///
    /// | Platform | Effect |
    /// |---|---|
    /// | Windows | Already guaranteed regardless of this knob: the kernel closes the Job Object handle when the parent dies, and kill-on-close takes the whole tree. Documented no-op. |
    /// | Linux | `prctl(PR_SET_PDEATHSIG, SIGKILL)` on the **direct child only** — grandchildren are not covered (with the parent gone, nothing tears the cgroup/pgroup down). |
    /// | macOS / BSD / other | No `pdeathsig` equivalent — does nothing (the graceful-exit guarantee via `Drop` still holds). |
    ///
    /// The reach of this hardening on the current platform is reported honestly
    /// by [`kill_on_parent_death_scope`](Self::kill_on_parent_death_scope) as a
    /// [`ParentDeathCleanup`] — [`WholeTree`](ParentDeathCleanup::WholeTree) on
    /// Windows, [`DirectChildOnly`](ParentDeathCleanup::DirectChildOnly) on
    /// Linux, [`Unsupported`](ParentDeathCleanup::Unsupported) on macOS/BSD — so
    /// a caller can surface the real scope instead of overpromising a whole-tree
    /// guarantee. This is best-effort hardening for **abrupt** owner death only;
    /// ordinary graceful teardown (`Drop`) still kills the whole tree everywhere.
    ///
    /// Two honest Linux caveats:
    /// - The death signal fires when the spawning **thread** dies, not only the
    ///   process — on a multi-threaded tokio runtime, a worker thread retired
    ///   while the child lives would kill it early (for the strongest guarantee
    ///   spawn from a current-thread runtime). The parent-died-before-arming race
    ///   is closed in the child by re-checking `getppid()` against the spawner's
    ///   pid captured before the fork — safe in containers where the spawner
    ///   itself is PID 1.
    /// - The kernel **clears `PR_SET_PDEATHSIG` across an `execve` of a set-uid /
    ///   set-gid binary** (a security measure), so this is silently void for a
    ///   `sudo …` / setuid child — it inherits the pdeathsig for the tiny window
    ///   before `execve`, then loses it. Contain such a child with the
    ///   kill-on-drop group (the default) rather than relying on this knob.
    ///
    /// (Idea borrowed from `execa`'s cleanup-on-exit, mapped to native
    /// primitives.)
    pub fn kill_on_parent_death(mut self) -> Self {
        self.kill_on_parent_death = true;
        self
    }

    /// The scope of whole-tree cleanup [`kill_on_parent_death`] actually
    /// achieves on this build's target platform when the owner dies
    /// **abruptly** — a [`ParentDeathCleanup`] capability report, so a caller
    /// can state the real reach of the hardening rather than overpromising a
    /// whole-tree guarantee the OS cannot keep.
    ///
    /// [`WholeTree`](ParentDeathCleanup::WholeTree) on Windows (the kernel
    /// closes the Job Object handle on owner death and kill-on-close reaps the
    /// whole tree), [`DirectChildOnly`](ParentDeathCleanup::DirectChildOnly) on
    /// Linux (`PR_SET_PDEATHSIG` reaches only the direct child; grandchildren
    /// survive), and [`Unsupported`](ParentDeathCleanup::Unsupported) on macOS /
    /// the BSDs (no `pdeathsig` equivalent). Fixed per target at build time — it
    /// does not depend on whether [`kill_on_parent_death`] was called or on any
    /// runtime state — so it is an associated function, not a method on a built
    /// command. See [`ParentDeathCleanup`] for the full contract; note it
    /// describes only the abrupt-death path, since ordinary graceful teardown
    /// (owner exits/panics, so `Drop` runs) kills the whole tree everywhere.
    ///
    /// [`kill_on_parent_death`]: Self::kill_on_parent_death
    #[must_use]
    pub const fn kill_on_parent_death_scope() -> ParentDeathCleanup {
        if cfg!(windows) {
            // Job Object kill-on-close: the kernel reaps the whole tree when the
            // owner's last job handle closes on death.
            ParentDeathCleanup::WholeTree
        } else if cfg!(target_os = "linux") {
            // PR_SET_PDEATHSIG(SIGKILL) fires on the direct child only; nothing
            // tears the surviving cgroup/pgroup down once the owner is gone.
            ParentDeathCleanup::DirectChildOnly
        } else {
            // macOS / the BSDs (and any other unix): no pdeathsig equivalent, so
            // an abrupt owner death triggers no cleanup at all.
            ParentDeathCleanup::Unsupported
        }
    }

    /// Spawn without a console window (Windows `CREATE_NO_WINDOW`) — for a
    /// GUI app launching a CLI tool without a flashing terminal.
    ///
    /// On non-Windows targets this is a harmless no-op (purely cosmetic — no
    /// console windows exist to suppress). Honored by the `Command`-driven
    /// launch paths; the raw
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape hatch still
    /// overwrites creation flags (see its docs).
    pub fn create_no_window(mut self) -> Self {
        // CREATE_NO_WINDOW, as a literal so the field exists on every platform.
        self.creation_flags_extra |= 0x0800_0000;
        self
    }

    /// Opt in to a **graceful Windows teardown** via a console `CTRL_BREAK`
    /// event, giving a **console** child a chance to shut down cleanly instead of
    /// being hard-killed at once.
    ///
    /// A graceful timeout ([`timeout_grace`](Self::timeout_grace)) or a group
    /// [`shutdown`](crate::ProcessGroup::shutdown) already posts `WM_CLOSE` to any
    /// top-level window a live member owns, so a **windowed** child (Electron app,
    /// desktop tool, windowed service) drains cleanly with no opt-in. A **console**
    /// child has no window, and Windows has no POSIX signal to reach it, so without
    /// this opt-in its graceful teardown collapses to the *atomic* Job Object kill —
    /// there is nothing to *trigger* a clean exit. With this opt-in
    /// the direct child is spawned in its own console process group
    /// (`CREATE_NEW_PROCESS_GROUP`), and at graceful teardown it is sent
    /// `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` before the grace window:
    /// a child that installs a `CTRL_BREAK` handler (as many CLIs, Node, Python,
    /// and Go services do) can flush and exit within the grace. Any survivor still
    /// running when the grace elapses is then `TerminateJobObject`'d — the same
    /// hard-kill fallback as before, so containment is never weakened.
    ///
    /// # Boundaries (read these)
    ///
    /// - **Console-only.** The event is delivered through the console this process
    ///   shares with the child. A child spawned
    ///   [`create_no_window`](Self::create_no_window) (or `DETACHED_PROCESS`) does
    ///   **not** share that console, so it never receives the `CTRL_BREAK` and
    ///   simply rides the grace to the `TerminateJobObject` fallback. A GUI /
    ///   service parent with no console of its own can't deliver the event either.
    /// - **`CTRL_BREAK`, not `CTRL_C`.** `CREATE_NEW_PROCESS_GROUP` disables
    ///   `CTRL_C` for the new group by default; `CTRL_BREAK` is always deliverable,
    ///   which is why it is the event sent. `timeout_signal`
    ///   (Unix's signal choice) does not apply — Windows always sends `CTRL_BREAK`.
    /// - **Direct child only.** Only the process launched by this run is a group
    ///   leader; its own descendants receive the event via the shared console and
    ///   group, but an `adopt`ed child (not spawned
    ///   here) is not addressed and falls back to the hard kill.
    /// - **No-op off Windows.** On Unix the graceful tier already sends a real
    ///   signal, so this builder does nothing there.
    ///
    /// Honored by the `Command`-driven launch paths (run helpers,
    /// [`start`](crate::ProcessGroup::start), the run-level graceful
    /// [`timeout_grace`](Self::timeout_grace), and a shared
    /// [`ProcessGroup`](crate::ProcessGroup)'s
    /// [`shutdown`](crate::ProcessGroup::shutdown)); the raw
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape hatch, which
    /// overwrites creation flags wholesale, does not participate.
    pub fn windows_graceful_ctrl_break(mut self) -> Self {
        self.windows_graceful_ctrl_break = true;
        self
    }

    /// Spawn the child under a **pseudo-terminal** (PTY) instead of three
    /// independent pipes, so tools that *demand* a controlling terminal work.
    ///
    /// Off by default. When set, the child is launched over a single PTY master
    /// — `openpty` on Unix, `CreatePseudoConsole` (ConPTY) on Windows — so
    /// `isatty()` reports a terminal and a program that refuses to run (or that
    /// hangs waiting for a prompt) without one behaves normally: an
    /// `isatty()`-gated agentic CLI, an `ssh`/`sudo` **password**/passphrase
    /// prompt, a credential helper. This is a **minimal single-master-fd mode**,
    /// not a general terminal emulator.
    ///
    /// # stdout and stderr are merged
    ///
    /// A PTY has one master fd carrying the child's combined output, so in this
    /// mode **stdout and stderr are merged** and can no longer be separated. The
    /// merged stream is delivered exactly where stdout normally is
    /// (`output_string`, `stdout_lines`, `on_stdout_line`, `stdout_tee`, …);
    /// [`on_stderr_line`](Self::on_stderr_line) is **never called** and
    /// [`stderr_tee`](Self::stderr_tee) never receives anything, because there is
    /// no separate stderr to deliver. [`ProcessResult::stderr`](crate::ProcessResult::stderr)
    /// is empty for a PTY run. If you need stdout and stderr apart, do not use
    /// PTY mode.
    ///
    /// # Interactive input
    ///
    /// [`keep_stdin_open`](Self::keep_stdin_open) plus
    /// [`take_stdin`](crate::RunningProcess::take_stdin) drive the master's input
    /// side exactly as with a pipe, and a configured [`stdin`](Self::stdin)
    /// source is written to it. On **Unix** the PTY line discipline's terminal
    /// **echo is disabled** so a written password is not echoed back into the
    /// merged output (see [`ProcessStdin`](crate::ProcessStdin)); the Windows
    /// ConPTY has no portable per-write echo control, so that guarantee is
    /// Unix-only.
    ///
    /// # Line framing default (`\r`-aware) and output hygiene
    ///
    /// A PTY child writes like a terminal: CRLF line endings, progress bars
    /// redrawn in place with a bare `\r` (no `\n` until the end), and VT/ANSI
    /// escape sequences (colors, cursor moves, alternate screen, OSC titles). Two
    /// deliberate, coded decisions handle this — one automatic, one opt-in:
    ///
    /// - **Framing is auto `\r`-aware.** Under `use_pty` the **effective** default
    ///   [`line_terminator`](Self::line_terminator) is
    ///   [`CarriageReturn`](LineTerminator::CarriageReturn) instead of `Newline`,
    ///   so each progress redraw surfaces as its own line rather than piling into
    ///   one ever-growing string a naive consumer never sees framed. This is a
    ///   **non-destructive** reframing (it only changes where lines split), so it
    ///   is the sensible default for the mode; a `\r\n` still counts as one
    ///   terminator, so ordinary CRLF text reads unchanged. It is applied only
    ///   when you have **not** pinned a terminator yourself — an explicit
    ///   [`line_terminator`](Self::line_terminator) (even `Newline`) always wins,
    ///   order-independently of `use_pty`.
    /// - **Escape sanitization stays opt-in.** Stripping VT/ANSI escapes is
    ///   **destructive** (it removes bytes from the captured output), so it is
    ///   *not* turned on automatically — reach for
    ///   [`sanitize_vt`](Self::sanitize_vt) when you want the merged output
    ///   de-escaped for line predicates and transcripts. Leaving it off keeps the
    ///   PTY's bytes verbatim in the backlog.
    ///
    /// # Containment is unchanged
    ///
    /// The PTY child is placed in the **same** Job Object / cgroup / process
    /// group as any other child, so whole-tree kill-on-drop, timeouts, and
    /// cancellation behave identically — the pseudo-terminal only changes the
    /// I/O wiring, never the teardown guarantee.
    ///
    /// Available only with the `pty` crate feature. A note for callers who leave
    /// it unset: without this the existing three-pipe behavior — including the
    /// `Newline` framing default — is byte-for-byte unchanged.
    ///
    /// # Terminal identity and environment overrides
    ///
    /// At spawn, ProcessKit identifies the terminal it creates: Unix children
    /// receive `TERM=xterm-256color`, and every PTY child receives `COLUMNS` and
    /// `LINES` matching the initial PTY geometry (80×24 by default, or the size
    /// set with [`pty_size`](Self::pty_size)). Windows does not synthesize
    /// `TERM`: ConPTY exposes VT handling through the Windows console APIs, so
    /// any inherited `TERM` remains governed by the normal environment rules.
    ///
    /// These values are defaults, not forced overrides. Explicit
    /// [`env`](Self::env) or [`env_remove`](Self::env_remove) operations for
    /// `TERM`, `COLUMNS`, or `LINES` always win, including with
    /// [`env_clear`](Self::env_clear) or [`inherit_env`](Self::inherit_env).
    #[cfg(feature = "pty")]
    #[cfg_attr(docsrs, doc(cfg(feature = "pty")))]
    pub fn use_pty(mut self) -> Self {
        self.use_pty = true;
        self
    }

    /// Set the **initial window size** — `cols` columns by `rows` rows — of the
    /// pseudo-terminal opened by [`use_pty`](Self::use_pty).
    ///
    /// The size matters to terminal-aware children: it drives line wrapping,
    /// progress-bar/TUI layout, and pager behavior, and is the geometry a child
    /// reads back with an `isatty`/`TIOCGWINSZ`-style query. Without this the PTY
    /// opens at the conventional **80×24** default. At spawn, the child's
    /// `COLUMNS` and `LINES` environment defaults are set to the same values;
    /// explicit [`env`](Self::env) / [`env_remove`](Self::env_remove) operations
    /// for either name win. A later
    /// [`RunningProcess::resize_pty`](crate::RunningProcess::resize_pty) changes
    /// the live terminal geometry but cannot rewrite an already-running
    /// process's environment.
    ///
    /// # Only meaningful with `use_pty`
    ///
    /// This is a **PTY-only** knob. On a command that is **not**
    /// [`use_pty`](Self::use_pty) it is a documented **no-op** — the three-pipe
    /// launch has no terminal to size, so the value is simply never read (it is
    /// *not* silently applied anywhere, and it is *not* an error to set; it just
    /// does nothing). Order-independent: `pty_size(..).use_pty()` and
    /// `use_pty().pty_size(..)` are equivalent.
    ///
    /// # Live resize
    ///
    /// To change the size of an already-running session (e.g. propagating a host
    /// window resize / `SIGWINCH`), use
    /// [`RunningProcess::resize_pty`](crate::RunningProcess::resize_pty).
    ///
    /// Available only with the `pty` crate feature.
    #[cfg(feature = "pty")]
    #[cfg_attr(docsrs, doc(cfg(feature = "pty")))]
    pub fn pty_size(mut self, cols: u16, rows: u16) -> Self {
        self.pty_size = Some((cols, rows));
        self
    }

    /// Provide standard input for the child (see [`Stdin`]).
    pub fn stdin(mut self, stdin: Stdin) -> Self {
        self.stdin = Some(stdin);
        self
    }

    /// Chain this command's stdout into `next`'s stdin — the first link of a
    /// shell-free [`Pipeline`](crate::Pipeline). Keep chaining with
    /// [`Pipeline::pipe`](crate::Pipeline::pipe) (or the `|` operator), then
    /// drive the whole thing with
    /// [`Pipeline::output_string`](crate::Pipeline::output_string) /
    /// [`Pipeline::run`](crate::Pipeline::run).
    pub fn pipe(self, next: Command) -> crate::Pipeline {
        crate::Pipeline::new(self, next)
    }

    /// Exempt this command, **as a pipeline stage**, from pipefail
    /// attribution: its unclean exit (non-zero code, signal kill — including
    /// SIGPIPE — or its own per-stage [`timeout`](Self::timeout) kill) is
    /// skipped when the chain decides what to report, and never shields a
    /// *checked* stage's failure. The motivating pattern is
    /// `producer | head -1`: the consumer exits early, the producer dies of
    /// `SIGPIPE`/`EPIPE`, and without this marker strict pipefail reports
    /// that perfectly normal death as the chain's failure. (Design borrowed
    /// from `duct`'s `unchecked()` — the idea, not the code.)
    ///
    /// Outside a [`Pipeline`](crate::Pipeline) this is a **no-op**: a single
    /// run's status is already plain data in its
    /// [`ProcessResult`], and
    /// [`ensure_success`](crate::ProcessResult::ensure_success) stays opt-in
    /// — `unchecked` does not relax it, nor a whole-chain
    /// [`Pipeline::timeout`](crate::Pipeline::timeout).
    pub fn unchecked_in_pipe(mut self) -> Self {
        self.unchecked = true;
        self
    }

    /// Whether this stage opted out of pipefail attribution.
    pub(crate) fn is_unchecked(&self) -> bool {
        self.unchecked
    }

    /// Wire `reader` (the previous pipeline stage's stdout) as this command's
    /// stdin, overriding any configured stdin source or `keep_stdin_open` —
    /// inner stages of a [`Pipeline`](crate::Pipeline) read from the pipe, full
    /// stop.
    pub(crate) fn set_pipe_stdin<R>(&mut self, reader: R)
    where
        R: tokio::io::AsyncRead + Send + 'static,
    {
        self.stdin = Some(Stdin::from_reader(reader));
        self.keep_stdin_open = false;
        // An inner pipeline stage reads the previous stage's stdout, never the
        // parent's stdin — clear any inherit so the wired pipe unconditionally wins.
        self.stdin_inherit = false;
    }

    /// Kill the run if it exceeds `timeout`.
    ///
    /// Clears a prior [`no_timeout`](Self::no_timeout) — the last of the two wins.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Timeout::After(timeout);
        self
    }

    /// Run **without** a timeout, and — unlike simply leaving the timeout unset —
    /// opt out of any client-wide [`default_timeout`](crate::CliClient::default_timeout)
    /// gap-fill. Use this to say "this one long-running command is *deliberately*
    /// unbounded" against a client that otherwise imposes a deadline on every call
    /// (a `tail -f`, a watch loop, an interactive session).
    ///
    /// A plain [`Command`] (no client) is already unbounded by default, so this is
    /// only meaningful when the command is run through a [`CliClient`](crate::CliClient)
    /// with a `default_timeout`. Clears a prior [`timeout`](Self::timeout) — the
    /// last of the two wins.
    pub fn no_timeout(mut self) -> Self {
        self.timeout = Timeout::Unbounded;
        self
    }

    /// Kill the run when neither stdout nor stderr produces bytes for `idle`.
    ///
    /// Unlike [`timeout`](Self::timeout), this is a **resettable** deadline: every
    /// successful read from either output stream grants a fresh `idle` window.
    /// The initial window starts when the child is spawned, so a process that
    /// never writes output is still bounded. Teardown uses the same
    /// [`timeout_grace`](Self::timeout_grace), `timeout_signal` (when the
    /// `process-control` feature is enabled), and whole-tree containment path as
    /// the absolute timeout.
    ///
    /// The result is reported as
    /// [`Outcome::InactivityTimedOut`](crate::Outcome::InactivityTimedOut),
    /// distinct from [`Outcome::TimedOut`](crate::Outcome::TimedOut). A zero
    /// duration is valid and expires as soon as the watchdog is polled.
    pub fn inactivity_timeout(mut self, idle: Duration) -> Self {
        self.inactivity_timeout = Some(idle);
        self
    }

    /// Set the timeout from an optional [`Duration`], folding the
    /// [`timeout`](Self::timeout) / [`no_timeout`](Self::no_timeout) split into a
    /// single composable verb for config-driven call sites. `Some(d)` is exactly
    /// [`timeout(d)`](Self::timeout); `None` is exactly
    /// [`no_timeout()`](Self::no_timeout).
    ///
    /// Reach for it when you hold an `Option<Duration>` (a parsed config value, a
    /// caller-supplied override) instead of the
    /// `match cfg { Some(d) => c.timeout(d), None => c.no_timeout() }` dance. Mind
    /// the `None` mapping: it means *deliberately unbounded* — opting out of a
    /// client-wide [`default_timeout`](crate::CliClient::default_timeout) gap-fill,
    /// **not** "leave the timeout unset for a default to fill". Like the two verbs
    /// it folds, it is last-write-wins with any earlier timeout call.
    pub fn timeout_opt(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = match timeout {
            Some(d) => Timeout::After(d),
            None => Timeout::Unbounded,
        };
        self
    }

    /// Make either [`timeout`](Self::timeout) or
    /// [`inactivity_timeout`](Self::inactivity_timeout) **graceful**: when the
    /// winning watchdog fires the run's
    /// tree is sent `SIGTERM` (or the signal chosen via `timeout_signal`, with the
    /// `process-control` feature), given up to `grace` to exit, then `SIGKILL`ed.
    /// Without it the watchdog hard-kills at once. No effect unless at least one
    /// timeout is set.
    ///
    /// **Windows** has no POSIX signal tier, but two best-effort soft triggers run
    /// before the atomic kill when the tree can act on one: `WM_CLOSE` is posted to
    /// every top-level window owned by a live member (a windowed child — Electron
    /// app, desktop tool, windowed service — can then close and drain within
    /// `grace`), and a child opted into
    /// [`windows_graceful_ctrl_break`](Self::windows_graceful_ctrl_break) is sent a
    /// console `CTRL_BREAK`. A tree with **neither** a window nor that opt-in has
    /// nothing to trigger a soft exit, so the deadline hard-kills the job at once,
    /// `grace` unused — timings unchanged from before this soft tier. Any survivor
    /// still running when `grace` elapses is `TerminateJobObject`'d. Either way
    /// [`timed_out`](crate::ProcessResult::timed_out) stays `true` (the deadline
    /// was exceeded), graceful or not.
    pub fn timeout_grace(mut self, grace: Duration) -> Self {
        self.timeout_grace = Some(grace);
        self
    }

    /// The signal sent at the start of a graceful
    /// [`timeout_grace`](Self::timeout_grace) window (default
    /// [`Signal::Term`](crate::Signal::Term)). Unix-only in effect; ignored on
    /// Windows (no signal tier).
    ///
    /// This builder lives behind the `process-control` feature because the
    /// [`Signal`](crate::Signal) type does. Without `process-control` the
    /// graceful timeout always uses `SIGTERM` (the default); the feature is only
    /// needed to *choose a different* teardown signal — promoting `Signal` into
    /// the base API would enlarge the always-on surface for a niche knob.
    #[cfg(feature = "process-control")]
    pub fn timeout_signal(mut self, signal: crate::Signal) -> Self {
        self.timeout_signal = Some(signal);
        self
    }

    /// Treat these exit codes (not just `0`) as success for the checking verbs —
    /// [`run`](Self::run) (and `run_unit`/`checked` via
    /// [`ProcessRunnerExt`]) and
    /// [`ProcessResult::ensure_success`] / [`is_success`](ProcessResult::is_success).
    /// For tools whose non-zero exit is a normal result — `grep` (1 = no match),
    /// `diff` (1 = differs), rsync's code families — so callers don't hand-match.
    ///
    /// An empty set is **ignored** — a no-op that leaves the previously configured
    /// codes (or the default `[0]`) in place, rather than resetting to `[0]`, since
    /// an empty accepted-set would make every exit a failure. Does not change
    /// [`exit_code`](Self::exit_code) (always the raw code) or
    /// [`probe`](Self::probe) (always the 0/1 convention).
    pub fn ok_codes(mut self, codes: impl IntoIterator<Item = i32>) -> Self {
        let codes: Vec<i32> = codes.into_iter().collect();
        if !codes.is_empty() {
            self.ok_codes = Some(codes);
        }
        self
    }

    /// Tie this run to `token`: cancelling it kills the process tree and makes
    /// every consuming path (`run`/`output_string`/`output_bytes`/`wait`/
    /// `exit_code`/`probe`/`profile`/`finish` and the streamed
    /// finishers) resolve to [`ErrorReason::Cancelled`].
    /// In a [`Pipeline`](crate::Pipeline), a token on any stage cancels that
    /// stage and the cancellation errors the whole pipeline (the private
    /// pipeline group tears the other stages down).
    ///
    /// Unlike [`timeout`](Self::timeout) — which is *captured* in the
    /// [`ProcessResult`] (`timed_out`) without erroring on the non-checking
    /// paths — a cancellation is **always** an error, on every path. When both
    /// fire, cancellation wins (it is checked first — except in `first_line`'s
    /// narrow tie where the deadline watchdog closes the stream in the same poll
    /// the token fires, which surfaces as `Timeout`). An already-cancelled token
    /// short-circuits before spawning. On a private group the whole tree is
    /// killed; on a shared group
    /// ([`ProcessGroup::start`](crate::ProcessGroup::start)) only the direct
    /// child is, like `timeout`. Both [`wait_any`](crate::wait_any) and
    /// [`first_line`](Self::first_line) surface a *mid-run* cancel as
    /// `Err(Cancelled)` — their streaming race resolves the cancellation and
    /// tears the child down — as does an already-cancelled token via the
    /// pre-spawn short-circuit. A mid-run cancel during
    /// [`wait_for_line`](crate::RunningProcess::wait_for_line), by contrast,
    /// closes the stream and surfaces as that probe's
    /// [`ErrorReason::NotReady`], not `Cancelled` — the
    /// consuming finisher afterwards still reports `Cancelled`.
    ///
    /// A cancelled run is never retried: [`retry`](Self::retry) policies and
    /// [`Supervisor`](crate::Supervisor) restarts both treat
    /// `ErrorReason::Cancelled` as terminal — the token stays cancelled forever, so
    /// another attempt could only fail the same way.
    ///
    /// On a `Command` this **replaces** any previously set token (last write
    /// wins) — contrast the *gap-fill* containers
    /// [`Pipeline::cancel_on`](crate::Pipeline::cancel_on) and
    /// [`CliClient::default_cancel_on`](crate::CliClient::default_cancel_on),
    /// which leave an explicit per-element token intact.
    pub fn cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Retry the run while `retry_if` accepts the error, up to `max_attempts`
    /// total attempts, sleeping a fixed `backoff` between tries. For exponential
    /// backoff + cap + jitter, use [`retry_with`](Self::retry_with).
    ///
    /// Applies to the **success-checking** helpers —
    /// `run`/`run_unit`/`checked`/`exit_code`/`probe`/`parse`/`try_parse` — on
    /// [`Command`](Self::run), on [`ProcessRunnerExt`],
    /// and on [`CliClient`](crate::CliClient): the ones that surface failure as an
    /// [`Error`] the classifier can inspect (e.g. a transient network failure in
    /// `stderr`, or [`ErrorReason::Timeout`]). The non-erroring
    /// `output_string`/`output_bytes` paths don't retry.
    ///
    /// Each attempt **re-executes the whole command** — a fresh process. Only
    /// retry operations that are safe to repeat: a side effect that already landed
    /// before the failure (a `git push` that reached the server, then dropped the
    /// connection) will be replayed. Prefer to gate retries on a classifier that
    /// matches *pre-effect* failures (DNS/connection errors, [`ErrorReason::Timeout`]
    /// while still connecting) rather than any non-zero exit.
    ///
    /// A [`timeout`](Self::timeout) bounds **each attempt**, not the whole retried
    /// operation — there is no total wall-clock ceiling across retries (worst case
    /// ≈ `attempts × timeout` + the sum of the backoffs). Bound the total with
    /// [`cancel_on`](Self::cancel_on) (a `Cancelled` is terminal — never retried).
    ///
    /// A **one-shot** stdin source
    /// ([`Stdin::from_reader`](crate::Stdin::from_reader) /
    /// [`from_lines`](crate::Stdin::from_lines)) feeds a single run, so a retry
    /// re-feeds it only when the failed attempt is **guaranteed not to have
    /// consumed it**. The launch reserves that payload *transactionally* and
    /// commits it only once a child exists, so a failure **before any child was
    /// spawned** — [`NotFound`](crate::ErrorReason::NotFound),
    /// [`Spawn`](crate::ErrorReason::Spawn) (e.g. a transient `ETXTBSY` that
    /// [`is_transient`](crate::Error::is_transient) accepts), or
    /// [`Unsupported`](crate::ErrorReason::Unsupported) — rolls the reservation back
    /// and leaves the payload intact: such a command **is** retried (subject to
    /// the classifier) and the next attempt feeds the untouched source. Any
    /// other error may have reached a live child that already consumed the
    /// source — a non-zero [`Exit`](crate::ErrorReason::Exit),
    /// [`Timeout`](crate::ErrorReason::Timeout), [`Signalled`](crate::ErrorReason::Signalled),
    /// a stdin-write [`Stdin`](crate::ErrorReason::Stdin) failure,
    /// [`OutputTooLarge`](crate::ErrorReason::OutputTooLarge), or the ambiguous
    /// [`Io`](crate::ErrorReason::Io) (which arises both before and after a child) — so
    /// the first attempt's error is returned as-is, **not** retried (a retry
    /// would either replay empty stdin or spuriously classify the re-consume).
    /// Use a reusable source (`from_string`/`from_bytes`/`from_file`/
    /// `from_iter_lines`) to retry unconditionally. (A one-shot source *re-run*
    /// outside this retry loop — a `Supervisor` incarnation, a pipeline re-run —
    /// does fail loud with [`ErrorReason::Io`] `InvalidInput` at
    /// launch instead.)
    ///
    /// **Inert outside the success-checking verbs.** A `retry` policy is
    /// honored only by the verbs listed above. It is **ignored** by:
    /// - [`Supervisor`](crate::Supervisor) — supervision is keep-alive
    ///   *restarting* with its own [`RestartPolicy`](crate::RestartPolicy) /
    ///   backoff / storm handling, a different concern from replay-to-success;
    ///   configure restarts there, not via `retry`.
    /// - [`output_all`](crate::output_all) — a bounded fan-out that collects
    ///   every outcome as data (no per-command retry); wrap each command's verb
    ///   yourself if a batch element must retry.
    /// - the raw [`Pipeline`](crate::Pipeline) verbs — a stage's `retry` does not
    ///   re-run that stage within the chain.
    ///
    /// **Counting:** `max_attempts` is the **total** number of runs (so
    /// `retry(3, …)` runs at most three times: the first plus two more).
    /// `max_attempts` of `0` **and** `1` both mean a single run with no retry — a
    /// command always runs at least once, so `0` does not mean "never run". For
    /// exponential backoff + cap + jitter instead of a fixed delay, use
    /// [`retry_with`](Self::retry_with), which takes a [`RetryPolicy`] — note that
    /// a `RetryPolicy` counts `max_retries` (the runs *after* the first), so
    /// `retry(3, …)` corresponds to `RetryPolicy::new().max_retries(2)`.
    ///
    /// [`ErrorReason::Timeout`]: crate::ErrorReason::Timeout
    pub fn retry(
        mut self,
        max_attempts: u32,
        backoff: Duration,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry = Some(RetryConfig::fixed(max_attempts, backoff, retry_if));
        self
    }

    /// Retry on a rich [`RetryPolicy`] — **exponential backoff + cap + jitter** —
    /// instead of the fixed `(max_attempts, backoff)` of [`retry`](Self::retry).
    /// The per-command analogue of
    /// [`CliClient::default_retry`](crate::CliClient::default_retry), with the same
    /// applicability and replay caveats as [`retry`](Self::retry). Note
    /// `RetryPolicy` counts `max_retries` (after the first attempt), whereas
    /// [`retry`](Self::retry) counts `max_attempts` (total).
    pub fn retry_with(
        mut self,
        policy: RetryPolicy,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry = Some(RetryConfig::new(policy, retry_if));
        self
    }

    /// Opt out of retries entirely: run this command **exactly once** and
    /// suppress any client-wide
    /// [`default_retry`](crate::CliClient::default_retry) gap-fill.
    ///
    /// The explicit, symmetric counterpart to [`no_timeout`](Self::no_timeout):
    /// a bare [`Command`] already retries nothing, so this is only meaningful
    /// against a [`CliClient`](crate::CliClient) whose `default_retry` would
    /// otherwise be filled in — it pins "run this one command once, whatever the
    /// client policy". Tidier than, and behaviorally identical to, the
    /// `retry(1, Duration::ZERO, |_| false)` idiom (one attempt, a classifier that
    /// accepts nothing). Last-write-wins with any earlier
    /// [`retry`](Self::retry) / [`retry_with`](Self::retry_with).
    pub fn retry_never(mut self) -> Self {
        self.retry = Some(RetryConfig::never());
        self
    }

    /// Leave stdin open after start so the child can be driven interactively via
    /// [`RunningProcess::take_stdin`](crate::RunningProcess::take_stdin).
    /// Takes precedence over a [`stdin`](Self::stdin) source — when set, that
    /// source is ignored and the pipe is handed to the caller instead.
    ///
    /// The open pipe lives until the caller takes it (`take_stdin`) or a
    /// consuming verb runs: at consume time an **untaken** pipe is closed
    /// (nothing could ever write to it again), so a stdin-reading child sees
    /// EOF instead of blocking — combining `keep_stdin_open` with a bulk
    /// helper (`output_string`, `run`, …) without ever taking the writer is
    /// equivalent to not setting it. A writer the caller *did* take is
    /// unaffected and keeps the pipe until dropped or
    /// [`finish`](crate::ProcessStdin::finish)ed.
    ///
    /// Mutually exclusive with [`inherit_stdin`](Self::inherit_stdin) — a child
    /// cannot both be handed an interactive stdin pipe and share the parent's
    /// stdin; setting both is rejected at launch (see `inherit_stdin`).
    pub fn keep_stdin_open(mut self) -> Self {
        self.keep_stdin_open = true;
        self
    }

    /// Give the child the parent's **own** standard input — it reads directly
    /// from whatever this process's stdin is connected to (a terminal, a file,
    /// a pipe) rather than from a crate-managed pipe.
    ///
    /// This is the stdin counterpart of
    /// [`stdout(StdioMode::Inherit)`](Self::stdout) /
    /// [`stderr(StdioMode::Inherit)`](Self::stderr): the child *shares* the
    /// parent stream instead of the crate mediating it. Reach for it when a
    /// child must talk to the real terminal — `git commit` opening `$EDITOR`, a
    /// tool prompting the user for a password or a yes/no, or simply forwarding
    /// the parent's piped stdin straight through to the child. Until a
    /// pseudo-terminal exists (a future direction, not yet provided) this covers
    /// the common non-tty-negotiating interactive cases without the crate having
    /// to pump bytes.
    ///
    /// Because the child reads the parent's stdin directly, the crate neither
    /// feeds nor captures that input, and there is no writer to
    /// [`take_stdin`](crate::RunningProcess::take_stdin) (it returns `None`, as
    /// for a non-`keep_stdin_open` run). stdout/stderr are unaffected — capture
    /// and streaming of the child's output keep working exactly as before.
    ///
    /// # Mutually exclusive with a mediated stdin
    ///
    /// Inheriting the parent's stdin cannot be combined with either way the crate
    /// would otherwise *drive* stdin — a configured [`stdin`](Self::stdin) source
    /// (`Stdin::from_string`/`from_bytes`/`from_file`/`from_reader`/`from_lines`,
    /// or an explicit `Stdin::empty()`) or [`keep_stdin_open`](Self::keep_stdin_open)'s
    /// interactive pipe. Setting `inherit_stdin` **and** one of those is a
    /// contradiction (feed the child a source *and* let it read the terminal?),
    /// so it is rejected at the launch boundary with a typed
    /// [`ErrorReason::Io`] (`InvalidInput`) — the same failure mode
    /// as the other stdin misconfiguration the crate refuses (re-running a
    /// consumed one-shot source) — rather than silently letting one win. Drop the
    /// other stdin knob to resolve it.
    pub fn inherit_stdin(mut self) -> Self {
        self.stdin_inherit = true;
        self
    }

    /// Invoke `handler` for each decoded stdout line as it is read (in addition
    /// to capture/streaming). Runs on the pump task; keep it cheap. A handler
    /// that **panics** is caught and disabled for the rest of the run — the
    /// child is still drained and the result still carries every line (the
    /// panic is reported as a `tracing` warn when that feature is on).
    ///
    /// **Ordering guarantees:** invocations are FIFO *within* a stream; there
    /// is no ordering between stdout and stderr handlers (two independent
    /// pumps). On the consuming verbs (`run`/`output_*`/`wait`/`profile`/
    /// `finish`) all handler invocations happen-before the awaited
    /// future resolves — a progress bar can be finalized the moment the call
    /// returns. (One documented exception: when a leaked pipe is held open
    /// past the child's death, teardown aborts the pump after a bounded
    /// grace, cutting any not-yet-delivered lines along with their handler
    /// calls.) On a streamed run, stdout handlers quiesce when the
    /// [`stdout_lines`](crate::RunningProcess::stdout_lines) stream ends.
    ///
    /// At most one handler per stream: a repeat call replaces the previous one
    /// (builder semantics, like [`timeout`](Self::timeout)). To fan out, compose
    /// inside a single closure.
    ///
    /// Requires stdout to be [`Piped`](crate::StdioMode::Piped) (the default):
    /// the handler runs on the capture pump, so it never fires under
    /// [`stdout(Inherit)`](Self::stdout) / [`stdout(Null)`](Self::stdout).
    ///
    /// **Byte cap caveat:** a single line whose length exceeds a **byte** cap
    /// ([`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes)) is never
    /// assembled, so the handler never sees it either — it is silently skipped
    /// for *every* sink (handler, tee, and capture buffer alike), counted only
    /// via the truncation/`dropped()` signal. If every line matters, leave the
    /// byte cap unset, or use a line cap instead.
    pub fn on_stdout_line<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.stdout_config.handler = Some(Arc::new(handler));
        self
    }

    /// Invoke `handler` for each decoded stderr line as it is read.
    ///
    /// Same contract as [`on_stdout_line`](Self::on_stdout_line): runs on the
    /// pump task, and a repeat call replaces the previous handler.
    pub fn on_stderr_line<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.stderr_config.handler = Some(Arc::new(handler));
        self
    }

    /// Set how the child's standard output stream is connected (default:
    /// [`StdioMode::Piped`]).
    ///
    /// - **`Piped`** (default) — captured into a pipe; all output-retrieval
    ///   verbs (`output_string`, `stdout_lines`, …) read from it.
    /// - **`Inherit`** — the child shares the parent's stdout; output appears
    ///   in the terminal/log but is not captured.
    /// - **`Null`** — suppressed entirely (redirected to `/dev/null`).
    ///
    /// With `Inherit`/`Null` there is no pipe to read, so the bulk capture
    /// verbs (`output_string`/`output_bytes`) **error** rather than return
    /// silently-empty output, and the streaming verbs (`stdout_lines`/
    /// `events`) yield an empty stream. Use a discard verb (`wait`) to run
    /// a command whose stdout you don't want to capture. Calling this after
    /// [`stdout_file`](Self::stdout_file) restores a normal stdio mode.
    pub fn stdout(mut self, mode: crate::StdioMode) -> Self {
        self.stdout_mode = mode;
        self.stdout_file = None;
        self
    }

    /// Set how the child's standard error stream is connected (default:
    /// [`StdioMode::Piped`]).
    ///
    /// Same semantics as [`stdout`](Self::stdout): `Piped` captures,
    /// `Inherit` passes through, `Null` suppresses.
    pub fn stderr(mut self, mode: crate::StdioMode) -> Self {
        self.stderr_mode = mode;
        self.stderr_file = None;
        self
    }

    /// Redirect stdout directly to `path`, creating or truncating the file at
    /// spawn time. The child owns the descriptor, so no parent-side pump or
    /// output buffer is involved.
    ///
    /// Capture and streaming verbs require a pipe and therefore reject this
    /// configuration; use [`wait`](crate::RunningProcess::wait) or another
    /// discard verb. For a shared supervisor log across restarts, use
    /// [`stdout_file_append`](Self::stdout_file_append).
    pub fn stdout_file(mut self, path: impl AsRef<Path>) -> Self {
        self.stdout_mode = StdioMode::Piped;
        self.stdout_file = Some(FileRedirect::truncate(path));
        self
    }

    /// Redirect stdout directly to `path`, creating it when absent and appending
    /// on every spawn. This is useful for a [`Supervisor`](crate::Supervisor)
    /// whose incarnations should share one log file.
    pub fn stdout_file_append(mut self, path: impl AsRef<Path>) -> Self {
        self.stdout_mode = StdioMode::Piped;
        self.stdout_file = Some(FileRedirect::append(path));
        self
    }

    /// Explicit spelling of [`stdout_file`](Self::stdout_file), for code that
    /// selects append versus truncate at the call site.
    pub fn stdout_file_truncate(self, path: impl AsRef<Path>) -> Self {
        self.stdout_file(path)
    }

    /// Redirect stderr directly to `path`, creating or truncating the file at
    /// spawn time. The child owns the descriptor, so no parent-side pump or
    /// output buffer is involved.
    pub fn stderr_file(mut self, path: impl AsRef<Path>) -> Self {
        self.stderr_mode = StdioMode::Piped;
        self.stderr_file = Some(FileRedirect::truncate(path));
        self
    }

    /// Redirect stderr directly to `path`, creating it when absent and appending
    /// on every spawn. See [`stdout_file_append`](Self::stdout_file_append) for
    /// the restart-log use case.
    pub fn stderr_file_append(mut self, path: impl AsRef<Path>) -> Self {
        self.stderr_mode = StdioMode::Piped;
        self.stderr_file = Some(FileRedirect::append(path));
        self
    }

    /// Explicit spelling of [`stderr_file`](Self::stderr_file), for code that
    /// selects append versus truncate at the call site.
    pub fn stderr_file_truncate(self, path: impl AsRef<Path>) -> Self {
        self.stderr_file(path)
    }

    /// Tee every decoded stdout line to `writer` as it is produced — capture
    /// *and* stream to `writer` simultaneously.
    ///
    /// `writer` is an async sink ([`tokio::io::AsyncWrite`]); each decoded line
    /// is written to it followed by `\n`. The write is **awaited on the capture
    /// pump**, so a slow sink applies backpressure (the pump slows, the OS pipe
    /// fills, the child blocks on its next write) rather than blocking the
    /// runtime. The sink must make forward progress, though: a destination
    /// that blocks *forever* (not merely slow) stalls the pump — no further
    /// lines are buffered and a live `stdout_lines`/`events` consumer
    /// parks — until the run's teardown grace aborts the pump. A write error
    /// disables the tee for the rest of the run — surfaced as a `tracing` warn
    /// under the `tracing` feature, not silently swallowed — and capture is
    /// unaffected.
    ///
    /// Runs **independently** of [`on_stdout_line`](Self::on_stdout_line): set
    /// both and both fire per line (the tee no longer replaces the handler).
    /// A second `stdout_tee` replaces an earlier one.
    ///
    /// **Shared across clones and attempts.** The sink is held in an
    /// `Arc<Mutex<…>>`, so cloning the `Command` shares *one* sink — and a
    /// `Command` is cloned for every [`Pipeline`](crate::Pipeline) stage, every
    /// [`Supervisor`](crate::Supervisor) incarnation, and every
    /// [`retry`](Self::retry) attempt. Concurrent clones (pipeline stages running
    /// at once) **interleave** their lines into it; sequential re-runs (retries,
    /// restarts) **append** — a retried command's sink accumulates the failed
    /// attempt's output *followed by* the successful one's, with no delimiter. For
    /// per-run or per-attempt separation, tee to distinct sinks (a fresh `Command`
    /// per run) or have the sink write its own delimiters.
    ///
    /// The tee fires **before** the buffer policy decides retention, so it sees
    /// *every* decoded line — including ones the capture buffer then drops or
    /// rejects, e.g. output past a [`fail_loud`](crate::OutputBufferPolicy::fail_loud)
    /// *line* ceiling (that ceiling bounds retained memory, not what streams past).
    /// One exception: a single line whose length exceeds a **byte** cap
    /// ([`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes)) is never
    /// assembled, so it is neither retained nor teed — nor delivered to
    /// [`on_stdout_line`](Self::on_stdout_line): the byte cap silently skips
    /// that line for *every* sink alike, counted only via the
    /// truncation/`dropped()` signal. Leave the byte cap unset (or use a line
    /// cap) if every line must reach the tee. The discard verbs
    /// ([`wait`](crate::RunningProcess::wait) / `profile`) apply a large internal
    /// in-flight byte cap for the same memory bound, so a line exceeding it is
    /// likewise not teed under those verbs;
    /// [`drain`](crate::RunningProcess::drain) instead honors *this* configured
    /// byte cap, so a line exceeding the configured
    /// [`max_bytes`](crate::OutputBufferPolicy::max_bytes) is the one skipped there.
    ///
    /// Requires stdout to be [`Piped`](crate::StdioMode::Piped) (the default):
    /// the tee fires from the capture pump, so it is a no-op under
    /// [`stdout(Inherit)`](Self::stdout) / [`stdout(Null)`](Self::stdout), which
    /// run no pump. It is likewise inert under
    /// [`output_bytes`](Self::output_bytes), which captures stdout **raw** (no
    /// line pump) — reach for a stdout tee with the line verbs (`output_string`,
    /// `start` + `stdout_lines`, `events`).
    pub fn stdout_tee<W>(mut self, writer: W) -> Self
    where
        W: tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let boxed: Box<dyn tokio::io::AsyncWrite + Send + Unpin> = Box::new(writer);
        self.stdout_config.tee = Some(Arc::new(tokio::sync::Mutex::new(boxed)));
        self
    }

    /// Tee every decoded stderr line to `writer` as it is produced.
    ///
    /// Same contract as [`stdout_tee`](Self::stdout_tee) — an async
    /// [`tokio::io::AsyncWrite`] sink, awaited on the pump (backpressure, not
    /// runtime-blocking), independent of [`on_stderr_line`](Self::on_stderr_line),
    /// and requiring stderr to be [`Piped`](crate::StdioMode::Piped).
    pub fn stderr_tee<W>(mut self, writer: W) -> Self
    where
        W: tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let boxed: Box<dyn tokio::io::AsyncWrite + Send + Unpin> = Box::new(writer);
        self.stderr_config.tee = Some(Arc::new(tokio::sync::Mutex::new(boxed)));
        self
    }

    /// Tee the child's stdout to `writer` **byte for byte, before any decoding or
    /// line splitting** — a transparent passthrough that hands the consumer the
    /// child's exact bytes.
    ///
    /// Where [`stdout_tee`](Self::stdout_tee) writes *decoded lines* (each plus a
    /// `\n`), this writes each chunk **exactly as read from the pipe**, ahead of
    /// the decoder. That is the difference between a log tee and a faithful
    /// wrapper: the raw tee neither loses nor invents a single byte, so a
    /// consumer can forward the stream live *and* hash/capture the exact output.
    /// Concretely, unlike the decoded tee it preserves:
    ///
    /// - **non-UTF-8 bytes** — a child writing binary to stdout (`git archive`,
    ///   `tar -cz -`, `ffmpeg … -`) is teed verbatim, not mangled into U+FFFD
    ///   replacement characters;
    /// - **CRLF and lone `\r`** — no newline normalization, no line framing;
    /// - **a missing final newline** — the last bytes are teed as-is, never
    ///   given a fabricated `\n`;
    /// - **an unterminated prompt** — `Password: ` (no newline) reaches the sink
    ///   the moment it is read, rather than waiting in the decode buffer until
    ///   EOF the way a decoded line does, so an interactive child does not read
    ///   as hung;
    /// - **a line the capture policy drops** — a line past a
    ///   [`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes) byte cap is
    ///   skipped from *every decoded sink* (the buffer, the handler, and
    ///   [`stdout_tee`](Self::stdout_tee)), but its bytes still reach the raw tee
    ///   **whole** — the digest a wrapper computes over the raw tee covers the
    ///   child's actual output, not a truncated re-encoding.
    ///
    /// Chunks arrive in **FIFO** order within the stream. This is **strictly
    /// additive**: the decoded-line path — capture buffer, its
    /// [`OutputBufferPolicy`], the `dropped()` truncation accounting,
    /// [`on_stdout_line`](Self::on_stdout_line), and
    /// [`stdout_tee`](Self::stdout_tee) — is unchanged whether or not a raw tee is
    /// set, and all configured sinks fire independently.
    ///
    /// # Backpressure and memory
    ///
    /// `writer` is an async [`tokio::io::AsyncWrite`]. Each raw write is **awaited
    /// on the capture pump**, the same backpressure seam as
    /// [`stdout_tee`](Self::stdout_tee): a slow sink slows the pump, the OS pipe
    /// fills, and the child blocks on its next write. Nothing is buffered in the
    /// crate between the pipe and the sink, so a lagging raw consumer **cannot**
    /// grow unbounded in-flight memory — the bound is the OS pipe, not a heap
    /// queue. A destination that blocks *forever* (not merely slow) stalls the
    /// pump until teardown's grace aborts it, exactly as for the decoded tee. A
    /// write error disables the raw tee for the rest of the run (a `tracing` warn
    /// under the `tracing` feature, not silently swallowed); the decoded path is
    /// unaffected. The sink is flushed once at stream end, so a buffering writer
    /// (`BufWriter`, a file) commits its tail.
    ///
    /// **Shared across clones and attempts**, like [`stdout_tee`](Self::stdout_tee):
    /// the sink is held in an `Arc<Mutex<…>>`, so cloned `Command`s (pipeline
    /// stages, supervisor incarnations, retry attempts) share *one* sink —
    /// concurrent clones interleave their bytes, sequential re-runs append. Tee to
    /// distinct sinks for per-run separation. A second `stdout_raw_tee` replaces an
    /// earlier one.
    ///
    /// # Requires a piped stdout; inert on the raw-capture verb
    ///
    /// The raw tee fires from the line **capture pump**, so — like
    /// [`stdout_tee`](Self::stdout_tee) — it is a **no-op** under
    /// [`stdout(Inherit)`](Self::stdout) / [`stdout(Null)`](Self::stdout) and a
    /// [`stdout_file`](Self::stdout_file) redirect (all of which run no capture
    /// pump); the builder accepts the combination but simply never invokes the
    /// sink, rather than rejecting it. It is likewise inert under
    /// [`output_bytes`](Self::output_bytes), whose *own* return value already **is**
    /// the exact raw stdout (a separate raw drain, no line pump) — reach for the
    /// raw tee with the line/streaming verbs (`output_string`, `start` +
    /// `stdout_lines` / `events`, `wait` / `drain`) when you need the raw
    /// bytes *alongside* decoded lines.
    ///
    /// # Record/replay caveat
    ///
    /// On a live run the tee is byte-exact. On a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner) double or a cassette
    /// replay there is no child: the scripted feeder writes the canned/recorded
    /// text back as **UTF-8**, so the raw tee receives *those* bytes — byte-exact
    /// only insofar as the recorded (already-decoded) text round-trips, the same
    /// fidelity limit that makes [`output_bytes`](Self::output_bytes) unsupported
    /// on a cassette. Rely on byte accuracy only against a real process.
    pub fn stdout_raw_tee<W>(mut self, writer: W) -> Self
    where
        W: tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let boxed: Box<dyn tokio::io::AsyncWrite + Send + Unpin> = Box::new(writer);
        self.stdout_config.raw_tee = Some(Arc::new(tokio::sync::Mutex::new(boxed)));
        self
    }

    /// Tee the child's stderr to `writer` **byte for byte, before any decoding or
    /// line splitting**.
    ///
    /// Same contract as [`stdout_raw_tee`](Self::stdout_raw_tee) — verbatim bytes
    /// (non-UTF-8, CRLF, missing final newline, and lines the buffer policy drops
    /// all preserved), FIFO order, awaited on the pump (backpressure, bounded
    /// memory), flushed at stream end, independent of
    /// [`stderr_tee`](Self::stderr_tee)/[`on_stderr_line`](Self::on_stderr_line),
    /// and requiring stderr to be [`Piped`](crate::StdioMode::Piped).
    pub fn stderr_raw_tee<W>(mut self, writer: W) -> Self
    where
        W: tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let boxed: Box<dyn tokio::io::AsyncWrite + Send + Unpin> = Box::new(writer);
        self.stderr_config.raw_tee = Some(Arc::new(tokio::sync::Mutex::new(boxed)));
        self
    }

    /// Cap the in-memory backlog of captured output lines (see
    /// [`OutputBufferPolicy`]). The pump still drains the pipe; only retention is
    /// bounded.
    ///
    /// This policy governs the **capturing** verbs
    /// ([`output_string`](crate::RunningProcess::output_string) and a streamed
    /// [`finish`](crate::RunningProcess::finish)) and, for its **byte** ceiling
    /// only, the in-flight bound of
    /// [`drain`](crate::RunningProcess::drain). The discard verbs
    /// [`wait`](crate::RunningProcess::wait) / `profile` ignore it entirely
    /// (they pin a fixed internal in-flight cap); `drain` is the discard path that
    /// *honors* this byte cap — retaining nothing, but bounding held memory by the
    /// configured [`max_bytes`](OutputBufferPolicy::max_bytes) rather than the
    /// child's output size. `max_lines` never affects the discard paths (they
    /// retain nothing).
    pub fn output_buffer(mut self, policy: OutputBufferPolicy) -> Self {
        self.output_buffer = policy;
        self
    }

    /// Install a [`CapturePolicy`] — a typed **redaction-at-capture** seam that
    /// shapes each decoded line of *both* streams **just before it is retained**,
    /// so the value the policy returns (not the raw line) is what lands in the
    /// capture backlog and therefore in
    /// [`output_string`](crate::RunningProcess::output_string) /
    /// [`ProcessResult`] and the streaming verbs
    /// ([`stdout_lines`](crate::RunningProcess::stdout_lines) /
    /// [`events`](crate::RunningProcess::events)).
    ///
    /// This is the capture-*shaping* counterpart to the *observing*
    /// [`on_stdout_line`](Self::on_stdout_line)/[`on_stderr_line`](Self::on_stderr_line)
    /// handlers: those see each line but run *alongside* capture and cannot
    /// change what is retained. Use it to scrub a secret a child echoes to its
    /// output before it settles in the captured result — completing the crate's
    /// secret-hygiene posture (a cassette stores env *names* only; `Debug`
    /// redacts env *values*).
    ///
    /// A single whole-command knob (like [`output_buffer`](Self::output_buffer)):
    /// the policy is handed the [`OutputStream`] each line
    /// came from, so one implementation can treat stdout and stderr differently.
    /// A repeat call replaces the previous policy (builder semantics).
    ///
    /// # Scope
    ///
    /// The seam shapes **only the capture backlog**. The per-line handlers, the
    /// decoded [`stdout_tee`](Self::stdout_tee)/`stderr_tee`, the byte-plane
    /// [`stdout_raw_tee`](Self::stdout_raw_tee)/`stderr_raw_tee`, and the raw
    /// stdout of [`output_bytes`](crate::RunningProcess::output_bytes) are
    /// **independent** and see the line un-redacted — if you also tee to a log,
    /// redact in that sink too. A line past an
    /// [`OutputBufferPolicy`] byte cap
    /// ([`with_max_bytes`](OutputBufferPolicy::with_max_bytes)) is never
    /// assembled, so — like the handlers/tee — the policy never sees it. See
    /// [`CapturePolicy`] for the full contract (including its fail-closed panic
    /// behavior).
    pub fn capture_policy(mut self, policy: impl CapturePolicy + 'static) -> Self {
        self.capture_policy = Some(Arc::new(policy));
        self
    }

    /// Decode stdout with `encoding` instead of UTF-8 (e.g.
    /// `encoding_rs::SHIFT_JIS`).
    pub fn stdout_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stdout_config.encoding = encoding;
        self
    }

    /// Decode stderr with `encoding` instead of UTF-8.
    pub fn stderr_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stderr_config.encoding = encoding;
        self
    }

    /// Decode both stdout and stderr with `encoding`.
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stdout_config.encoding = encoding;
        self.stderr_config.encoding = encoding;
        self
    }

    /// Choose where the line pump splits **both** streams into lines (see
    /// [`LineTerminator`]). The default is [`LineTerminator::Newline`] — split on
    /// `\n` only, unchanged from before this knob existed.
    ///
    /// Pass [`LineTerminator::CarriageReturn`] to also treat a bare `\r` as a line
    /// terminator, so **carriage-return progress output** (`curl`/`pip`/`apt`: a
    /// bar redrawn in place with `\r`, no `\n` until the end) streams **live, one
    /// frame at a time** instead of piling up as a single line that only surfaces
    /// at EOF. In that mode each `\r`-delimited frame is a line for *every* line
    /// sink alike — [`stdout_lines`](crate::RunningProcess::stdout_lines) /
    /// [`events`](crate::RunningProcess::events), the
    /// [`on_stdout_line`](Self::on_stdout_line)/[`on_stderr_line`](Self::on_stderr_line)
    /// handlers, the [`stdout_tee`](Self::stdout_tee)/[`stderr_tee`](Self::stderr_tee)
    /// sinks, and `output_string` — so there is a single, shared notion of a line.
    /// A `\r\n` pair stays one terminator (no empty line between them), and the
    /// [`OutputBufferPolicy`] byte cap now bounds an individual runaway frame
    /// rather than dropping the whole stream.
    ///
    /// Set it per stream with
    /// [`stdout_line_terminator`](Self::stdout_line_terminator) /
    /// [`stderr_line_terminator`](Self::stderr_line_terminator) when only one
    /// stream carries progress output (progress usually lands on stderr, while
    /// stdout stays newline-structured data).
    ///
    /// # Interaction with `use_pty`
    ///
    /// A PTY child writes progress as bare `\r` redraws, so `use_pty` (with the
    /// `pty` feature) makes the **effective** default terminator
    /// [`CarriageReturn`](LineTerminator::CarriageReturn) instead of `Newline`,
    /// so a naive PTY consumer gets framed progress rather than one ever-growing
    /// line (see `use_pty` for the rationale). Calling this
    /// method — with **either** variant, including an explicit `Newline` — opts
    /// out of that auto-default and pins your choice, order-independently of
    /// `use_pty`.
    pub fn line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stdout_config.terminator = terminator;
        self.stderr_config.terminator = terminator;
        #[cfg(feature = "pty")]
        {
            self.stdout_terminator_explicit = true;
            self.stderr_terminator_explicit = true;
        }
        self
    }

    /// Choose where the line pump splits **stdout** into lines (see
    /// [`LineTerminator`]); the stderr framing is left untouched. See
    /// [`line_terminator`](Self::line_terminator) for both streams at once (and
    /// for how it interacts with the `use_pty` auto-default).
    pub fn stdout_line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stdout_config.terminator = terminator;
        #[cfg(feature = "pty")]
        {
            self.stdout_terminator_explicit = true;
        }
        self
    }

    /// Choose where the line pump splits **stderr** into lines (see
    /// [`LineTerminator`]); the stdout framing is left untouched. Handy when
    /// progress output lands on stderr while stdout stays newline-structured.
    pub fn stderr_line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stderr_config.terminator = terminator;
        #[cfg(feature = "pty")]
        {
            self.stderr_terminator_explicit = true;
        }
        self
    }

    /// Enable the opt-in **VT/ANSI output sanitizer** on **both** streams' capture
    /// backlog: each decoded line is stripped of terminal escape sequences and
    /// lone control codes before it is retained, so a line-oriented consumer sees
    /// readable text instead of `\x1b[31m…`-mucked strings.
    ///
    /// The motivating case is a PTY agent CLI (`use_pty`, the `pty` feature) whose
    /// **merged** output is full of colors, cursor moves, alternate-screen
    /// switches, and OSC title/hyperlink escapes: with this on, the line predicates
    /// ([`wait_for_line`](crate::RunningProcess::wait_for_line) / `first_line`),
    /// [`output_string`](crate::RunningProcess::output_string) /
    /// [`ProcessResult`], and the streaming verbs
    /// ([`stdout_lines`](crate::RunningProcess::stdout_lines) /
    /// [`events`](crate::RunningProcess::events)) all carry the de-escaped text.
    /// It drops CSI (`ESC [ … final`), OSC (`ESC ] … BEL/ST`), DCS/SOS/PM/APC
    /// string escapes, other two-/n-byte `ESC` escapes, and lone C0 control bytes
    /// / `DEL` — **keeping** the horizontal tab `\t`.
    ///
    /// # Scope (the same boundary as `capture_policy`)
    ///
    /// Sanitization shapes **only the capture backlog**, exactly like
    /// [`capture_policy`](Self::capture_policy). The observing per-line handlers
    /// ([`on_stdout_line`](Self::on_stdout_line)/`on_stderr_line`), the decoded
    /// [`stdout_tee`](Self::stdout_tee)/`stderr_tee`, the byte-plane
    /// [`stdout_raw_tee`](Self::stdout_raw_tee)/`stderr_raw_tee`, and the raw
    /// [`output_bytes`](crate::RunningProcess::output_bytes) stream are
    /// **independent** and keep seeing the un-sanitized bytes — if you also tee to
    /// a log and want it clean, sanitize in that sink. When combined with
    /// [`capture_policy`](Self::capture_policy), sanitization runs **first** so a
    /// secret-scrubbing policy matches on already-cleaned text (a token cannot
    /// hide behind a color escape). A line past an
    /// [`OutputBufferPolicy`] byte cap is judged on its
    /// **raw** length (before this transform) and, if over-cap, is never assembled
    /// — so, like the handlers/tee, sanitization never sees it.
    ///
    /// Off by default and strictly additive: an existing run that never calls this
    /// captures byte-for-byte as before. Set it per stream with
    /// [`stdout_sanitize_vt`](Self::stdout_sanitize_vt) /
    /// [`stderr_sanitize_vt`](Self::stderr_sanitize_vt).
    pub fn sanitize_vt(mut self) -> Self {
        self.stdout_config.sanitize_vt = true;
        self.stderr_config.sanitize_vt = true;
        self
    }

    /// Enable the [`sanitize_vt`](Self::sanitize_vt) VT/ANSI sanitizer on
    /// **stdout** only; stderr capture is left verbatim.
    pub fn stdout_sanitize_vt(mut self) -> Self {
        self.stdout_config.sanitize_vt = true;
        self
    }

    /// Enable the [`sanitize_vt`](Self::sanitize_vt) VT/ANSI sanitizer on
    /// **stderr** only; stdout capture is left verbatim.
    pub fn stderr_sanitize_vt(mut self) -> Self {
        self.stderr_config.sanitize_vt = true;
        self
    }

    // --- Accessors used by the runner layer --------------------------------

    pub(crate) fn keeps_stdin_open(&self) -> bool {
        self.keep_stdin_open
    }

    /// Whether the child should read the parent's own stdin
    /// ([`inherit_stdin`](Self::inherit_stdin)).
    pub(crate) fn inherits_stdin(&self) -> bool {
        self.stdin_inherit
    }

    /// A clone with the per-line push side-effects — the
    /// [`on_stdout_line`](Self::on_stdout_line)/[`on_stderr_line`](Self::on_stderr_line)
    /// handlers, the [`stdout_tee`](Self::stdout_tee)/[`stderr_tee`](Self::stderr_tee)
    /// sinks, and the
    /// [`stdout_raw_tee`](Self::stdout_raw_tee)/[`stderr_raw_tee`](Self::stderr_raw_tee)
    /// sinks — removed. Used by the record/replay cassette's streaming `start`: its
    /// internal whole-run capture pass (`inner.output_string`) must stay silent,
    /// because the scripted handle it hands back fires the caller's handlers/tees
    /// once when the caller consumes it — exactly as a live `start` would. Without
    /// the strip they would fire twice (once for the capture, once for the replay).
    #[cfg(feature = "record")]
    pub(crate) fn without_line_side_effects(&self) -> Self {
        let mut clone = self.clone();
        clone.stdout_config.handler = None;
        clone.stderr_config.handler = None;
        clone.stdout_config.tee = None;
        clone.stderr_config.tee = None;
        clone.stdout_config.raw_tee = None;
        clone.stderr_config.raw_tee = None;
        clone
    }

    /// The stdout stream's pump config (encoding/handler/tee/terminator), cloned
    /// for the spawn. Replaces the four individual `out_encoding`/`stdout_handler`/
    /// `stdout_tee_sink`/`out_line_terminator` proxies — the launch paths take the
    /// whole [`StreamConfig`]. Cheap: handler and tee are `Arc`s.
    ///
    /// The whole-command [`capture_policy`](Self::capture_policy) and the
    /// [`OutputStream::Stdout`] tag are injected here (rather than stored on the
    /// per-stream config) so a single owning point wires the redaction seam into
    /// every pump — every launch path already routes through this getter.
    pub(crate) fn stdout_config(&self) -> StreamConfig {
        let mut config = self.stdout_config.clone();
        config.stream = OutputStream::Stdout;
        config.buffer_policy = self.capture_policy.clone();
        // A PTY child writes progress as bare `\r` redraws: default the effective
        // terminator to `CarriageReturn` under `use_pty` (unless the caller pinned
        // one) so those frames stream as lines instead of one growing blob. See
        // `use_pty`'s rustdoc for why this framing default is auto — and why the
        // (destructive) sanitizer is NOT.
        #[cfg(feature = "pty")]
        if self.use_pty && !self.stdout_terminator_explicit {
            config.terminator = LineTerminator::CarriageReturn;
        }
        config
    }

    /// The stderr stream's pump config — see [`stdout_config`](Self::stdout_config).
    /// Injects the same [`capture_policy`](Self::capture_policy) with the
    /// [`OutputStream::Stderr`] tag.
    pub(crate) fn stderr_config(&self) -> StreamConfig {
        let mut config = self.stderr_config.clone();
        config.stream = OutputStream::Stderr;
        config.buffer_policy = self.capture_policy.clone();
        // Mirror the stdout PTY `CarriageReturn` auto-default (see
        // [`stdout_config`](Self::stdout_config)). PTY mode merges stderr into the
        // stdout master, so this stderr config is unused under `use_pty`; keeping
        // the resolution symmetric avoids a surprising asymmetry for any consumer
        // that reads it.
        #[cfg(feature = "pty")]
        if self.use_pty && !self.stderr_terminator_explicit {
            config.terminator = LineTerminator::CarriageReturn;
        }
        config
    }

    pub(crate) fn output_buffer_policy(&self) -> OutputBufferPolicy {
        self.output_buffer
    }

    pub(crate) fn retry_config(&self) -> Option<RetryConfig> {
        self.retry.clone()
    }

    /// Whether stdout is captured into a pipe (vs `Inherit`/`Null`/a file). The bulk
    /// capture verbs use this to fail loudly instead of returning silently-empty
    /// output when stdout wasn't piped.
    pub(crate) fn stdout_is_piped(&self) -> bool {
        matches!(self.stdout_mode, StdioMode::Piped) && self.stdout_file.is_none()
    }

    /// Whether stderr is observable through a pipe rather than inherited,
    /// discarded, or redirected directly to a file.
    pub(crate) fn stderr_is_piped(&self) -> bool {
        matches!(self.stderr_mode, StdioMode::Piped) && self.stderr_file.is_none()
    }

    pub(crate) fn program_name(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    /// The [`prefer_local`](Self::prefer_local) directories, in priority order
    /// (read by the `ErrorReason::NotFound` diagnostic enrichment in `runner.rs`).
    pub(crate) fn prefer_local_dirs(&self) -> &[PathBuf] {
        &self.prefer_local
    }

    /// Whether the command customizes the environment in a way that could move
    /// `PATH` away from the process `PATH` — an explicit `PATH` override/removal,
    /// [`env_clear`](Self::env_clear), or [`inherit_env`](Self::inherit_env)
    /// (which clears the inherited set). When true, the *`PATH`*-directory
    /// naming in [`ErrorReason::NotFound`] is skipped:
    /// `find_in_path` reads the *process* `PATH`, so against a custom child
    /// `PATH` that list would be wrong. [`prefer_local`](Self::prefer_local)
    /// directories are unaffected by this gate and still get named — they're
    /// resolved by plain filesystem probes on the parent side, independent of
    /// the child's environment. A missing program still surfaces as
    /// `ErrorReason::NotFound` (so [`is_not_found`](crate::Error::is_not_found)
    /// holds), with `searched: None` only when there are no `prefer_local`
    /// directories to name either.
    pub(crate) fn customizes_path(&self) -> bool {
        self.env_clear
            || self.inherit_env.is_some()
            || self
                .envs
                .iter()
                .any(|(key, _)| env_key_eq(key, OsStr::new("PATH")))
    }

    /// Whether [`setsid`](Self::setsid) was requested (read by the spawn seam).
    pub(crate) fn wants_setsid(&self) -> bool {
        self.setsid
    }

    /// Whether [`kill_on_parent_death`](Self::kill_on_parent_death) was
    /// requested (read by the spawn seam).
    pub(crate) fn wants_kill_on_parent_death(&self) -> bool {
        self.kill_on_parent_death
    }

    /// Whether [`use_pty`](Self::use_pty) was requested (read by the launch seam
    /// to route the spawn through the PTY path). Always `false` without the `pty`
    /// feature, so the non-PTY spawn is byte-identical.
    #[cfg(feature = "pty")]
    pub(crate) fn wants_pty(&self) -> bool {
        self.use_pty
    }

    /// Without the `pty` feature there is no PTY mode, so a launch never routes
    /// through the PTY spawn path.
    #[cfg(not(feature = "pty"))]
    pub(crate) fn wants_pty(&self) -> bool {
        false
    }

    /// The configured [`pty_size`](Self::pty_size) `(cols, rows)`, or `None` to
    /// use the backend default (80×24). Read by the launch seam to fill
    /// [`SpawnOptions::pty_size`](crate::sys::SpawnOptions). Always `None` without
    /// the `pty` feature, so the non-PTY spawn is byte-identical.
    #[cfg(feature = "pty")]
    pub(crate) fn configured_pty_size(&self) -> Option<(u16, u16)> {
        self.pty_size
    }

    /// See [`configured_pty_size`](Self::configured_pty_size) — always `None`
    /// without the `pty` feature.
    #[cfg(not(feature = "pty"))]
    pub(crate) fn configured_pty_size(&self) -> Option<(u16, u16)> {
        None
    }

    /// The cancellation token, if any (an `Arc`-cheap clone).
    pub(crate) fn cancel_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.cancel_token.clone()
    }

    /// Fill in a [`CliClient`](crate::CliClient)'s default env ops for keys this
    /// command has **not** already set. Per-command `env`/`env_remove` wins.
    /// Case-insensitive key comparison on Windows.
    pub(crate) fn fill_default_envs(&mut self, defaults: &[(OsString, Option<OsString>)]) {
        for (key, value) in defaults {
            if !self.has_env_override(key) {
                self.envs.push((key.clone(), value.clone()));
            }
        }
    }

    /// Whether the command has taken control of `name` such that a client-wide
    /// env default ([`CliClient::default_env`](crate::CliClient::default_env) /
    /// [`default_env_fn`](crate::CliClient::default_env_fn)) must **not** gap-fill
    /// it. True when:
    ///
    /// 1. an explicit per-command [`env`](Self::env)/[`env_remove`](Self::env_remove)
    ///    already sets `name` (platform env-case rules); or
    /// 2. [`env_clear`](Self::env_clear) was called — a clean slate the client
    ///    must not pierce, for *any* key; or
    /// 3. `name` is in an [`inherit_env`](Self::inherit_env) allow-list — a client
    ///    default must not **override** a value the command chose to inherit from
    ///    the parent.
    ///
    /// Note the asymmetry between (2) and (3): `env_clear` blocks every key
    /// (nothing was asked for), but a bare `inherit_env` blocks only its
    /// *allow-listed* keys — a client default for a key the command did **not**
    /// list (a safety default like `GIT_TERMINAL_PROMPT=0`) still fills, since that
    /// is an explicit override layered on top, orthogonal to which vars are copied
    /// from the parent. A command that wants none of the client's env defaults uses
    /// `env_clear` (and sets what it needs with [`env`](Self::env)).
    pub(crate) fn has_env_override(&self, name: &OsStr) -> bool {
        self.env_clear
            || self.envs.iter().any(|(key, _)| env_key_eq(key, name))
            || self
                .inherit_env
                .as_deref()
                .is_some_and(|list| list.iter().any(|k| env_key_eq(k, name)))
    }

    /// Fill a client-wide retry config ([`CliClient::default_retry`](crate::CliClient::default_retry))
    /// only when this command set no [`retry`](Self::retry) of its own — so a
    /// per-command policy wins (gap-fill, not override).
    pub(crate) fn fill_default_retry(&mut self, default: &Option<RetryConfig>) {
        if self.retry.is_none() {
            self.retry = default.clone();
        }
    }

    /// Extra Windows creation flags (read by the spawn seam on every target):
    /// [`create_no_window`](Self::create_no_window)'s bit, OR'd with a
    /// requested [`priority`](Self::priority)'s priority-class flag on
    /// Windows (a no-op elsewhere, matching `creation_flags_extra`'s own
    /// cross-platform harmlessness).
    pub(crate) fn extra_creation_flags(&self) -> u32 {
        #[cfg(windows)]
        {
            let mut flags = self.creation_flags_extra;
            if let Some(priority) = self.priority {
                flags |= priority.creation_flag();
            }
            flags
        }
        #[cfg(not(windows))]
        {
            self.creation_flags_extra
        }
    }

    /// Whether this command opted into the Windows graceful console-CTRL teardown
    /// ([`windows_graceful_ctrl_break`](Self::windows_graceful_ctrl_break)). Read
    /// by the launch seam to set
    /// [`SpawnOptions::windows_new_process_group`](crate::sys::SpawnOptions); a
    /// documented no-op off Windows.
    pub(crate) fn wants_windows_graceful_ctrl_break(&self) -> bool {
        self.windows_graceful_ctrl_break
    }

    /// The requested privilege-drop uid — read only by the non-Unix
    /// unsupported gate (Unix consumes the field directly in `build_tokio`).
    #[cfg(not(unix))]
    pub(crate) fn requested_uid(&self) -> Option<u32> {
        self.uid
    }

    /// See [`requested_uid`](Self::requested_uid).
    #[cfg(not(unix))]
    pub(crate) fn requested_gid(&self) -> Option<u32> {
        self.gid
    }

    /// Whether supplementary groups were requested — read only by the non-Unix
    /// unsupported gate (Unix consumes the field directly in `build_tokio`).
    #[cfg(not(unix))]
    pub(crate) fn requested_groups(&self) -> bool {
        self.groups.is_some()
    }

    /// The requested `umask` — read only by the non-Unix unsupported gate
    /// (Unix consumes the field directly in `build_tokio`).
    #[cfg(not(unix))]
    pub(crate) fn requested_umask(&self) -> Option<u32> {
        self.umask
    }

    /// The requested Linux I/O priority — read by the non-Linux unsupported gate.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn requested_io_priority(&self) -> Option<crate::IoPriority> {
        self.io_priority
    }

    // ----- Public accessors -----------------------------------------------
    // Let `ScriptedRunner::when(|cmd| …)` predicates and other inspection read
    // what a command will run. Named to avoid clashing with the builder methods
    // (`arguments` vs `args`, `working_dir` vs `current_dir`, …).

    /// The program to launch.
    pub fn program(&self) -> &OsStr {
        &self.program
    }

    /// The arguments, in order.
    pub fn arguments(&self) -> &[OsString] {
        &self.args
    }

    /// Render this command as a single shell-quoted line for **display** — logs,
    /// error messages, a dry-run echo. Quoting is per-platform (POSIX
    /// single-quote / Windows double-quote) and is for readability, **not
    /// execution**: the crate never invokes a shell, and the rendering is not
    /// guaranteed to round-trip through one. Do **not** feed the output back to a
    /// shell to re-run the command — the escaping targets human legibility, not
    /// any specific shell's parsing rules.
    ///
    /// The line includes the arguments, which may carry secrets (a `--token=…`
    /// flag). Unlike the `tracing` feature — which never logs argv — this is
    /// opt-in: render it only into a sink you control.
    pub fn command_line(&self) -> String {
        let mut line = quote_arg(&self.program.to_string_lossy());
        for arg in &self.args {
            line.push(' ');
            line.push_str(&quote_arg(&arg.to_string_lossy()));
        }
        line
    }

    /// The working-directory override, if one was set.
    pub fn working_dir(&self) -> Option<&Path> {
        self.cwd.as_deref().map(Path::new)
    }

    /// The environment overrides, in order (a `None` value removes the variable).
    pub fn env_overrides(&self) -> &[(OsString, Option<OsString>)] {
        &self.envs
    }

    /// Environment operations applied at spawn, in priority order.
    ///
    /// PTY identity defaults are seeded first and explicit user operations are
    /// appended afterwards, so every launch consumer gets the same last-write-
    /// wins behavior. Keeping this derivation here prevents the Unix
    /// `build_tokio`, Windows raw-ConPTY, and hermetic `Invocation` paths from
    /// drifting apart.
    pub(crate) fn spawn_env_ops(&self) -> Vec<(OsString, Option<OsString>)> {
        let mut ops = Vec::new();
        #[cfg(feature = "pty")]
        if self.use_pty {
            let (cols, rows) = self.pty_size.unwrap_or(crate::sys::pty::DEFAULT_PTY_SIZE);
            #[cfg(unix)]
            ops.push((
                OsString::from("TERM"),
                Some(OsString::from("xterm-256color")),
            ));
            ops.push((
                OsString::from("COLUMNS"),
                Some(OsString::from(cols.to_string())),
            ));
            ops.push((
                OsString::from("LINES"),
                Some(OsString::from(rows.to_string())),
            ));
        }
        ops.extend(self.envs.iter().cloned());
        ops
    }

    /// The configured stdin source, if any.
    pub fn stdin_source(&self) -> Option<&Stdin> {
        self.stdin.as_ref()
    }

    /// The configured stdin source that will actually be passed to the child.
    ///
    /// [`keep_stdin_open`](Self::keep_stdin_open) and
    /// [`inherit_stdin`](Self::inherit_stdin) both take precedence over a
    /// source set with [`stdin`](Self::stdin); this mirrors the shared
    /// [`crate::runner::take_stdin_for_run`] launch boundary, which never
    /// reserves a source for an inherit-stdin command (the conflict between
    /// `inherit_stdin` and a configured source is rejected at the launch
    /// boundary before any reservation happens).
    pub(crate) fn effective_stdin_source(&self) -> Option<&Stdin> {
        (!self.keeps_stdin_open() && !self.inherits_stdin())
            .then_some(())
            .and(self.stdin_source())
    }

    /// The configured deadline, if any — `Some(d)` for a
    /// [`timeout(d)`](Self::timeout), `None` for both an unset timeout and an
    /// explicitly [`no_timeout`](Self::no_timeout) (neither imposes a deadline).
    pub fn configured_timeout(&self) -> Option<Duration> {
        self.timeout.as_duration()
    }

    /// The configured output-inactivity window, if any.
    pub fn configured_inactivity_timeout(&self) -> Option<Duration> {
        self.inactivity_timeout
    }

    /// Whether a client-wide [`default_timeout`](crate::CliClient::default_timeout)
    /// may gap-fill this command: only when the timeout is still
    /// [`Unset`](Timeout::Unset). An explicit [`timeout`](Self::timeout)
    /// ([`After`](Timeout::After)) or [`no_timeout`](Self::no_timeout)
    /// ([`Unbounded`](Timeout::Unbounded), deliberately unbounded) both opt out.
    pub(crate) fn accepts_default_timeout(&self) -> bool {
        matches!(self.timeout, Timeout::Unset)
    }

    /// The graceful-timeout grace window, if set.
    pub(crate) fn configured_timeout_grace(&self) -> Option<Duration> {
        self.timeout_grace
    }

    /// The raw signal for the graceful-timeout phase (default `SIGTERM`).
    pub(crate) fn timeout_signal_raw(&self) -> i32 {
        #[cfg(all(unix, feature = "process-control"))]
        if let Some(sig) = self.timeout_signal {
            return sig.raw();
        }
        crate::sys::SIGTERM_RAW
    }

    /// The exit codes this command treats as success (defaults to `[0]`).
    pub(crate) fn ok_codes_vec(&self) -> Vec<i32> {
        self.ok_codes.clone().unwrap_or_else(|| vec![0])
    }

    /// The child's fully-resolved environment for the Windows raw-`CreateProcessW`
    /// PTY spawn, which bypasses `std`'s env handling (ConPTY needs a raw spawn).
    ///
    /// `None` means "inherit the parent environment unchanged" (a null env block),
    /// `Some(list)` is the exact set of `KEY=VALUE` pairs the child should get —
    /// mirroring the [`env_clear`](Self::env_clear) / [`inherit_env`](Self::inherit_env)
    /// / [`env`](Self::env) / [`env_remove`](Self::env_remove) layering
    /// [`build_tokio`](Self::build_tokio) applies (so a customized-env PTY run
    /// matches a customized-env pipe run). Keys are folded case-insensitively —
    /// matching Windows env semantics — and returned in that (sorted) order, which
    /// `CreateProcessW`'s Unicode env block also requires. Computed cross-platform
    /// to keep the spawn seam uniform; only the Windows backend consumes it (on
    /// Unix the pty child keeps `build_tokio`'s env, applied by `std`).
    #[cfg(feature = "pty")]
    pub(crate) fn resolved_pty_env(&self) -> Option<Vec<(OsString, OsString)>> {
        let env_ops = self.spawn_env_ops();
        if !self.env_clear && self.inherit_env.is_none() && env_ops.is_empty() {
            return None; // no customization → inherit the parent env unchanged
        }
        use std::collections::BTreeMap;
        // Fold on an upper-cased key so a later `env("path", …)` overrides an
        // inherited `PATH`, matching Windows' case-insensitive env — and BTreeMap
        // order gives the case-insensitively sorted block `CreateProcessW` wants.
        let ci = |k: &OsStr| OsString::from(k.to_string_lossy().to_uppercase());
        let mut map: BTreeMap<OsString, (OsString, OsString)> = BTreeMap::new();
        // Seed from the parent env only when neither `env_clear` nor `inherit_env`
        // asked for a clean slate — exactly `build_tokio`'s condition.
        if !self.env_clear && self.inherit_env.is_none() {
            for (k, v) in std::env::vars_os() {
                map.insert(ci(&k), (k, v));
            }
        }
        if let Some(names) = &self.inherit_env {
            for name in names {
                if let Some(v) = std::env::var_os(name) {
                    map.insert(ci(name), (name.clone(), v));
                }
            }
        }
        for (k, v) in &env_ops {
            match v {
                Some(val) => {
                    map.insert(ci(k), (k.clone(), val.clone()));
                }
                None => {
                    map.remove(&ci(k));
                }
            }
        }
        Some(map.into_values().collect())
    }

    /// The exit codes explicitly configured via [`ok_codes`](Self::ok_codes), if
    /// any — `None` when unset, in which case the default `{0}` applies (see
    /// `ok_codes_vec` for the always-populated effective
    /// set). Mirrors [`configured_timeout`](Self::configured_timeout): the raw
    /// *configured* state, not a resolved default — lets `ScriptedRunner::when`
    /// predicates and other inspection code (see the "Public accessors" note
    /// above) tell "left at the default" apart from "explicitly set to `{0}`".
    pub fn configured_ok_codes(&self) -> Option<&[i32]> {
        self.ok_codes.as_deref()
    }

    /// The canonical logical CPU set configured via
    /// [`cpu_affinity`](Self::cpu_affinity), if any. The slice is sorted and
    /// deduplicated; `Some([])` preserves an explicitly empty (invalid) request so
    /// inspection can distinguish it from an unset affinity before launch.
    pub fn configured_cpu_affinity(&self) -> Option<&[usize]> {
        self.cpu_affinity.as_deref()
    }

    /// Lower this builder to a raw [`tokio::process::Command`] — the escape hatch
    /// for a platform knob ProcessKit deliberately doesn't model.
    ///
    /// **Prefer the typed verbs.** Almost every launch should go through
    /// `run`/`output_string`/`output_bytes`/[`start`](crate::ProcessGroup::start)
    /// or a [`ProcessGroup`](crate::ProcessGroup): those drive the async output
    /// pump, capture, timeouts/cancellation, and the graceful-teardown machinery
    /// for you. This bridge exists for the rare case where you need to set
    /// something on the OS command that the builder has no typed knob for (a niche
    /// creation flag, your own `pre_exec`), *without* re-deriving the crate's
    /// launch wiring by hand.
    ///
    /// The returned command carries everything this builder resolves at the OS
    /// level: the (optionally `prefer_local`-resolved) program and arguments, the
    /// working directory, the layered environment
    /// ([`env_clear`](Self::env_clear)/[`inherit_env`](Self::inherit_env)/
    /// [`env`](Self::env)/[`env_remove`](Self::env_remove)), the platform launch
    /// hooks (Unix `priority`/`cpu_affinity`/`umask`/privilege-drop/
    /// [`setsid`](Self::setsid) `pre_exec` hooks; Windows creation flags), and stdio wired to match the
    /// builder's [`stdout`](Self::stdout_file)/`stdin` configuration (piped for
    /// capture by default). Mutate the returned command, then hand it to
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) to keep containment.
    ///
    /// **What you keep, and what you give up.** Spawning the result through
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) still enrolls the child
    /// in the group's Job/cgroup/process-group, so **containment is preserved**
    /// (kill-on-drop and the group-level teardown verbs still reach it). You
    /// **give up** the high-level machinery keyed off this builder that lives
    /// *above* the OS command: the async output pump and capture, the
    /// `ProcessResult`/`RunningProcess` verbs, and the per-run
    /// [`timeout`](Self::timeout)/[`cancel_on`](Self::cancel_on)/
    /// [`timeout_grace`](Self::timeout_grace)/
    /// [`windows_graceful_ctrl_break`](Self::windows_graceful_ctrl_break) wiring —
    /// you drive the bare [`tokio::process::Child`] (draining
    /// its pipes, reaping it) yourself. On Windows,
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) re-sets the child's
    /// creation flags to make containment race-free, so a creation flag left on this
    /// command is overwritten by that path (see its docs) — reach for the typed
    /// [`create_no_window`](Self::create_no_window) on a high-level launch path
    /// instead.
    ///
    /// # Errors
    ///
    /// The same preflight failures a normal launch would raise while resolving the
    /// program / opening a `stdout_file` redirect
    /// ([`ErrorReason::Io`]), plus
    /// [`ErrorReason::Unsupported`] for a
    /// Linux-only I/O-priority request on another platform, affinity on a target
    /// other than Linux/Windows, or Windows affinity (which requires the typed
    /// suspended-child launch seam and cannot be encoded in a raw command).
    pub fn to_tokio_command(&self) -> Result<tokio::process::Command> {
        #[cfg(windows)]
        if self.cpu_affinity.is_some() {
            return Err(ErrorReason::Unsupported {
                operation: "cpu_affinity through to_tokio_command on Windows".into(),
            }
            .into());
        }
        self.build_tokio()
    }

    /// Build the `tokio` command with stdio wired for capture. Containment
    /// (cgroup/job/process-group) is added by the group's `spawn`.
    pub(crate) fn build_tokio(&self) -> Result<tokio::process::Command> {
        #[cfg(not(target_os = "linux"))]
        if self.io_priority.is_some() {
            return Err(ErrorReason::Unsupported {
                operation: "io_priority (Linux-only)".into(),
            }
            .into());
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        if self.cpu_affinity.is_some() {
            return Err(ErrorReason::Unsupported {
                operation: "cpu_affinity (Linux/Windows only)".into(),
            }
            .into());
        }

        // A bare-name `program` may be spawned via a resolved absolute path so
        // the OS launches *exactly* what the spawn-free preflight
        // (`resolve_program`) reports it would: a `prefer_local` match (always),
        // or — on Windows — a `PATH` match whose PATHEXT extension is not `.exe`
        // (`.cmd`/`.bat`/`.com`/…), which the OS's own `.exe`-only bare-name
        // `PATH` search would never find. Everything else (a `.exe` `PATH`
        // match, a path-form program, no match) is left untouched — the OS still
        // resolves it against the child's own `PATH`, exactly as before this
        // builder existed. See `spawn_program_override` for the full rationale.
        let program = self.spawn_program_override();
        let mut cmd = match program {
            Some(resolved) => tokio::process::Command::new(resolved),
            None => tokio::process::Command::new(&self.program),
        };
        cmd.args(&self.args);
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }
        if self.env_clear || self.inherit_env.is_some() {
            cmd.env_clear();
        }
        if let Some(names) = &self.inherit_env {
            for name in names {
                if let Some(value) = std::env::var_os(name) {
                    cmd.env(name, value);
                }
            }
        }
        for (key, value) in self.spawn_env_ops() {
            match value {
                Some(val) => {
                    cmd.env(key, val);
                }
                None => {
                    cmd.env_remove(key);
                }
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // CPU/I/O priority and umask are independent of the privilege-drop
            // hooks below; register scheduling hooks first so a privileged
            // priority request is made while the child still holds its original
            // credentials. Both the `Some(groups)` and `None` branches below
            // perform the whole uid/gid drop inside a user `pre_exec` closure
            // registered after these hooks, so that guarantee holds uniformly.
            if let Some(priority) = self.priority {
                let nice = priority.nice_value();
                // SAFETY: setpriority is async-signal-safe; `nice` is a plain
                // i32 copy, not a pointer into anything shared.
                unsafe {
                    cmd.as_std_mut().pre_exec(move || {
                        if libc::setpriority(libc::PRIO_PROCESS, 0, nice) == -1 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
            #[cfg(target_os = "linux")]
            if let Some(cpus) = &self.cpu_affinity {
                let set = crate::cpu_affinity::linux_set(cpus).map_err(Error::io)?;
                // SAFETY: the fixed-size cpu_set_t is built before fork; the
                // closure performs one syscall over that copied buffer.
                unsafe {
                    cmd.as_std_mut().pre_exec(move || {
                        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
                            == -1
                        {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
            #[cfg(target_os = "linux")]
            if let Some(priority) = self.io_priority {
                let value = priority.linux_value().map_err(Error::io)?;
                const IOPRIO_WHO_PROCESS: libc::c_int = 1;
                // SAFETY: `syscall(SYS_ioprio_set, ...)` is a direct Linux system
                // call; its arguments are integer copies and it allocates nothing
                // in the post-fork child.
                unsafe {
                    cmd.as_std_mut().pre_exec(move || {
                        if libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, value) == -1 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
            if let Some(mask) = self.umask {
                // SAFETY: umask(2) is async-signal-safe and — unlike the other
                // hooks here — cannot fail, so no error path is needed.
                unsafe {
                    cmd.as_std_mut().pre_exec(move || {
                        libc::umask(mask as libc::mode_t);
                        Ok(())
                    });
                }
            }
            match &self.groups {
                // Do the *whole* drop (setgroups → setgid → setuid) in one
                // pre_exec: std runs its own setgid/setuid before any user hook,
                // so a separate setgroups hook would run post-uid-drop and fail
                // EPERM. (`CommandExt::groups` is unstable, so unusable here.)
                Some(groups) => {
                    let groups = groups.clone();
                    let gid = self.gid;
                    let uid = self.uid;
                    // SAFETY: setgroups/setgid/setuid are async-signal-safe; the
                    // captured gid buffer is read-only in the forked child.
                    unsafe {
                        cmd.as_std_mut().pre_exec(move || {
                            let n = groups.len();
                            if libc::setgroups(n as _, groups.as_ptr().cast::<libc::gid_t>()) == -1
                            {
                                return Err(std::io::Error::last_os_error());
                            }
                            if let Some(gid) = gid
                                && libc::setgid(gid) == -1
                            {
                                return Err(std::io::Error::last_os_error());
                            }
                            if let Some(uid) = uid
                                && libc::setuid(uid) == -1
                            {
                                return Err(std::io::Error::last_os_error());
                            }
                            Ok(())
                        });
                    }
                }
                // Mirror std's own drop path (setgroups(0, ...) to clear
                // supplementary groups, then setgid before setuid — changing
                // gid is barred once the uid drops) but inside a user
                // pre_exec hook registered *after* the priority/umask hooks
                // above, instead of using `Command::gid`/`uid` directly:
                // those std builder methods apply their drop *before* any
                // user pre_exec hook runs, which would silently drop
                // privileges before `Priority::High` gets to raise them.
                None => {
                    let gid = self.gid;
                    let uid = self.uid;
                    if gid.is_some() || uid.is_some() {
                        // SAFETY: setgroups/setgid/setuid are async-signal-safe;
                        // `gid`/`uid` are plain copies, not pointers into
                        // anything shared.
                        unsafe {
                            cmd.as_std_mut().pre_exec(move || {
                                if libc::setgroups(0, std::ptr::null()) == -1 {
                                    return Err(std::io::Error::last_os_error());
                                }
                                if let Some(gid) = gid
                                    && libc::setgid(gid) == -1
                                {
                                    return Err(std::io::Error::last_os_error());
                                }
                                if let Some(uid) = uid
                                    && libc::setuid(uid) == -1
                                {
                                    return Err(std::io::Error::last_os_error());
                                }
                                Ok(())
                            });
                        }
                    }
                }
            }
            if self.setsid {
                // Registered before any backend hook (e.g. the cgroup join) so
                // the session exists first. The pgroup backend skips its setpgid
                // under setsid: setsid fails EPERM on an existing group leader.
                // SAFETY: the closure calls only setsid() and reads errno —
                // both async-signal-safe.
                unsafe {
                    cmd.as_std_mut().pre_exec(|| {
                        if libc::setsid() == -1 {
                            Err(std::io::Error::last_os_error())
                        } else {
                            Ok(())
                        }
                    });
                }
            }
        }
        #[cfg(windows)]
        {
            // Includes both `create_no_window`'s bit and a requested
            // `priority`'s class flag (see `extra_creation_flags`).
            let flags = self.extra_creation_flags();
            if flags != 0 {
                use std::os::windows::process::CommandExt;
                // Non-group launch paths only; the group spawn overwrites flags
                // with CREATE_SUSPENDED | these extras.
                cmd.as_std_mut().creation_flags(flags);
            }
        }
        cmd.stdout(match &self.stdout_file {
            Some(file) => Stdio::from(file.open().map_err(Error::io)?),
            None => match self.stdout_mode {
                StdioMode::Piped => Stdio::piped(),
                StdioMode::Inherit => Stdio::inherit(),
                StdioMode::Null => Stdio::null(),
            },
        });
        cmd.stderr(match &self.stderr_file {
            Some(file) => Stdio::from(file.open().map_err(Error::io)?),
            None => match self.stderr_mode {
                StdioMode::Piped => Stdio::piped(),
                StdioMode::Inherit => Stdio::inherit(),
                StdioMode::Null => Stdio::null(),
            },
        });
        if self.keep_stdin_open {
            cmd.stdin(Stdio::piped());
        } else if self.stdin_inherit {
            // Share the parent's own stdin fd — one portable primitive that
            // behaves the same across unix/linux/windows (the sys spawn paths only
            // add group/job containment and never re-wire stdio). The
            // mutual-exclusion with keep_stdin_open / a configured source is
            // enforced at the launch boundary (`runner::take_stdin_for_run`); the
            // ordering here is only the tie-break for the raw `to_tokio_command`
            // escape hatch, which bypasses that check.
            cmd.stdin(Stdio::inherit());
        } else {
            match &self.stdin {
                Some(src) => {
                    cmd.stdin(src.stdio());
                }
                None => {
                    cmd.stdin(Stdio::null());
                }
            }
        }
        Ok(cmd)
    }

    /// The absolute program path [`build_tokio`](Self::build_tokio) substitutes
    /// for a bare-name `program`, or `None` to hand the OS the name verbatim.
    ///
    /// Two kinds of bare name are rewritten so the OS spawns *exactly* what the
    /// spawn-free preflight ([`resolve_program`](Self::resolve_program))
    /// resolved — closing the gap where `which` promised a program the launch
    /// then couldn't reach:
    /// - a [`prefer_local`](Self::prefer_local) match — its resolved absolute
    ///   path, always, independent of extension (the OS never searches for it);
    /// - on Windows, a `PATH` match whose PATHEXT extension is **not** `.exe`
    ///   (`.cmd`/`.bat`/`.com`/…). The OS's own bare-name `PATH` search appends
    ///   only `.exe`, so it would never launch such a program by bare name — yet
    ///   the crate's PATHEXT-aware resolution (the *same* one `which` uses) found
    ///   it. Handing the OS the resolved absolute path closes that divergence
    ///   (std then routes a `.cmd`/`.bat` through `cmd.exe` with BatBadBut-safe
    ///   quoting, exactly as it already does for a `prefer_local` `.cmd`).
    ///
    /// A `.exe` `PATH` match is deliberately left as the bare name: the OS
    /// resolves it — and, on Windows, may prefer the application/current/system
    /// directories this `PATH`-only model doesn't touch — exactly as before, so
    /// that richer OS search is never overridden. A path-form program is never
    /// rewritten here (the OS receives it verbatim).
    ///
    /// Inert on non-Windows: the `PATH`-rewrite branch is `#[cfg(windows)]`
    /// (Unix has no PATHEXT), so a Unix bare name yields only a `prefer_local`
    /// match or `None`, byte-for-byte as before.
    fn spawn_program_override(&self) -> Option<PathBuf> {
        let program = self.program.as_os_str();
        if !is_bare_name(program) {
            return None;
        }
        // A `prefer_local` match is spawned via its resolved absolute path,
        // always — independent of extension and of the child's `PATH`. The
        // emptiness guard skips `probe_prefer_local`'s `current_dir` read on the
        // common no-`prefer_local` path.
        if !self.prefer_local.is_empty()
            && let Some(found) = probe_prefer_local(&self.prefer_local, program)
        {
            return Some(found);
        }
        // Windows-only: rescue a bare name whose only `PATH` match carries a
        // non-`.exe` PATHEXT extension — the OS's `.exe`-only bare-name search
        // would miss it. Resolve against the *same* `resolve_program` the
        // preflight uses (same `prefer_local`, same effective-child-`PATH`
        // source), so a rewrite can never disagree with what `which` reports.
        // `prefer_local` already missed above, so a `Found` here is a `PATH`
        // match; only substitute it when it is not the `.exe` the OS would find
        // on its own.
        #[cfg(windows)]
        {
            if let ProgramResolution::Found(found) =
                resolve_program(program, &self.prefer_local, self.resolution_path_source())
                && !has_exe_extension(&found)
            {
                return Some(found);
            }
        }
        None
    }

    /// The [`PathSource`] this command's bare-name `program` resolves against —
    /// its *effective child* `PATH` when the command relocates `PATH`
    /// ([`env`](Self::env)/[`env_remove`](Self::env_remove) of `PATH`,
    /// [`env_clear`](Self::env_clear), or [`inherit_env`](Self::inherit_env)),
    /// otherwise the process `PATH`. Shared by the spawn-free preflight
    /// ([`resolve_program`](Self::resolve_program)) and the live-launch
    /// [`build_tokio`](Self::build_tokio) rewrite, so both resolve a bare name
    /// against the identical `PATH` list — the single source of the parity
    /// between what `which` reports and what a run actually spawns.
    fn resolution_path_source(&self) -> PathSource {
        if self.customizes_path() {
            // The command moves the child's `PATH` away from the process `PATH`,
            // so resolve against the value the child will actually receive.
            PathSource::Explicit(self.effective_path_value())
        } else {
            // The child inherits the process `PATH`; searching it (via the same
            // `find_in_path` the launch models with) is exact.
            PathSource::ProcessPath
        }
    }

    // --- Detached spawn (deliberately outside kill-on-drop containment) ----

    /// Spawn the child **deliberately released from this crate's kill-on-drop
    /// containment**, handing back a [`DetachedChild`] whose lifetime is entirely
    /// yours: the crate will never kill, reap, time out, or capture it.
    ///
    /// # Warning — this inverts the crate's headline guarantee
    ///
    /// Every other run/`start` verb keeps the child in a kill-on-drop container,
    /// so nothing is orphaned. `spawn_detached` is the crate's **one** deliberate
    /// escape hatch for the legitimate handoff cases — daemonizing, a
    /// `nohup`-style long-lived helper meant to outlive the launcher — where the
    /// child *must* survive its owner. Reach for it only when you truly want that;
    /// for everything else, `start`/`run`/`output_*` are what you want.
    ///
    /// The returned [`DetachedChild`] is a **separate, non-interchangeable type**
    /// (not a [`RunningProcess`]) carrying nothing but the
    /// [`pid`](DetachedChild::pid) — no `kill`, no timeout, no capture, no
    /// teardown — precisely because it is no longer contained.
    ///
    /// # Detach happens *at birth*
    ///
    /// - **Unix** — the child is launched into a **new session** (`setsid`), so it
    ///   has no controlling terminal and its own session/process group; it is not
    ///   tracked by any of this crate's kill-on-drop groups.
    /// - **Windows** — the child is **not assigned** to this crate's Job Object,
    ///   so closing/dropping any handle here cannot kill it. It is **not** made to
    ///   break away from an *external* Job Object the OS already places it in — see
    ///   below.
    ///
    /// # Still bound by a *host* container (by design)
    ///
    /// "Detached" means detached from **this crate's** per-run containment — **not**
    /// from a broader host container your process already lives in. If your process
    /// runs under an external Windows Job Object or a Linux cgroup (a CI runner, a
    /// `systemd` scope, this crate's own supervisor), the child may still inherit
    /// and be bound by it. `spawn_detached` deliberately does **not** attempt a job
    /// breakaway or cgroup escape: that would be hostile to whoever set up the host
    /// containment (and on Windows would simply fail the spawn where breakaway is
    /// disallowed).
    ///
    /// # stdio: null, or a file — never a pipe
    ///
    /// A detached child has no owner draining its output, so a pipe would deadlock
    /// it the moment the buffer fills after you go away (the classic daemon bug).
    /// stdout/stderr are therefore **null by default**; the only alternative is a
    /// **file redirect** ([`stdout_file`](Self::stdout_file) /
    /// [`stderr_file`](Self::stderr_file) and their `_append` forms). stdin is
    /// always null. A pipe or an inherited parent fd is rejected (see below).
    ///
    /// # Rejected configuration — a loud, typed refusal
    ///
    /// A detached child has no owner to enforce a timeout, no pump to capture
    /// output, and no interactive stdin, so a `Command` carrying any of those knobs
    /// is refused with [`ErrorReason::Unsupported`]
    /// naming it — **never** silently ignored (the same "fail loud, don't drop a
    /// requested behavior" contract as `uid`/`gid`/`umask` off Unix). The refused
    /// knobs are: [`timeout`](Self::timeout)/[`timeout_grace`](Self::timeout_grace),
    /// [`retry`](Self::retry)/[`retry_with`](Self::retry_with),
    /// [`cancel_on`](Self::cancel_on),
    /// [`kill_on_parent_death`](Self::kill_on_parent_death) (its exact opposite),
    /// [`windows_graceful_ctrl_break`](Self::windows_graceful_ctrl_break),
    /// [`keep_stdin_open`](Self::keep_stdin_open)/[`inherit_stdin`](Self::inherit_stdin)/
    /// a configured [`stdin`](Self::stdin) source, any capture wiring
    /// ([`on_stdout_line`](Self::on_stdout_line)/[`on_stderr_line`](Self::on_stderr_line),
    /// the tee sinks, a [`capture_policy`](Self::capture_policy)), an inherited
    /// stdout/stderr connection, and (with the `pty` feature) `use_pty`.
    /// Program/argument/env/working-directory and the
    /// privilege-drop knobs ([`uid`](Self::uid)/[`gid`](Self::gid)/
    /// [`groups`](Self::groups)/[`umask`](Self::umask)/[`priority`](Self::priority))
    /// **are** honored — a detached daemon may still drop privileges.
    ///
    /// # Not `async`
    ///
    /// Detaching is fire-and-forget: there is nothing to await and no tokio runtime
    /// is required, so this is a plain synchronous spawn (like the low-level
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape hatch), callable
    /// from daemonizing code that runs before any runtime exists.
    ///
    /// # Errors
    ///
    /// - [`ErrorReason::Unsupported`] — an
    ///   incompatible knob (above), or a POSIX-only privilege primitive requested
    ///   off Unix.
    /// - [`ErrorReason::NotFound`] — the program could
    ///   not be located.
    /// - [`ErrorReason::Spawn`] — the program was located
    ///   but the OS refused to start it (bad working directory, permission denied, a
    ///   Windows `.cmd`/`.bat` that needs `cmd.exe`, …).
    ///
    /// ```no_run
    /// use processkit::Command;
    ///
    /// // Launch a long-lived helper that must outlive this process, logging to a
    /// // file (never a pipe — there is no owner left to drain one).
    /// let child = Command::new("my-daemon")
    ///     .arg("--serve")
    ///     .stdout_file("/var/log/my-daemon.log")
    ///     .spawn_detached()?;
    /// println!("detached daemon pid = {}", child.pid());
    /// // Dropping `child` does NOT kill the daemon — it keeps running.
    /// # Ok::<(), processkit::Error>(())
    /// ```
    pub fn spawn_detached(&self) -> Result<DetachedChild> {
        self.ensure_detach_compatible()?;

        // A missing/non-directory cwd otherwise surfaces as a bare ENOENT,
        // indistinguishable from "program not found" — name the real cause up
        // front, mirroring `runner::launch`.
        if let Some(cwd) = self.working_dir()
            && !cwd.is_dir()
        {
            let (kind, what) = if cwd.exists() {
                (std::io::ErrorKind::NotADirectory, "is not a directory")
            } else {
                (std::io::ErrorKind::NotFound, "does not exist")
            };
            return Err(ErrorReason::Spawn {
                program: self.program_name(),
                source: std::io::Error::new(
                    kind,
                    format!("working directory {what}: {}", cwd.display()),
                ),
            }
            .into());
        }

        let mut cmd = self.build_detached_tokio()?;
        // Spawn via `std` directly: a detached child is deliberately NOT registered
        // with the tokio reactor or assigned to any group of ours — it is fully
        // handed off. `std::process::Child`'s `Drop` neither kills nor waits the
        // child (the OS reaps it — on Unix, `init` once this process exits), so
        // dropping the handle below leaves the child running.
        let child = match cmd.as_std_mut().spawn() {
            Ok(child) => child,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(self.detached_not_found(source));
            }
            Err(source) => {
                return Err(ErrorReason::Spawn {
                    program: self.program_name(),
                    source,
                }
                .into());
            }
        };
        let pid = child.id();
        // Detach: dropping the `std` handle neither kills nor reaps it.
        drop(child);
        Ok(DetachedChild::new(pid))
    }

    /// Reject a `Command` configuration [`spawn_detached`](Self::spawn_detached)
    /// cannot honor — loudly and typed, never silently ignored. Each incompatible
    /// knob surfaces as [`ErrorReason::Unsupported`]
    /// naming it, matching the crate's precedent for a refused spawn-time request
    /// (`uid`/`gid`/`umask` off Unix).
    fn ensure_detach_compatible(&self) -> Result<()> {
        let refuse = |what: &str| -> Result<()> {
            Err(ErrorReason::Unsupported {
                operation: format!("spawn_detached with {what}"),
            }
            .into())
        };

        // Knobs that need an owner/pump the detach path deliberately doesn't keep.
        if self.configured_timeout().is_some() {
            return refuse("a timeout");
        }
        if self.inactivity_timeout.is_some() {
            return refuse("an output-inactivity timeout");
        }
        if self.timeout_grace.is_some() {
            return refuse("a graceful-timeout window");
        }
        if self.retry.is_some() {
            return refuse("a retry policy");
        }
        if self.cancel_token.is_some() {
            return refuse("a cancellation token");
        }
        if self.kill_on_parent_death {
            // The exact inverse of detach — it would kill the child when the owner
            // dies, where detach is precisely about outliving the owner.
            return refuse("kill_on_parent_death");
        }
        if self.windows_graceful_ctrl_break {
            return refuse("windows_graceful_ctrl_break");
        }
        if self.io_priority.is_some() {
            return refuse("io_priority (owner-dependent)");
        }
        if self.cpu_affinity.is_some() {
            return refuse("cpu_affinity (owner-dependent)");
        }
        // stdin can only be null for a detached child.
        if self.keep_stdin_open {
            return refuse("keep_stdin_open");
        }
        if self.stdin_inherit {
            return refuse("inherit_stdin");
        }
        if self.stdin.is_some() {
            return refuse("a stdin source");
        }
        // Any capture wiring: a detached child has no pump to feed it.
        if self.stdout_config.handler.is_some() || self.stderr_config.handler.is_some() {
            return refuse("an output line handler");
        }
        if self.stdout_config.tee.is_some()
            || self.stderr_config.tee.is_some()
            || self.stdout_config.raw_tee.is_some()
            || self.stderr_config.raw_tee.is_some()
        {
            return refuse("an output tee sink");
        }
        if self.capture_policy.is_some() {
            return refuse("a capture policy");
        }
        // Only null (default) or a file redirect is allowed; an inherited
        // stdout/stderr fd would dangle once the owner's console/pipe closes.
        if matches!(self.stdout_mode, StdioMode::Inherit)
            || matches!(self.stderr_mode, StdioMode::Inherit)
        {
            return refuse("an inherited stdout/stderr (use a file redirect, or leave it null)");
        }
        #[cfg(feature = "pty")]
        if self.use_pty {
            return refuse("a pseudo-terminal (use_pty)");
        }

        // POSIX-only privilege primitives are unavailable off Unix — the same loud
        // refusal the run helpers give, so a requested drop is never silently
        // skipped for a detached child either. (On Unix these are honored.)
        #[cfg(not(unix))]
        {
            if self.uid.is_some() {
                return refuse("uid (POSIX-only)");
            }
            if self.gid.is_some() {
                return refuse("gid (POSIX-only)");
            }
            if self.groups.is_some() {
                return refuse("supplementary groups (POSIX-only)");
            }
            if self.umask.is_some() {
                return refuse("umask (POSIX-only)");
            }
            if self.setsid {
                return refuse("setsid (POSIX-only)");
            }
        }

        Ok(())
    }

    /// Build the `tokio` command for [`spawn_detached`](Self::spawn_detached): the
    /// full program/env/privilege-drop build from [`build_tokio`](Self::build_tokio),
    /// but with a **new session forced on Unix** (`setsid` — detach at birth) and
    /// stdio rewired to the detached policy (null, or a file redirect — never a
    /// pipe or an inherited fd, which would deadlock or dangle once the owner is
    /// gone). Incompatible knobs were already rejected by
    /// [`ensure_detach_compatible`](Self::ensure_detach_compatible).
    fn build_detached_tokio(&self) -> Result<tokio::process::Command> {
        let mut base = self.clone();
        // Detach at birth: force a new session on Unix even if `.setsid()` was not
        // called. Idempotent with an explicit call — the field just gates the one
        // `setsid` pre-exec hook `build_tokio` installs. (Off Unix there is no
        // session mechanism; detach there is "not assigned to a Job Object", which
        // this path achieves by never creating one.)
        #[cfg(unix)]
        {
            base.setsid = true;
        }
        // Detached stdio policy: keep any file redirect; force everything else to
        // null. The only stdout/stderr connection that can reach here is the
        // default `Piped` (neutralized to null — a detached child must never own a
        // pipe with no reader) or an explicit file (kept). stdin is always null.
        base.stdin = None;
        base.keep_stdin_open = false;
        base.stdin_inherit = false;
        if base.stdout_file.is_none() {
            base.stdout_mode = StdioMode::Null;
        }
        if base.stderr_file.is_none() {
            base.stderr_mode = StdioMode::Null;
        }
        base.build_tokio()
    }

    /// Map an OS `NotFound` spawn failure to the enriched reason a live launch
    /// would produce — [`NotFound`](crate::ErrorReason::NotFound) (with the
    /// `searched` directories for a bare name) when the program truly isn't
    /// resolvable, or [`Spawn`](crate::ErrorReason::Spawn) when it resolves but the
    /// OS still refused it directly (a `.cmd`/`.bat` on Windows). Mirrors
    /// `runner::launch`'s post-spawn translation, sharing the same spawn-free
    /// [`resolve_program`] so the two can't disagree.
    fn detached_not_found(&self, source: std::io::Error) -> Error {
        if is_bare_name(&self.program) {
            let path = if self.customizes_path() {
                PathSource::Skip
            } else {
                PathSource::ProcessPath
            };
            return match resolve_program(&self.program, &self.prefer_local, path) {
                ProgramResolution::Found(_) => ErrorReason::Spawn {
                    program: self.program_name(),
                    source,
                }
                .into(),
                ProgramResolution::NotFound { searched } => ErrorReason::NotFound {
                    program: self.program_name(),
                    searched,
                }
                .into(),
            };
        }
        ErrorReason::NotFound {
            program: self.program_name(),
            searched: None,
        }
        .into()
    }

    // --- Live handle (private one-shot group) ------------------------------

    /// Start the command and return a live [`RunningProcess`] backed by a fresh
    /// private group. Use this for streaming stdout
    /// ([`RunningProcess::stdout_lines`]) or inspecting the process while it
    /// runs; keep the handle in scope, as dropping it tears the tree down.
    ///
    /// # Errors
    ///
    /// The launch surface shared by every run verb on `Command`:
    ///
    /// - [`ErrorReason::NotFound`] — the program could not be located (not installed,
    ///   not on `PATH`, or the given path does not resolve to an executable).
    /// - [`ErrorReason::Spawn`] — the program was located but the OS refused to start
    ///   it (permission denied, a missing or non-directory working directory, a
    ///   Windows `.cmd`/`.bat` that needs `cmd.exe`, …).
    /// - [`ErrorReason::Unsupported`] — a requested POSIX-only primitive (running as
    ///   another user/group, a new session via `setsid`, or a `umask`) is not
    ///   available on this platform.
    /// - [`ErrorReason::Cancelled`] — the [`cancel_on`](Self::cancel_on) token was
    ///   already cancelled before the spawn.
    /// - [`ErrorReason::Io`] — the private [`ProcessGroup`](crate::ProcessGroup) backing
    ///   the run could not be created, or a one-shot streaming stdin source
    ///   ([`Stdin::from_reader`](crate::Stdin::from_reader) /
    ///   [`Stdin::from_lines`](crate::Stdin::from_lines)) was already consumed by
    ///   a previous run.
    #[cfg_attr(
        feature = "limits",
        doc = "- [`ErrorReason::ResourceLimit`] — a resource cap configured on the run's group could not be enforced."
    )]
    pub async fn start(&self) -> Result<RunningProcess> {
        JobRunner::new().start(self).await
    }

    // --- High-level run helpers (private one-shot group) -------------------

    /// Run to completion and capture stdout as text, stderr, and the exit code.
    /// A non-zero exit is reported, not raised — call
    /// [`ProcessResult::ensure_success`] to turn it into an error.
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start). A non-zero exit, a
    /// timeout, and a signal-kill are *captured* in the returned
    /// [`ProcessResult`] rather than raised (call
    /// [`ensure_success`](crate::ProcessResult::ensure_success) to promote them);
    /// beyond launch, only [`ErrorReason::Cancelled`] (a cancellation is always
    /// raised), [`ErrorReason::OutputTooLarge`] (a fail-loud buffer overflowed),
    /// [`ErrorReason::Stdin`] (a non-broken-pipe stdin failure on an
    /// otherwise-successful run), and [`ErrorReason::Io`] surface.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().start(self).await?.output_string().await
    }

    /// Run to completion and capture stdout as raw bytes (plus stderr/exit code).
    ///
    /// # Errors
    ///
    /// Identical to [`output_string`](Self::output_string) — a non-zero exit, a
    /// timeout, or a signal-kill is captured in the [`ProcessResult`], not raised
    /// — except that a fail-loud [`ErrorReason::OutputTooLarge`] applies to the raw
    /// stdout *byte* ceiling.
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        JobRunner::new().start(self).await?.output_bytes().await
    }

    /// Run to completion and return just the exit code (output is discarded). A
    /// run that yields no code surfaces as an error — a timeout as
    /// [`ErrorReason::Timeout`], a signal-kill as
    /// [`ErrorReason::Signalled`] — consistent with
    /// [`ProcessRunnerExt::exit_code`] and
    /// [`CliClient::exit_code`](crate::CliClient::exit_code).
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start), plus — when the run
    /// produced no code — [`ErrorReason::Timeout`] (the deadline elapsed),
    /// [`ErrorReason::Signalled`] (killed by a signal), or [`ErrorReason::Cancelled`]. A
    /// non-zero exit is returned as the code, not raised.
    pub async fn exit_code(&self) -> Result<i32> {
        JobRunner::new().exit_code(self).await
    }

    /// Run to completion, requiring an **accepted** exit (`0` by default, widened
    /// by [`ok_codes`](Self::ok_codes)), and return trimmed stdout. Any other
    /// code is [`ErrorReason::Exit`].
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start), plus the
    /// success-checking failures: [`ErrorReason::Exit`] (a non-accepted exit code),
    /// [`ErrorReason::Signalled`] (a signal-kill), [`ErrorReason::Timeout`] (the deadline
    /// elapsed — *raised* here, unlike on
    /// [`output_string`](Self::output_string)), [`ErrorReason::Cancelled`],
    /// [`ErrorReason::OutputTooLarge`] (a fail-loud buffer truncated the presented
    /// stdout), and [`ErrorReason::Stdin`] (a non-broken-pipe stdin failure on an
    /// otherwise-successful run).
    pub async fn run(&self) -> Result<String> {
        JobRunner::new().run(self).await
    }

    /// Run to completion, require an **accepted** exit, and return the full
    /// captured [`ProcessResult`] (untrimmed stdout) — the building block when you
    /// need the whole result after success-checking rather than trimmed stdout
    /// ([`run`](Self::run)) or the raw result ([`output_string`](Self::output_string)).
    /// Consistent with [`ProcessRunnerExt::checked`]
    /// and [`CliClient::checked`](crate::CliClient::checked).
    ///
    /// # Errors
    ///
    /// The same success-checking surface as [`run`](Self::run) —
    /// [`ErrorReason::Exit`] / [`ErrorReason::Signalled`] / [`ErrorReason::Timeout`] /
    /// [`ErrorReason::Cancelled`] / [`ErrorReason::Stdin`], atop the launch failures on
    /// [`start`](Self::start) — except that, as the lenient building block,
    /// `checked` does **not** fail loud on a bounded-buffer truncation (inspect
    /// [`ProcessResult::truncated`](crate::ProcessResult::truncated) yourself), so
    /// it never returns [`ErrorReason::OutputTooLarge`].
    pub async fn checked(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().checked(self).await
    }

    /// Run for the side effect: require an **accepted** exit (`0`, or any code in
    /// [`ok_codes`](Self::ok_codes)) and discard the output. Consistent with
    /// [`ProcessRunnerExt::run_unit`] and
    /// [`CliClient::run_unit`](crate::CliClient::run_unit).
    ///
    /// # Errors
    ///
    /// The same surface as [`checked`](Self::checked) (the launch failures on
    /// [`start`](Self::start) plus [`ErrorReason::Exit`] / [`ErrorReason::Signalled`] /
    /// [`ErrorReason::Timeout`] / [`ErrorReason::Cancelled`] / [`ErrorReason::Stdin`]); only the
    /// captured output is discarded.
    pub async fn run_unit(&self) -> Result<()> {
        JobRunner::new().run_unit(self).await
    }

    /// Run a predicate command and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err` (any other code
    /// as [`ErrorReason::Exit`], a timeout as [`ErrorReason::Timeout`],
    /// a signal-kill as [`ErrorReason::Signalled`]). For tools
    /// whose exit code *is* the answer —
    /// `git diff --quiet`, `git show-ref --verify --quiet`, `grep -q`, …
    ///
    /// # Errors
    ///
    /// Any exit code other than `0`/`1` becomes [`ErrorReason::Exit`], and — atop the
    /// launch failures on [`start`](Self::start) — a run that produced no code
    /// errors as [`ErrorReason::Timeout`], [`ErrorReason::Signalled`], or
    /// [`ErrorReason::Cancelled`]. The strict `0`/`1` contract holds regardless of the
    /// command's [`ok_codes`](Self::ok_codes).
    pub async fn probe(&self) -> Result<bool> {
        JobRunner::new().probe(self).await
    }

    /// Run (requiring an **accepted** exit) and feed stdout to an **infallible**
    /// `parse` closure, returning the parsed value. Fails loud on a bounded-buffer
    /// truncation so the parser never sees a clipped tail. Consistent with
    /// [`ProcessRunnerExt::parse`] and
    /// [`CliClient::parse`](crate::CliClient::parse).
    ///
    /// # Errors
    ///
    /// The success-checking surface of [`run`](Self::run) (the launch failures on
    /// [`start`](Self::start), plus [`ErrorReason::Exit`] / [`ErrorReason::Signalled`] /
    /// [`ErrorReason::Timeout`] / [`ErrorReason::Cancelled`] / [`ErrorReason::Stdin`]), plus
    /// [`ErrorReason::OutputTooLarge`] when a fail-loud buffer truncated the stdout the
    /// parser would see. The `parse` closure is infallible, so it adds no error.
    pub async fn parse<T, F>(&self, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> T + Send,
    {
        JobRunner::new().parse(self, parse).await
    }

    /// Run (requiring an **accepted** exit) and feed stdout to a *fallible*
    /// `parse` closure (the JSON-deserialization shape; a failure becomes
    /// [`ErrorReason::Parse`] or whatever the closure returns).
    /// Fails loud on truncation. Consistent with
    /// [`ProcessRunnerExt::try_parse`] and
    /// [`CliClient::try_parse`](crate::CliClient::try_parse).
    ///
    /// # Errors
    ///
    /// Everything [`parse`](Self::parse) can return, plus whatever the fallible
    /// `parse` closure yields on malformed output — typically
    /// [`ErrorReason::Parse`].
    pub async fn try_parse<T, F>(&self, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> Result<T> + Send,
    {
        JobRunner::new().try_parse(self, parse).await
    }

    /// Return the first stdout line matching `predicate` (or the first line when
    /// the predicate is trivial), then tear the process down.
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start), plus
    /// [`ErrorReason::Timeout`] when a [`timeout`](Self::timeout) is set and its
    /// deadline elapses mid-stream (which tears the process down),
    /// [`ErrorReason::Cancelled`], or [`ErrorReason::Io`] while streaming. A stream that ends
    /// with no match is `Ok(None)`, not an error.
    pub async fn first_line<F>(&self, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool + Send,
    {
        // Delegate to the `ProcessRunnerExt` seam so the streaming-search logic
        // lives in one place and stays exercisable with any runner.
        JobRunner::new().first_line(self, predicate).await
    }

    /// Resolve this command's `program` to a concrete executable path **without
    /// launching it** — a spawn-free preflight, for a *doctor* / early-diagnosis
    /// check ("is `git` installed?") that must have **no** side effects. Unlike
    /// [`probe`](Self::probe) (which actually runs the tool), this only *locates*
    /// it — no process is ever started.
    ///
    /// Resolution is byte-for-byte the same as the one the real launch performs,
    /// because it reuses the *same* internal logic — not a second copy: a bare
    /// name is resolved against this command's [`prefer_local`](Self::prefer_local)
    /// directories first (in priority order), then the `PATH`, honoring PATHEXT on
    /// Windows and the execute bit on Unix; a path-form `program` (absolute, or
    /// relative with a separator) is probed directly, exactly as the OS receives
    /// it. When the command has **relocated** the child's `PATH`
    /// ([`env`](Self::env)/[`env_remove`](Self::env_remove) of `PATH`,
    /// [`env_clear`](Self::env_clear), or [`inherit_env`](Self::inherit_env)),
    /// the lookup runs against that *effective child* `PATH`, so preflight never
    /// disagrees with which list the spawn searches.
    ///
    /// A resolved **hit** is exactly what a run spawns, at that same path — on
    /// Windows including a bare name found only through a non-`.exe` PATHEXT
    /// extension (`.cmd`/`.bat`/`.com`/…): the launch substitutes the resolved
    /// absolute path (the OS's own bare-name search appends only `.exe`), so such
    /// a hit spawns instead of raising [`ErrorReason::Spawn`]. The one residual
    /// asymmetry is a preflight **miss** on Windows: the OS can still locate a
    /// bare name through the application directory, the current directory, or the
    /// system directories — routes this `PATH`-based model doesn't cover — so a
    /// miss there is not proof a run couldn't launch it. Unix (`execvp`,
    /// `PATH`-only) has no such gap.
    ///
    /// On success returns the resolved **absolute** path. This is a synchronous,
    /// cheap filesystem probe (a few `stat`s) — no async runtime is required.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::NotFound`] when the program can't be
    /// located — not installed, not on `PATH`, or a path that doesn't resolve to
    /// an executable. Its `searched` field lists the directories that were
    /// checked (`prefer_local` first, then `PATH`) for a bare-name lookup, and is
    /// `None` for a path-form program; [`is_not_found`](crate::Error::is_not_found)
    /// classifies it, exactly as it would for the same missing program on a real
    /// run.
    pub fn resolve_program(&self) -> Result<PathBuf> {
        // Resolve against the same `PATH` source the live launch's
        // `build_tokio` rewrite uses (`resolution_path_source`), so preflight
        // and spawn can never disagree about which list a bare name is found in.
        let path = self.resolution_path_source();
        match resolve_program(self.program.as_os_str(), &self.prefer_local, path) {
            ProgramResolution::Found(found) => Ok(found),
            ProgramResolution::NotFound { searched } => Err(ErrorReason::NotFound {
                program: self.program_name(),
                searched,
            }
            .into()),
        }
    }

    /// The value of the child's `PATH` after this command's env ops — the exact
    /// `PATH` the OS would search at spawn, for [`resolve_program`](Self::resolve_program)
    /// to resolve against when the command has relocated it. Mirrors
    /// [`build_tokio`](Self::build_tokio)'s env application for the single `PATH`
    /// key: start from the inherited base (empty under
    /// [`env_clear`](Self::env_clear)/[`inherit_env`](Self::inherit_env), else the
    /// process env), let an `inherit_env` allow-list reintroduce the parent
    /// `PATH`, then apply the per-command [`env`](Self::env)/[`env_remove`](Self::env_remove)
    /// ops in order (last write wins). Case-insensitive `PATH` matching on Windows.
    fn effective_path_value(&self) -> Option<OsString> {
        let path_key = OsStr::new("PATH");
        // Base: what the child inherits before per-command env ops. `env_clear`
        // and `inherit_env` both start the child from a clean slate; only an
        // `inherit_env` allow-list that names `PATH` reintroduces it from the
        // parent (build_tokio copies each listed var from the parent env).
        let mut value: Option<OsString> = if self.env_clear || self.inherit_env.is_some() {
            self.inherit_env
                .as_deref()
                .filter(|names| names.iter().any(|n| env_key_eq(n, path_key)))
                .and_then(|_| std::env::var_os("PATH"))
        } else {
            std::env::var_os("PATH")
        };
        // Per-command env ops win, in registration order (a later op supersedes
        // an earlier one for the same key).
        for (key, v) in &self.envs {
            if env_key_eq(key, path_key) {
                value = v.clone();
            }
        }
        value
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never render argv or env *values* in Debug — they may carry secrets
        // (the crate-wide rule). Surface the argument *count* and env *names*;
        // `command_line()` is the explicit secret-bearing escape hatch for argv.
        let mut d = f.debug_struct("Command");
        d.field("program", &self.program)
            .field("args", &self.args.len())
            .field("cwd", &self.cwd)
            .field("prefer_local", &self.prefer_local)
            .field("env_names", &redacted_env_names(&self.envs))
            .field("env_clear", &self.env_clear)
            .field("stdin", &self.stdin)
            .field("keep_stdin_open", &self.keep_stdin_open)
            .field("stdin_inherit", &self.stdin_inherit)
            .field("unchecked", &self.unchecked)
            .field("timeout", &self.timeout)
            .field("inactivity_timeout", &self.inactivity_timeout)
            .field("timeout_grace", &self.timeout_grace)
            .field("ok_codes", &self.ok_codes)
            .field("stdout_mode", &self.stdout_mode)
            .field("stderr_mode", &self.stderr_mode)
            .field("stdout_file", &self.stdout_file)
            .field("stderr_file", &self.stderr_file)
            .field("has_stdout_handler", &self.stdout_config.handler.is_some())
            .field("has_stderr_handler", &self.stderr_config.handler.is_some())
            .field("has_stdout_tee", &self.stdout_config.tee.is_some())
            .field("has_stderr_tee", &self.stderr_config.tee.is_some())
            .field("has_stdout_raw_tee", &self.stdout_config.raw_tee.is_some())
            .field("has_stderr_raw_tee", &self.stderr_config.raw_tee.is_some())
            .field("output_buffer", &self.output_buffer)
            // The named seam is introspectable, unlike an opaque closure: report
            // its `name()` (never captured data) when a policy is installed.
            .field(
                "capture_policy",
                &self.capture_policy.as_ref().map(|p| p.name()),
            )
            .field("stdout_encoding", &self.stdout_config.encoding.name())
            .field("stderr_encoding", &self.stderr_config.encoding.name())
            .field("stdout_line_terminator", &self.stdout_config.terminator)
            .field("stderr_line_terminator", &self.stderr_config.terminator)
            .field("stdout_sanitize_vt", &self.stdout_config.sanitize_vt)
            .field("stderr_sanitize_vt", &self.stderr_config.sanitize_vt)
            .field("has_retry", &self.retry.is_some())
            .field("inherit_env", &self.inherit_env)
            .field("uid", &self.uid)
            .field("gid", &self.gid)
            // Security-relevant: the supplementary-group set of a privilege drop.
            .field("groups", &self.groups)
            .field("setsid", &self.setsid)
            .field("priority", &self.priority)
            .field("cpu_affinity", &self.cpu_affinity)
            .field("io_priority", &self.io_priority)
            .field("umask", &self.umask)
            .field("kill_on_parent_death", &self.kill_on_parent_death)
            .field("creation_flags_extra", &self.creation_flags_extra)
            .field(
                "windows_graceful_ctrl_break",
                &self.windows_graceful_ctrl_break,
            );
        #[cfg(feature = "process-control")]
        d.field("timeout_signal", &self.timeout_signal);
        d.field("has_cancel_token", &self.cancel_token.is_some());
        d.finish_non_exhaustive()
    }
}

/// Render env *names* (sorted, deduped) for a redacted `Debug` — values are
/// never shown. Shared by `Command`, `CliClient`, and `Invocation` so
/// the redaction lives in one audited place.
pub(crate) fn redacted_env_names(
    envs: &[(OsString, Option<OsString>)],
) -> Vec<std::borrow::Cow<'_, str>> {
    let mut names: Vec<std::borrow::Cow<'_, str>> = envs
        .iter()
        .map(|(name, _value)| name.to_string_lossy())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Compare two environment-variable names with the platform's case rules:
/// case-insensitive on Windows (where env names are), case-sensitive elsewhere.
/// Used to decide whether a command already sets a key before filling a client
/// default for it, and by [`Invocation`](crate::testing::Invocation)'s env
/// assertions so a test double reads the same effective key a spawn would. A
/// non-UTF-8 name on Windows falls back to exact bytes.
pub(crate) fn env_key_eq(a: &OsStr, b: &OsStr) -> bool {
    #[cfg(windows)]
    {
        match (a.to_str(), b.to_str()) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            _ => a == b,
        }
    }
    #[cfg(not(windows))]
    {
        a == b
    }
}

/// Render one argument shell-quoted for **display** (POSIX single-quote rules).
/// Not a security boundary — the crate never invokes a shell; this only makes a
/// `command_line()` echo readable and unambiguous.
#[cfg(unix)]
fn quote_arg(arg: &str) -> String {
    // Bare when entirely shell-safe; else single-quote, rewriting `'` as `'\''`.
    let safe = !arg.is_empty()
        && arg.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'@' | b'%' | b'_' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        });
    if safe {
        return arg.to_owned();
    }
    let mut out = String::with_capacity(arg.len() + 2);
    out.push('\'');
    for ch in arg.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Render one argument quoted for **display** on Windows (double-quote rules,
/// best-effort). Not a security boundary — the crate never invokes a shell.
/// Handles common cases: whitespace, `"`, and trailing backslashes. CMD-special
/// characters (`%`, `!`, `(`, `)`) inside a quoted argument are not escaped.
#[cfg(not(unix))]
fn quote_arg(arg: &str) -> String {
    let needs_quote = arg.is_empty()
        || arg
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '"' | '^' | '&' | '|' | '<' | '>' | '%'));
    if !needs_quote {
        return arg.to_owned();
    }
    // MSVCRT `CommandLineToArgvW` backslash rule: a run of backslashes is literal
    // unless it precedes a `"`, in which case each backslash is doubled and the
    // quote is escaped `\"`. A run before the *closing* quote we add is likewise
    // doubled. Buffer the current run so backslashes-before-a-quote (e.g. `a\"b`)
    // are handled, not just trailing ones.
    let mut out = String::with_capacity(arg.len() + 4);
    out.push('"');
    let mut backslashes = 0usize;
    for ch in arg.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                // Preceding backslashes double, then the quote is escaped.
                for _ in 0..backslashes * 2 + 1 {
                    out.push('\\');
                }
                out.push('"');
                backslashes = 0;
            }
            _ => {
                for _ in 0..backslashes {
                    out.push('\\');
                }
                out.push(ch);
                backslashes = 0;
            }
        }
    }
    // Trailing backslashes precede the closing quote → double them.
    for _ in 0..backslashes * 2 {
        out.push('\\');
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// PATH resolution helpers (used to enrich a not-found spawn error in runner.rs)
// ---------------------------------------------------------------------------

/// Whether `program` is a bare name (exactly one `Normal` path component) that
/// should be looked up on `PATH`. Absolute and relative paths return `false`.
pub(crate) fn is_bare_name(program: &OsStr) -> bool {
    use std::path::{Component, Path};
    // components() normalizes trailing separators away ("git/" → Normal("git")),
    // so check raw bytes first: any separator makes it path-ish.
    let bytes = program.as_encoded_bytes();
    if bytes.contains(&b'/') {
        return false;
    }
    #[cfg(windows)]
    if bytes.contains(&b'\\') {
        return false;
    }
    let mut comps = Path::new(program).components();
    matches!(comps.next(), Some(Component::Normal(_))) && comps.next().is_none()
}

/// Search the process's own `PATH` for an executable named `program` (bare
/// name, no separators) — the process-`PATH` specialization of
/// [`find_in_path_in`].
///
/// Returns `(found, searched)`:
/// - `found` — the resolved absolute path when the program is on `PATH`.
/// - `searched` — the raw `PATH` value (for the error message when not found).
pub(crate) fn find_in_path(program: &OsStr) -> (Option<std::path::PathBuf>, String) {
    find_in_path_in(program, std::env::var_os("PATH").as_deref())
}

/// Search a specific `PATH` **value** for an executable named `program` (bare
/// name, no separators). The single `PATH`/PATHEXT/execute-bit search shared by
/// [`find_in_path`] (which passes the process `PATH`) and the spawn-free
/// preflight (which passes a command's *effective child* `PATH` when it has
/// relocated it) — so both resolve a bare name through exactly the same
/// [`probe_dir`] logic, never a second copy.
///
/// An absent or empty `PATH` value yields `(None, String::new())` — no
/// directories to search or to name — matching the process-`PATH` behavior.
pub(crate) fn find_in_path_in(
    program: &OsStr,
    path_value: Option<&OsStr>,
) -> (Option<std::path::PathBuf>, String) {
    let path_var = match path_value {
        Some(p) if !p.is_empty() => p,
        _ => return (None, String::new()),
    };
    let searched = path_var.to_string_lossy().into_owned();
    for dir in std::env::split_paths(path_var) {
        if let Some(found) = probe_dir(&dir, program) {
            return (Some(found), searched);
        }
    }
    (None, searched)
}

/// Which `PATH` a [`resolve_program`] call searches for a bare name — the one
/// knob that lets the spawn-free preflight resolve against the *same* `PATH` the
/// launch will, without a second search implementation.
pub(crate) enum PathSource {
    /// Search the process's own `PATH` (via [`find_in_path`]) — the child `PATH`
    /// when the command hasn't relocated it, so it faithfully models the launch.
    ProcessPath,
    /// Search this explicit `PATH` value — a command's computed *effective
    /// child* `PATH` (see [`Command::effective_path_value`]), used by preflight
    /// when the command relocates `PATH` (`env`/`env_remove`/`env_clear`/
    /// `inherit_env`) so the process `PATH` would be the wrong list.
    Explicit(Option<OsString>),
    /// Do **not** search a `PATH` — report only the `prefer_local` directories
    /// in `searched`. Used by the launch-path `NotFound` enrichment for a
    /// command that relocated its child `PATH`: the OS already searched the
    /// child `PATH` and came up empty, and the *process* `PATH` (all
    /// [`find_in_path`] can read) is the wrong list to name here.
    Skip,
}

/// The outcome of resolving a command's `program` to a concrete executable path
/// **without spawning it** — the single decision the live launch path's
/// [`ErrorReason::NotFound`] enrichment (in `runner.rs`) and
/// the spawn-free [`which`](crate::which) / [`Command::resolve_program`]
/// preflight both derive from, so the two can never disagree about whether a
/// program is available.
pub(crate) enum ProgramResolution {
    /// Resolved to this absolute path — a `prefer_local` match, a `PATH`/PATHEXT
    /// hit, or (for a path-form program) the path itself. The launch spawns a
    /// `prefer_local` match via this exact path and lets the OS resolve a bare
    /// name the same way [`find_in_path_in`] models; preflight returns it.
    Found(PathBuf),
    /// Not resolvable. `searched` is the directory list for
    /// [`ErrorReason::NotFound`]'s field: `Some` for a
    /// bare-name `PATH` lookup (the searched dirs, `prefer_local` first),
    /// `None` for a path-form program (no `PATH` search applied).
    NotFound { searched: Option<String> },
}

/// Resolve `program` to a concrete executable path without spawning, composing
/// the *same* primitives the live launch uses — [`probe_prefer_local`] (the
/// exact resolution `build_tokio` spawns a `prefer_local` match through),
/// [`find_in_path_in`]/[`probe_dir`] (the `PATH`/PATHEXT/execute-bit probe), and
/// [`probe_path_form`] for a path-form program. There is no second copy of the
/// resolution logic, so the spawn-free preflight built on this can never diverge
/// from the actual launch.
///
/// A **bare name** is looked up `prefer_local` directories first (in priority
/// order), then the `PATH` per `path`. A **path-form** program (absolute, or
/// relative with a separator) is never looked up on `PATH` — it is probed
/// directly, mirroring how the OS receives it verbatim at spawn.
pub(crate) fn resolve_program(
    program: &OsStr,
    prefer_local: &[PathBuf],
    path: PathSource,
) -> ProgramResolution {
    if !is_bare_name(program) {
        return match probe_path_form(program) {
            Some(found) => ProgramResolution::Found(found),
            None => ProgramResolution::NotFound { searched: None },
        };
    }
    // A bare name: `prefer_local` directories are parent-side plain filesystem
    // probes, independent of the child's environment, so they always apply —
    // even under a relocated child `PATH`.
    if let Some(found) = probe_prefer_local(prefer_local, program) {
        return ProgramResolution::Found(found);
    }
    let (found, path_searched) = match path {
        PathSource::Skip => {
            // No `PATH` search: only the `prefer_local` directories are safe to
            // name (the process `PATH` would be the wrong list for a relocated
            // child `PATH`).
            let prefer = prepend_prefer_local_to_searched(prefer_local, "");
            let searched = if prefer.is_empty() {
                None
            } else {
                Some(prefer)
            };
            return ProgramResolution::NotFound { searched };
        }
        PathSource::ProcessPath => find_in_path(program),
        PathSource::Explicit(value) => find_in_path_in(program, value.as_deref()),
    };
    if let Some(found) = found {
        return ProgramResolution::Found(found);
    }
    let searched = prepend_prefer_local_to_searched(prefer_local, &path_searched);
    ProgramResolution::NotFound {
        searched: Some(searched),
    }
}

/// Resolve a **path-form** program (absolute, or relative with a separator) to
/// an absolute executable path without spawning — the path-form companion to the
/// bare-name [`find_in_path_in`]/[`probe_prefer_local`] lookups. The launch
/// hands such a program to the OS verbatim (no `PATH` search), so this probes the
/// path itself with the very same per-file executability predicate
/// [`probe_dir`] applies (execute-bit on Unix, PATHEXT-aware on Windows) and
/// absolutizes a relative match against the process's current directory
/// (mirroring [`probe_prefer_local`]), so the returned path is absolute and
/// can't later be reinterpreted.
fn probe_path_form(program: &OsStr) -> Option<PathBuf> {
    // A trailing separator names a directory, never an executable file.
    let bytes = program.as_encoded_bytes();
    if matches!(bytes.last(), Some(b'/')) {
        return None;
    }
    #[cfg(windows)]
    if matches!(bytes.last(), Some(b'\\')) {
        return None;
    }
    let path = Path::new(program);
    // Absolutize a relative path against the process cwd so the match is
    // absolute (and thus can't be reinterpreted against a child's `current_dir`
    // later), exactly as `probe_prefer_local` does for a relative directory.
    let absolute: PathBuf = if path.is_absolute() {
        path.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path.to_path_buf()
    };
    let parent = absolute.parent()?;
    let file = absolute.file_name()?;
    probe_dir(parent, file)
}

/// Probe `dirs` in order for `program` (a bare name), reusing [`probe_dir`] —
/// the same PATHEXT-aware lookup the `PATH` search uses, not a separate copy.
/// Returns the first match, i.e. the earliest directory in priority order
/// (see [`Command::prefer_local`]).
///
/// A relative `dir` (e.g. `"./node_modules/.bin"`, the form used throughout
/// the docs) is probed exactly as before — against the process's actual
/// current directory, unaffected by anything set via
/// [`Command::current_dir`] — but the returned match is always made
/// **absolute** first, by joining it onto that same current directory. A
/// relative match handed unchanged to `Command::new` would later be
/// reinterpreted at spawn time against the *child's* working directory (on
/// Unix) or stay relative to the parent's cwd only by accident (on Windows)
/// once [`Command::current_dir`] is set — a divergence between "what
/// `probe_dir` verified exists" and "what the OS actually spawns". Making it
/// absolute here closes that gap regardless of platform. If the current
/// directory can't be read (rare), the relative path is probed as-is, same
/// as before this existed.
pub(crate) fn probe_prefer_local(dirs: &[PathBuf], program: &OsStr) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok();
    dirs.iter().find_map(|dir| {
        let absolutized;
        let probe_target: &Path = if dir.is_absolute() {
            dir
        } else if let Some(cwd) = &cwd {
            absolutized = cwd.join(dir);
            &absolutized
        } else {
            dir
        };
        probe_dir(probe_target, program)
    })
}

/// Build the combined `searched` diagnostic for
/// [`ErrorReason::NotFound`]: the [`Command::prefer_local`]
/// directories (first, in priority order) followed by the `PATH` directories
/// (`path_searched`, as returned by [`find_in_path`]) — joined by the
/// platform's `PATH`-list separator (`:` on Unix, `;` on Windows), matching
/// `find_in_path`'s own format. An empty `prefer_local` leaves `path_searched`
/// unchanged.
pub(crate) fn prepend_prefer_local_to_searched(
    prefer_local: &[PathBuf],
    path_searched: &str,
) -> String {
    if prefer_local.is_empty() {
        return path_searched.to_string();
    }
    const SEP: char = if cfg!(windows) { ';' } else { ':' };
    let prefer_str = prefer_local
        .iter()
        .map(|d| d.to_string_lossy())
        .collect::<Vec<_>>()
        .join(&SEP.to_string());
    if path_searched.is_empty() {
        prefer_str
    } else {
        format!("{prefer_str}{SEP}{path_searched}")
    }
}

/// Check whether `program` is an executable in `dir`.
#[cfg(unix)]
fn probe_dir(dir: &std::path::Path, program: &OsStr) -> Option<std::path::PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let candidate = dir.join(program);
    std::fs::metadata(&candidate)
        .ok()
        .filter(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .map(|_| candidate)
}

/// Check whether `program` (with PATHEXT expansion) exists in `dir`.
#[cfg(not(unix))]
fn probe_dir(dir: &std::path::Path, program: &OsStr) -> Option<std::path::PathBuf> {
    let pathext = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
    let carries_exec_ext = |path: &std::path::Path| -> bool {
        path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
            pathext
                .split(';')
                .map(|pe| pe.trim_start_matches('.'))
                .any(|pe| !pe.is_empty() && pe.eq_ignore_ascii_case(e))
        })
    };
    // Exact name first — but only accept it if it already carries a
    // recognized executable extension (handles `git.exe`, `git.cmd`, ...). A
    // same-named file with no extension (or an unrecognized one) is not
    // directly executable on Windows, so it must not be reported as found —
    // that would falsely short-circuit the PATHEXT search below and turn a
    // genuinely missing `git.exe` into a false "found but not executable".
    let candidate = dir.join(program);
    if carries_exec_ext(&candidate) && candidate.is_file() {
        return Some(candidate);
    }
    // Then each PATHEXT extension appended to the bare name.
    for ext in pathext.split(';') {
        if ext.is_empty() {
            continue;
        }
        let mut name = program.to_os_string();
        name.push(ext);
        let candidate = dir.join(&name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Whether `path`'s extension is `.exe` (ASCII case-insensitive) — the single
/// extension the OS's own bare-name `PATH` search appends. A `.exe` `PATH` match
/// therefore needs no rewrite at spawn (the OS locates it by bare name); every
/// other executable extension a PATHEXT probe can return (`.cmd`/`.bat`/`.com`/…)
/// does, since the OS would never reach it by bare name — see
/// [`Command::spawn_program_override`]. Windows-only: Unix has no PATHEXT and
/// never calls this.
#[cfg(windows)]
fn has_exe_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("exe"))
}

#[cfg(test)]
mod tests {
    use super::Command;
    use crate::buffer::LineTerminator;
    use std::ffi::OsStr;
    use std::path::PathBuf;

    #[test]
    fn effective_stdin_source_respects_keep_stdin_open() {
        let absent = Command::new("tool");
        assert!(absent.effective_stdin_source().is_none());

        let configured = Command::new("tool").stdin(crate::Stdin::from_string("input"));
        assert!(configured.stdin_source().is_some());
        assert!(configured.effective_stdin_source().is_some());

        let open_without_source = Command::new("tool").keep_stdin_open();
        assert!(open_without_source.effective_stdin_source().is_none());

        let open_with_source = Command::new("tool")
            .stdin(crate::Stdin::from_string("ignored"))
            .keep_stdin_open();
        assert!(open_with_source.stdin_source().is_some());
        assert!(open_with_source.effective_stdin_source().is_none());
    }

    #[test]
    fn effective_stdin_source_ignores_configured_source_when_inheriting_stdin() {
        let inherit_with_bytes_source = Command::new("tool")
            .stdin(crate::Stdin::from_bytes(b"ignored".to_vec()))
            .inherit_stdin();
        assert!(inherit_with_bytes_source.stdin_source().is_some());
        assert!(inherit_with_bytes_source.effective_stdin_source().is_none());

        let inherit_with_string_source = Command::new("tool")
            .stdin(crate::Stdin::from_string("ignored"))
            .inherit_stdin();
        assert!(inherit_with_string_source.stdin_source().is_some());
        assert!(
            inherit_with_string_source
                .effective_stdin_source()
                .is_none()
        );

        // Same result regardless of whether `keep_stdin_open` is also set.
        let inherit_with_source_and_keep_open = Command::new("tool")
            .stdin(crate::Stdin::from_string("ignored"))
            .keep_stdin_open()
            .inherit_stdin();
        assert!(
            inherit_with_source_and_keep_open
                .effective_stdin_source()
                .is_none()
        );
    }

    #[test]
    fn line_terminator_defaults_to_newline_and_setters_target_the_right_streams() {
        // Default: both streams split on `\n` only — the pre-existing behavior.
        let default = Command::new("x");
        assert_eq!(default.stdout_config.terminator, LineTerminator::Newline);
        assert_eq!(default.stderr_config.terminator, LineTerminator::Newline);

        // The combined setter moves both streams together.
        let both = Command::new("x").line_terminator(LineTerminator::CarriageReturn);
        assert_eq!(
            both.stdout_config.terminator,
            LineTerminator::CarriageReturn
        );
        assert_eq!(
            both.stderr_config.terminator,
            LineTerminator::CarriageReturn
        );

        // Per-stream setters touch only their own stream.
        let out_only = Command::new("x").stdout_line_terminator(LineTerminator::CarriageReturn);
        assert_eq!(
            out_only.stdout_config.terminator,
            LineTerminator::CarriageReturn
        );
        assert_eq!(
            out_only.stderr_config.terminator,
            LineTerminator::Newline,
            "stdout_line_terminator must not touch stderr"
        );
        let err_only = Command::new("x").stderr_line_terminator(LineTerminator::CarriageReturn);
        assert_eq!(
            err_only.stderr_config.terminator,
            LineTerminator::CarriageReturn
        );
        assert_eq!(err_only.stdout_config.terminator, LineTerminator::Newline);

        // The framing is surfaced in Debug (no secrets involved).
        let dbg = format!("{both:?}");
        assert!(
            dbg.contains("stdout_line_terminator") && dbg.contains("CarriageReturn"),
            "Debug should surface the line-terminator mode: {dbg}"
        );
    }

    #[test]
    fn sanitize_vt_defaults_off_and_setters_target_the_right_streams() {
        // Default: neither stream sanitizes — capture is verbatim.
        let default = Command::new("x");
        assert!(!default.stdout_config.sanitize_vt);
        assert!(!default.stderr_config.sanitize_vt);

        // The combined setter enables both.
        let both = Command::new("x").sanitize_vt();
        assert!(both.stdout_config.sanitize_vt);
        assert!(both.stderr_config.sanitize_vt);

        // Per-stream setters touch only their own stream.
        let out_only = Command::new("x").stdout_sanitize_vt();
        assert!(out_only.stdout_config.sanitize_vt);
        assert!(
            !out_only.stderr_config.sanitize_vt,
            "stdout_sanitize_vt must not touch stderr"
        );
        let err_only = Command::new("x").stderr_sanitize_vt();
        assert!(err_only.stderr_config.sanitize_vt);
        assert!(!err_only.stdout_config.sanitize_vt);

        // Surfaced in Debug for introspection.
        let dbg = format!("{both:?}");
        assert!(
            dbg.contains("stdout_sanitize_vt: true") && dbg.contains("stderr_sanitize_vt: true"),
            "Debug should surface the sanitizer state: {dbg}"
        );
    }

    // The whole-command policy injection is threaded into both stream configs
    // regardless of the sanitizer, so the sanitizer flag also survives the getter.
    #[test]
    fn sanitize_vt_flag_survives_the_stream_config_getters() {
        let cmd = Command::new("x").stdout_sanitize_vt();
        assert!(
            cmd.stdout_config().sanitize_vt,
            "the getter carries the stdout sanitizer flag to the pump"
        );
        assert!(!cmd.stderr_config().sanitize_vt, "stderr stays verbatim");
    }

    #[cfg(feature = "pty")]
    #[test]
    fn use_pty_defaults_effective_terminator_to_carriage_return() {
        // The coded PTY framing decision: `use_pty` makes the EFFECTIVE default
        // terminator `CarriageReturn` (so bare-`\r` progress frames stream as
        // lines), resolved in the config getter — order-independent with `pty_size`
        // and independent of which order `use_pty` is called.
        let pty = Command::new("agent").use_pty();
        assert_eq!(
            pty.stdout_config().terminator,
            LineTerminator::CarriageReturn,
            "use_pty defaults the effective stdout terminator to CarriageReturn"
        );
        assert_eq!(
            pty.stderr_config().terminator,
            LineTerminator::CarriageReturn,
            "the resolution is symmetric on stderr (unused under PTY, but consistent)"
        );

        // The STORED config is untouched (the default is applied only at getter
        // time), and a non-PTY command is unchanged.
        assert_eq!(pty.stdout_config.terminator, LineTerminator::Newline);
        let piped = Command::new("agent");
        assert_eq!(piped.stdout_config().terminator, LineTerminator::Newline);
    }

    #[cfg(feature = "pty")]
    #[test]
    fn explicit_line_terminator_wins_over_the_pty_auto_default() {
        // An explicit choice — including `Newline` — pins the terminator and opts
        // out of the PTY auto-default, order-independently of `use_pty`.
        let pinned_newline = Command::new("agent")
            .use_pty()
            .line_terminator(LineTerminator::Newline);
        assert_eq!(
            pinned_newline.stdout_config().terminator,
            LineTerminator::Newline,
            "explicit Newline must beat the PTY CarriageReturn auto-default"
        );

        // Order-independent: setting the terminator before use_pty is the same.
        let other_order = Command::new("agent")
            .line_terminator(LineTerminator::Newline)
            .use_pty();
        assert_eq!(
            other_order.stdout_config().terminator,
            LineTerminator::Newline
        );

        // A per-stream explicit choice pins only that stream; the other still
        // takes the PTY auto-default.
        let stderr_pinned = Command::new("agent")
            .use_pty()
            .stderr_line_terminator(LineTerminator::Newline);
        assert_eq!(
            stderr_pinned.stderr_config().terminator,
            LineTerminator::Newline,
            "pinned stderr wins"
        );
        assert_eq!(
            stderr_pinned.stdout_config().terminator,
            LineTerminator::CarriageReturn,
            "un-pinned stdout still auto-defaults to CarriageReturn"
        );
    }

    #[test]
    fn debug_redacts_argv_and_env_values_keeping_names_and_count() {
        // The manual Debug must never expose argv or env *values* — only the
        // arg count and the sorted env *names*.
        let cmd = Command::new("git")
            .arg("--password=hunter2")
            .arg("secret-positional")
            .env("API_TOKEN", "deadbeef-secret")
            .env("MODE", "fast-but-secret");
        let dbg = format!("{cmd:?}");
        assert!(
            !dbg.contains("hunter2")
                && !dbg.contains("secret-positional")
                && !dbg.contains("password"),
            "argv values must not appear in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("deadbeef-secret") && !dbg.contains("fast-but-secret"),
            "env values must not appear in Debug: {dbg}"
        );
        assert!(
            dbg.contains("API_TOKEN") && dbg.contains("MODE"),
            "env names should appear: {dbg}"
        );
        assert!(dbg.contains("args: 2"), "arg count should appear: {dbg}");
        assert!(
            dbg.contains("env_names"),
            "env_names field should appear: {dbg}"
        );
    }

    /// The explicit env ops recorded on the built OS command, as
    /// (key, Some(value)|None-for-remove) pairs.
    fn built_envs(cmd: &Command) -> Vec<(String, Option<String>)> {
        cmd.build_tokio()
            .expect("build tokio command")
            .as_std()
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect()
    }

    #[test]
    fn inherit_env_copies_named_parent_vars_onto_a_cleared_env() {
        // PATH exists in every test environment — no global env mutation.
        let parent_path = std::env::var_os("PATH").expect("PATH set in tests");
        let cmd = Command::new("x").inherit_env(["PATH"]);
        let built = cmd.build_tokio().expect("build tokio command");
        assert!(
            built
                .as_std()
                .get_envs()
                .any(|(k, v)| { k == OsStr::new("PATH") && v == Some(parent_path.as_os_str()) }),
            "PATH should be copied from the parent env"
        );
        // inherit_env implies env_clear: only allow-listed/explicit ops remain.
        assert_eq!(built.as_std().get_envs().count(), 1);
    }

    #[test]
    fn inherit_env_skips_vars_the_parent_lacks() {
        let cmd = Command::new("x").inherit_env(["PROCESSKIT_DEFINITELY_NOT_SET_424242"]);
        assert!(
            built_envs(&cmd).is_empty(),
            "a var the parent lacks must be skipped, not set empty"
        );
    }

    #[test]
    fn explicit_env_ops_apply_after_the_allow_list() {
        let cmd = Command::new("x")
            .inherit_env(["PATH"])
            .env("PATH", "overridden")
            .env("EXTRA", "1");
        let envs = built_envs(&cmd);
        // std keeps one entry per key, last write winning — the explicit
        // override (applied after the inherited copy) is what remains.
        assert!(
            envs.contains(&("PATH".to_string(), Some("overridden".to_string()))),
            "explicit env must override the inherited value: {envs:?}"
        );
        assert!(
            envs.contains(&("EXTRA".to_string(), Some("1".to_string()))),
            "explicit extras apply too: {envs:?}"
        );
        assert_eq!(envs.len(), 2, "cleared env + two explicit keys: {envs:?}");
    }

    #[test]
    fn inherit_env_calls_accumulate() {
        // If a second call REPLACED the allow-list (instead of extending it),
        // PATH from the first call would be lost.
        let cmd = Command::new("x")
            .inherit_env(["PATH"])
            .inherit_env(["PROCESSKIT_DEFINITELY_NOT_SET_424242"]);
        let envs = built_envs(&cmd);
        assert!(
            envs.iter().any(|(k, _)| k == "PATH"),
            "the first call's names must survive a second call: {envs:?}"
        );
    }

    #[test]
    fn privilege_builders_record_their_requests() {
        let cmd = Command::new("x").uid(1000).gid(1000).setsid();
        assert!(cmd.wants_setsid());
        let debug = format!("{cmd:?}");
        assert!(debug.contains("uid: Some(1000)"), "debug: {debug}");
        assert!(debug.contains("gid: Some(1000)"), "debug: {debug}");
    }

    #[test]
    fn kill_on_parent_death_records_the_request() {
        assert!(
            Command::new("x")
                .kill_on_parent_death()
                .wants_kill_on_parent_death()
        );
        assert!(!Command::new("x").wants_kill_on_parent_death());
    }

    #[test]
    fn kill_on_parent_death_scope_reports_the_platform_capability() {
        use crate::ParentDeathCleanup;
        // The honest per-platform capability report backing the CLI's
        // direct_child_only/none decision — fixed at build time for this target,
        // never overpromising a whole-tree guarantee the OS can't keep.
        let scope = Command::kill_on_parent_death_scope();
        #[cfg(windows)]
        assert_eq!(scope, ParentDeathCleanup::WholeTree);
        #[cfg(target_os = "linux")]
        assert_eq!(scope, ParentDeathCleanup::DirectChildOnly);
        #[cfg(all(unix, not(target_os = "linux")))]
        assert_eq!(scope, ParentDeathCleanup::Unsupported);
    }

    // --- spawn_detached: the loud, typed refusals (all reached *before* any
    //     spawn, so these are pure and CI-safe — no subprocess) --------------

    #[test]
    fn spawn_detached_refuses_a_timeout_loudly() {
        let err = Command::new("x")
            .timeout(std::time::Duration::from_secs(1))
            .spawn_detached()
            .expect_err("a timeout has no owner to enforce on a detached child");
        match err.reason() {
            crate::ErrorReason::Unsupported { operation } => {
                assert!(
                    operation.contains("spawn_detached") && operation.contains("timeout"),
                    "operation should name the offending knob: {operation}"
                );
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }

        let err = Command::new("x")
            .inactivity_timeout(std::time::Duration::from_secs(1))
            .spawn_detached()
            .expect_err("an inactivity watchdog has no output pump on a detached child");
        assert!(matches!(
            err.reason(),
            crate::ErrorReason::Unsupported { operation }
                if operation.contains("output-inactivity timeout")
        ));
    }

    #[test]
    fn spawn_detached_refuses_capture_and_interactive_knobs() {
        // A representative sample of the rejected knobs — each a loud, typed
        // refusal (never a silent drop), all short-circuited before any spawn.
        let refused: Vec<(&str, Command)> = vec![
            ("keep_stdin_open", Command::new("x").keep_stdin_open()),
            ("inherit_stdin", Command::new("x").inherit_stdin()),
            (
                "stdin source",
                Command::new("x").stdin(crate::Stdin::from_string("in")),
            ),
            ("output handler", Command::new("x").on_stdout_line(|_| {})),
            (
                "cancel token",
                Command::new("x").cancel_on(crate::CancellationToken::new()),
            ),
            (
                "retry policy",
                Command::new("x").retry(3, std::time::Duration::ZERO, |_| true),
            ),
            (
                "kill_on_parent_death",
                Command::new("x").kill_on_parent_death(),
            ),
        ];
        for (what, cmd) in refused {
            let err = cmd
                .spawn_detached()
                .expect_err("a detached spawn must refuse this knob loudly");
            assert!(
                matches!(err.reason(), crate::ErrorReason::Unsupported { .. }),
                "{what} must be refused with Unsupported, got {err:?}"
            );
        }
    }

    #[cfg(not(unix))]
    #[test]
    fn spawn_detached_refuses_posix_privilege_off_unix() {
        // uid/gid/umask/setsid are POSIX-only; off Unix a detached spawn refuses
        // them just as the run helpers do — never a silent skip of a privilege drop.
        let err = Command::new("x")
            .uid(1000)
            .spawn_detached()
            .expect_err("uid is a POSIX-only primitive off Unix");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Unsupported { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn create_no_window_sets_the_flag_bit() {
        let cmd = Command::new("x").create_no_window();
        assert_eq!(cmd.extra_creation_flags(), 0x0800_0000);
        assert_eq!(Command::new("x").extra_creation_flags(), 0);
    }

    #[test]
    fn windows_graceful_ctrl_break_records_the_opt_in() {
        assert!(
            Command::new("x")
                .windows_graceful_ctrl_break()
                .wants_windows_graceful_ctrl_break(),
            "the builder opts the command into the Windows console-CTRL teardown"
        );
        // Off by default — every existing command keeps the atomic-kill behavior.
        assert!(!Command::new("x").wants_windows_graceful_ctrl_break());
        // Composes with create_no_window (both are independent spawn knobs).
        let combined = Command::new("x")
            .windows_graceful_ctrl_break()
            .create_no_window();
        assert!(combined.wants_windows_graceful_ctrl_break());
        assert_eq!(combined.extra_creation_flags(), 0x0800_0000);
    }

    #[test]
    fn scheduling_knobs_record_their_requests() {
        let cmd = Command::new("x")
            .priority(crate::Priority::BelowNormal)
            .io_priority(crate::IoPriority::BestEffort(7))
            .umask(0o022);
        let debug = format!("{cmd:?}");
        assert!(debug.contains("BelowNormal"), "debug: {debug}");
        assert!(debug.contains("BestEffort(7)"), "debug: {debug}");
        assert!(debug.contains("umask: Some(18)"), "debug: {debug}"); // 0o022 == 18
        assert!(
            !format!("{:?}", Command::new("x")).contains("BelowNormal"),
            "an unset priority must not appear"
        );
    }

    #[cfg(windows)]
    #[test]
    fn priority_ors_the_windows_creation_flag_alongside_create_no_window() {
        use windows_sys::Win32::System::Threading::{
            BELOW_NORMAL_PRIORITY_CLASS, IDLE_PRIORITY_CLASS,
        };
        let cmd = Command::new("x").priority(crate::Priority::Idle);
        assert_eq!(cmd.extra_creation_flags(), IDLE_PRIORITY_CLASS);

        // Composes with create_no_window (an independent bit).
        let both = Command::new("x")
            .priority(crate::Priority::BelowNormal)
            .create_no_window();
        assert_eq!(
            both.extra_creation_flags(),
            BELOW_NORMAL_PRIORITY_CLASS | 0x0800_0000
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn umask_is_gated_unsupported_on_non_unix_but_priority_is_not() {
        // The launch-time gate reads these accessors directly; priority has no
        // such accessor at all, because it is never gated.
        assert_eq!(Command::new("x").umask(0o022).requested_umask(), Some(18));
        assert_eq!(Command::new("x").requested_umask(), None);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn io_priority_is_gated_unsupported_on_non_linux() {
        let err = Command::new("x")
            .io_priority(crate::IoPriority::Idle)
            .build_tokio()
            .expect_err("I/O priority must be refused outside Linux");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Unsupported { operation } if operation.contains("io_priority")),
            "got {err:?}"
        );
        assert_eq!(
            Command::new("x")
                .io_priority(crate::IoPriority::Idle)
                .requested_io_priority(),
            Some(crate::IoPriority::Idle)
        );
    }

    #[test]
    fn spawn_detached_refuses_io_priority_loudly() {
        let err = Command::new("x")
            .io_priority(crate::IoPriority::Idle)
            .spawn_detached()
            .expect_err("a detached spawn must refuse an owner-dependent I/O priority");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Unsupported { operation } if operation.contains("io_priority")),
            "got {err:?}"
        );
    }

    #[test]
    fn cancel_on_records_the_token() {
        let token = tokio_util::sync::CancellationToken::new();
        let cmd = Command::new("x").cancel_on(token.clone());
        // The accessor hands back a clone sharing the same cancel state.
        let stored = cmd.cancel_token().expect("token recorded");
        token.cancel();
        assert!(stored.is_cancelled(), "clones share one cancel state");
        assert!(Command::new("x").cancel_token().is_none());
    }

    #[test]
    fn debug_reports_token_presence_not_contents() {
        let with = Command::new("x").cancel_on(tokio_util::sync::CancellationToken::new());
        assert!(format!("{with:?}").contains("has_cancel_token: true"));
        assert!(format!("{:?}", Command::new("x")).contains("has_cancel_token: false"));
    }

    /// A trivial identity [`crate::CapturePolicy`] for the config/Debug tests.
    struct NamedPolicy(&'static str);
    impl crate::CapturePolicy for NamedPolicy {
        fn name(&self) -> &str {
            self.0
        }
        fn on_capture<'a>(
            &self,
            _stream: crate::OutputStream,
            line: &'a str,
        ) -> std::borrow::Cow<'a, str> {
            std::borrow::Cow::Borrowed(line)
        }
    }

    #[test]
    fn capture_policy_threads_into_both_stream_configs_with_stream_tags() {
        // The whole-command policy reaches BOTH pumps via the config getters,
        // each stamped with its own `OutputStream` tag; a plain command carries
        // none. This is the single owning point (K-012) — no per-stream re-derive.
        let plain = Command::new("x");
        assert!(plain.stdout_config().buffer_policy.is_none());
        assert!(plain.stderr_config().buffer_policy.is_none());

        let cmd = Command::new("x").capture_policy(NamedPolicy("test-policy"));
        let out = cmd.stdout_config();
        let err = cmd.stderr_config();
        assert!(out.buffer_policy.is_some(), "stdout pump gets the policy");
        assert!(err.buffer_policy.is_some(), "stderr pump gets the policy");
        assert_eq!(out.stream, crate::OutputStream::Stdout);
        assert_eq!(err.stream, crate::OutputStream::Stderr);
        assert_eq!(
            out.buffer_policy.as_ref().unwrap().name(),
            "test-policy",
            "the same policy is shared across both streams"
        );
    }

    #[test]
    fn debug_reports_capture_policy_name_not_contents() {
        // The named seam is introspectable (unlike an opaque closure): Debug
        // surfaces its `name()`, and `None` when unset — never captured data.
        let with = Command::new("x").capture_policy(NamedPolicy("redactor-x"));
        assert!(format!("{with:?}").contains(r#"capture_policy: Some("redactor-x")"#));
        assert!(format!("{:?}", Command::new("x")).contains("capture_policy: None"));
    }

    #[test]
    fn is_bare_name_distinguishes_bare_from_path() {
        use super::is_bare_name;
        // Bare names — should be looked up on PATH.
        assert!(is_bare_name(OsStr::new("git")));
        assert!(is_bare_name(OsStr::new("git.exe")));
        assert!(is_bare_name(OsStr::new("python3")));
        // Relative / absolute paths — caller already located the program.
        assert!(!is_bare_name(OsStr::new("./tool")));
        assert!(!is_bare_name(OsStr::new("../bin/x")));
        assert!(!is_bare_name(OsStr::new("/usr/bin/git")));
        assert!(!is_bare_name(OsStr::new("subdir/tool")));
        #[cfg(windows)]
        assert!(!is_bare_name(OsStr::new("C:\\git.exe")));
        // A trailing separator is path-ish (Path normalizes it away).
        assert!(!is_bare_name(OsStr::new("git/")));
        #[cfg(windows)]
        assert!(!is_bare_name(OsStr::new("git\\")));
        #[cfg(not(windows))]
        assert!(is_bare_name(OsStr::new("git\\")));
    }

    #[cfg(not(unix))]
    #[test]
    fn quote_arg_handles_trailing_backslash() {
        use super::quote_arg;
        // Single trailing backslash (space triggers quoting):
        // `C:\my tools\` → `"C:\my tools\\"`, not `"C:\my tools\"`.
        assert_eq!(quote_arg("C:\\my tools\\"), "\"C:\\my tools\\\\\"");
        // Two trailing backslashes: both must be doubled → four before the quote.
        assert_eq!(quote_arg("C:\\my tools\\\\"), "\"C:\\my tools\\\\\\\\\"");
        // No trailing backslash: no doubling needed.
        assert_eq!(quote_arg("C:\\my tools"), "\"C:\\my tools\"");
        // G9: a backslash *before an embedded quote* must double AND the quote
        // escape — `a\"b` → `"a\\\"b"` — so it round-trips through
        // CommandLineToArgvW (the old code left it `a\"` → re-parsed as `a"`).
        assert_eq!(quote_arg("a\\\"b"), r#""a\\\"b""#);
        // An interior backslash NOT before a quote stays literal (single).
        assert_eq!(quote_arg("a\\b c"), "\"a\\b c\"");
    }

    #[test]
    fn no_timeout_opts_out_of_client_default_but_timeout_is_last_wins() {
        use std::time::Duration;
        // G4: no_timeout is "explicitly unbounded" — no timeout AND opts out of a
        // client default_timeout gap-fill.
        let cmd = Command::new("tail").no_timeout();
        assert_eq!(cmd.configured_timeout(), None);
        assert!(
            !cmd.accepts_default_timeout(),
            "no_timeout opts out of the client gap-fill"
        );
        // An unset command DOES accept the fill.
        assert!(Command::new("x").accepts_default_timeout());
        // Last of timeout()/no_timeout() wins, both directions.
        let re_bounded = Command::new("x")
            .no_timeout()
            .timeout(Duration::from_secs(1));
        assert_eq!(
            re_bounded.configured_timeout(),
            Some(Duration::from_secs(1))
        );
        assert!(!re_bounded.accepts_default_timeout());
        let re_unbounded = Command::new("x")
            .timeout(Duration::from_secs(1))
            .no_timeout();
        assert_eq!(re_unbounded.configured_timeout(), None);
        assert!(!re_unbounded.accepts_default_timeout());
    }

    #[test]
    fn timeout_opt_folds_option_into_timeout_and_no_timeout() {
        use std::time::Duration;
        // G4: Some(d) is exactly timeout(d) — a bounded deadline that also opts
        // out of a client default fill.
        let some = Command::new("x").timeout_opt(Some(Duration::from_secs(2)));
        assert_eq!(some.configured_timeout(), Some(Duration::from_secs(2)));
        assert!(!some.accepts_default_timeout());
        // None is exactly no_timeout() — deliberately unbounded, opts out of the
        // fill (NOT "leave unset").
        let none = Command::new("x").timeout_opt(None);
        assert_eq!(none.configured_timeout(), None);
        assert!(
            !none.accepts_default_timeout(),
            "timeout_opt(None) is no_timeout, not 'leave the timeout unset'"
        );
        // Equivalences against the verbs it folds.
        assert_eq!(
            Command::new("x")
                .timeout_opt(Some(Duration::from_secs(5)))
                .configured_timeout(),
            Command::new("x")
                .timeout(Duration::from_secs(5))
                .configured_timeout(),
        );
        assert_eq!(
            Command::new("x")
                .timeout_opt(None)
                .accepts_default_timeout(),
            Command::new("x").no_timeout().accepts_default_timeout(),
        );
        // Last-write-wins with an earlier timeout call.
        let overridden = Command::new("x")
            .timeout(Duration::from_secs(9))
            .timeout_opt(None);
        assert_eq!(overridden.configured_timeout(), None);
        assert!(!overridden.accepts_default_timeout());
    }

    #[test]
    fn retry_never_is_a_single_run_that_suppresses_a_client_default() {
        use crate::retry::RetryConfig;
        use std::time::Duration;
        // G4: retry_never sets a config (so it survives a default_retry gap-fill)
        // whose schedule is a single attempt — behaviorally identical to
        // retry(1, ZERO, |_| false).
        let mut cmd = Command::new("x").retry_never();
        let cfg = cmd.retry_config().expect("retry_never sets a retry config");
        assert_eq!(cfg.policy.max_attempts(), 1, "exactly one run, no retry");
        // A client default_retry must NOT override the explicit opt-out.
        let client_default = Some(RetryConfig::fixed(5, Duration::ZERO, |_| true));
        cmd.fill_default_retry(&client_default);
        assert_eq!(
            cmd.retry_config().expect("still set").policy.max_attempts(),
            1,
            "retry_never suppresses the client default_retry gap-fill"
        );
        // A command with no retry opinion DOES accept the client default.
        let mut plain = Command::new("x");
        plain.fill_default_retry(&client_default);
        assert_eq!(
            plain.retry_config().expect("filled").policy.max_attempts(),
            5,
            "a command without retry_never still accepts the client default"
        );
    }

    #[test]
    fn ok_codes_empty_is_ignored_keeping_the_previous_set() {
        // G7: an empty set is a no-op (doc says "ignored"), not a reset to [0].
        assert_eq!(
            Command::new("x")
                .ok_codes([2, 3])
                .ok_codes([])
                .ok_codes_vec(),
            vec![2, 3],
            "an empty ok_codes must not clobber a previously configured set"
        );
        // No previous set: an empty set leaves the default [0].
        assert_eq!(Command::new("x").ok_codes([]).ok_codes_vec(), vec![0]);
    }

    #[test]
    fn env_isolation_opts_out_of_client_env_defaults() {
        // G2: a client default_env must not pierce env_clear, nor override an
        // inherit_env allow-listed key — but a non-allow-listed default still fills.
        let key = OsStr::new("LANG");
        // env_clear blocks every key (clean slate).
        assert!(
            Command::new("x").env_clear().has_env_override(key),
            "env_clear isolates from client env defaults"
        );
        // inherit_env blocks only its allow-listed keys...
        assert!(
            Command::new("x")
                .inherit_env(["LANG"])
                .has_env_override(key),
            "an allow-listed key must not be overridden by a client default"
        );
        // ...a key NOT in the allow-list still accepts the client default (a
        // client-wide safety default reaches an inherit_env command).
        assert!(
            !Command::new("x")
                .inherit_env(["HOME"])
                .has_env_override(key),
            "a non-allow-listed client default still fills"
        );
        // A plain command with no opinion about LANG still accepts the default.
        assert!(!Command::new("x").has_env_override(key));
        // An explicit per-command env for the key still counts (unchanged).
        assert!(Command::new("x").env("LANG", "C").has_env_override(key));
    }

    #[test]
    fn customizes_path_gates_the_not_found_enrichment() {
        // A plain command does not customize PATH — the rich NotFound applies.
        assert!(!Command::new("git").customizes_path());
        assert!(!Command::new("git").env("FOO", "1").customizes_path());
        // Anything that can move PATH away from the process PATH disables the
        // process-PATH enrichment (else its "searched" list would be wrong).
        assert!(
            Command::new("git")
                .env("PATH", "/opt/bin")
                .customizes_path()
        );
        #[cfg(windows)]
        assert!(
            Command::new("git")
                .env("path", "/opt/bin")
                .customizes_path(),
            "Windows environment keys are case-insensitive"
        );
        #[cfg(unix)]
        assert!(
            !Command::new("git")
                .env("path", "/opt/bin")
                .customizes_path(),
            "Unix `path` is distinct from `PATH`"
        );
        assert!(Command::new("git").env_remove("PATH").customizes_path());
        assert!(Command::new("git").env_clear().customizes_path());
        assert!(Command::new("git").inherit_env(["HOME"]).customizes_path());
    }

    /// Write a file in `dir` that resolves as directly executable for
    /// `program` — a `.exe` sibling on Windows (matching `probe_dir`'s PATHEXT
    /// rules), an executable-bit-set file with the exact name on Unix.
    /// Returns the resolved absolute path `probe_dir`/`build_tokio` should
    /// report.
    fn write_executable(dir: &std::path::Path, program: &str) -> PathBuf {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join(program);
            std::fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write stub");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod +x");
            path
        }
        #[cfg(not(unix))]
        {
            let path = dir.join(format!("{program}.exe"));
            std::fs::write(&path, b"stub").expect("write stub");
            path
        }
    }

    #[test]
    fn prefer_local_accumulates_in_call_order() {
        let cmd = Command::new("tool")
            .prefer_local("/opt/first")
            .prefer_local("/opt/second");
        assert_eq!(
            cmd.prefer_local_dirs(),
            &[PathBuf::from("/opt/first"), PathBuf::from("/opt/second")]
        );
        assert!(Command::new("tool").prefer_local_dirs().is_empty());
    }

    #[test]
    fn probe_prefer_local_returns_the_first_matching_directory_in_priority_order() {
        let empty_dir = tempfile::tempdir().expect("temp dir");
        let match_dir = tempfile::tempdir().expect("temp dir");
        let also_match_dir = tempfile::tempdir().expect("temp dir");
        let expected = write_executable(match_dir.path(), "tool-a");
        write_executable(also_match_dir.path(), "tool-a");

        // The first directory that actually contains a match wins, regardless
        // of later directories also matching.
        let dirs = vec![
            empty_dir.path().to_path_buf(),
            match_dir.path().to_path_buf(),
            also_match_dir.path().to_path_buf(),
        ];
        let found = super::probe_prefer_local(&dirs, OsStr::new("tool-a")).expect("must find it");
        // On Windows, PATHEXT expansion's resolved extension case follows
        // whatever the PATHEXT env var carries (commonly `.EXE`), not
        // necessarily the on-disk file's case — compare case-insensitively,
        // same as the existing `probe_dir` PATHEXT tests.
        assert!(
            found
                .to_string_lossy()
                .eq_ignore_ascii_case(&expected.to_string_lossy()),
            "expected {expected:?}, got {found:?}"
        );

        // No match anywhere → None (the fallback to PATH is the caller's job).
        let none_dirs = vec![empty_dir.path().to_path_buf()];
        assert_eq!(
            super::probe_prefer_local(&none_dirs, OsStr::new("tool-a")),
            None
        );
    }

    #[test]
    fn build_tokio_resolves_bare_program_via_prefer_local_ahead_of_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        let expected = write_executable(dir.path(), "prefer-local-tool");

        let cmd = Command::new("prefer-local-tool").prefer_local(dir.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        // Case-insensitive for the same PATHEXT-casing reason as above.
        assert!(
            tokio_cmd
                .as_std()
                .get_program()
                .to_string_lossy()
                .eq_ignore_ascii_case(&expected.to_string_lossy()),
            "a prefer_local match must be spawned via its resolved absolute path, \
             so the OS never has to search PATH for it; got {:?}, expected {expected:?}",
            tokio_cmd.as_std().get_program()
        );
    }

    #[cfg(unix)]
    #[test]
    fn backslash_program_name_resolves_as_bare_across_preflight_and_build() {
        let program = r"we\ird";
        let dir = tempfile::tempdir().expect("temp dir");
        let expected = write_executable(dir.path(), program);
        let path = std::env::join_paths([dir.path()]).expect("single PATH entry");

        assert!(
            super::is_bare_name(OsStr::new(program)),
            "a backslash is an ordinary filename character on Unix"
        );

        let resolved = Command::new(program)
            .env("PATH", &path)
            .resolve_program()
            .expect("Command preflight must search PATH for a backslash name");
        assert_eq!(resolved, expected);

        let client_resolved = crate::CliClient::new(program)
            .default_env("PATH", &path)
            .resolve_program()
            .expect("CliClient preflight must search PATH for a backslash name");
        assert_eq!(client_resolved, expected);

        let built = Command::new(program)
            .prefer_local(dir.path())
            .build_tokio()
            .expect("build tokio command");
        assert_eq!(
            built.as_std().get_program(),
            expected.as_os_str(),
            "prefer_local must substitute a Unix backslash name before spawn"
        );
    }

    // R-01: a relative `prefer_local` directory (the form used throughout the
    // docs, e.g. `"./node_modules/.bin"`) must resolve to an *absolute* program
    // path — one that a later `.current_dir(other)` on the same command cannot
    // reinterpret. Before the fix, `probe_dir` returned `dir.join(program)`
    // unchanged, so a relative `prefer_local` dir produced a relative resolved
    // path that `Command::new` would hand to the OS verbatim; combined with
    // `current_dir`, that's the documented `std::process::Command` footgun
    // (Unix: relative program resolved against the *child's* cwd after chdir;
    // Windows: against the parent's cwd) — a spurious `NotFound`, or worse, a
    // different same-named file executed under the child's working directory.
    #[test]
    fn build_tokio_absolutizes_a_relative_prefer_local_match_so_current_dir_cannot_move_it() {
        // Serialize with any other test in this binary that reads/writes the
        // process's current directory (none currently do, but this guards
        // against future additions racing on global process state).
        static CWD_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = CWD_GUARD.lock().unwrap_or_else(|e| e.into_inner());

        let original_cwd = std::env::current_dir().expect("read current dir");
        let base = tempfile::tempdir().expect("temp dir");
        std::env::set_current_dir(base.path()).expect("chdir into temp base");

        struct RestoreCwd(PathBuf);
        impl Drop for RestoreCwd {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }
        let _restore = RestoreCwd(original_cwd);

        // The `prefer_local` match lives under the (now current) temp base, at
        // a relative path — exactly the `"./bin"`-style form the docs use.
        let bin_dir = base.path().join("bin");
        std::fs::create_dir(&bin_dir).expect("mkdir bin");
        let expected = write_executable(&bin_dir, "relative-prefer-local-tool");

        // A *different* directory is set as the command's own `current_dir` —
        // the resolved program path must not be influenced by it.
        let other_cwd = tempfile::tempdir().expect("other temp dir");

        let cmd = Command::new("relative-prefer-local-tool")
            .prefer_local("./bin")
            .current_dir(other_cwd.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        let resolved = tokio_cmd.as_std().get_program();

        assert!(
            std::path::Path::new(resolved).is_absolute(),
            "a relative prefer_local match must be absolutized before reaching \
             Command::new; got {resolved:?}"
        );
        // Compare canonicalized forms: `resolved` is built by literally joining
        // `"./bin"` onto the cwd (so it carries a `.` component `expected`
        // doesn't), but both name the exact same on-disk file.
        let resolved_canon = std::fs::canonicalize(resolved).expect("resolved path must exist");
        let expected_canon = std::fs::canonicalize(&expected).expect("expected path must exist");
        assert!(
            resolved_canon
                .to_string_lossy()
                .eq_ignore_ascii_case(&expected_canon.to_string_lossy()),
            "resolved program must be the absolute path under the temp base cwd \
             at build time, unaffected by .current_dir(other_cwd); got {resolved:?} \
             (canonical: {resolved_canon:?}), expected {expected:?} (canonical: {expected_canon:?})"
        );
    }

    #[test]
    fn build_tokio_falls_back_to_the_bare_name_when_prefer_local_misses() {
        let dir = tempfile::tempdir().expect("temp dir"); // no matching file inside

        let cmd = Command::new("not-in-prefer-local").prefer_local(dir.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        assert_eq!(
            tokio_cmd.as_std().get_program(),
            OsStr::new("not-in-prefer-local"),
            "a prefer_local miss must leave the bare name for the OS's own PATH search"
        );
    }

    #[test]
    fn file_redirect_is_separate_from_the_copyable_stdio_mode() {
        let mode = crate::StdioMode::Piped;
        let mode_copy = mode;
        assert_eq!(mode, mode_copy, "StdioMode stays Copy");

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("service.log");
        let command = Command::new("tool").stdout_file(&path);
        assert!(
            !command.stdout_is_piped(),
            "a file destination must take the capture gate even though StdioMode remains Piped"
        );
        command
            .build_tokio()
            .expect("the launch mapper opens a creatable file");

        let missing_parent = dir.path().join("missing").join("service.log");
        let err = Command::new("tool")
            .stdout_file(missing_parent)
            .build_tokio()
            .expect_err("file opening fails at the launch boundary");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(e) if e.kind() == std::io::ErrorKind::NotFound)
        );
    }

    // T-125: a bare name that exists on `PATH` ONLY via a non-`.exe` PATHEXT
    // extension (`yarn.cmd`/`npx.cmd` npm/scoop shims) must be spawned via its
    // resolved absolute path — the OS's own bare-name search appends only
    // `.exe`, so it would never launch such a program by bare name, breaking the
    // documented `which`/spawn parity. This is the PATH-side analogue of the
    // `prefer_local` substitution above.
    #[cfg(windows)]
    #[test]
    fn build_tokio_substitutes_a_non_exe_pathext_path_match() {
        let dir = tempfile::tempdir().expect("temp dir");
        let unique = "pk_build_tokio_cmd_shim";
        let cmd_path = dir.path().join(format!("{unique}.cmd"));
        std::fs::write(&cmd_path, "@echo off\r\nexit /b 0\r\n").expect("write .cmd shim");

        // `env("PATH", …)` relocates the child PATH, so both build_tokio and the
        // preflight resolve against this single directory (its effective child
        // PATH) — the substitution and `resolve_program` must land on one path.
        let cmd = Command::new(unique).env("PATH", dir.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        assert!(
            tokio_cmd
                .as_std()
                .get_program()
                .to_string_lossy()
                .eq_ignore_ascii_case(&cmd_path.to_string_lossy()),
            "a non-.exe PATHEXT PATH match must be spawned via its resolved \
             absolute path; got {:?}, expected {cmd_path:?}",
            tokio_cmd.as_std().get_program()
        );
        let resolved = cmd
            .resolve_program()
            .expect("preflight must resolve the same .cmd");
        assert!(
            resolved
                .to_string_lossy()
                .eq_ignore_ascii_case(&cmd_path.to_string_lossy()),
            "preflight and build_tokio must resolve the identical path; got {resolved:?}"
        );
    }

    // T-125: the complement — a `.exe` match on `PATH` is exactly what the OS's
    // own bare-name search already finds, so build_tokio must NOT substitute it.
    // Leaving the bare name preserves the OS's richer search order (application
    // directory / current directory / System32) that this `PATH`-only model
    // deliberately doesn't touch — the pre-existing, unchanged `.exe` behavior.
    #[cfg(windows)]
    #[test]
    fn build_tokio_leaves_a_bare_name_with_an_exe_path_match_for_the_os() {
        let dir = tempfile::tempdir().expect("temp dir");
        let unique = "pk_build_tokio_exe_on_path";
        write_executable(dir.path(), unique); // writes `<unique>.exe`

        let cmd = Command::new(unique).env("PATH", dir.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        assert_eq!(
            tokio_cmd.as_std().get_program(),
            OsStr::new(unique),
            "an .exe PATH match must be left as the bare name for the OS's own search"
        );
    }

    #[test]
    fn prefer_local_has_no_effect_on_a_path_form_program() {
        let dir = tempfile::tempdir().expect("temp dir");
        // Even a directory that *would* match if the program were a bare name
        // must not be consulted for a path-form program.
        write_executable(dir.path(), "tool");
        let path_program = "./tool";

        let cmd = Command::new(path_program).prefer_local(dir.path());
        let tokio_cmd = cmd.build_tokio().expect("build tokio command");
        assert_eq!(
            tokio_cmd.as_std().get_program(),
            OsStr::new(path_program),
            "a path-form program must reach the OS verbatim, unaffected by prefer_local"
        );
    }

    #[test]
    fn prepend_prefer_local_to_searched_merges_in_priority_order() {
        // No prefer_local directories: the PATH searched string passes through
        // unchanged.
        assert_eq!(
            super::prepend_prefer_local_to_searched(&[], "/usr/bin:/bin"),
            "/usr/bin:/bin"
        );

        let sep = if cfg!(windows) { ';' } else { ':' };
        let dirs = vec![PathBuf::from("/opt/a"), PathBuf::from("/opt/b")];

        // prefer_local dirs, then the PATH dirs, in that order.
        assert_eq!(
            super::prepend_prefer_local_to_searched(&dirs, "/usr/bin"),
            format!("/opt/a{sep}/opt/b{sep}/usr/bin")
        );

        // An empty PATH searched string still surfaces the prefer_local dirs.
        assert_eq!(
            super::prepend_prefer_local_to_searched(&dirs, ""),
            format!("/opt/a{sep}/opt/b")
        );
    }

    // ── spawn-free program resolution (which / Command::resolve_program) ──────

    // T-101: a bare name found under a `prefer_local` directory resolves to its
    // absolute path — the same `probe_prefer_local` the launch spawns through,
    // so preflight can't disagree with a real run.
    #[test]
    fn resolve_program_finds_a_bare_name_under_prefer_local() {
        let dir = tempfile::tempdir().expect("temp dir");
        let expected = write_executable(dir.path(), "pk-resolve-tool");

        let resolved = Command::new("pk-resolve-tool")
            .prefer_local(dir.path())
            .resolve_program()
            .expect("prefer_local match must resolve without spawning");
        assert!(resolved.is_absolute(), "resolved path must be absolute");
        // Windows PATHEXT casing follows the env var, so compare case-insensitively
        // (same reason as the `probe_prefer_local` tests).
        assert!(
            resolved
                .to_string_lossy()
                .eq_ignore_ascii_case(&expected.to_string_lossy()),
            "expected {expected:?}, got {resolved:?}"
        );
    }

    // T-101: a bare name that resolves nowhere yields `ErrorReason::NotFound` whose
    // `searched` names the `prefer_local` directories (first) — the same typed
    // error the launch raises, reusing `ErrorReason::NotFound` rather than a parallel.
    #[test]
    fn resolve_program_missing_bare_name_is_not_found_with_searched() {
        let dir = tempfile::tempdir().expect("temp dir"); // deliberately empty
        let err = Command::new("pk-definitely-absent-tool-101")
            .prefer_local(dir.path())
            .resolve_program()
            .expect_err("an absent program must not resolve");
        assert!(err.is_not_found(), "must classify as not-found: {err:?}");
        match err.into_reason() {
            crate::ErrorReason::NotFound { searched, .. } => {
                let searched = searched.expect("a bare-name lookup reports searched dirs");
                assert!(
                    searched.contains(&dir.path().to_string_lossy().into_owned()),
                    "searched must include the prefer_local directory: {searched}"
                );
            }
            other => panic!("expected ErrorReason::NotFound, got {other:?}"),
        }
    }

    // T-101: a path-form program is probed directly (no PATH search) — found when
    // it is an executable file, `NotFound { searched: None }` otherwise, matching
    // how the launch hands such a program to the OS verbatim.
    #[test]
    fn resolve_program_handles_a_path_form_program() {
        let dir = tempfile::tempdir().expect("temp dir");
        let exe = write_executable(dir.path(), "pk-path-form");

        // The absolute path to the real executable resolves to itself.
        let resolved = Command::new(&exe)
            .resolve_program()
            .expect("an existing path-form executable resolves");
        assert!(
            resolved
                .to_string_lossy()
                .eq_ignore_ascii_case(&exe.to_string_lossy()),
            "expected {exe:?}, got {resolved:?}"
        );

        // A path-form program that doesn't exist → NotFound with no searched dirs
        // (no PATH lookup applies to a path-form program).
        let missing = dir.path().join("pk-path-form-missing");
        let err = Command::new(&missing)
            .resolve_program()
            .expect_err("a missing path-form program must not resolve");
        match err.into_reason() {
            crate::ErrorReason::NotFound { searched, .. } => assert_eq!(
                searched, None,
                "a path-form lookup applies no PATH search, so searched is None"
            ),
            other => panic!("expected ErrorReason::NotFound, got {other:?}"),
        }
    }

    // T-101: `probe_path_form` rejects a trailing-separator name — it designates a
    // directory, never an executable file, so it must not falsely resolve a
    // same-named sibling.
    #[test]
    fn probe_path_form_rejects_a_trailing_separator() {
        assert_eq!(super::probe_path_form(OsStr::new("tool/")), None);
        #[cfg(windows)]
        assert_eq!(super::probe_path_form(OsStr::new("tool\\")), None);
    }

    #[cfg(unix)]
    #[test]
    fn probe_path_form_accepts_a_trailing_backslash_in_a_unix_filename() {
        let dir = tempfile::tempdir().expect("temp dir");
        let expected = write_executable(dir.path(), "tool\\");

        assert_eq!(
            super::probe_path_form(expected.as_os_str()),
            Some(expected),
            "a trailing backslash is not a directory marker on Unix"
        );
    }

    // T-101: the effective child `PATH` — what preflight resolves against when a
    // command relocates `PATH` — mirrors `build_tokio`'s env application.
    #[test]
    fn effective_path_value_mirrors_the_child_env() {
        use std::ffi::OsString;

        let process_path = std::env::var_os("PATH");

        // A plain command inherits the process PATH.
        assert_eq!(Command::new("x").effective_path_value(), process_path);

        // An explicit `env("PATH", …)` override wins (last write, too).
        assert_eq!(
            Command::new("x")
                .env("PATH", "/one")
                .env("PATH", "/two")
                .effective_path_value(),
            Some(OsString::from("/two"))
        );

        // `env_remove("PATH")` leaves the child with no PATH.
        assert_eq!(
            Command::new("x").env_remove("PATH").effective_path_value(),
            None
        );

        // `env_clear` clears everything (no PATH) unless one is set back.
        assert_eq!(Command::new("x").env_clear().effective_path_value(), None);
        assert_eq!(
            Command::new("x")
                .env_clear()
                .env("PATH", "/only")
                .effective_path_value(),
            Some(OsString::from("/only"))
        );

        // `inherit_env` without PATH → cleared (no PATH); with PATH → the parent's.
        assert_eq!(
            Command::new("x")
                .inherit_env(["HOME"])
                .effective_path_value(),
            None
        );
        assert_eq!(
            Command::new("x")
                .inherit_env(["PATH"])
                .effective_path_value(),
            process_path
        );
    }

    // T-101 (anti-drift): for a command that explicitly sets or removes `PATH`,
    // `effective_path_value` must equal exactly what `build_tokio` hands the
    // child — so the preflight can never resolve against a different PATH than
    // the spawn does.
    #[test]
    fn effective_path_value_agrees_with_build_tokio_for_explicit_path_ops() {
        use std::ffi::OsString;

        // The PATH the built tokio command carries: `Some(Some(v))` set to `v`,
        // `Some(None)` explicitly removed, `None` not mentioned (inherited).
        fn tokio_path_env(cmd: &Command) -> Option<Option<OsString>> {
            cmd.build_tokio()
                .expect("build tokio command")
                .as_std()
                .get_envs()
                .find(|(k, _)| super::env_key_eq(k, OsStr::new("PATH")))
                .map(|(_, v)| v.map(|v| v.to_os_string()))
        }

        let set = Command::new("x").env("PATH", "/custom/bin");
        assert_eq!(
            tokio_path_env(&set),
            Some(Some(OsString::from("/custom/bin")))
        );
        assert_eq!(
            set.effective_path_value(),
            Some(OsString::from("/custom/bin"))
        );

        let removed = Command::new("x").env_remove("PATH");
        assert_eq!(tokio_path_env(&removed), Some(None));
        assert_eq!(removed.effective_path_value(), None);
    }

    #[test]
    fn envs_builder_adds_multiple_vars() {
        let cmd = Command::new("x").env("EXISTING", "old").envs([
            ("FOO", "1"),
            ("BAR", "2"),
            ("EXISTING", "new"),
        ]);
        let envs: Vec<_> = cmd
            .env_overrides()
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.as_ref().map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert!(
            envs.contains(&("FOO".into(), Some("1".into()))),
            "FOO not found: {envs:?}"
        );
        assert!(
            envs.contains(&("BAR".into(), Some("2".into()))),
            "BAR not found: {envs:?}"
        );
        // Last writer wins on the built command; we just check that envs()
        // appended the overriding entry (std Command keeps last-write).
        assert_eq!(
            envs.iter().filter(|(k, _)| k == "EXISTING").count(),
            2,
            "should have two EXISTING entries (original + override): {envs:?}"
        );
    }

    #[test]
    fn command_line_quotes_args_for_display() {
        let cmd = Command::new("git").args(["commit", "-m", "hello world"]);
        #[cfg(unix)]
        assert_eq!(cmd.command_line(), "git commit -m 'hello world'");
        #[cfg(not(unix))]
        assert_eq!(cmd.command_line(), "git commit -m \"hello world\"");
    }

    #[cfg(unix)]
    #[test]
    fn command_line_single_quotes_specials_and_empty_args() {
        // empty -> ''; embedded `'` -> '\''; the safe `x=1` stays bare.
        let cmd = Command::new("tool").args(["", "a'b", "x=1"]);
        assert_eq!(cmd.command_line(), r#"tool '' 'a'\''b' x=1"#);
    }

    #[test]
    fn timeout_grace_records_its_value() {
        use std::time::Duration;
        let cmd = Command::new("x").timeout_grace(Duration::from_secs(5));
        assert_eq!(cmd.configured_timeout_grace(), Some(Duration::from_secs(5)));
        assert_eq!(Command::new("x").configured_timeout_grace(), None);
    }

    #[test]
    fn cpu_affinity_is_canonical_and_last_write_wins() {
        let cmd = Command::new("x")
            .cpu_affinity([3, 1, 3, 2])
            .cpu_affinity([5, 4, 5]);
        assert_eq!(cmd.configured_cpu_affinity(), Some(&[4, 5][..]));
        assert_eq!(
            Command::new("x")
                .cpu_affinity(std::iter::empty())
                .configured_cpu_affinity(),
            Some(&[][..]),
            "an explicit invalid empty set remains inspectable until launch"
        );
    }

    #[test]
    fn spawn_detached_refuses_cpu_affinity_loudly() {
        let err = Command::new("x")
            .cpu_affinity([0])
            .spawn_detached()
            .expect_err("detached spawn cannot honor cross-platform affinity semantics");
        assert!(matches!(
            err.reason(),
            crate::ErrorReason::Unsupported { operation } if operation.contains("cpu_affinity")
        ));
    }

    #[cfg(windows)]
    #[test]
    fn to_tokio_command_refuses_windows_cpu_affinity() {
        let err = Command::new("x")
            .cpu_affinity([0])
            .to_tokio_command()
            .expect_err("a raw Windows command has no suspended-child configuration seam");
        assert!(matches!(
            err.reason(),
            crate::ErrorReason::Unsupported { operation } if operation.contains("cpu_affinity")
        ));
    }

    #[cfg(not(any(target_os = "linux", windows)))]
    #[test]
    fn cpu_affinity_is_unsupported_off_linux_and_windows() {
        let err = Command::new("x")
            .cpu_affinity([0])
            .to_tokio_command()
            .expect_err("unsupported affinity must fail before spawn");
        assert!(matches!(
            err.reason(),
            crate::ErrorReason::Unsupported { operation } if operation.contains("cpu_affinity")
        ));
    }

    #[test]
    fn inactivity_timeout_records_its_value_independently() {
        use std::time::Duration;
        let cmd = Command::new("x")
            .timeout(Duration::from_secs(30))
            .inactivity_timeout(Duration::from_secs(5));
        assert_eq!(
            cmd.configured_inactivity_timeout(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(cmd.configured_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(Command::new("x").configured_inactivity_timeout(), None);
    }

    #[cfg(all(unix, feature = "process-control"))]
    #[test]
    fn timeout_signal_defaults_to_term_and_is_configurable() {
        use crate::Signal;
        // Default (no `timeout_signal`) resolves to SIGTERM…
        assert_eq!(
            Command::new("x").timeout_signal_raw(),
            crate::sys::SIGTERM_RAW
        );
        // …and an explicit signal overrides it.
        assert_eq!(
            Command::new("x")
                .timeout_signal(Signal::Int)
                .timeout_signal_raw(),
            Signal::Int.raw(),
        );
    }

    // T-041: a same-named file with no recognized executable extension (e.g.
    // a unix shell script named `git` living beside a missing `git.exe`) must
    // not make `probe_dir` report a match — otherwise `find_in_path` returns
    // `found == Some(..)` for a file that Windows cannot actually run, and the
    // error-enrichment branch in `runner::launch` turns a genuinely missing
    // `git.exe` into `ErrorReason::Spawn` instead of `ErrorReason::NotFound`, breaking
    // `is_not_found()` for callers.
    #[cfg(windows)]
    #[test]
    fn probe_dir_rejects_extensionless_same_named_file_but_still_finds_pathext_sibling() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("git"), b"#!/bin/sh\necho hi\n")
            .expect("write extensionless file");

        assert!(
            super::probe_dir(dir.path(), OsStr::new("git")).is_none(),
            "an extensionless same-named file must not be reported as found"
        );

        // Once a real `.exe` sibling shows up, PATHEXT expansion must still
        // find it — the exact-name check tightening must not regress the
        // extension-search fallback. The resolved name's *case* follows
        // whatever the `PATHEXT` env var carries (commonly `.EXE`), so
        // compare case-insensitively rather than pinning an exact case.
        std::fs::write(dir.path().join("git.exe"), b"stub").expect("write git.exe");
        let found =
            super::probe_dir(dir.path(), OsStr::new("git")).expect("git.exe must now be found");
        assert!(
            found
                .to_string_lossy()
                .eq_ignore_ascii_case(&dir.path().join("git.exe").to_string_lossy()),
            "PATHEXT expansion must still resolve the real executable, got {found:?}"
        );
    }

    // Existing behavior must not regress: a candidate whose exact name
    // already carries a recognized executable extension (`git.cmd`) is
    // accepted directly, without needing the PATHEXT expansion loop.
    #[cfg(windows)]
    #[test]
    fn probe_dir_accepts_exact_name_already_carrying_a_recognized_extension() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("git.cmd"), b"@echo off").expect("write git.cmd");

        assert_eq!(
            super::probe_dir(dir.path(), OsStr::new("git.cmd")),
            Some(dir.path().join("git.cmd"))
        );
    }

    #[cfg(feature = "pty")]
    #[test]
    fn pty_size_builder_records_the_requested_geometry() {
        // Configured size is carried through to the launch-seam getter.
        let sized = Command::new("tui").use_pty().pty_size(120, 40);
        assert_eq!(sized.configured_pty_size(), Some((120, 40)));

        // Unset → `None`, so the backend falls back to its 80×24 default.
        assert_eq!(
            Command::new("tui").use_pty().configured_pty_size(),
            None,
            "an unset pty_size resolves to the backend default, not a stored value"
        );

        // Order-independent, and independent of `use_pty` (it is a plain stored
        // value the PTY launch path reads only when it is a PTY run).
        assert_eq!(
            Command::new("x")
                .pty_size(90, 30)
                .use_pty()
                .configured_pty_size(),
            Some((90, 30)),
        );
        assert_eq!(
            Command::new("x").pty_size(90, 30).configured_pty_size(),
            Some((90, 30)),
            "pty_size is stored even without use_pty (a documented no-op at launch)",
        );

        // The last call wins if set twice.
        assert_eq!(
            Command::new("x")
                .pty_size(10, 10)
                .pty_size(200, 50)
                .configured_pty_size(),
            Some((200, 50)),
        );
    }

    #[cfg(all(feature = "pty", windows))]
    #[test]
    fn resolved_conpty_env_uses_shared_identity_defaults_and_explicit_overrides() {
        let env = Command::new("tui")
            .use_pty()
            .pty_size(120, 40)
            .env_clear()
            .env("columns", "132")
            .env_remove("LINES")
            .resolved_pty_env()
            .expect("PTY identity requires an explicit ConPTY environment block");
        let find = |name: &str| {
            env.iter()
                .find(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case(name))
                .map(|(_, value)| value.to_string_lossy().into_owned())
        };

        assert_eq!(find("COLUMNS").as_deref(), Some("132"));
        assert_eq!(find("LINES"), None);
        assert_eq!(
            find("TERM"),
            None,
            "ConPTY exposes terminal capabilities without a synthesized TERM"
        );
    }
}
