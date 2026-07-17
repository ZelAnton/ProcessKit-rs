//! [`Command`] — a builder describing a process to run.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use encoding_rs::Encoding;

use crate::buffer::{LineTerminator, OutputBufferPolicy, StdioMode};
use crate::error::{Error, Result};
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
    output_buffer: OutputBufferPolicy,
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
    /// File-mode creation mask for the child (Unix `umask(2)`, see
    /// [`Self::umask`]). Unix-only — `Unsupported` elsewhere.
    umask: Option<u32>,
    /// Kill the direct child if this process dies abruptly (see
    /// [`Self::kill_on_parent_death`]).
    kill_on_parent_death: bool,
    /// Extra Windows process-creation flags (e.g. `CREATE_NO_WINDOW`), OR'd
    /// into the spawn by the Command-driven launch paths.
    creation_flags_extra: u32,
    /// When cancelled, the run's tree is killed and every consuming path
    /// resolves to `Error::Cancelled`. Cheap to clone (internally `Arc`'d), so
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
            timeout_grace: None,
            #[cfg(feature = "process-control")]
            timeout_signal: None,
            ok_codes: None,
            stdout_config: StreamConfig::new(),
            stderr_config: StreamConfig::new(),
            stdout_mode: StdioMode::Piped,
            stderr_mode: StdioMode::Piped,
            output_buffer: OutputBufferPolicy::unbounded(),
            retry: None,
            inherit_env: None,
            uid: None,
            gid: None,
            groups: None,
            setsid: false,
            priority: None,
            umask: None,
            kill_on_parent_death: false,
            creation_flags_extra: 0,
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
    /// If resolution fails everywhere, [`Error::NotFound`](crate::Error::NotFound)'s
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
    /// [`Error::Unsupported`](crate::Error::Unsupported) — a requested
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
    /// [`Error::Unsupported`](crate::Error::Unsupported) — never silently
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
    /// [`Error::Unsupported`](crate::Error::Unsupported).
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
    /// [`Error::Unsupported`](crate::Error::Unsupported) — see
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

    /// Set the file-mode creation mask for the child (Unix `umask(2)`),
    /// controlling the default permissions of files it creates.
    ///
    /// Applied via `pre_exec`, alongside [`setsid`](Self::setsid)/
    /// [`groups`](Self::groups) — another knob on that same seam. On
    /// non-Unix targets the run fails with
    /// [`Error::Unsupported`](crate::Error::Unsupported) rather than
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
    /// [`ProcessResult`](crate::ProcessResult), and
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

    /// Make the [`timeout`](Self::timeout) **graceful**: at the deadline the run's
    /// tree is sent `SIGTERM` (or the signal chosen via `timeout_signal`, with the
    /// `process-control` feature), given up to `grace` to exit, then `SIGKILL`ed.
    /// Without it the deadline hard-kills at once. No effect unless
    /// [`timeout`](Self::timeout) is also set.
    ///
    /// **Windows** has no signal tier: the deadline kills the job atomically
    /// regardless of `grace`. Either way
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
    /// [`ProcessRunnerExt`](crate::ProcessRunnerExt)) and
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
    /// finishers) resolve to [`Error::Cancelled`](crate::Error::Cancelled).
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
    /// [`Error::NotReady`](crate::Error::NotReady), not `Cancelled` — the
    /// consuming finisher afterwards still reports `Cancelled`.
    ///
    /// A cancelled run is never retried: [`retry`](Self::retry) policies and
    /// [`Supervisor`](crate::Supervisor) restarts both treat
    /// `Error::Cancelled` as terminal — the token stays cancelled forever, so
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
    /// [`Command`](Self::run), on [`ProcessRunnerExt`](crate::ProcessRunnerExt),
    /// and on [`CliClient`](crate::CliClient): the ones that surface failure as an
    /// [`Error`] the classifier can inspect (e.g. a transient network failure in
    /// `stderr`, or [`Error::Timeout`](crate::Error::Timeout)). The non-erroring
    /// `output_string`/`output_bytes` paths don't retry.
    ///
    /// Each attempt **re-executes the whole command** — a fresh process. Only
    /// retry operations that are safe to repeat: a side effect that already landed
    /// before the failure (a `git push` that reached the server, then dropped the
    /// connection) will be replayed. Prefer to gate retries on a classifier that
    /// matches *pre-effect* failures (DNS/connection errors, [`Error::Timeout`]
    /// while still connecting) rather than any non-zero exit.
    ///
    /// A [`timeout`](Self::timeout) bounds **each attempt**, not the whole retried
    /// operation — there is no total wall-clock ceiling across retries (worst case
    /// ≈ `attempts × timeout` + the sum of the backoffs). Bound the total with
    /// [`cancel_on`](Self::cancel_on) (a `Cancelled` is terminal — never retried).
    ///
    /// Because the command is replayed from scratch, a **one-shot** stdin source
    /// ([`Stdin::from_reader`](crate::Stdin::from_reader) /
    /// [`from_lines`](crate::Stdin::from_lines)) can't survive a retry: its
    /// payload is consumed by the first attempt and can't be re-fed. So such a
    /// command is **not retried at all** — the first attempt's error is returned
    /// as-is (retrying would either replay empty stdin or spuriously classify the
    /// re-consume), and the retry policy is inert for it. Use a reusable source
    /// (`from_string`/`from_bytes`/`from_file`/`from_iter_lines`) when retrying.
    /// (A one-shot source *re-run* outside this retry loop — a `Supervisor`
    /// incarnation, a pipeline re-run — does fail loud with
    /// [`Error::Io`](crate::Error::Io) `InvalidInput` at launch instead.)
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
    /// [`Error::Timeout`]: crate::Error::Timeout
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
    /// [`Error::Io`](crate::Error::Io) (`InvalidInput`) — the same failure mode
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
    /// [`StdioMode::Piped`](crate::StdioMode::Piped)).
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
    /// `output_events`) yield an empty stream. Use a discard verb (`wait`) to run
    /// a command whose stdout you don't want to capture.
    pub fn stdout(mut self, mode: crate::StdioMode) -> Self {
        self.stdout_mode = mode;
        self
    }

    /// Set how the child's standard error stream is connected (default:
    /// [`StdioMode::Piped`](crate::StdioMode::Piped)).
    ///
    /// Same semantics as [`stdout`](Self::stdout): `Piped` captures,
    /// `Inherit` passes through, `Null` suppresses.
    pub fn stderr(mut self, mode: crate::StdioMode) -> Self {
        self.stderr_mode = mode;
        self
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
    /// lines are buffered and a live `stdout_lines`/`output_events` consumer
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
    /// likewise not teed under those verbs.
    ///
    /// Requires stdout to be [`Piped`](crate::StdioMode::Piped) (the default):
    /// the tee fires from the capture pump, so it is a no-op under
    /// [`stdout(Inherit)`](Self::stdout) / [`stdout(Null)`](Self::stdout), which
    /// run no pump. It is likewise inert under
    /// [`output_bytes`](Self::output_bytes), which captures stdout **raw** (no
    /// line pump) — reach for a stdout tee with the line verbs (`output_string`,
    /// `start` + `stdout_lines`, `output_events`).
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

    /// Cap the in-memory backlog of captured output lines (see
    /// [`OutputBufferPolicy`]). The pump still drains the pipe; only retention is
    /// bounded.
    pub fn output_buffer(mut self, policy: OutputBufferPolicy) -> Self {
        self.output_buffer = policy;
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
    /// [`output_events`](crate::RunningProcess::output_events), the
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
    pub fn line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stdout_config.terminator = terminator;
        self.stderr_config.terminator = terminator;
        self
    }

    /// Choose where the line pump splits **stdout** into lines (see
    /// [`LineTerminator`]); the stderr framing is left untouched. See
    /// [`line_terminator`](Self::line_terminator) for both streams at once.
    pub fn stdout_line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stdout_config.terminator = terminator;
        self
    }

    /// Choose where the line pump splits **stderr** into lines (see
    /// [`LineTerminator`]); the stdout framing is left untouched. Handy when
    /// progress output lands on stderr while stdout stays newline-structured.
    pub fn stderr_line_terminator(mut self, terminator: LineTerminator) -> Self {
        self.stderr_config.terminator = terminator;
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
    /// handlers and the [`stdout_tee`](Self::stdout_tee)/[`stderr_tee`](Self::stderr_tee)
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
        clone
    }

    /// The stdout stream's pump config (encoding/handler/tee/terminator), cloned
    /// for the spawn. Replaces the four individual `out_encoding`/`stdout_handler`/
    /// `stdout_tee_sink`/`out_line_terminator` proxies — the launch paths take the
    /// whole [`StreamConfig`]. Cheap: handler and tee are `Arc`s.
    pub(crate) fn stdout_config(&self) -> StreamConfig {
        self.stdout_config.clone()
    }

    /// The stderr stream's pump config — see [`stdout_config`](Self::stdout_config).
    pub(crate) fn stderr_config(&self) -> StreamConfig {
        self.stderr_config.clone()
    }

    pub(crate) fn output_buffer_policy(&self) -> OutputBufferPolicy {
        self.output_buffer
    }

    pub(crate) fn retry_config(&self) -> Option<RetryConfig> {
        self.retry.clone()
    }

    /// Whether stdout is captured into a pipe (vs `Inherit`/`Null`). The bulk
    /// capture verbs use this to fail loudly instead of returning silently-empty
    /// output when stdout wasn't piped.
    pub(crate) fn stdout_is_piped(&self) -> bool {
        matches!(self.stdout_mode, StdioMode::Piped)
    }

    pub(crate) fn program_name(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    /// The [`prefer_local`](Self::prefer_local) directories, in priority order
    /// (read by the `Error::NotFound` diagnostic enrichment in `runner.rs`).
    pub(crate) fn prefer_local_dirs(&self) -> &[PathBuf] {
        &self.prefer_local
    }

    /// Whether the command customizes the environment in a way that could move
    /// `PATH` away from the process `PATH` — an explicit `PATH` override/removal,
    /// [`env_clear`](Self::env_clear), or [`inherit_env`](Self::inherit_env)
    /// (which clears the inherited set). When true, the *`PATH`*-directory
    /// naming in [`Error::NotFound`](crate::Error::NotFound) is skipped:
    /// `find_in_path` reads the *process* `PATH`, so against a custom child
    /// `PATH` that list would be wrong. [`prefer_local`](Self::prefer_local)
    /// directories are unaffected by this gate and still get named — they're
    /// resolved by plain filesystem probes on the parent side, independent of
    /// the child's environment. A missing program still surfaces as
    /// `Error::NotFound` (so [`is_not_found`](crate::Error::is_not_found)
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

    /// The configured stdin source, if any.
    pub fn stdin_source(&self) -> Option<&Stdin> {
        self.stdin.as_ref()
    }

    /// The configured stdin source that will actually be passed to the child.
    ///
    /// [`keep_stdin_open`](Self::keep_stdin_open) takes precedence over a source
    /// set with [`stdin`](Self::stdin); this mirrors the shared
    /// [`crate::runner::take_stdin_for_run`] launch boundary.
    pub(crate) fn effective_stdin_source(&self) -> Option<&Stdin> {
        (!self.keeps_stdin_open())
            .then_some(())
            .and(self.stdin_source())
    }

    /// The configured deadline, if any — `Some(d)` for a
    /// [`timeout(d)`](Self::timeout), `None` for both an unset timeout and an
    /// explicitly [`no_timeout`](Self::no_timeout) (neither imposes a deadline).
    pub fn configured_timeout(&self) -> Option<Duration> {
        self.timeout.as_duration()
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

    /// Build a `tokio::process::Command` for the low-level
    /// [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape hatch.
    /// Not part of the advertised surface; prefer the `start`/`output_string`/`run` verbs.
    #[doc(hidden)]
    pub fn to_tokio_command(&self) -> tokio::process::Command {
        self.build_tokio()
    }

    /// Build the `tokio` command with stdio wired for capture. Containment
    /// (cgroup/job/process-group) is added by the group's `spawn`.
    pub(crate) fn build_tokio(&self) -> tokio::process::Command {
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
        for (key, value) in &self.envs {
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
            // Priority and umask are independent of the privilege-drop hooks
            // below; register them first so a `Priority::High` that needs
            // CAP_SYS_NICE/root is still requested while the child holds its
            // original (pre-drop) privileges. Both the `Some(groups)` and
            // `None` branches below perform the whole uid/gid drop inside a
            // user `pre_exec` closure registered *after* these hooks, so this
            // ordering guarantee holds uniformly regardless of whether
            // `groups` was set.
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
        cmd.stdout(match self.stdout_mode {
            StdioMode::Piped => Stdio::piped(),
            StdioMode::Inherit => Stdio::inherit(),
            StdioMode::Null => Stdio::null(),
        });
        cmd.stderr(match self.stderr_mode {
            StdioMode::Piped => Stdio::piped(),
            StdioMode::Inherit => Stdio::inherit(),
            StdioMode::Null => Stdio::null(),
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
        cmd
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
    /// - [`Error::NotFound`] — the program could not be located (not installed,
    ///   not on `PATH`, or the given path does not resolve to an executable).
    /// - [`Error::Spawn`] — the program was located but the OS refused to start
    ///   it (permission denied, a missing or non-directory working directory, a
    ///   Windows `.cmd`/`.bat` that needs `cmd.exe`, …).
    /// - [`Error::Unsupported`] — a requested POSIX-only primitive (running as
    ///   another user/group, a new session via `setsid`, or a `umask`) is not
    ///   available on this platform.
    /// - [`Error::Cancelled`] — the [`cancel_on`](Self::cancel_on) token was
    ///   already cancelled before the spawn.
    /// - [`Error::Io`] — the private [`ProcessGroup`](crate::ProcessGroup) backing
    ///   the run could not be created, or a one-shot streaming stdin source
    ///   ([`Stdin::from_reader`](crate::Stdin::from_reader) /
    ///   [`Stdin::from_lines`](crate::Stdin::from_lines)) was already consumed by
    ///   a previous run.
    #[cfg_attr(
        feature = "limits",
        doc = "- [`Error::ResourceLimit`](crate::Error::ResourceLimit) — a resource cap configured on the run's group could not be enforced."
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
    /// beyond launch, only [`Error::Cancelled`] (a cancellation is always
    /// raised), [`Error::OutputTooLarge`] (a fail-loud buffer overflowed),
    /// [`Error::Stdin`] (a non-broken-pipe stdin failure on an
    /// otherwise-successful run), and [`Error::Io`] surface.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().start(self).await?.output_string().await
    }

    /// Run to completion and capture stdout as raw bytes (plus stderr/exit code).
    ///
    /// # Errors
    ///
    /// Identical to [`output_string`](Self::output_string) — a non-zero exit, a
    /// timeout, or a signal-kill is captured in the [`ProcessResult`], not raised
    /// — except that a fail-loud [`Error::OutputTooLarge`] applies to the raw
    /// stdout *byte* ceiling.
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        JobRunner::new().start(self).await?.output_bytes().await
    }

    /// Run to completion and return just the exit code (output is discarded). A
    /// run that yields no code surfaces as an error — a timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), a signal-kill as
    /// [`Error::Signalled`](crate::Error::Signalled) — consistent with
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code) and
    /// [`CliClient::exit_code`](crate::CliClient::exit_code).
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start), plus — when the run
    /// produced no code — [`Error::Timeout`] (the deadline elapsed),
    /// [`Error::Signalled`] (killed by a signal), or [`Error::Cancelled`]. A
    /// non-zero exit is returned as the code, not raised.
    pub async fn exit_code(&self) -> Result<i32> {
        JobRunner::new().exit_code(self).await
    }

    /// Run to completion, requiring an **accepted** exit (`0` by default, widened
    /// by [`ok_codes`](Self::ok_codes)), and return trimmed stdout. Any other
    /// code is [`Error::Exit`](crate::Error::Exit).
    ///
    /// # Errors
    ///
    /// The launch failures listed on [`start`](Self::start), plus the
    /// success-checking failures: [`Error::Exit`] (a non-accepted exit code),
    /// [`Error::Signalled`] (a signal-kill), [`Error::Timeout`] (the deadline
    /// elapsed — *raised* here, unlike on
    /// [`output_string`](Self::output_string)), [`Error::Cancelled`],
    /// [`Error::OutputTooLarge`] (a fail-loud buffer truncated the presented
    /// stdout), and [`Error::Stdin`] (a non-broken-pipe stdin failure on an
    /// otherwise-successful run).
    pub async fn run(&self) -> Result<String> {
        JobRunner::new().run(self).await
    }

    /// Run to completion, require an **accepted** exit, and return the full
    /// captured [`ProcessResult`] (untrimmed stdout) — the building block when you
    /// need the whole result after success-checking rather than trimmed stdout
    /// ([`run`](Self::run)) or the raw result ([`output_string`](Self::output_string)).
    /// Consistent with [`ProcessRunnerExt::checked`](crate::ProcessRunnerExt::checked)
    /// and [`CliClient::checked`](crate::CliClient::checked).
    ///
    /// # Errors
    ///
    /// The same success-checking surface as [`run`](Self::run) —
    /// [`Error::Exit`] / [`Error::Signalled`] / [`Error::Timeout`] /
    /// [`Error::Cancelled`] / [`Error::Stdin`], atop the launch failures on
    /// [`start`](Self::start) — except that, as the lenient building block,
    /// `checked` does **not** fail loud on a bounded-buffer truncation (inspect
    /// [`ProcessResult::truncated`](crate::ProcessResult::truncated) yourself), so
    /// it never returns [`Error::OutputTooLarge`].
    pub async fn checked(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().checked(self).await
    }

    /// Run for the side effect: require an **accepted** exit (`0`, or any code in
    /// [`ok_codes`](Self::ok_codes)) and discard the output. Consistent with
    /// [`ProcessRunnerExt::run_unit`](crate::ProcessRunnerExt::run_unit) and
    /// [`CliClient::run_unit`](crate::CliClient::run_unit).
    ///
    /// # Errors
    ///
    /// The same surface as [`checked`](Self::checked) (the launch failures on
    /// [`start`](Self::start) plus [`Error::Exit`] / [`Error::Signalled`] /
    /// [`Error::Timeout`] / [`Error::Cancelled`] / [`Error::Stdin`]); only the
    /// captured output is discarded.
    pub async fn run_unit(&self) -> Result<()> {
        JobRunner::new().run_unit(self).await
    }

    /// Run a predicate command and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err` (any other code
    /// as [`Error::Exit`], a timeout as [`Error::Timeout`](crate::Error::Timeout),
    /// a signal-kill as [`Error::Signalled`](crate::Error::Signalled)). For tools
    /// whose exit code *is* the answer —
    /// `git diff --quiet`, `git show-ref --verify --quiet`, `grep -q`, …
    ///
    /// # Errors
    ///
    /// Any exit code other than `0`/`1` becomes [`Error::Exit`], and — atop the
    /// launch failures on [`start`](Self::start) — a run that produced no code
    /// errors as [`Error::Timeout`], [`Error::Signalled`], or
    /// [`Error::Cancelled`]. The strict `0`/`1` contract holds regardless of the
    /// command's [`ok_codes`](Self::ok_codes).
    pub async fn probe(&self) -> Result<bool> {
        JobRunner::new().probe(self).await
    }

    /// Run (requiring an **accepted** exit) and feed stdout to an **infallible**
    /// `parse` closure, returning the parsed value. Fails loud on a bounded-buffer
    /// truncation so the parser never sees a clipped tail. Consistent with
    /// [`ProcessRunnerExt::parse`](crate::ProcessRunnerExt::parse) and
    /// [`CliClient::parse`](crate::CliClient::parse).
    ///
    /// # Errors
    ///
    /// The success-checking surface of [`run`](Self::run) (the launch failures on
    /// [`start`](Self::start), plus [`Error::Exit`] / [`Error::Signalled`] /
    /// [`Error::Timeout`] / [`Error::Cancelled`] / [`Error::Stdin`]), plus
    /// [`Error::OutputTooLarge`] when a fail-loud buffer truncated the stdout the
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
    /// [`Error::Parse`](crate::Error::Parse) or whatever the closure returns).
    /// Fails loud on truncation. Consistent with
    /// [`ProcessRunnerExt::try_parse`](crate::ProcessRunnerExt::try_parse) and
    /// [`CliClient::try_parse`](crate::CliClient::try_parse).
    ///
    /// # Errors
    ///
    /// Everything [`parse`](Self::parse) can return, plus whatever the fallible
    /// `parse` closure yields on malformed output — typically
    /// [`Error::Parse`](crate::Error::Parse).
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
    /// [`Error::Timeout`] when a [`timeout`](Self::timeout) is set and its
    /// deadline elapses mid-stream (which tears the process down),
    /// [`Error::Cancelled`], or [`Error::Io`] while streaming. A stream that ends
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
    /// a hit spawns instead of raising [`Error::Spawn`]. The one residual
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
    /// [`Error::NotFound`](crate::Error::NotFound) when the program can't be
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
            ProgramResolution::NotFound { searched } => Err(Error::NotFound {
                program: self.program_name(),
                searched,
            }),
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
            .field("timeout_grace", &self.timeout_grace)
            .field("ok_codes", &self.ok_codes)
            .field("stdout_mode", &self.stdout_mode)
            .field("stderr_mode", &self.stderr_mode)
            .field("has_stdout_handler", &self.stdout_config.handler.is_some())
            .field("has_stderr_handler", &self.stderr_config.handler.is_some())
            .field("has_stdout_tee", &self.stdout_config.tee.is_some())
            .field("has_stderr_tee", &self.stderr_config.tee.is_some())
            .field("output_buffer", &self.output_buffer)
            .field("stdout_encoding", &self.stdout_config.encoding.name())
            .field("stderr_encoding", &self.stderr_config.encoding.name())
            .field("stdout_line_terminator", &self.stdout_config.terminator)
            .field("stderr_line_terminator", &self.stderr_config.terminator)
            .field("has_retry", &self.retry.is_some())
            .field("inherit_env", &self.inherit_env)
            .field("uid", &self.uid)
            .field("gid", &self.gid)
            // Security-relevant: the supplementary-group set of a privilege drop.
            .field("groups", &self.groups)
            .field("setsid", &self.setsid)
            .field("priority", &self.priority)
            .field("umask", &self.umask)
            .field("kill_on_parent_death", &self.kill_on_parent_death)
            .field("creation_flags_extra", &self.creation_flags_extra);
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
/// [`Error::NotFound`](crate::Error::NotFound) enrichment (in `runner.rs`) and
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
    /// [`Error::NotFound`](crate::Error::NotFound)'s field: `Some` for a
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
/// [`Error::NotFound`](crate::Error::NotFound): the [`Command::prefer_local`]
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
        let built = cmd.build_tokio();
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
    fn create_no_window_sets_the_flag_bit() {
        let cmd = Command::new("x").create_no_window();
        assert_eq!(cmd.extra_creation_flags(), 0x0800_0000);
        assert_eq!(Command::new("x").extra_creation_flags(), 0);
    }

    #[test]
    fn priority_and_umask_record_their_requests() {
        let cmd = Command::new("x")
            .priority(crate::Priority::BelowNormal)
            .umask(0o022);
        let debug = format!("{cmd:?}");
        assert!(debug.contains("BelowNormal"), "debug: {debug}");
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
        let tokio_cmd = cmd.build_tokio();
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

        let built = Command::new(program).prefer_local(dir.path()).build_tokio();
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
        let tokio_cmd = cmd.build_tokio();
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
        let tokio_cmd = cmd.build_tokio();
        assert_eq!(
            tokio_cmd.as_std().get_program(),
            OsStr::new("not-in-prefer-local"),
            "a prefer_local miss must leave the bare name for the OS's own PATH search"
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
        let tokio_cmd = cmd.build_tokio();
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
        let tokio_cmd = cmd.build_tokio();
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
        let tokio_cmd = cmd.build_tokio();
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

    // T-101: a bare name that resolves nowhere yields `Error::NotFound` whose
    // `searched` names the `prefer_local` directories (first) — the same typed
    // error the launch raises, reusing `Error::NotFound` rather than a parallel.
    #[test]
    fn resolve_program_missing_bare_name_is_not_found_with_searched() {
        let dir = tempfile::tempdir().expect("temp dir"); // deliberately empty
        let err = Command::new("pk-definitely-absent-tool-101")
            .prefer_local(dir.path())
            .resolve_program()
            .expect_err("an absent program must not resolve");
        assert!(err.is_not_found(), "must classify as not-found: {err:?}");
        match err {
            crate::Error::NotFound { searched, .. } => {
                let searched = searched.expect("a bare-name lookup reports searched dirs");
                assert!(
                    searched.contains(&dir.path().to_string_lossy().into_owned()),
                    "searched must include the prefer_local directory: {searched}"
                );
            }
            other => panic!("expected Error::NotFound, got {other:?}"),
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
        match err {
            crate::Error::NotFound { searched, .. } => assert_eq!(
                searched, None,
                "a path-form lookup applies no PATH search, so searched is None"
            ),
            other => panic!("expected Error::NotFound, got {other:?}"),
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
    // `git.exe` into `Error::Spawn` instead of `Error::NotFound`, breaking
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
}
