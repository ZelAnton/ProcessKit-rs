//! [`Command`] — a builder describing a process to run.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use encoding_rs::{Encoding, UTF_8};

use crate::buffer::{OutputBufferPolicy, StdioMode};
use crate::error::{Error, Result};
use crate::pump::LineHandler;
use crate::result::ProcessResult;
use crate::runner::{JobRunner, ProcessRunnerExt};
use crate::running::RunningProcess;
use crate::stdin::Stdin;

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
    envs: Vec<(OsString, Option<OsString>)>,
    env_clear: bool,
    stdin: Option<Stdin>,
    keep_stdin_open: bool,
    /// Exempt this stage from pipefail attribution (see [`Self::unchecked`]).
    unchecked: bool,
    timeout: Option<Duration>,
    /// Grace window after the deadline before `SIGKILL`; its presence makes the
    /// timeout graceful (see [`Self::timeout_grace`]).
    timeout_grace: Option<Duration>,
    /// Signal sent at the start of a graceful timeout (default `SIGTERM`).
    #[cfg(feature = "process-control")]
    timeout_signal: Option<crate::Signal>,
    /// Exit codes treated as success by the checking verbs (`run`/`run_unit`/
    /// `checked` via [`ProcessResult::ensure_success`]). `None` accepts only `0`.
    ok_codes: Option<Vec<i32>>,
    stdout_handler: Option<LineHandler>,
    stderr_handler: Option<LineHandler>,
    stdout_mode: StdioMode,
    stderr_mode: StdioMode,
    output_buffer: OutputBufferPolicy,
    stdout_encoding: &'static Encoding,
    stderr_encoding: &'static Encoding,
    retry: Option<RetryPolicy>,
    /// `Some` once `inherit_env` was called (even with an empty list): clear
    /// the inherited environment and copy only these parent vars.
    inherit_env: Option<Vec<OsString>>,
    uid: Option<u32>,
    gid: Option<u32>,
    /// Supplementary group ids to set (Unix privilege drop); `Some` replaces the
    /// inherited set. See [`Self::groups`].
    groups: Option<Vec<u32>>,
    setsid: bool,
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
    #[cfg(feature = "cancellation")]
    cancel_token: Option<tokio_util::sync::CancellationToken>,
}

/// A retry policy attached to a [`Command`] via [`Command::retry`], honored by
/// the success-checking run helpers. Cheap to clone (the classifier is `Arc`'d).
#[derive(Clone)]
pub(crate) struct RetryPolicy {
    pub(crate) max_attempts: u32,
    pub(crate) backoff: Duration,
    pub(crate) classifier: Arc<dyn Fn(&Error) -> bool + Send + Sync>,
}

impl Command {
    /// Start a command for `program` (resolved on `PATH`).
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            args: Vec::new(),
            cwd: None,
            envs: Vec::new(),
            env_clear: false,
            stdin: None,
            keep_stdin_open: false,
            unchecked: false,
            timeout: None,
            timeout_grace: None,
            #[cfg(feature = "process-control")]
            timeout_signal: None,
            ok_codes: None,
            stdout_handler: None,
            stderr_handler: None,
            stdout_mode: StdioMode::Piped,
            stderr_mode: StdioMode::Piped,
            output_buffer: OutputBufferPolicy::unbounded(),
            stdout_encoding: UTF_8,
            stderr_encoding: UTF_8,
            retry: None,
            inherit_env: None,
            uid: None,
            gid: None,
            groups: None,
            setsid: false,
            kill_on_parent_death: false,
            creation_flags_extra: 0,
            #[cfg(feature = "cancellation")]
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
    pub fn current_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.cwd = Some(dir.as_ref().as_os_str().to_os_string());
        self
    }

    /// Set (or, with a `None` value, remove) an environment variable.
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

    /// Set multiple environment variables at once.
    ///
    /// Equivalent to chaining [`env`](Self::env) calls; order is preserved and
    /// later entries win on a duplicated key.
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
    ///
    /// ```no_run
    /// # fn demo() -> processkit::Command {
    /// use processkit::Command;
    /// // Drop to uid/gid 1000 with exactly that user's groups.
    /// Command::new("worker").uid(1000).gid(1000).groups([1000, 27])
    /// # }
    /// ```
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
    /// One honest Linux caveat: the death signal fires when the spawning
    /// **thread** dies, not only the process — on a multi-threaded tokio
    /// runtime, a worker thread retired while the child lives would kill it
    /// early (for the strongest guarantee spawn from a current-thread
    /// runtime). The parent-died-before-arming race is closed in the child
    /// by re-checking `getppid()` against the spawner's pid captured before
    /// the fork — safe in containers where the spawner itself is PID 1.
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
        // CREATE_NO_WINDOW = 0x0800_0000; spelled as a literal so the field
        // exists (and tests compile) on every platform.
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
    pub fn unchecked(mut self) -> Self {
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
    }

    /// Kill the run if it exceeds `timeout`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
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
    /// An empty set is ignored (it would make every exit a failure); the default
    /// stays `[0]`. Does not change [`exit_code`](Self::exit_code) (always the raw
    /// code) or [`probe`](Self::probe) (always the 0/1 convention).
    pub fn ok_codes(mut self, codes: impl IntoIterator<Item = i32>) -> Self {
        let codes: Vec<i32> = codes.into_iter().collect();
        self.ok_codes = (!codes.is_empty()).then_some(codes);
        self
    }

    /// Tie this run to `token`: cancelling it kills the process tree and makes
    /// every consuming path (`run`/`output_string`/`output_bytes`/`wait`/
    /// `exit_code`/`probe`/`profile`/`finish_streamed` and the streamed
    /// finishers) resolve to [`Error::Cancelled`](crate::Error::Cancelled).
    /// In a [`Pipeline`](crate::Pipeline), a token on any stage cancels that
    /// stage and the cancellation errors the whole pipeline (the private
    /// pipeline group tears the other stages down).
    ///
    /// Unlike [`timeout`](Self::timeout) — which is *captured* in the
    /// [`ProcessResult`] (`timed_out`) without erroring on the non-checking
    /// paths — a cancellation is **always** an error, on every path. When both
    /// fire, cancellation wins (it is checked first). An already-cancelled
    /// token short-circuits before spawning. On a private group the whole tree
    /// is killed; on a shared group
    /// ([`ProcessGroup::start`](crate::ProcessGroup::start)) only the child
    /// is, exactly like `timeout`. [`wait_any`](crate::wait_any) and
    /// [`first_line`](Self::first_line) don't synthesize the error for a
    /// *mid-run* cancel — their stream simply ends, mirroring how they treat
    /// `timeout` — though a token that was already cancelled still surfaces
    /// the pre-spawn `Err(Cancelled)` short-circuit. Likewise a mid-run cancel
    /// during [`wait_for_line`](crate::RunningProcess::wait_for_line) closes
    /// the stream and surfaces as that probe's
    /// [`Error::NotReady`](crate::Error::NotReady), not `Cancelled` — the
    /// consuming finisher afterwards still reports `Cancelled`.
    ///
    /// A cancelled run is never retried: [`retry`](Self::retry) policies and
    /// [`Supervisor`](crate::Supervisor) restarts both treat
    /// `Error::Cancelled` as terminal — the token stays cancelled forever, so
    /// another attempt could only fail the same way.
    #[cfg(feature = "cancellation")]
    pub fn cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Retry the run while `retry_if` accepts the error, up to `max_attempts`
    /// total attempts, sleeping `backoff` between tries.
    ///
    /// Applies to the **success-checking** helpers — [`run`](Self::run),
    /// [`exit_code`](Self::exit_code), [`probe`](Self::probe), and the
    /// [`CliClient`](crate::CliClient) `run`/`run_unit`/`exit_code`/`parse`/`try_parse`
    /// helpers — i.e. the ones that surface failure as an [`Error`] the classifier
    /// can inspect (e.g. a transient network failure in `stderr`, or
    /// [`Error::Timeout`](crate::Error::Timeout)). The non-erroring
    /// `output_string`/`output_bytes`/`output` paths don't retry.
    ///
    /// Each attempt **re-executes the whole command** — a fresh process. Only
    /// retry operations that are safe to repeat: a side effect that already landed
    /// before the failure (a `git push` that reached the server, then dropped the
    /// connection) will be replayed. Prefer to gate retries on a classifier that
    /// matches *pre-effect* failures (DNS/connection errors, [`Error::Timeout`]
    /// while still connecting) rather than any non-zero exit.
    ///
    /// Because the command is replayed from scratch, a **one-shot** stdin source
    /// ([`Stdin::from_reader`](crate::Stdin::from_reader) /
    /// [`from_lines`](crate::Stdin::from_lines)) won't survive a retry — the second
    /// attempt sees empty stdin. Use a reusable source
    /// (`from_string`/`from_bytes`/`from_file`/`from_iter_lines`) when retrying.
    ///
    /// [`Error::Timeout`]: crate::Error::Timeout
    pub fn retry(
        mut self,
        max_attempts: u32,
        backoff: Duration,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry = Some(RetryPolicy {
            max_attempts,
            backoff,
            classifier: Arc::new(retry_if),
        });
        self
    }

    /// Leave stdin open after start so the child can be driven interactively via
    /// [`RunningProcess::standard_input`](crate::RunningProcess::standard_input).
    /// Takes precedence over a [`stdin`](Self::stdin) source — when set, that
    /// source is ignored and the pipe is handed to the caller instead.
    ///
    /// The open pipe lives until the caller takes it (`standard_input`) or a
    /// consuming verb runs: at consume time an **untaken** pipe is closed
    /// (nothing could ever write to it again), so a stdin-reading child sees
    /// EOF instead of blocking — combining `keep_stdin_open` with a bulk
    /// helper (`output_string`, `run`, …) without ever taking the writer is
    /// equivalent to not setting it. A writer the caller *did* take is
    /// unaffected and keeps the pipe until dropped or
    /// [`finish`](crate::ProcessStdin::finish)ed.
    pub fn keep_stdin_open(mut self) -> Self {
        self.keep_stdin_open = true;
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
    /// `finish_streamed`) all handler invocations happen-before the awaited
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
    pub fn on_stdout_line<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.stdout_handler = Some(Arc::new(handler));
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
        self.stderr_handler = Some(Arc::new(handler));
        self
    }

    /// Set how the child's standard output stream is connected (default:
    /// [`StdioMode::Piped`](crate::StdioMode::Piped)).
    ///
    /// - **`Piped`** (default) — captured into a pipe; all output-retrieval
    ///   verbs (`output_string`, `stdout_lines`, …) read from it.
    /// - **`Inherit`** — the child shares the parent's stdout; output appears
    ///   in the terminal/log but is not captured. `output_string` returns an
    ///   empty stdout; `stdout_lines` / `output_events` yield nothing.
    /// - **`Null`** — suppressed entirely (redirected to `/dev/null`).
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

    /// Tee every decoded stdout line to `writer` as it is produced —
    /// capture *and* stream to `writer` simultaneously.
    ///
    /// Equivalent to [`on_stdout_line`](Self::on_stdout_line) with a
    /// `writeln!` handler. Replaces any previously set stdout handler.
    /// Compose multiple sinks inside a single [`on_stdout_line`](Self::on_stdout_line)
    /// handler when you need more than one.
    pub fn stdout_tee<W>(mut self, writer: W) -> Self
    where
        W: std::io::Write + Send + 'static,
    {
        let writer = Arc::new(std::sync::Mutex::new(writer));
        self.stdout_handler = Some(Arc::new(move |line: &str| {
            if let Ok(mut w) = writer.lock() {
                let _ = writeln!(w, "{line}");
            }
        }));
        self
    }

    /// Tee every decoded stderr line to `writer` as it is produced.
    ///
    /// Same contract as [`stdout_tee`](Self::stdout_tee). Replaces any
    /// previously set stderr handler.
    pub fn stderr_tee<W>(mut self, writer: W) -> Self
    where
        W: std::io::Write + Send + 'static,
    {
        let writer = Arc::new(std::sync::Mutex::new(writer));
        self.stderr_handler = Some(Arc::new(move |line: &str| {
            if let Ok(mut w) = writer.lock() {
                let _ = writeln!(w, "{line}");
            }
        }));
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
        self.stdout_encoding = encoding;
        self
    }

    /// Decode stderr with `encoding` instead of UTF-8.
    pub fn stderr_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stderr_encoding = encoding;
        self
    }

    /// Decode both stdout and stderr with `encoding`.
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stdout_encoding = encoding;
        self.stderr_encoding = encoding;
        self
    }

    // --- Accessors used by the runner layer --------------------------------

    pub(crate) fn keeps_stdin_open(&self) -> bool {
        self.keep_stdin_open
    }

    pub(crate) fn stdout_handler(&self) -> Option<LineHandler> {
        self.stdout_handler.clone()
    }

    pub(crate) fn stderr_handler(&self) -> Option<LineHandler> {
        self.stderr_handler.clone()
    }

    pub(crate) fn output_buffer_policy(&self) -> OutputBufferPolicy {
        self.output_buffer
    }

    pub(crate) fn retry_policy(&self) -> Option<RetryPolicy> {
        self.retry.clone()
    }

    pub(crate) fn out_encoding(&self) -> &'static Encoding {
        self.stdout_encoding
    }

    pub(crate) fn err_encoding(&self) -> &'static Encoding {
        self.stderr_encoding
    }

    pub(crate) fn program_name(&self) -> String {
        self.program.to_string_lossy().into_owned()
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
    #[cfg(feature = "cancellation")]
    pub(crate) fn cancel_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.cancel_token.clone()
    }

    /// Extra Windows creation flags (read by the spawn seam on every target).
    pub(crate) fn extra_creation_flags(&self) -> u32 {
        self.creation_flags_extra
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

    // ----- Public accessors -----------------------------------------------
    // Exposed so external `ScriptedRunner::when(|cmd| …)` predicates and other
    // inspection can read what a command will run. Named to avoid clashing with
    // the same-named builder methods (`program` has no builder; `arguments` vs
    // `args`, `working_dir` vs `current_dir`, etc.).

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
    /// guaranteed to round-trip through one.
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

    /// The configured timeout, if any.
    pub fn configured_timeout(&self) -> Option<Duration> {
        self.timeout
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

    /// Build a `tokio::process::Command` with this command's program, args,
    /// working dir, and environment — stdio wired for capture. Use it to feed
    /// the low-level [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape
    /// hatch directly (which returns a raw [`tokio::process::Child`]).
    pub fn to_tokio_command(&self) -> tokio::process::Command {
        self.build_tokio()
    }

    /// Build the `tokio` command with stdio wired for capture. Containment
    /// (cgroup/job/process-group) is added by the group's `spawn`.
    pub(crate) fn build_tokio(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.program);
        cmd.args(&self.args);
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }
        // An inherit_env allow-list implies a cleared environment.
        if self.env_clear || self.inherit_env.is_some() {
            cmd.env_clear();
        }
        if let Some(names) = &self.inherit_env {
            // Copy the allow-listed vars from the parent env at spawn time;
            // vars the parent lacks are skipped. Explicit overrides below win.
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
            match &self.groups {
                // Supplementary groups requested: do the *whole* privilege drop
                // (setgroups → setgid → setuid) in one async-signal-safe
                // pre_exec. std applies its own setgid/setuid BEFORE any user
                // pre_exec hook, so a separate setgroups hook would run after the
                // uid already dropped and fail EPERM — setgroups must precede
                // setuid while still privileged. (`CommandExt::groups` is still
                // unstable, so it can't carry this for us.)
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
                // No supplementary groups: keep std's path — it applies gid
                // before uid (once the uid drops, changing gid is no longer
                // permitted), before any user pre_exec hook. Unchanged.
                None => {
                    if let Some(gid) = self.gid {
                        cmd.as_std_mut().gid(gid);
                    }
                    if let Some(uid) = self.uid {
                        cmd.as_std_mut().uid(uid);
                    }
                }
            }
            if self.setsid {
                // Registered before any backend hook (e.g. the Linux cgroup
                // join), so the new session exists first. The pgroup backend
                // skips its setpgid when setsid is requested — std applies
                // setpgid before pre_exec hooks, and setsid fails EPERM for a
                // process that is already a group leader.
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
        if self.creation_flags_extra != 0 {
            use std::os::windows::process::CommandExt;
            // Covers non-group launch paths; the group spawn on Windows
            // overwrites flags with CREATE_SUSPENDED | these extras.
            cmd.as_std_mut().creation_flags(self.creation_flags_extra);
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
            // Interactive: keep a pipe open for the caller to write to.
            cmd.stdin(Stdio::piped());
        } else {
            match &self.stdin {
                Some(src) => {
                    cmd.stdin(src.stdio());
                }
                // No source given: close stdin so the child reads EOF at start.
                None => {
                    cmd.stdin(Stdio::null());
                }
            }
        }
        cmd
    }

    // --- Live handle (private one-shot group) ------------------------------

    /// Start the command and return a live [`RunningProcess`] backed by a fresh
    /// private group. Use this for streaming stdout
    /// ([`RunningProcess::stdout_lines`]) or inspecting the process while it
    /// runs; keep the handle in scope, as dropping it tears the tree down.
    pub async fn start(&self) -> Result<RunningProcess> {
        JobRunner::new().start(self).await
    }

    // --- High-level run helpers (private one-shot group) -------------------

    /// Run to completion and capture stdout as text, stderr, and the exit code.
    /// A non-zero exit is reported, not raised — call
    /// [`ProcessResult::ensure_success`] to turn it into an error.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().start(self).await?.output_string().await
    }

    /// Run to completion and capture stdout as raw bytes (plus stderr/exit code).
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        JobRunner::new().start(self).await?.output_bytes().await
    }

    /// Run to completion and return just the exit code (output is discarded). A
    /// run that yields no code surfaces as an error — a timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), a signal-kill as an IO error —
    /// consistent with
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code) and
    /// [`CliClient::exit_code`](crate::CliClient::exit_code).
    pub async fn exit_code(&self) -> Result<i32> {
        JobRunner::new().exit_code(self).await
    }

    /// Run to completion, requiring a zero exit, and return trimmed stdout.
    pub async fn run(&self) -> Result<String> {
        JobRunner::new().run(self).await
    }

    /// Run a predicate command and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err` (any other code
    /// as [`Error::Exit`], a timeout as [`Error::Timeout`](crate::Error::Timeout),
    /// a signal-kill as an IO error). For tools whose exit code *is* the answer —
    /// `git diff --quiet`, `git show-ref --verify --quiet`, `grep -q`, …
    pub async fn probe(&self) -> Result<bool> {
        JobRunner::new().probe(self).await
    }

    /// Return the first stdout line matching `predicate` (or the first line when
    /// the predicate is trivial), then tear the process down.
    pub async fn first_line<F>(&self, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool,
    {
        use tokio_stream::StreamExt;

        let process = JobRunner::new().start(self).await?;
        let search = async move {
            let mut process = process;
            // Close an untaken `keep_stdin_open` pipe (taking it here drops it →
            // EOF), so a stdin-reading filter isn't left blocking — `first_line`
            // gives no way to write to it. A no-op for the usual case.
            let _ = process.standard_input();
            let mut lines = process.stdout_lines();
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None
        };
        match self.timeout {
            // Bound the search by the configured deadline. On elapse, `search`
            // is dropped — tearing the process tree down — and the timeout is
            // surfaced as `Error::Timeout` (consistent with `run`/`exit_code`),
            // never an indefinite hang on a process that stalls without exiting.
            Some(limit) => match tokio::time::timeout(limit, search).await {
                Ok(found) => Ok(found),
                Err(_elapsed) => Err(Error::Timeout {
                    program: self.program_name(),
                    timeout: limit,
                }),
            },
            None => Ok(search.await),
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Command");
        d.field("program", &self.program)
            .field("args", &self.args)
            .field("cwd", &self.cwd)
            .field("envs", &self.envs)
            .field("env_clear", &self.env_clear)
            .field("stdin", &self.stdin)
            .field("keep_stdin_open", &self.keep_stdin_open)
            .field("unchecked", &self.unchecked)
            .field("timeout", &self.timeout)
            .field("stdout_mode", &self.stdout_mode)
            .field("stderr_mode", &self.stderr_mode)
            .field("has_stdout_handler", &self.stdout_handler.is_some())
            .field("has_stderr_handler", &self.stderr_handler.is_some())
            .field("output_buffer", &self.output_buffer)
            .field("stdout_encoding", &self.stdout_encoding.name())
            .field("stderr_encoding", &self.stderr_encoding.name())
            .field("has_retry", &self.retry.is_some())
            .field("inherit_env", &self.inherit_env)
            .field("uid", &self.uid)
            .field("gid", &self.gid)
            .field("setsid", &self.setsid)
            .field("kill_on_parent_death", &self.kill_on_parent_death)
            .field("creation_flags_extra", &self.creation_flags_extra);
        #[cfg(feature = "cancellation")]
        d.field("has_cancel_token", &self.cancel_token.is_some());
        d.finish()
    }
}

/// Render one argument shell-quoted for **display** (POSIX single-quote rules).
/// Not a security boundary — the crate never invokes a shell; this only makes a
/// `command_line()` echo readable and unambiguous.
#[cfg(unix)]
fn quote_arg(arg: &str) -> String {
    // Bare when entirely shell-safe; otherwise single-quote, rewriting an
    // embedded `'` as the classic `'\''`.
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

/// Render one argument quoted for **display** on Windows (double-quote rules).
/// Not a security boundary — the crate never invokes a shell.
#[cfg(not(unix))]
fn quote_arg(arg: &str) -> String {
    let needs_quote = arg.is_empty()
        || arg
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '"' | '^' | '&' | '|' | '<' | '>' | '%'));
    if !needs_quote {
        return arg.to_owned();
    }
    let mut out = String::with_capacity(arg.len() + 2);
    out.push('"');
    for ch in arg.chars() {
        if ch == '"' {
            out.push_str("\\\"");
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// PATH resolution helpers (used by the pre-spawn check in runner.rs)
// ---------------------------------------------------------------------------

/// Whether `program` is a bare name that should be looked up on `PATH`.
///
/// Returns `true` when `program` has exactly one path component and that
/// component is a `Normal` segment (no `/`, `\`, `.`, or `..` prefix).
/// Absolute paths (`/usr/bin/git`, `C:\git.exe`) and relative paths
/// (`./tool`, `../bin/x`) return `false` — the caller already spelled out
/// the location.
pub(crate) fn is_bare_name(program: &OsStr) -> bool {
    use std::path::{Component, Path};
    let mut comps = Path::new(program).components();
    matches!(comps.next(), Some(Component::Normal(_))) && comps.next().is_none()
}

/// Search `PATH` for an executable named `program` (bare name, no separators).
///
/// Returns `(found, searched)`:
/// - `found` — the resolved absolute path when the program is on `PATH`.
/// - `searched` — the raw `PATH` value (for the error message when not found).
pub(crate) fn find_in_path(program: &OsStr) -> (Option<std::path::PathBuf>, String) {
    let path_var = match std::env::var_os("PATH") {
        Some(p) if !p.is_empty() => p,
        _ => return (None, String::new()),
    };
    let searched = path_var.to_string_lossy().into_owned();
    for dir in std::env::split_paths(&path_var) {
        if let Some(found) = probe_dir(&dir, program) {
            return (Some(found), searched);
        }
    }
    (None, searched)
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
    // Try the name exactly as given first (handles `git.exe` already having an ext).
    let candidate = dir.join(program);
    if candidate.is_file() {
        return Some(candidate);
    }
    // Then try each PATHEXT extension (.EXE, .CMD, .BAT, …).
    let pathext = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
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

#[cfg(test)]
mod tests {
    use super::Command;
    use std::ffi::OsStr;

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
        // The std Command keeps one entry per key, last write winning — so the
        // explicit override (applied after the inherited copy) is what remains.
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

    #[cfg(feature = "cancellation")]
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

    #[cfg(feature = "cancellation")]
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
}
