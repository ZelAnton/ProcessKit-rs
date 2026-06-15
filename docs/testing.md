# Testing your code

[‹ docs index](README.md)

Code that shells out is miserable to test — unless the subprocess is behind a
seam. In `processkit` that seam is one small trait. Only `output_string` is required;
`output_bytes` (raw-byte stdout) and `start` (a live handle for streaming/probes)
are **defaulted**, so a minimal double implements just `output_string`:

```rust,ignore
#[async_trait]
pub trait ProcessRunner: Send + Sync {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>>;
    // Defaulted (route through `start`); override for byte/streaming support:
    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>>;
    async fn start(&self, command: &Command) -> Result<RunningProcess>;
}
```

Production code takes a runner (generically or as `&dyn ProcessRunner`); tests
hand it a double. Four doubles ship with the crate, plus a macro that makes
whole CLI wrappers testable for free.

- [The `ProcessRunner` seam](#the-processrunner-seam)
- [Scripting replies: `ScriptedRunner`](#scripting-replies)
- [Asserting invocations: `RecordingRunner`](#asserting-invocations)
- [Expectation-style: `MockRunner`](#expectation-style-mockrunner)
- [Record/replay cassettes: `RecordReplayRunner`](#recordreplay-cassettes)
- [Wrapping a CLI tool: `CliClient`](#wrapping-a-cli-tool)

## The `ProcessRunner` seam

`JobRunner` is the real implementation (each run in a fresh private group); a
[`ProcessGroup`](process-groups.md) is *also* a runner (runs land in that
shared group); and `impl ProcessRunner for &R` means a **borrowed** runner
works wherever an owned one does — inject `&group` or `&recording` without
giving ownership away.

Every runner — real or double — gets the convenience helpers of
`ProcessRunnerExt` for free: `run` (trimmed stdout, success required),
`run_unit`, `exit_code`, `probe` (exit code as a boolean), `checked`
(success-checked full result), and `parse`/`try_parse` (feed stdout to a
closure; like `first_line`, generic over the closure so unavailable on a
`&dyn ProcessRunner`). [Retry
policies](timeouts-and-cancellation.md#retries) work through the seam too, so
a double exercises your retry handling hermetically.

The seam covers **streaming as well as bulk runs**: `ProcessRunner::start`
returns a live `RunningProcess`, and a `ScriptedRunner`'s `start` hands back a
scripted handle whose canned lines flow through the same pump machinery a real
child uses — `stdout_lines`, `wait_for_line`, and `finish` behave
identically, with no subprocess (see
[Scripted streaming](#scripted-streaming) below). An `output_string`-only custom
runner keeps compiling: `start` is defaulted to `Error::Unsupported`.

```rust,no_run
use processkit::{Command, ProcessRunner, ProcessRunnerExt, Result};

// Production code: generic over the runner.
async fn current_branch(runner: &impl ProcessRunner) -> Result<String> {
    runner
        .run(&Command::new("git").args(["branch", "--show-current"]))
        .await
}
```

## Scripting replies

`ScriptedRunner` returns canned `Reply`s for matched commands — the
work-horse double:

```rust,no_run
use processkit::{Command, ProcessRunnerExt};
use processkit::testing::{Reply, ScriptedRunner};

#[tokio::test]
async fn detects_the_branch() {
    let runner = ScriptedRunner::new()
        // Match by program + argument PREFIX (element-wise; first element is
        // the program name, in registration order):
        .on(["git", "branch", "--show-current"], Reply::ok("main\n"))
        // …or by any predicate over the full Command:
        .when(
            |cmd| cmd.working_dir().is_some(),
            Reply::fail(128, "fatal: not a git repository"),
        )
        // …with an optional catch-all:
        .fallback(Reply::ok(""));

    assert_eq!(current_branch(&runner).await.unwrap(), "main");
}
```

The pieces:

- **`Reply::ok(stdout)`** — exit 0. **`Reply::fail(code, stderr)`** — non-zero
  with stderr. **`Reply::lines(["a", "b"])`** — exit 0 with the lines joined
  (and streamed one by one on a scripted [`start`](#scripted-streaming)).
  **`Reply::timeout()`** — a timed-out run (the checking helpers raise
  `Error::Timeout` from it, carrying the command's own configured deadline). On a
  scripted [`start`](#scripted-streaming) it resolves *immediately* as timed-out;
  to exercise a real deadline race, use `Reply::pending()` + a `Command::timeout`.
  **`.with_stdout(text)`** — attach stdout to any of them (e.g. the
  `CONFLICT …` text git prints on a failing merge).
  **`.with_line_delay(d)`** — pace a scripted stream's lines.
- **`Reply::pending()`** — parks the call until the
  command's cancellation token (per-command `cancel_on` or the client-level
  [`default_cancel_on`](timeouts-and-cancellation.md#client-level-default))
  fires, then resolves with `Error::Cancelled` — so a test can prove an
  orchestration *actually cancels* a blocked call, not just that it formats a
  canned error. With no token it parks forever, like a hung child.
- Rules are tried in **registration order**; first match wins. Prefix
  matching is element-wise over the **program name then the arguments** (the
  first element is the program) — `on(["git", "foo"])` matches `git foo bar`
  but not `git foobar` (and not `rm foo`). Use
  [`on_sequence`](https://docs.rs/processkit) to serve an ordered sequence of
  replies (each once, then the last repeats) for a fail-then-succeed scenario.
- **No match and no fallback is a loud error** (`Error::Spawn`, not-found) —
  an unexpected invocation can't slip through a test silently.
- Bulk runs also **replay the canned lines through the command's
  `on_stdout_line`/`on_stderr_line` handlers**, so a wrapper's
  progress-reporting path is exercised without a subprocess.

## Scripted streaming

`ScriptedRunner::start` returns a live `RunningProcess` backed by the canned
reply instead of an OS child. The canned stdout/stderr feed the **same pump
machinery** a real child uses, so the whole streaming surface works
hermetically — `stdout_lines` yields the lines, `wait_for_line` probes them,
`finish` reports the canned outcome and stderr:

```rust,no_run
use processkit::{Command, Outcome, ProcessRunner, StreamExt, Finished};
use processkit::testing::{Reply, ScriptedRunner};
use std::time::Duration;

#[tokio::test]
async fn server_becomes_ready() {
    let runner = ScriptedRunner::new()
        .on(["server", "serve"], Reply::lines(["booting", "listening on 8080"]));

    let mut run = runner.start(&Command::new("server").arg("serve")).await.unwrap();
    run.wait_for_line(|l| l.contains("listening"), Duration::from_secs(5))
        .await
        .unwrap(); // satisfied by the canned banner — no subprocess

    let Finished { outcome, .. } = run.finish().await.unwrap();
    assert_eq!(outcome, Outcome::Exited(0));
}
```

`Reply::lines([...])` scripts the stdout lines; `.with_line_delay(d)` paces
them (deterministic under `#[tokio::test(start_paused = true)]`), and the
scripted run "exits" after the last line. The honest boundaries: a scripted
handle has no OS identity (`pid()` is `None`, `profile` reports empty
samples), does not compose into a real `Pipeline`, and does not model
interactive stdin. `Reply::pending()` scripts a run that never exits on its
own — cancel or time it out through the command's own knobs. A command
`timeout` *does* bound a scripted stream (it ends at the deadline and reports
`Outcome::TimedOut`, like a real child), but a scripted handle has no signal
tier, so — like on Windows — it ignores `timeout_grace` and ends at once.

## Asserting invocations

`RecordingRunner` wraps another runner and records every `Invocation` — what
was *asked* — so a test asserts inputs, not just outputs:

```rust,no_run
use processkit::{Command, ProcessRunnerExt};
use processkit::testing::{RecordingRunner, Reply, ScriptedRunner};

#[tokio::test]
async fn passes_the_right_flags() {
    let runner = RecordingRunner::new(
        ScriptedRunner::new().fallback(Reply::ok("done")),
    );

    runner
        .run(&Command::new("gh").args(["pr", "create", "--draft"]).current_dir("/repo"))
        .await
        .unwrap();

    let call = runner.only_call(); // panics unless exactly one call
    assert_eq!(call.args_str(), ["pr", "create", "--draft"]);
    assert!(call.has_flag("--draft"));
    assert_eq!(call.cwd.as_deref().map(|c| c.to_str().unwrap()), Some("/repo"));
    assert!(!call.has_stdin);
}
```

An `Invocation` captures the *routing* knobs — `program`, `args`, `cwd`,
`envs` (explicit overrides, `None` = removal), `has_stdin` — not the
I/O-shaping ones (timeout, encodings, buffer policy); assert those through a
`when` predicate over the `Command` itself. `calls()` returns the full list
when more than one run is expected.

## Expectation-style: `MockRunner`

With the **`mock`** feature, `mockall` generates a `MockRunner` for
expectation-style tests (call counts, argument matchers, ordered
expectations) — the right tool when the *interaction* is the contract.

> **Note:** `MockRunner`'s `expect_*` surface is generated by `mockall` and is
> **exempt from this crate's semver guarantees** — it tracks the `mockall`
> dependency, not a frozen API. For a stable double, prefer `ScriptedRunner`
> (canned replies) or `RecordingRunner` (input assertions) above.

```rust,ignore
use processkit::testing::MockRunner;

let mut mock = MockRunner::new();
mock.expect_output_string()
    .times(1)
    .returning(|_cmd| /* build a Result<ProcessResult<String>> */ …);
```

> **`MockRunner` does not inherit the defaults.** Unlike a hand-written runner
> (where `output_bytes`/`start` are defaulted), `mockall::automock` replaces
> **every** method with an expectation — so a verb that routes through `start` or
> `output_bytes` needs its own `expect_start()` / `expect_output_bytes()`, or the
> unset call panics ("no expectation"). `ScriptedRunner` provides the defaults and
> the streaming seam out of the box.

For most tests `ScriptedRunner`/`RecordingRunner` read better; reach for the
mock when you need `mockall`'s matching machinery.

## Record/replay cassettes

With the **`record`** feature, `RecordReplayRunner` closes the loop: record
real runs to a JSON *cassette* once, then replay them deterministically —
fast, hermetic, byte-stable, no subprocess in CI:

```rust,no_run
use processkit::{Command, JobRunner, ProcessRunnerExt};
use processkit::testing::RecordReplayRunner;

// Record once against the real tool (an opt-in `--record` test run, say):
let runner = RecordReplayRunner::record("fixtures/git.json", JobRunner::new());
let version = runner.run(&Command::new("git").arg("--version")).await?;
runner.save()?;                                  // the error-surfacing flush
                                                 // (best-effort on drop too)

// Replay everywhere else:
let runner = RecordReplayRunner::replay("fixtures/git.json")?;
assert_eq!(runner.run(&Command::new("git").arg("--version")).await?, version);
```

Semantics worth knowing before you commit a cassette:

| Aspect | Behavior |
|---|---|
| Match key | program + args + cwd + a stdin **source digest** (hashed, never persisted: in-memory bytes hash their content, a `from_file` source hashes its path) — no stdin (absent or `Stdin::empty()`) keys distinctly; lossy UTF-8 on the text parts |
| Environment | **values never reach the file** — only sorted variable names, so *env* secrets can't leak through a committed fixture; env is *not* matched, so env differences can't cause spurious misses |
| Duplicates of one key | replay in capture order, then the **last entry repeats** — a recorded sequence (`git rev-parse HEAD` before/after a commit) replays faithfully, while retry/probe loops keep getting a stable final answer |
| Miss | strict `Error::CassetteMiss` (distinct from a missing program — `is_not_found()` is `false`) — replay never spawns a surprise subprocess; a stale cassette fails loudly |
| Timeouts | a recorded timed-out run replays as one, surfacing `Error::Timeout` with the *replaying* command's deadline |
| Format | pretty-printed JSON with a `version` field; unknown versions / corrupt files / an entry with a contradictory outcome / a file over 64 MiB are `Error::Io(InvalidData)`, a missing file keeps `NotFound` |
| Err results | not recorded — only completed runs (non-zero exits and captured timeouts *are* results and are recorded) |

Only env **values** are redacted. `program`, `args`, `cwd`, `stdout`, and
`stderr` are stored **verbatim** and can carry secrets (a `--password=…` flag, a
token echoed to output), so review a fixture before committing it. On Unix the
file is written `0600` and the write **refuses to follow a symlink** at the
cassette path (`O_NOFOLLOW`, so a planted link can't redirect the secret-bearing
write — it fails loud instead). On Windows the file inherits the containing
directory's ACL, so restrict that directory (or use a per-user temp dir, not a
world-writable shared one) for secret-bearing fixtures.

A neat trick: in tests, record against a `ScriptedRunner` instead of
`JobRunner` — the whole record→save→replay round trip is then itself
hermetic.

## Wrapping a CLI tool

`CliClient` is the foundation for typed wrappers around external tools
(`git`, `jj`, `gh`, `kubectl`, …): it owns the program name, per-client
defaults, and the runner; your wrapper contributes only commands and parsers.
The `cli_client!` macro generates the boilerplate:

```rust,no_run
use processkit::{cli_client, Error, ProcessRunner, Result};
use std::path::Path;
use std::time::Duration;

cli_client!(
    /// A typed `git` client.
    pub struct Git => "git"
);

impl<R: ProcessRunner> Git<R> {
    /// HEAD's commit id.
    pub async fn head(&self, repo: &Path) -> Result<String> {
        self.core.run(self.core.command_in(repo, ["rev-parse", "HEAD"])).await
    }

    /// Is the work tree clean? (exit code IS the answer)
    pub async fn is_clean(&self, repo: &Path) -> Result<bool> {
        self.core.probe(self.core.command_in(repo, ["diff", "--quiet"])).await
    }

    /// Branch list, parsed — the parser is fallible and returns the crate's
    /// `Result`, typically an `Error::Parse` naming the program.
    pub async fn branches(&self, repo: &Path) -> Result<Vec<String>> {
        self.core
            .try_parse(
                self.core.command_in(repo, ["branch", "--format=%(refname:short)"]),
                |out| {
                    let list: Vec<String> = out.lines().map(str::to_owned).collect();
                    if list.is_empty() {
                        Err(Error::Parse {
                            program: "git".into(),
                            message: "no branches".into(),
                        })
                    } else {
                        Ok(list)
                    }
                },
            )
            .await
    }
}

// Production: the real runner, with per-client defaults.
let git = Git::new().default_timeout(Duration::from_secs(30));
let head = git.head(Path::new(".")).await?;
```

The generated type is `Git<R: ProcessRunner = JobRunner>` with `Git::new()`,
`Git::with_runner(runner)`, `default_timeout` / `default_env` /
`default_env_remove` builders, and a public `core: CliClient<R>` whose helpers
speak the crate-wide verb vocabulary: `run` (trimmed stdout), `output_string` (full
result), `run_unit` (success only), `exit_code`, `probe`, plus `parse`
(infallible) and `try_parse` (fallible → `Error::Parse`).

And the payoff — the wrapper tests hermetically with any double:

```rust,no_run
#[tokio::test]
async fn head_is_trimmed() {
    let git = Git::with_runner(
        ScriptedRunner::new().on(["git", "rev-parse", "HEAD"], Reply::ok("abc123\n")),
    );
    assert_eq!(git.head(Path::new("/repo")).await.unwrap(), "abc123");
}
```

…or with a [cassette](#recordreplay-cassettes) recorded against the real tool
once.

---

Next: [Platform support](platform-support.md) ·
[Supervision](supervision.md) ·
[Running commands](commands.md)
