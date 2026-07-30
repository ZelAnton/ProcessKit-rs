# Testing your code

[‹ docs index](README.md)

Code that shells out is miserable to test — unless the subprocess is behind a
seam. In `processkit` that seam is one small trait. Only `output_string` is required;
`output_bytes` (raw-byte stdout) and `start` (a live handle for streaming/probes)
are **defaulted**, so a minimal double implements just `output_string`:

```text
#[async_trait]
pub trait ProcessRunner: Send + Sync {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>>;
    // Defaulted (route through `start`); override for byte/streaming support:
    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>>;
    async fn start(&self, command: &Command) -> Result<RunningProcess>;
}
```

Production code takes a runner (generically or as `&dyn ProcessRunner`); tests
hand it a double. Five doubles ship with the crate, plus a macro that makes
whole CLI wrappers testable for free.

- [The `ProcessRunner` seam](#the-processrunner-seam)
- [Scripting replies: `ScriptedRunner`](#scripting-replies)
- [Asserting invocations: `RecordingRunner`](#asserting-invocations)
- [Echoing without spawning: `DryRunRunner`](#echoing-without-spawning-dryrunrunner)
- [Expectation-style: `MockRunner`](#expectation-style-mockrunner)
- [Record/replay cassettes: `RecordReplayRunner`](#recordreplay-cassettes)
- [Wrapping a CLI tool: `CliClient`](#wrapping-a-cli-tool)
- [How the crate tests itself: the stress tier](#how-the-crate-tests-itself-the-stress-tier)

## The `ProcessRunner` seam

`JobRunner` is the real implementation (each run in a fresh private group); a
[`ProcessGroup`](process-groups.md) is *also* a runner (runs land in that
shared group); and `impl ProcessRunner for &R` means a **borrowed** runner
works wherever an owned one does — inject `&group` or `&recording` without
giving ownership away.

Every runner — real or double — gets the convenience helpers of
`ProcessRunnerExt` for free: `run` (trimmed stdout, success required),
`run_unit`, `exit_code`, `probe` (exit code as a boolean), `checked`
(success-checked full result), `parse`/`try_parse` (feed stdout to a closure),
and — with `json` — `output_json` (typed deserialization). These are all callable
on a `&dyn ProcessRunner`; being generic over the closure or output type,
`parse`/`try_parse`/`output_json`/`first_line` simply can't be dispatched through
a `dyn ProcessRunnerExt` **object** (the ext trait isn't object-safe). [Retry
policies](timeouts-and-cancellation.md#retries) work through the seam too, so
a double exercises your retry handling hermetically.

The seam covers **streaming as well as bulk runs**: `ProcessRunner::start`
returns a live `RunningProcess`, and a `ScriptedRunner`'s `start` hands back a
scripted handle whose canned lines flow through the same pump machinery a real
child uses — `stdout_lines`, `wait_for_line`, and `finish` behave
identically, with no subprocess (see
[Scripted streaming](#scripted-streaming) below). An `output_string`-only custom
runner keeps compiling: `start` is defaulted to `ErrorReason::Unsupported`.

```rust,no_run
use processkit::{Command, ProcessRunner, ProcessRunnerExt, Result};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Production code: generic over the runner.
    async fn current_branch(runner: &impl ProcessRunner) -> Result<String> {
        runner
            .run(&Command::new("git").args(["branch", "--show-current"]))
            .await
    }
    Ok(())
}
```

## Scripting replies

`ScriptedRunner` returns canned `Reply`s for matched commands — the
work-horse double:

```rust,no_run
use processkit::{Command, ProcessRunnerExt};
use processkit::testing::{Reply, ScriptedRunner};

#[tokio::main]
async fn main() -> processkit::Result<()> {
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
    Ok(())
}
```

The pieces:

- **`Reply::ok(stdout)`** — exit 0. **`Reply::fail(code, stderr)`** — non-zero
  with stderr. **`Reply::lines(["a", "b"])`** — exit 0 with the lines joined
  (and streamed one by one on a scripted [`start`](#scripted-streaming)).
  **`Reply::timeout()`** — a timed-out run (the checking helpers raise
  `ErrorReason::Timeout` from it, carrying the command's own configured deadline). On a
  scripted [`start`](#scripted-streaming) it resolves *immediately* as timed-out;
  to exercise a real deadline race, use `Reply::pending()` + a `Command::timeout`.
  **`.with_stdout(text)`** — attach stdout to any of them (e.g. the
  `CONFLICT …` text git prints on a failing merge).
  **`.with_line_delay(d)`** — pace a scripted stream's lines.
- **`Reply::pending()`** — parks the call until the
  command's cancellation token (per-command `cancel_on` or the client-level
  [`default_cancel_on`](timeouts-and-cancellation.md#client-level-default))
  fires, resolving with `ErrorReason::Cancelled` — so a test can prove an
  orchestration *actually cancels* a blocked call, not just that it formats a
  canned error — or until the command's `timeout` deadline elapses, resolving
  timed-out (`Outcome::TimedOut`) on the bulk verbs and `start` alike, like a
  child killed for overrunning its deadline. Whichever fires first wins. With
  neither a token nor a timeout it parks forever, like a hung child.
- Rules are tried in **registration order**; first match wins. Prefix
  matching is element-wise over the **program name then the arguments** (the
  first element is the program) — `on(["git", "foo"])` matches `git foo bar`
  but not `git foobar` (and not `rm foo`). Use
  [`on_sequence`](https://docs.rs/processkit) to serve an ordered sequence of
  replies (each once, then the last repeats) for a fail-then-succeed scenario.
- **`when` has a fallible twin, `try_when`** — the predicate returns
  `Result<bool, E>` (any `E: Into<Box<dyn Error + Send + Sync>>`). `Ok(true)` /
  `Ok(false)` match exactly as `when`'s `true` / `false`, but an `Err` **aborts
  the run**: the verb fails with an `ErrorReason::Predicate` (kind
  `ErrorKind::Predicate`) carrying the predicate's own error verbatim, and no
  later rule is consulted. The `ScriptedRunner` counterpart of the `Supervisor`
  [`try_*` predicate twins](supervision.md#fallible-control-predicates) — for
  exercising a wrapper whose match callback can throw.
- **No match and no fallback is a loud error** (`ErrorReason::Spawn`, not-found) —
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
use processkit::prelude::StreamExt;
use processkit::testing::{Reply, ScriptedRunner};
use processkit::{Command, Finished, Outcome, ProcessRunner};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
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
    Ok(())
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

#[tokio::main]
async fn main() -> processkit::Result<()> {
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
    Ok(())
}
```

An `Invocation` captures the *routing* knobs — `program`, `args`, `cwd`,
`envs` (explicit overrides, `None` = removal), `has_stdin` — not the
I/O-shaping ones (timeout, encodings, buffer policy); assert those through a
`when` predicate over the `Command` itself. `calls()` returns the full list
when more than one run is expected.

## Echoing without spawning: `DryRunRunner`

`DryRunRunner` never spawns a process at all: it renders each command through
[`Command::command_line`](https://docs.rs/processkit) — the crate's own
display quoting, not a hand-rolled shell-escaper — and hands back a synthetic
successful result. It's the seam behind a tool's own `--dry-run`/`--echo`
mode: wire your production code to it (instead of `JobRunner`) and it shows
what *would* run instead of running it.

```rust,no_run
use processkit::{Command, ProcessRunner};
use processkit::testing::DryRunRunner;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    #[tokio::test]
    async fn dry_run_shows_the_command_without_running_it() {
        let runner = DryRunRunner::new();
        let out = runner
            .output_string(&Command::new("rm").args(["-rf", "build"]))
            .await
            .unwrap();
        assert!(out.is_success()); // synthetic — no process ever ran
        assert_eq!(runner.only_command(), "rm -rf build");
    }
    Ok(())
}
```

Unlike `ScriptedRunner`, there is nothing to script — a dry run has no real
output to fake, only a command line to show — so every call unconditionally
succeeds on both `output_string` and `start`: empty stdout/stderr, and an
exit code drawn from the command's own `ok_codes` (`0` by default) so
`is_success()` and the ergonomic `run`/`run_unit`/`checked`/`parse` verbs
agree it succeeded even for a command whose `ok_codes` excludes `0`. The
rendered lines are available two ways, usable together or alone:

- a collected snapshot, in the style of `RecordingRunner::calls` —
  `commands()` (all of them, in order) / `only_command()` (panics unless
  exactly one call was made);
- a live `on_invocation(|line| …)` callback, invoked with the rendered line as
  each call happens — e.g. printing it to the terminal immediately, in
  addition to (not instead of) the collected snapshot:

```rust,no_run
use processkit::testing::DryRunRunner;

let runner = DryRunRunner::new().on_invocation(|line| println!("+ {line}"));
```

## Expectation-style: `MockRunner`

With the **`mock`** feature, `mockall` generates a `MockRunner` for
expectation-style tests (call counts, argument matchers, ordered
expectations) — the right tool when the *interaction* is the contract.

> **Note:** `MockRunner`'s `expect_*` surface is generated by `mockall` and is
> **exempt from this crate's semver guarantees** — it tracks the `mockall`
> dependency, not a frozen API. For a stable double, prefer `ScriptedRunner`
> (canned replies) or `RecordingRunner` (input assertions) above.

```rust,no_run
use processkit::testing::MockRunner;

let mut mock = MockRunner::new();
mock.expect_output_string()
    .times(1)
    .returning(|_cmd| todo!("build a Result<ProcessResult<String>>"));
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

The runnable
[`examples/record_replay.rs`](https://github.com/ZelAnton/ProcessKit-rs/blob/main/examples/record_replay.rs)
does the complete round trip with a self-exec child and a temporary cassette,
including a scrub hook and a guard proving replay never spawns:

```text
cargo run --example record_replay --features record
```

```rust,no_run
use processkit::{Command, JobRunner, ProcessRunnerExt};
use processkit::testing::RecordReplayRunner;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Record once against the real tool (an opt-in `--record` test run, say):
    let runner = RecordReplayRunner::record("fixtures/git.json", JobRunner::new());
    let version = runner.run(&Command::new("git").arg("--version")).await?;
    runner.save()?;                                  // the error-surfacing flush
                                                     // (best-effort on drop too)

    // Replay everywhere else:
    let runner = RecordReplayRunner::replay("fixtures/git.json")?;
    assert_eq!(runner.run(&Command::new("git").arg("--version")).await?, version);
    Ok(())
}
```

Semantics worth knowing before you commit a cassette:

| Aspect | Behavior |
|---|---|
| Match key | program + args + a stdin **source digest** (hashed, never persisted: in-memory bytes hash their content, a `from_file` source hashes its path) — no stdin (absent or `Stdin::empty()`) keys distinctly; lossy UTF-8 on the text parts. **`cwd` is not part of the key by default** — a cassette recorded from one absolute working directory still replays when the same invocation runs from another (a dev box vs. a CI workspace); `cwd` is still stored on the entry for visibility. Opt in to a stricter key with `match_on_cwd` / `match_on_env` (below). A `scrub_with` hook transforms args symmetrically on record and replay (and cwd too when matched), so redacted keys still hit |
| Environment | **values never reach the file** — only sorted variable names, so *env* secrets can't leak through a committed fixture. Env is **not matched by default**, so irrelevant env differences can't cause spurious misses. Opt in with `match_on_env(["NAME", …])` to also key on selected variables' *values* — still via a **digest**, so raw values remain off-disk (see [Opt-in stricter matching](#opt-in-stricter-matching-cwd--selected-env-values)) |
| Duplicates of one key | replay in capture order, then the **last entry repeats** — a recorded sequence (`git rev-parse HEAD` before/after a commit) replays faithfully, while retry/probe loops keep getting a stable final answer |
| Miss | strict `ErrorReason::CassetteMiss` (distinct from a missing program — `is_not_found()` is `false`) — replay never spawns a surprise subprocess; a stale cassette fails loudly |
| Timeouts | a recorded timed-out run replays as one, surfacing `ErrorReason::Timeout` with the *replaying* command's deadline |
| Format | pretty-printed JSON with a `version` field; unknown versions / corrupt files / an entry with a contradictory outcome / a file over 64 MiB are `ErrorReason::Io(InvalidData)`, a missing file keeps `NotFound` |
| Err results | recorded and replayed faithfully (`ErrorReason::Spawn`/`NotFound`/`Stdin`/`OutputTooLarge`/`Unsupported`/`Io` — with its `ErrorKind` preserved by name — plus an `Other` fallback); replaying such an entry surfaces the reconstructed error instead of `ErrorReason::CassetteMiss`. `ErrorReason::Cancelled` is the one exception — never recorded, since replay short-circuits on the replaying command's own token first |
| Verbs (`output_string` + `start`) | a cassette is **verb-agnostic**: record through either and replay through either. Replaying `start` hands back a scripted `RunningProcess` whose recorded lines flow through the command's real pumps (`stdout_lines` / `wait_for_line` / `finish`), no subprocess. *Recording* a `start` captures the run whole (the child runs to completion before the handle returns), so an **interactive** run fed stdin mid-stream can't be recorded that way — bound it with `Command::timeout` or script it with `ScriptedRunner` |
| `output_bytes` | **unsupported** (`ErrorReason::Unsupported`) in both modes — a lossy-UTF-8 text fixture can't reproduce exact raw bytes; capture bytes from a real or scripted runner |

By default only env **values** are excluded. `program`, `args`, `cwd`, `stdout`,
and `stderr` are stored **verbatim** and can carry secrets (a `--password=…`
flag, a token echoed to output). For a fixture that can contain one, opt into a
field-aware scrub hook:

```rust,no_run
use processkit::{JobRunner, ProcessRunnerExt};
use processkit::testing::{CassetteField, RecordReplayRunner};

let scrub = |field: CassetteField, text: &str| match field {
    CassetteField::Argument | CassetteField::Stdout | CassetteField::Stderr =>
        text.replace("s3cret", "<redacted>"),
    CassetteField::Cwd => text.replace("/home/alice", "<workspace>"),
    _ => text.to_owned(),
};

let recorder = RecordReplayRunner::record("fixtures/tool.json", JobRunner::new())
    .scrub_with(scrub);
// run commands, then recorder.save()?;

// Apply the same deterministic hook before replay lookup: a live secret argv
// is transformed to the same redacted key stored in the fixture.
let replayer = RecordReplayRunner::replay("fixtures/tool.json")?
    .scrub_with(scrub);
# let _ = (recorder, replayer);
# Ok::<(), processkit::Error>(())
```

The hook changes only the persisted entry. Record-mode callers still receive
the real unsanitized stdout/stderr; replay naturally returns the scrubbed text
that exists in the fixture. Use the same deterministic hook on both runners.
Still review a fixture before committing it. On Unix the
file is written `0600` and the write **refuses to follow a symlink** at the
cassette path (`O_NOFOLLOW`, so a planted link can't redirect the secret-bearing
write — it fails loud instead). On Windows the file inherits the containing
directory's ACL, so restrict that directory (or use a per-user temp dir, not a
world-writable shared one) for secret-bearing fixtures.

A neat trick: in tests, record against a `ScriptedRunner` instead of
`JobRunner` — the whole record→save→replay round trip is then itself
hermetic.

### Opt-in stricter matching (cwd / selected env values)

The portable default keys on `program` + `args` + stdin only. It deliberately
leaves `cwd` and the environment **out** of the key: a cassette recorded in one
absolute working directory (a dev box, a tempdir) then replays cleanly from a
different one (a CI workspace), and an env variable that differs between the
record and replay machines but doesn't change the tool's output can't cause a
spurious miss. That portability is the right default for most tools.

But some tools' output genuinely depends on where they run or on a specific
environment variable — and with those out of the key, two such invocations
collide on one entry: the first recording silently answers for both on replay.
When that matters, opt in to a stricter key:

```rust,no_run
use processkit::{Command, JobRunner, ProcessRunnerExt};
use processkit::testing::RecordReplayRunner;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Record: also key on the working directory and on LC_ALL's value.
    let runner = RecordReplayRunner::record("fixtures/tool.json", JobRunner::new())
        .match_on_cwd()
        .match_on_env(["LC_ALL"]);
    let cmd = Command::new("tool").current_dir("/repo").env("LC_ALL", "C");
    let _ = runner.run(&cmd).await?;
    runner.save()?;

    // Replay: set the *same* policy — it must match on both sides.
    let runner = RecordReplayRunner::replay("fixtures/tool.json")?
        .match_on_cwd()
        .match_on_env(["LC_ALL"]);
    // A differing cwd or LC_ALL value now MISSES (a loud `CassetteMiss`) instead
    // of replaying the wrong entry; the same cwd + LC_ALL hits.
    let cmd = Command::new("tool").current_dir("/repo").env("LC_ALL", "C");
    let _ = runner.run(&cmd).await?;
    Ok(())
}
```

Notes:

- **Env values still never reach the file.** `match_on_env` keys on an FNV
  *digest* of the selected `(name, value)` pairs, not the raw values — the
  cassette continues to store variable *names* only, so no env secret is written
  to disk even under the stricter policy. (`cwd` is keyed the same way; it is
  also already stored verbatim on the entry, as before.)
- **Symmetric by contract.** Set the *same* policy on the record and replay
  runner, exactly as you target the same tool. A mismatched policy simply
  *misses* (it never serves a wrong entry) — including replaying a policy-keyed
  cassette with no policy, or vice versa.
- **Only the named variables participate.** Variables you don't name stay out of
  the key, preserving portability for env differences that don't matter. A named
  variable that is *set*, *removed* (`env_remove`), or *untouched* are three
  distinct keys.
- **On-disk format.** A policy-keyed entry carries an extra opaque
  `match_digest` number; a cassette recorded without a policy omits it and is
  byte-identical to before but for the bumped `version` (now `4`). Older
  cassettes (no `match_digest`) load and replay unchanged under a no-policy
  replayer.

## Wrapping a CLI tool

`CliClient` is the foundation for typed wrappers around external tools
(`git`, `jj`, `gh`, `kubectl`, …): it owns the program name, per-client
defaults, and the runner; your wrapper contributes only commands and parsers.
The `cli_client!` macro generates the boilerplate:

For the full design guide — default precedence, dynamic env resolution,
retry/error policy, typed JSON, and worked wrappers — see
[Building typed CLI clients](cli-clients.md). This section focuses on the test
seam.

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
    /// `Result`, typically an `ErrorReason::Parse` naming the program.
    pub async fn branches(&self, repo: &Path) -> Result<Vec<String>> {
        self.core
            .try_parse(
                self.core.command_in(repo, ["branch", "--format=%(refname:short)"]),
                |out| {
                    let list: Vec<String> = out.lines().map(str::to_owned).collect();
                    if list.is_empty() {
                        Err(Error::parse("git", "no branches"))
                    } else {
                        Ok(list)
                    }
                },
            )
            .await
    }
}

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Production: the real runner, with per-client defaults.
    let git = Git::new().default_timeout(Duration::from_secs(30));
    let head = git.head(Path::new(".")).await?;
    Ok(())
}
```

The generated type is `Git<R: ProcessRunner = JobRunner>` with `Git::new()`,
`Git::with_runner(runner)`, `default_timeout` / `default_env` /
`default_env_remove` builders, and a **module-private** `core: CliClient<R>` (reach
it as `self.core` from the wrapper's own methods) whose helpers
speak the crate-wide verb vocabulary: `run` (trimmed stdout), `output_string` (full
result), `run_unit` (success only), `exit_code`, `probe`, plus `parse`
(infallible) and `try_parse` (fallible → `ErrorReason::Parse`).

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

## How the crate tests itself: the stress tier

The doubles above are for testing *your* code. The crate's own suite has an
extra, non-functional layer you'll only meet if you hack on `processkit`
itself: an opt-in **stress tier** (`tests/stress/`) that exercises behaviour at
scale and under multiplicity — spawn bursts, cancel storms, mass teardown, and
buffer floods. It is gated behind the `PROCESSKIT_STRESS` env var (so the normal
PR matrix compiles it for free but pays nothing) and runs on its own scheduled /
on-demand CI leg (`.github/workflows/stress.yml`), never in the fast PR run.

Its most searching scenario is the **seeded randomized-interleaving harness**
(`tests/stress/interleave.rs`). A seed deterministically generates a *random
combination* of the public lifecycle operations — `wait` / `start_kill` /
`take_stdin` / the streaming verbs / group `signal` / `suspend` / `shutdown` —
over a shared [`ProcessGroup`](process-groups.md) and children of three shapes
(short-lived, long-lived, stdout-flooding), then runs them concurrently so real
thread scheduling explores the orderings. After every combination it checks the
invariants that must hold under *any* interleaving:

- no operation panics or returns an impossible `Error` (only the documented,
  operational variants);
- no live descendants or zombies survive (reusing the tier's reap helpers);
- no background pump/drain/supervision task faulted unnoticed;
- every descriptor and group is released (Job handle closed, cgroup directory
  removed).

The seed fixes the *plan*, not the scheduling: a re-run with the same seed
replays the identical sequence of operations, so a failure is reproducible. The
offending seed is printed on failure — re-run just that plan in a loop with
`PROCESSKIT_STRESS_SEED=<seed>` while you debug. Run the tier locally with:

```text
PROCESSKIT_STRESS=1 cargo test --all-features --test stress -- --test-threads=1
```

---

Next: [Platform support](platform-support.md) ·
[Supervision](supervision.md) ·
[Running commands](commands.md)
