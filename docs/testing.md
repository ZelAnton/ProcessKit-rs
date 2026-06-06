# Testing your code

[‹ docs index](README.md)

Code that shells out is miserable to test — unless the subprocess is behind a
seam. In `processkit` that seam is one trait with one method:

```rust,ignore
#[async_trait]
pub trait ProcessRunner: Send + Sync {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>>;
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
`exit_code`, `probe` (exit code as a boolean), `checked` (success-checked full
result). [Retry policies](timeouts-and-cancellation.md#retries) work through
the seam too, so a double exercises your retry handling hermetically.

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
use processkit::{Command, ProcessRunnerExt, Reply, ScriptedRunner};

#[tokio::test]
async fn detects_the_branch() {
    let runner = ScriptedRunner::new()
        // Match by argument PREFIX (element-wise, in registration order):
        .on(["branch", "--show-current"], Reply::ok("main\n"))
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
  with stderr. **`Reply::timeout()`** — a timed-out run (the checking helpers
  raise `Error::Timeout` from it, carrying the command's own configured
  deadline). **`.with_stdout(text)`** — attach stdout to any of them (e.g.
  the `CONFLICT …` text git prints on a failing merge).
- Rules are tried in **registration order**; first match wins. Prefix
  matching is element-wise — `on(["foo"])` matches args `["foo", "bar"]` but
  not `["foobar"]`.
- **No match and no fallback is a loud error** (`Error::Spawn`, not-found) —
  an unexpected invocation can't slip through a test silently.

## Asserting invocations

`RecordingRunner` wraps another runner and records every `Invocation` — what
was *asked* — so a test asserts inputs, not just outputs:

```rust,no_run
use processkit::{Command, ProcessRunnerExt, RecordingRunner, Reply, ScriptedRunner};

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
expectations) — the right tool when the *interaction* is the contract:

```rust,ignore
use processkit::MockRunner;

let mut mock = MockRunner::new();
mock.expect_output()
    .times(1)
    .returning(|_cmd| /* build a Result<ProcessResult<String>> */ …);
```

For most tests `ScriptedRunner`/`RecordingRunner` read better; reach for the
mock when you need `mockall`'s matching machinery.

## Record/replay cassettes

With the **`record`** feature, `RecordReplayRunner` closes the loop: record
real runs to a JSON *cassette* once, then replay them deterministically —
fast, hermetic, byte-stable, no subprocess in CI:

```rust,no_run
use processkit::{Command, JobRunner, ProcessRunnerExt, RecordReplayRunner};

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
| Match key | program + args + cwd + has-stdin (lossy UTF-8 on both sides) |
| Environment | **values never reach the file** — only sorted variable names (a committed fixture can't leak secrets); env is *not* matched, so env differences can't cause spurious misses |
| Duplicates of one key | replay in capture order, then the **last entry repeats** — a recorded sequence (`git rev-parse HEAD` before/after a commit) replays faithfully, while retry/probe loops keep getting a stable final answer |
| Miss | strict `Error::Spawn` (not-found) — replay never spawns a surprise subprocess; a stale cassette fails loudly |
| Timeouts | a recorded timed-out run replays as one, surfacing `Error::Timeout` with the *replaying* command's deadline |
| Format | pretty-printed JSON with a `version` field; unknown versions / corrupt files are `Error::Io(InvalidData)`, a missing file keeps `NotFound` |
| Err results | not recorded — only completed runs (non-zero exits and captured timeouts *are* results and are recorded) |

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
        self.core.text(self.core.command_in(repo, ["rev-parse", "HEAD"])).await
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
cover the spectrum: `text` (trimmed stdout), `capture` (full result), `unit`
(success only), `code`, `probe`, `parse` (infallible), `try_parse` (fallible →
`Error::Parse`).

And the payoff — the wrapper tests hermetically with any double:

```rust,no_run
#[tokio::test]
async fn head_is_trimmed() {
    let git = Git::with_runner(
        ScriptedRunner::new().on(["rev-parse", "HEAD"], Reply::ok("abc123\n")),
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
