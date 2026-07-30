# Building typed CLI clients

`CliClient` is the reusable middle layer between a raw [`Command`](commands.md)
and a domain-specific wrapper such as `Git`, `Jj`, or `Gh`. It owns the program,
an injectable `ProcessRunner`, and defaults shared by every invocation. Your
wrapper owns the public vocabulary and parsing rules.

Use `CliClient` when several operations target the same executable, share
timeout/environment/retry policy, or need hermetic tests. For one or two direct
calls, `Command` is simpler and loses no capability.

## Scaffold a wrapper

`cli_client!` creates a generic wrapper whose production runner is `JobRunner`
and whose tests can inject any `ProcessRunner`. The generated `core` field is
module-private: implement the tool-specific methods beside the macro invocation.

```rust,no_run
use processkit::{cli_client, Error, ProcessRunner, Result};
use std::path::Path;

cli_client!(
    /// A small typed wrapper around Git.
    pub struct Git => "git"
);

impl<R: ProcessRunner> Git<R> {
    pub async fn head(&self, repo: &Path) -> Result<String> {
        self.core
            .run(self.core.command_in(repo, ["rev-parse", "HEAD"]))
            .await
    }

    pub async fn is_clean(&self, repo: &Path) -> Result<bool> {
        self.core
            .probe(self.core.command_in(repo, ["diff", "--quiet"]))
            .await
    }

    pub async fn branches(&self, repo: &Path) -> Result<Vec<String>> {
        self.core
            .try_parse(
                self.core
                    .command_in(repo, ["branch", "--format=%(refname:short)"]),
                |stdout| {
                    let branches: Vec<String> = stdout.lines().map(str::to_owned).collect();
                    if branches.is_empty() {
                        Err(Error::parse("git", "no branches returned"))
                    } else {
                        Ok(branches)
                    }
                },
            )
            .await
    }

    pub async fn version_json(&self) -> Result<serde_json::Value> {
        self.core.output_json(["version", "--json"]).await
    }
}
```

The macro supplies `Git::new()`, `Default`, `Git::with_runner(runner)`, and
builders for the client defaults. A hand-written struct containing
`CliClient<R>` is equally supported when you need a different layout or want to
expose the core deliberately.

## Build commands and choose verbs

`command(args)` builds `program <args>`; `command_in(dir, args)` adds a working
directory. Most verbs also accept an argument list directly, so build a
`Command` only for a per-call override:

```rust,no_run
use processkit::CliClient;
use std::time::Duration;

# async fn example() -> processkit::Result<()> {
let git = CliClient::new("git").default_timeout(Duration::from_secs(30));

let version = git.run(["--version"]).await?;
let status = git
    .output_string(git.command(["status", "--short"]).timeout(Duration::from_secs(5)))
    .await?;
assert!(status.is_success());
# let _ = version;
# Ok(())
# }
```

The verbs are the same vocabulary used by `Command` and `ProcessRunnerExt`:

| Need | Verb | Result contract |
|---|---|---|
| trimmed stdout, accepted exit required | `run` | `String` |
| accepted exit, no value | `run_unit` | `()` |
| full accepted result | `checked` | `ProcessResult<String>` |
| inspect any exit | `output_string` / `output_bytes` | full result; non-zero is data |
| exit code is the answer | `exit_code` / `probe` | `i32` or `0 → true`, `1 → false` |
| typed parsing | `parse` / `try_parse` | infallible or fallible parser |
| typed JSON (`json`) | `output_json` | deserialized `T` with bounded diagnostics |

`parse` and `try_parse` require an accepted exit and reject truncated capture
before invoking the parser. `try_parse` should return `Error::parse` (or another
typed processkit error) when output is malformed. With the `json` feature,
`output_json` supplies that policy for complete JSON documents; its parse
failure is `ErrorReason::Parse` with a bounded fragment and location.

## Client defaults and precedence

Defaults are gap-filled into every command. An explicit per-command value wins.

```rust,no_run
use processkit::{CliClient, RetryPolicy};
use std::time::Duration;

let gh = CliClient::new("gh")
    .default_timeout(Duration::from_secs(30))
    .default_env("GH_PAGER", "cat")
    .default_env_remove("GH_PROMPT_DISABLED")
    .default_env_fn("REQUEST_ID", || "request-42")
    .default_retry(
        RetryPolicy::new().max_retries(2),
        |error| error.is_transient(),
    );

// The explicit 5 s timeout wins; the env and retry defaults are still filled in.
let command = gh.command(["api", "user"]).timeout(Duration::from_secs(5));
# let _ = command;
```

- `default_timeout`, `default_cancel_on`, and `default_retry` apply wherever the
  command did not set its own value.
- Static `default_env` / `default_env_remove` values override a dynamic resolver
  for the same key. Per-command `env` / `env_remove` override both.
- `default_env_fn` runs synchronously **once when a command is built**. A retry
  or second run of that already-built command reuses the baked value. Use it for
  cheap per-operation values, not a token that must change for every retry.
- Retry only operations safe to replay, and classify typed errors instead of
  retrying every failure. Non-erroring capture verbs do not retry.

Defaults also apply when a verb receives a ready-made `Command`, but that
command keeps its own program. Prefer argument lists or `client.command(...)`
unless deliberately grafting one client's defaults onto another executable.

## Error boundaries

The wrapper should preserve processkit's typed taxonomy:

- launch and resolution failures remain `NotFound`, `Spawn`, `Unsupported`, or
  `Io`;
- a checking verb maps an unaccepted exit to `Exit` and a run deadline to
  `Timeout`;
- parser failures belong in `Parse`, never a fabricated exit code;
- `checked` is intentionally lenient about truncated capture, while `run`,
  `parse`, `try_parse`, and `output_json` fail loud with `OutputTooLarge`.

This keeps callers able to branch on `error.reason()`, `is_not_found()`,
`is_timeout()`, or `is_transient()` without parsing strings. See
[Errors](errors.md) for the complete taxonomy and [Timeouts, retries &
cancellation](timeouts-and-cancellation.md) for replay safety.

## Test the wrapper without subprocesses

`ScriptedRunner` matches program plus argv and returns deterministic replies.
The wrapper code and parsers are the production code; only process execution is
replaced.

```rust,no_run
use processkit::testing::{Reply, ScriptedRunner};
use processkit::{cli_client, ProcessRunner, Result};

cli_client!(pub struct Git => "git");

impl<R: ProcessRunner> Git<R> {
    pub async fn current_branch(&self) -> Result<String> {
        self.core.run(["branch", "--show-current"]).await
    }
}

#[tokio::test]
async fn current_branch_is_trimmed() {
    let git = Git::with_runner(
        ScriptedRunner::new()
            .on(["git", "branch", "--show-current"], Reply::ok("main\n")),
    );
    assert_eq!(git.current_branch().await.unwrap(), "main");
}
```

For a response too awkward to maintain by hand, record it once and replay the
cassette in tests. `RecordReplayRunner` still implements `ProcessRunner`, so the
wrapper needs no alternate code path:

```rust,no_run
use processkit::testing::RecordReplayRunner;
use processkit::CliClient;

# async fn replay() -> processkit::Result<String> {
let runner = RecordReplayRunner::replay("fixtures/git.json")?;
let git = CliClient::with_runner("git", runner);
git.run(["rev-parse", "HEAD"]).await
# }
```

Use replay in normal tests; keep recording as an explicit fixture-refresh step.
The full cassette security and matching contract is in
[Testing your code](testing.md#recordreplay-cassettes).

## Worked wrappers

- [The .NET version](dotnet-version.md) shows a larger typed surface and error
  translation around a real CLI.
- [The Python wrapper](python-wrapper.md) shows how the same runner/client seam
  maps across a language boundary.
- [Cookbook: wrap a CLI tool](cookbook.md#wrap-a-cli-tool-behind-a-typed-api)
  is the short recipe; this chapter is the design and testing guide behind it.
