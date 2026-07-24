# Cookbook

[‹ docs index](README.md)

Task-oriented recipes: find the thing you're trying to do, copy the snippet,
follow the link when you need the fine print. Every snippet assumes a tokio
runtime and `use processkit::Command;` unless shown otherwise.

- [Run a command and get its output](#run-a-command-and-get-its-output)
- [Inspect a failure instead of erroring](#inspect-a-failure-instead-of-erroring)
- [Ask a yes/no question](#ask-a-yesno-question)
- [Accept non-zero exit codes as success](#accept-non-zero-exit-codes-as-success)
- [Bound a run with a timeout](#bound-a-run-with-a-timeout)
- [Let a tool clean up on timeout](#let-a-tool-clean-up-on-timeout)
- [Show a useful error message](#show-a-useful-error-message)
- [Check a tool is installed without running it](#check-a-tool-is-installed-without-running-it)
- [Feed the child's stdin](#feed-the-childs-stdin)
- [Stream output as it arrives](#stream-output-as-it-arrives)
- [Talk to an interactive child](#talk-to-an-interactive-child)
- [Run a tool that requires a terminal](#run-a-tool-that-requires-a-terminal)
- [Answer an unterminated PTY prompt](#answer-an-unterminated-pty-prompt)
- [Read in-place progress from an agent CLI](#read-in-place-progress-from-an-agent-cli)
- [Avoid PTY full-duplex deadlocks](#avoid-pty-full-duplex-deadlocks)
- [Test PTY behavior without a terminal](#test-pty-behavior-without-a-terminal)
- [Driving ssh](#driving-ssh)
- [Pipe commands without a shell](#pipe-commands-without-a-shell)
- [Start a server and wait until it's ready](#start-a-server-and-wait-until-its-ready)
- [Tear down several children as a unit](#tear-down-several-children-as-a-unit)
- [React to whichever child exits first](#react-to-whichever-child-exits-first)
- [Sandbox an untrusted tool](#sandbox-an-untrusted-tool)
- [Keep a crash-prone service running](#keep-a-crash-prone-service-running)
- [Retry a flaky command](#retry-a-flaky-command)
- [Cancel runs on shutdown](#cancel-runs-on-shutdown)
- [Measure what a run cost](#measure-what-a-run-cost)
- [Contain a process you didn't spawn](#contain-a-process-you-didnt-spawn)
- [Test code that runs processes — without processes](#test-code-that-runs-processes--without-processes)
- [Test streaming code — without processes](#test-streaming-code--without-processes)
- [Wrap a CLI tool behind a typed API](#wrap-a-cli-tool-behind-a-typed-api)

## Run a command and get its output

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let head = Command::new("git").args(["rev-parse", "HEAD"]).run().await?;
    Ok(())
}
```

`run()` requires a zero exit and returns stdout with trailing whitespace
trimmed; a non-zero exit, spawn failure, or timeout is a typed `Error`. For a
one-liner without the builder: `processkit::run("git", ["rev-parse", "HEAD"])`.

*Fine print: [Running commands → consuming verbs](commands.md).*

## Inspect a failure instead of erroring

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("git").args(["merge", "topic"]).output_string().await?;
    if !result.is_success() {
        eprintln!("merge exited {:?}: {}", result.code(), result.stderr());
    }
    Ok(())
}
```

`output_string()` (and `output_bytes()` for raw bytes) treats the exit code as
data — `Err` means the run couldn't happen at all. Call
`result.ensure_success()?` later to convert a stored failure into the same
typed error `run()` would have produced.

*Fine print: [Running commands → results and errors](commands.md).*

## Ask a yes/no question

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let dirty = !Command::new("git").args(["diff", "--quiet"]).probe().await?;
    Ok(())
}
```

`probe()` maps exit 0 → `true`, exit 1 → `false`, and anything else to an
error — the `git diff --quiet` / `grep -q` convention without manual code
matching.

## Accept non-zero exit codes as success

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // `grep` exits 1 when it finds no match — not a failure for this call.
    let found = Command::new("grep")
        .args(["needle", "haystack.txt"])
        .ok_codes([0, 1])
        .output_string()
        .await?;
    let matched = found.code() == Some(0); // 0 = matched, 1 = no match (both "success")
    Ok(())
}
```

`ok_codes` widens what the checking verbs (`run`/`run_unit`) and
`is_success`/`ensure_success` treat as success — for tools whose non-zero exit is a
normal result (`grep` 1 = no match, `diff` 1 = differs, rsync's code families). It
does **not** change `exit_code` (always the raw code) or `probe` (always the 0/1
convention). An empty set is ignored, so the default stays exit `0`.

## Bound a run with a timeout

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("slow-tool")
        .timeout(Duration::from_secs(30))
        .output_string()
        .await?;
    if result.timed_out() {
        eprintln!("gave up after 30s; partial output: {}", result.stdout());
    }
    Ok(())
}
```

At the deadline the whole tree is killed. On the capture verbs the timeout is
*captured* (`timed_out()`, partial output kept); on the success-checking verbs
(`run`, `exit_code`) it surfaces as `ErrorReason::Timeout`.

## Let a tool clean up on timeout

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("dev-server")
        .timeout(Duration::from_secs(30))
        .timeout_grace(Duration::from_secs(5)) // SIGTERM, wait up to 5s, then SIGKILL
        .output_string()
        .await?;
    Ok(())
}
```

`timeout_grace` turns the hard deadline kill into a graceful one: `SIGTERM` (or the
signal from `timeout_signal`, with the `process-control` feature), up to the grace
window to exit, then `SIGKILL`. A signal-handling child exits early; `timed_out()`
stays `true`. Windows has no signal tier — the deadline kills atomically.

*Fine print: [Timeouts → graceful timeout](timeouts-and-cancellation.md#graceful-timeout).*

*Fine print: [Timeouts, retries & cancellation](timeouts-and-cancellation.md).*

## Show a useful error message

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    if let Err(e) = Command::new("git").args(["merge", "topic"]).run().await {
        eprintln!("merge failed: {}", e.diagnostic().unwrap_or("(no output)"));
    }
    Ok(())
}
```

`Error::diagnostic()` picks the most explanatory captured text — stderr,
falling back to stdout (git writes `CONFLICT …` there) — so callers don't
re-implement the same heuristic.

## Check a tool is installed without running it

```rust,no_run
use processkit::Command;

fn main() {
    // The crate-level shortcut for a bare tool:
    match processkit::which("git") {
        Ok(path) => println!("git is at {}", path.display()),
        Err(e) if e.is_not_found() => eprintln!("git is not installed"),
        Err(e) => eprintln!("could not resolve git: {e}"),
    }

    // On a builder it honors that command's `prefer_local` / env, resolving
    // exactly what a real run would launch:
    let _ = Command::new("eslint")
        .prefer_local("./node_modules/.bin")
        .resolve_program();
}
```

`which` / `Command::resolve_program()` locate a program **without spawning it**
— a side-effect-free *doctor* / preflight check ("is the tool installed?") for a
friendly up-front error. Resolution reuses the crate's own launch-path logic
(the same PATH/PATHEXT/execute-bit and `prefer_local` handling a real run uses),
so a hit is exactly what would be launched and a miss is exactly the
`ErrorReason::NotFound` (`is_not_found()`) a run would raise. It is synchronous — no
tokio runtime needed. A wrapped tool's client offers the same via
`CliClient::resolve_program()`.

*Fine print: [Running commands → preflight](commands.md).*

## Feed the child's stdin

```rust,no_run
use processkit::Command;
use processkit::Stdin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A string you already have:
    let sorted = Command::new("sort")
        .stdin(Stdin::from_string("banana\napple\n"))
        .run()
        .await?;

    // …or any async source: a reader (file, socket) or a stream of lines.
    let from_file = Stdin::from_reader(tokio::fs::File::open("input.txt").await?);
    let from_chan = Stdin::from_lines(tokio_stream::iter(vec!["one".to_owned()]));
    let _ = (sorted, from_file, from_chan);
    Ok(())
}
```

One-shot sources (`from_reader`/`from_lines`) feed a single run; re-running the
same `Command` afterwards **fails loud** (an `ErrorReason::Io` at launch, D10) instead
of silently seeing empty stdin. For a conversation, see the next recipe but one.

*Fine print: [Running commands → standard input](commands.md).*

## Stream output as it arrives

```rust,no_run
use processkit::Command;
use processkit::Finished;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    use processkit::prelude::StreamExt; // re-exported; provides `.next()`

    let mut run = Command::new("cargo").args(["build", "--verbose"]).start().await?;
    let mut lines = run.stdout_lines()?;
    while let Some(line) = lines.next().await {
        println!("build: {line}");
    }
    let Finished { outcome, stderr, .. } = run.finish().await?; // outcome + buffered stderr
    Ok(())
}
```

No waiting for exit, no full-output buffering; stderr is drained in the
background so the child can't block. A `timeout` on the command bounds the
stream itself. Prefer a callback? `.on_stdout_line(|l| …)` runs one per line
while any capture verb drives the run.

*Fine print: [Streaming & interactive I/O](streaming.md).*

## Forward a child's exact bytes (binary passthrough)

```rust,no_run
use processkit::Command;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let exact = File::create("archive.tar").await?;
    let outcome = Command::new("git")
        .args(["archive", "HEAD"]) // binary tar on stdout
        .stdout_raw_tee(exact)     // exact bytes, before any decoding
        .start()
        .await?
        .drain()                   // stream through, retain nothing
        .await?;
    println!("done: {outcome:?}");
    Ok(())
}
```

`stdout_raw_tee` / `stderr_raw_tee` write each chunk **exactly as read from the
pipe** — no decoding, no CRLF rewrite, no line splitting, no lost tail — so
non-UTF-8 output (`git archive`, `tar -cz -`, `ffmpeg … -`) survives byte for
byte. It runs *alongside* the decoded sinks (`stdout_lines`, `on_stdout_line`,
`stdout_tee`), so you can forward the raw stream and react to decoded lines at
once. For the whole output in memory instead of streamed through, `output_bytes()`
returns the exact bytes directly.

*Fine print: [Streaming → byte-accurate raw output](streaming.md).*

## Talk to an interactive child

```rust,no_run
use processkit::Command;
use processkit::prelude::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut run = Command::new("bc").keep_stdin_open().start().await?;
    let mut stdin = run.take_stdin().expect("stdin was kept open");
    stdin.write_line("2 + 2").await?;
    stdin.finish().await?; // EOF — bc exits

    let mut answers = run.stdout_lines()?;
    while let Some(answer) = answers.next().await {
        println!("{answer}");
    }
    Ok(())
}
```

`keep_stdin_open()` hands you an async writer instead of closing stdin at
spawn; interleave writes with reads for request/response tools. Its writer
methods return `std::io::Result` (idiomatic for a writer) — convert with
`.map_err(processkit::ErrorReason::Io)?` in a `processkit::Result` function, or use
`Box<dyn std::error::Error>`.

*Fine print: [Streaming & interactive I/O → interactive stdin](streaming.md).*

## Run a tool that requires a terminal

Some tools change behavior behind `isatty()` or refuse to start without a
controlling terminal. Enable the `pty` feature, opt this run into PTY mode, and
read its output through the usual capture verbs:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("terminal-only-tool")
        .arg("--status")
        .use_pty()
        .pty_size(120, 30)
        .output_string()
        .await?;

    // A PTY has one merged output stream. Everything the child wrote to its
    // terminal is captured as stdout; the separate stderr result is empty.
    println!("{}", result.stdout());
    assert!(result.stderr().is_empty());
    Ok(())
}
```

`use_pty()` changes the transport, not the lifecycle: timeout, cancellation,
containment, and kill-on-drop behave as for a piped child. It is a minimal
terminal session rather than a terminal emulator; set `pty_size` when wrapping
and layout matter.

*Fine print: [Streaming → PTY window size and live resize](streaming.md#pty-window-size-and-live-resize) ·
[Platform support → PTY mode](platform-support.md#pty-mode-use_pty-the-pty-feature).*

## Answer an unterminated PTY prompt

Terminal prompts commonly end in `": "` rather than a newline. Use
`wait_for_output`, not `wait_for_line`, to match that live partial tail; then
answer over the same PTY master:

```rust,no_run
use processkit::Command;
use std::time::Duration;

async fn authenticate(secret: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut run = Command::new("sudo")
        .args(["-k", "-v"])
        .use_pty()
        .keep_stdin_open()
        .start()
        .await?;

    run.wait_for_output(
        |tail| tail.contains("Password:"),
        Duration::from_secs(10),
    )
    .await?;

    let mut stdin = run.take_stdin().expect("PTY stdin was kept open");
    stdin.write_line(secret).await?;
    run.finish().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    authenticate("secret supplied by a protected source").await
}
```

On Unix, processkit disables terminal echo for its PTY slave, so the secret is
not copied into merged output. That echo-off guarantee is **Unix-only**:
ConPTY has no portable per-session equivalent, so do not retain or log the
merged transcript when sending a secret on Windows. `wait_for_output` is
byte-driven and works for ordinary prompts on both platforms; it does not need
a newline or a signal from the child.

*Fine print: [Streaming → prompt-aware waiting](streaming.md#prompt-aware-waiting-wait_for_output) ·
[Running commands → interactive auth / TTY](commands.md#privileges-and-spawn-flags).*

## Read in-place progress from an agent CLI

Agent CLIs often require a terminal, redraw progress with bare carriage returns,
and decorate it with VT/ANSI control sequences. Frame each redraw as a line and
sanitize the captured text before inspecting it:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, LineTerminator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut run = Command::new("agent")
        .arg("run")
        .use_pty()
        // PTY output is the logical stdout stream. `line_terminator(...)`
        // is the equivalent whole-command spelling.
        .stdout_line_terminator(LineTerminator::CarriageReturn)
        .stdout_sanitize_vt()
        .start()
        .await?;

    let mut frames = run.stdout_lines()?;
    while let Some(frame) = frames.next().await {
        println!("agent: {frame}");
    }
    run.finish().await?;
    Ok(())
}
```

PTY mode already defaults to carriage-return-aware framing; spelling it out is
useful when this behavior is part of your wrapper's contract. Use
`line_terminator` and `sanitize_vt` to configure both logical streams when the
same builder may also run without PTY. VT sanitization is destructive and
affects captured text only; exact-byte output and raw tees remain untouched.

*Fine print: [Streaming → PTY output hygiene](streaming.md#pty-output-hygiene-line-framing-and-vt-sanitization) ·
[Untrusted children → content transforms](untrusted-children.md).*

## Avoid PTY full-duplex deadlocks

A PTY master, like a pair of pipes, has finite kernel buffers. Do not await a
large write while leaving output unread: the child may fill its output buffer,
stop reading input, and leave both sides parked. Drain and write concurrently:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let payload = vec![b'x'; 4 * 1024 * 1024];
    let mut run = Command::new("terminal-filter")
        .use_pty()
        .keep_stdin_open()
        .start()
        .await?;
    let mut stdin = run.take_stdin().expect("PTY stdin was kept open");
    let mut output = run.stdout_lines()?;

    let write = async move {
        stdin.write(&payload).await?;
        stdin.finish().await
    };
    let drain = async move {
        while let Some(line) = output.next().await {
            println!("{line}");
        }
        Ok::<(), std::io::Error>(())
    };

    tokio::try_join!(write, drain)?;
    run.finish().await?;
    Ok(())
}
```

Also remember that a PTY has no independent stderr channel. Both child file
descriptors arrive through `stdout_lines`/`on_stdout_line`;
`on_stderr_line` is not delivered, `stderr_tee` has nothing to write, and the
finished result's stderr is empty. If stream identity matters, use ordinary
pipes instead of PTY.

*Fine print: [Streaming → interactive stdin](streaming.md#interactive-stdin) ·
[Running commands → interactive auth / TTY](commands.md#privileges-and-spawn-flags).*

## Test PTY behavior without a terminal

The `ScriptedRunner` PTY variant is selected by putting `use_pty()` on the
scripted command. It hermetically models the merged master and PTY line framing,
so CI needs neither a real terminal nor a platform-specific helper:

```rust
use processkit::prelude::StreamExt;
use processkit::testing::{Reply, ScriptedRunner};
use processkit::{Command, LineTerminator, ProcessRunner};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let runner = ScriptedRunner::new().fallback(
        Reply::ok("working 10%\rworking 100%\r")
            .with_stderr("final diagnostic"),
    );
    let command = Command::new("agent")
        .use_pty()
        .line_terminator(LineTerminator::CarriageReturn);

    let mut run = runner.start(&command).await?;
    let mut merged = run.stdout_lines()?;
    let mut seen = Vec::new();
    while let Some(frame) = merged.next().await {
        seen.push(frame);
    }
    let finished = run.finish().await?;

    assert!(seen.iter().any(|line| line == "working 100%"));
    assert!(seen.iter().any(|line| line == "final diagnostic"));
    assert!(finished.stderr.is_empty());
    Ok(())
}
```

For dialogs, script `Reply::dialog("Password: ", "accepted\n")`; it emits the
unterminated prompt, waits for `take_stdin()` to receive an answer, then emits
the continuation. This exercises `wait_for_output` without subprocess timing.

*Fine print: [Testing → scripted streaming](testing.md#scripted-streaming) ·
[Streaming → prompt-aware waiting](streaming.md#prompt-aware-waiting-wait_for_output).*

## Driving ssh

`ssh` is the one tool worth its own recipe. It often *demands* a terminal
(password/passphrase prompts), and — alone among the tools you'll launch — it
spawns work on **another host**, past anything kill-on-drop can reach. Two
things to get right: how you authenticate, and what "contained" does and
doesn't cover.

**Prefer the non-interactive path — key auth, `BatchMode=yes`, ordinary
pipes.** With key-based auth and `BatchMode=yes`, ssh never prompts, so a plain
run captures the remote command's output like any local one:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("ssh")
        .args(["-o", "BatchMode=yes", "deploy@host", "systemctl is-active app"])
        .output_string()
        .await?;

    match result.code() {
        // 255 is ssh's OWN failure (host unreachable, auth/BatchMode rejected) —
        // NOT the remote command's exit; don't read it as the remote result.
        Some(255) => eprintln!("ssh could not connect: {}", result.stderr()),
        // Any other code came straight through from the remote command.
        Some(code) => println!("remote `systemctl is-active` exited {code}"),
        None => eprintln!("ssh was signalled or timed out on this side"),
    }
    Ok(())
}
```

**Exit code 255 is ssh's own "connection/auth failed", not the remote
command's code.** ssh forwards the remote command's real exit status as its own
— *except* 255, which it reserves for its own failures (host unreachable, auth
rejected, `BatchMode` with no usable key). A caller that cares about the remote
side must special-case 255 before trusting the code, as above. The one residual
ambiguity ssh can't resolve for you: a remote command that *itself* exits 255
looks identical to a connection failure.

**The remote command line is parsed by the *remote* shell — the crate's
shell-free guarantee stops at the local process.** processkit runs the local
`ssh` with **no shell**: its argv is passed literally, with no interpolation and
no injection surface (the crate's shell-free property). But `ssh host "some
command"` hands that command string to a login shell **on the far end**, which
word-splits and expands it like any shell line. So any value you interpolate
into a remote command needs manual quoting/escaping (or build it so no untrusted
value reaches the remote shell at all) — the injection surface the local API
removes reappears on the remote side.

**Password / passphrase / host-key prompts need a PTY.** When key-auth isn't an
option, `use_pty()` (the `pty` feature) gives ssh the terminal it insists on;
drive the prompt over the merged master with `keep_stdin_open` + `take_stdin`:

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut run = Command::new("ssh")
        .args(["deploy@host", "systemctl status app"])
        .use_pty()          // real terminal (needs the `pty` feature) so ssh prompts
        .keep_stdin_open()
        .start()
        .await?;

    // Match the live, unterminated prompt tail; no newline is required.
    run.wait_for_output(
        |tail| tail.contains("passphrase"),
        Duration::from_secs(10),
    )
    .await?;
    let mut stdin = run.take_stdin().expect("stdin kept open");
    stdin.write_line("s3cr3t-passphrase").await?;

    // stdout and stderr are MERGED onto the one master in PTY mode — read the
    // rest of the exchange from run.stdout_lines().
    Ok(())
}
```

`wait_for_output` sees the un-terminated prompt tail that `wait_for_line`
deliberately cannot. The same platform echo caveat as the general
[PTY prompt recipe](#answer-an-unterminated-pty-prompt) applies: echo is
disabled by processkit on Unix, but not portably controllable through ConPTY.

**The containment boundary stops at the local ssh client.** kill-on-drop,
`timeout`, and cancellation reap the *local* process tree — here, the `ssh`
client itself. They do **not** reach across the connection: dropping the handle
(or a timeout, or a panic) severs the local ssh client, but the command it
started **on the remote host** can keep running, orphaned. The crate's
whole-tree guarantee is about *your* machine's process tree; an ssh hop is a
boundary it cannot cross. Contain the remote side on the remote side:

- **`ssh -tt`** forces a remote pseudo-terminal, so when the connection drops
  the remote session receives `SIGHUP` — which ends many (not all) remote
  programs.
- **A server-side deadline** — `ssh host 'timeout 300 long-job'`, or the
  service's own timeout/idle limit — bounds the remote work regardless of the
  local client's fate.
- For anything that *must* be torn down reliably, make the remote command own
  its own lifecycle (a unit/scope with a deadline, a job the remote scheduler
  can kill), not the ssh client's liveness.

*Fine print: [Running commands → interactive auth](commands.md#privileges-and-spawn-flags) ·
[Running untrusted children → what not to rely on](untrusted-children.md#what-not-to-rely-on).*

## Pipe commands without a shell

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let authors = Command::new("git").args(["log", "--format=%an"])
        .pipe(Command::new("sort"))
        .pipe(Command::new("uniq").arg("-c"))
        .output_string()
        .await?;
    Ok(())
}
```

Native pipes — no shell string, no quoting, no injection surface. The outcome
is **pipefail**: stdout comes from the last stage, the reported failure from
the first stage that didn't exit cleanly. All stages share one kill-on-drop
group. The `|` operator is equivalent sugar:
`(a | b | c).output_string()`.

For a consumer that legitimately stops reading early — the `| head -1` shape,
where the producer's broken-pipe death (its next write fails once the downstream
closes, or `SIGPIPE` where the OS delivers it) is expected — mark the producer
`unchecked_in_pipe()` so that death doesn't fail the chain:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let first = (Command::new("seq").args(["1", "1000000"]).unchecked_in_pipe()
        | Command::new("head").args(["-n", "1"]))
        .run()
        .await?;
    Ok(())
}
```

*Fine print: [Pipelines → unchecked stages](pipelines.md#unchecked-stages).*

## Start a server and wait until it's ready

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut server = Command::new("my-server").args(["--port", "8080"]).start().await?;

    // Pick the probe that matches how the server announces readiness:
    server.wait_for_line(|l| l.contains("listening"), Duration::from_secs(10)).await?;
    // server.wait_for_port("127.0.0.1:8080".parse().unwrap(), Duration::from_secs(10)).await?;
    // server.wait_for_socket("/tmp/my-server.sock", Duration::from_secs(10)).await?; // Unix only
    // server.wait_for(|| async { http_health().await }, Duration::from_secs(10)).await?;

    // …use the server; dropping `server` kills its whole tree.
    Ok(())
}
```

A probe that can't succeed fails fast with `ErrorReason::NotReady` and never kills
the child — you decide what happens next. No more `sleep(2)` and hoping.

*Fine print: [Streaming & interactive I/O → readiness probes](streaming.md).*

## Tear down several children as a unit

```rust,no_run
use processkit::Command;
use processkit::ProcessGroup;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _db = group.start(&Command::new("dev-db")).await?;
    let _api = group.start(&Command::new("dev-api")).await?;

    // Either: graceful — SIGTERM, bounded wait, optional SIGKILL escalation…
    group.shutdown().await?;
    // …or just drop(group): hard kill-on-drop of everything, grandchildren included.
    Ok(())
}
```

The group is the unit of fate: a panic or early return anywhere reaps every
member. Configure the grace window via `ProcessGroupOptions`.

*Fine print: [Process groups](process-groups.md).*

## React to whichever child exits first

```rust,no_run
use processkit::Command;
use processkit::{ProcessGroup, wait_any};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let mut a = group.start(&Command::new("worker-a")).await?;
    let mut b = group.start(&Command::new("worker-b")).await?;

    let (idx, outcome) = wait_any(&mut [&mut a, &mut b]).await?;
    println!("worker #{idx} exited first with {outcome:?}");
    // `a` and `b` are only borrowed — the loser is still usable here.
    Ok(())
}
```

*Fine print: [Streaming & interactive I/O → racing children](streaming.md).*

## Sandbox an untrusted tool

```rust,no_run
use processkit::Command;
use processkit::{ProcessGroup, ProcessGroupOptions};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Cap the whole tree (requires the `limits` feature; Windows Job / Linux cgroup):
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .max_memory(512 * 1024 * 1024)
            .max_processes(64)
            .cpu_quota(0.5),
    )?;

    let result = group
        .start(
            &Command::new("untrusted-tool")
                .inherit_env(["PATH"]) // allow-list: everything else is cleared
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await?
        .output_string()
        .await?;
    Ok(())
}
```

Unenforceable limits are a hard `ErrorReason::ResourceLimit`, never a silently
unbounded group. On Unix, add `.uid(…)`/`.gid(…)` to drop privileges (note the
cgroup-mechanism caveat in the guide).

*Fine print: [Process groups → resource limits](process-groups.md) ·
[Running commands → privileges](commands.md).*

## Keep a crash-prone service running

```rust,no_run
use processkit::Command;
use processkit::{RestartPolicy, Supervisor};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let outcome = Supervisor::new(Command::new("my-service"))
        .restart(RestartPolicy::OnCrash)
        .max_restarts(5)
        .backoff(Duration::from_millis(200), 2.0)
        .storm_pause(Duration::from_secs(15)) // crash-loop guard (off by default)
        .run()
        .await?;
    println!(
        "stopped after {} restarts ({} storm pauses): {:?}",
        outcome.restarts, outcome.storm_pauses, outcome.stopped
    );
    Ok(())
}
```

Exponential backoff with jitter by default; `stop_when(…)` ends supervision on
a condition; `.with_runner(&group)` keeps every incarnation inside one shared
kill-on-drop group. `storm_pause` arms the failure-storm guard: failures feed
a decaying score, and past the threshold the supervisor takes one collective
pause instead of hammering restarts — "fails rarely" and "crash-looping" stop
being the same case.

*Fine print: [Supervision](supervision.md), [failure storms](supervision.md#failure-storms).*

## Retry a flaky command

```rust,no_run
use processkit::Command;
use processkit::ErrorReason;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let fetched = Command::new("git")
        .args(["fetch", "--quiet"])
        .timeout(Duration::from_secs(10))
        .retry(3, Duration::from_millis(200), |e| {
            matches!(e.reason(), ErrorReason::Timeout { .. })
                || e.diagnostic().is_some_and(|m| m.contains("Could not resolve host"))
        })
        .run()
        .await?;
    Ok(())
}
```

The classifier sees the typed error and decides whether this failure is worth
another attempt; each attempt is a fresh process. `retry` replays a run to
success — for keeping a process *alive*, use a `Supervisor` (previous recipe).

*Fine print: [Timeouts, retries & cancellation → retry](timeouts-and-cancellation.md).*

## Cancel runs on shutdown

```rust,no_run
use processkit::Command;
use processkit::CancellationToken;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let token = CancellationToken::new();

    let job = tokio::spawn({
        let token = token.child_token();
        async move { Command::new("long-job").cancel_on(token).run().await }
    });

    // On Ctrl-C / shutdown signal / sibling failure:
    token.cancel(); // kills the tree; the run resolves to ErrorReason::Cancelled
    let outcome = job.await; // Err(ErrorReason::Cancelled { .. }) inside
    Ok(())
}
```

Cancellation is always an error (the run was abandoned, there is no result),
beats a simultaneous timeout, and is terminal for `retry` and `Supervisor`
alike.

For a typed wrapper whose commands never cross your code, set the token once
on the client — every command it builds carries it:

```rust,no_run
use processkit::{CancellationToken, CliClient};

let token = CancellationToken::new();
let gh = CliClient::new("gh").default_cancel_on(token.child_token());
// token.cancel() → every in-flight command of THIS client dies.
```

*Fine print: [Timeouts, retries & cancellation → cancellation](timeouts-and-cancellation.md), [client-level default](timeouts-and-cancellation.md#client-level-default).*

## Measure what a run cost

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // One run, summarized (requires the opt-in `stats` feature):
    let profile = Command::new("crunch").start().await?.profile(Duration::from_millis(100)).await?;
    println!("outcome={:?} took={:?} peak_rss={:?} avg_cpu_cores={:?}",
        profile.outcome, profile.duration, profile.peak_memory_bytes, profile.avg_cpu_cores());
    Ok(())
}
```

For a live series over a whole group, `group.sample_stats(every)` yields a
`Stream` of snapshots. CPU/memory need a real container (Windows Job / Linux
cgroup); elsewhere you still get process counts.

*Fine print: [Process groups → stats](process-groups.md) ·
[Streaming → profiling](streaming.md).*

## Contain a process you didn't spawn

```rust,no_run
use processkit::{Error, ErrorReason, ProcessGroup};

fn main() -> processkit::Result<()> {
    // `tokio`/`std` calls return `io::Error`, which the crate does NOT auto-convert
    // into `processkit::Error` (there is no blanket `From<io::Error>`, by design) —
    // map it explicitly, or use a `Box<dyn std::error::Error>` / `anyhow` return in
    // your own code so both error types `?` freely.
    let child = tokio::process::Command::new("legacy-launcher")
        .spawn()
        .map_err(|e| Error::from(ErrorReason::Io(e)))?;

    let group = ProcessGroup::new()?; // `adopt` is part of `process-control` (default-on)
    group.adopt(&child)?;            // from now on the group's teardown covers it
    Ok(())
}
```

Adoption is best-effort by mechanism — on Windows/cgroup the whole running
tree joins; on the POSIX process-group backends an exec'd child is contained
individually (its *future* forks too, where it could be re-grouped). The guide
spells out exactly what each mechanism can promise.

*Fine print: [Process groups → adopt](process-groups.md) ·
[Platform support](platform-support.md).*

## Test code that runs processes — without processes

```rust,no_run
use processkit::Command;
use processkit::testing::{Reply, ScriptedRunner};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Your code takes any `R: ProcessRunner`; in tests, hand it a script.
    // Rules match on a prefix of the *program name followed by its arguments*
    // (the first element is the program):
    let runner = ScriptedRunner::new()
        .on(["git", "rev-parse"], Reply::ok("abc123\n"))
        .on(["git", "push"], Reply::fail(128, "remote: permission denied"))
        .fallback(Reply::ok(""));

    // my_deploy(&runner).await? — no subprocess, fully deterministic.
    Ok(())
}
```

`RecordingRunner` wraps any runner and captures every `Invocation` for
assertions; `MockRunner` (feature `mock`) gives `mockall` expectations; and
the `record` feature's `RecordReplayRunner` records real runs into a JSON
cassette once and replays them hermetically in CI.

*Fine print: [Testing your code](testing.md).*

## Test streaming code — without processes

```rust,no_run
use processkit::{Command, Outcome, ProcessRunner, Finished};
use processkit::testing::{Reply, ScriptedRunner};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let runner = ScriptedRunner::new()
        .on(["gh", "run", "watch"], Reply::lines(["queued", "in_progress", "completed"])
            .with_line_delay(Duration::from_millis(50))); // paced delivery

    let mut run = runner.start(&Command::new("gh").args(["run", "watch", "123"])).await?;
    run.wait_for_line(|l| l.contains("completed"), Duration::from_secs(5)).await?;
    let Finished { outcome, .. } = run.finish().await?;
    assert_eq!(outcome, Outcome::Exited(0));
    Ok(())
}
```

A scripted `start()` feeds the canned lines through the **same pump
machinery** a real child uses, so `stdout_lines`, the readiness probes, and
`finish` behave identically — and `with_line_delay` is deterministic
under `#[tokio::test(start_paused = true)]`. Canned output also replays
through `on_stdout_line`/`on_stderr_line` handlers on the bulk verbs, so
progress-reporting paths test hermetically too.

*Fine print: [Testing → scripted streaming](testing.md#scripted-streaming).*

## Wrap a CLI tool behind a typed API

```rust,no_run
use processkit::{cli_client, ProcessRunner, Result};

cli_client!(pub struct Git => "git");

impl<R: ProcessRunner> Git<R> {
    pub async fn current_branch(&self) -> Result<String> {
        // A verb takes the args directly (D7); pass a built `command(..)` only
        // when you need to customize it (per-call timeout, stdin, …).
        self.core.run(["branch", "--show-current"]).await
    }
    pub async fn is_clean(&self) -> Result<bool> {
        self.core.probe(["diff", "--quiet"]).await
    }
}
```

The generated struct carries a runner and per-client defaults
(`default_timeout`, `default_env`); your methods are just argument lists and
parsers — and because the runner is injectable, the whole wrapper is testable
with the previous recipe's `ScriptedRunner`.

*Fine print: [Testing your code → CliClient](testing.md).*
