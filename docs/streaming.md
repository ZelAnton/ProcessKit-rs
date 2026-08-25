# Streaming & interactive I/O

[‹ docs index](README.md)

The one-shot verbs in [Running commands](commands.md) buffer the whole output.
For long-running or conversational children, `Command::start()` returns a live
`RunningProcess` you drive yourself: stream stdout as it arrives, write stdin
incrementally, probe for readiness, race several children, or profile a run.

The same live-handle model extends to **multi-stage chains**:
`Pipeline::start()` returns a `PipelineSession` — the multi-stage analogue of
`RunningProcess` — that streams the last stage's stdout, waits for a readiness
line, and folds the pipefail outcome at `finish()`, all while the whole chain is
bounded and torn down as a unit. See
[Pipelines → streaming a live chain](pipelines.md#streaming-a-live-chain).

- [Lifecycle](#lifecycle)
- [Streaming stdout](#streaming-stdout)
- [The full lifecycle as one stream (`events()`)](#the-full-lifecycle-as-one-stream-events)
- [Interactive stdin](#interactive-stdin)
- [Readiness probes](#readiness-probes)
- [Prompt-aware waiting (`wait_for_output`)](#prompt-aware-waiting-wait_for_output)
- [Racing children with `wait_any`](#racing-children-with-wait_any)
- [Per-run telemetry](#per-run-telemetry)

## Lifecycle

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("dev-server").start().await?;

    run.pid();        // Option<u32> — None once the child is reaped
    run.elapsed();    // time since spawn

    // Consume the handle exactly one way:
    //   output_string() / output_bytes()  → capture everything (same as the one-shot verbs)
    //   wait()                            → just the Outcome; output is discarded
    //   drain()                           → like wait(), but honors output_buffer's byte cap; feeds tees, retains nothing
    //   finish()                 → after streaming stdout (below)
    //   profile(every)                    → resource samples; output discarded, like wait() (stats feature)
    let outcome = run.wait().await?;   // Outcome: Exited(code) / Signalled(sig) / TimedOut
    Ok(())
}
```

### `wait()` vs `drain()`

Both wait for exit while draining stdout/stderr so the child never blocks on a
full pipe, and both return the same `Outcome`. They differ only in the in-flight
memory bound:

- **`wait()`** ignores `output_buffer` and pins a large fixed internal cap. Reach
  for it when you just want the exit outcome.
- **`drain()`** honors the configured
  [`output_buffer`](commands.md) **byte cap** ([`with_max_bytes`]) for the
  in-flight bound, retaining nothing. Reach for it when the output is already going
  where you want it — a `stdout_tee`/`stderr_tee` writing to a file, or an
  `on_stdout_line`/`on_stderr_line` handler — and you want held memory bounded by
  *your* configured limit rather than the child's output size. Those sinks still
  see every line that fits the cap; a line longer than the byte cap is skipped for
  every sink alike (counted only via the truncation signal), and an unbounded
  `output_buffer` falls back to the same fixed floor `wait` uses.

```rust,no_run
use processkit::{Command, OutputBufferPolicy};
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tee a noisy build to a file, keep only ~1 MiB in flight, capture nothing.
    let log = File::create("build.log").await?;
    let outcome = Command::new("cargo")
        .args(["build", "--release"])
        .output_buffer(OutputBufferPolicy::unbounded().with_max_bytes(1 << 20))
        .stdout_tee(log)
        .start()
        .await?
        .drain()
        .await?;
    println!("build finished: {outcome:?}");
    Ok(())
}
```

[`with_max_bytes`]: https://docs.rs/processkit/latest/processkit/struct.OutputBufferPolicy.html#method.with_max_bytes

`start()` puts the child in a **private group the handle owns**: dropping the
`RunningProcess` kills the whole tree, exactly like dropping a one-shot run's
future. The shared-group variant — `group.start(&cmd)` — gives the same handle
but the *group* controls the tree's fate (see
[Process groups](process-groups.md#putting-processes-in)).

There is also an explicit `run.start_kill()` for "stop it now, I'll `wait()`
for the code myself".

## Streaming stdout

`stdout_lines()` yields decoded lines as the child produces them — no waiting
for exit, no full-output buffering. `StreamExt` (in `processkit::prelude`,
re-exported from `tokio-stream`) provides `.next()`:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, Finished, Outcome};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("cargo")
        .args(["build", "--release"])
        .start()
        .await?;

    let mut lines = run.stdout_lines()?;
    while let Some(line) = lines.next().await {
        println!("build: {line}");
    }

    // The stream ended (stdout closed). Collect the outcome and stderr —
    // stderr was drained in the background the whole time, so a noisy child
    // could never block on a full pipe.
    let Finished { outcome, stderr, .. } = run.finish().await?;
    if outcome != Outcome::Exited(0) {
        eprintln!("build failed ({outcome:?}):\n{stderr}");
    }
    Ok(())
}
```

Things to know:

- **Call `stdout_lines()` once.** It is fallible: a second `stdout_lines` /
  `events` call (stdout is consumed once), or a non-piped stdout
  (`StdioMode::Inherit`/`Null` or `Command::stdout_file*`), returns `Err` rather than a silently-empty
  stream.
- **The command's `timeout` bounds the stream**: at the deadline the tree
  (own-group handle) or the direct child (shared-group handle) is killed, the
  pipes close, and the stream ends — a
  streamed run can't hang past its deadline. A `cancel_on` token ends it the
  same way; the following
  `finish` then reports `ErrorReason::Cancelled`. Details in
  [Timeouts & cancellation](timeouts-and-cancellation.md).
- Line counters tick live: `run.stdout_line_count()` / `stderr_line_count()`
  are cheap progress gauges even while you stream.
- Byte counters tick live: `run.stdout_bytes_seen()` /
  `stderr_bytes_seen()` count raw bytes read from each pipe before decoding or
  line splitting. They are monotonic and include bytes discarded by the
  buffer policy, including oversized lines, and remain stable after the pump
  completes. A stream that is not pumped (a file redirect or
  `StdioMode::Null`/`Inherit`) reports `0` honestly.
- The [buffer policy and line handlers](commands.md#output-handling) apply to
  streamed runs too — a handler sees each line on the pump, in addition to
  your loop.
- The whole streaming surface is **hermetically testable**: a
  `ScriptedRunner`'s `start()` returns a handle whose canned lines flow
  through the same pump machinery — `stdout_lines`, the readiness probes, and
  `finish` behave identically with no subprocess. See
  [Testing → scripted streaming](testing.md#scripted-streaming).

### Carriage-return progress output

Tools like `curl`, `pip`, and `apt` redraw a progress bar *in place* with a
carriage return (`\rProgress: 50%\rProgress: 100%`) and emit no `\n` until the
very end. By default the pump splits on `\n` only, so that whole sequence is a
single, ever-growing line: nothing streams live, and under a byte cap the one
over-cap line is dropped whole.

Set `line_terminator(LineTerminator::CarriageReturn)` to treat a bare `\r` as a
line terminator too. Each carriage-return frame then arrives as its own line —
live, one at a time:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, LineTerminator};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("pip")
        .args(["install", "big-package"])
        // Frame the stream you actually consume: `stdout_lines()` surfaces
        // stdout only (stderr is drained in the background and discarded), so
        // put the CR framing on stdout. If a tool draws its bar on stderr,
        // set `line_terminator(..)` for both streams and read `events()`.
        .stdout_line_terminator(LineTerminator::CarriageReturn)
        .start()
        .await?;

    let mut lines = run.stdout_lines()?;
    while let Some(frame) = lines.next().await {
        // Overwrite your own display with the latest frame.
        print!("\r{frame}");
    }
    run.finish().await?;
    Ok(())
}
```

The chosen framing is **one shared definition of a line** for every sink: the
streaming verbs, the [`on_stdout_line`/`on_stderr_line`
handlers](commands.md#output-handling), a `stdout_tee`/`stderr_tee` (which
writes each frame followed by `\n`), and `output_string` all see the same
per-frame lines. A `\r\n` pair stays a single terminator (no empty line between
them), so ordinary CRLF text reads identically to the default; only a `\r` *not*
followed by a `\n` splits a frame. The `OutputBufferPolicy` byte cap now bounds
an individual runaway frame — a frame whose content exceeds the cap is skipped
as it streams (never assembled whole) — rather than dropping the whole progress
stream. Use `stdout_line_terminator` / `stderr_line_terminator` for one stream,
or `line_terminator` for both.

### Byte-accurate raw output (`stdout_raw_tee`)

Every path above hands you **decoded** text: bytes go through `encoding_rs`,
lines are split on the terminator, and CRLF is normalized. That is exactly right
for text logic — but a **transparent wrapper** needs the child's bytes *unaltered*.
Decoding mangles four things a passthrough must preserve: non-UTF-8 stdout (binary
from `git archive`, `tar -cz -`, `ffmpeg … -`) becomes U+FFFD; CRLF is rewritten
and a missing final newline is fabricated; an unterminated prompt (`Password: `)
sits in the decode buffer until EOF and reads as a hang; and a line past the byte
cap vanishes from the transcript entirely.

`stdout_raw_tee(writer)` / `stderr_raw_tee(writer)` are the byte plane, orthogonal
to `stdout_tee`. Each chunk is written to `writer` **exactly as read from the
pipe** — before decoding, before line splitting — so it is byte-for-byte the
child's output, in order: non-UTF-8 bytes survive, CRLF is untouched, the tail is
never padded, an unterminated chunk arrives the instant it is read, and even a
policy-dropped line is teed whole. It is **strictly additive** — the decoded line
path (capture, `on_*_line`, `stdout_tee`, the buffer policy) is unchanged, and
both tees can run at once, each seeing its own view. The write is awaited on the
capture pump, so a slow raw sink applies the same backpressure as the line tee (no
unbounded in-flight buffer), and it is flushed at stream end.

```rust,no_run
use processkit::Command;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Transparent passthrough: forward the child's *exact* stdout bytes to a file
    // (binary-safe — no decoding, no CRLF rewrite, no lost tail), retaining
    // nothing in memory. `drain` streams the bytes through to the tee and returns
    // just the classified exit outcome.
    let exact = File::create("archive.tar").await?;
    let outcome = Command::new("git")
        .args(["archive", "HEAD"]) // writes a binary tar to stdout
        .stdout_raw_tee(exact)
        .start()
        .await?
        .drain()
        .await?;
    println!("archive written: {outcome:?}");
    Ok(())
}
```

The raw tee fires from the line/streaming verbs (`output_string`, `start` +
`stdout_lines` / `events`, `wait` / `drain`). It is a **no-op** under
`stdout(Inherit)` / `stdout(Null)` / a `stdout_file` redirect — none run a capture
pump — and under `output_bytes`, whose own return value already *is* the exact raw
stdout. Reach for it when you need the raw bytes *alongside* decoded lines.

### Redaction at capture (`capture_policy`)

Every plane above either *observes* a line (`on_stdout_line`, `stdout_tee`) or
returns it verbatim (`stdout_raw_tee`, `output_bytes`). None can *change what is
retained*: a secret a child echoes — a passphrase prompt from an agent CLI, a
`--token` re-printed in a diagnostic — lands in the captured `ProcessResult`
word for word. `capture_policy` is the seam that shapes the backlog **before**
the line settles:

```rust,no_run
use std::borrow::Cow;
use processkit::{Command, CapturePolicy, OutputStream};

// A typed, named policy — introspectable in `Command`'s `Debug`, not an opaque
// closure. It rewrites secrets as each line is captured.
struct RedactTokens;
impl CapturePolicy for RedactTokens {
    fn name(&self) -> &str { "redact-tokens" }
    fn on_capture<'a>(&self, _stream: OutputStream, line: &'a str) -> Cow<'a, str> {
        if let Some(i) = line.find("token=") {
            Cow::Owned(format!("{}token=[REDACTED]", &line[..i]))
        } else {
            Cow::Borrowed(line) // unchanged: retained verbatim, no allocation
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("agent-cli")
        .arg("login")
        .capture_policy(RedactTokens)
        .output_string()
        .await?;
    // `out.stdout()` — and a streamed `stdout_lines` / `events` — carry
    // the redacted text; the raw secret never reached the buffer.
    assert!(!out.stdout().contains("token=hunter2"));
    Ok(())
}
```

Return `Cow::Borrowed(line)` to keep a line unchanged (no allocation), a
`Cow::Owned` to rewrite it, or an empty string to blank the content while
keeping the line's slot and the exact line counter. The policy is handed the
`OutputStream` each line came from, so one implementation can treat stdout and
stderr differently. This completes the crate's secret-hygiene story — a cassette
stores env *names* only, `Debug` redacts env *values*, and `capture_policy`
scrubs secrets a child prints — see
[Running untrusted children](untrusted-children.md#4-keep-the-environment-hermetic).

**Scope.** The seam shapes the capture backlog only — `output_string` /
`ProcessResult` and the streaming verbs. The observing `on_*_line` handlers, the
decoded `stdout_tee` / `stderr_tee`, the byte-plane `stdout_raw_tee`, and the raw
stdout of `output_bytes` are **independent** and see the line un-redacted; if you
also tee to a log, redact in that sink too. A line past an `OutputBufferPolicy`
byte cap is never assembled, so — like the handlers/tee — it never reaches the
policy. *How much* is retained stays `OutputBufferPolicy`'s job (the two compose:
the policy shapes each retained line's content, the buffer policy bounds how many
survive). A policy that panics **fails closed** — the offending line is blanked,
never leaked raw.

## The full lifecycle as one stream (`events()`)

`stdout_lines()` gives you stdout; `events()` gives you the child's **whole
lifecycle** as one ordered, typed stream — the single asynchronous source a TUI,
dashboard, or supervisor wants, instead of stitching together separate channels
for "started", output, and "exited":

```text
Started { pid } → interleaved Stdout / Stderr lines → Exited(outcome)
```

- **`ProcessEvent::Started { pid }`** leads the stream, emitted as soon as the
  pid is known (before any output). `pid` is `None` for a scripted double.
- **`ProcessEvent::Stdout(line)` / `Stderr(line)`** carry each decoded
  [`OutputLine`] (read `line.text()`), the two streams polled fairly so neither
  starves the other.
- **`ProcessEvent::Exited(outcome)`** ends the stream, carrying the run's
  [`Outcome`] — the same value `finish()` reports.

The enum is `#[non_exhaustive]`: a `match` needs a `_` arm, and future kinds
(e.g. the graceful-teardown transitions) can be added without a breaking change.
`ProcessEvent::name()` gives a stable `"started"` / `"stdout"` / `"stderr"` /
`"exited"` tag for logging or serialization.

**Drive the stream and the finisher together.** The terminal `Exited` is
delivered when the run is *reaped* — which is what `finish()` (or `wait()`) does.
So poll the stream and that finisher **concurrently** (e.g. with `tokio::join!`),
rather than draining the stream to completion first and only then calling
`finish()` — the stream would park waiting for an `Exited` no one is yet
producing. After the stream ends, `finish()`'s `stderr` is empty because stderr
was delivered to you as `Stderr` events.

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, ProcessEvent};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("deploy").start().await?;

    let mut events = run.events()?;
    let render = async {
        while let Some(ev) = events.next().await {
            match ev {
                ProcessEvent::Started { pid } => eprintln!("started {pid:?}"),
                ProcessEvent::Stdout(line) => println!("{}", line.text()),
                ProcessEvent::Stderr(line) => eprintln!("! {}", line.text()),
                ProcessEvent::Exited(outcome) => eprintln!("exited {outcome:?}"),
                _ => {} // non_exhaustive: future event kinds
            }
        }
    };
    // Poll the event stream and the reaping `finish()` together.
    let (_, finished) = tokio::join!(render, run.finish());
println!("run finished: {:?}", finished?.outcome);
Ok(())
}
```

Run the same pattern without external dependencies in
[`examples/lifecycle_events.rs`](https://github.com/ZelAnton/ProcessKit-rs/blob/main/examples/lifecycle_events.rs).

[`OutputLine`]: https://docs.rs/processkit/latest/processkit/struct.OutputLine.html
[`Outcome`]: https://docs.rs/processkit/latest/processkit/enum.Outcome.html

## Interactive stdin

Conversational tools — write a request, read the response, repeat. Keep stdin
open with `keep_stdin_open()`, take the writer with `take_stdin()`:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, Finished, Outcome};

// `ProcessStdin`'s writer methods return `std::io::Result`; `Box<dyn Error>`
// mixes them with the crate's `Result` (or `.map_err(processkit::ErrorReason::Io)?`).
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `bc` evaluates each stdin line and prints the result.
    let mut run = Command::new("bc").keep_stdin_open().start().await?;
    let mut stdin = run.take_stdin().expect("stdin was kept open");
    let mut answers = run.stdout_lines()?;

    stdin.write_line("2 + 2").await?;             // sends a line + Enter, flushed
    println!("= {}", answers.next().await.unwrap());

    stdin.write_line("6 * 7").await?;
    println!("= {}", answers.next().await.unwrap());

    stdin.finish().await?;                        // send EOF — bc exits
    let Finished { outcome, .. } = run.finish().await?;
    assert_eq!(outcome, Outcome::Exited(0));
    Ok(())
}
```

`ProcessStdin` offers `write(&[u8])`, `write_line(&str)` (terminal Enter + flush),
`flush()`, and `finish()` (EOF). Dropping the writer requests the same EOF: while
the PTY session and its async runtime remain live, the backend retains that
request through temporary input backpressure and delivers it after the child
resumes reading. `finish()` makes delivery explicit and awaitable, including its
I/O error; dropping has no error channel. Dropping the whole `RunningProcess`
tears the child tree down, so neither EOF form promises delivery after the
session/runtime is already irreversibly ending. `write_line` sends `\n` to a pipe
or Unix PTY and `\r` to a Windows ConPTY, where a lone LF is Ctrl-J rather than
the Enter key; use `write` when you need byte-exact input.

**Avoid the full-duplex deadlock.** A child's stdout pipe has a finite OS
buffer; once it fills, the child blocks *writing* stdout until something reads
it. If you push a large interactive stdin while nothing drains the child's
stdout, the child stops reading stdin (blocked on stdout), your `write` parks
waiting for stdin buffer space, and neither side progresses. The `bc` example
above is safe because it interleaves one write with one read; when you both feed
a sizable stdin **and** the child produces output, drain `stdout_lines` from one
task while writing stdin from another. The same rule applies to a PTY's
single, full-duplex master: awaiting one large write while no sink reads merged
output can deadlock. (The non-interactive `Stdin::from_*` sources are safe —
the crate writes them on a background task that runs concurrently with the
output pumps.)

For *one-directional* streamed input (a channel, a file tail) you don't need
interactivity — give the command `Stdin::from_lines(stream)` /
`Stdin::from_reader(reader)` and let the background writer feed it; see the
[stdin source table](commands.md#standard-input).

### PTY dialog: wait for a prompt, then answer

A terminal prompt is usually an un-terminated tail (`Password: `), not a line.
Combine `use_pty`, `keep_stdin_open`, and `wait_for_output` to drive an
expect-style exchange without guessing when the child is ready:

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut run = Command::new("terminal-login")
        .use_pty()
        .pty_size(100, 30)
        .keep_stdin_open()
        .start()
        .await?;

    run.wait_for_output(
        |tail| tail.ends_with("Password: "),
        Duration::from_secs(10),
    )
    .await?;
    run.take_stdin()
        .expect("PTY stdin was kept open")
        .write_line("secret supplied by a protected source")
        .await?;

    run.finish().await?;
    Ok(())
}
```

The self-contained
[`examples/pty_dialog.rs`](https://github.com/ZelAnton/ProcessKit-rs/blob/main/examples/pty_dialog.rs)
also demonstrates live resize; run it with `--features pty`.

On Unix, the PTY slave has echo disabled so the answer is not copied into the
merged capture. That guarantee is Unix-only; ConPTY offers no portable
equivalent, so a Windows caller sending secrets should avoid retaining or
logging the transcript. The prompt wait itself is byte-driven on both
platforms. Signal-driven terminal behavior is separate: on Unix, `use_pty`
creates a controlling terminal so terminal signals such as the `SIGWINCH`
raised by `resize_pty` reach the child as described below.

### PTY output destination

A PTY has one merged output master, exposed as **piped logical stdout**. Keep
both output destinations at their default `StdioMode::Piped`: capture,
`stdout_lines`, stdout handlers, and stdout tees then receive the merged bytes,
while `ProcessResult::stderr` stays empty. There is no independent stderr
destination after the terminal has merged the child descriptors.

Accordingly, `use_pty` rejects stdout `Inherit`, `Null`,
`stdout_file`/`stdout_file_append`, and any separate stderr `Inherit`, `Null`,
`stderr_file`/`stderr_file_append` destination with
`ErrorReason::Unsupported`. The check runs before opening, creating, or
truncating a redirect file and before spawning the child on both Unix and
Windows. This makes the unsupported combinations fail visibly instead of
creating an unused file or secretly draining the real PTY master to a discard
sink. Use ordinary pipe mode when those per-descriptor destinations are needed.

### PTY window size and live resize

Under `use_pty` the child runs on a real pseudo-terminal, and terminal-aware
tools care about its **size**: it drives line wrapping, TUI/progress layout, and
pager behavior. Set the initial geometry with `pty_size(cols, rows)` (default
80×24), and change it on a *running* session with
`RunningProcess::resize_pty(cols, rows)` — the way you propagate a host window
resize down to the child. At spawn, `COLUMNS`/`LINES` match that initial
geometry (and Unix defaults `TERM=xterm-256color`); explicit `env`/`env_remove`
operations override those values:

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the terminal at 120×40 instead of the 80×24 default.
    let mut run = Command::new("htop").use_pty().pty_size(120, 40).start().await?;
    let mut screen = run.stdout_lines()?;

    // …later, the host window grows — tell the child so it re-renders.
    run.resize_pty(160, 50)?;

    while let Some(line) = screen.next().await {
        // render `line`…
        let _ = line;
        break;
    }
    Ok(())
}
```

Dimensions must be non-zero. Windows ConPTY represents each axis in a signed
16-bit `COORD`, so `1..=32767` is accepted there and larger values fail with
`ErrorReason::Io` (`InvalidInput`) before a child is spawned or a live terminal
is resized. Unix `winsize` uses `u16` fields and accepts the full non-zero range
through `65535`. Values are never clamped: a successful spawn therefore keeps
the applied terminal geometry and synthesized `COLUMNS`/`LINES` aligned, while
explicit environment operations retain their documented precedence.

`resize_pty` works while you still hold the handle — typically interleaved with
driving an owned `stdout_lines()`/`events()` stream and the `take_stdin()` writer
of a live session. It returns `ErrorReason::Unsupported`
— never a panic or a silent no-op — on a run that is **not** `use_pty` (there is
no terminal to size) or once the child has **exited**. Platform delivery differs:
on **Unix** the resize (`TIOCSWINSZ`) raises `SIGWINCH` on the child immediately;
on **Windows** `ResizePseudoConsole` has no signal, so a console client observes
the new size on its next console query and conhost may reflow a little later. On a
non-`use_pty` command `pty_size` is a documented no-op (nothing to size). See
[platform support](platform-support.md#pty-mode-use_pty-the-pty-feature).
An invalid resize is checked before the backend call and leaves the live PTY
usable for a later valid resize.
Because a running process's environment is immutable, live resize does not
rewrite `COLUMNS`/`LINES`; applications learn the new size through the platform
resize mechanism.

### PTY output hygiene: line framing and VT sanitization

A PTY child writes like a terminal, which two things about `use_pty` handle so a
line-oriented consumer gets sensible output — one automatic, one opt-in:

- **Framing is `\r`-aware by default under `use_pty`.** Terminal tools draw
  progress by redrawing a line in place with a bare `\r` (no `\n` until the end).
  Under the default `Newline` framing that whole progress stream is *one*
  ever-growing line that only surfaces at EOF; so `use_pty` makes the **effective**
  default `line_terminator`
  `CarriageReturn`, and each redraw becomes its own frame/line live. This only
  changes *where* lines split (a `\r\n` still counts as one terminator), so it is
  the safe default for the mode. An explicit `line_terminator(...)` — even
  `Newline` — always wins, so you can pin the framing if you need to.
- **Escape sanitization is opt-in.** Agentic CLIs spray VT/ANSI escapes (colors,
  cursor moves, alternate-screen switches, OSC window-title/hyperlink codes) into
  their merged output, so `output_string`, `wait_for_line`/`first_line`, and the
  streaming verbs otherwise carry `\x1b[31m…`-mucked strings. Turn on
  `Command::sanitize_vt()`
  to strip those sequences (and lone control codes, keeping tabs) from the capture
  backlog. It is opt-in because it is *destructive* — it removes bytes from the
  captured output — unlike the non-destructive framing default.

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, LineTerminator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A PTY agent CLI: make the progress-frame contract explicit and opt into
    // de-escaping, so every captured line is readable text.
    let mut run = Command::new("agent")
        .use_pty()
        .pty_size(120, 30)
        .line_terminator(LineTerminator::CarriageReturn)
        .sanitize_vt()
        .start()
        .await?;
    let mut lines = run.stdout_lines()?;
    while let Some(line) = lines.next().await {
        // `line` is a clean, de-escaped frame — safe for a `contains(...)` probe.
        let _ = line;
        break;
    }
    Ok(())
}
```

Because a PTY merges both child descriptors into its logical stdout, the
whole-command setters above are the clearest spelling. For a wrapper that can
also run over ordinary pipes, the per-stream variants let each real pipe keep
its own policy:

```rust,no_run
use processkit::{Command, LineTerminator};

fn command_for_pipes() -> Command {
    Command::new("agent")
        .stdout_line_terminator(LineTerminator::CarriageReturn)
        .stderr_line_terminator(LineTerminator::CarriageReturn)
        .stdout_sanitize_vt()
        .stderr_sanitize_vt()
}

fn command_for_pty() -> Command {
    Command::new("agent")
        .use_pty()
        .pty_size(120, 30)
        .line_terminator(LineTerminator::CarriageReturn)
        .sanitize_vt()
}

fn main() {
    let _ = (command_for_pipes(), command_for_pty());
}
```

In PTY mode, `stderr_line_terminator` and `stderr_sanitize_vt` have no separate
stream to act on: the child stderr bytes have already joined stdout on the
master. Likewise, `on_stderr_line` is not delivered and the final stderr
capture is empty. Use the stdout/whole-command settings for PTY output, or use
pipes when preserving stream identity is required.

Sanitization shapes **only the capture backlog** — exactly the boundary
`capture_policy`
draws. The per-line handlers (`on_stdout_line`), the decoded `stdout_tee`, the
byte-plane `stdout_raw_tee`, and `output_bytes` are independent and keep seeing the
raw, escape-laden bytes; if you also tee to a log and want it clean, sanitize in
that sink. When combined with `capture_policy`, sanitization runs **first**, so a
secret-scrubbing policy matches on already-cleaned text rather than a token a color
escape could split mid-word. Set it per stream with `stdout_sanitize_vt()` /
`stderr_sanitize_vt()` when only one stream needs it. See
[platform support](platform-support.md#pty-mode-use_pty-the-pty-feature) for the
per-platform PTY table.

## Readiness probes

"Start a server, then use it" needs *ready*, not merely *started*. Readiness
probes replace the arbitrary sleep, each bounded by its own deadline:

```rust,no_run
async fn health_check() -> bool { true }
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("my-server").start().await?;

    // 1. A line on stdout (returns the matching line):
    let banner = run
        .wait_for_line(|l| l.contains("listening on"), Duration::from_secs(10))
        .await?;

    // 2. A pidfile or readiness sentinel appearing:
    run.wait_for_path("run/my-server.ready", Duration::from_secs(10))
        .await?;

    // 3. A TCP port accepting connections:
    run.wait_for_port("127.0.0.1:8080".parse().unwrap(), Duration::from_secs(10))
        .await?;

    // 4. A plain HTTP endpoint returning an expected status:
    run.wait_for_http(
        "127.0.0.1:8080".parse().unwrap(),
        "/healthz",
        |status| (200..300).contains(&status),
        Duration::from_secs(10),
    )
    .await?;

    // 5. A Unix domain socket accepting connections (Unix only):
    run.wait_for_socket("/tmp/my-server.sock", Duration::from_secs(10))
        .await?;

    // 6. A Windows named pipe accepting clients (Windows only):
    run.wait_for_pipe("my-service", Duration::from_secs(10)).await?;

    // 7. Any async predicate (an HTTPS/body or metadata check, …):
    run.wait_for(|| async { health_check().await }, Duration::from_secs(10))
        .await?;

    // ready — use the server…
    let _ = banner;
    Ok(())
}
```

If a tool writes its banner to stderr, use the matching counterpart instead:

```rust,ignore
let banner = run
    .wait_for_stderr_line(|line| line.contains("backend ready"), Duration::from_secs(10))
    .await?;
```

Choose one consuming line probe for a handle: while it hands you the selected
stream, the other stream is already being drained in the background for
liveness and later capture.

Probe semantics, deliberately uniform:

- A probe that can't pass within its deadline fails with **`ErrorReason::NotReady`**
  — distinct from `ErrorReason::Timeout`, which is the run's own deadline.
- A probe also fails *fast* once readiness can no longer happen: the child
  exits, or (for `wait_for_line`) its stdout closes — no waiting out a 30s
  deadline on a dead server.
- A failed probe **never kills the child.** You decide: retry, log and
  continue, or tear down.
- All probes background-drain stdout/stderr while they poll, so a
  child with a large startup burst can't stall in `write()` on a full OS pipe
  buffer. `wait_for_line` and `wait_for_stderr_line` consume the selected stream
  up to (and including) the match — continue with `finish` (whose stderr omits
  lines already consumed by `wait_for_stderr_line`). `wait_for_path` /
  `wait_for_port` / `wait_for_http` / `wait_for_socket` / `wait_for_pipe` / `wait_for`
  drain the same way but never hand any of it back mid-probe; `wait` /
  `output_string` afterward still see the full captured output, but
  `output_bytes` or a fresh `stdout_lines` / `events` call do not
  compose with any readiness probe that started a line pump (same as calling `wait_for_line`
  first).
- `wait_for_socket` uses a real connection attempt, not just a socket-file
  existence check, and returns `ErrorReason::Unsupported` immediately on platforms
  without AF_UNIX (including Windows).
- `wait_for_path` is deliberately existence-only: a file or directory counts.
  Use `wait_for` with `tokio::fs::metadata` when readiness also depends on type,
  size, permissions, or other metadata.
- `wait_for_pipe` accepts a bare name or a fully-qualified `\\.\pipe\...` path.
  It returns `ErrorReason::Unsupported` off Windows. A busy pipe counts as ready:
  all server instances being occupied still proves the endpoint is serving.
- `wait_for_http` sends a minimal HTTP/1.1 GET and reads only a bounded status
  line. It is plain HTTP only: no TLS, redirect following, response bodies, or
  authentication policy. Redirects count only if your status predicate accepts
  them; use `wait_for` with your own HTTP client for HTTPS or richer checks.

## Prompt-aware waiting (`wait_for_output`)

`wait_for_line` only ever sees **complete lines** — text with its terminator. An
interactive prompt is the opposite: `Password: `, `passphrase: `, `(y/N) `, a
REPL `>>> ` are all written **without** a trailing newline and then *blocked on*,
waiting for you to answer. Such a prompt never becomes a line, so `wait_for_line`
(and `stdout_lines`) cannot see it until the stream ends — the "wait for the
prompt, then answer it" dialog can't be expressed line by line.

`wait_for_output(predicate, within)` closes that gap. Its stderr counterpart is
`wait_for_stderr_output`; both are `expect`-style
primitive (in the spirit of `rexpect`): it matches the child's **current
un-terminated output tail** — the partial line the pump has decoded but not yet
split — and hands it back so you can answer over `take_stdin()`. PTY is the
motivating case (a merged terminal stream is full of un-terminated prompts), but
it is **not** PTY-specific — a plain piped run benefits too (e.g. a progress
meter that rewrites one line without a newline).

Unlike the line probes, these methods **do not consume** their selected stream
and are **repeatable**: a whole session can be a sequence of
`wait_for_output` → answer turns, one per prompt.

```rust
use processkit::{Command, ProcessRunner};
use processkit::testing::{Reply, ScriptedRunner};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A hermetic stand-in for a tool that prompts, reads a secret, then
    // continues — no real subprocess. `Reply::dialog` emits the prompt, waits
    // for the answer over stdin, then emits the continuation.
    let runner = ScriptedRunner::new()
        .fallback(Reply::dialog("Password: ", "granted, welcome> "));

    let mut run = runner
        .start(&Command::new("login").keep_stdin_open())
        .await?;

    // Wait for the un-terminated `Password: ` prompt (a whole line never arrives).
    let prompt = run
        .wait_for_output(|tail| tail.ends_with("Password: "), Duration::from_secs(5))
        .await?;
    assert!(prompt.contains("Password:"));

    // Answer it, then wait for the next (also un-terminated) prompt.
    run.take_stdin().expect("interactive stdin").write_line("s3cret").await?;
    let cont = run
        .wait_for_output(|tail| tail.contains("welcome>"), Duration::from_secs(5))
        .await?;
    assert!(cont.contains("welcome>"));

    run.finish().await?;
    Ok(())
}
```

Semantics, and how it differs from `wait_for_line`:

- **Un-terminated tail, not lines.** `wait_for_output` matches the live partial
  line; once the child terminates it with a newline it becomes a *complete line*
  (seen by `wait_for_line` / `stdout_lines`, not here). So answer a prompt before
  waiting for the next, and reach for `wait_for_line` when you want whole lines.
- **Non-consuming and repeatable.** It only peeks while the background pump keeps
  draining stdout under your `OutputBufferPolicy`; the handle stays usable for
  `take_stdin`, further `wait_for_output` calls, and `finish`. `wait_for_line`
  *takes* the stdout stream and so is one-shot.
- **Raw, not redacted.** The predicate (and the returned fragment) see the tail
  **before** any `capture_policy` redaction (see [Redaction at capture](#redaction-at-capture-capture_policy)) —
  the same observation category as `handler` / `tee` / `raw_tee` / `output_bytes`,
  not the redacted backlog `wait_for_line` and `ProcessResult` draw from. A
  partial line can't be run through a per-line redactor, and a prompt
  is a synchronization token you match verbatim. The retained/`finish`ed output
  stays redacted independently — just match on prompts, not on secret-bearing
  partial text.
- **Same probe deadline.** It fails with `ErrorReason::NotReady` when `within`
  elapses (or stdout closes) with no match, never killing the child or arming the
  run's `timeout` watchdog — exactly like the readiness probes above.
- **Choose the actual pipe.** Use `wait_for_stderr_line` /
  `wait_for_stderr_output` when the tool reports readiness on stderr. Both keep
  stdout draining in the background. A PTY exposes one merged stream, so its
  stdout methods observe output originally written to either child stream.

## Racing children with `wait_any`

The free function `wait_any` races several running processes and reports
whichever exits first — the natural primitive for "restart whatever died" or
"first answer wins":

```rust,no_run
use processkit::{Command, ProcessGroup, wait_any};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let mut a = group.start(&Command::new("replica-a")).await?;
    let mut b = group.start(&Command::new("replica-b")).await?;

    let (index, outcome) = wait_any(&mut [&mut a, &mut b]).await?;
    println!("contender #{index} exited first with {outcome:?}");

    // Only borrows: the loser is still usable.
    let survivor = if index == 0 { &mut b } else { &mut a };
    Ok(())
}
```

`wait_any` takes `&mut` borrows, applies no timeout of its own (wrap it in
`tokio::time::timeout` to bound the race), and does no output pumping — drain
chatty children first or give them bounded
[buffer policies](commands.md#output-handling).

## Per-run telemetry

With the opt-in **`stats`** feature, a running child reports its own
resource usage, and `profile()` turns a whole run into a summary:

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let run = Command::new("crunch").start().await?;
    run.cpu_time();          // Option<Duration> — user+kernel so far
    run.peak_memory_bytes(); // Option<u64>

    // …or capture + sample on an interval until exit:
    let profile = Command::new("crunch")
        .start().await?
        .profile(Duration::from_millis(100))
        .await?;

    println!(
        "outcome={:?} wall={:?} cpu={:?} peak_rss={:?} avg_cpu_cores={:?} ({} samples)",
        profile.outcome,            // Exited(code) / Signalled(sig) / TimedOut
        profile.duration,
        profile.cpu_time,
        profile.peak_memory_bytes,
        profile.avg_cpu_cores(),    // cpu / wall — e.g. Some(1.7) ≈ 1.7 cores busy
        profile.samples,
    );
    Ok(())
}
```

These read the *child process itself* (not a whole tree — that's
[`ProcessGroup::stats`](process-groups.md#stats-and-sampling)), and
availability follows the platform: full CPU/memory on Windows and Linux,
`None` where the kernel doesn't account per-process cheaply — see
[Platform support](platform-support.md).

That boundary is why `RunProfile` carries no whole-tree counters — no I/O bytes,
no peak process count — even though `ProcessGroupStats` does. Those come from the
containment object, and a run has no whole-tree scope of its own to report them
under: started into a shared `ProcessGroup`, it shares that container with every
other run in it, and a container's counters cannot be split into per-run shares.
When you want a run's *tree*, give it a group of its own and read
[`ProcessGroup::stats`](process-groups.md#stats-and-sampling) — the same numbers,
named as the group's.

### Lifecycle narration (`tracing`)

With the opt-in **`tracing`** feature, a run narrates its lifecycle on the
`processkit` target — `spawn` (pid, mechanism), the graceful-teardown transitions
(`soft_signal` → `grace_started` → `drained` / `escalated` / `spared`, each in a
stable `phase` field), and `exited` — so a subscriber can stamp each transition the
instant the layer that observed it crossed it, and map it to an event of your own
protocol. The teardown transitions are narrated the same way for a streaming
handle's own `shutdown(grace)` as for a whole-group `ProcessGroup::stop`, so you get
one uniform timeline. It is observation only (never a control surface) and never
carries argv/env; for the same teardown facts as a typed *value* returned after the
fact, see
[`ShutdownReport`](process-groups.md#observing-the-teardown-stop-and-shutdownreport).

---

Next: [Pipelines](pipelines.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Supervision](supervision.md)
