# Running commands

[‹ docs index](README.md)

`Command` is the entry point of the runner layer: a builder describing *what*
to run and *how*, plus a family of consuming verbs that decide *what you get
back*. Every one-shot verb spawns the child into a fresh, private kill-on-drop
[process group](process-groups.md), so an early return, panic, or dropped
future can never leak a process tree.

- [Program, arguments, working directory](#program-arguments-working-directory)
- [Environment](#environment)
- [Standard input](#standard-input)
- [Output handling](#output-handling)
- [Timeouts and retries](#timeouts-and-retries)
- [Privileges and spawn flags](#privileges-and-spawn-flags)
- [Consuming verbs](#consuming-verbs)
- [Results and errors](#results-and-errors)

## Program, arguments, working directory

```rust,no_run
use processkit::Command;

let out = Command::new("git")
    .arg("log")                          // one at a time…
    .args(["--oneline", "-n", "10"])     // …or in bulk
    .current_dir("/path/to/repo")        // run there
    .run()
    .await?;
```

Arguments are passed as an array — there is **no shell** between you and the
child, so there is no quoting, no word-splitting, and no injection surface.
(When you actually want `a | b | c`, use a [pipeline](pipelines.md), which
connects the stages in-process instead of invoking a shell.)

The program name reaches the OS **verbatim** — two deliberate non-goals
(conveniences some libraries layer on, e.g. `duct`): a bare name is resolved
on `PATH` by the OS, never rewritten to `./name`; and `current_dir` does not
re-anchor a *relative* program path against the new directory — whether
`Command::new("./tool").current_dir(dir)` resolves `tool` relative to `dir`
is the platform's behavior (Unix: yes; Windows: the parent's directory may
win). Pass absolute program paths when combining the two.

For quick one-liners the free functions skip the builder:

```rust,no_run
let version = processkit::run("cargo", ["--version"]).await?;       // trimmed stdout, success required
let result  = processkit::output_string("git", ["status", "-s"]).await?;   // full ProcessResult
```

## Environment

Four builders compose, applied in a fixed order at spawn:

```rust,no_run
use processkit::Command;

Command::new("worker")
    .env("RUST_LOG", "debug")        // set one variable
    .env_remove("GIT_DIR")           // unset one inherited variable
    .run().await?;

// Allow-list mode: clear everything, copy only the named parent variables.
Command::new("sandboxed-tool")
    .inherit_env(["PATH", "HOME", "LANG"])
    .env("MODE", "ci")               // explicit env/env_remove still apply on top
    .run().await?;

// Scorched earth: the child starts with an empty environment.
Command::new("hermetic-tool").env_clear().run().await?;
```

`inherit_env` is the sandboxing middle ground: it implies `env_clear`, then
copies the listed variables *from the parent at each spawn* (so a retry sees
fresh values), and repeated calls accumulate names. A name the parent doesn't
have is skipped, not set to empty.

## Standard input

By default stdin is **closed at spawn** — the child reads EOF immediately and
can never hang waiting for input. Everything else is opt-in via
`stdin(Stdin::…)`:

| Source | Reusable on re-run? | Use for |
|---|---|---|
| `Stdin::empty()` | — | The default, explicit |
| `Stdin::from_string("…")` | ✅ | Text payloads |
| `Stdin::from_bytes(vec![…])` | ✅ | Binary payloads |
| `Stdin::from_iter_lines(["a", "b"])` | ✅ | Anything iterable; each item is written `\n`-terminated |
| `Stdin::from_file(path)` | ✅ (re-opened per run) | Large inputs streamed from disk |
| `Stdin::from_reader(reader)` | ❌ one-shot | Any `AsyncRead` — a socket, a decompressor, … |
| `Stdin::from_lines(stream)` | ❌ one-shot | Any `Stream<Item = String>` — a channel, a tail, … |

```rust,no_run
use processkit::{Command, Stdin};

let sorted = Command::new("sort")
    .stdin(Stdin::from_iter_lines(["banana", "apple", "cherry"]))
    .run()
    .await?;
assert_eq!(sorted, "apple\nbanana\ncherry");
```

The payload is written on a background task (so a large input can't deadlock
against the child's output) and the pipe is dropped afterwards to signal EOF.
The two *one-shot* sources are consumed by their first run: a retried or
cloned command reusing them **fails loud** the second time — re-running a
consumed `from_reader`/`from_lines` source is an `Error::Io` (`InvalidInput`)
at launch (D10), not a silent empty stdin. Prefer the reusable sources when
a command may run more than once.

For conversational, request/response stdin — write a line, read the answer,
repeat — use `keep_stdin_open()` and the streaming API instead: see
[Streaming & interactive I/O](streaming.md#interactive-stdin).

## Output handling

### Encodings

Output is decoded line by line, UTF-8 by default (invalid bytes become
`U+FFFD`, never an error). Legacy-encoding tools can override per stream:

```rust,no_run
use processkit::Command;

let out = Command::new("legacy-tool")
    .encoding(encoding_rs::SHIFT_JIS)          // both streams…
    // .stdout_encoding(…) / .stderr_encoding(…) // …or each its own
    .output_string()
    .await?;
```

(`processkit::Encoding` re-exports `encoding_rs::Encoding`, so any of its
encodings works — the single-byte and ASCII-compatible multibyte ones
(`WINDOWS_1252`, `GBK`, `SHIFT_JIS`, …) **and** the non-ASCII-compatible ones
(`UTF_16LE`/`UTF_16BE`): output is fed through one persistent decoder and split
on decoded newlines, so a `0x0A` byte inside a UTF-16 code unit is not mistaken
for a line break. A leading byte-order mark of the chosen encoding is stripped
once at the stream start.)

### Buffer policies — bounding memory on chatty children

Captured lines are held in memory; a multi-gigabyte log would normally grow
the buffer to match. `output_buffer` bounds *retention* (the pipe is always
fully drained, so the child never blocks):

```rust,no_run
use processkit::{Command, OutputBufferPolicy, OverflowMode};

let tail = Command::new("verbose-build")
    .output_buffer(OutputBufferPolicy::bounded(1_000)) // keep the newest 1000 lines
    .output_string()
    .await?;

// …or keep the head instead of the tail:
let head_policy = OutputBufferPolicy::bounded(1_000).with_overflow(OverflowMode::DropNewest);
```

`DropOldest` (the default) keeps a rolling tail; `DropNewest` freezes the
head. `bounded(0)` retains nothing — useful when a line handler (below) is the
real consumer. Under a *line* cap, dropped or not, **every** line still feeds
the handlers and the line counters.

The line cap alone does not bound memory — one enormous newline-free "line"
(`base64 -w0`) is held whole. Add `with_max_bytes` to cap the *retained bytes*
too (either ceiling, or both); the byte cap also bounds the pump's in-flight
assembly buffer, so a never-terminated flood can't exhaust memory. One
consequence: a line whose own length exceeds the byte cap can't be assembled, so
it is dropped **whole** — counted, but **not** delivered to a per-line handler or
`stdout_tee` (don't set a byte cap if a tee must see arbitrarily long lines):

```rust,no_run
# use processkit::{Command, OutputBufferPolicy};
let policy = OutputBufferPolicy::unbounded().with_max_bytes(8 << 20); // 8 MiB ring
let strict = OutputBufferPolicy::fail_loud(10_000).with_max_bytes(8 << 20); // error on either
```

`fail_loud` makes the ceiling **error** instead of dropping: the run fails with
`Error::OutputTooLarge` once the cumulative output (lines *or* bytes) crosses the
cap — even when a streaming consumer is draining lines as they arrive. It bounds
memory, not wall-time, so pair it with `timeout` against a flooding child.

Even under a *drop* policy (`DropOldest`/`DropNewest`), the checking verbs that
hand back stdout as if complete — `run`, `parse`, `try_parse` — **refuse**
silently-truncated output (B12): if the policy dropped lines they fail with
`Error::OutputTooLarge` rather than feed a parser a truncated tail. The lenient
capture verbs (`output_string` / `output_bytes`) are unaffected — they return
the partial result with `truncated()` set for you to inspect.

### Line handlers — tee output as it arrives

`on_stdout_line` / `on_stderr_line` run a callback on each decoded line *in
addition to* capture or streaming — logging, progress bars, metrics:

```rust,no_run
use processkit::Command;

let result = Command::new("cargo")
    .args(["build", "--release"])
    .on_stderr_line(|line| eprintln!("[build] {line}"))
    .output_string()
    .await?;
```

The handler runs on the read pump — keep it cheap. The contract is forgiving
and precisely specified:

- **A panicking handler does not poison the run.** The panic is caught, the
  handler is disabled for the rest of the run (surfaced as a `tracing` warn
  when that feature is on), and pumping continues — the final result still
  carries **every** line. You can safely re-export this callback seam to your
  own users without auditing their closures.
- **Ordering:** invocations are FIFO within a stream; there is no ordering
  between stdout and stderr handlers (two independent pumps). On the
  consuming verbs, **all handler calls happen-before the awaited future
  resolves** — finalize a progress bar the moment the call returns. (One
  documented exception: a leaked pipe held open past the child's death is cut
  off after a bounded teardown grace.)
- Handlers are **hermetically testable**: `ScriptedRunner` replays canned
  output through them — see
  [Testing → scripting replies](testing.md#scripting-replies).

For a ready-made tee to an async sink — a file, socket, or any
[`tokio::io::AsyncWrite`] — reach for `stdout_tee` / `stderr_tee` instead of
hand-writing a handler. Each decoded line is written to the sink (plus a `\n`)
as it is produced, **awaited on the pump** so a slow sink applies backpressure
(the pump slows, the pipe fills, the child blocks) rather than blocking the
runtime; a write error disables the tee with a `tracing` warn instead of being
swallowed. It runs **independently** of `on_stdout_line` — set both and both
fire per line.

## Timeouts and retries

```rust,no_run
use processkit::{Command, Error};
use std::time::Duration;

let out = Command::new("flaky-network-tool")
    .timeout(Duration::from_secs(30))                 // kill the tree at the deadline
    .retry(3, Duration::from_millis(200), |e| {       // up to 3 attempts total
        matches!(e, Error::Timeout { .. })            // …but only retry timeouts
    })
    .run()
    .await?;
```

- **`timeout`** kills the whole process tree at the deadline. On the capturing
  verbs the expiry is *captured* (`ProcessResult::timed_out`), on the
  success-checking verbs it *raises* `Error::Timeout` — the full decision
  table lives in [Timeouts, retries & cancellation](timeouts-and-cancellation.md).
- **`retry`** applies to the success-checking verbs only (`run`, `exit_code`,
  `probe`, and `ProcessRunnerExt::checked`); the classifier sees the typed
  error and decides. The non-erroring `output_string` path never retries.

## Privileges and spawn flags

Spawn-time controls for sandboxing and service launch:

```rust,no_run
use processkit::Command;

// Unix: drop privileges (uid + gid + supplementary groups) and detach.
Command::new("worker")
    .gid(1000)            // applied before uid (a gid change needs privilege)
    .groups([1000])       // replace the inherited (often root's) supplementary groups
    .uid(1000)            // dropped last
    .setsid()             // new session: survives the controlling terminal
    .run().await?;

// Windows: no console window flashing up from a GUI app.
Command::new("helper").create_no_window().run().await?;

// Hardening: take the direct child down even if THIS process is SIGKILLed
// (Drop never runs). Windows has this for free; Linux arms PDEATHSIG.
Command::new("worker").kill_on_parent_death().start().await?;
```

`uid` / `gid` / `groups` / `setsid` are POSIX-only — on Windows the run
fails with `Error::Unsupported` rather than silently skipping a privilege drop.
A correct drop sets all three of `uid`/`gid`/`groups`: dropping the uid alone
leaves the child holding the parent's (often root's) supplementary groups.
`create_no_window` is a harmless no-op outside Windows.
`kill_on_parent_death` is best-effort by design: guaranteed on Windows
(regardless of the knob), direct-child-only on Linux, unavailable on
macOS/BSD — the graceful-exit guarantee via `Drop` holds everywhere either
way. Containment is preserved in every combination; the platform fine print
(the Linux cgroup × `uid` interaction, `setsid` × process-group coordination,
the pdeathsig thread caveat) is collected in
[Platform support](platform-support.md#caveats).

**Interactive auth / TTY.** processkit wires **pipes**, not a pseudo-terminal,
so a tool that *demands* a tty — an `ssh`/`sudo` **password** prompt, some
credential helpers — won't get one (PTY support is not implemented; the
trade-off is recorded in `decisions/permissions-privileges-pty-network.md`). Drive
such tools **non-interactively** instead: key-based auth, `ssh -o
BatchMode=yes`, `GIT_SSH_COMMAND` / `GIT_TERMINAL_PROMPT=0`, or feed a known
answer over [interactive stdin](streaming.md#interactive-stdin). Conversational
tools that read stdin without needing a tty already work today via
`keep_stdin_open` + `stdout_lines`.

## Consuming verbs

| Verb | Returns | Non-zero exit | Timeout | Use when |
|---|---|---|---|---|
| `output_string()` | `ProcessResult<String>` | captured | captured (`timed_out`) | You want to inspect the outcome yourself |
| `output_bytes()` | `ProcessResult<Vec<u8>>` | captured | captured | Binary stdout (images, archives, …) |
| `run()` | trimmed stdout `String` | `Error::Exit` | `Error::Timeout` | "Give me the answer or fail" |
| `exit_code()` | `i32` | the code, `Ok` | `Error::Timeout` | The code *is* the answer |
| `probe()` | `bool` | `0`→`true`, `1`→`false`, else `Error::Exit` | `Error::Timeout` | Predicate commands: `git diff --quiet`, `grep -q` |
| `first_line(pred)` | `Option<String>` | — (stream-based) | `Error::Timeout` | Grab one matching line, kill the rest |
| `start()` | live `RunningProcess` | — | bounds the stream | [Streaming, interactive I/O, probes](streaming.md) |

```rust,no_run
use processkit::Command;

// probe(): the exit code as a boolean.
let clean = Command::new("git").args(["diff", "--quiet"]).probe().await?;

// first_line(): stop as soon as the interesting line appears.
let first_match = Command::new("git")
    .args(["log", "--oneline"])
    .first_line(|l| l.contains("fix:"))
    .await?;
```

`first_line` returns `Ok(None)` when stdout closes without a match, and kills
the (private-group) child once it has its answer — you never wait out a long
log for one line. If the command's [`cancel_on`](timeouts-and-cancellation.md)
token has fired, it returns `Error::Cancelled` instead of `Ok(None)`, so a
readiness probe with a shutdown token can't misread cancellation as "the line
never appeared".

## Results and errors

The capturing verbs hand back a `ProcessResult`:

```rust,no_run
use processkit::Command;

let result = Command::new("git").args(["merge", "feature"]).output_string().await?;

result.code();         // Option<i32> — None = killed (timeout/signal), no code
result.signal();       // Option<i32> — the signal number (Unix), else None
result.is_success();   // code in ok_codes (default {0})
result.timed_out();    // the run's own deadline expired
result.outcome();      // the explicit three-way enum behind the accessors above
result.stdout();       // &str (or &[u8] from output_bytes)
result.stderr();       // &str
result.combined();     // stdout + stderr concatenated
result.diagnostic();   // stderr if non-empty, else stdout — the human-facing line
                       // (git/jj put "CONFLICT …" on stdout!)

// Opt into erroring whenever you're ready:
let ok = result.ensure_success()?; // Exit / Timeout / Signalled (signal-kill) as typed errors
```

When the three-way distinction matters, match on `Outcome` instead of
mentally decoding the `code()`/`timed_out()` pair:

```rust,no_run
use processkit::Outcome;

match result.outcome() {
    Outcome::Exited(0) => println!("clean"),
    Outcome::Exited(code) => println!("failed with {code}"),
    Outcome::Signalled(signal) => println!("killed by signal {signal:?}"),
    Outcome::TimedOut => println!("hit its deadline"),
    _ => {} // non_exhaustive: future dispositions
}
```

For a single query you usually don't need the `match` (and its
`#[non_exhaustive]` wildcard): `Outcome` carries the same `code()` /
`signal()` / `timed_out()` accessors as `ProcessResult`, so a bare `Outcome`
(from `RunningProcess::wait` or `Finished::outcome`) answers directly —
`outcome.code()`, `outcome.signal()`, `outcome.timed_out()`. There is no
`Outcome::is_success` (success is `ok_codes`-aware — use
`ProcessResult::is_success`).

The error enum is structured and `#[non_exhaustive]`:

| Variant | Meaning |
|---|---|
| `Error::Spawn { program, source }` | The program was located but the OS couldn't start it (permissions, a bad working directory, a Windows `.cmd`/`.bat` needing `cmd.exe`, …) — **not** `is_not_found()` |
| `Error::NotFound { program, searched }` | The program couldn't be located (the single "not found" representation — `is_not_found()` is true); `searched` is `Some(dirs)` for a bare-name `PATH` lookup, `None` otherwise |
| `Error::Exit { program, code, stdout, stderr }` | Non-zero exit, both streams attached in full (the `Display` message is bounded, but the fields carry the complete captured text for classification) |
| `Error::Signalled { program, signal, stdout, stderr }` | The process was killed by a signal (no exit code); `signal` carries the number on Unix, `None` elsewhere; the partial streams captured before the kill are attached (reach them via `diagnostic()`) |
| `Error::OutputTooLarge { program, line_limit, byte_limit, total_lines, total_bytes }` | A `fail_loud` buffer's line or byte ceiling was exceeded |
| `Error::Timeout { program, timeout, stdout, stderr }` | The run's own deadline killed it; whatever the run captured before the kill is attached — a hung tool's last stderr line tails the `Display` and is reachable via `diagnostic()` |
| `Error::NotReady { program, timeout }` | A [readiness probe](streaming.md#readiness-probes) gave up |
| `Error::Parse { program, message }` | A `try_parse` parser (on `Command`, `ProcessRunnerExt`, `CliClient`, or `Pipeline`) rejected the output (the `Display`/`Debug` of `message` is bounded to a 200-byte preview; the field carries the full text) |
| `Error::Stdin { program, source }` | Feeding the child's stdin failed for a non-broken-pipe reason on an *otherwise-successful* run (a louder failure — exit/signal/timeout — wins instead); a routine broken pipe never surfaces |
| `Error::CassetteMiss { program }` | (`record` feature) a cassette replay found no matching recording (stale/incomplete cassette) — kept distinct from a missing program, so `is_not_found()` is `false` |
| `Error::Unsupported { operation }` | The platform can't do what was asked (and silently skipping would be wrong) |
| `Error::Cancelled { program }` | the run's token was cancelled |
| `Error::ResourceLimit { message }` | (`limits` feature) a requested cap couldn't be enforced |
| `Error::Io(source)` | A low-level IO error from the crate's own machinery (driving a child, group control, cassette files) — never an arbitrary foreign `io::Error` (no blanket `From`, D13) |

`Error::diagnostic()` returns the most useful human-facing line out of a
failure that captured output — `Exit`, and (D12) `Timeout` / `Signalled` (the
partial streams of a hung-then-killed or crashed tool). Each of those variants'
one-line `Display` also appends a bounded excerpt of that diagnostic (the last
non-empty line, capped at 200 bytes), so a bare `eprintln!("{e}")` reads
`` `git` exited with code 2: fatal: boom `` — actionable in a log line without
dumping multi-KiB streams into it.

---

Next: [Streaming & interactive I/O](streaming.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Process groups](process-groups.md)
