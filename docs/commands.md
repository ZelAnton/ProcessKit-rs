# Running commands

[‹ docs index](README.md)

`Command` is the entry point of the runner layer: a builder describing *what*
to run and *how*, plus a family of consuming verbs that decide *what you get
back*. Every one-shot verb spawns the child into a fresh, private kill-on-drop
[process group](process-groups.md), so an early return, panic, or dropped
future can never leak a process tree.

- [Program, arguments, working directory](#program-arguments-working-directory)
- [Resolving a locally-installed tool: `prefer_local`](#resolving-a-locally-installed-tool-prefer_local)
- [Environment](#environment)
- [Standard input](#standard-input)
- [Redirecting output directly to a file](#redirecting-output-directly-to-a-file)
- [Output handling](#output-handling)
- [Timeouts and retries](#timeouts-and-retries)
- [Privileges and spawn flags](#privileges-and-spawn-flags)
- [Consuming verbs](#consuming-verbs)
- [Results and errors](#results-and-errors)

## Program, arguments, working directory

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("git")
        .arg("log")                          // one at a time…
        .args(["--oneline", "-n", "10"])     // …or in bulk
        .current_dir("/path/to/repo")        // run there
        .run()
        .await?;
    Ok(())
}
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
#[tokio::main]
async fn main() -> processkit::Result<()> {
    let version = processkit::run("cargo", ["--version"]).await?;       // trimmed stdout, success required
    let result  = processkit::output_string("git", ["status", "-s"]).await?;   // full ProcessResult
    Ok(())
}
```

## Resolving a locally-installed tool: `prefer_local`

`prefer_local` adds a directory to check **before** the system `PATH` when
resolving a bare-name `program` for this one run — for a project's own
`node_modules/.bin`, a `target/debug` build, or a vendored toolchain, without
hand-rolling a `PATH` override:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("eslint")
        .prefer_local("./node_modules/.bin")
        .arg("src/")
        .output_string()
        .await?;
    Ok(())
}
```

**Resolution order.** Repeated calls **accumulate**, in priority order: the
directory from the first call is probed first, then the second, and so on,
with the system `PATH` tried last as the final fallback:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    Command::new("tool")
        .prefer_local("./vendor/bin")   // checked first
        .prefer_local("./target/debug") // checked second
        .run()                          // then the system PATH
        .await?;
    Ok(())
}
```

Resolution reuses the exact same PATHEXT-aware lookup as the `PATH` search
(the same internal `probe_dir` helper — not a separate implementation), so a
`.exe`/`.cmd`/`.bat` on Windows is found under a `prefer_local` directory
exactly as it would be on `PATH`.

**Only a bare name is affected.** If the `program` passed to `Command::new` is
a path — absolute, or relative with a separator (`"./tool"`, `"../bin/x"`) —
`prefer_local` has no effect at all: the existing contract that such a program
is never looked up on `PATH` (or here) is unchanged.

**Interaction with `PATH`/`inherit_env`/`env`.** `prefer_local` only changes
*where the parent looks* to resolve the program for this one launch. It does
**not** rewrite or extend the `PATH` the child sees in its own environment —
that is governed entirely by `env`/`inherit_env`/`env_clear`, as usual. When
the program is found under a `prefer_local` directory, the child is simply
spawned via that resolved absolute path instead of the bare name; a
grandchild the program itself spawns does not inherit this reach — only the
one program named in this `Command` benefits.

**Interaction with `current_dir`.** A relative `prefer_local` directory (as in
the examples above) is probed against the *process's* actual current
directory, never against whatever is set via `current_dir` on the same
`Command`. The resolved match is then always turned into an absolute path
before being handed to the OS, so it can't later be reinterpreted against the
child's working directory once `current_dir` is set — unlike a relative-path
`program` passed straight to `Command::new`, which *is* subject to that
footgun (see [Program, arguments, working directory](#program-arguments-working-directory)
above).

**Diagnostics.** If resolution fails everywhere, `ErrorReason::NotFound`'s
`searched` field includes the `prefer_local` directories too — first, in
priority order, ahead of the `PATH` directories — so the diagnostic never
hides that they were checked.

## Preflight: resolve a program without running it

Sometimes you want to know whether an external tool is *available* before you
run it — a **doctor** check at startup, a friendly "is `git` installed?" error
up front — with **no** side effects. `resolve_program` locates a command's
program and returns its absolute path **without spawning** anything:

```rust,no_run
use processkit::Command;

fn main() -> processkit::Result<()> {
    // `which` is the crate-level shortcut for a bare tool.
    let git = processkit::which("git")?;   // Ok(/usr/bin/git) or Err(NotFound)
    println!("git lives at {}", git.display());

    // On a builder it honors that command's own `prefer_local` and env, so it
    // resolves exactly what a real run of that command would launch.
    let eslint = Command::new("eslint")
        .prefer_local("./node_modules/.bin")
        .resolve_program()?;
    println!("eslint lives at {}", eslint.display());
    Ok(())
}
```

**No divergence from a real run.** Resolution reuses the crate's *own*
launch-path logic — the same `PATH`/PATHEXT/execute-bit resolution and
`prefer_local` handling a spawn performs, not a second copy — so a
`resolve_program` hit is exactly the executable a run would launch, and a miss
is exactly the `ErrorReason::NotFound` (with the same `searched` diagnostic and
`is_not_found()` classification) a run would raise. A command that relocates the
child's `PATH` (`env`/`env_remove` of `PATH`, `env_clear`, `inherit_env`) is
resolved against that *effective child* `PATH`, so preflight still matches the
spawn.

It is a **synchronous**, cheap filesystem probe (a few `stat`s) — no async
runtime is required, and no process is ever started. Contrast `probe()`, which
*runs* the tool to read its exit code; `resolve_program` only *locates* it.

```rust,no_run
fn main() {
    match processkit::which("definitely-not-installed") {
        Ok(path) => println!("found: {}", path.display()),
        Err(e) if e.is_not_found() => eprintln!("tool not installed"),
        Err(e) => eprintln!("resolution error: {e}"),
    }
}
```

For a tool wrapped behind a `CliClient`, `CliClient::resolve_program()` does the
same for the client's program, honoring its env defaults.

## Environment

Four builders compose, applied in a fixed order at spawn:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
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
    Ok(())
}
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

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let sorted = Command::new("sort")
        .stdin(Stdin::from_iter_lines(["banana", "apple", "cherry"]))
        .run()
        .await?;
    assert_eq!(sorted, "apple\nbanana\ncherry");
    Ok(())
}
```

The payload is written on a background task (so a large input can't deadlock
against the child's output) and the pipe is dropped afterwards to signal EOF.
The two *one-shot* sources are consumed by their first run: a retried or
cloned command reusing them **fails loud** the second time — re-running a
consumed `from_reader`/`from_lines` source is an `ErrorReason::Io` (`InvalidInput`)
at launch (D10), not a silent empty stdin. Prefer the reusable sources when
a command may run more than once.

For conversational, request/response stdin — write a line, read the answer,
repeat — use `keep_stdin_open()` and the streaming API instead: see
[Streaming & interactive I/O](streaming.md#interactive-stdin).

### Inheriting the parent's stdin: `inherit_stdin()`

`inherit_stdin()` hands the child the parent's **own** standard input — it reads
directly from whatever this process's stdin is (a terminal, a file, a pipe)
rather than from a crate-managed pipe. It is the stdin counterpart of
`stdout(StdioMode::Inherit)` / `stderr(StdioMode::Inherit)`: the child *shares*
the parent stream instead of the crate mediating it.

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // `git commit` opens $EDITOR on the parent's terminal; the child talks to the
    // real tty directly. stdout/stderr are still captured as usual.
    Command::new("git").arg("commit").inherit_stdin().run().await?;
    Ok(())
}
```

Reach for it when a child must talk to the real terminal — `git commit` opening
`$EDITOR`, a tool prompting for a password or a yes/no — or to forward the
parent's piped stdin straight through. This covers the common non-tty-negotiating
interactive cases without the crate having to pump bytes; a tool that truly
demands a *tty* (not just inherited stdin) instead wants
[`use_pty`](#interactive-auth--tty) (the `pty` feature). Because the child reads
the parent's stdin directly, the crate neither feeds nor captures that input, and
`take_stdin()` returns `None` (as for a non-`keep_stdin_open` run). Capturing and
streaming the child's *output* is unaffected.

**Why a dedicated verb rather than a `Stdin::Inherit` source or a mode enum.**
For stdout/stderr the three `StdioMode` variants map cleanly onto one setter, but
stdin's "piped" case is not modeless — it *needs a payload* (which source? what
bytes?), already expressed by `stdin(Stdin::…)`, and its "null" case is
`Stdin::empty()`. Folding inheritance into that same `stdin(Stdin)` field would
make "inherit **and** a source" collapse to silent last-write-wins, impossible to
flag. A separate `inherit_stdin()` keeps the two intents in distinct fields so an
incompatible pairing is a **detectable, rejectable** error instead.

Accordingly, `inherit_stdin()` is **mutually exclusive** with either way the crate
would otherwise drive stdin — a configured `stdin(Stdin::…)` source (including an
explicit `Stdin::empty()`) or `keep_stdin_open()`'s interactive pipe. Setting
`inherit_stdin()` together with one of those is a contradiction (feed the child a
source *and* let it read the terminal?), so it is refused at the launch boundary
with a typed `ErrorReason::Io` (`InvalidInput`) — the same failure mode as re-running a
consumed one-shot source — rather than silently letting one win. Drop the other
stdin knob to resolve it. The refusal is enforced on the same launch seam the
hermetic test doubles route through, so a `ScriptedRunner` rejects the conflict
exactly as a live run does.

## Redirecting output directly to a file

`stdout_file(path)` and `stderr_file(path)` give the child a file descriptor at
spawn (`Stdio::from(File)`). They do **not** tee through a parent task: no output
is line-pumped, decoded, or retained in memory, and the child can keep writing if
the parent exits suddenly.

The plain builder creates the file when needed and **truncates** it for that
spawn. Use the explicit `*_file_truncate` spelling when choosing a mode in a
conditional expression, or `*_file_append` to preserve existing contents and
append each new child incarnation:

```rust,no_run
use processkit::{Command, Supervisor};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let service = Command::new("my-service")
        .stdout_file_append("service.log") // every Supervisor incarnation shares this log
        .stderr_file_append("service.log");

    Supervisor::new(service).run().await?;
    Ok(())
}
```

This is the low-overhead choice for a service under `Supervisor` that writes its
own log: append mode accumulates every restart in one file, while truncate mode
starts a fresh log for each spawn. The command's `stdout(StdioMode::…)` /
`stderr(StdioMode::…)` setters are last-wins and clear a prior file destination.

A redirected stdout is deliberately **not piped**. `output_string`,
`output_bytes`, `stdout_lines`, and `events` therefore reject it just as
they reject `Inherit`/`Null`; call `start().await?.wait().await?` (or supervise
the command) when only its exit outcome matters. Stderr may be redirected
independently; it does not prevent stdout capture.

## Output handling

### Encodings

Output is decoded line by line, UTF-8 by default (invalid bytes become
`U+FFFD`, never an error). Legacy-encoding tools can override per stream:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("legacy-tool")
        .encoding(encoding_rs::SHIFT_JIS)          // both streams…
        // .stdout_encoding(…) / .stderr_encoding(…) // …or each its own
        .output_string()
        .await?;
    Ok(())
}
```

(`processkit::prelude::Encoding` re-exports `encoding_rs::Encoding`, so any of its
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

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let tail = Command::new("verbose-build")
        .output_buffer(OutputBufferPolicy::bounded(1_000)) // keep the newest 1000 lines
        .output_string()
        .await?;

    // …or keep the head instead of the tail:
    let head_policy = OutputBufferPolicy::bounded(1_000).with_overflow(OverflowMode::DropNewest);
    Ok(())
}
```

`DropOldest` (the default) keeps a rolling tail; `DropNewest` freezes the
head — a **contiguous prefix** of the output: the first line that doesn't fit
*seals* the head, so every later line is dropped too (even a shorter one that
would still fit), never leaving a set that skipped a dropped line and kept a
later one. `bounded(0)` retains nothing — useful when a line handler (below) is
the real consumer. Under a *line* cap, dropped or not, **every** line still
feeds the handlers and the line counters.

The line cap alone does not bound memory — one enormous newline-free "line"
(`base64 -w0`) is held whole. Add `with_max_bytes` to cap the *retained bytes*
too (either ceiling, or both); the byte cap also bounds the pump's in-flight
assembly buffer, so a never-terminated flood can't exhaust memory. One
consequence: a line whose own length exceeds the byte cap can't be assembled, so
it is dropped **whole** — counted, but **not** delivered to a per-line handler or
`stdout_tee` (don't set a byte cap if a tee must see arbitrarily long lines):

```rust,no_run
use processkit::{Command, OutputBufferPolicy};
let policy = OutputBufferPolicy::unbounded().with_max_bytes(8 << 20); // 8 MiB ring
let strict = OutputBufferPolicy::fail_loud(10_000).with_max_bytes(8 << 20); // error on either
```

`fail_loud` makes the ceiling **error** instead of dropping: the run fails with
`ErrorReason::OutputTooLarge` once the cumulative output (lines *or* bytes) crosses the
cap — even when a streaming consumer is draining lines as they arrive. It bounds
memory, not wall-time, so pair it with `timeout` against a flooding child.

Even under a *drop* policy (`DropOldest`/`DropNewest`), the checking verbs that
hand back stdout as if complete — `run`, `parse`, `try_parse` — **refuse**
silently-truncated output (B12): if the policy dropped lines they fail with
`ErrorReason::OutputTooLarge` rather than feed a parser a truncated tail. The lenient
capture verbs (`output_string` / `output_bytes`) are unaffected — they return
the partial result with `truncated()` set for you to inspect.

### Line handlers — tee output as it arrives

`on_stdout_line` / `on_stderr_line` run a callback on each decoded line *in
addition to* capture or streaming — logging, progress bars, metrics:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("cargo")
        .args(["build", "--release"])
        .on_stderr_line(|line| eprintln!("[build] {line}"))
        .output_string()
        .await?;
    Ok(())
}
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
use processkit::{Command, ErrorReason};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("flaky-network-tool")
        .timeout(Duration::from_secs(30))                 // kill the tree at the deadline
        .retry(3, Duration::from_millis(200), |e| {       // up to 3 attempts total
            matches!(e.reason(), ErrorReason::Timeout { .. })            // …but only retry timeouts
        })
        .run()
        .await?;
    Ok(())
}
```

- **`timeout`** kills the whole process tree at the deadline. On the capturing
  verbs the expiry is *captured* (`ProcessResult::timed_out`), on the
  success-checking verbs it *raises* `ErrorReason::Timeout` — the full decision
  table lives in [Timeouts, retries & cancellation](timeouts-and-cancellation.md).
- **`retry`** applies to the success-checking verbs only — `run`, `run_unit`,
  `exit_code`, `probe`, `checked`, `parse`, and `try_parse` (seven in all; each
  runs through the retry loop). The classifier sees the typed error and decides.
  The non-erroring `output_string`/`output_bytes` paths never retry, and neither
  does `first_line` (its stream search is single-attempt).

## Privileges and spawn flags

Spawn-time controls for sandboxing and service launch:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
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
    Ok(())
}
```

`uid` / `gid` / `groups` / `setsid` are POSIX-only — on Windows the run
fails with `ErrorReason::Unsupported` rather than silently skipping a privilege drop.
A correct drop sets all three of `uid`/`gid`/`groups`: dropping the uid alone
leaves the child holding the parent's (often root's) supplementary groups.
`create_no_window` is a harmless no-op outside Windows.
`kill_on_parent_death` is best-effort by design: guaranteed on Windows
(regardless of the knob), direct-child-only on Linux, unavailable on
macOS/BSD — the graceful-exit guarantee via `Drop` holds everywhere either
way. When the owner dies *abruptly* the kernel has no portable Unix way to take
the whole tree down, so `Command::kill_on_parent_death_scope()` reports the
honest reach as a `ParentDeathCleanup` — `WholeTree` (Windows),
`DirectChildOnly` (Linux), or `Unsupported` (macOS/BSD). A wrapper can surface
that scope instead of overpromising a whole-tree cleanup:

```rust,no_run
use processkit::{Command, ParentDeathCleanup};

match Command::kill_on_parent_death_scope() {
    ParentDeathCleanup::WholeTree => { /* the whole tree dies with the owner */ }
    ParentDeathCleanup::DirectChildOnly => { /* only the direct child; grandchildren survive */ }
    ParentDeathCleanup::Unsupported => { /* no abrupt-death cleanup on this platform */ }
    _ => {}
}
```

Containment is preserved in every combination; the platform fine print
(the Linux cgroup × `uid` interaction, `setsid` × process-group coordination,
the pdeathsig thread caveat) is collected in
[Platform support](platform-support.md#caveats).

### Scheduling: CPU priority, affinity, I/O priority, and `umask`

Spawn-time knobs reuse the same seams as the builders above — Unix `pre_exec`
and Windows' suspended-child configuration — for background/batch children
that shouldn't starve the foreground, and for controlling the permissions of
files a child creates:

```rust,no_run
use processkit::{Command, IoPriority, Priority};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Run at a lower CPU-scheduling priority — supported on BOTH platforms.
    Command::new("batch-job")
        .priority(Priority::BelowNormal)
        .run().await?;

    // Linux + Windows: keep a noisy worker on logical CPUs 2 and 3.
    Command::new("compiler")
        .cpu_affinity([2, 3])
        .run().await?;

    // Linux only: yield disk time to foreground users.
    Command::new("indexer")
        .io_priority(IoPriority::BestEffort(7))
        .run().await?;

    // Unix only: files this child creates get 0644/0755 instead of 0666/0777.
    Command::new("worker").umask(0o022).run().await?;
    Ok(())
}
```

`priority` maps onto `nice`/`setpriority` on Unix and a priority class on
Windows (`Idle`/`BelowNormal`/`Normal`/`AboveNormal`/`High`); unlike the
privilege builders, **every variant is supported on both platforms**, so this
knob never yields `ErrorReason::Unsupported`. One caveat: lowering `nice` below its
inherited value on Unix — raising priority via `Priority::AboveNormal`/`High`,
or even requesting `Priority::Normal` under a positively-niced parent (e.g. a
`nice`d CI/batch launcher) — needs `CAP_SYS_NICE`/root; without it the OS
rejects the change and the spawn fails loud (`ErrorReason::Spawn`), never silently
downgrading to a lower priority.

`cpu_affinity` accepts logical CPU indices. Linux applies a `cpu_set_t` with
`sched_setaffinity` before `exec`; Windows calls `SetProcessAffinityMask` after
race-free Job assignment while the child is still suspended (the ConPTY launch
does the same), then resumes it. Descendants inherit the mask, though a child
with sufficient rights may later change its own. Empty or unrepresentable sets
fail before user code runs; an OS-rejected processor fails the spawn. macOS/BSD
return `ErrorReason::Unsupported`. The Windows API is one processor-group mask,
so indices are limited to the native mask width. `spawn_detached` refuses the
knob; on Windows `to_tokio_command` does too because a raw command cannot carry
the required post-spawn configuration seam.

`io_priority` is Linux-only: it calls `ioprio_set(2)` in `pre_exec` before the
program starts. `BestEffort(7)` is the lowest normal Linux I/O priority; smaller
data values are more urgent, while `Idle` runs only when the device is otherwise
idle. `RealTime` can starve other users and normally needs `CAP_SYS_ADMIN`; a
rejected request fails as `ErrorReason::Spawn`. On Windows, macOS/BSD, and other
Unix targets, requesting I/O priority fails with `ErrorReason::Unsupported` rather
than silently inheriting the caller's I/O priority. It is also refused by
`spawn_detached`, whose owner-independent launch contract cannot honor it.

`umask` is Unix-only — like `setsid`/`groups`, requesting it on Windows fails
with `ErrorReason::Unsupported` rather than being silently ignored.

**Interactive auth / TTY.** By default processkit wires **pipes**, not a
pseudo-terminal, so a tool that *demands* a tty — an `ssh`/`sudo` **password**
prompt, some credential helpers, an `isatty()`-gated agentic CLI — won't get one.
Two ways to satisfy them:

- **Non-interactive (no PTY needed).** Prefer this when possible: key-based auth,
  `ssh -o BatchMode=yes`, `GIT_SSH_COMMAND` / `GIT_TERMINAL_PROMPT=0`, or feed a
  known answer over [interactive stdin](streaming.md#interactive-stdin).
  Conversational tools that read stdin without needing a tty work today via
  `keep_stdin_open` + `stdout_lines`.
- **PTY mode (`use_pty`, the `pty` feature).** For a tool that truly requires a
  controlling terminal, `Command::use_pty()` launches it under a real
  pseudo-terminal — `openpty` on Unix, `CreatePseudoConsole` (ConPTY) on Windows —
  so `isatty()` reports a terminal. This is a **minimal single-master-fd mode**,
  not a terminal emulator, with four things to know:
  - **stdout and stderr are merged** onto the one master, so in this mode the
    `on_stderr_line` / `stderr_tee` split collapses and
    `ProcessResult::stderr` is empty — the whole output arrives where stdout does.
  - Interactive input runs over the same master
    (`keep_stdin_open` + `take_stdin`); on **Unix** terminal **echo is disabled**
    so a written password is not echoed back into the merged output (the ConPTY
    has no portable per-write echo control, so that is Unix-only).
  - The child receives `COLUMNS`/`LINES` matching the initial PTY size (80×24,
    or `pty_size(cols, rows)`). Unix also defaults `TERM=xterm-256color`;
    Windows relies on ConPTY's console/VT APIs and does not synthesize `TERM`.
    Explicit `env(...)` or `env_remove(...)` calls for any of these names win.
  - **Containment is unchanged** — the PTY child lives in the same
    job/cgroup/process group, so whole-tree kill-on-drop, timeouts, and
    cancellation behave exactly as for a piped run.

  Scenario recipes live in the cookbook: [run an `isatty()`-requiring
  tool](cookbook.md#run-a-tool-that-requires-a-terminal), [wait for an
  unterminated prompt and answer](cookbook.md#answer-an-unterminated-pty-prompt),
  [consume agent-CLI progress](cookbook.md#read-in-place-progress-from-an-agent-cli),
  [avoid full-duplex deadlock](cookbook.md#avoid-pty-full-duplex-deadlocks), and
  [test the PTY contract hermetically](cookbook.md#test-pty-behavior-without-a-terminal).
  The streaming guide owns the detailed [PTY I/O
  mechanics](streaming.md#pty-dialog-wait-for-a-prompt-then-answer) and
  [output-hygiene knobs](streaming.md#pty-output-hygiene-line-framing-and-vt-sanitization).
  For the end-to-end `ssh` case specifically — including the boundary where
  kill-on-drop stops at the local client — see [Driving
  ssh](cookbook.md#driving-ssh).

  The historical defer/design is recorded in
  `decisions/permissions-privileges-pty-network.md` §4.

## Consuming verbs

| Verb | Returns | Non-zero exit | Timeout | Use when |
|---|---|---|---|---|
| `output_string()` | `ProcessResult<String>` | captured | captured (`timed_out`) | You want to inspect the outcome yourself |
| `output_bytes()` | `ProcessResult<Vec<u8>>` | captured | captured | Binary stdout (images, archives, …) |
| `run()` | trimmed stdout `String` | `ErrorReason::Exit` | `ErrorReason::Timeout` | "Give me the answer or fail" |
| `exit_code()` | `i32` | the code, `Ok` | `ErrorReason::Timeout` | The code *is* the answer |
| `probe()` | `bool` | `0`→`true`, `1`→`false`, else `ErrorReason::Exit` | `ErrorReason::Timeout` | Predicate commands: `git diff --quiet`, `grep -q` |
| `first_line(pred)` | `Option<String>` | — (stream-based) | `ErrorReason::Timeout` | Grab one matching line, kill the rest |
| `start()` | live `RunningProcess` | — | bounds the stream | [Streaming, interactive I/O, probes](streaming.md) |

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // probe(): the exit code as a boolean.
    let clean = Command::new("git").args(["diff", "--quiet"]).probe().await?;

    // first_line(): stop as soon as the interesting line appears.
    let first_match = Command::new("git")
        .args(["log", "--oneline"])
        .first_line(|l| l.contains("fix:"))
        .await?;
    Ok(())
}
```

`first_line` returns `Ok(None)` when stdout closes without a match, and kills
the (private-group) child once it has its answer — you never wait out a long
log for one line. A [`cancel_on`](timeouts-and-cancellation.md) token that fires
while the search is still running surfaces as `ErrorReason::Cancelled`, so a readiness
probe with a shutdown token can't misread token-driven teardown as "the line
never appeared" — while a run that genuinely ends with no match still reports
`Ok(None)`, even if the token happens to fire an instant later.

## Results and errors

The capturing verbs hand back a `ProcessResult`:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("git").args(["merge", "feature"]).output_string().await?;

    result.code();         // Option<i32> — None = killed (timeout/signal), no code
    result.signal();       // Option<i32> — the signal number (Unix), else None
    result.is_success();   // code in ok_codes (default {0})
    result.timed_out();    // an absolute or inactivity timeout expired
    result.inactivity_timed_out(); // specifically: stdout/stderr went quiet
    result.outcome();      // the explicit disposition behind the accessors above
    result.stdout();       // &str (or &[u8] from output_bytes)
    result.stderr();       // &str
    result.combined();     // stdout + stderr concatenated
    result.diagnostic();   // stderr if non-empty, else stdout — the human-facing line
                           // (git/jj put "CONFLICT …" on stdout!)
    result.configured_timeout(); // Option<Duration> — the timeout this run was launched with
    result.ok_codes();     // &[i32] — the accepted exit codes ({0} by default)

    // Opt into erroring whenever you're ready:
    let ok = result.ensure_success()?; // Exit / Timeout / Signalled (signal-kill) as typed errors
    Ok(())
}
```

When the exact disposition matters, match on `Outcome` instead of
mentally decoding the `code()`/`timed_out()` pair:

```rust,no_run
use processkit::Outcome;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = processkit::Command::new("git").args(["merge", "feature"]).output_string().await?;
    match result.outcome() {
        Outcome::Exited(0) => println!("clean"),
        Outcome::Exited(code) => println!("failed with {code}"),
        Outcome::Signalled(signal) => println!("killed by signal {signal:?}"),
        Outcome::TimedOut => println!("hit its deadline"),
        Outcome::InactivityTimedOut => println!("stopped producing output"),
        _ => {} // non_exhaustive: future dispositions
    }
    Ok(())
}
```

For a single query you usually don't need the `match` (and its
`#[non_exhaustive]` wildcard): `Outcome` carries the same `code()` /
`signal()` / `timed_out()` / `inactivity_timed_out()` accessors as
`ProcessResult`, so a bare `Outcome`
(from `RunningProcess::wait` or `Finished::outcome`) answers directly —
`outcome.code()`, `outcome.signal()`, `outcome.timed_out()`. There is no
`Outcome::is_success` (success is `ok_codes`-aware — use
`ProcessResult::is_success`).

The error enum is structured and `#[non_exhaustive]`:

| Variant | Meaning |
|---|---|
| `ErrorReason::Spawn { program, source }` | The program was located but the OS couldn't start it (permissions, a bad working directory, a Windows `.cmd`/`.bat` needing `cmd.exe`, …) — **not** `is_not_found()` |
| `ErrorReason::NotFound { program, searched }` | The program couldn't be located (the single "not found" representation — `is_not_found()` is true); `searched` is `Some(dirs)` for a bare-name `PATH` lookup, `None` otherwise |
| `ErrorReason::Exit { program, code, stdout, stderr, stdout_bytes }` | Non-zero exit, both streams attached in full (the `Display` message is bounded, but the fields carry the complete captured text for classification); `stdout_bytes` is `Some(exact bytes)` for a checking verb built over `output_bytes`, `None` on the text path — read via `Error::stdout_bytes()` (the variant is `#[non_exhaustive]`) |
| `ErrorReason::Signalled { program, signal, stdout, stderr, stdout_bytes }` | The process was killed by a signal (no exit code); `signal` carries the number on Unix, `None` elsewhere; the partial streams captured before the kill are attached (reach them via `diagnostic()`); `stdout_bytes` as above |
| `ErrorReason::OutputTooLarge { program, max_lines, max_bytes, total_lines, total_bytes }` | A `fail_loud` buffer's line or byte ceiling was exceeded |
| `ErrorReason::Timeout { program, timeout, stdout, stderr, stdout_bytes }` | The run's own deadline killed it; whatever the run captured before the kill is attached — a hung tool's last stderr line tails the `Display` and is reachable via `diagnostic()`; `stdout_bytes` as above |
| `ErrorReason::NotReady { program, timeout }` | A [readiness probe](streaming.md#readiness-probes) gave up |
| `ErrorReason::Parse { program, message }` | A `try_parse` parser (on `Command`, `ProcessRunnerExt`, `CliClient`, or `Pipeline`) rejected the output (the `Display`/`Debug` of `message` is bounded to a 200-byte preview; the field carries the full text) |
| `ErrorReason::Stdin { program, source }` | Feeding the child's stdin failed for a non-broken-pipe reason on an *otherwise-successful* run (a louder failure — exit/signal/timeout — wins instead); a routine broken pipe never surfaces |
| `ErrorReason::CassetteMiss { program }` | (`record` feature) a cassette replay found no matching recording (stale/incomplete cassette) — kept distinct from a missing program, so `is_not_found()` is `false` |
| `ErrorReason::Unsupported { operation }` | The platform can't do what was asked (and silently skipping would be wrong) |
| `ErrorReason::Cancelled { program }` | the run's token was cancelled |
| `ErrorReason::ResourceLimit { kind, reason, detail }` | (`limits` feature) a requested cap couldn't be enforced — `kind` (`LimitKind::Memory`/`Processes`/`Cpu`) says *which* limit, `reason` (`LimitReason::Invalid`/`Unsupported`/`Unenforceable`) says *why*, without parsing `detail`'s English text; read via `Error::limit_kind()`/`limit_reason()` (the variant is `#[non_exhaustive]`) |
| `ErrorReason::Io(source)` | A low-level IO error from the crate's own machinery (driving a child, group control, cassette files) — never an arbitrary foreign `io::Error` (no blanket `From`, D13) |

`Error::diagnostic()` returns the most useful human-facing line out of a
failure that captured output — `Exit`, and (D12) `Timeout` / `Signalled` (the
partial streams of a hung-then-killed or crashed tool). Each of those variants'
one-line `Display` also appends a bounded excerpt of that diagnostic (the last
non-empty line, capped at 200 bytes), so a bare `eprintln!("{e}")` reads
`` `git` exited with code 2: fatal: boom `` — actionable in a log line without
dumping multi-KiB streams into it.

## Escape hatch: a platform knob the crate doesn't model

`Command` exposes typed builders for the OS knobs that carry their weight —
`priority`, `cpu_affinity`, `io_priority`, `umask`, `create_no_window`, `windows_graceful_ctrl_break`,
`run_as` (uid/gid), `parent_death`, and so on. New *real* needs are added the
same way: as a typed verb, so the command stays inspectable (its `Debug`, its
`Clone`, and — with the `record` feature — the cassette it serialises to all
stay truthful). There is deliberately **no** `before_spawn`-style hook that
stores an opaque closure to mutate the raw command per launch: a stored mutator
is invisible to `Debug`/`Clone` and would make a recorded cassette *lie* — it
would replay the command as written while a different, mutated command actually
ran. (The full reasoning is in `decisions/before-spawn-hook-2026-07.md`.)

When you genuinely hit a platform knob the crate has no verb for — a niche
creation flag, your own `pre_exec` — the honest escape is to lower the builder
to a raw [`tokio::process::Command`] and spawn it into a
[`ProcessGroup`](process-groups.md):

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the OS command exactly as ProcessKit would: program resolution,
    // environment, working directory, scheduling/umask knobs, capture-wired stdio.
    let raw = Command::new("odd-tool")
        .args(["--serve"])
        .to_tokio_command()?;

    // `raw` is a plain `tokio::process::Command`. Set the platform knob the
    // builder has no typed verb for here — e.g. a raw creation flag on Windows
    // or your own `pre_exec` on Unix.

    // Spawn INTO a group so containment still holds (see below).
    let group = ProcessGroup::new()?;
    let mut child = group.spawn(raw)?;
    let status = child.wait().await?; // the bare tokio Child is yours to drive
    let _ = status;
    Ok(())
}
```

**What survives, and what you give up.** `to_tokio_command()` carries over
everything the builder resolves at the OS level (program/args/cwd, the layered
environment, the Unix `priority`/`umask`/privilege-drop/`setsid` `pre_exec`
hooks (including Linux-only `io_priority` and `cpu_affinity`), Windows creation
flags, and stdio wired for capture). Windows `cpu_affinity` is the deliberate
exception: it needs a live, still-suspended process handle, so lowering such a
command fails with `Unsupported` instead of dropping the request. Spawning the result
through [`ProcessGroup::spawn`](process-groups.md) still enrolls the child in the
group's Job/cgroup/process-group, so **containment is preserved** — kill-on-drop
and the group-level teardown verbs still reach it. What you leave behind is the
high-level machinery that lives *above* the OS command: the async output pump and
capture, the `ProcessResult`/`RunningProcess` verbs, and the per-run
`timeout`/`cancel_on`/`timeout_grace`/`windows_graceful_ctrl_break` wiring. You
own the bare [`tokio::process::Child`] — draining its pipes and reaping it are
your job. (On Windows, `spawn` re-sets creation flags for a race-free
job assignment, so a creation flag left on the raw command is overwritten there;
prefer the typed `create_no_window` on a high-level launch path — see
[Process groups](process-groups.md) for the full raw-`spawn` contract.)

[`tokio::process::Command`]: https://docs.rs/tokio/latest/tokio/process/struct.Command.html
[`tokio::process::Child`]: https://docs.rs/tokio/latest/tokio/process/struct.Child.html

---

Next: [Streaming & interactive I/O](streaming.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Process groups](process-groups.md)
