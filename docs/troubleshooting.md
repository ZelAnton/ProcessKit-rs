# Troubleshooting

Start here when you have a symptom but do not yet know which processkit
subsystem owns it. The sections below give a short diagnosis and route to the
guide that owns the full contract.

Before changing code, preserve the structured evidence:

- inspect `Error::reason()` / `Error::kind()` and `Error::diagnostic()` instead
  of parsing `Display` text;
- record `ProcessGroup::mechanism()` (or use `host_containment()` before any
  group exists);
- note whether the run owns a private group or uses a shared group —
  `RunningProcess::kills_tree_on_drop()` answers that directly;
- distinguish the command's lifetime deadline from a readiness probe's
  `within` deadline.

| Symptom | First distinction |
|---|---|
| [A child survived drop](#a-child-survived-drop) | Private owner, shared group, deliberate detach, external/remote process, or POSIX `setsid` escape? |
| [`NotFound`, but the tool is installed](#notfound-but-the-tool-is-installed) | Bare-name lookup or an explicit path? Which effective `PATH` / `PATHEXT` was used? |
| [`ResourceLimit` while creating a group](#resourcelimit-while-creating-a-group) | Invalid value, unsupported mechanism, or an existing but undelegated cgroup? |
| [The process runs, but no output appears](#the-process-runs-but-no-output-appears) | End-of-run capture, incomplete line, child-side pipe buffering, or non-piped output? |
| [PTY output contains ANSI/VT garbage](#pty-output-contains-ansivt-garbage) | Retained text that can be sanitized, or an exact/raw sink that intentionally stays byte-accurate? |
| [`wait` or a stream never finishes](#wait-or-a-stream-never-finishes) | Open stdin, undrained full-duplex output, missing line terminator, or a live process/descendant? |
| [Graceful shutdown does not work on Windows](#graceful-shutdown-does-not-work-on-windows) | Windowed `WM_CLOSE`, opted-in console `CTRL_BREAK`, or hard-kill-only member? |
| [`Timeout` instead of `NotReady` — or vice versa](#timeout-instead-of-notready--or-vice-versa) | Run contract versus non-killing readiness observation? |

## A child survived drop

First identify **what was dropped** and who owns containment:

- `Command::start()` creates a private group. Its handle reports
  `kills_tree_on_drop() == true`; dropping it hard-kills the local tree.
- `ProcessGroup::start()` creates a shared-group handle. It reports `false`:
  the separately held `ProcessGroup` controls the tree's lifetime, so stop or
  drop that group (or call `start_kill()` when only the direct child should
  stop). Keeping an `Arc<ProcessGroup>` clone alive keeps the owner alive too.
- `spawn_detached()` is the deliberate exception. Dropping `DetachedChild`
  does nothing because the child was explicitly launched to outlive this
  process.
- A process spawned outside processkit is outside the boundary until `adopt()`
  (with its `Child` handle) or `adopt_external()` (with only its pid).
  Adoption moves the named process, not descendants it already created; the
  POSIX process-group backend can only track an already-executed adopted child
  individually. `adopt_external` additionally reports `Unsupported` on FreeBSD
  and the other BSDs, where the crate has no start-time reader to anchor the pid
  on — a refusal, so a process left running there was never contained.
- **A group whose members die to a teardown it never ran, or whose `start` suddenly
  fails "job is full" (Windows).** Adopting a pid that already belonged to another
  Job Object nests this crate's job under that one, so the outer job's terminate or
  close reaches these members (later-started ones included) and its limits bind them.
  The mirror image on Linux cgroup v2: adopting a process takes it **out of** its
  previous cgroup, so an outside supervisor's teardown and limits stop applying to
  the process it thought it held. Neither is reverted on drop — see
  [platform support](platform-support.md#capability-matrices).
- **`adopt_external` returned "pid … was recycled while it was being adopted".** The
  number changed hands inside the call, so nothing was adopted. Read the rest of that
  message before retrying: on Linux cgroup v2 it also says whether the number could
  be moved back out of this group's cgroup, and in the case where it could not, the
  process now holding the number is a member of this group and will be killed by its
  teardown — drop or tear down that group promptly if that is not acceptable.
- Nothing reaps a process adopted by pid. No exit status for it appears anywhere
  in this API, so a `wait`-shaped answer for it has to come from whoever is its
  actual parent; on the POSIX process-group backends an exited one that nobody
  reaps stays a zombie, keeps probing alive through a graceful shutdown's whole
  grace, and cannot be cleared by `escalate_to_kill`.
- A command reached through `ssh` is local containment only: the local `ssh`
  client dies, but the remote command needs remote-side containment.

Then inspect the mechanism. `JobObject` and `CgroupV2` are real tree
containers. `ProcessGroup` is intentionally reported as the weaker fallback:
a descendant that calls `setsid` can escape it. Also remember that a process
abort or `SIGKILL` can skip Rust `Drop`. Windows still closes the Job Object and
reaps the whole tree; Linux's opt-in `kill_on_parent_death()` reaches only the
direct child, and macOS/BSD have no equivalent owner-death hook.

If an ordinary private-group child survives a normal Rust drop without one of
those boundaries, capture the mechanism, platform, program shape, and a minimal
reproducer — that is not an expected lifecycle outcome.

Full contracts: [process ownership and adoption](process-groups.md#putting-processes-in),
[deliberate detachment](process-groups.md#deliberately-detaching-a-child-spawn_detached),
[containment mechanisms](platform-support.md#containment-mechanisms), and the
[remote-execution boundary](untrusted-children.md#what-not-to-rely-on).

## `NotFound`, but the tool is installed

`NotFound` means processkit could not resolve the program under the command's
actual launch rules. Check these in order:

1. A **bare name** is searched on the command's effective child `PATH` (and on
   Windows with `PATHEXT`). `env_clear()`, `inherit_env(...)`, an explicit
   `PATH`, or a missing extension can make that different from the interactive
   shell where you verified the installation.
2. A program containing a path separator is **not** searched on `PATH`.
   `current_dir` does not portably re-anchor a relative program path; combine a
   child working directory with an absolute program path.
3. `prefer_local(dir)` affects only bare names. A relative preferred directory
   is resolved against the parent's real current directory, not the command's
   `current_dir`.
4. Run `Command::resolve_program()` (or `processkit::which` for a bare name).
   It is a spawn-free preflight using the same `PATH` / `PATHEXT`, executable-bit,
   environment, and `prefer_local` logic as the real launch.

If preflight finds the file but launch still fails, inspect whether the error is
actually `Spawn`: permissions, a bad `cwd`, or a Windows `.cmd`/`.bat` that
needs `cmd.exe` are launch failures, not missing programs.

Full contracts: [program and working-directory resolution](commands.md#program-arguments-working-directory),
[`prefer_local`](commands.md#resolving-a-locally-installed-tool-prefer_local),
[spawn-free preflight](commands.md#preflight-resolve-a-program-without-running-it),
and [`NotFound` versus `Spawn`](errors.md#variants-that-look-alike-but-arent).

## `ResourceLimit` while creating a group

Do not parse the English detail. Read `limit_kind()` and `limit_reason()`:

- `Invalid` means the requested value itself is invalid;
- `Unsupported` means there is no whole-tree resource accounting for that limit
  (no container at all, or — on FreeBSD — a reaper that contains but does not
  account);
- `Unenforceable` means a suitable mechanism exists but this process cannot
  apply the cap.

On Linux, processkit's per-tree limits require the process to own the **real
cgroup v2 hierarchy root** so it can enable controllers. A container cgroup
namespace root is not that root, and a normal systemd session, scope, or service
is not delegated this way. Both ordinary and privileged containers therefore
commonly return `Unenforceable`; requesting a cap fails loud instead of silently
creating an unbounded group.

Inside Docker/Kubernetes, use the orchestrator's memory/CPU limits as the outer
boundary. Use processkit's `limits` on a host where the process genuinely owns
the cgroup root (typically a minimal non-systemd init on bare metal or a VM).
Kill-on-drop can still fall back to a POSIX process group when no per-tree cap
was requested; it is the unenforceable protection request that must fail.

Full contracts: [container limits versus processkit limits](containers.md#container-resource-limits-vs-the-crates-limits),
[resource-limit semantics](process-groups.md#resource-limits), and
[cgroup prerequisites](platform-support.md#containment-mechanisms).

## The process runs, but no output appears

Separate transport from the child's own buffering:

- One-shot capture (`output_string`, `run`) returns output when the child exits.
  For live output, use `start()` plus `stdout_lines()`, a line handler, or a tee.
- A line stream emits only after a terminator. An interactive `Password: ` or
  REPL prompt without `\n` needs `wait_for_output`, not `wait_for_line`.
- A child may block-buffer stdout when it sees a pipe. Prefer the tool's own
  unbuffered/line-buffered switch when available. If its behavior is genuinely
  `isatty()`-gated, enable the `pty` feature and use `use_pty()`.
- `StdioMode::Inherit`, `Null`, and `stdout_file*` intentionally bypass the
  capture pump. A PTY instead merges stdout and stderr into logical stdout, so
  its separate stderr capture is empty.

PTY is a semantic change, not just a flushing switch: it merges streams, uses
terminal line framing, and may add control sequences. Keep pipes when stream
identity or exact bytes matter.

Full contracts: [streaming stdout](streaming.md#streaming-stdout),
[prompt-aware waiting](streaming.md#prompt-aware-waiting-wait_for_output),
[interactive/TTY launch](commands.md#privileges-and-spawn-flags), and the
[PTY platform matrix](platform-support.md#pty-mode-use_pty-the-pty-feature).

## PTY output contains ANSI/VT garbage

Terminal applications emit color, cursor movement, alternate-screen, OSC
title/hyperlink, and other VT sequences. `use_pty()` does not emulate a screen.
Opt into `sanitize_vt()` (or the per-stream `stdout_sanitize_vt()` /
`stderr_sanitize_vt()` variants for pipe-capable wrappers) when retained text
should be plain.

Sanitization is deliberately scoped to the capture backlog. Handlers, decoded
tees, raw tees, and `output_bytes()` still see exact input; sanitize inside
those sinks if they also need clean text. PTY framing is separately `\r`-aware
by default; an explicit `LineTerminator::Newline` can make redraw-style progress
look delayed even after escapes are removed.

Full contract: [PTY output hygiene](streaming.md#pty-output-hygiene-line-framing-and-vt-sanitization).

## `wait` or a stream never finishes

Work through the resources that can still be open:

1. Stdin is closed by default. If you opted into `inherit_stdin()`, the child
   can wait for EOF from the parent's terminal or pipe. If you called
   `keep_stdin_open()` and took the writer, call `ProcessStdin::finish()` or
   drop it when the conversation ends.
2. Do not perform a large interactive write while nothing drains output. A
   full stdout/PTY buffer can stop the child reading stdin while the parent's
   stdin write is also blocked. Read and write concurrently.
3. `stdout_lines()` and `wait_for_line()` need a complete line. For an
   unterminated prompt, use `wait_for_output()`.
4. `wait_any()` does not pump output. Drain chatty children first (or concurrently)
   before racing them.
5. A still-running descendant can hold an inherited pipe open after its direct
   parent exits. Bound the run with `Command::timeout()` or
   `inactivity_timeout()`. On a shared-group handle those watchdogs stop the
   direct child; stop the owning group when the whole shared tree must end.

A readiness `within` bound is not a run bound: `NotReady` leaves the child
alive. If giving up on readiness should end the run, explicitly `shutdown()` an
own-group handle, `start_kill()` a direct child, or stop the shared group.

Full contracts: [interactive stdin and full-duplex deadlock](streaming.md#interactive-stdin),
[`wait` versus `drain`](streaming.md#wait-vs-drain),
[stream deadlines](streaming.md#streaming-stdout), and
[timeouts and inactivity](timeouts-and-cancellation.md#timeouts).

## Graceful shutdown does not work on Windows

Dropping a handle/group is always the hard safety net; it never waits for
application cleanup. Use `shutdown`, `stop`, or `timeout_grace` for a graceful
tier, then identify which Windows soft-close path the child can receive:

| Child shape | Soft-close path |
|---|---|
| Owns a top-level window | `WM_CLOSE` is posted automatically; the child has the grace window to exit. |
| Console child spawned with `windows_graceful_ctrl_break()` | The direct child receives `CTRL_BREAK`, then survivors are terminated after the grace. |
| `create_no_window`, `DETACHED_PROCESS`, or console child without the opt-in | No console event can land; teardown reaches the hard Job Object kill. |
| Adopted console child | Not a registered CTRL leader; only an owned top-level window can receive the automatic `WM_CLOSE` path. |

`soft_stop_scope()` is the side-effect-free check for the live group:
`OptInMembers` means at least a console/window target is reachable;
`Unsupported` means no soft target exists. The console event is `CTRL_BREAK`,
not `CTRL_C`, and the child must install the corresponding handler and exit
within the grace.

Full contract: [Windows graceful teardown](process-groups.md#windows-the-graceful-soft-tier-wm_close-opt-in-ctrl_break)
and [graceful timeouts](timeouts-and-cancellation.md#graceful-timeout).

## `Timeout` instead of `NotReady` — or vice versa

They answer different questions:

| Result | Clock | Side effect |
|---|---|---|
| `ErrorReason::NotReady` | A readiness method's `within` argument | Observation stops; the child remains alive. |
| `Outcome::TimedOut` / `ErrorReason::Timeout` | `Command::timeout()` or `inactivity_timeout()` | The applicable process tree/direct child is torn down. |

Readiness probes deliberately do **not** arm the command watchdog while they
poll. A probe can therefore return `NotReady` even if the command's absolute
deadline has passed; the following consuming verb (`finish`, `wait`,
`output_string`, and so on) enforces that run deadline and can then report
`Timeout`. Conversely, a probe whose output stream closes before a match returns
`NotReady` immediately because readiness can no longer happen — it does not wait
out the rest of `within`.

After `NotReady`, choose explicitly: keep waiting, stop the handle/group, or
surface startup failure. For checking code, branch on `err.reason()` /
`is_timeout()`; `is_timeout()` is intentionally false for `NotReady`.

Full contracts: [readiness probes](streaming.md#readiness-probes),
[deadline families](timeouts-and-cancellation.md#timeouts), and
[look-alike errors](errors.md#variants-that-look-alike-but-arent).

---

Next: [Errors](errors.md) · [Streaming & interactive I/O](streaming.md) ·
[Platform support](platform-support.md) · [Running in containers](containers.md)
