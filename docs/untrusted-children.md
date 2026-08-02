# Running untrusted children

[‹ docs index](README.md)

Sometimes the program you launch isn't yours: a CI plugin, a user-submitted
build script, a tool discovered on `PATH` at runtime. The hardening knobs for
that case already exist across this guide set — containment, resource limits,
privilege drop, environment hygiene, output/wall-time bounds — but they're
scattered one guide per topic. This page pulls them into one threat-aware
checklist, states plainly what `processkit` actually guarantees, and links
back to the deep guide for each item.

- [What this crate is — and isn't](#what-this-crate-is--and-isnt)
- [1. Containment: pick a mechanism, then check it](#1-containment-pick-a-mechanism-then-check-it)
- [2. Resource limits and their platform caveats](#2-resource-limits-and-their-platform-caveats)
- [3. Drop privileges in the right order](#3-drop-privileges-in-the-right-order)
- [4. Keep the environment hermetic](#4-keep-the-environment-hermetic)
- [5. Bound output, wall time, and give yourself an exit](#5-bound-output-wall-time-and-give-yourself-an-exit)
- [What not to rely on](#what-not-to-rely-on)

## What this crate is — and isn't

`processkit` guarantees the things a process-management *library* can
guarantee:

- **Whole-tree containment that can't leak.** Every `Command::run()`-style
  call spawns into a fresh, private kill-on-drop group; an early return, a
  panic, a dropped future — the tree still dies. See
  [Process groups](process-groups.md).
- **Honest failure instead of silent degradation.** A resource cap that can't
  be enforced fails the call with a typed `ErrorReason::ResourceLimit` rather than
  handing back a group that looks capped but isn't; a privilege-drop knob
  unsupported on the current platform fails with `ErrorReason::Unsupported` rather
  than being quietly skipped. Nothing you asked for is ever dropped on the
  floor without telling you.
- **No secrets in diagnostics by default.** Argv and environment *values*
  never appear in `Debug`, in `tracing` output, or in a `MemberInfo` snapshot
  — see [§4](#4-keep-the-environment-hermetic) for the one deliberate,
  opt-in exception.

It does **not** guarantee the things only the OS (or a purpose-built sandbox)
can guarantee. `processkit` is not a sandbox, and using it does not replace
**seccomp**, an **AppContainer**/restricted token, Linux **namespaces**
(mount/user/net), **SELinux**/**AppArmor** profiles, or a real OS-level
container/VM (gVisor, Firecracker, `docker run` with a security profile, …).
A child that fully controls its own process can still do anything the
credentials you handed it allow: read/write files it has permission to
touch, open sockets, fork and exec other binaries. This crate's job is to
make sure *you* — the parent — can always find and kill the whole tree,
apply OS-level caps and a privilege drop cleanly, and never leak secrets
into your own logs — not to build a security boundary the OS itself doesn't
provide. Reach for [real isolation](#what-not-to-rely-on) when "untrusted"
means "actively adversarial", not just "third-party".

## 1. Containment: pick a mechanism, then check it

You don't have to opt in to containment — every one-shot verb already runs
the child inside a private group. What you *do* need to check, for an
untrusted child, is **which** mechanism you actually got, because the
strength of "contained" varies:

```rust,no_run
use processkit::{Mechanism, ProcessGroup};

fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    match group.mechanism() {
        Mechanism::JobObject | Mechanism::CgroupV2 => {
            // Whole-tree kill AND whole-tree resource accounting/limits.
        }
        Mechanism::ProcessReaper => {
            // FreeBSD's procctl reaper: whole-tree kill and whole-tree
            // `members()` — a child that `setsid`s away stays contained and
            // visible, unlike the process-group fallback below. No whole-tree
            // resource accounting, so `limits` is refused (see §2).
        }
        Mechanism::ProcessGroup => {
            // Kill-on-drop still holds for the whole tree — but `members()`
            // only reports tracked leaders, a child that calls `setsid` escapes,
            // and `limits` is refused outright (see §2) rather than silently
            // doing nothing.
        }
        _ => {} // non_exhaustive: a future mechanism
    }
    Ok(())
}
```

Kill-on-drop containment is unconditional in every case — a `ProcessGroup`
(POSIX process-group) fallback still reaps the whole tree on drop/shutdown.
What narrows on the fallback is *accounting and limits*: no whole-tree
memory/CPU totals, and (§2) resource caps refuse to apply rather than
pretend to — plus the one containment gap worth knowing for an *untrusted*
child, a descendant that calls `setsid` and leaves the process group. Only the
process-group fallback has that gap: Job Objects, cgroups and FreeBSD's
`ProcessReaper` all follow the escapee. See [Platform support → containment mechanisms](platform-support.md#containment-mechanisms)
for exactly which platform/privilege combination gets which mechanism, and
[Running in containers](containers.md#which-containment-mechanism-you-get)
for the concrete `docker run` case (an unprivileged container almost always
lands on the pgroup fallback).

## 2. Resource limits and their platform caveats

The `limits` feature caps a group's memory, process count, and CPU quota as
a whole-tree property, set once at creation:

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .max_memory(512 * 1024 * 1024) // bytes, whole tree
            .max_processes(64)             // fork-bomb ceiling
            .cpu_quota(0.5),               // half of one core
    )?;
    let _sandboxed = group.start(&Command::new("untrusted-tool")).await?;
    Ok(())
}
```

**Creation fails fast when a cap can't be enforced.** `with_options` (and
`ProcessGroup::new` if any limit was requested) returns
`ErrorReason::ResourceLimit { kind, reason, detail }` instead of handing back a
group that looks capped but isn't — an unenforced limit is no protection for
an untrusted child. `reason` tells you *why*: `Unsupported` means no mechanism
with whole-tree resource *accounting* exists here — macOS/the other BSDs and a
Linux host with no cgroup v2 mounted have no whole-tree container at all, and
FreeBSD's process reaper contains a tree without accounting for it;
`Unenforceable` means a mechanism exists but this
particular request was rejected (most commonly: this process isn't at the
**real** cgroup-v2 hierarchy root — true of essentially every ordinary
container, privileged or not, and every systemd session/scope/service). Only
rely on `limits` for hosts where this process genuinely owns the cgroup
root; inside a container, rely on the orchestrator's own memory/CPU limits
instead. Full detail: [Process groups → resource limits](process-groups.md#resource-limits),
[Platform support → containment mechanisms](platform-support.md#containment-mechanisms),
[Running in containers → container limits](containers.md#container-resource-limits-vs-the-crates-limits).

**`max_processes` enforces differently on Windows vs. Linux — read this
before relying on it as an admission control.** On **Windows** the Job
Object's `ActiveProcessLimit` rejects the *(n+1)*th process assigned to the
job, so it caps repeated `start()` calls into the same group too. On
**Linux** the kernel only checks `pids.max` when a process *forks inside the
cgroup*; this crate's children fork in the parent cgroup and migrate in
during pre-exec, so the cap reliably bounds the descendants a contained
child forks (the fork-bomb case), but does **not** reject additional
top-level `start()` calls each placing one more child into an already-full
group. If you need Linux to reject the *n+1*th `start()` too, count starts
yourself. Detail: [`ResourceLimits::max_processes`](https://docs.rs/processkit/latest/processkit/struct.ResourceLimits.html).

## 3. Drop privileges in the right order

On Unix, `uid`/`gid`/`groups` drop the child's identity before it ever
`exec`s; the crate handles the ordering that privilege drop demands:

```rust,no_run
use processkit::{Command, RlimitResource};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    Command::new("untrusted-tool")
        .groups([1000])       // replace the inherited (often root's) supplementary groups
        .gid(1000)            // applied before uid — a gid change needs privilege
        .uid(1000)            // dropped last
        .setsid()             // new session: detaches from the controlling terminal
        .umask(0o022)          // files the child creates get 0644/0755, not 0666/0777
        .rlimit(RlimitResource::Core, 0, 0) // never write a secret-bearing core dump
        .rlimit(RlimitResource::NoFile, 256, 256) // cap this process's open fds
        .run().await?;
    Ok(())
}
```

The OS syscall order is fixed internally regardless of builder call order:
`setgroups` → `setgid` → `setuid`. **All three matter** — dropping `uid`
alone leaves the child holding the *parent's* (often root's) supplementary
groups, which can still grant access you meant to remove; always pair
`uid`/`gid` with an explicit `groups()` (or `groups([])` to drop every
extra). `uid`/`gid`/`groups`/`setsid`/`umask`/`rlimit` are Unix-only: on Windows the
run fails with `ErrorReason::Unsupported` rather than silently proceeding without
a drop — a wrapper that must run cross-platform should branch on the
platform ahead of time rather than assume the drop happened. None of this
needs an external helper (`setpriv`/`su-exec`/`gosu`) or a shell — the crate
calls the raw syscalls itself inside the child's pre-exec hook and always
`exec`s the target directly — so a minimal/no-shell final image (`FROM
scratch`-style) still drops privileges correctly. Detail: [Running commands
→ privileges and spawn flags](commands.md#privileges-and-spawn-flags),
[Running in containers → minimal images](containers.md#minimal-images-muslalpine-no-shell-no-setpriv),
platform fine print (the cgroup × `uid` ordering interaction, `setsid` ×
process-group coordination) in [Platform support → caveats](platform-support.md#caveats).

`rlimit` is per process, not an aggregate substitute for cgroup/Job Object
limits. It is useful even where whole-tree caps are unavailable: disable core
dumps, bound descriptors or generated file size, and set a hard ceiling the
child cannot raise. Descendants inherit the cap but each has its own accounting;
for a shared tree-wide memory/CPU/process budget, keep using `ResourceLimits`
where the active containment mechanism can enforce them.

## 4. Keep the environment hermetic

By default the child inherits your process's full environment — often not
what you want for something untrusted. `env_clear()`/`inherit_env()` narrow
that to an explicit allow-list:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Allow-list: clear everything, copy only the named parent variables
    // (re-read from the parent on every retry), then layer explicit overrides.
    Command::new("untrusted-tool")
        .inherit_env(["PATH", "LANG"])
        .env("MODE", "sandboxed")
        .run().await?;

    // Scorched earth: the child starts with an empty environment.
    Command::new("hermetic-tool").env_clear().run().await?;
    Ok(())
}
```

**Argv and environment values are never rendered in diagnostics by
default.** `Command`'s manual `Debug` impl reduces argv to a count and env
overrides to variable *names* only — never values, never argv contents; the
`tracing` feature's own event stream follows the identical rule. Reach for
`command_line()` only when you deliberately need the full, human-readable
line (a dry-run echo, a log line you control) — it's an explicit, opt-in
escape hatch specifically because the default elsewhere is silence.

**The one deliberate exception: `record`-feature cassettes.** A cassette
(`RecordReplayRunner`) redacts env *values* the same way — only variable
*names* are persisted — but it stores **argv, the working directory, and
both captured streams verbatim**, by design (a cassette has to reproduce the
exact invocation on replay). Any of those can carry a secret a hostile or
merely careless child's stdout/stderr echoed back, or a `--token=…` argument
you passed it. The cassette file is therefore created owner-only (`0600` on
Unix) rather than inheriting a world-readable umask — but on Windows the
file inherits the containing directory's ACL, so **restrict the directory**
(or use a per-user temp path, never a shared/world-writable one) if you
record cassettes for anything that can carry secrets. Don't assume "the
crate never logs argv" extends to cassette recording — it's the one
opt-in feature where argv reaches disk on purpose. Detail: [Testing your
code → record/replay cassettes](testing.md#recordreplay-cassettes).

**Redacting what a child *prints* — `capture_policy`.** The rules above cover
what the crate logs (argv/env) and stores (cassettes); the remaining leak is a
secret the *child itself* echoes to stdout/stderr — a passphrase prompt from an
agent CLI under a pseudo-terminal, a token re-printed in a diagnostic — which
otherwise lands in the captured `ProcessResult` verbatim. `capture_policy`
installs a typed [`CapturePolicy`] that shapes each decoded line **before it is
retained**, so you can scrub the secret out of the capture (and the streaming
verbs) at the source:

```rust,no_run
use std::borrow::Cow;
use processkit::{Command, CapturePolicy, OutputStream};

struct Scrub;
impl CapturePolicy for Scrub {
    fn name(&self) -> &str { "scrub" }
    fn on_capture<'a>(&self, _s: OutputStream, line: &'a str) -> Cow<'a, str> {
        if line.contains("passphrase") { Cow::Borrowed("<redacted>") } else { Cow::Borrowed(line) }
    }
}

# async fn f() -> Result<(), Box<dyn std::error::Error>> {
let out = Command::new("agent").capture_policy(Scrub).output_string().await?;
# let _ = out; Ok(()) }
```

It is a **narrow, capture-scoped** seam: it shapes the backlog only. The
`on_*_line` handlers, a `stdout_tee`/`stderr_tee`, and `stdout_raw_tee` are
independent and still see the un-redacted line — if you also tee a child's output
to a log, scrub in that sink too, or don't tee a stream that can carry secrets. A
policy that panics fails closed (the line is blanked, never leaked). Full contract
and examples: [Streaming → redaction at capture](streaming.md#redaction-at-capture-capture_policy).

[`CapturePolicy`]: https://docs.rs/processkit/latest/processkit/trait.CapturePolicy.html

## 5. Bound output, wall time, and give yourself an exit

An untrusted child can flood its own stdout/stderr, hang forever, or need to
be abandoned mid-run for reasons that have nothing to do with its exit code.

```rust,no_run
use processkit::{Command, OutputBufferPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("untrusted-tool")
        // Fail loud instead of growing an unbounded in-memory buffer; the
        // pipe is still fully drained either way, so the child never blocks.
        .output_buffer(OutputBufferPolicy::fail_loud(10_000).with_max_bytes(8 << 20))
        // Bound wall-time — kills the whole tree at the deadline.
        .timeout(Duration::from_secs(30))
        // Take the direct child down even if THIS process is SIGKILLed.
        .kill_on_parent_death()
        .output_string()
        .await?;
    Ok(())
}
```

- **`output_buffer(...fail_loud(...))`** bounds *memory*, not wall time — an
  unbounded flood with no `timeout` keeps the child running (its pipe is
  drained into nothing) until it exits on its own, so pair the two. Detail:
  [Running commands → buffer policies](commands.md#buffer-policies--bounding-memory-on-chatty-children).
- **`timeout`** kills the whole tree (not just the direct child) at the
  deadline, on every consuming verb — captured as data on the capturing
  verbs, raised as `ErrorReason::Timeout` on the checking ones. A caller-initiated
  abandonment (rather than a fixed deadline) is `cancel_on(token)` instead —
  it always errors, on every path. Detail: [Timeouts, retries &
  cancellation](timeouts-and-cancellation.md).
- **`kill_on_parent_death()`** hardens the case where *your own* process is
  SIGKILLed before it can run `Drop` (which is what normally tears the
  child tree down). Its reach is honest, not overpromised — guaranteed
  whole-tree on Windows (Job Object kill-on-close), direct-child-only on
  Linux (`PR_SET_PDEATHSIG`; a surviving cgroup/pgroup leaves grandchildren
  running), a documented no-op on macOS/the BSDs. Query
  `Command::kill_on_parent_death_scope()` (a `ParentDeathCleanup` — no
  `Command` instance needed) to report the *actual* reach to your own
  caller instead of promising a whole-tree cleanup the kernel can't
  deliver on that platform. There is no portable Unix "kill the tree when
  its creator dies" primitive; when that guarantee genuinely matters, keep
  an outer container/cgroup or subreaper that owns the tree independently
  of your process. Detail: [Running commands → privileges and spawn
  flags](commands.md#privileges-and-spawn-flags), [Platform support →
  caveats](platform-support.md#caveats).

## What not to rely on

- **A non-tty child is not a sandboxed child.** processkit wires **pipes**,
  never a pseudo-terminal, to a spawned child's stdio — but that's a
  plumbing choice for capture/streaming, not a security boundary. A child
  with no controlling terminal can still fork, exec, open sockets, and
  read/write anything its credentials allow; don't reason "it can't get a
  tty, so it can't do X" for any `X` beyond "prompt interactively on a
  terminal it doesn't have". If a tool *demands* a tty (an `ssh`/`sudo`
  password prompt), that's a different, unrelated limitation — see [Running
  commands → interactive auth](commands.md#privileges-and-spawn-flags).
- **Kill-on-drop stops at your machine's process tree — it does not cross an
  `ssh` hop (or any remote-execution client).** The whole-tree guarantee reaps
  the local tree, including a local `ssh` *client*; the command that client runs
  on a **remote** host is beyond its reach, so a dropped handle, a `timeout`, or
  a panic that kills the client can leave the remote command running, orphaned.
  Contain the remote side *on the remote side* — `ssh -tt` (a connection drop
  then delivers `SIGHUP` to the remote session), a server-side `timeout(1)` or
  unit deadline. See the cookbook's [Driving ssh](cookbook.md#driving-ssh)
  recipe for the full boundary and mitigations.
- **Stdin closed by default is hygiene, not confinement.** It stops a child
  from hanging on unexpected input; it says nothing about what the child
  can do once running.
- **A resource limit or privilege drop this crate refused to apply
  (`ErrorReason::ResourceLimit`/`ErrorReason::Unsupported`) is not "close enough".**
  Treat either as a hard stop for the launch, not a degraded-but-acceptable
  mode — see [§2](#2-resource-limits-and-their-platform-caveats) and
  [§3](#3-drop-privileges-in-the-right-order).
- **When the child is genuinely adversarial, go get real isolation.**
  `processkit` composes cleanly underneath any of these, but doesn't
  replace them: an OS container with a seccomp/AppArmor/SELinux profile, a
  Windows AppContainer or a restricted access token, a microVM (Firecracker,
  gVisor), or — the low-tech option that's often enough — a dedicated,
  minimally-privileged service account with no access to anything the child
  shouldn't reach even if it does everything an ordinary process running as
  that account is allowed to do.

For the error types this section's failure modes actually raise, see
[Errors](errors.md); for the crate's security-relevance and how to report a
vulnerability, see [`SECURITY.md`](../SECURITY.md).

---

Next: [Running commands](commands.md) ·
[Running in containers](containers.md) ·
[Platform support](platform-support.md) ·
[docs index](README.md)
