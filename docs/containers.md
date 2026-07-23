# Running in containers

[‹ docs index](README.md)

Containers are where most `processkit`-using services actually run, and it's
the environment with the most sharp edges: which containment mechanism you
get depends on privileges the orchestrator may or may not grant, your process
is usually PID 1 (with everything that implies for signal delivery and
reaping), and the image itself is often minimal (musl/Alpine, sometimes no
shell at all). None of this is new machinery — every fact below is already
documented in [Platform support](platform-support.md) — this page is the
container-shaped tour through it, with the Dockerfile fragments and gotchas
that only show up once you actually run the crate inside `docker run` /
Kubernetes.

- [Which containment mechanism you get](#which-containment-mechanism-you-get)
- [PID 1: signals, zombies, and what's contained](#pid-1-signals-zombies-and-whats-contained)
- [Graceful shutdown on the orchestrator's `SIGTERM`](#graceful-shutdown-on-the-orchestrators-sigterm)
- [Minimal images: musl/Alpine, no shell, no `setpriv`](#minimal-images-muslalpine-no-shell-no-setpriv)
- [Container resource limits vs the crate's `limits`](#container-resource-limits-vs-the-crates-limits)

## Which containment mechanism you get

On Linux, `ProcessGroup`'s cgroup backend needs write access to the cgroup v2
hierarchy **at the real hierarchy root** (see
[Platform support → containment mechanisms](platform-support.md#containment-mechanisms)
for exactly why). A plain, unprivileged `docker run` container gets neither:
`/sys/fs/cgroup` is typically mounted **read-only**, so the crate quietly
falls back to the `ProcessGroup` (POSIX process-group) mechanism — kill-on-drop
still works, but the whole-tree accounting and `limits` capabilities of the
cgroup backend do not (confirmed by running the crate inside `docker run`
with no extra flags: `mechanism()` reports `ProcessGroup`, and
`/sys/fs/cgroup` refuses even a `touch`). `--privileged` (or an equivalent
cgroup-namespace delegation) makes the filesystem writable enough for
`mechanism()` to report `CgroupV2`, but the container's cgroup is still a
**namespace root**, not the real hierarchy root — `/proc/self/cgroup` reads
`0::/` either way — so [resource limits](#container-resource-limits-vs-the-crates-limits)
stay unenforceable even then.

```rust,no_run
use processkit::{Mechanism, ProcessGroup};

fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    if group.mechanism() == Mechanism::ProcessGroup {
        // The common case inside an unprivileged Linux container: kill-on-drop
        // still holds (see the next section), but `members()`'s CPU/memory
        // totals and `limits` are not available — see the capability
        // matrices in Platform support. `CgroupV2`/`JobObject` get those too.
    }
    Ok(())
}
```

Never assume a mechanism; **check `mechanism()`** if your service's behavior
must not silently degrade (e.g. it relies on `limits` for sandboxing an
untrusted child) — see
[Container resource limits](#container-resource-limits-vs-the-crates-limits)
for the fail-fast alternative when a cap truly matters. Whichever mechanism
you land on, whole-tree kill-on-drop itself is unconditional — the fallback
only narrows *accounting and limits*, never containment.

**Preflight without spawning anything.** A container entrypoint often runs a
host-check / *doctor* step before it starts any real work, whose whole point is
to have **no side effects** — no child spawned, no container created. Such a step
can't build a `ProcessGroup` just to read `mechanism()`, so use
`host_containment()` instead: it reports the mechanism a group *would* get here (plus the reach of a
soft stop and abrupt-owner-death cleanup, and the crate version) from cheap
read-only checks, creating nothing — on Linux specifically, no new cgroup
directory. The mechanism it predicts is the same one a real `ProcessGroup::new`
selects; the Linux cgroup answer is best-effort (it checks whether a cgroup
*could* be created rather than creating one), so in the rare window where a
writable-looking cgroup then rejects creation the live `mechanism()` is the final
word.

```rust,no_run
fn main() {
    let host = processkit::host_containment();
    // Log the containment story a run *would* get — no container created, no
    // process spawned (on Linux: no cgroup directory left behind):
    println!(
        "mechanism={:?} parent_death={:?} processkit={}",
        host.mechanism(),
        host.parent_death_cleanup(),
        host.crate_version(),
    );
}
```

## PID 1: signals, zombies, and what's contained

Inside a container your process is almost always PID 1 — Docker and
Kubernetes don't run a real init unless you ask for one. PID 1 has two kernel
duties an ordinary process doesn't: it's the implicit **reparent target** for
every orphaned descendant in the container's PID namespace, and — for signals
without an installed handler — some default dispositions are ignored instead
of applied (irrelevant to `SIGKILL`/`SIGSTOP`, which are never blockable, but
relevant to graceful termination — see the [next section](#graceful-shutdown-on-the-orchestrators-sigterm)).
Neither duty is something `processkit` does for you, and neither needs to be:

- **Containment (killing the tree) is unaffected.** The whole point of the
  cgroup/pgroup mechanisms is that `kill_all`/`shutdown`/`Drop` reach every
  descendant, including ones spawned after your direct child — that's true
  whether or not your process happens to be PID 1.
- **Reaping an *orphan that isn't yours* is not.** A grandchild your own
  child forked and then exited without waiting for still needs *someone* to
  call `wait()` on it once it dies, or it lingers as a zombie
  (`kill(pid, 0)` still reports it alive) forever. Once that grandchild
  reparents to PID 1, "someone" has to be PID 1 itself — and an ordinary
  process, `processkit`-managed or not, doesn't indiscriminately reap
  processes it never spawned.
- **If *your own* process dies abruptly (SIGKILL), its `Drop` never runs**, so
  the cgroup/pgroup teardown that normally reaps the tree does not fire. The
  opt-in [`kill_on_parent_death()`](commands.md#privileges-and-spawn-flags)
  hardens this case, but its reach is platform-limited and reported honestly by
  `Command::kill_on_parent_death_scope()` — `WholeTree` on Windows,
  `DirectChildOnly` on Linux (the direct child dies; a surviving cgroup keeps
  grandchildren alive), `Unsupported` on macOS/BSD. There is no portable Unix
  kernel primitive that tears a whole tree down on its creator's abrupt death;
  when that guarantee matters, keep an outer container/cgroup (or a subreaper)
  that owns the tree independently of your process.

**Why there's no "reattach to the leaked tree by its id" escape hatch.** On Linux
the surviving cgroup *could* in principle be reaped by a later process that knew
its path (`cgroup.kill` acts on the whole subtree) — so it's natural to wish for a
stable container identifier you could record and later reattach to. The crate
deliberately does **not** expose one, because such an identifier cannot be made
**identity-safe** across reuse: a cgroup path (or a Windows Job Object name) is
freed when its container is torn down and can be handed to an *unrelated* later
container, so acting on a recorded id risks killing an **innocent** tree. Nothing
the crate can stamp on the container fixes this — a cgroup directory has nowhere to
hold a crate-chosen marker, its pseudo-filesystem inode/birth-time are not a
contractual identity, and giving a second process a live Windows job handle would
itself defeat the `KILL_ON_JOB_CLOSE` kill-on-owner-death that makes the abrupt-death
case *already* clean on Windows (the kernel reaps the whole tree there, so there is
no leaked tree to reattach to in the first place). The honest remedy is the same one
above: have an **external owner** — a subreaper, or a delegated `systemd` scope — own
the tree independently of your process, so its death, not a fragile recorded id, is
what triggers cleanup.

This is exactly the gap [Platform support's CI section](platform-support.md#ci-coverage)
documents and works around with `--init` for the crate's *own* test suite —
and it reproduces identically for any container. Run a process that
orphans a short-lived grandchild as the container's PID 1 with a plain
`docker run` (no `--init`), and the grandchild is left a permanent zombie
after it exits; the identical container run with `--init` (which runs
[tini](https://github.com/krallin/tini) as the real PID 1, a subreaper) shows
no zombie at all — confirmed by running both side by side. Baking `tini` into
the image itself works identically, and is the only option on Kubernetes,
which has no `--init`-equivalent flag:

```dockerfile
FROM alpine:latest
RUN apk add --no-cache tini
COPY my-app /usr/local/bin/my-app
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/my-app"]
```

(Confirmed against a real `pk-container-demo` image built from this exact
pattern: with `tini` as `ENTRYPOINT`, an orphaned grandchild is reaped with no
`--init` flag at all; the same image invoked without `tini` — the bare binary
as PID 1 — leaks the identical zombie that the plain `docker run` case above
does.) `sh -c 'sleep 0.3 &'`-style orphans are the illustrative worst case;
in practice this only bites processes with descendants that outlive their
direct parent — most single-service containers with no forking children
never hit it. When in doubt, run with a subreaper as PID 1; it's a no-op cost
otherwise.

## Graceful shutdown on the orchestrator's `SIGTERM`

Docker (`docker stop`) and Kubernetes both stop a container by sending
`SIGTERM` to PID 1 — your process — then wait a grace period (Docker's
`--stop-timeout` / `docker stop -t`, Kubernetes'
`terminationGracePeriodSeconds`, both default to a small handful of seconds)
before escalating to `SIGKILL`. That `SIGTERM` targets **your process**, not
the tree `processkit` manages — the two are related but distinct signals:
catching the orchestrator's `SIGTERM` is ordinary application code (e.g.
[`tokio::signal::unix::signal`](https://docs.rs/tokio/latest/tokio/signal/unix/index.html)
with tokio's own `signal` feature, or a crate like `signal-hook`); reacting to
it by tearing down the child tree gracefully is
[`ProcessGroup::shutdown`](process-groups.md#tearing-down-drop-terminate-shutdown)
(or [`RunningProcess::shutdown`](streaming.md#lifecycle) for a single
`start()`ed service): `SIGTERM` the tree, wait up to `shutdown_timeout`, then
`SIGKILL` any survivor.

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _server = group.start(&Command::new("app-server")).await?;

    // Left to the application: install a handler for the orchestrator's own
    // SIGTERM (tokio::signal::unix::signal(SignalKind::terminate()) with
    // tokio's `signal` feature, or the `signal-hook`/`ctrlc` crates) and
    // resolve this future when it fires.
    wait_for_orchestrator_sigterm().await;

    // SIGTERM the tree, wait shutdown_timeout, SIGKILL stragglers:
    group.shutdown().await?;
    Ok(())
}

async fn wait_for_orchestrator_sigterm() {
    // …
}
```

(This exact pattern — `tokio::signal::unix::signal(SignalKind::terminate())`
followed by `group.shutdown()` — was built and run for real as a small
`processkit`-consuming binary: `docker stop` on the resulting container
delivered `SIGTERM` to PID 1, the handler fired, `shutdown()` tore the tree
down, and the process exited well inside Docker's default 10-second grace —
no `SIGKILL` needed.)

To *observe* that teardown — log whether the tree drained within your grace or had
to be hard-killed, export the elapsed as a shutdown metric, or drive your own race
between the orchestrator's `SIGTERM` and a control-socket "drain" command — use the
`process-control` [`ProcessGroup::stop(grace, escalate)`](process-groups.md#observing-the-teardown-stop-and-shutdownreport)
in place of `shutdown()`: the same teardown, returning a `ShutdownReport` (the
attempted soft signal, member counts before/after, drained-within-grace vs
escalated, and the actual elapsed). `stop(Duration::ZERO, true)` additionally gives
a "kill and wait for the tree to actually empty" that bare `kill_all` — which
returns as soon as the kill is *issued* — does not.

Two things worth setting deliberately, both already documented at the
`ProcessGroup`/`Command` level:

- **The orchestrator's grace period must exceed your own.** If
  `shutdown_timeout` (or a per-`Command` `timeout_grace`) is longer than
  Docker's `--stop-timeout` / Kubernetes' `terminationGracePeriodSeconds`,
  the *orchestrator* sends the hard `SIGKILL` to your still-shutting-down
  process before your own escalation ever gets a chance to run — set the
  outer grace at least as generous as the inner one.
- **A frozen tree can't shut down gracefully.** If you've called
  `group.suspend()`, the frozen processes can't run their `SIGTERM` handler —
  `shutdown()` waits out the whole grace and then hard-kills. Resume first;
  see [Platform support's frozen-tree caveat](platform-support.md#caveats).

## Minimal images: musl/Alpine, no shell, no `setpriv`

Two properties of a lean final image turn out to be non-issues for this
crate specifically, because of *how* it does privilege drop and spawning —
confirmed by building and running against `rust:alpine`/`alpine:latest`:

- **No `setpriv`/`su-exec`/`gosu` needed for `uid()`/`gid()`/`groups()`.**
  The privilege drop is `setgroups` → `setgid` → `setuid` called directly as
  raw syscalls inside the child's `pre_exec` hook (see
  [Running commands → privileges](commands.md#privileges-and-spawn-flags)) —
  the crate never shells out to an external helper binary, so a final stage
  that lacks one entirely still drops privileges correctly. Confirmed by
  running the drop (`.uid(...).gid(...).groups(...)`) inside a container and
  getting back the target identity with no such binary invoked.
- **No shell needed to spawn.** `Command`/`ProcessGroup::spawn` always
  `exec`s the target program directly with an argv array — there is no
  `sh -c` step anywhere in the crate's own spawn path (pipelines wire pipes
  at the OS level, not through a shell either — see [Pipelines](pipelines.md)).
  A `FROM scratch`-style final stage with no shell at all works, *as long as
  the programs you spawn are themselves present* — the crate not needing a
  shell doesn't exempt a command you invoke as `sh -c "…"` yourself.

```dockerfile
# Builder: musl/Alpine — the same base the crate's own CI `test-musl` job and
# `just test-musl` build against (see Platform support → CI coverage), so the
# toolchain and libc pairing is already exercised.
FROM rust:alpine AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# Runtime: no shell, no setpriv/su-exec/gosu — the crate needs none of them.
FROM alpine:latest
COPY --from=builder /src/target/release/my-app /usr/local/bin/my-app
ENTRYPOINT ["/usr/local/bin/my-app"]
```

(Built and run for real, end to end: `cargo build --release` inside
`rust:alpine`, then the resulting binary run from a bare `alpine:latest`
final stage — privilege drop and a no-shell spawn both succeeded exactly as
above.) Add `tini` to the final stage — see [PID 1](#pid-1-signals-zombies-and-whats-contained) — if
anything you spawn can outlive its own children.

## Container resource limits vs the crate's `limits`

The orchestrator's own cgroup limits (Docker's `--memory`/`--cpus`,
Kubernetes' `resources.limits`) apply to **the whole container**, enforced by
the kernel regardless of anything `processkit` does — a container that hits
its memory limit gets OOM-killed independent of any `max_memory` the crate
was asked to set. That outer limit is not the same thing as the crate's own
[`limits` feature](process-groups.md#resource-limits) (`max_memory` /
`max_processes` / `cpu_quota` on `ProcessGroupOptions`), which caps a
specific *tree within* the container and needs this process to sit at the
**real cgroup v2 hierarchy root** — a requirement an ordinary container
essentially never meets, privileged or not (see
[Which containment mechanism you get](#which-containment-mechanism-you-get)).
Confirmed by actually requesting a limit from inside a container:

```text
plain docker run:       ErrorReason::ResourceLimit { kind: Memory, reason: Unenforceable,
                           detail: "…Read-only file system…" }
docker run --privileged: ErrorReason::ResourceLimit { kind: Memory, reason: Unenforceable,
                           detail: "…cgroup v2's 'no internal processes' rule…Resource busy…" }
```

Both fail the *same* way the crate documents for any non-delegated host —
`ErrorReason::ResourceLimit` with `reason: LimitReason::Unenforceable` — never a
silently-unbounded group (see
[Errors → `ResourceLimit`](errors.md) and
[Platform support → containment mechanisms](platform-support.md#containment-mechanisms)
for the delegation requirement in full). In practice: rely on the
orchestrator's own memory/CPU limits as the outer boundary for anything
running in a container, and reserve the crate's `limits` feature for hosts
where this process genuinely owns the cgroup root — a minimal, non-systemd
init on bare metal or a VM, not a container. `mechanism()`/kill-on-drop
containment keep working either way; only the `limits` cap itself is
unavailable.

On a host where the crate's `limits` *do* apply, the caps are not frozen at
creation: [`ProcessGroup::update_limits`](process-groups.md#updating-a-live-group)
re-applies a fresh set to the live tree (a full replacement) for adaptive
tightening or relaxing, and refuses — rather than silently drops — a cap on a
mechanism that can't enforce it, exactly as creation does.

Running something you don't trust inside that container? See [Running
untrusted children](untrusted-children.md) for the full hardening checklist —
containment, resource limits, privilege drop, env hygiene, output/wall-time
bounds — with the platform caveats from this page and
[Platform support](platform-support.md) folded in.
---

Next: [Platform support](platform-support.md) ·
[Running untrusted children](untrusted-children.md) ·
[Process groups](process-groups.md) ·
[docs index](README.md)
