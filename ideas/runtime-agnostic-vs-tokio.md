# Runtime-agnostic vs the tokio coupling — what's worth doing

> **Status:** decision record. Assessed 2026-06-09 after an owner question: rather
> than port the crate from tokio to smol ("just another hard dependency"), make it
> **runtime-agnostic** — abstract the seam over `spawn + timer + io (futures-io) +
> async-process`, accepting the executor from outside or hiding it behind a runtime
> feature flag, so today's tokio consumers are preserved. No concrete blocker drove
> it — it's a "what's in scope" sweep, like the sibling records. Sibling decision
> records: [`architecture-audit-2026-06.md`](architecture-audit-2026-06.md) (the
> standing rejected/confirmed-sound list) and
> [`permissions-privileges-pty-network.md`](permissions-privileges-pty-network.md)
> (whose §3 "no credential seam through `tokio::process::Command`" finding is
> load-bearing here too).

## TL;DR verdicts

| Aspect | Verdict | Rationale |
|---|---|---|
| **Direct tokio → smol port** | **Reject** | Swaps one hard runtime coupling for another *and* loses the existing tokio userbase. The critique that motivated this assessment is correct: a straight port buys nothing. |
| **Timers + futures-io seam** | **Cheap / feasible** | `tokio::time::{timeout,sleep,interval,Instant}` → `futures-timer`; `AsyncRead`/`AsyncWrite` → `futures-io`. Genuinely abstractable — the easy 20%. |
| **Child spawn/reap as an injected executor** | **Not feasible** | Async `child.wait()` needs a reactor watching SIGCHLD (Unix) / the process handle (Windows). There is **no** portable "inject your executor" trait for child-process readiness. This is the hard 80%, and the crate exists *for* this. |
| **`async-process` as the runtime-agnostic backend** | **Just another hard runtime** | `async-process` brings its **own** reactor (`async-io`/`polling`). "Agnostic" resolves to depending on async-io's stack instead of tokio's — the same завязка one layer down, now behind a feature flag with double the test matrix. |
| **Windows Job-Object containment on a non-tokio backend** | **Unproven / correctness risk** | The kill-on-drop-the-whole-tree promise (the README headline) is welded to a tokio-`Child`-specific spawn seam. A non-tokio backend must re-prove it; if it can't, the guarantee silently weakens. This is the gating spike, not an afterthought. |
| **Overall** | **Defer** | Largest possible core churn, doubled CI matrix, public-API abstraction-leak decisions — all for a **hypothetical** consumer. No concrete non-tokio user exists. |

---

## 1. Direct tokio → smol port — Reject

The motivating critique is sound: porting to smol is *itself* a hard dependency,
not the removal of one, and it strictly loses the tokio consumers the crate has
today (it supersedes the sibling `vcs-process` crate; those wrappers are
tokio-based). If anything is worth doing, it is the agnostic version below — not a
port. The rest of this record assesses that.

## 2. Timers + futures-io — the abstractable part

These are the pieces the "spawn + timer + io" framing gets right, and they are the
*easy* ones:

- **Timers** — `tokio::time::timeout` / `sleep` / `sleep_until` / `interval` /
  `Instant` appear across `src/running/mod.rs`, `src/command.rs`,
  `src/pipeline.rs`, `src/running/probes.rs`, `src/runner.rs`, `src/stats.rs`,
  `src/supervisor.rs`, and `src/sys/{linux,pgroup}.rs`. All have runtime-neutral
  analogues (`futures-timer`, or a generic timer trait).
- **IO** — the line pump (`src/pump.rs` `pump_lines<R: AsyncRead>`), stdin sources
  (`src/stdin.rs`), and stream reads are written against `tokio::io` traits that
  map onto `futures-io`'s `AsyncRead`/`AsyncWrite`.

If the rest were as clean as this, the idea would be straightforward. It isn't.

## 3. Child spawn / reap — not an "inject the executor" problem

The crate's spawn primitive is `tokio::process::{Command, Child}` and it is
load-bearing everywhere: `src/command.rs` `build_tokio`, `ProcessGroup::spawn`
(`src/group.rs`), every `sys/*.rs` backend, and the `RunningProcess` lifecycle
(`child.start_kill()` / `child.wait().await` / `try_wait()` in
`src/running/mod.rs`).

The catch is that **async child reaping is inherently runtime-coupled.** Making
`child.wait()` a future requires a reactor that watches SIGCHLD on Unix and the
process handle on Windows; tokio's `process` feature *is* that reactor wiring.
There is no portable trait by which a caller "passes in their executor" and gets
async child-process readiness — task-spawning and timers can be injected, child
reaping cannot. So the seam the idea wants to keep thin (`async-process`) is not a
thin shim over an external executor; it is a second, complete runtime.

## 4. `async-process` is just another hard runtime

The only off-the-shelf runtime-neutral child API is `async-process`, and it brings
its **own** reactor stack (`async-io` → `polling`). Adopting it does not remove a
hard async dependency — it replaces tokio's with smol's, exactly the outcome the
original critique flagged for the direct port, only now hidden behind a `runtime`
feature flag. The cost side is also concrete:

- **Task spawning** — `tokio::spawn` / `JoinHandle` / `.abort()` / `tokio::select!`
  drive every background pump, sampler, deadline, and cancel task
  (`src/running/mod.rs`, `src/pump.rs`, `src/running/stream.rs`,
  `src/pipeline.rs`, `src/runner.rs`). Each needs a runtime-neutral spawn/join/
  abort/select equivalent.
- **Sync** — `tokio::sync::Notify` (`src/pump.rs` `SharedLines`) and the async
  `tokio::sync::Mutex` (`src/stdin.rs`) need neutral replacements;
  `tokio::io::duplex` (scripted-double pipes in `src/running/mod.rs`) too.
- **CI** — the matrix already spans Windows + Linux×2 + macOS. A `runtime`
  dimension doubles it.

## 5. Windows containment is welded to the tokio `Child` — the real risk

This is the decisive point, because containment is the crate's entire reason to
exist. On Windows the spawn is race-free *only* because it goes
`CREATE_SUSPENDED → AssignProcessToJobObject → resume`, and that sequence reaches
directly into the tokio `Child`'s raw OS handle:

- `src/sys/windows.rs:155-203` — `Job::spawn` sets `creation_flags(CREATE_SUSPENDED
  | …)`, spawns, then calls `child.raw_handle()` (a `tokio::process::Child`
  method) and `child.id()` to assign the process to the Job *before* resuming its
  primary thread. The in-code comment is explicit that this closes the
  spawn→assign escape window.

A non-tokio backend (`async-process` / smol) must reproduce **all** of: create the
child suspended, extract its raw handle/pid *before* it runs, and resume after the
Job assignment. If its `Child` doesn't expose that seam — and §3 of
[`permissions-privileges-pty-network.md`](permissions-privileges-pty-network.md)
already records that `tokio::process::Command` itself has "no credential seam" for
the analogous run-as-user case — then kill-on-drop over the whole tree silently
degrades on that backend. That is not a refactor risk; it is a correctness risk to
the headline guarantee, on the platform where escape is hardest to detect.

## 6. Public-API abstraction leaks

A few public signatures expose tokio directly; each becomes an abstraction-leak
decision under any agnostic scheme:

- `Command::to_tokio_command() -> tokio::process::Command` (`src/command.rs:624`) —
  an explicit escape hatch that names the runtime in its return type.
- `pub use tokio_util::sync::CancellationToken;` (`src/lib.rs:373`, behind the
  `cancellation` feature) — re-exports a tokio-util type as the crate's
  cancellation currency.
- `Stdin::from_reader<R: AsyncRead>` (`src/stdin.rs:76`) — a public generic bound
  on `tokio::io::AsyncRead`.

(`Command::set_pipe_stdin` `src/command.rs:334` also carries an `AsyncRead` bound
but is `pub(crate)`, so it's an internal detail, not a public leak.)

## 7. Why defer (cost vs consumer)

The change touches essentially every core file — `command.rs`, `runner.rs`,
`running/mod.rs`, `pump.rs`, `pipeline.rs`, `supervisor.rs`, `running/probes.rs`,
`stats.rs`, and all of `sys/` — doubles the CI matrix, and forces the §6 API
decisions. There is **no concrete non-tokio consumer**. This repo's discipline is
to defer even smaller asks until a real consumer appears: PTY (~2–3k LoC) is
deferred for exactly that reason in the sibling record. A full-core runtime
rearchitecture has a *weaker* justification than PTY and a *larger* blast radius.

## If a real consumer ever appears

Record the realistic path so it isn't re-derived from scratch:

- **Not** executor-injection. The design is a `runtime` cargo feature selecting one
  of **two concrete backends** — the existing tokio one, and an
  `async-process`-based one — behind a thin internal `spawn / wait / timer / io`
  facade. Be honest that "agnostic" here means "two maintained backends," not "no
  runtime."
- **Gate on the Windows seam first.** The first spike re-validates that the
  non-tokio `Child` can be created suspended and have its raw handle/pid extracted
  *before* resume, so `CREATE_SUSPENDED → AssignProcessToJobObject → resume`
  (`src/sys/windows.rs:155-203`) still holds. If it can't, the effort dies there —
  no point abstracting timers and IO around a backend that can't contain a process
  tree on Windows.
- Then the cheap parts (§2) and the task-spawning/sync replacements (§4) follow.

## Revisit when

A concrete smol / async-std / embassy consumer needs ProcessKit and genuinely
cannot adopt tokio — mirroring the PTY precedent (defer until a real consumer
asks). Until then, the crate stays **explicitly tokio-bound**, as stated in
`README.md` and `src/lib.rs`.
