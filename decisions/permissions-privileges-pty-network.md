# Launch permissions, locks, users, SSH/tty, network — what's worth adding

> **Status:** decision record. Assessed 2026-06-08 after a preemptive owner
> question ("can we add anything for: no-launch-permission, locks by other
> processes, running as another user, special SSH processing, network work?").
> No concrete blocker drove it — it's a "what's in scope" sweep. Three angles
> explored (error model + retry, spawn-flags + privilege plumbing, I/O model +
> PTY feasibility). Sibling decision record:
> [`architecture-audit-2026-06.md`](architecture-audit-2026-06.md) — the standing
> "rejected / confirmed-sound" list. The two **Do** items below shipped in the
> companion change (`feat(error,command): spawn-error classifiers +
> Command::groups()`); the rest is recorded here.

## TL;DR verdicts

| Scenario | Verdict | Rationale |
|---|---|---|
| **Launch-permission** (EACCES/ENOENT) | **Done** | The raw `io::Error` was already preserved on `Error::Spawn`; added ergonomic `Error::is_not_found()` / `is_permission_denied()`. |
| **Locks by other processes / transient spawn** | **Done (io-level)** | ETXTBSY / Windows sharing-violation are transient spawn io-errors → `Error::is_transient()`. Locks held by *other* processes aren't our process → out of scope. |
| **Run as another user** | **Done (Unix)** | `uid`/`gid`/`setsid` already existed; added `Command::groups()` (the missing leg of a correct drop). Windows run-as-user = **declined**. |
| **SSH / tty (PTY)** | **Defer** | Pipe model already covers conversational tools + key-auth. Password/passphrase/sudo need a **PTY** — major (~2–3k LoC), architecturally misaligned. Design recorded; revisit on a real consumer. |
| **Network** | **Already covered** | Readiness probes exist; subprocess network *errors* are domain-specific exit codes → downstream. |

---

## 1. Launch-permission errors — Done

**Gap:** `Error::Spawn { program, source: io::Error }` (`src/error.rs`) already
carries the underlying `io::Error`, so a caller *could* distinguish ENOENT from
EACCES via `source.kind()` — but there were **no ergonomic classifiers**; every
caller had to reach into raw `io::ErrorKind`/`raw_os_error`. No "transient"
notion, and `retry` (`src/command.rs`) needs a caller-supplied closure (no
default classifier).

**Shipped:** `Error::is_not_found()` and `is_permission_denied()` over the
`Spawn`/`Io` io-error. A caller can now give a "command not installed?" /
"not executable?" hint without matching raw kinds.

---

## 2. Locks by other processes / transient spawn — Done (io-level)

**The honest split:**
- **ETXTBSY** (Linux: the executable is being written/held) and **Windows
  `ERROR_SHARING_VIOLATION`/`ERROR_LOCK_VIOLATION`** (a file the launch needs is
  briefly locked) surface as spawn io-errors, and a bare retry usually clears
  them → **classify as transient.**
- A file **locked by another, unrelated process** is not our process and not our
  spawn — processkit can't and shouldn't arbitrate it → **out of scope.** (Our
  own teardown already releases the tree's locks via kill-on-drop.)

**Shipped:** `Error::is_transient()` — `EINTR`/`EAGAIN`/busy kinds, plus
`ETXTBSY` (unix) and sharing/lock-violation (Windows). **Scope is io/spawn-level
only:** a tool's non-zero *exit* is never generically transient (a `git` 128 is
domain-specific), so exit-code retryability stays the caller's domain;
`Error::Timeout` is excluded too (compose explicitly). Pairs with
`cmd.retry(n, backoff, |e| e.is_transient())`.

---

## 3. Run as another user — Done (Unix); Windows declined

**Already present:** `Command::uid()` / `gid()` / `setsid()`
(`src/command.rs`, applied in `build_tokio` via `std::os::unix::process::CommandExt`),
with a non-Unix `Error::Unsupported` gate so a privilege drop is never silently
skipped.

**The real gap — supplementary groups.** Dropping the uid *without* clearing the
parent's (often root's) supplementary groups leaves the child able to reach
group-owned resources the target user shouldn't — a genuine privilege-drop hole.

**Shipped:** `Command::groups([gid, …])`. Implementation note worth keeping:
`std`'s `CommandExt::groups` is unstable, **and** `std` applies its own
`setgid`/`setuid` *before* any user `pre_exec` hook — so supplementary groups
(which must precede `setuid` while still privileged) can't be a separate later
hook. When `groups` is set we therefore do the **whole** drop
(`setgroups → setgid → setuid`) in one async-signal-safe `pre_exec` and skip
std's uid/gid path; when `groups` is unset the existing std path is untouched.

**Declined — Windows run-as-user.** `CreateProcessAsUserW` + `LogonUserW` +
token duplication is not reachable through `tokio::process::Command` (the spawn
path is `CREATE_SUSPENDED` → `AssignProcessToJobObject` → resume; no credential
seam). It would be a major, Windows-only mechanism. Out of scope; the non-Unix
gate already returns `Unsupported` for `groups` as it does for `uid`/`gid`.

**Untouched (pre-existing, tracked):** the Linux **cgroup-v2 × uid** interaction
— the cgroup join runs after the uid drops and fails on the root-owned
`cgroup.procs`. Documented on `Command::uid`; `groups` inherits the same caveat
and does not address it.

---

## 4. SSH / tty (PTY) — Defer, design recorded

**Why it comes up:** tools that *demand* a controlling terminal — `ssh`/`sudo`
**password**/passphrase prompts, some credential helpers — detect "not a tty"
and either refuse or hang.

**What already works (no PTY needed):** the I/O model is entirely pipe-based
(`src/pump.rs`, `src/running/mod.rs`) — three independent streams. Conversational
tools that read stdin without a tty work today via `keep_stdin_open` +
`standard_input` + `stdout_lines`; key-based SSH and `BatchMode=yes` need no
prompt. The honest non-interactive guidance is now in
[`docs/commands.md`](../docs/commands.md) (Privileges & spawn flags → Interactive
auth / TTY).

**Why defer the rest:** PTY is **major and architecturally misaligned**
(~2–3k LoC). The crate is built around three independent streams; a PTY **merges
stdout/stderr** onto one master fd and adds terminal line-discipline (echo,
`ICANON`, signals). No prior `ideas/` decision exists, and the audit didn't scope
it. With no concrete consumer (this assessment is preemptive), the cost isn't
justified yet.

**Design sketch (so it isn't re-derived):**
- Spawn: `openpty` (Unix) / `CreatePseudoConsole` ConPTY (Windows) instead of
  three `Stdio::piped()`; a `use_pty` flag on `SpawnOptions` (`src/sys/mod.rs`);
  retain the master fd/handle. Per-backend work in `sys/{unix,linux,windows}.rs`.
- I/O: a new `Backend::Pty` variant beside `Real`/`Scripted`
  (`src/running/mod.rs`); a single pump reads the merged stream — **stderr is no
  longer separable**, and the `on_stdout_line`/`on_stderr_line` split collapses.
- Containment: **no change** — the PTY child still lives in the existing
  job/cgroup/pgroup, so kill-on-drop holds.
- Stdin: one fd; `ProcessStdin::write`/`finish` mostly unchanged, plus termios
  (disable echo) for password entry.
- Testing doubles: `ScriptedRunner` would need a PTY-mode variant.

**Revisit when:** a concrete consumer needs interactive password/passphrase or a
tty-only tool (e.g. an `ssh`/`sudo` flow that can't use key-auth/`BatchMode`).
Then build the minimal single-master-fd PTY mode above — not a general terminal
emulator.

---

## 5. Network — already covered

- **Wait-for-service**: `wait_for_port(addr, within)` /
  `wait_for_line` / `wait_for` (`src/running/probes.rs`) already cover readiness
  with a typed `Error::NotReady`; the probe never kills the child.
- **Network *errors* from a subprocess** (a `git fetch` failing on "could not
  resolve host") arrive as **domain-specific exit codes + stderr**, which
  processkit cannot generically classify — that's the wrapper's job (it knows its
  tool's codes/messages). The only general piece processkit can offer is the
  io-level `is_transient()` from §2, which it now does.

No new network feature is warranted.

---

## Cross-cutting

Two small, additive, in-scope wins shipped (`Error` classifiers + `groups()`);
they serve scenarios 1–3 and the io-level part of 5 with no new dependency
(`libc` is already a unix dep). The two heavy asks — **PTY** and **Windows
run-as-user** — are recorded as defer/decline so they aren't re-litigated from
scratch; PTY has a concrete revisit condition.
