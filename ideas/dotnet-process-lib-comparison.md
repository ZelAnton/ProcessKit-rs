# Decision record: comparison with a .NET process-management library

> **Status:** decision record / closed. Captured 2026-06-07 after a full
> functional inventory of a proprietary C# process-management library
> against processkit. Outcome: nothing to adopt now; one idea parked, two
> non-goals confirmed. Read this before re-running that comparison.

## What was compared

A small proprietary C# library (~560 lines of library code, ~1200 with
tests; net472 / netstandard2.0 / 2.1): `IProcess`/`ProcessWrapper` over
`System.Diagnostics.Process`, a fluent `ProcessStartArgs` builder, per-line
async output capture via events, and a Windows Job Object with
`KILL_ON_JOB_CLOSE` (returns `null` on non-Windows).

**Finding: it is a strict subset of processkit.** Every capability it has —
spawn, kill-on-drop containment, line-by-line capture, stdin lines, wait with
timeout, exit codes — processkit covers more deeply: bounded
`OutputBufferPolicy` vs their unbounded `ConcurrentQueue`, TERM→grace→KILL
shutdown vs force-kill only, pgroup/cgroup fallbacks vs Windows-only-or-null,
plus cancellation, readiness probes, pipelines, resource limits, supervision,
retry, record/replay. Their test double (`MockProcess` implementing
`IProcess`) mocks at the process level; our `ScriptedRunner` mocks at the
runner seam, which keeps `Command` construction under test — the better seam.
Nothing to take structurally.

The comparison surfaced exactly one gap on our side, fixed alongside this
note: `on_stdout_line`/`on_stderr_line` silently replace a previously set
handler (they support a handler list), and our docs did not say so. Doc-only
fix — "last call wins" is consistent builder semantics; fan-out composes
trivially in one closure.

## Parked idea: named Job Objects

`CreateJobObject(name)` lets a *different* process open the same job by name —
an external watchdog or sibling tool can observe or kill the containment
object without any handle passing. processkit's containers are anonymous.

Why not now:

- **No consumer.** None of our downstream users (vcs-toolkit, vcs-flow,
  agent-workspace) coordinates containment across process boundaries.
- **Platform asymmetry.** Unix has no named handle to a pgroup; a cgroup
  *path* is only a partial parallel (Linux-only, needs delegation, and not
  guaranteed — the mechanism can fall back to a bare process group, which
  has no named handle either). A `name()` API
  that only means something on Windows cuts against the crate's
  one-surface-everywhere posture.

Revisit only when a real cross-process coordination consumer shows up; the
API would likely be an opt-in `ProcessGroupOptions` knob plus an
`open-by-name` constructor, both documented as Windows-mechanism-specific.

## Confirmed non-goals

- **Windows run-as credentials** (`PasswordInClearText`, `LoadUserProfile`
  in `ProcessStartInfo`). Deliberately out: it requires
  `CreateProcessWithLogonW`-family FFI, which does not compose with our
  suspended-spawn → assign-to-job → resume sequence (no `CREATE_SUSPENDED` +
  logon combination through `std::process`), and the security surface
  (cleartext passwords in API) is not one this crate should own. Unix-side
  privilege dropping (uid/setsid) already exists and is the deliberate
  extent of identity control here.
- **Process discovery / enumeration by name** (their tests `KillAll` by
  process name). That is `sysinfo`-crate territory; processkit's half of the
  story — managing an already-known foreign pid — is covered by `adopt`.
