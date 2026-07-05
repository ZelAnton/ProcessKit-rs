# next: vcs-toolkit-rs feedback

> **RESOLVED 2026-06-29 (in [Unreleased], unshipped).** All of A–E shipped
> (`Error` stream accessors + `code`/`signal`/`program`/`is_timeout`/`is_cancelled`;
> `#[doc(hidden)]` `Error::{exit,timeout,signalled}`; `Invocation::{env,env_is,has_env}`;
> `ProcessResult::output_contains_any`). Two of the three reinforced ideas shipped:
> `RetryPolicy` (exponential+cap+jitter) + `Command::retry_with` + `CliClient::default_retry`
> (later-retry-jitter), and `CliClient::default_env_fn` (later-extensibility-hooks). The
> third — a first-class `Secret` env type (later-buffer-policy-seam strand) — was
> **deferred**: see [`../decisions/secret-type-deferral-2026-06.md`](../decisions/secret-type-deferral-2026-06.md);
> shipped secret-handling docs on `Command::env` instead. Nothing here is breaking.

> **Status:** open idea (next), raised 2026-06-28 by the `vcs-toolkit-rs` team.
> From the toolkit that wraps `git`/`jj`/`gh`/`glab`/`tea` as typed async Rust on
> top of `processkit` (currently pinned `= "0.11.0"`; every "processkit has / lacks
> X" claim below was re-verified against the **1.0.1** dev tree, so they're current).
> **None are bugs — the crate is excellent.** These are ergonomics for the
> "someone wraps N CLIs over this" consumer, toward code that's simpler, more
> reliable, more extensible, and more testable. I checked `ideas/` + `decisions/`
> first so this neither duplicates an open idea nor re-litigates a settled one;
> where it touches an existing idea I say so and add the missing dimension / a
> concrete consumer (Later items are gated on exactly that).

## Why a multi-CLI wrapper is useful feedback

A binding stresses an API's *shape* (see `next-python-binding-feedback.md`); a
wrapper over **five** CLIs stresses a different axis — the **error / result /
testing** surface, exercised against five different tools' exit-code and
stderr conventions. Every place the toolkit had to **destructure**, **re-implement**,
or **hand-roll a constructor** marks a spot worth an accessor or a helper. Items are
ordered by how much consumer code (and `#[non_exhaustive]`-bump fragility) they remove.

## Already-open ideas this reinforces (a concrete consumer, + the missing dimension)

These exist in `ideas/`; I'm not re-filing them — just confirming a real consumer and
noting where our use needs slightly more than the current sketch:

- **`later-retry-jitter.md`** (jitter on backoff; already from us) — confirmed: at
  agent-workspace fan-out scale the fixed-backoff fetch retry thunders. But our use
  needs two more dimensions the jitter note doesn't cover. (1) **Exponential backoff
  + a cap**, not just jitter on a fixed delay. (2) **Client-wide** retry:
  `Command::retry(max, backoff, retry_if)` (`src/command.rs`) is fixed-backoff *and*
  per-command, so to retry *every* verb of a client on lock contention we hand-rolled
  a parallel engine — `RetryPolicy` + `backoff_for` + `full_jitter` + `retry_async`
  (`vcs-cli-support/src/lib.rs:408-541`, ~130 lines) — and wrap each verb in it. A
  `CliClient::default_retry(RetryPolicy)` (client-wide, like `default_timeout`) plus
  an exponential+cap+jitter `RetryPolicy` would delete all ~130 lines and is half the
  reason our `ManagedClient` exists. *(Additive.)*

- **`later-extensibility-hooks.md`** (`before_spawn` raw-`Command` mutator) — we are a
  concrete consumer. The *other* half of why `ManagedClient` exists is per-invocation
  secret/env injection: `CliClient` has `default_env` (static, client-wide) but no way
  to compute an env value *per spawn* from a resolver, so we hand-wrote a wrapper that
  re-implements every verb just to `cmd.env(var, secret.expose())` first
  (`vcs-cli-support/src/lib.rs:682-691`, one line of real work per verb). A
  `before_spawn`/`default_env_fn(key, resolver)` client hook would let us drop the
  whole `managed_client!` macro (`lib.rs:200-267`, which itself only re-creates the
  `new`/`with_runner`/`default_*` that `cli_client!` already generates).

- **`later-buffer-policy-seam.md`** (redaction-at-capture) — relatedly, processkit
  redacts env **values** in `Debug` by convention but has no `Secret` *type*. We built
  one (`vcs-cli-support/src/credentials.rs:47-87`: redacts `Debug`+`Display`, no `Eq`
  oracle) plus a `CredentialProvider` trait "modelled on processkit's `ProcessRunner`
  pattern" (our CHANGELOG's words). A first-class `Secret` newtype accepted by
  `Command::env` would let processkit *type* the values it already redacts, and every
  CLI wrapper would stop re-inventing it. (The `git -c credential.helper=!f(){…}`
  trick and the per-forge `GH_TOKEN`/`GITLAB_TOKEN` mapping stay ours — those are
  git/forge-specific.) *(Additive.)*

## High-impact — the `Error` accessor family (removes destructures + insulates the tree)

This is the strongest ask and lines up exactly with the 1.1.0 direction (accessor-front
public fields; never make a consumer `match` a `#[non_exhaustive]` type with a
wildcard). `Error` today offers `is_not_found()` / `is_permission_denied()` /
`is_transient()` / `diagnostic()` (`src/error.rs`) — but the stream-bearing variants
(`Exit`/`Timeout`/`Signalled`) can only be *read* by destructuring. So consumers that
classify on output destructure variants directly, which is both repetitive and
**source-breaking every time a `#[non_exhaustive]` variant gains a field** (each of
0.8→0.10→0.11 did exactly this to us; the only logic that rode the bumps free was what
sat behind our own `is_*()` accessors).

### A. Output accessors across the stream-bearing variants
*Affects: 3+ direct destructures, 3 reimplemented stream-scanners · Additive · Cost: small*

`Error::diagnostic()` returns only **one** stream (trimmed stderr *else* stdout), so a
marker that lands on stdout while stderr is non-empty is missed — which is precisely
why we destructure both fields instead:

- `vcs-cli-support/src/lib.rs:335` — `let Error::Exit { stdout, stderr, .. } = err else …`
- `vcs-forge/src/error.rs:67` — `Error::Forge(Error::Exit { stdout, stderr, .. }) => …`
  (a *second*, different-shaped reimplementation: `format!("{stdout}\n{stderr}")`)
- a *third* shape scans `ProcessResult` streams in `github`/`jj` (see E).

> **Ask:** `Error::stdout() -> Option<&str>`, `stderr() -> Option<&str>`, and
> `combined() -> Option<String>` (the `Exit`/`Timeout`/`Signalled` streams; `None`
> elsewhere) — the `Error` twin of `ProcessResult::combined()`. Collapses the three
> reimplementations into one and removes the destructures.

### B. `exit_code()` and `is_timeout()`
*Affects: many `matches!`/destructures that read one field · Additive · Cost: trivial*

There is no `exit_code()` (callers must `match Error::Exit { code, .. }`) and no
`is_timeout()` — the crate's own docs tell consumers to compose the latter by hand
(`src/error.rs`: *"compose it if wanted: `… || matches!(e, Error::Timeout { .. })`"*),
which is a standing admission of a missing accessor. We do exactly that at
`vcs-cli-support/src/lib.rs:367`, and read codes via `Error::Exit { code, .. }` matches
in tests across `git`/`github`.

> **Ask:** `Error::exit_code() -> Option<i32>` and `Error::is_timeout() -> bool`.
> Together with A, these let a consumer read everything off `Error` through accessors
> and **stop destructuring** — which also makes a future 1.x field addition a non-event
> for the whole re-exporting dependent tree.

## Testing ergonomics (the double surface is great; two rough edges)

The doubles are excellent — `Reply::{ok,fail,timeout,signalled,pending,lines}`,
`ScriptedRunner::{on,on_sequence,when,fallback}` (program-led prefix matching is
exactly right), `RecordingRunner`, `Invocation::{args_str,has_flag}`. Two edges:

### C. An env-assertion twin of `has_flag`
*Affects: ~15+ hand-rolled closures in one file · Additive · Cost: trivial*

`Invocation` exposes `has_flag(name)` for argv but nothing for env, so every env
assertion is a raw `envs.iter().any(|(k,v)| k.to_str()==Some("LC_ALL") && v…== Some("C"))`.
That shape repeats 15+ times in `vcs-git/src/lib.rs` alone (e.g. L2378, L2760, L2844,
L2877, L3197, L3415, L3613, L3725, L3869) — we force `LC_ALL=C`, `GIT_EDITOR=true`,
`--no-color` etc. and assert each was injected.

> **Ask:** `Invocation::env(name) -> Option<Option<&OsStr>>`, `env_is(name, value) -> bool`,
> and `has_env(name) -> bool` — the env analogue of `has_flag`.

### D. Stable constructors for the stream-bearing error variants
*Affects: every custom fake + classifier test · Additive (or `#[doc(hidden)]`) · Cost: small*

A custom `ProcessRunner` fake or an error-classifier test must spell out the full
struct literal `Error::Exit { program, code, stdout, stderr }`
(`vcs-watch/src/lib.rs:1010-1015`, and every classifier test in
`vcs-cli-support/src/lib.rs:838-1056`). Because `Error` is `#[non_exhaustive]`, any new
field on `Exit`/`Timeout`/`Signalled` breaks all of these at once, with no insulating
builder.

> **Ask:** test/`#[doc(hidden)]` constructors — `Error::exit(program, code, stdout, stderr)`,
> `Error::timeout(program, dur, stdout, stderr)`, `Error::signalled(program, sig, stdout, stderr)`
> — so doubles and classifier tests stop spelling out literals that field additions break.

## Smaller / consistency

### E. `ProcessResult::output_contains_any(&[&str])`
*Affects: the "exit code means X unless a stderr marker says Y" idiom · Additive · Cost: trivial*

`ProcessResult` has `combined()` and `diagnostic()` but no case-insensitive contains
helper, so the lenient "one specific non-zero is benign per a stderr marker" path
re-lowercases a stream by hand each time — byte-identical in `vcs-github/src/lib.rs:728-757`
(`pr_checks`, `"no checks reported"`) and `vcs-jj/src/lib.rs:1017-1031` (`resolve_list`,
`"no conflicts"`). (We deliberately *don't* want `ok_codes` for these; the only wart is
the manual `to_ascii_lowercase().contains`.)

> **Ask:** `ProcessResult::output_contains_any(&[&str])` (case-insensitive, both
> streams) — the lenient-path twin of A's `Error` helper.

Note on division of labor: the **marker strings** (git/jj/gh/glab grammar like
`"index.lock': file exists"`, `"http 401"`, `"nothing to commit"`), the per-forge env
mapping, the git inline `credential.helper` trick, and the `LC_ALL=C`/`GIT_EDITOR`
*values* are correctly CLI-specific and **stay with us**. Every ask above is about the
generic *mechanism* underneath, not the CLI grammar on top.

## Please KEEP — already ideal for a wrapper, do not "fix"

- **The `ProcessRunner` seam + `with_runner` + full-fidelity `ScriptedRunner`/`RecordingRunner`.**
  This is what makes the whole toolkit testable: we re-export the doubles as our own
  `testing` surface and build clients/facades over a scripted runner with zero real
  subprocesses. `ScriptedRunner::on()` matching a **program-led** argv prefix is exactly
  right (a rule for `git status` must not answer `rm status`). Keep the doubles in
  lockstep with the real runner's verb set.
- **The by-value `Command` builder + `IntoCommand` (arg-list *or* `Command`).** Maps
  cleanly onto our typed verbs and our `at_forwarders!`/`managed_client!` macros.
- **`#[non_exhaustive]` on `Error`/`Outcome` — *with* accessors.** Keep the
  forward-compat; the asks in A/B are what let us *use* it without destructuring.
- **The uniform run vocabulary across `Command`/`CliClient`/`Runner`**
  (`run`/`run_unit`/`output_string`/`checked`/`exit_code`/`probe`/`parse`). `run`
  already trims + errors on non-accepted exit, and `output_string`/`checked` give the
  full result — we wrap these only for the cross-cutting concerns above, never because
  a flatten-to-string convenience is missing. Keep it identical across layers.
- **`processkit` itself being re-exportable.** We re-export it (`vcs_core::processkit`
  etc.) so our consumers name `Error`/`ProcessResult`/`ProcessRunner`/`CancellationToken`
  without a direct dep — please keep these nameable at the crate root.

## Assessment

The **`Error` accessor family (A + B)** is the highest-value, best-aligned ask: it
deletes every direct `Error::Exit { … }` destructure, collapses three reimplemented
stream-scanners into one, and — because we re-export `processkit::Error` through our
facade errors — insulates the entire dependent tree from the next `#[non_exhaustive]`
field bump. C/D are pure testing wins; E is a small twin of A. The two biggest *line*
removals (retry, per-spawn injection) live in the already-open `later-retry-jitter` /
`later-extensibility-hooks` ideas — this file just confirms a concrete consumer and the
missing dimensions (exponential+cap+client-wide retry; a typed `Secret` in env). Nothing
here is a new subsystem; all of it is additive and fits the existing seams.

## Revisit condition

Pick up A/B/C/D/E opportunistically when next touching `error.rs` / `result.rs` /
`doubles.rs` — all additive, all 1.x-safe. When the retry / `before_spawn` / `Secret`
dimensions land in their respective ideas, re-sync `vcs-cli-support` to delete
`RetryPolicy`/`retry_async` and shrink `ManagedClient`/`managed_client!`.
