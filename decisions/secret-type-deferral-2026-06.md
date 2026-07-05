# Decision: no first-class `Secret` env type in 1.x (defer / likely decline)

> **Status:** decided 2026-06-29 (local-only). Outcome of the `next-vcs-toolkit-feedback.md`
> additive sweep — the last of its three reinforced ideas (`later-buffer-policy-seam`,
> the `Secret`-in-`env` strand). The other two strands shipped: exponential+cap+jitter
> `RetryPolicy` + `CliClient::default_retry` (later-retry-jitter), and
> `CliClient::default_env_fn` (later-extensibility-hooks). Items A–E shipped too.

## The ask

`vcs-cli-support` built its own `Secret` (`credentials.rs`: redacts `Debug`+`Display`,
no `Eq` oracle) plus a `CredentialProvider` trait, and asked processkit to own a
first-class `Secret` newtype **accepted by `Command::env`**, so "every CLI wrapper would
stop re-inventing it" and processkit could *type* the env values it already redacts.

## Decision

Do **not** add a bespoke `Secret`/`Redacted` type in 1.x, and do not pre-commit it to
2.0. Instead ship **secret-handling guidance** in the public docs (the committable Stage-5
deliverable) and revisit the type only on stronger, multi-consumer demand.

Shipped instead (doc-only, `public-api.txt` unchanged):
- `Command::env` gains a **Secrets** note: env values are redacted in `Debug`/tracing;
  bring your own `secrecy`/`zeroize` and pass the exposed value; prefer env/stdin over
  argv (argv is world-readable via the OS process table); use `default_env_fn` for a
  per-run rotating secret.
- `CliClient::default_env_fn` back-references that note.

## Why defer rather than implement

1. **Ecosystem standard already exists.** `secrecy` + `zeroize` are the de-facto typed-
   secret crates. A library-owned `Secret` *fragments* — a consumer already holding a
   `secrecy::SecretString` would juggle two incompatible secret types and convert at the
   boundary. Interop (accept `secrecy::SecretBox` behind a feature) couples us to another
   crate's API and adds a feature+dep; not worth it for one consumer.
2. **The name over-promises.** A type called `Secret` implies memory zeroization. A
   redaction-only newtype (what the consumer built, and the only thing cheap to add) is
   misleading. Real zeroize means a new dependency and freezing a *security contract*
   under freeze-deadline pressure — exactly when not to.
3. **The marginal value is small.** processkit already redacts env *values* in `Debug`
   (via `redacted_env_names`) and in cassettes (names-only), and never emits them via
   tracing; argv is reduced to a count in `Debug` and never emitted in tracing, but is
   deliberately exposed verbatim by `command_line()` and cassette recording. Redaction
   covers the *logging* leak vector for env values — which is the consumer's stated
   motivation ("type the values it already redacts"); it is not memory hygiene and does not
   cover argv at those explicit escape hatches. So a `Secret` *type* adds little to the
   env-value path the ask is about.
4. **The real pain is already gone.** The consumer's ~130-line retry engine is deleted by
   `RetryPolicy`/`default_retry`; the per-spawn-secret-injection boilerplate (re-implementing
   every verb to `cmd.env(var, secret.expose())`) is deleted by `default_env_fn`. The
   `Secret` *type* is the small residue, not the pain.
5. **Asymmetry favors waiting.** Adding a type later is non-breaking; freezing a wrong
   security primitive is forever. One consumer's bespoke type is not enough signal to
   freeze processkit's.

## Design forks (recorded for a future revisit)

If demand grows, these are the freeze-permanent decisions to make deliberately:
- **Name:** `Secret` (familiar, over-promises) vs `Redacted<T>` (honest about redaction-only).
- **Zeroize:** none / mandatory (`zeroize` dep) / optional feature. Mandatory is the only
  one that earns the name `Secret`; it's also the heaviest commitment.
- **Backing:** `OsString` (matches `env`) vs generic `Secret<T>` vs `String`.
- **Access:** `expose()` / `expose_secret()` (match `secrecy`) naming, and whether `Display`
  exists at all (it should not — force explicit exposure).
- **Equality:** no `PartialEq` (avoid an oracle) vs constant-time `ct_eq` (another dep).
- **serde:** opt-out by default; if present, must refuse to serialize or serialize redacted.
- **Acceptance:** a dedicated `Command::env_secret(key, Secret)` (clean, no generic-`AsRef`
  leak) vs making `env` generic (risky for a frozen signature). `Secret: AsRef<OsStr>` is a
  **footgun** — it would let the value leak through any generic sink, defeating the point.

## Revisit when

A *second, independent* consumer asks for the typed secret (not just the injection
mechanism, which `default_env_fn` already provides), or processkit grows a feature that
must carry secret material across its own API surface (not merely into a child's env).
Until then the redaction-by-convention + the `secrecy`/`zeroize` recommendation suffice.
