# Comparative benchmarks

`processkit` adds process-tree containment, decoded line streaming, bounded
capture, and a consistent async API around child processes. Those guarantees
have a cost, so this repository keeps an end-to-end comparison against the
plain Tokio and standard-library APIs. The benchmark is intended to answer
"what does the convenience and safety layer cost for this workload?", not to
declare one API universally faster.

## What is measured

`benches/compare.rs` runs the same real passthrough child (`cat` on Unix,
`cmd /c findstr` on Windows) for every contender. Payload construction is
outside the timed sections where possible. Each sample includes child spawn,
pipe setup, stdin/stdout transfer, output handling, and process wait.
The children are intentionally trivial: they only echo stdin to stdout, so the
measurement is about process handling rather than application work.

| Scenario | Children | Input / output | ProcessKit path | Plain baselines |
|---|---:|---:|---|---|
| Small capture | 1 | 32 bytes, 3 lines | `Command::output_string` plus `stdout(StdioMode::Null)` + `start().wait()` to isolate capture cost | `tokio::process::Command` and `std::process::Command` |
| Large streaming | 1 | 1,032,000 bytes, 8,000 lines of 128 bytes | `start` + `stdout_lines` | async Tokio line reader and synchronous `BufRead::lines` |
| Concurrent fan-out | 16 | 32 bytes per child | 16 concurrent `output_string` runs | 16 Tokio tasks or 16 standard threads |

The numbers in this table describe fixed workloads, not performance results.
Criterion prints the measured time, throughput, and comparison deltas for each
contender under `target/criterion/`. Those results are machine-dependent:
CPU, OS process-spawn cost, scheduler load, filesystem state, Rust toolchain,
and the available Tokio runtime threads can all change them. Compare contenders
from the same invocation and keep the command line and environment with any
result you publish.

## Running the comparison

Run the complete local suite with:

```text
just bench-compare
```

The equivalent Cargo command is:

```text
cargo bench --bench compare
```

Criterion's normal filtering and measurement options remain available, for
example `cargo bench --bench compare -- stream_large_stdout`. The benchmark
spawns real children and is deliberately not part of the ordinary CI test
gate; use it when comparing a change, a platform, or a toolchain.

## Reading the result

The comparison is meaningful only when the workloads and semantics match:

- `processkit` measures its normal private process-group setup. This is the
  price of its unconditional kill-on-drop tree guarantee, and is not present
  in the plain baselines.
- The small-capture group includes a `processkit_discard_stdout` series using
  `stdout(StdioMode::Null)` + `start().wait()`. Compare it with `processkit` in the same
  group to see the crate's capture/pump cost separately from its private-group
  cost; `StdioMode::Null` is a discard path, so it is not an output-equivalent
  replacement for capture.
- The capture case compares a decoded `ProcessResult<String>` with byte output
  from the plain APIs. The benchmark uses ASCII payloads so decoding does not
  change the transferred content; the APIs still have deliberately different
  result and failure semantics.
- The streaming case consumes every line before waiting for the child. This
  prevents a full stdout pipe from turning the benchmark into a deadlock and
  measures the live line-delivery path rather than only a bulk read.
- The fan-out case gives each API its normal concurrency primitive: Tokio tasks
  for async contenders and scoped standard threads for `std`. It measures the
  whole batch, not a synthetic per-child loop.

If a change improves one scenario and regresses another, report the scenario
and its Criterion output rather than collapsing the results into one score.
Containment, streaming, and typed outcome handling are features; a faster
plain spawn is not automatically a better substitute for them.
