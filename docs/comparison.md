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

The recorded run used an Intel Core i9-9880H CPU with 16 logical processors,
Windows 10 Enterprise 22H2 (build 22621.6060), and Rust 1.96.0. Criterion was
configured in `benches/compare.rs::configure` with `sample_size=20` and
`measurement_time=5s` for each series. The table reports the Criterion mean and
standard deviation; throughput is derived from the fixed workload size (it is
not a separately configured Criterion throughput measurement). Deltas use the
`std_process` result in the same scenario as the baseline. Absolute values are
machine-dependent: CPU, OS process-spawn cost, scheduler load, filesystem
state, Rust toolchain, and available Tokio runtime threads can all change them.
Compare contenders only within the same invocation and retain the command and
environment with any published result.

## Measured results

These measurements were produced by `cargo bench --bench compare` on the host
described above. Times are end-to-end and shown as mean +/- standard deviation.
The throughput units are runs/s for small capture, MiB/s for the 1,032,000-byte
stream, and child processes/s for the 16-child fan-out.

| Scenario | Contender | Time | Derived throughput | Delta vs `std_process` |
|---|---|---:|---:|---:|
| Small capture | `processkit` | 45.049 +/- 2.524 ms | 22.20 runs/s | +101.6% |
| Small capture | `processkit_discard_stdout` | 53.861 +/- 2.365 ms | 18.57 runs/s | +141.1% |
| Small capture | `tokio_process` | 21.765 +/- 1.718 ms | 45.95 runs/s | -2.6% |
| Small capture | `std_process` | 22.341 +/- 0.721 ms | 44.76 runs/s | baseline |
| Large streaming | `processkit` | 56.441 +/- 2.496 ms | 17.44 MiB/s | +125.6% |
| Large streaming | `tokio_process` | 27.162 +/- 2.021 ms | 36.23 MiB/s | +8.6% |
| Large streaming | `std_process` | 25.018 +/- 0.912 ms | 39.34 MiB/s | baseline |
| Concurrent fan-out | `processkit` | 711.350 +/- 83.905 ms | 22.49 children/s | +700.4% |
| Concurrent fan-out | `tokio_process` | 83.680 +/- 15.137 ms | 191.21 children/s | -5.8% |
| Concurrent fan-out | `std_process` | 88.873 +/- 16.349 ms | 180.03 children/s | baseline |

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

## Conclusions

Using a 10% delta as a practical negligible-overhead threshold, no ProcessKit
series in this run was negligible relative to the direct `std_process` API.
The small-capture result was 101.6% slower, large streaming was 125.6% slower,
and concurrent fan-out was 700.4% slower; see the measured times and derived
throughput in the table above. The plain Tokio contender stayed within 10% of
the standard-library baseline in all three scenarios, so the larger deltas are
specific to the ProcessKit path measured here rather than a general async
penalty.

The noticeable cost is the trade-off for ProcessKit's containment, streaming,
bounded-capture, and typed-outcome behavior. Choose ProcessKit when those
guarantees matter; choose a plain process API when minimum process-launch
overhead is the priority and those guarantees are not required. These are
single-machine measurements, so they describe this workload and environment,
not a universal performance ranking. For this Windows run, `std_process` is the
direct `CreateProcess`-based reference; Unix users should rerun the benchmark
before treating it as a comparison with their platform's fork/exec path.

If a change improves one scenario and regresses another, report the scenario
and its Criterion output rather than collapsing the results into one score.
Containment, streaming, and typed outcome handling are features; a faster
plain spawn is not automatically a better substitute for them.
