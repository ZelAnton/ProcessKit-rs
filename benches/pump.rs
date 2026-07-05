//! Baseline benchmarks for the background output pump (`src/pump.rs`), driven
//! entirely through the public `Command` API — never the crate-internal pump
//! types directly, so these track the same surface callers actually use.
//!
//! Each scenario feeds a real child process a large in-memory `Stdin` payload
//! through a trivial per-platform passthrough filter (`cat` / `findstr`), then
//! captures it back with [`Command::output_string`] — a real OS pipe, decoded
//! and line-split by the pump exactly as any other run. Building the payload
//! once outside the timed loop and only `Command::clone`-ing it per iteration
//! keeps the measured time on the pump's own decode/split/buffer work rather
//! than payload construction; the per-run `Stdin` byte clone the crate does at
//! launch is left in (a real cost every caller pays, not pump-internal).
//!
//! - `short_lines` / `long_lines`: the same total shape (many small lines vs.
//!   few large ones) at comparable byte volume, so the two are directly
//!   comparable. A stream of many short lines packs hundreds of them into a
//!   single decoder read (the pump's read chunk is 8 KiB): every line pulled
//!   out of that shared buffer shifts the remaining tail down, so short lines
//!   pay that cost far more often per byte than long ones — the write
//!   amplification this baseline exists to make visible before the pump's
//!   line-extraction is optimized.
//! - `non_utf8_decode`: the same shape as `short_lines`, decoded as
//!   Windows-1252 instead of UTF-8 (`Command::stdout_encoding`).
//! - `tee_handler`: the same short-line volume, additionally routed through a
//!   per-line handler ([`Command::on_stdout_line`]) and a tee sink
//!   ([`Command::stdout_tee`]) — the two extra per-line hooks the pump's
//!   `emit` step threads through.
//!
//! Run with: `cargo bench --bench pump`

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use processkit::{Command, Stdin};
use tokio::runtime::Runtime;

/// Number of lines in the short-line / non-UTF-8 / tee-handler workloads.
/// Large enough that the pump's own per-line work is a large share of each
/// iteration next to the fixed cost of spawning the passthrough child (a
/// measured in-process baseline: ~100 ms of pump time at this size, against
/// a spawn overhead in the same ballpark).
const SHORT_LINE_COUNT: usize = 300_000;
/// Number of lines in the long-line workload.
const LONG_LINE_COUNT: usize = 500;
/// Byte width of each long line (comparable total volume to the short-line
/// workload at a much lower line count).
const LONG_LINE_WIDTH: usize = 2_000;

/// A trivial per-platform passthrough filter: echoes stdin to stdout
/// unchanged, so the benchmarked bytes are exactly what we fed it, decoded and
/// line-split by our own pump on the way back out.
fn passthrough() -> Command {
    if cfg!(windows) {
        // `findstr` re-tokenizes through `cmd`'s own escape rules: `^^` on the
        // Rust side becomes the literal regex `^`, matching (and passing
        // through) every line — the same trick `tests/integration/capture.rs`
        // uses for a Windows `cat` equivalent.
        Command::new("cmd").args(["/c", "findstr", "^^"])
    } else {
        Command::new("cat")
    }
}

/// `count` short numbered lines, e.g. `"line-000000\n"` (12 bytes each).
fn short_lines_payload(count: usize) -> String {
    let mut text = String::with_capacity(count * 12);
    for i in 0..count {
        let _ = writeln!(text, "line-{i:06}");
    }
    text
}

/// `count` lines of `width` filler bytes each.
fn long_lines_payload(count: usize, width: usize) -> String {
    let filler = "x".repeat(width);
    let mut text = String::with_capacity(count * (width + 1));
    for _ in 0..count {
        text.push_str(&filler);
        text.push('\n');
    }
    text
}

/// `count` copies of a short non-ASCII phrase, encoded as Windows-1252 raw
/// bytes (never valid UTF-8) — the non-UTF-8 decode path's input.
fn legacy_payload(count: usize) -> Vec<u8> {
    const PHRASE: &str = "café \u{20AC} déjà vu\n";
    let mut text = String::with_capacity(PHRASE.len() * count);
    for _ in 0..count {
        text.push_str(PHRASE);
    }
    encoding_rs::WINDOWS_1252.encode(&text).0.into_owned()
}

/// Runs `command` once and panics with a descriptive message if the pump
/// didn't round-trip `expected_lines` lines cleanly — a workload that silently
/// decoded fewer/garbled lines (e.g. a passthrough filter mangling bytes)
/// would measure the wrong thing without this check.
fn sanity_check(rt: &Runtime, command: &Command, expected_lines: usize) {
    let result = rt
        .block_on(command.clone().output_string())
        .expect("sanity run must complete");
    assert!(result.is_success(), "sanity run must exit 0: {result:?}");
    assert_eq!(
        result.stdout().lines().count(),
        expected_lines,
        "workload must round-trip every line intact"
    );
}

fn bench_short_lines(c: &mut Criterion) {
    let rt = Runtime::new().expect("build a tokio runtime for the async benches");
    let base = passthrough().stdin(Stdin::from_string(short_lines_payload(SHORT_LINE_COUNT)));
    sanity_check(&rt, &base, SHORT_LINE_COUNT);

    c.bench_function("short_lines", |b| {
        b.to_async(&rt).iter(|| {
            let command = base.clone();
            async move {
                let result = command.output_string().await.expect("run passthrough");
                assert!(result.is_success());
                let _ = std::hint::black_box(result);
            }
        });
    });
}

fn bench_long_lines(c: &mut Criterion) {
    let rt = Runtime::new().expect("build a tokio runtime for the async benches");
    let base = passthrough().stdin(Stdin::from_string(long_lines_payload(
        LONG_LINE_COUNT,
        LONG_LINE_WIDTH,
    )));
    sanity_check(&rt, &base, LONG_LINE_COUNT);

    c.bench_function("long_lines", |b| {
        b.to_async(&rt).iter(|| {
            let command = base.clone();
            async move {
                let result = command.output_string().await.expect("run passthrough");
                assert!(result.is_success());
                let _ = std::hint::black_box(result);
            }
        });
    });
}

fn bench_non_utf8_decode(c: &mut Criterion) {
    let rt = Runtime::new().expect("build a tokio runtime for the async benches");
    let base = passthrough()
        .stdin(Stdin::from_bytes(legacy_payload(SHORT_LINE_COUNT)))
        .stdout_encoding(encoding_rs::WINDOWS_1252);
    sanity_check(&rt, &base, SHORT_LINE_COUNT);

    c.bench_function("non_utf8_decode", |b| {
        b.to_async(&rt).iter(|| {
            let command = base.clone();
            async move {
                let result = command.output_string().await.expect("run passthrough");
                assert!(result.is_success());
                let _ = std::hint::black_box(result);
            }
        });
    });
}

fn bench_tee_handler(c: &mut Criterion) {
    let rt = Runtime::new().expect("build a tokio runtime for the async benches");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let counted = handler_calls.clone();
    let base = passthrough()
        .stdin(Stdin::from_string(short_lines_payload(SHORT_LINE_COUNT)))
        .on_stdout_line(move |_line: &str| {
            counted.fetch_add(1, Ordering::Relaxed);
        })
        // A discard sink: exercises the tee's per-line await without adding a
        // second in-memory copy of the payload to the measurement.
        .stdout_tee(tokio::io::sink());
    sanity_check(&rt, &base, SHORT_LINE_COUNT);

    c.bench_function("tee_handler", |b| {
        b.to_async(&rt).iter(|| {
            let command = base.clone();
            async move {
                let result = command.output_string().await.expect("run passthrough");
                assert!(result.is_success());
                let _ = std::hint::black_box(result);
            }
        });
    });
}

fn configure() -> Criterion {
    // Every iteration spawns a real child process; a smaller sample and a
    // longer measurement window keep the suite's total runtime reasonable
    // without starving criterion's statistics.
    Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(8))
}

criterion_group! {
    name = pump_benches;
    config = configure();
    targets = bench_short_lines, bench_long_lines, bench_non_utf8_decode, bench_tee_handler
}
criterion_main!(pump_benches);
