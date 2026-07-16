//! Background output pump: drain a child's stream line by line into a shared,
//! bounded buffer, decoding text and feeding optional per-line handlers and a
//! live line counter.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use encoding_rs::{Encoding, UTF_8};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Notify;

use crate::buffer::{LineTerminator, OutputBufferPolicy, OverflowMode};

/// A push-style per-line callback (e.g. tee each line to a log).
pub(crate) type LineHandler = Arc<dyn Fn(&str) + Send + Sync>;

pub(crate) fn invoke_handler_isolated(handler: &mut Option<LineHandler>, line: &str) {
    if let Some(h) = handler {
        // AssertUnwindSafe is sound: the handler is `Fn` (no `&mut` state to
        // observe torn) and is dropped right after a panic.
        let invoked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(line)));
        if invoked.is_err() {
            *handler = None;
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "processkit",
                "line handler panicked; disabled for the rest of the run"
            );
        }
    }
}

/// A shared, bounded line buffer written by a [`pump_lines`] task and read by
/// the bulk collectors (drain) or the streaming consumer (`next_line`).
///
/// The line counter increments on every line *before* the buffer write, so it
/// stays exact even when the policy drops lines.
pub(crate) struct SharedLines {
    inner: Mutex<Inner>,
    notify: Notify,
    count: AtomicUsize,
    /// Lines discarded by the buffer *policy* (DropOldest/DropNewest/Error) —
    /// NOT lines a streaming consumer popped via [`try_pop`](Self::try_pop).
    /// This is the truncation signal (`dropped() > 0`): unlike
    /// `count() > retained`, it stays `0` when a stream merely consumed lines
    /// under an unbounded policy, so `output_string` after partial streaming is
    /// not falsely reported as truncated.
    dropped: AtomicUsize,
    /// The first OS read error the pump hit while draining this stream, if any.
    /// Set once by [`pump_lines_core`] just before it closes the sink; a clean
    /// EOF (or a broken-pipe read, treated as EOF) leaves it `None`. A consuming
    /// finisher reads it (via [`take_read_error`](Self::take_read_error)) after
    /// the pump joins and surfaces an incomplete capture as
    /// [`Error::Io`](crate::Error::Io) instead of a silent short read reported as
    /// a full, successful capture. Its own `Mutex` (not `Inner`'s) so the hot
    /// `push` path is untouched; poison is recovered rather than propagated,
    /// matching [`close`](Self::close).
    read_error: Mutex<Option<std::io::Error>>,
}

struct Inner {
    lines: VecDeque<String>,
    /// Retained-line cap (`OutputBufferPolicy::max_lines`).
    max_lines: Option<usize>,
    /// Retained-byte cap (`OutputBufferPolicy::max_bytes`).
    max_bytes: Option<usize>,
    /// Sum of the retained lines' byte lengths — kept in step with `lines` so
    /// the byte backlog can be bounded without re-summing.
    bytes: usize,
    /// Cumulative bytes the pump has seen (including dropped lines) — the byte
    /// analogue of `SharedLines::count`, used by the `Error` fail-loud ceiling
    /// which fires on the total seen, not the current backlog.
    seen_bytes: usize,
    mode: OverflowMode,
    closed: bool,
    /// Set when `OverflowMode::Error` is active and a ceiling is reached — the
    /// consuming path turns this into [`Error::OutputTooLarge`](crate::Error::OutputTooLarge).
    overflowed: bool,
    /// Flipped on by [`start_discarding`](SharedLines::start_discarding) when a
    /// streaming consumer is gone and a discard verb adopts this sink: the
    /// still-running pump keeps draining the pipe (so the child never blocks) but
    /// retains nothing, so the backlog can't grow O(total).
    discarding: bool,
}

impl Inner {
    /// Whether the retained backlog is over either drop-mode ceiling.
    ///
    /// The byte ceiling is checked two ways: the raw content-byte sum
    /// (`self.bytes`, unchanged), *and* a derived line-count bound
    /// (`self.lines.len() > b`). Without the latter, a flood of empty lines —
    /// each contributing `0` to `self.bytes` — would never be judged "over" and
    /// the backlog would grow without bound even under a byte cap. A `b`-byte
    /// cap cannot legitimately retain more than `b` lines if every retained
    /// line is charged a minimum footprint of `1` (its stripped terminator, if
    /// nothing else), so `lines.len() > b` is a sound, minimal per-line charge
    /// expressed as a derived cap rather than added to `self.bytes` — which
    /// keeps every existing exact-content-byte-boundary case (a line whose
    /// content is exactly `max_bytes` is still retained) unaffected, since it
    /// only ever bites once the retained *count* alone would already exceed the
    /// byte budget.
    fn over_backlog(&self) -> bool {
        self.max_lines.is_some_and(|n| self.lines.len() > n)
            || self
                .max_bytes
                .is_some_and(|b| self.bytes > b || self.lines.len() > b)
    }

    /// Whether a line of `len` bytes would still fit both ceilings if appended.
    /// See [`over_backlog`](Self::over_backlog) for why the byte ceiling also
    /// checks the derived line-count bound.
    fn would_fit(&self, len: usize) -> bool {
        self.max_lines.is_none_or(|n| self.lines.len() < n)
            && self
                .max_bytes
                .is_none_or(|b| self.bytes + len <= b && self.lines.len() < b)
    }
}

/// Result of a non-blocking pop from a [`SharedLines`].
pub(crate) enum Popped {
    /// A buffered line.
    Line(String),
    /// No line available yet, and the pump is still running.
    Empty,
    /// No line available and the pump has finished.
    Closed,
}

impl SharedLines {
    pub(crate) fn new(policy: &OutputBufferPolicy) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                lines: VecDeque::new(),
                max_lines: policy.max_lines,
                max_bytes: policy.max_bytes,
                bytes: 0,
                seen_bytes: 0,
                mode: policy.overflow,
                closed: false,
                overflowed: false,
                discarding: false,
            }),
            notify: Notify::new(),
            count: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            read_error: Mutex::new(None),
        })
    }

    pub(crate) fn push(&self, line: String) {
        // Count every line, even one we are about to drop.
        let total_lines = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        // Whether the policy discarded a line here — distinct from a streaming
        // consumer's pop, so the truncation signal ignores consumed lines.
        let mut policy_dropped = false;
        {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            inner.seen_bytes = inner.seen_bytes.saturating_add(line.len());
            // A dropped streaming consumer flips `discarding` on (via
            // `start_discarding`): keep counting/draining, but skip all
            // retention and overflow bookkeeping so an adopting discard verb
            // (wait/profile) can't grow O(total) heap.
            if inner.discarding {
                // Retain nothing.
            } else {
                match inner.mode {
                    // Fires on the CUMULATIVE total seen, not the current backlog: a
                    // streaming consumer draining lines frees space but must not reset
                    // the ceiling. With neither cap set it is a ceiling with no
                    // ceiling — a misconfiguration treated as zero-tolerance. The pipe
                    // is still drained so the child never blocks; the consuming verb
                    // turns `overflowed` into `Error::OutputTooLarge`.
                    OverflowMode::Error => {
                        let over = match (inner.max_lines, inner.max_bytes) {
                            (None, None) => true,
                            (lines_cap, bytes_cap) => {
                                lines_cap.is_some_and(|n| total_lines > n)
                                    || bytes_cap.is_some_and(|b| {
                                        // Raw content-byte total, *and* the same
                                        // derived line-count bound `over_backlog`
                                        // uses: without it, a flood of empty lines
                                        // (each `+0` to `seen_bytes`) would never
                                        // trip a byte-only fail-loud ceiling, so
                                        // `OverflowMode::Error` could never fire and
                                        // the anti-DoS guarantee would be defeated.
                                        inner.seen_bytes > b || total_lines > b
                                    })
                            }
                        };
                        if over {
                            inner.overflowed = true;
                            policy_dropped = true;
                        } else {
                            inner.bytes += line.len();
                            inner.lines.push_back(line);
                        }
                    }
                    // Ring-buffer "tail": append, then evict the oldest until the
                    // backlog is back within both ceilings (a single line larger than
                    // `max_bytes` is evicted whole).
                    OverflowMode::DropOldest => {
                        inner.bytes += line.len();
                        inner.lines.push_back(line);
                        while inner.over_backlog() {
                            match inner.lines.pop_front() {
                                Some(old) => {
                                    inner.bytes = inner.bytes.saturating_sub(old.len());
                                    policy_dropped = true;
                                }
                                None => break,
                            }
                        }
                    }
                    // "Head": keep what is buffered; drop the incoming line if it
                    // would breach either ceiling.
                    OverflowMode::DropNewest => {
                        if inner.would_fit(line.len()) {
                            inner.bytes += line.len();
                            inner.lines.push_back(line);
                        } else {
                            policy_dropped = true;
                        }
                    }
                }
            }
        }
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        // `notify_one` stores a permit if no consumer is waiting yet, so a
        // streaming consumer that registers just after this can't miss it.
        self.notify.notify_one();
    }

    /// The retained-byte ceiling (`OutputBufferPolicy::max_bytes`), read by the
    /// pump once at start to bound the in-flight decode buffer.
    pub(crate) fn byte_cap(&self) -> Option<usize> {
        self.inner.lock().expect("SharedLines poisoned").max_bytes
    }

    /// Record a line whose own byte length exceeds `max_bytes`: it is counted
    /// and added to the seen-byte total, but never retained (it cannot fit the
    /// cap). Under [`OverflowMode::Error`] it trips the fail-loud ceiling; under
    /// the drop modes it sets the truncation signal. A discarding sink instead
    /// only counts the line and its bytes, skipping all retention and overflow
    /// bookkeeping like [`push`](Self::push). Mirrors the "cannot fit" accounting
    /// in [`push`](Self::push) for a line the pump never buffered (so it is also
    /// not delivered to the per-line handler or tee).
    pub(crate) fn record_oversized_line(&self, byte_len: usize) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let mut policy_dropped = false;
        {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            inner.seen_bytes = inner.seen_bytes.saturating_add(byte_len);
            // A discarded streaming consumer retains nothing and skips all
            // overflow bookkeeping, even for an over-cap line the pump skipped.
            if !inner.discarding {
                if matches!(inner.mode, OverflowMode::Error) {
                    inner.overflowed = true;
                }
                policy_dropped = true;
            }
        }
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_one();
    }

    fn close(&self) {
        // Recover a poisoned lock instead of panicking: `close` runs from a
        // `Drop` guard on the pump task's unwind path, where a second panic would
        // abort the process. Only the `closed` flag is set here, safe regardless
        // of any prior poisoning.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
        self.notify.notify_one();
    }

    /// Mark the buffer finished without a pump (e.g. a second `stdout_lines`
    /// call has no pipe left to drain), so a streaming consumer ends promptly.
    pub(crate) fn close_now(&self) {
        self.close();
    }

    /// Switch the sink to retain nothing and drop its current backlog. A discard
    /// verb (`wait`/`profile`) calls this when it adopts a sink a **dropped**
    /// stream left populated under the caller's `OutputBufferPolicy`, so the
    /// still-running pump stops accumulating lines nobody will read. The line
    /// counter is untouched (it still reflects the total the pump has seen); only
    /// the retained backlog and future retention are dropped.
    pub(crate) fn start_discarding(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.discarding = true;
        inner.lines.clear();
        inner.bytes = 0;
    }

    /// Total lines seen by the pump (including dropped ones).
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Total bytes seen by the pump (including dropped lines) — the byte
    /// analogue of [`count`](Self::count), used to report the byte total on a
    /// fail-loud overflow.
    pub(crate) fn seen_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .seen_bytes
    }

    /// Lines discarded by the buffer policy (DropOldest/DropNewest/Error), not
    /// counting lines a streaming consumer popped. `> 0` iff output was actually
    /// truncated by the policy.
    pub(crate) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Record the first OS read error the pump hit while draining the stream —
    /// the incomplete-capture signal a consuming finisher turns into
    /// [`Error::Io`](crate::Error::Io). Only the *first* error is kept (a later
    /// one is ignored); a clean EOF, or a broken-pipe read treated as EOF, records
    /// nothing. Poison-tolerant, like [`close`](Self::close), because it runs on
    /// the pump task's normal *and* unwind exit paths.
    pub(crate) fn set_read_error(&self, err: std::io::Error) {
        let mut slot = self.read_error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(err);
        }
    }

    /// Take the recorded OS read error, if any. Consumes it (by value — a
    /// `std::io::Error` is not `Clone`), so a consuming finisher calls it once
    /// after the pump has joined and wraps a `Some` in
    /// [`Error::Io`](crate::Error::Io); `None` means the stream drained to a clean
    /// EOF and the capture is complete.
    pub(crate) fn take_read_error(&self) -> Option<std::io::Error> {
        self.read_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }

    /// Whether the `OverflowMode::Error` ceiling was hit during pumping.
    /// Always `false` for `DropOldest`/`DropNewest` buffers.
    pub(crate) fn overflowed(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .overflowed
    }

    /// Take all currently-retained lines (used by the bulk collectors once the
    /// pump has finished).
    pub(crate) fn drain(&self) -> Vec<String> {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        inner.bytes = 0;
        inner.lines.drain(..).collect()
    }

    /// Non-blocking pop for the streaming consumer.
    pub(crate) fn try_pop(&self) -> Popped {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        if let Some(line) = inner.lines.pop_front() {
            // Keep the retained-byte tally in step as a streaming consumer drains.
            inner.bytes = inner.bytes.saturating_sub(line.len());
            Popped::Line(line)
        } else if inner.closed {
            Popped::Closed
        } else {
            Popped::Empty
        }
    }

    /// Await the next buffer change (a push or close). Owns the `Arc` so the
    /// returned future is `'static` and can be boxed by the `Stream` impl.
    pub(crate) async fn changed(self: Arc<Self>) {
        self.notify.notified().await;
    }
}

/// A per-stream async tee sink: each decoded line is written to it (plus a
/// `\n`) as it is produced — [`Command::stdout_tee`](crate::Command::stdout_tee)
/// / [`stderr_tee`](crate::Command::stderr_tee). Behind an `Arc<Mutex>` so a
/// cloned `Command` shares one writer. The write is **awaited on the pump
/// task**, so a slow sink applies backpressure (the pump slows → the OS pipe
/// fills → the child blocks on write) rather than blocking the runtime, and a
/// write error disables the tee with a `tracing` warn instead of being silently
/// swallowed.
pub(crate) type TeeSink = Arc<tokio::sync::Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>;

/// The four per-stream pump knobs that differ between stdout and stderr — the
/// decode [`encoding`](Self::encoding), an optional per-line
/// [`handler`](Self::handler), an optional [`tee`](Self::tee) sink, and the line
/// [`terminator`](Self::terminator) mode — carried as one value.
///
/// Held one-per-stream by [`Command`](crate::Command),
/// [`Spawned`](crate::running::Spawned), and
/// [`RunningProcess`](crate::RunningProcess), and handed to [`pump_lines_core`]
/// whole. Folding the four knobs into a single struct means a new per-stream knob
/// is threaded through *this* type instead of duplicated across every field pair
/// and pump call site. Cheap to clone: `handler`/`tee` are `Arc`s and the rest is
/// `Copy`.
#[derive(Clone)]
pub(crate) struct StreamConfig {
    /// Decode bytes with this encoding (default UTF-8).
    pub encoding: &'static Encoding,
    /// Optional per-line callback invoked on the pump task.
    pub handler: Option<LineHandler>,
    /// Optional async sink each decoded line is also written to.
    pub tee: Option<TeeSink>,
    /// Where the pump splits the stream into lines (default `\n`-only).
    pub terminator: LineTerminator,
}

impl StreamConfig {
    /// The default per-stream config: UTF-8 decode, no handler, no tee, split on
    /// `\n` only — the state a freshly built [`Command`](crate::Command) stream
    /// starts in.
    pub(crate) fn new() -> Self {
        Self {
            encoding: UTF_8,
            handler: None,
            tee: None,
            terminator: LineTerminator::Newline,
        }
    }

    /// This config with the decode `encoding` replaced. Used by the scripted
    /// feeder, which forces UTF-8 (it writes the canned `String`'s UTF-8 bytes)
    /// while keeping the command's handler/tee/terminator.
    pub(crate) fn with_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = encoding;
        self
    }
}

/// The no-tee, `\n`-only shorthand over [`pump_lines_core`] — used by this
/// module's tests (production always threads the terminator and optional tee
/// through `pump_lines_core`).
#[cfg(test)]
pub(crate) async fn pump_lines<R>(
    reader: R,
    encoding: &'static Encoding,
    handler: Option<LineHandler>,
    sink: Arc<SharedLines>,
) where
    R: AsyncRead + Unpin,
{
    pump_lines_core(
        reader,
        StreamConfig {
            encoding,
            handler,
            tee: None,
            terminator: LineTerminator::Newline,
        },
        sink,
    )
    .await
}

/// The no-tee shorthand over [`pump_lines_core`] for a chosen
/// [`LineTerminator`] — used by this module's `\r`-aware tests.
#[cfg(test)]
pub(crate) async fn pump_lines_term<R>(
    reader: R,
    encoding: &'static Encoding,
    terminator: LineTerminator,
    sink: Arc<SharedLines>,
) where
    R: AsyncRead + Unpin,
{
    pump_lines_core(
        reader,
        StreamConfig {
            encoding,
            handler: None,
            tee: None,
            terminator,
        },
        sink,
    )
    .await
}

/// Drain `reader` into `sink` line by line, decoding text with `encoding`,
/// invoking `handler` (if any) and writing each line to `tee` (if any). Always
/// reads to EOF so the child never blocks on a full pipe; on an OS read error it
/// flushes what it has, **records the error on the sink**
/// ([`set_read_error`](SharedLines::set_read_error)) so a consuming finisher can
/// surface the incomplete capture as [`Error::Io`](crate::Error::Io) rather than
/// a silent short read, and closes the sink. A broken-pipe read (the writer end
/// closing) is the normal end of a stream and is treated as a clean EOF, not an
/// error.
///
/// A **panicking handler does not poison the run**: the panic is caught, the
/// handler is disabled for the rest of the run (and the fact surfaced as a
/// `tracing` warn when the feature is on), and pumping continues — the child
/// is still drained and the final result still carries every line. The
/// callback seam is handed to consumers' consumers, so "panic-free or else"
/// is not a re-exportable contract. A `tee` write error is isolated the same
/// way: the tee is disabled (with a `tracing` warn) and pumping continues.
///
/// **Decoding:** bytes are fed through a single persistent
/// `encoding_rs::Decoder` and the *decoded* text is split into lines — correct
/// for every encoding, including non-ASCII-compatible ones (UTF-16LE/BE, whose
/// code units contain `0x0A` bytes that are *not* line breaks) and stateful ones
/// (ISO-2022-JP shift state carries across reads). One persistent decoder also
/// means a byte-order mark is handled once at the stream start
/// (`with_bom_removal`: a leading BOM *of the chosen encoding* is stripped, never
/// a foreign one — so a legacy line that happens to start with BOM-looking bytes
/// is not silently re-decoded as UTF-16).
///
/// **Line splitting** follows `terminator` (see [`LineTerminator`]):
/// - [`Newline`](LineTerminator::Newline) (default): split on `\n`; each line is
///   stripped of its `\n` and, if present, exactly **one** preceding `\r` (a CRLF
///   terminator — not every trailing CR, so a lone or repeated `\r` is content).
/// - [`CarriageReturn`](LineTerminator::CarriageReturn): also split on a bare
///   `\r`, so each carriage-return progress frame is emitted as its own line. A
///   `\r\n` pair is one terminator (no empty line between them); a `\r` at a read
///   boundary whose follower is not yet known is held over to the next read (or
///   resolved as a terminator at EOF).
///
/// Either way the final line is emitted even without a trailing terminator, on
/// both EOF and a mid-stream read error (the partial tail is flushed, not
/// dropped). This holds for every sink — the handler, the tee, and the
/// buffer — for lines that fit the configured `max_bytes` byte cap. A line
/// (or unterminated tail) whose length exceeds `max_bytes` is instead counted
/// via [`record_oversized_line`](SharedLines::record_oversized_line) — visible
/// through the truncation/`dropped()` signal — but is **not** delivered to the
/// handler, the tee, or the buffer.
pub(crate) async fn pump_lines_core<R>(mut reader: R, config: StreamConfig, sink: Arc<SharedLines>)
where
    R: AsyncRead + Unpin,
{
    let StreamConfig {
        encoding,
        handler,
        tee,
        terminator,
    } = config;
    // Close the sink on *every* exit from this task: a panic out of this loop
    // must never leave a streaming `StdoutLines` consumer parked.
    struct CloseOnDrop(Arc<SharedLines>);
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    let sink = CloseOnDrop(sink);
    let mut handler = handler;
    let mut tee = tee;

    // Emit one decoded line: run the (panic-isolated) handler, await the tee
    // (disabling it on a write error), then buffer the line.
    async fn emit(
        handler: &mut Option<LineHandler>,
        tee: &mut Option<TeeSink>,
        sink: &SharedLines,
        line: String,
    ) {
        invoke_handler_isolated(handler, &line);
        if let Some(t) = tee {
            use tokio::io::AsyncWriteExt;
            let mut w = t.lock().await;
            // Awaiting the write here is the backpressure point.
            let wrote = async {
                w.write_all(line.as_bytes()).await?;
                w.write_all(b"\n").await
            }
            .await;
            drop(w);
            if wrote.is_err() {
                *tee = None;
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    "tee writer errored; disabled for the rest of the run"
                );
            }
        }
        sink.push(line);
    }

    // Where the next complete line ends within `pending`.
    struct Term {
        // Byte length of the line's content, excluding the terminator sequence —
        // matching `push`'s retained-content definition, so the over-cap decision
        // judges a CRLF/CR line exactly like its bare-LF twin.
        content_len: usize,
        // Byte offset just past the whole terminator sequence: `drain(..resume)`
        // removes the line and its terminator in one go.
        resume: usize,
    }

    // Locate the next line terminator, honoring `terminator`.
    //
    // `Newline` splits on `\n`, treating a `\r` immediately before it as the CR of
    // a CRLF (excluded from content). `CarriageReturn` additionally splits on a
    // bare `\r`: a `\r\n` pair stays a single terminator, and a lone trailing `\r`
    // whose follower is not yet decoded is *deferred* (returns `None`) so a CRLF
    // straddling a read is not mistaken for a bare-CR frame — unless `eof`, when it
    // terminates the final frame. Returns `None` when no complete terminator is
    // present yet (the caller keeps reading or, at EOF, flushes the tail).
    fn next_terminator(pending: &str, terminator: LineTerminator, eof: bool) -> Option<Term> {
        let bytes = pending.as_bytes();
        match terminator {
            LineTerminator::Newline => {
                let nl = pending.find('\n')?;
                let content_len = if nl > 0 && bytes[nl - 1] == b'\r' {
                    nl - 1
                } else {
                    nl
                };
                Some(Term {
                    content_len,
                    resume: nl + 1,
                })
            }
            LineTerminator::CarriageReturn => {
                // The earliest `\r` or `\n`. A `\r` is found before the `\n` of a
                // CRLF, so reaching a `\n` here means it has no preceding `\r`.
                let pos = bytes.iter().position(|&b| b == b'\n' || b == b'\r')?;
                if bytes[pos] == b'\n' {
                    Some(Term {
                        content_len: pos,
                        resume: pos + 1,
                    })
                } else {
                    match bytes.get(pos + 1) {
                        // CRLF: a single terminator; drop both bytes.
                        Some(b'\n') => Some(Term {
                            content_len: pos,
                            resume: pos + 2,
                        }),
                        // A `\r` followed by other content: a bare-CR frame end.
                        Some(_) => Some(Term {
                            content_len: pos,
                            resume: pos + 1,
                        }),
                        // Trailing `\r`, follower not yet decoded: defer unless EOF.
                        None => eof.then_some(Term {
                            content_len: pos,
                            resume: pos + 1,
                        }),
                    }
                }
            }
        }
    }

    // How many leading bytes of `sub` (the buffered prefix of an over-cap line
    // being skipped) to advance past, EXCEPT a single trailing `\r`, held back
    // rather than counted in case it is the CR of a CRLF whose `\n` lands in the
    // next chunk (a terminator, which `content_len` excludes). Deferring keeps
    // the recorded length identical regardless of read boundary; a `\r` that
    // turns out to be mid-line content is counted later (a subsequent split,
    // another `skip_over_cap_len` pass, or the EOF finalizer carries it on).
    // Index-only (no buffer mutation): the caller advances its cursor by the
    // returned amount and bulk-drains the consumed prefix once per chunk,
    // instead of memmove-ing the tail on every skipped line.
    fn skip_over_cap_len(sub: &str) -> usize {
        if sub.ends_with('\r') {
            sub.len() - 1 // keep only the trailing '\r' unconsumed
        } else {
            sub.len()
        }
    }

    // The OS read size.
    const CHUNK: usize = 8192;
    // The retained-byte ceiling, read once. When set it bounds the *in-flight*
    // decode buffer too, not just the retained backlog: a line longer than the cap
    // can never be retained whole, so once `pending` passes the cap we stop
    // buffering it and skip to its newline — a newline-free flood can no longer
    // OOM the parent. The bound is `cap + CHUNK` (rechecked once per read, after a
    // whole chunk decodes in), not exactly `cap`.
    let cap = sink.0.byte_cap();
    let mut decoder = encoding.new_decoder_with_bom_removal();
    let mut pending = String::new(); // decoded text not yet split into a line
    let mut chunk = [0u8; CHUNK];
    // `Some(bytes_so_far)` while skipping an over-cap line: bytes are discarded as
    // decoded, only the length is tracked for the seen-byte total.
    let mut oversized: Option<usize> = None;
    loop {
        // Distinguish a clean EOF (`Ok(0)`) from a read error: both stop the
        // pump, but only a clean EOF signals the decoder's end-of-stream flush. On
        // an error we pass `last = false` so a trailing *incomplete* multibyte
        // sequence (truncated by the error) is dropped, not fabricated into a
        // phantom replacement char / final line — and the error itself is recorded
        // on the sink at stream end (below) so a consuming finisher surfaces the
        // incomplete capture instead of a silent short read.
        let (n, eof, read_err) = match reader.read(&mut chunk).await {
            Ok(0) => (0, true, None),
            Ok(n) => (n, false, None),
            // A broken-pipe read means the writer end closed — the normal end of a
            // child stream. Rust's std already maps this to `Ok(0)` on both Unix
            // (EOF) and Windows (`ERROR_BROKEN_PIPE`), so this arm is a defensive
            // net for any reader that ever surfaces it as an error: treat it
            // exactly like a clean EOF, never an incomplete-capture error.
            Err(e) if crate::running::is_broken_pipe(&e) => (0, true, None),
            Err(e) => (0, true, Some(e)),
        };
        let errored = read_err.is_some();
        let last = eof && !errored;
        // Reserve the decoder's worst-case output up front so `decode_to_string`
        // (which uses the `String`'s spare capacity as its output limit, never
        // reallocating) consumes the whole chunk in one call.
        if let Some(need) = decoder.max_utf8_buffer_length(n) {
            pending.reserve(need);
        }
        let _ = decoder.decode_to_string(&chunk[..n], &mut pending, last);

        // Split out every complete line decoded so far, bounding memory by
        // `cap`. `start` is a byte cursor into `pending`: instead of draining
        // (and memmove-ing the remaining tail) on every single line — the
        // write-amplification a busy stream of short lines used to pay for —
        // the cursor only advances here, over an index subslice of `pending`,
        // and the whole consumed prefix is removed in one bulk `drain` after
        // the loop (below), amortized over every line the chunk produced.
        let mut start = 0usize;
        loop {
            let sub = &pending[start..];
            if let Some(skipped) = oversized {
                // Skipping an over-cap line: discard through its terminator, keeping
                // only its length for accounting.
                match next_terminator(sub, terminator, eof) {
                    Some(term) => {
                        let line_len = skipped.saturating_add(term.content_len);
                        start += term.resume;
                        oversized = None;
                        sink.0.record_oversized_line(line_len);
                    }
                    None => {
                        let advance = skip_over_cap_len(sub);
                        start += advance;
                        oversized = Some(skipped.saturating_add(advance));
                        break;
                    }
                }
            } else {
                match next_terminator(sub, terminator, eof) {
                    Some(term) => {
                        // Compare *content* length (excluding the terminator) to the
                        // cap, so a CRLF/CR line is judged exactly like its LF twin.
                        let len = term.content_len;
                        if cap.is_none_or(|c| len <= c) {
                            let line = sub[..len].to_owned(); // drop the terminator sequence
                            start += term.resume;
                            emit(&mut handler, &mut tee, &sink.0, line).await;
                        } else {
                            // Over-cap line, terminator already here: drop it whole,
                            // counting its content length.
                            start += term.resume;
                            sink.0.record_oversized_line(len);
                        }
                    }
                    // No terminator yet and already over the cap: skip to it. A lone
                    // *trailing* `\r` may be a CRLF terminator (or, in `\r`-aware
                    // mode, a bare-CR frame end), so it alone must not push the line
                    // over the cap — else a content-at-cap line is dropped when its
                    // `\r`/`\n` straddle a read but retained in one chunk. Exclude
                    // that byte; the next read re-decides (terminator → fits, or
                    // content → counts).
                    None if cap
                        .is_some_and(|c| sub.len() - usize::from(sub.ends_with('\r')) > c) =>
                    {
                        let advance = skip_over_cap_len(sub);
                        start += advance;
                        oversized = Some(advance);
                        break;
                    }
                    None => break,
                }
            }
        }
        if start > 0 {
            // The single bulk memmove for this chunk: drop exactly the
            // consumed prefix, leaving `pending` holding only the unconsumed
            // remainder for the next read to append to (same invariant the
            // per-line drains used to maintain one line at a time).
            pending.drain(..start);
        }

        if eof {
            // Finalize a final line (or an un-terminated over-cap tail). At EOF the
            // split loop above ran with `eof = true`, so in `\r`-aware mode a
            // trailing `\r` was already resolved as a frame terminator; whatever
            // remains in `pending` here is pure content with no terminator.
            if let Some(skipped) = oversized.take() {
                // An un-terminated tail: `pending` is all content (in `Newline`
                // mode a trailing `\r` is content; in `\r`-aware mode none remains).
                let line_len = skipped.saturating_add(pending.len());
                pending.clear();
                sink.0.record_oversized_line(line_len);
            } else if !pending.is_empty() {
                // An un-terminated final line: `pending` is all content (in
                // `Newline` mode a trailing `\r` is content). Re-apply the byte cap:
                // the enter-skip deferred a lone trailing `\r` in case a `\n`
                // followed, but at EOF none does, so an over-cap tail must be
                // dropped (counted, never handed to the handler/tee) like any
                // over-cap line — not emitted.
                let line = std::mem::take(&mut pending);
                if cap.is_some_and(|c| line.len() > c) {
                    sink.0.record_oversized_line(line.len());
                } else {
                    emit(&mut handler, &mut tee, &sink.0, line).await;
                }
            }
            // Flush the tee once at stream end (best-effort).
            if let Some(t) = &tee {
                use tokio::io::AsyncWriteExt;
                let _ = t.lock().await.flush().await;
            }
            if let Some(e) = read_err {
                // Record the OS read error AFTER flushing the partial tail (so the
                // already-decoded prefix is still delivered) and before the sink
                // closes, so a finisher that joins this pump surfaces the
                // incomplete capture as `Error::Io`.
                sink.0.set_read_error(e);
            }
            break;
        }
    }
    // `sink` (the guard) closes here.
}

/// A reader that yields predefined byte chunks one `poll_read` at a time,
/// then EOFs (or returns one IO error) — to exercise cross-read decoding and
/// the mid-stream-error flush deterministically. Shared by the unit tests,
/// the `proptests` below, and the `fuzz_decode_pump_lines` fuzz entry point
/// (`cfg(fuzzing)`) so none of them re-implement chunked-read simulation.
#[cfg(any(test, fuzzing))]
struct ChunkedReader {
    chunks: VecDeque<Vec<u8>>,
    /// One-shot error emitted once the chunks drain (`take`n on first hit), then
    /// the reader EOFs like `new`. `None` = drain straight to a clean EOF.
    err_at_end: Option<std::io::Error>,
}

#[cfg(any(test, fuzzing))]
impl ChunkedReader {
    fn new(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            err_at_end: None,
        }
    }

    #[allow(dead_code, reason = "only exercised by the hand-written unit tests")]
    fn erroring(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self::erroring_with(chunks, std::io::Error::other("boom"))
    }

    /// Like [`erroring`](Self::erroring) but with a caller-chosen error, so a test
    /// can drive a specific `ErrorKind` (e.g. `BrokenPipe`, to prove the pump
    /// treats a writer-closed read as a clean EOF, not an incomplete capture).
    #[allow(dead_code, reason = "only exercised by the hand-written unit tests")]
    fn erroring_with(chunks: impl IntoIterator<Item = Vec<u8>>, err: std::io::Error) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            err_at_end: Some(err),
        }
    }
}

#[cfg(any(test, fuzzing))]
impl AsyncRead for ChunkedReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(chunk) = self.chunks.pop_front() {
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            if n < chunk.len() {
                self.chunks.push_front(chunk[n..].to_vec());
            }
            std::task::Poll::Ready(Ok(()))
        } else if let Some(err) = self.err_at_end.take() {
            std::task::Poll::Ready(Err(err))
        } else {
            std::task::Poll::Ready(Ok(())) // 0 bytes filled == EOF
        }
    }
}

/// Split `bytes` into consecutive, non-empty chunks whose lengths cycle
/// through `sizes` (each clamped to at least 1 so [`ChunkedReader`] never
/// sees a zero-length chunk, which it — like a real EOF read — would read as
/// end of stream). An empty `bytes` yields no chunks. Shared by the
/// `proptests` below and `fuzz_decode_pump_lines`.
#[cfg(any(test, fuzzing))]
fn to_chunks(bytes: &[u8], sizes: &[usize]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0;
    let mut i = 0;
    while pos < bytes.len() {
        let size = sizes[i % sizes.len()].max(1);
        let end = (pos + size).min(bytes.len());
        out.push(bytes[pos..end].to_vec());
        pos = end;
        i += 1;
    }
    out
}

/// Fuzzing-only entry point driving the `pump_lines` decode path from
/// `fuzz/fuzz_targets/decode_pump_lines.rs`: arbitrary raw bytes, chunked at
/// arbitrary boundaries (`chunk_sizes`), decoded under one of a handful of
/// encodings (`encoding_idx`) — the exact same oracle as the
/// `pump_never_panics_on_arbitrary_bytes_under_any_chunking` proptest below,
/// reusing its [`ChunkedReader`]/[`to_chunks`] rather than re-deriving them,
/// but driven by libFuzzer-guided input instead of proptest-shrunk cases so it
/// can run far more iterations and keep the long tail shrinking discards.
///
/// Gated behind the `fuzzing` cfg that `cargo fuzz build` sets automatically
/// for every crate in the dependency graph (see the cargo-fuzz guide) — inert
/// in every ordinary build, so it never appears on the public API surface
/// tracked in `public-api.txt` (that check runs with `--all-features`, never
/// `--cfg fuzzing`).
#[cfg(fuzzing)]
pub fn fuzz_decode_pump_lines(raw: &[u8], chunk_sizes: &[u8], encoding_idx: u8) {
    const ENCODINGS: [&Encoding; 4] = [
        encoding_rs::UTF_8,
        encoding_rs::SHIFT_JIS,
        encoding_rs::UTF_16LE,
        encoding_rs::WINDOWS_1252,
    ];
    let encoding = ENCODINGS[encoding_idx as usize % ENCODINGS.len()];

    let sizes: Vec<usize> = if chunk_sizes.is_empty() {
        vec![raw.len().max(1)]
    } else {
        chunk_sizes.iter().map(|&b| b as usize + 1).collect()
    };
    let chunks = to_chunks(raw, &sizes);
    let reader = ChunkedReader::new(chunks);
    let sink = SharedLines::new(&OutputBufferPolicy::unbounded());

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");
    rt.block_on(pump_lines_core(
        reader,
        StreamConfig {
            encoding,
            handler: None,
            tee: None,
            terminator: LineTerminator::Newline,
        },
        sink.clone(),
    ));

    // Same invariant the proptest asserts: the retained backlog can never
    // exceed the total lines seen, no matter how garbled the input.
    let lines = sink.drain();
    assert!(lines.len() <= sink.count(), "backlog exceeds lines seen");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::OutputBufferPolicy;

    #[tokio::test]
    async fn pumps_utf8_lines_and_counts() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(
            &b"one\ntwo\nthree\n"[..],
            encoding_rs::UTF_8,
            None,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 3);
        assert_eq!(sink.drain(), vec!["one", "two", "three"]);
    }

    #[tokio::test]
    async fn decodes_shift_jis() {
        // 0x82 0xA0 is Hiragana あ (U+3042) in Shift-JIS.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(
            &[0x82, 0xA0, b'\n'][..],
            encoding_rs::SHIFT_JIS,
            None,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["\u{3042}"]);
    }

    #[tokio::test]
    async fn drop_oldest_keeps_tail_but_counts_all() {
        let sink = SharedLines::new(&OutputBufferPolicy::bounded(2));
        pump_lines(&b"a\nb\nc\nd\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 4, "every line is counted");
        assert_eq!(sink.drain(), vec!["c", "d"], "only the newest two retained");
    }

    #[tokio::test]
    async fn drop_newest_keeps_head() {
        let policy = OutputBufferPolicy::bounded(2).with_overflow(OverflowMode::DropNewest);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"a\nb\nc\nd\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn fail_loud_sets_overflow_once_full_but_retains_the_cap() {
        let sink = SharedLines::new(&OutputBufferPolicy::fail_loud(2));
        pump_lines(&b"a\nb\nc\nd\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(sink.overflowed(), "third line must trip the fail-loud flag");
        assert_eq!(sink.count(), 4, "every line is still counted");
        assert_eq!(sink.drain(), vec!["a", "b"], "retains up to the cap");
    }

    #[tokio::test]
    async fn fail_loud_under_the_cap_does_not_overflow() {
        let sink = SharedLines::new(&OutputBufferPolicy::fail_loud(5));
        pump_lines(&b"a\nb\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(!sink.overflowed(), "two lines under a 5-line cap is fine");
    }

    #[tokio::test]
    async fn fail_loud_zero_errors_on_the_first_line() {
        // `fail_loud(0)` = "tolerate no output, error on the first line." The
        // retain-nothing fast-path must still trip the flag.
        let sink = SharedLines::new(&OutputBufferPolicy::fail_loud(0));
        pump_lines(&b"oops\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(sink.overflowed(), "any line is over a 0-line ceiling");
        assert!(sink.drain().is_empty(), "still retains nothing");
    }

    #[tokio::test]
    async fn unbounded_with_error_mode_is_zero_tolerance_not_inert() {
        // `unbounded().with_overflow(Error)` must fail loud on any output (and
        // retain nothing, like fail_loud(0)).
        let sink =
            SharedLines::new(&OutputBufferPolicy::unbounded().with_overflow(OverflowMode::Error));
        pump_lines(&b"anything\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(
            sink.overflowed(),
            "unbounded + Error must fail loud on any output, not be inert"
        );
        assert!(sink.drain().is_empty(), "zero-tolerance retains nothing");
    }

    #[tokio::test]
    async fn unbounded_without_error_mode_retains_everything() {
        // The default unbounded (DropOldest) is unchanged: retain all, no overflow.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"a\nb\nc\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(!sink.overflowed());
        assert_eq!(sink.drain(), ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn dropped_counts_policy_drops_not_consumer_pops() {
        // The truncation signal must reflect lines the *policy* discarded, not
        // lines a streaming consumer popped: under unbounded, popping must leave
        // dropped() == 0.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"a\nb\nc\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 3);
        assert_eq!(sink.dropped(), 0, "unbounded policy discards nothing");
        assert!(matches!(sink.try_pop(), Popped::Line(_)));
        assert!(matches!(sink.try_pop(), Popped::Line(_)));
        assert_eq!(
            sink.dropped(),
            0,
            "a streaming consumer's pops are not truncation"
        );

        // A bounded policy that genuinely discards lines reports them.
        let bounded = SharedLines::new(&OutputBufferPolicy::bounded(2));
        pump_lines(
            &b"a\nb\nc\nd\n"[..],
            encoding_rs::UTF_8,
            None,
            bounded.clone(),
        )
        .await;
        assert_eq!(
            bounded.dropped(),
            2,
            "DropOldest discarded the two oldest lines"
        );
        // DropNewest and fail-loud likewise tally each discard.
        let newest = SharedLines::new(
            &OutputBufferPolicy::bounded(2).with_overflow(OverflowMode::DropNewest),
        );
        pump_lines(
            &b"a\nb\nc\nd\n"[..],
            encoding_rs::UTF_8,
            None,
            newest.clone(),
        )
        .await;
        assert_eq!(
            newest.dropped(),
            2,
            "DropNewest discarded the two newest lines"
        );
    }

    #[tokio::test]
    async fn start_discarding_drops_the_backlog_and_retains_nothing_after() {
        // The mechanism behind "a discard verb adopts a dropped stream's sink":
        // the buffered backlog is freed and later pushes keep nothing, while the
        // total line count stays exact (still every line the pump has seen).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.push("a".into());
        sink.push("b".into());
        sink.start_discarding();
        assert!(sink.drain().is_empty(), "the buffered backlog is dropped");
        sink.push("c".into());
        assert!(
            sink.drain().is_empty(),
            "lines pushed after discarding are not retained"
        );
        assert_eq!(sink.count(), 3, "every line is still counted");
    }

    #[test]
    fn discarding_oversized_line_skips_overflow_bookkeeping() {
        let policy = OutputBufferPolicy::fail_loud(10).with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        sink.start_discarding();
        sink.record_oversized_line(4);

        assert_eq!(sink.count(), 1, "the oversized line is still counted");
        assert_eq!(sink.dropped(), 0, "discarding skips truncation bookkeeping");
        assert!(
            !sink.overflowed(),
            "discarding skips fail-loud bookkeeping for oversized lines"
        );
    }

    #[tokio::test]
    async fn bounded_zero_without_error_mode_never_overflows() {
        // A plain `bounded(0)` (DropOldest) retains nothing and must NOT flag
        // overflow — only the fail-loud variant errors.
        let sink = SharedLines::new(&OutputBufferPolicy::bounded(0));
        pump_lines(&b"a\nb\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(!sink.overflowed());
    }

    #[tokio::test]
    async fn handler_sees_every_line_even_when_nothing_retained() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::bounded(0));
        pump_lines(
            &b"x\ny\n"[..],
            encoding_rs::UTF_8,
            Some(handler),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 2);
        assert!(
            sink.drain().is_empty(),
            "retain-nothing policy keeps no lines"
        );
        assert_eq!(*seen.lock().unwrap(), vec!["x", "y"]);
    }

    #[tokio::test]
    async fn crlf_only_line_is_one_empty_line() {
        // A bare Windows line ending must read as one (empty) line — the
        // terminator strip may not under- or over-consume.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"\r\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 1);
        assert_eq!(sink.drain(), vec![""]);
    }

    #[tokio::test]
    async fn final_line_without_a_trailing_newline_is_emitted() {
        // A last line ending at EOF with no `\n` must still be delivered
        // (`echo -n`-style output, common to tools whose final line lacks a
        // newline).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"alpha\nomega"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 2, "the un-terminated tail still counts");
        assert_eq!(sink.drain(), vec!["alpha", "omega"]);
    }

    #[tokio::test]
    async fn empty_reader_closes_with_no_lines() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b""[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 0);
        assert!(sink.drain().is_empty());
        assert!(
            matches!(sink.try_pop(), Popped::Closed),
            "the sink must close on EOF so a streaming consumer ends"
        );
    }

    #[tokio::test]
    async fn invalid_multibyte_decodes_lossily_not_fatally() {
        // A lone Shift-JIS lead byte is an invalid sequence: the decode must
        // produce a replacement character, never panic or drop the line.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(
            &[0x82, b'\n'][..],
            encoding_rs::SHIFT_JIS,
            None,
            sink.clone(),
        )
        .await;
        let lines = sink.drain();
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].contains('\u{FFFD}'),
            "invalid bytes decode to the replacement char: {lines:?}"
        );
    }

    #[tokio::test]
    async fn panicking_handler_is_isolated_and_capture_completes() {
        // A panicking handler is caught and disabled; the pump keeps draining,
        // every line is still captured, and the sink closes normally — capture is
        // never the casualty of a progress callback.
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: LineHandler = {
            let calls = calls.clone();
            Arc::new(move |_: &str| {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    panic!("boom on the second line");
                }
            })
        };
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let task = tokio::spawn(pump_lines(
            &b"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"[..],
            encoding_rs::UTF_8,
            Some(handler),
            sink.clone(),
        ));
        task.await
            .expect("the pump task must survive a handler panic");
        assert_eq!(sink.count(), 10, "every line captured despite the panic");
        assert_eq!(
            sink.drain(),
            (1..=10).map(|n| n.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the handler is disabled after its panic (called for lines 1 and 2 only)"
        );
        assert!(
            matches!(sink.try_pop(), Popped::Closed),
            "sink closes normally after the drain"
        );
    }

    // `ChunkedReader` lives at module scope (shared with the `proptests`
    // module below and the `fuzz_decode_pump_lines` fuzz entry point).

    #[tokio::test]
    async fn utf16le_lines_decode_and_split_correctly() {
        // "AB\nCD\n" in UTF-16LE. Each `\n` is the byte pair `0A 00`; the
        // `0A` is a real newline but the trailing `00` is part of the code unit.
        // A byte-level split on `0A` would graft that `00` onto the next line —
        // the streaming decoder splits the *decoded* text instead.
        let bytes = [
            0x41, 0x00, 0x42, 0x00, 0x0A, 0x00, // A B \n
            0x43, 0x00, 0x44, 0x00, 0x0A, 0x00, // C D \n
        ];
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&bytes[..], encoding_rs::UTF_16LE, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["AB", "CD"]);
    }

    #[tokio::test]
    async fn utf16le_code_unit_split_across_reads_is_reassembled() {
        // A 2-byte code unit straddles a read boundary. A per-read decode
        // would mangle it; the persistent decoder holds the partial unit until
        // the next chunk. Chunks: [41 00 42] then [00 0A 00] → "AB".
        let reader = ChunkedReader::new([vec![0x41, 0x00, 0x42], vec![0x00, 0x0A, 0x00]]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_16LE, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["AB"]);
    }

    #[tokio::test]
    async fn utf16le_leading_bom_is_removed_once() {
        // FF FE is the UTF-16LE BOM; `with_bom_removal` strips it once at the
        // stream start, leaving the content line.
        let bytes = [0xFF, 0xFE, 0x41, 0x00, 0x0A, 0x00];
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&bytes[..], encoding_rs::UTF_16LE, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["A"]);
    }

    #[tokio::test]
    async fn utf8_leading_bom_is_removed_once_not_per_line() {
        // A leading UTF-8 BOM (EF BB BF) is stripped once at the start; later
        // lines are untouched (the BOM handling is not re-run per line).
        let bytes = [0xEF, 0xBB, 0xBF, b'h', b'i', b'\n', b'b', b'y', b'e', b'\n'];
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&bytes[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["hi", "bye"]);
    }

    #[tokio::test]
    async fn strips_exactly_one_trailing_cr_not_all() {
        // In "data\r\r\n" only the CR forming the CRLF is a terminator; the
        // earlier CR is content. Must yield "data\r", not "data".
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"data\r\r\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["data\r"]);
    }

    #[tokio::test]
    async fn lone_trailing_cr_at_eof_is_kept_as_content() {
        // A `\r` with no following `\n` is data, not a terminator (`read_until`
        // never split on it; the decoded-split must not either).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"tail\r"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["tail\r"]);
    }

    #[tokio::test]
    async fn mid_stream_read_error_flushes_the_partial_tail() {
        // A complete line, then a partial line, then an IO error. The partial
        // tail must still be emitted, not silently dropped (the error path must
        // flush it like the EOF path does).
        let reader = ChunkedReader::erroring([b"done\npart".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 2, "the partial tail still counts");
        assert_eq!(sink.drain(), vec!["done", "part"]);
        assert!(
            sink.take_read_error().is_some(),
            "the OS read error is recorded on the sink for a consuming finisher"
        );
    }

    #[tokio::test]
    async fn legacy_line_starting_with_bom_bytes_is_not_resniffed() {
        // A Windows-1252 line legitimately starting with FF FE (ÿþ) must stay
        // Windows-1252, not be re-decoded as UTF-16LE: one persistent decoder
        // (with_bom_removal of *this* encoding only) never re-sniffs per line.
        let bytes = [0xFF, 0xFE, b'x', b'\n'];
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&bytes[..], encoding_rs::WINDOWS_1252, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["\u{00FF}\u{00FE}x"]);
    }

    #[tokio::test]
    async fn fail_loud_trips_on_total_even_when_streamed_dry() {
        // `fail_loud(2)` with a consumer draining each line as it arrives. The
        // live backlog never exceeds 2, but the *total* does — the ceiling counts
        // the total seen, not the live backlog, so pops must not free it.
        let sink = SharedLines::new(&OutputBufferPolicy::fail_loud(2));
        sink.push("a".into());
        assert!(matches!(sink.try_pop(), Popped::Line(_))); // drain a
        sink.push("b".into());
        assert!(matches!(sink.try_pop(), Popped::Line(_))); // drain b
        assert!(!sink.overflowed(), "two lines is within the cap");
        sink.push("c".into()); // the 3rd line is over the cap
        assert!(
            sink.overflowed(),
            "the 3rd line trips the ceiling even though the backlog was drained dry"
        );
    }

    #[tokio::test]
    async fn max_bytes_drop_oldest_evicts_to_fit_the_byte_cap() {
        // Byte-bounded ring buffer. Each line "aa" is 2 bytes; a 5-byte cap
        // holds at most two of them — the third evicts the oldest.
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(5);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"aa\nbb\ncc\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["bb", "cc"]);
        assert_eq!(sink.count(), 3, "every line is still counted");
    }

    #[tokio::test]
    async fn max_bytes_drops_a_single_oversized_line_whole() {
        // A line larger than the entire byte cap cannot be retained under a drop
        // mode — it is dropped whole (a line cap alone would have kept it and
        // blown the memory bound, which is the gap the byte cap closes).
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        pump_lines(
            &b"toolong\nok\n"[..],
            encoding_rs::UTF_8,
            None,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["ok"], "the oversized line was dropped");
        assert_eq!(sink.count(), 2);
        assert!(sink.dropped() >= 1);
    }

    #[tokio::test]
    async fn max_bytes_fail_loud_trips_on_byte_total() {
        // A byte fail-loud ceiling errors once cumulative bytes exceed the cap,
        // independent of the line count.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::Error)
            .with_max_bytes(4);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"ab\ncd\nef\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(
            sink.overflowed(),
            "6 cumulative bytes over a 4-byte ceiling must trip it"
        );
    }

    #[tokio::test]
    async fn max_bytes_under_the_cap_does_not_trip_or_drop() {
        // Within the byte cap, nothing is dropped and (under Error) nothing trips.
        let policy = OutputBufferPolicy::fail_loud(10).with_max_bytes(100);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"ab\ncd\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(!sink.overflowed());
        assert_eq!(sink.dropped(), 0);
        assert_eq!(sink.drain(), vec!["ab", "cd"]);
    }

    #[tokio::test]
    async fn max_bytes_drop_newest_keeps_head_within_byte_cap() {
        // DropNewest with a byte cap: keep the earliest lines that fit, drop
        // later ones that would breach it.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(4);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"ab\ncd\nef\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["ab", "cd"]);
    }

    #[tokio::test]
    async fn max_bytes_bounds_a_flood_of_empty_lines_drop_oldest() {
        // A stream of nothing but newlines (`yes ''`-style) contributes 0
        // content bytes per line. Without a derived per-line minimum, DropOldest
        // would never see the backlog as "over" and it would grow unbounded even
        // under a byte cap — the anti-DoS gap this fix closes.
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(100);
        let sink = SharedLines::new(&policy);
        let flood = "\n".repeat(10_000);
        pump_lines(flood.as_bytes(), encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 10_000, "every empty line is still counted");
        let retained = sink.drain();
        assert!(
            retained.len() <= 100,
            "the backlog must stay bounded by the byte cap even for all-empty lines, got {}",
            retained.len()
        );
        assert!(
            sink.dropped() > 0,
            "the flood must be evicted, not retained without bound"
        );
    }

    #[tokio::test]
    async fn max_bytes_bounds_a_flood_of_empty_lines_drop_newest() {
        // Same flood under DropNewest: the "head" retained must also stay
        // bounded rather than growing without limit.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(100);
        let sink = SharedLines::new(&policy);
        let flood = "\n".repeat(10_000);
        pump_lines(flood.as_bytes(), encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 10_000, "every empty line is still counted");
        let retained = sink.drain();
        assert!(
            retained.len() <= 100,
            "the retained head must stay bounded by the byte cap even for all-empty lines, got {}",
            retained.len()
        );
        assert!(
            sink.dropped() > 0,
            "the excess of the flood must be dropped, not retained without bound"
        );
    }

    #[tokio::test]
    async fn max_bytes_error_mode_trips_on_a_flood_of_empty_lines() {
        // Content-byte-only accounting leaves `seen_bytes` at 0 forever for an
        // all-empty-line flood, so a byte-only fail-loud ceiling would never
        // fire — defeating the crate's "memory cannot be exhausted" guarantee.
        // The derived per-line minimum must still trip it.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::Error)
            .with_max_bytes(100);
        let sink = SharedLines::new(&policy);
        let flood = "\n".repeat(10_000);
        pump_lines(flood.as_bytes(), encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(
            sink.overflowed(),
            "a flood of empty lines under a byte cap must trip OverflowMode::Error"
        );
        assert!(
            sink.dropped() > 0,
            "the excess lines must be flagged dropped"
        );
    }

    #[tokio::test]
    async fn max_bytes_skips_an_over_cap_line_streamed_across_reads_without_buffering_it() {
        // An over-cap line arriving as a newline-free flood across many reads
        // (`base64 -w0`-style) must be dropped whole WITHOUT the pump ever
        // buffering it in full — the byte cap bounds the *in-flight* decode buffer.
        // We can't measure memory, but the pump resyncing at the newline (small
        // trailing line retained, flood truncated) proves it skipped rather than
        // accumulated the 50 KB line under an 8-byte cap.
        let reader = ChunkedReader::new([vec![b'X'; 50_000], b"\n".to_vec(), b"tail\n".to_vec()]);
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(8);
        let sink = SharedLines::new(&policy);
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.drain(),
            vec!["tail"],
            "the over-cap flood is dropped; the small line is kept"
        );
        assert_eq!(sink.count(), 2, "both lines are counted");
        assert!(sink.dropped() >= 1, "the over-cap line is a truncation");
    }

    #[tokio::test]
    async fn over_cap_crlf_line_byte_count_is_stable_across_a_read_boundary() {
        // An over-cap CRLF line must record the same content-byte length whether
        // its `\r` and `\n` arrive together or split across a read boundary — else
        // `seen_bytes` (which drives the Error ceiling and truncation total) would
        // depend on chunking. 10 X's + "\r\n" over a 4-byte cap, then a "tail\n"
        // line (4 bytes, retained): the over-cap line counts 10, so both runs see
        // 14 total.
        let content = vec![b'X'; 10];

        // Single chunk: "XXXXXXXXXX\r\ntail\n".
        let mut one = content.clone();
        one.extend_from_slice(b"\r\ntail\n");
        let single = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines(&one[..], encoding_rs::UTF_8, None, single.clone()).await;

        // Split so the CRLF straddles a read: ["XXXXXXXXXX\r", "\ntail\n"].
        let mut first = content.clone();
        first.push(b'\r');
        let reader = ChunkedReader::new([first, b"\ntail\n".to_vec()]);
        let split = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines(reader, encoding_rs::UTF_8, None, split.clone()).await;

        assert_eq!(
            split.seen_bytes(),
            single.seen_bytes(),
            "the CRLF terminator must not be counted only when it lands at a chunk end"
        );
        assert_eq!(
            single.seen_bytes(),
            14,
            "over-cap content (10) excluding the CRLF, plus the retained 'tail' (4)"
        );
        assert_eq!(split.drain(), vec!["tail"], "the over-cap line is dropped");
        assert_eq!(single.drain(), vec!["tail"]);
    }

    #[tokio::test]
    async fn over_cap_skip_keeps_a_lone_cr_as_content_across_reads() {
        // The deferral must not lose a `\r` that is real content: when the byte
        // after a held-back `\r` is NOT `\n` (a lone CR mid-line), it counts. An
        // over-cap line "XXXXXXXXXX\rYYYYY\n" split right after the `\r` records
        // all 16 content bytes (no terminator CR exists here — the `\r` is data).
        let mut first = vec![b'X'; 10];
        first.push(b'\r');
        let reader = ChunkedReader::new([first, b"YYYYY\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.seen_bytes(),
            16,
            "a lone CR before non-newline content is counted, not dropped"
        );
        assert!(
            sink.drain().is_empty(),
            "the over-cap line is dropped whole"
        );
    }

    #[tokio::test]
    async fn crlf_line_at_exactly_the_cap_is_retained_regardless_of_read_boundary() {
        // A line whose *content* is exactly `max_bytes` fits the cap, so it must
        // be retained whether its CRLF arrives in one chunk or split across a read
        // (the verdict must not depend on the chunk boundary). One line only: a
        // second retained line would push the *backlog* past the same 2-byte cap
        // and evict this one — the unrelated DropOldest path.
        let single = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(&b"ab\r\n"[..], encoding_rs::UTF_8, None, single.clone()).await;

        // Split so the CRLF straddles a read: ["ab\r", "\n"].
        let reader = ChunkedReader::new([b"ab\r".to_vec(), b"\n".to_vec()]);
        let split = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(reader, encoding_rs::UTF_8, None, split.clone()).await;

        assert_eq!(
            single.drain(),
            vec!["ab"],
            "one-chunk: an at-cap CRLF line is retained"
        );
        assert_eq!(
            split.drain(),
            vec!["ab"],
            "split CRLF must retain the at-cap line identically — not drop it"
        );
    }

    #[tokio::test]
    async fn over_cap_unterminated_tail_at_eof_is_dropped_not_delivered() {
        // An unterminated final line whose content exceeds the cap must be dropped
        // (and NOT handed to the handler/tee), even though the enter-skip deferred
        // its lone trailing `\r`. "ab\r" at EOF is 3 content bytes (no `\n`, so the
        // `\r` is content) over a 2-byte cap; without the EOF cap re-check it would
        // be emitted.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(
            &b"ab\r"[..],
            encoding_rs::UTF_8,
            Some(handler),
            sink.clone(),
        )
        .await;
        assert!(
            sink.drain().is_empty(),
            "an over-cap unterminated tail is not retained"
        );
        assert!(
            seen.lock().unwrap().is_empty(),
            "an over-cap line is never delivered to the handler"
        );
        assert!(
            sink.dropped() >= 1,
            "the over-cap tail counts as a truncation"
        );
    }

    #[tokio::test]
    async fn error_mode_byte_cap_drains_a_post_trip_flood_without_retaining() {
        // Error mode with a byte cap: after the ceiling trips, a large newline-free
        // flood is still bounded (in-flight bytes skipped, not buffered) and
        // drained to EOF so the child never blocks, while nothing is retained.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::Error)
            .with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        // "abcd" (4 bytes) trips the 3-byte ceiling; a flood and a line follow.
        let reader =
            ChunkedReader::new([b"abcd\n".to_vec(), vec![b'Z'; 20_000], b"\nmore\n".to_vec()]);
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(
            sink.overflowed(),
            "the over-cap first line trips the ceiling"
        );
        assert!(
            sink.drain().is_empty(),
            "nothing is retained under the fail-loud ceiling"
        );
    }

    #[tokio::test]
    async fn byte_cap_judges_a_crlf_line_like_its_lf_twin_at_the_boundary() {
        // The over-cap decision must measure line *content* (excluding the
        // stripped CRLF '\r'), so a CRLF line whose content is exactly `max_bytes`
        // is retained identically to the same content with a bare LF — not wrongly
        // dropped (and, under Error mode, not wrongly tripped).
        let lf = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(&b"ab\n"[..], encoding_rs::UTF_8, None, lf.clone()).await;
        assert_eq!(
            lf.drain(),
            vec!["ab"],
            "LF: 2-byte content fits a 2-byte cap"
        );

        let crlf = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(&b"ab\r\n"[..], encoding_rs::UTF_8, None, crlf.clone()).await;
        assert_eq!(
            crlf.drain(),
            vec!["ab"],
            "CRLF: the same 2-byte content must also fit (the '\\r' is a terminator)"
        );
        assert_eq!(crlf.dropped(), 0, "nothing was over-cap");

        // One byte over (3-byte content) is genuinely over-cap under both endings.
        let over = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(&b"abc\r\n"[..], encoding_rs::UTF_8, None, over.clone()).await;
        assert!(
            over.drain().is_empty(),
            "3-byte content exceeds the 2-byte cap"
        );
        assert!(over.dropped() >= 1);
    }

    #[tokio::test]
    async fn read_error_after_incomplete_multibyte_does_not_fabricate_a_phantom_char() {
        // A complete line, then a lone UTF-8 lead byte (0xC3, an incomplete 2-byte
        // sequence), then a read ERROR. A clean EOF would flush the decoder and
        // turn the dangling byte into U+FFFD, but an error means the stream was
        // truncated mid-character, so the incomplete byte is dropped, never
        // fabricated into a phantom replacement-char line.
        let reader = ChunkedReader::erroring([b"ok\n\xC3".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.drain(),
            vec!["ok"],
            "the truncated lead byte produces no phantom line"
        );
        assert_eq!(sink.count(), 1);
        assert!(
            sink.take_read_error().is_some(),
            "the read error is recorded even though the truncated multibyte tail is dropped"
        );
    }

    #[tokio::test]
    async fn clean_eof_records_no_read_error() {
        // A stream that drains to a normal EOF is a complete capture: the sink must
        // carry no read error, so a finisher reports success rather than
        // `Error::Io` — the non-regression guard against a false-positive read error.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"a\nb\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["a", "b"]);
        assert!(
            sink.take_read_error().is_none(),
            "a clean EOF is a complete capture, not an incomplete one"
        );
    }

    #[tokio::test]
    async fn read_error_on_a_line_boundary_keeps_the_line_and_records_the_error() {
        // The error lands exactly on a line boundary (a complete "done\n", no
        // partial tail): the completed line is retained AND the read error is
        // recorded — a boundary-aligned error is still an incomplete capture, since
        // lines past it may have been lost.
        let reader = ChunkedReader::erroring([b"done\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["done"], "the completed line is retained");
        assert!(
            sink.take_read_error().is_some(),
            "even a boundary-aligned read error is recorded"
        );
    }

    #[tokio::test]
    async fn broken_pipe_read_is_treated_as_clean_eof_not_an_incomplete_capture() {
        // A `BrokenPipe` read (the writer end closing) is the normal end of a child
        // stream — std maps it to `Ok(0)` already, but the pump also defensively
        // folds it into a clean EOF: the buffered line is delivered and NO read
        // error is recorded, so a normal writer-closed stream never spuriously
        // reports `Error::Io`.
        let reader = ChunkedReader::erroring_with(
            [b"tail\n".to_vec()],
            std::io::Error::from(std::io::ErrorKind::BrokenPipe),
        );
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["tail"]);
        assert!(
            sink.take_read_error().is_none(),
            "a broken-pipe read is the normal end of a stream, not an incomplete capture"
        );
    }

    #[tokio::test]
    async fn concurrent_read_errors_on_both_streams_are_each_recorded() {
        // A read error on one stream must not stop the other from draining and
        // recording its own: two pumps run concurrently, each errors, and each sink
        // independently flushes its tail, records its error, and closes — no
        // deadlock, no cross-contamination (the "simultaneous error on the second
        // stream" case).
        let out_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let err_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let out = tokio::spawn(pump_lines(
            ChunkedReader::erroring([b"o1\no2".to_vec()]),
            encoding_rs::UTF_8,
            None,
            out_sink.clone(),
        ));
        let err = tokio::spawn(pump_lines(
            ChunkedReader::erroring([b"e1\ne2".to_vec()]),
            encoding_rs::UTF_8,
            None,
            err_sink.clone(),
        ));
        out.await.expect("stdout pump");
        err.await.expect("stderr pump");
        assert_eq!(out_sink.drain(), vec!["o1", "o2"]);
        assert_eq!(err_sink.drain(), vec!["e1", "e2"]);
        assert!(
            out_sink.take_read_error().is_some(),
            "stdout error recorded"
        );
        assert!(
            err_sink.take_read_error().is_some(),
            "stderr error recorded"
        );
        assert!(
            matches!(out_sink.try_pop(), Popped::Closed),
            "each sink still closes so a streaming consumer ends"
        );
        assert!(matches!(err_sink.try_pop(), Popped::Closed));
    }

    // --- `\r`-aware (CarriageReturn) line-terminator mode --------------------

    #[tokio::test]
    async fn cr_mode_splits_progress_frames_live() {
        // The motivating case: carriage-return progress redraws each become their
        // own line, so a consumer sees them one at a time instead of one giant
        // line only at EOF.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"Progress: 0%\rProgress: 50%\rProgress: 100%\n"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 3, "three frames, not one accumulated line");
        assert_eq!(
            sink.drain(),
            vec!["Progress: 0%", "Progress: 50%", "Progress: 100%"]
        );
    }

    #[tokio::test]
    async fn cr_mode_leading_cr_and_unterminated_tail() {
        // A leading `\r` yields a leading empty frame; the final frame has no
        // trailing terminator and is still emitted at EOF.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"\rA\rB"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["", "A", "B"]);
    }

    #[tokio::test]
    async fn cr_mode_crlf_is_a_single_terminator_no_empty_lines() {
        // A `\r\n` pair must stay ONE terminator — CRLF text reads identically to
        // Newline mode, with no spurious empty line between the `\r` and the `\n`.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"a\r\nb\r\n"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["a", "b"],
            "no empty line between CR and LF"
        );
    }

    #[tokio::test]
    async fn cr_mode_mixed_terminators() {
        // Bare `\r`, bare `\n`, and `\r\n` interleaved: each is one line boundary,
        // and the trailing content with no terminator is the final frame.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"a\rb\nc\r\nd"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn cr_mode_crlf_split_across_reads_stays_one_terminator() {
        // The `\r` and `\n` of a CRLF straddle a read boundary. The deferral must
        // hold the `\r` until the `\n` arrives so it is still one terminator, not a
        // bare-CR frame plus an empty line.
        let reader = ChunkedReader::new([b"a\r".to_vec(), b"\nb\r".to_vec(), b"\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            reader,
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["a", "b"], "split CRLF is one terminator");
    }

    #[tokio::test]
    async fn cr_mode_lone_cr_at_read_boundary_is_a_frame_terminator() {
        // A `\r` at a chunk end whose follower (non-`\n`) arrives next read is a
        // bare-CR frame terminator, resolved once the next byte is seen.
        let reader = ChunkedReader::new([b"a\r".to_vec(), b"b\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            reader,
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn cr_mode_trailing_cr_at_eof_terminates_the_frame() {
        // Unlike Newline mode (where a lone trailing `\r` is content, "tail\r"),
        // in `\r`-aware mode it terminates the final frame — so "tail\r" is "tail".
        let cr = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"tail\r"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            cr.clone(),
        )
        .await;
        assert_eq!(
            cr.drain(),
            vec!["tail"],
            "CR mode: trailing `\\r` terminates"
        );

        // The default mode's behavior is unchanged for the same bytes.
        let nl = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"tail\r"[..],
            encoding_rs::UTF_8,
            LineTerminator::Newline,
            nl.clone(),
        )
        .await;
        assert_eq!(
            nl.drain(),
            vec!["tail\r"],
            "Newline mode: trailing `\\r` is content"
        );
    }

    #[tokio::test]
    async fn cr_mode_default_newline_is_unchanged_lone_cr_is_content() {
        // The default `Newline` mode must keep a mid-line `\r` as content even
        // though the `\r`-aware mode would split there — proving the knob, not the
        // pump, changes the framing.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_term(
            &b"a\rb\n"[..],
            encoding_rs::UTF_8,
            LineTerminator::Newline,
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["a\rb"],
            "Newline mode keeps the inner `\\r`"
        );
    }

    #[tokio::test]
    async fn cr_mode_byte_cap_skips_an_over_cap_frame_but_keeps_small_ones() {
        // A newline-free `\r`-terminated flood over the byte cap is skipped as it
        // streams (never assembled whole), while the small following frame is
        // retained — the byte cap bounds an individual frame, not the whole stream.
        let reader = ChunkedReader::new([vec![b'X'; 50_000], b"\rtail\n".to_vec()]);
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(8);
        let sink = SharedLines::new(&policy);
        pump_lines_term(
            reader,
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["tail"],
            "the over-cap frame is dropped; the small frame is kept"
        );
        assert_eq!(sink.count(), 2, "both frames are counted");
        assert!(sink.dropped() >= 1, "the over-cap frame is a truncation");
    }

    #[tokio::test]
    async fn cr_mode_at_cap_frame_retained_regardless_of_read_boundary() {
        // A frame whose content is exactly `max_bytes` fits and must be retained
        // whether its bare-CR terminator arrives with the content or in the next
        // read — the verdict cannot depend on chunking (the trailing `\r` is
        // excluded from the cap comparison, like a CRLF `\r`). One frame only: a
        // second retained frame would push the backlog past the 2-byte cap and
        // evict this one (the unrelated DropOldest path).
        let single = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines_term(
            &b"ab\r"[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            single.clone(),
        )
        .await;

        // Split so the bare CR lands in the next read: ["ab", "\r"].
        let reader = ChunkedReader::new([b"ab".to_vec(), b"\r".to_vec()]);
        let split = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines_term(
            reader,
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            split.clone(),
        )
        .await;

        assert_eq!(
            single.drain(),
            vec!["ab"],
            "one-chunk: at-cap CR frame retained"
        );
        assert_eq!(
            split.drain(),
            vec!["ab"],
            "split CR must retain the at-cap frame identically — not drop it"
        );
        assert_eq!(split.dropped(), 0, "nothing was over-cap");
    }

    #[tokio::test]
    async fn cr_mode_over_cap_frame_byte_count_is_stable_across_a_read_boundary() {
        // An over-cap frame must record the same content-byte length whether its
        // terminating `\r` arrives with the content or in the next read — else the
        // seen-byte total (driving the Error ceiling and truncation total) would
        // depend on chunking.
        let content = vec![b'X'; 10];

        let mut one = content.clone();
        one.extend_from_slice(b"\rtail\n");
        let single = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines_term(
            &one[..],
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            single.clone(),
        )
        .await;

        let mut first = content.clone();
        first.push(b'\r');
        let reader = ChunkedReader::new([first, b"tail\n".to_vec()]);
        let split = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines_term(
            reader,
            encoding_rs::UTF_8,
            LineTerminator::CarriageReturn,
            split.clone(),
        )
        .await;

        assert_eq!(
            split.seen_bytes(),
            single.seen_bytes(),
            "the CR terminator must not be counted only when it lands at a chunk end"
        );
        assert_eq!(
            single.seen_bytes(),
            14,
            "over-cap content (10) excluding the CR, plus the retained 'tail' (4)"
        );
        assert_eq!(split.drain(), vec!["tail"]);
    }

    #[tokio::test]
    async fn cr_mode_handler_and_tee_see_each_frame() {
        // The handler and tee observe the same per-frame lines as the buffer —
        // one shared notion of "a line" across every sink.
        let buf = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"50%\r100%\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: Some(handler),
                tee: Some(tee_of(VecSink(buf.clone()))),
                terminator: LineTerminator::CarriageReturn,
            },
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["50%", "100%"], "buffer sees frames");
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["50%", "100%"],
            "handler sees frames"
        );
        assert_eq!(
            String::from_utf8(buf.lock().unwrap().clone()).unwrap(),
            "50%\n100%\n",
            "the tee writes each frame followed by a newline"
        );
    }

    /// An in-memory `AsyncWrite` collecting every byte written.
    #[derive(Clone)]
    struct VecSink(Arc<Mutex<Vec<u8>>>);
    impl tokio::io::AsyncWrite for VecSink {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn tee_of(sink: impl tokio::io::AsyncWrite + Send + Unpin + 'static) -> TeeSink {
        Arc::new(tokio::sync::Mutex::new(Box::new(sink)))
    }

    async fn assert_buffered_tee_flushes_after_read_error(stream: &str) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let buffered = tokio::io::BufWriter::with_capacity(1024, VecSink(buf.clone()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            ChunkedReader::erroring([b"complete\npartial".to_vec()]),
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: None,
                tee: Some(tee_of(buffered)),
                terminator: LineTerminator::Newline,
            },
            sink.clone(),
        )
        .await;

        assert_eq!(sink.drain(), vec!["complete", "partial"]);
        assert_eq!(
            &*buf.lock().unwrap(),
            b"complete\npartial\n",
            "{stream} tee must flush through its buffering writer after a read error"
        );
    }

    #[tokio::test]
    async fn tee_writes_each_decoded_line_plus_newline_to_the_async_sink() {
        // The async tee receives every decoded line followed by '\n', while
        // capture still sees the same lines.
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"one\ntwo\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: None,
                tee: Some(tee_of(VecSink(buf.clone()))),
                terminator: LineTerminator::Newline,
            },
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["one", "two"], "capture is unaffected");
        let teed = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(teed, "one\ntwo\n", "the tee got each line + a newline");
    }

    #[tokio::test]
    async fn tee_write_error_is_isolated_and_capture_continues() {
        // A sink that errors on write must not poison the run: the tee is
        // disabled for the rest of the run and capture still gets every line.
        struct ErrSink;
        impl tokio::io::AsyncWrite for ErrSink {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Err(std::io::Error::other("nope")))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"a\nb\nc\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: None,
                tee: Some(tee_of(ErrSink)),
                terminator: LineTerminator::Newline,
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["a", "b", "c"],
            "capture survives a tee write error"
        );
    }

    #[tokio::test]
    async fn stdout_buffered_tee_flushes_after_read_error() {
        assert_buffered_tee_flushes_after_read_error("stdout").await;
    }

    #[tokio::test]
    async fn stderr_buffered_tee_flushes_after_read_error() {
        assert_buffered_tee_flushes_after_read_error("stderr").await;
    }

    #[tokio::test]
    async fn tee_flush_error_is_isolated_and_capture_completes() {
        struct FlushErrSink;
        impl tokio::io::AsyncWrite for FlushErrSink {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::Error::other("nope")))
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"a\nb\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: None,
                tee: Some(tee_of(FlushErrSink)),
                terminator: LineTerminator::Newline,
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["a", "b"],
            "capture survives a tee flush error"
        );
    }

    #[tokio::test]
    async fn tee_and_line_handler_both_fire_independently() {
        // The tee no longer replaces the handler — both run per line.
        let buf = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"x\ny\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: Some(handler),
                tee: Some(tee_of(VecSink(buf.clone()))),
                terminator: LineTerminator::Newline,
            },
            sink.clone(),
        )
        .await;
        assert_eq!(*seen.lock().unwrap(), vec!["x", "y"], "handler fired");
        assert_eq!(
            String::from_utf8(buf.lock().unwrap().clone()).unwrap(),
            "x\ny\n",
            "tee fired"
        );
    }

    // --- Capacity-ceiling boundary pins (T-120) ------------------------------
    //
    // These pin the exact `>`/`<`/`+`/`<=` boundaries in the retention logic
    // (`Inner::over_backlog`, `Inner::would_fit`, the `SharedLines::push`
    // `OverflowMode::Error` branch) and the pump's over-cap skip accounting
    // (`skip_over_cap_len`, the enter-skip guard, the byte-cursor arithmetic)
    // plus the `ChunkedReader` partial-read path — cases the coarse-grained
    // tests above pass either side of the boundary and so left unpinned.

    #[tokio::test]
    async fn over_backlog_byte_ceiling_retains_at_cap_and_evicts_past_it() {
        // DropOldest with a byte cap and no line cap: a retained byte sum sitting
        // *exactly* on `max_bytes` is within budget (not "over"), so nothing is
        // evicted; one byte past it evicts the oldest to fit. Pins the byte
        // comparison at the boundary — a `>=`/`==` there would wrongly evict at
        // the cap, a `<` would never evict.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        sink.push("aa".into()); // 2 bytes
        sink.push("bb".into()); // 2 bytes -> exactly 4, sitting on the cap
        assert_eq!(
            sink.dropped(),
            0,
            "a backlog exactly at the byte cap is not over"
        );
        // A third line pushes the sum to 6 > 4, so the oldest is evicted to fit.
        sink.push("cc".into());
        assert_eq!(
            sink.dropped(),
            1,
            "one byte past the cap evicts exactly one line"
        );
        assert_eq!(
            sink.drain(),
            vec!["bb", "cc"],
            "only the newest two fit the 4-byte cap"
        );
    }

    #[tokio::test]
    async fn over_backlog_derived_line_ceiling_bounds_empty_lines() {
        // Empty lines add 0 content bytes, so only the derived per-line bound
        // (`self.lines.len() > b`) can bound them under a byte cap. Exactly `b`
        // empty lines sit on the bound (retained); the (b+1)-th trips it.
        let at = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(3));
        at.push(String::new());
        at.push(String::new());
        at.push(String::new());
        assert_eq!(
            at.dropped(),
            0,
            "three empty lines sit exactly on the derived cap of 3"
        );
        assert_eq!(
            at.drain(),
            vec!["", "", ""],
            "all three at-bound empty lines are retained"
        );

        let over = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(3));
        for _ in 0..4 {
            over.push(String::new());
        }
        assert_eq!(over.dropped(), 1, "the 4th empty line evicts the oldest");
        assert_eq!(
            over.drain().len(),
            3,
            "the empty-line backlog stays bounded at 3"
        );
    }

    #[tokio::test]
    async fn would_fit_byte_sum_governs_dropnewest_at_the_boundary() {
        // DropNewest keeps the head: a line fits only if the retained byte sum
        // PLUS its own length still fits `max_bytes`. Pins the `self.bytes + len`
        // sum and the `<= b` boundary — a `*`/`-` on the sum, or a `>` on the
        // comparison, changes which lines are judged to fit.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(2);
        let sink = SharedLines::new(&policy);
        sink.push("aa".into()); // 2 bytes -> exactly fills the 2-byte cap
        sink.push("b".into()); // 2 + 1 = 3 > 2 -> cannot fit, dropped
        assert_eq!(
            sink.dropped(),
            1,
            "the over-budget line is dropped, not retained"
        );
        assert_eq!(
            sink.drain(),
            vec!["aa"],
            "only the head that fills the cap is kept"
        );
    }

    #[tokio::test]
    async fn would_fit_derived_line_ceiling_bounds_empty_lines_dropnewest() {
        // Under DropNewest a byte cap must still bound a flood of empty lines via
        // the derived `self.lines.len() < b` count bound (empty lines add 0
        // bytes). Exactly `b` empty lines fit; the (b+1)-th does not.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(2);
        let sink = SharedLines::new(&policy);
        sink.push(String::new());
        sink.push(String::new());
        sink.push(String::new()); // lines.len() is already 2, not < 2 -> dropped
        assert_eq!(
            sink.dropped(),
            1,
            "the 3rd empty line cannot fit the derived 2-line bound"
        );
        assert_eq!(
            sink.drain(),
            vec!["", ""],
            "two empty lines fit the derived bound"
        );
    }

    #[tokio::test]
    async fn error_mode_byte_ceiling_retains_at_cap_and_trips_past_it() {
        // OverflowMode::Error with a byte cap fires on the cumulative seen-byte
        // total. A total sitting exactly on `max_bytes` is within budget
        // (retained, not overflowed); one byte past it trips the fail-loud
        // ceiling. Pins `inner.seen_bytes > b` at the boundary.
        let at = SharedLines::new(
            &OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(4),
        );
        at.push("ab".into()); // seen 2
        at.push("cd".into()); // seen 4 -> exactly on the cap
        assert!(
            !at.overflowed(),
            "a cumulative total exactly at the byte cap does not trip"
        );
        assert_eq!(
            at.drain(),
            vec!["ab", "cd"],
            "both at-cap lines are retained"
        );

        let over = SharedLines::new(
            &OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(4),
        );
        over.push("ab".into()); // seen 2
        over.push("cde".into()); // seen 5 > 4 -> trips
        assert!(
            over.overflowed(),
            "one byte past the cap trips the fail-loud ceiling"
        );
    }

    #[tokio::test]
    async fn error_mode_derived_line_ceiling_trips_on_empty_lines() {
        // Empty lines add 0 to seen_bytes, so only the derived `total_lines > b`
        // bound can trip OverflowMode::Error under a byte cap. Exactly `b` empty
        // lines are within budget; the (b+1)-th trips it.
        let at = SharedLines::new(
            &OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(3),
        );
        at.push(String::new());
        at.push(String::new());
        at.push(String::new());
        assert!(
            !at.overflowed(),
            "three empty lines sit exactly on the derived 3-line bound"
        );
        assert_eq!(
            at.drain(),
            vec!["", "", ""],
            "the at-bound empty lines are retained"
        );

        let over = SharedLines::new(
            &OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(3),
        );
        for _ in 0..4 {
            over.push(String::new());
        }
        assert!(
            over.overflowed(),
            "a 4th empty line trips the derived line ceiling"
        );
    }

    #[tokio::test]
    async fn error_mode_retained_byte_sum_accumulates_exactly() {
        // Under OverflowMode::Error each retained line adds its own byte length
        // to the retained byte sum. That sum never changes an observable verdict
        // on its own (Error mode never consults `over_backlog`/`would_fit`) and
        // has no public getter, so this pins the `inner.bytes += line.len()`
        // accounting by reading the private field directly (same-crate test
        // access) — a `*=`/`-=` there leaves it at 0 (or underflow-panics).
        let sink = SharedLines::new(
            &OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(100),
        );
        sink.push("abc".into()); // +3
        sink.push("de".into()); // +2
        let retained_bytes = sink.inner.lock().expect("SharedLines poisoned").bytes;
        assert_eq!(
            retained_bytes, 5,
            "the retained byte sum is the exact total of kept lines"
        );
    }

    #[tokio::test]
    async fn chunked_reader_partial_read_preserves_the_full_remainder() {
        // A chunk larger than the pump's 8 KiB read buffer is delivered across
        // several `poll_read` calls: the tail beyond `buf.remaining()` must be put
        // back at the front of the queue (`n < chunk.len()`), never dropped. A
        // 10 000-byte line proves every byte survives the partial reads.
        let reader = ChunkedReader::new([vec![b'a'; 10_000], b"\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 1, "one line total");
        let lines = sink.drain();
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0].len(),
            10_000,
            "every byte of the oversized chunk survives the partial reads"
        );
    }

    #[tokio::test]
    async fn over_cap_multibyte_line_skipped_across_reads_stays_on_char_boundaries() {
        // An over-cap line of multibyte UTF-8, arriving split across reads, must
        // be skipped by whole `sub`-length steps that land on character
        // boundaries and account for every content byte — advancing by a fixed
        // 1 byte would slice mid-codepoint and panic, and a broken cursor
        // (`*=`/`-=`) would mis-count. '€' is 3 bytes; five of them (15 bytes)
        // over a 4-byte cap, delivered across three reads so the skip
        // continuation runs.
        let reader = ChunkedReader::new([
            "\u{20ac}\u{20ac}".as_bytes().to_vec(), // "€€" (6 bytes), no terminator
            "\u{20ac}\u{20ac}".as_bytes().to_vec(), // "€€" (6 bytes), still skipping
            "\u{20ac}\n".as_bytes().to_vec(),       // "€\n" -> terminator
        ]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 1, "the over-cap line is counted once");
        assert!(
            sink.drain().is_empty(),
            "the over-cap multibyte line is dropped whole"
        );
        assert_eq!(
            sink.seen_bytes(),
            15,
            "all 15 content bytes are accounted for"
        );
    }

    #[tokio::test]
    async fn byte_cap_under_cap_line_split_across_reads_is_retained() {
        // A line whose content is UNDER the byte cap but which arrives split
        // across reads (no terminator in the first chunk) must be retained once
        // completed — the over-cap skip guard must not fire for an under-cap
        // line. "abc" (3 bytes) under a 5-byte cap, split as ["ab", "c\n"].
        let reader = ChunkedReader::new([b"ab".to_vec(), b"c\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(5));
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.drain(),
            vec!["abc"],
            "an under-cap line split across reads is retained"
        );
        assert_eq!(sink.dropped(), 0, "nothing was over cap");
    }

    #[tokio::test]
    async fn unterminated_tail_exactly_at_byte_cap_is_retained_at_eof() {
        // An unterminated final line whose length is EXACTLY the byte cap fits
        // and must be emitted at EOF, not dropped as over-cap. Pins the EOF tail
        // check `line.len() > c` at the boundary — a `>=`/`==` there wrongly
        // drops the at-cap tail.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        pump_lines(&b"ab"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.drain(),
            vec!["ab"],
            "an unterminated tail exactly at the cap is kept"
        );
        assert_eq!(sink.dropped(), 0, "the at-cap tail is not a truncation");
    }

    /// Property tests over the pump + decoder for arbitrary input, chunked at
    /// arbitrary read boundaries: the hand-written cases above pin known
    /// tricky shapes (Shift-JIS, lone lead bytes, CRLF-at-a-boundary), while
    /// these generate the shapes themselves so the invariants hold for any
    /// chunking, not just the ones a human thought to write down.
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        // `to_chunks` lives at module scope (shared with the
        // `fuzz_decode_pump_lines` fuzz entry point above).

        /// Arbitrary Unicode content for one "line": any scalar value except
        /// `\n`/`\r` (so joining lines unambiguously marks line boundaries —
        /// CRLF-terminator stripping is already covered by the hand-written
        /// cases above) and except U+FEFF (BOM), which `with_bom_removal`
        /// strips once *if it opens the stream* — a per-line oracle can't tell
        /// a real leading BOM from content that merely starts with the same
        /// scalar value, and that stripping is covered by the hand-written
        /// BOM cases above too.
        fn arb_line_content() -> impl Strategy<Value = String> {
            prop::collection::vec(
                any::<char>().prop_filter("no CR/LF/BOM in line content", |c| {
                    !matches!(*c, '\n' | '\r' | '\u{feff}')
                }),
                0..12,
            )
            .prop_map(|chars| chars.into_iter().collect())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// Arbitrary UTF-8 text, built from a known list of lines each properly
            /// `\n`-terminated plus an optional non-empty unterminated tail, then
            /// chunked at arbitrary byte boundaries (so multibyte UTF-8 sequences
            /// routinely split across reads) and pumped through unbounded. The
            /// pump must lose no line, count every line/byte exactly, and never
            /// panic, regardless of chunking.
            #[test]
            fn pump_preserves_lines_and_counts_across_arbitrary_chunking(
                lines in prop::collection::vec(arb_line_content(), 0..12),
                tail in prop::option::of(
                    arb_line_content().prop_filter("tail must be non-empty", |s| !s.is_empty())
                ),
                chunk_sizes in prop::collection::vec(1usize..=7, 1..20),
            ) {
                let mut text = String::new();
                for line in &lines {
                    text.push_str(line);
                    text.push('\n');
                }
                if let Some(t) = &tail {
                    text.push_str(t);
                }
                let mut expected = lines.clone();
                if let Some(t) = &tail {
                    expected.push(t.clone());
                }

                let bytes = text.into_bytes();
                let chunks = to_chunks(&bytes, &chunk_sizes);
                let reader = ChunkedReader::new(chunks);
                let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("current-thread runtime");
                rt.block_on(pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()));

                let expected_bytes: usize = expected.iter().map(String::len).sum();
                prop_assert_eq!(sink.count(), expected.len(), "no line lost or fabricated");
                prop_assert_eq!(sink.seen_bytes(), expected_bytes, "byte counter is exact");
                prop_assert_eq!(sink.dropped(), 0, "unbounded policy drops nothing");
                prop_assert_eq!(sink.drain(), expected, "every line reassembled correctly");
            }

            /// Arbitrary (possibly invalid) bytes, chunked at arbitrary boundaries
            /// and pumped under a handful of encodings including multi-byte-unit
            /// ones (Shift-JIS, UTF-16LE). `encoding_rs` decoders must never panic
            /// on malformed input, and the pump's own counters must stay
            /// internally consistent no matter how garbled the bytes are.
            #[test]
            fn pump_never_panics_on_arbitrary_bytes_under_any_chunking(
                raw in prop::collection::vec(any::<u8>(), 0..512),
                chunk_sizes in prop::collection::vec(1usize..=9, 1..20),
                encoding_idx in 0usize..4,
            ) {
                const ENCODINGS: [&encoding_rs::Encoding; 4] = [
                    encoding_rs::UTF_8,
                    encoding_rs::SHIFT_JIS,
                    encoding_rs::UTF_16LE,
                    encoding_rs::WINDOWS_1252,
                ];
                let encoding = ENCODINGS[encoding_idx];

                let chunks = to_chunks(&raw, &chunk_sizes);
                let reader = ChunkedReader::new(chunks);
                let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("current-thread runtime");
                rt.block_on(pump_lines(reader, encoding, None, sink.clone()));

                // Reaching here without panicking (or hanging) is the primary
                // invariant. The counters must also stay internally consistent:
                // the retained backlog can never exceed the total lines seen.
                let lines = sink.drain();
                prop_assert!(lines.len() <= sink.count());
            }
        }
    }
}
