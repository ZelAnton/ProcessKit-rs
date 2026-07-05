//! Background output pump: drain a child's stream line by line into a shared,
//! bounded buffer, decoding text and feeding optional per-line handlers and a
//! live line counter.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use encoding_rs::Encoding;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Notify;

use crate::buffer::{OutputBufferPolicy, OverflowMode};

/// A push-style per-line callback (e.g. tee each line to a log).
pub(crate) type LineHandler = Arc<dyn Fn(&str) + Send + Sync>;

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
    fn over_backlog(&self) -> bool {
        self.max_lines.is_some_and(|n| self.lines.len() > n)
            || self.max_bytes.is_some_and(|b| self.bytes > b)
    }

    /// Whether a line of `len` bytes would still fit both ceilings if appended.
    fn would_fit(&self, len: usize) -> bool {
        self.max_lines.is_none_or(|n| self.lines.len() < n)
            && self.max_bytes.is_none_or(|b| self.bytes + len <= b)
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
                                    || bytes_cap.is_some_and(|b| inner.seen_bytes > b)
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
    /// the drop modes it sets the truncation signal. Mirrors the "cannot fit"
    /// accounting in [`push`](Self::push) for a line the pump never buffered (so
    /// it is also not delivered to the per-line handler or tee).
    pub(crate) fn record_oversized_line(&self, byte_len: usize) {
        self.count.fetch_add(1, Ordering::Relaxed);
        {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            inner.seen_bytes = inner.seen_bytes.saturating_add(byte_len);
            if matches!(inner.mode, OverflowMode::Error) {
                inner.overflowed = true;
            }
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
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

/// The no-tee shorthand over [`pump_lines_core`] — used by this module's tests
/// (production always threads the optional tee through `pump_lines_core`).
#[cfg(test)]
pub(crate) async fn pump_lines<R>(
    reader: R,
    encoding: &'static Encoding,
    handler: Option<LineHandler>,
    sink: Arc<SharedLines>,
) where
    R: AsyncRead + Unpin,
{
    pump_lines_core(reader, encoding, handler, None, sink).await
}

/// Drain `reader` into `sink` line by line, decoding text with `encoding`,
/// invoking `handler` (if any) and writing each line to `tee` (if any). Always
/// reads to EOF so the child never blocks on a full pipe; on an IO error it
/// flushes what it has and closes the sink.
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
/// `encoding_rs::Decoder` and the *decoded* text is split on the `\n`
/// character — correct for every encoding, including non-ASCII-compatible ones
/// (UTF-16LE/BE, whose code units contain `0x0A` bytes that are *not* line
/// breaks) and stateful ones (ISO-2022-JP shift state carries across reads).
/// One persistent decoder also means a byte-order mark is handled once at the
/// stream start (`with_bom_removal`: a leading BOM *of the chosen encoding* is
/// stripped, never a foreign one — so a legacy line that happens to start with
/// BOM-looking bytes is not silently re-decoded as UTF-16). Each line is
/// stripped of its `\n` and, if present, exactly **one** preceding `\r`
/// (a CRLF terminator — not every trailing CR). The final line is emitted
/// even without a trailing newline, on both EOF and a mid-stream read error
/// (the partial tail is flushed, not dropped).
pub(crate) async fn pump_lines_core<R>(
    mut reader: R,
    encoding: &'static Encoding,
    handler: Option<LineHandler>,
    tee: Option<TeeSink>,
    sink: Arc<SharedLines>,
) where
    R: AsyncRead + Unpin,
{
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
        if let Some(h) = handler {
            // AssertUnwindSafe is sound: the handler is `Fn` (no `&mut` state to
            // observe torn) and is dropped right after a panic.
            let invoked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(&line)));
            if invoked.is_err() {
                *handler = None;
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    "line handler panicked; disabled for the rest of the run"
                );
            }
        }
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

    // Byte length of the line content ending at the `\n` at index `nl`, excluding
    // a CRLF `\r` — matching `push`'s retained-content definition, so the over-cap
    // decision judges a CRLF line exactly like its LF twin.
    fn content_len(pending: &str, nl: usize) -> usize {
        if nl > 0 && pending.as_bytes()[nl - 1] == b'\r' {
            nl - 1
        } else {
            nl
        }
    }

    // Discard the buffered prefix of an over-cap line being skipped, returning its
    // content-byte count and emptying `pending` — EXCEPT a single trailing `\r`,
    // held back rather than counted in case it is the CR of a CRLF whose `\n`
    // lands in the next chunk (a terminator, which `content_len` excludes).
    // Deferring keeps the recorded length identical regardless of read boundary; a
    // `\r` that turns out to be mid-line content is counted later (a subsequent
    // split, another `skip_over_cap` pass, or the EOF finalizer carries it on).
    fn skip_over_cap(pending: &mut String) -> usize {
        if pending.ends_with('\r') {
            let counted = pending.len() - 1;
            pending.drain(..counted); // keep only the trailing '\r'
            counted
        } else {
            let counted = pending.len();
            pending.clear();
            counted
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
        // phantom replacement char / final line.
        let (n, eof, errored) = match reader.read(&mut chunk).await {
            Ok(0) => (0, true, false),
            Ok(n) => (n, false, false),
            Err(_) => (0, true, true),
        };
        let last = eof && !errored;
        // Reserve the decoder's worst-case output up front so `decode_to_string`
        // (which uses the `String`'s spare capacity as its output limit, never
        // reallocating) consumes the whole chunk in one call.
        if let Some(need) = decoder.max_utf8_buffer_length(n) {
            pending.reserve(need);
        }
        let _ = decoder.decode_to_string(&chunk[..n], &mut pending, last);

        // Split out every complete line decoded so far, bounding memory by `cap`.
        loop {
            if let Some(skipped) = oversized {
                // Skipping an over-cap line: discard through its newline, keeping
                // only its length for accounting.
                match pending.find('\n') {
                    Some(nl) => {
                        let line_len = skipped.saturating_add(content_len(&pending, nl));
                        pending.drain(..=nl);
                        oversized = None;
                        sink.0.record_oversized_line(line_len);
                    }
                    None => {
                        oversized = Some(skipped.saturating_add(skip_over_cap(&mut pending)));
                        break;
                    }
                }
            } else {
                match pending.find('\n') {
                    Some(nl) => {
                        // Compare *content* length (excluding a CRLF `\r`) to the
                        // cap, so a CRLF line is judged exactly like its LF twin.
                        let len = content_len(&pending, nl);
                        if cap.is_none_or(|c| len <= c) {
                            let mut line: String = pending.drain(..=nl).collect();
                            line.pop(); // drop the '\n'
                            if line.ends_with('\r') {
                                line.pop(); // drop exactly one preceding '\r' (CRLF)
                            }
                            emit(&mut handler, &mut tee, &sink.0, line).await;
                        } else {
                            // Over-cap line, newline already here: drop it whole,
                            // counting its length.
                            pending.drain(..=nl);
                            sink.0.record_oversized_line(len);
                        }
                    }
                    // No newline yet and already over the cap: skip to the newline.
                    // A lone *trailing* `\r` may be a CRLF terminator, so it alone
                    // must not push the line over the cap — else a content-at-cap
                    // CRLF line is dropped when its `\r`/`\n` straddle a read but
                    // retained in one chunk. Exclude that byte; the next read
                    // re-decides (terminator → fits, or content → counts).
                    None if cap.is_some_and(|c| {
                        pending.len() - usize::from(pending.ends_with('\r')) > c
                    }) =>
                    {
                        oversized = Some(skip_over_cap(&mut pending));
                        break;
                    }
                    None => break,
                }
            }
        }

        if eof {
            // Finalize a final line (or an un-terminated over-cap tail).
            if let Some(skipped) = oversized.take() {
                // An un-terminated tail has no '\n', so a trailing '\r' is content.
                let line_len = skipped.saturating_add(pending.len());
                pending.clear();
                sink.0.record_oversized_line(line_len);
            } else if !pending.is_empty() {
                // An un-terminated final line: no '\n', so a trailing '\r' is
                // content. Re-apply the byte cap: the enter-skip deferred a lone
                // trailing '\r' in case a '\n' followed, but at EOF none does, so
                // an over-cap tail must be dropped (counted, never handed to the
                // handler/tee) like any over-cap line — not emitted.
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
            break;
        }
    }
    // `sink` (the guard) closes here.
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

    /// A reader that yields predefined byte chunks one `poll_read` at a time,
    /// then EOFs (or returns one IO error) — to exercise cross-read decoding and
    /// the mid-stream-error flush deterministically.
    struct ChunkedReader {
        chunks: VecDeque<Vec<u8>>,
        err_at_end: bool,
    }

    impl ChunkedReader {
        fn new(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                err_at_end: false,
            }
        }

        fn erroring(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                err_at_end: true,
            }
        }
    }

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
            } else if self.err_at_end {
                self.err_at_end = false;
                std::task::Poll::Ready(Err(std::io::Error::other("boom")))
            } else {
                std::task::Poll::Ready(Ok(())) // 0 bytes filled == EOF
            }
        }
    }

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

    #[tokio::test]
    async fn tee_writes_each_decoded_line_plus_newline_to_the_async_sink() {
        // The async tee receives every decoded line followed by '\n', while
        // capture still sees the same lines.
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"one\ntwo\n"[..],
            encoding_rs::UTF_8,
            None,
            Some(tee_of(VecSink(buf.clone()))),
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
            encoding_rs::UTF_8,
            None,
            Some(tee_of(ErrSink)),
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
            encoding_rs::UTF_8,
            Some(handler),
            Some(tee_of(VecSink(buf.clone()))),
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

    /// Property tests over the pump + decoder for arbitrary input, chunked at
    /// arbitrary read boundaries: the hand-written cases above pin known
    /// tricky shapes (Shift-JIS, lone lead bytes, CRLF-at-a-boundary), while
    /// these generate the shapes themselves so the invariants hold for any
    /// chunking, not just the ones a human thought to write down.
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Split `bytes` into consecutive, non-empty chunks whose lengths cycle
        /// through `sizes` (each clamped to at least 1 so `ChunkedReader` never
        /// sees a zero-length chunk, which it — like a real EOF read — would
        /// read as end of stream). An empty `bytes` yields no chunks.
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
