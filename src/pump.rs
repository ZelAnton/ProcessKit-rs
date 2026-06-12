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
    /// not falsely reported as truncated (Б4).
    dropped: AtomicUsize,
}

struct Inner {
    lines: VecDeque<String>,
    max: Option<usize>,
    mode: OverflowMode,
    closed: bool,
    /// Set when `OverflowMode::Error` is active and the buffer fills — the
    /// consuming path turns this into [`Error::OutputTooLarge`](crate::Error::OutputTooLarge).
    overflowed: bool,
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
                max: policy.max_lines,
                mode: policy.overflow,
                closed: false,
                overflowed: false,
            }),
            notify: Notify::new(),
            count: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        })
    }

    // pub(crate): the pump feeds lines through here; tests also pre-fill a sink
    // directly (e.g. the `OutputEvents` fairness test). Crate-internal only.
    pub(crate) fn push(&self, line: String) {
        // Count every line, even one we are about to drop.
        self.count.fetch_add(1, Ordering::Relaxed);
        // Whether *this* line (or a displaced older one) was discarded by the
        // policy — distinct from a streaming consumer's pop. Tallied into
        // `dropped` so the truncation signal ignores consumed-by-stream lines.
        let mut policy_dropped = false;
        {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            match inner.max {
                // Retain-nothing ceiling: still trips the fail-loud flag — with a
                // 0-line cap, *any* line is already over it. (`fail_loud(0)` =
                // "tolerate no output; error on the first line.") DropOldest /
                // DropNewest just discard silently as before.
                Some(0) => {
                    policy_dropped = true;
                    if matches!(inner.mode, OverflowMode::Error) {
                        inner.overflowed = true;
                    }
                }
                Some(n) if inner.lines.len() >= n => match inner.mode {
                    OverflowMode::DropOldest => {
                        inner.lines.pop_front();
                        inner.lines.push_back(line);
                        policy_dropped = true; // an older line was discarded
                    }
                    OverflowMode::DropNewest => policy_dropped = true, // drop the incoming line
                    OverflowMode::Error => {
                        // Mark overflow and drop the incoming line; the pipe
                        // is still drained so the child never blocks.
                        inner.overflowed = true;
                        policy_dropped = true;
                    }
                },
                // D9c: `Error` overflow with NO cap (`unbounded().with_overflow(Error)`)
                // used to be a silent no-op. It is a misconfiguration — a fail-loud
                // ceiling with no ceiling — so treat it as zero-tolerance: mark
                // overflow on any line (dropped; the pipe is still drained) and let
                // the consuming verb surface `Error::OutputTooLarge`. Use
                // `fail_loud(n)` for a real cap.
                None if matches!(inner.mode, OverflowMode::Error) => {
                    inner.overflowed = true;
                    policy_dropped = true;
                }
                _ => inner.lines.push_back(line),
            }
        }
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        // `notify_one` stores a permit if no consumer is waiting yet, so a
        // streaming consumer that registers just after this can't miss it.
        self.notify.notify_one();
    }

    fn close(&self) {
        // Recover a poisoned lock instead of panicking: `close` runs from a
        // `Drop` guard on the pump task's unwind path (see `pump_lines`), where a
        // second panic would abort the process. Only the `closed` flag is set
        // here, and that is safe regardless of any prior poisoning.
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

    /// Total lines seen by the pump (including dropped ones).
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Lines discarded by the buffer policy (DropOldest/DropNewest/Error), not
    /// counting lines a streaming consumer popped. `> 0` iff output was actually
    /// truncated by the policy (Б4).
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
        inner.lines.drain(..).collect()
    }

    /// Non-blocking pop for the streaming consumer.
    pub(crate) fn try_pop(&self) -> Popped {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        if let Some(line) = inner.lines.pop_front() {
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

/// Drain `reader` into `sink` line by line, decoding text with `encoding` and
/// invoking `handler` (if any). Always reads to EOF so the child never blocks
/// on a full pipe; on an IO error it flushes what it has and closes the sink.
///
/// A **panicking handler does not poison the run**: the panic is caught, the
/// handler is disabled for the rest of the run (and the fact surfaced as a
/// `tracing` warn when the feature is on), and pumping continues — the child
/// is still drained and the final result still carries every line. The
/// callback seam is handed to consumers' consumers, so "panic-free or else"
/// is not a re-exportable contract.
///
/// **Decoding (Б7/Э3):** bytes are fed through a single persistent
/// `encoding_rs::Decoder` and the *decoded* text is split on the `\n`
/// character — correct for every encoding, including non-ASCII-compatible ones
/// (UTF-16LE/BE, whose code units contain `0x0A` bytes that are *not* line
/// breaks) and stateful ones (ISO-2022-JP shift state carries across reads).
/// One persistent decoder also means a byte-order mark is handled once at the
/// stream start (`with_bom_removal`: a leading BOM *of the chosen encoding* is
/// stripped, never a foreign one — so a legacy line that happens to start with
/// BOM-looking bytes is not silently re-decoded as UTF-16). Each line is
/// stripped of its `\n` and, if present, exactly **one** preceding `\r`
/// (Э1: a CRLF terminator — not every trailing CR). The final line is emitted
/// even without a trailing newline, on both EOF and a mid-stream read error
/// (Э2: the partial tail is flushed, not dropped).
pub(crate) async fn pump_lines<R>(
    mut reader: R,
    encoding: &'static Encoding,
    handler: Option<LineHandler>,
    sink: Arc<SharedLines>,
) where
    R: AsyncRead + Unpin,
{
    // Close the sink on *every* exit from this task — including the
    // can't-happen-anymore handler unwind (defense in depth: a panic out of
    // this loop must never leave a streaming `StdoutLines` consumer parked).
    struct CloseOnDrop(Arc<SharedLines>);
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    let sink = CloseOnDrop(sink);
    let mut handler = handler;

    // Emit one decoded line: run the (panic-isolated) handler, then buffer it.
    fn emit(handler: &mut Option<LineHandler>, sink: &SharedLines, line: String) {
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
        sink.push(line);
    }

    let mut decoder = encoding.new_decoder_with_bom_removal();
    let mut pending = String::new(); // decoded text not yet split into a line
    let mut chunk = [0u8; 8192];
    loop {
        // Treat a read error like EOF: flush what we have and stop (the child is
        // reaped by its group). `last` triggers the decoder's end-of-stream
        // flush (a trailing incomplete sequence becomes one replacement char).
        let (n, last) = match reader.read(&mut chunk).await {
            Ok(0) => (0, true),
            Ok(n) => (n, false),
            Err(_) => (0, true),
        };
        // Reserve the decoder's worst-case output up front so `decode_to_string`
        // (which uses the `String`'s spare capacity as its output limit, never
        // reallocating) consumes the whole chunk in one call.
        if let Some(need) = decoder.max_utf8_buffer_length(n) {
            pending.reserve(need);
        }
        let _ = decoder.decode_to_string(&chunk[..n], &mut pending, last);

        // Split out every complete line decoded so far.
        while let Some(nl) = pending.find('\n') {
            let mut line: String = pending.drain(..=nl).collect();
            line.pop(); // drop the '\n'
            if line.ends_with('\r') {
                line.pop(); // drop exactly one preceding '\r' (CRLF)
            }
            emit(&mut handler, &sink.0, line);
        }
        if last {
            // Flush a final line that ended at EOF/error without a newline.
            if !pending.is_empty() {
                emit(&mut handler, &sink.0, std::mem::take(&mut pending));
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
        // retain-nothing fast-path must still trip the flag (regression: it
        // used to short-circuit before the overflow-mode check).
        let sink = SharedLines::new(&OutputBufferPolicy::fail_loud(0));
        pump_lines(&b"oops\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert!(sink.overflowed(), "any line is over a 0-line ceiling");
        assert!(sink.drain().is_empty(), "still retains nothing");
    }

    #[tokio::test]
    async fn unbounded_with_error_mode_is_zero_tolerance_not_inert() {
        // D9c: `unbounded().with_overflow(Error)` was a silent no-op; it must now
        // fail loud on any output (and retain nothing, like fail_loud(0)).
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
        // Б4: the truncation signal must reflect lines the *policy* discarded,
        // not lines a streaming consumer popped. Under the default unbounded
        // policy, popping lines must leave dropped() == 0 (nothing truncated).
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
        // A last line that ends at EOF with no `\n` must still be delivered:
        // `read_until` returns the un-terminated tail, and the terminator strip
        // must be a no-op rather than dropping the line. (`echo -n`-style output,
        // and many tools whose final line lacks a newline.)
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
        // The panic-isolation contract: a user handler that panics is caught
        // and disabled; the pump keeps draining, EVERY line is still captured,
        // and the sink closes normally. (Capture is never the casualty of a
        // progress callback.)
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
        // Б7: "AB\nCD\n" in UTF-16LE. Each `\n` is the byte pair `0A 00`; the
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
        // Б7: a 2-byte code unit straddles a read boundary. A per-read decode
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
        // Э1: in "data\r\r\n" only the CR forming the CRLF is a terminator; the
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
        // Э2: a complete line, then a partial line, then an IO error. The partial
        // tail must still be emitted, not silently dropped (the EOF path already
        // flushed it; the error path must too).
        let reader = ChunkedReader::erroring([b"done\npart".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(sink.count(), 2, "the partial tail still counts");
        assert_eq!(sink.drain(), vec!["done", "part"]);
    }

    #[tokio::test]
    async fn legacy_line_starting_with_bom_bytes_is_not_resniffed() {
        // Э3: a Windows-1252 line legitimately starting with FF FE (ÿþ) must stay
        // Windows-1252, not be silently re-decoded as UTF-16LE. The old per-line
        // `Encoding::decode` sniffed a BOM on every line; one persistent decoder
        // (with_bom_removal of *this* encoding only) does not.
        let bytes = [0xFF, 0xFE, b'x', b'\n'];
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&bytes[..], encoding_rs::WINDOWS_1252, None, sink.clone()).await;
        assert_eq!(sink.drain(), vec!["\u{00FF}\u{00FE}x"]);
    }
}
