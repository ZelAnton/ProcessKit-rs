//! Background output pump: drain a child's stream line by line into a shared,
//! bounded buffer, decoding text and feeding optional per-line handlers and
//! live line/byte counters.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use encoding_rs::{Encoding, UTF_8};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Notify;

use crate::buffer::{
    LineTerminator, OutputBufferPolicy, OutputStream, OverflowMode, SharedCapturePolicy,
};

/// Shared resettable clock for stdout/stderr activity. Both line pumps (and the
/// raw stdout path) update one instance, so activity on either stream resets the
/// same inactivity watchdog.
pub(crate) struct OutputActivity {
    last: Mutex<tokio::time::Instant>,
    changed: Notify,
}

impl OutputActivity {
    pub(crate) fn new(started: tokio::time::Instant) -> Self {
        Self {
            last: Mutex::new(started),
            changed: Notify::new(),
        }
    }

    pub(crate) fn record(&self) {
        *self.last.lock().expect("output activity clock poisoned") = tokio::time::Instant::now();
        self.changed.notify_waiters();
    }

    /// Wait until a complete `window` passes after the most recent activity.
    /// Enabling the notification before reading `last` closes both missed-wake
    /// windows: an earlier write is in the clock snapshot, while a later write
    /// stores a notification for this waiter and restarts the loop.
    pub(crate) async fn wait_for_inactivity(&self, window: std::time::Duration) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let remaining = window
                .checked_sub(
                    self.last
                        .lock()
                        .expect("output activity clock poisoned")
                        .elapsed(),
                )
                .unwrap_or(std::time::Duration::ZERO);
            tokio::select! {
                biased;
                () = &mut changed => continue,
                () = tokio::time::sleep(remaining) => return,
            }
        }
    }
}

// The oversized-line paths deliberately discard decoded text before it can
// accumulate. Unit tests need to observe that internal bound without making it
// part of the production pump contract; task-local storage keeps parallel tests
// isolated and compiles out of non-test builds.
#[cfg(test)]
#[derive(Default)]
struct PumpTestProbe {
    max_pending_bytes: AtomicUsize,
    skip_calls: AtomicUsize,
    guard_entries: AtomicUsize,
}

#[cfg(test)]
impl PumpTestProbe {
    fn max_pending_bytes(&self) -> usize {
        self.max_pending_bytes.load(Ordering::Relaxed)
    }

    fn skip_calls(&self) -> usize {
        self.skip_calls.load(Ordering::Relaxed)
    }

    fn guard_entries(&self) -> usize {
        self.guard_entries.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
tokio::task_local! {
    static PUMP_TEST_PROBE: Arc<PumpTestProbe>;
}

#[cfg(test)]
fn observe_pending(pending: &str) {
    let _ = PUMP_TEST_PROBE.try_with(|probe| {
        probe
            .max_pending_bytes
            .fetch_max(pending.len(), Ordering::Relaxed);
    });
}

#[cfg(test)]
fn observe_skip_call() {
    let _ = PUMP_TEST_PROBE.try_with(|probe| {
        probe.skip_calls.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
fn observe_guard_entry() {
    let _ = PUMP_TEST_PROBE.try_with(|probe| {
        probe.guard_entries.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(all(test, feature = "process-control"))]
thread_local! {
    static PARTIAL_TAIL_TEST_TX:
        std::cell::RefCell<Option<tokio::sync::mpsc::UnboundedSender<String>>> =
        const { std::cell::RefCell::new(None) };
}

/// Test-only observation of the real pump publication seam. Pipeline regression
/// tests use it to prove that unterminated stdout/stderr text reached
/// `SharedLines::partial_tail` before they fire teardown.
#[cfg(all(test, feature = "process-control"))]
pub(crate) struct PartialTailPublicationGuard {
    receiver: tokio::sync::mpsc::UnboundedReceiver<String>,
}

#[cfg(all(test, feature = "process-control"))]
impl PartialTailPublicationGuard {
    pub(crate) async fn wait_for_all(&mut self, expected: &[&str]) {
        let mut remaining: std::collections::BTreeSet<String> =
            expected.iter().map(|value| (*value).to_owned()).collect();
        while !remaining.is_empty() {
            let value = self
                .receiver
                .recv()
                .await
                .expect("partial-tail publication sender remains installed");
            remaining.remove(&value);
        }
    }
}

#[cfg(all(test, feature = "process-control"))]
impl Drop for PartialTailPublicationGuard {
    fn drop(&mut self) {
        PARTIAL_TAIL_TEST_TX.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(all(test, feature = "process-control"))]
pub(crate) fn observe_partial_tail_publications() -> PartialTailPublicationGuard {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    PARTIAL_TAIL_TEST_TX.with(|slot| {
        assert!(
            slot.borrow_mut().replace(sender).is_none(),
            "partial-tail publication observer already installed"
        );
    });
    PartialTailPublicationGuard { receiver }
}

#[cfg(all(test, feature = "process-control"))]
fn publish_partial_tail_for_test(tail: &str) {
    if tail.is_empty() {
        return;
    }
    PARTIAL_TAIL_TEST_TX.with(|slot| {
        if let Some(sender) = slot.borrow().as_ref() {
            let _ = sender.send(tail.to_owned());
        }
    });
}

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

/// Run the optional consumer [`CapturePolicy`](crate::CapturePolicy) over one
/// decoded `line` just before it enters the backlog, returning the text to
/// retain. `None` policy (the default) returns the line verbatim with no
/// allocation. A policy that returns [`Cow::Borrowed`] pointing back at the
/// input `line` reuses the already-owned `String` (no re-allocation); a
/// [`Cow::Owned`], or a `Cow::Borrowed` of some *other* text (e.g. a
/// `&'static` placeholder), retains that text — copied when borrowed.
///
/// The policy is **panic-isolated** like [`invoke_handler_isolated`], but
/// **fails closed**: because this seam exists to scrub secrets, a panicking
/// policy must not fall back to retaining the raw (possibly-secret) line, so the
/// line is retained *empty* instead. The policy is **not** disabled — it is
/// retried on the next line — so a transient panic can't silently leave the rest
/// of the run un-redacted; a policy that panics on every line simply blanks
/// every line (a loud, safe symptom), never leaking. `AssertUnwindSafe` is sound
/// for the same reason as the handler: the closure only borrows the shared
/// `&dyn CapturePolicy` and the `&line`, and neither is observed torn after a
/// caught panic.
fn apply_capture_policy(
    policy: &Option<SharedCapturePolicy>,
    stream: OutputStream,
    line: String,
) -> String {
    let Some(policy) = policy else {
        return line;
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        policy.on_capture(stream, &line)
    }));
    match outcome {
        Ok(Cow::Owned(redacted)) => redacted,
        Ok(Cow::Borrowed(borrowed)) => {
            // Reuse the already-owned `line` (no re-allocation) ONLY when the
            // policy handed back exactly it — same pointer and length. A policy
            // may instead borrow a *different* `&str` (a `&'static` placeholder
            // like `""`), which must be copied, not confused for the input line.
            // `borrowed` is a plain reference with no drop glue, so its borrow of
            // `line` ends here, freeing `line` to move in the identity branch.
            let is_input_line =
                std::ptr::eq(borrowed.as_ptr(), line.as_ptr()) && borrowed.len() == line.len();
            if is_input_line {
                line
            } else {
                borrowed.to_owned()
            }
        }
        Err(_) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "processkit",
                stream = stream.name(),
                "capture policy panicked; the line was retained empty (fail-closed) \
                 and the policy left active for later lines"
            );
            String::new()
        }
    }
}

/// `ESC` (`0x1B`) — the introducer of every VT/ANSI escape sequence.
const ESC: u8 = 0x1B;
/// `BEL` (`0x07`) — one of the two OSC string terminators (the other is `ST`,
/// `ESC \`).
const BEL: u8 = 0x07;

/// Whether `b` is a C0 control byte (or `DEL`, `0x7F`) the VT sanitizer drops.
///
/// The horizontal tab `\t` (`0x09`) is kept — it is legitimate column content,
/// not terminal control — and `ESC` (`0x1B`) is excluded here because the escape
/// parser ([`skip_escape`]) consumes it together with its whole sequence. A
/// line-*content* `\r` (kept as content only by [`LineTerminator::Newline`], since
/// the `\r`-aware mode treats it as a terminator that never reaches the content)
/// is deliberately dropped as bare cursor-return noise; `\n` never appears in a
/// content line under either mode.
fn is_strippable_control(b: u8) -> bool {
    (b < 0x20 && b != b'\t' && b != ESC) || b == 0x7F
}

/// Return the index just past the escape sequence beginning at `bytes[start]`
/// (which the caller has verified is [`ESC`]) — the extent [`strip_vt`] drops. An
/// **incomplete** sequence (no terminator before the line ends) returns
/// `bytes.len()`, dropping the dangling remainder rather than leaving a mangled
/// tail in the line.
fn skip_escape(bytes: &[u8], start: usize) -> usize {
    let n = bytes.len();
    // A lone `ESC` at the very end of the line: drop it.
    let Some(&kind) = bytes.get(start + 1) else {
        return n;
    };
    match kind {
        // CSI: `ESC [` (params/intermediates `0x20..=0x3F`)* final `0x40..=0x7E`
        // — colors, cursor moves, erase/scroll, alternate-screen switches.
        b'[' => {
            let mut j = start + 2;
            while j < n && (0x20..=0x3F).contains(&bytes[j]) {
                j += 1;
            }
            // Consume the final byte too; if the line ended first (or the byte
            // there is not a valid final), drop what we have as an incomplete CSI.
            if j < n && (0x40..=0x7E).contains(&bytes[j]) {
                j + 1
            } else {
                j
            }
        }
        // OSC (`ESC ]`) and the DCS/SOS/PM/APC string escapes (`ESC` `P`/`X`/`^`/
        // `_`) share a body terminated by `BEL` or `ST` (`ESC \`).
        b']' | b'P' | b'X' | b'^' | b'_' => skip_string_escape(bytes, start + 2),
        // `ESC ESC`: drop only the first `ESC`; the second is re-parsed from
        // scratch by the caller's loop (so `ESC ESC [ … m` still strips the CSI).
        ESC => start + 1,
        // nF escapes: `ESC` (intermediate `0x20..=0x2F`)+ final `0x30..=0x7E`
        // (charset selection like `ESC ( B`).
        0x20..=0x2F => {
            let mut j = start + 1;
            while j < n && (0x20..=0x2F).contains(&bytes[j]) {
                j += 1;
            }
            if j < n && (0x30..=0x7E).contains(&bytes[j]) {
                j + 1
            } else {
                j
            }
        }
        // Any other two-byte escape (`Fe`/`Fs`/`Fp`: `RIS` = `ESC c`,
        // `IND`/`NEL`/`HTS`, …). Such a final is ALWAYS a single ASCII byte, so
        // consuming it (`start + 2`) lands on a char boundary — and `start + 2
        // <= n` here, since `bytes.get(start + 1)` above already proved
        // `start + 1 < n`. If the byte after `ESC` is instead the non-ASCII lead
        // of a multi-byte UTF-8 scalar (`ESC ©`/`ESC €`/`ESC 🚀` — a truncated or
        // garbled escape a terminal-driven child can emit before a glyph), it is
        // no valid escape final at all: drop only the `ESC` (`start + 1`, still a
        // boundary because `ESC` is ASCII) and leave the scalar as content.
        // Returning `start + 2` there would index the SECOND byte of that scalar
        // — not a char boundary — and later panic `strip_vt`'s `&line[..]` slice
        // with "byte index N is not a char boundary" (R-01).
        _ if kind < 0x80 => start + 2,
        _ => start + 1,
    }
}

/// Skip an OSC/DCS/SOS/PM/APC string body starting at `from`, up to and including
/// its `BEL` or `ST` (`ESC \`) terminator. A bare `ESC` that is **not** the start
/// of an `ST` ends the scan *before* it, so that `ESC` is re-parsed as a fresh
/// escape rather than swallowed with the string body. An unterminated body runs
/// to the end of the line.
fn skip_string_escape(bytes: &[u8], from: usize) -> usize {
    let n = bytes.len();
    let mut j = from;
    while j < n {
        match bytes[j] {
            BEL => return j + 1,
            ESC if bytes.get(j + 1) == Some(&b'\\') => return j + 2,
            ESC => return j,
            _ => j += 1,
        }
    }
    n
}

/// Strip terminal control noise — VT/ANSI escape sequences and lone C0 control
/// codes — from one **complete** decoded line, for the opt-in
/// [`Command::sanitize_vt`](crate::Command::sanitize_vt) capture-hygiene seam.
///
/// Removes what a terminal-driven child (an agentic CLI under `use_pty`, a
/// progress/TUI tool) sprays into its
/// merged output, so a line-oriented consumer — `wait_for_line`/`first_line`,
/// [`output_string`](crate::RunningProcess::output_string), the streaming verbs —
/// sees readable text instead of `\x1b[31m…`-mucked strings. It drops:
///
/// - **CSI** — `ESC [` … a final byte `0x40..=0x7E` (colors, cursor moves,
///   erase/scroll, alternate-screen switches);
/// - **OSC** — `ESC ]` … `BEL`/`ST` (window-title / hyperlink escapes);
/// - **DCS/SOS/PM/APC** string escapes — `ESC` `P`/`X`/`^`/`_` … `ST`;
/// - other two-/n-byte `ESC` escapes (charset selection, `RIS`, …);
/// - lone **C0 control** bytes and `DEL`, **except** the horizontal tab `\t`.
///
/// A sequence with no terminator before the line ends (a dangling `ESC [` at line
/// end) is dropped to the line end, never left as a mangled tail. The pump calls
/// this only on a *complete* line — already reassembled in its `pending` buffer
/// from however many pipe reads it spanned — so an escape **split across chunk/
/// read boundaries is already whole here**, stripped in one piece with no
/// per-read carry state (a valid CSI/OSC never contains `\n`/`\r`, so it cannot
/// straddle a line boundary either).
///
/// Returns [`Cow::Borrowed`] of the **input** line (never of some other `&str`)
/// on its unchanged fast path, so the caller reuses the already-owned line with
/// no re-allocation — and, unlike an arbitrary
/// [`CapturePolicy`](crate::CapturePolicy), that identity
/// is guaranteed by construction, so no pointer check is needed here (contrast
/// [`apply_capture_policy`]'s [[K-065]] guard).
fn strip_vt(line: &str) -> Cow<'_, str> {
    let bytes = line.as_bytes();
    // Fast path: no escape and nothing strippable — return the input untouched,
    // AS the same `Cow::Borrowed(line)` so the caller can reuse its owned buffer.
    // Every escape/control byte is ASCII (`< 0x80`), so scanning bytes never
    // splits a multi-byte UTF-8 character.
    if !bytes.iter().any(|&b| b == ESC || is_strippable_control(b)) {
        return Cow::Borrowed(line);
    }
    let mut out = String::with_capacity(line.len());
    let mut copy_from = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == ESC {
            // Flush the clean run up to the escape, then skip the whole sequence.
            // `copy_from..i` ends on an ASCII byte, always a char boundary.
            out.push_str(&line[copy_from..i]);
            i = skip_escape(bytes, i);
            // `skip_escape` must always return a char boundary so the next
            // `&line[..]` slice can never split a multi-byte scalar (R-01):
            // every arm lands after an ASCII byte, on `ESC`/end, or — for an
            // unrecognized non-ASCII byte after `ESC` — on that scalar's own
            // lead byte. Pin the contract here so a future edit that breaks it
            // trips at the source, not at an incidental downstream slice panic.
            debug_assert!(
                line.is_char_boundary(i),
                "skip_escape returned non-char-boundary index {i} in {line:?}"
            );
            copy_from = i;
        } else if is_strippable_control(b) {
            out.push_str(&line[copy_from..i]);
            i += 1;
            copy_from = i;
        } else {
            i += 1;
        }
    }
    out.push_str(&line[copy_from..]);
    Cow::Owned(out)
}

/// Fuzzing-only entry point for the standalone VT sanitizer target. Lossy UTF-8
/// admits every byte sequence while still driving `strip_vt` with valid `&str`.
#[cfg(fuzzing)]
pub fn fuzz_strip_vt(raw: &[u8]) {
    let input = String::from_utf8_lossy(raw);
    let cleaned = strip_vt(&input).into_owned();

    assert!(
        cleaned.len() <= input.len(),
        "sanitization expanded its input"
    );
    assert!(
        !cleaned
            .bytes()
            .any(|byte| byte == ESC || is_strippable_control(byte)),
        "sanitized output retained terminal control bytes"
    );
    assert_eq!(
        strip_vt(&cleaned),
        cleaned,
        "VT sanitization must be idempotent"
    );
}

/// Run the opt-in VT sanitizer over one decoded `line` when
/// [`sanitize_vt`](StreamConfig::sanitize_vt) is set, returning the text to hand
/// onward. Off (the default) returns the line untouched with no allocation.
///
/// Like [`apply_capture_policy`], this shapes **only** what is retained: the
/// pump runs it in `emit` *after* the raw-observing handler/tee (which keep
/// seeing the un-sanitized decoded line, matching the
/// [`CapturePolicy`](crate::CapturePolicy) boundary,
/// see [[K-066]]) and *before* the capture policy — so a secret-scrubbing policy
/// matches on already-cleaned text, never one where a color escape could hide a
/// token mid-word (`to\x1b[0mken`). It touches none of the `count`/`seen_bytes`/
/// overflow bookkeeping ([[K-054]]/[[K-059]]), which runs on its returned text in
/// `SharedLines::push` exactly as for a raw or capture-policy-shaped line.
fn apply_vt_sanitize(enabled: bool, line: String) -> String {
    if !enabled {
        return line;
    }
    match strip_vt(&line) {
        // `strip_vt` returns `Cow::Borrowed` only on its unchanged fast path, and
        // only ever of the input `line` — so reusing the owned `line` is sound
        // WITHOUT the pointer-identity guard `apply_capture_policy` needs for an
        // arbitrary policy ([[K-065]]); the contract is guaranteed here, not
        // merely observed.
        Cow::Borrowed(_) => line,
        Cow::Owned(scrubbed) => scrubbed,
    }
}

/// A shared, bounded line buffer written by a [`pump_lines_core`] task and read by
/// the bulk collectors (drain) or the streaming consumer (`next_line`).
///
/// The line counter increments on every line *before* the buffer write, so it
/// stays exact even when the policy drops lines.
pub(crate) struct SharedLines {
    inner: Mutex<Inner>,
    notify: Notify,
    /// Monotonic publication generation paired with `notify_waiters`. A
    /// consumer snapshots it with the state it inspected, then `changed`
    /// registers before re-checking it. That closes both the snapshot-to-await
    /// race and `Notify`'s lack of a stored broadcast permit.
    generation: AtomicUsize,
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
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) instead of a silent short read reported as
    /// a full, successful capture. Its own `Mutex` (not `Inner`'s) so the hot
    /// `push` path is untouched; poison is recovered rather than propagated,
    /// matching [`close`](Self::close).
    read_error: Mutex<Option<std::io::Error>>,
    activity: Arc<OutputActivity>,
}

#[derive(Clone)]
struct Inner {
    lines: VecDeque<String>,
    /// Retained-line cap (`OutputBufferPolicy::max_lines`).
    max_lines: Option<usize>,
    /// Retained-byte cap (`OutputBufferPolicy::max_bytes`).
    max_bytes: Option<usize>,
    /// Sum of the retained lines' byte lengths — kept in step with `lines` so
    /// the byte backlog can be bounded without re-summing.
    bytes: usize,
    /// Cumulative raw bytes read from the pipe, including bytes in dropped
    /// lines and line terminators. This is the byte analogue of
    /// `SharedLines::count` and is updated before decoding.
    seen_bytes: usize,
    mode: OverflowMode,
    closed: bool,
    /// Set when `OverflowMode::Error` is active and a ceiling is reached — the
    /// consuming path turns this into [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge).
    overflowed: bool,
    /// Flipped on by [`start_discarding`](SharedLines::start_discarding) when a
    /// streaming consumer is gone and a discard verb adopts this sink: the
    /// still-running pump keeps draining the pipe (so the child never blocks) but
    /// retains nothing, so the backlog can't grow O(total).
    discarding: bool,
    /// `OverflowMode::DropNewest` only: set the first time a line is dropped
    /// (it did not fit a ceiling, or it was an over-cap line the pump skipped via
    /// [`record_oversized_line`](SharedLines::record_oversized_line)). Once set,
    /// every later line is dropped too. This keeps the retained buffer a
    /// **contiguous prefix** (head) of the process's output: without it, an
    /// over-budget long line would be dropped while a *shorter* line after it
    /// still fit and got retained, so the buffer would skip a line and no longer
    /// be a true prefix. Unused by `DropOldest`/`Error` (never read there).
    dropnewest_sealed: bool,
    /// The current **unterminated tail**: decoded content the pump has read but
    /// not yet split into a complete line (no line terminator seen for it yet).
    /// This is the *live partial line* — an interactive prompt like `Password: `
    /// that the child writes without a trailing newline and then blocks on,
    /// which the line-oriented backlog (and so `wait_for_line`/`stdout_lines`)
    /// never sees until the stream ends. It backs
    /// [`RunningProcess::wait_for_output`](crate::RunningProcess::wait_for_output)
    /// and timeout salvage;
    /// it is a *side view* of what is otherwise the pump's local `pending`
    /// buffer, and is deliberately **independent** of every retention/overflow
    /// counter beside it: the pump updates it via
    /// [`set_partial_tail`](SharedLines::set_partial_tail) *without* touching
    /// `seen_bytes` ([[K-059]]), `count`, `dropped`, or the `dropnewest_sealed`
    /// seal ([[K-054]]), so exposing the tail can never re-decide retention or
    /// shift the byte/line accounting an existing consumer relies on. The text
    /// is **raw** — pre-`capture_policy` — mirroring `handler`/`tee`/`raw_tee`'s
    /// observation category (see `pump_lines_core`'s `emit`).
    partial_tail: String,
    /// Whether `partial_tail` currently represents an unfinished line. An
    /// over-cap unfinished line has an empty tail but remains pending so a
    /// timeout salvage can count it as dropped.
    partial_tail_pending: bool,
    /// Whether the pending line was already rejected by the in-flight byte cap.
    partial_tail_oversized: bool,
    /// Whether the published tail's text has already reached the backlog as part
    /// of a completed line. Every completed-line path sets it in its own critical
    /// section (`SharedLines::supersede_partial_tail_locked`), which covers both
    /// EOF finalization — where the tail *is* the final line, kept visible to
    /// `wait_for_output` — and mid-stream lines, where the pump publishes the
    /// replacement tail only one lock acquisition later. Without that seal a
    /// timeout snapshot landing in between would append an already-drained line's
    /// prefix a second time. Cleared by every fresh publish
    /// (`set_partial_tail_state`), including one whose text repeats the sealed
    /// tail verbatim.
    partial_tail_finalized: bool,
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
    Empty(ChangeToken),
    /// No line available and the pump has finished.
    Closed,
}

/// Opaque generation observed alongside a [`SharedLines`] state snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChangeToken(usize);

impl SharedLines {
    #[cfg(any(test, fuzzing))]
    pub(crate) fn new(policy: &OutputBufferPolicy) -> Arc<Self> {
        Self::new_with_activity(
            policy,
            Arc::new(OutputActivity::new(tokio::time::Instant::now())),
        )
    }

    pub(crate) fn new_with_activity(
        policy: &OutputBufferPolicy,
        activity: Arc<OutputActivity>,
    ) -> Arc<Self> {
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
                dropnewest_sealed: false,
                partial_tail: String::new(),
                partial_tail_pending: false,
                partial_tail_oversized: false,
                partial_tail_finalized: false,
            }),
            notify: Notify::new(),
            generation: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            read_error: Mutex::new(None),
            activity,
        })
    }

    pub(crate) fn push(&self, line: String) {
        // Count every line, even one we are about to drop.
        let total_lines = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        // Whether the policy discarded a line here — distinct from a streaming
        // consumer's pop, so the truncation signal ignores consumed lines.
        let policy_dropped = {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            Self::retain_line_locked(&mut inner, line, total_lines)
        };
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.publish_change();
    }

    /// The locked half of [`push`](Self::push): supersede the published partial
    /// tail and apply the buffer policy to one completed line, in a **single**
    /// critical section. Returns whether the *policy* dropped the line (the
    /// caller bumps the `dropped` counter off the lock).
    ///
    /// Shared with [`drain_with_partial_tail`](Self::drain_with_partial_tail),
    /// which retains a salvaged tail through exactly this path so a recovered
    /// tail obeys the same ceilings, seal, and accounting as a completed line.
    fn retain_line_locked(inner: &mut Inner, line: String, total_lines: usize) -> bool {
        // A completed line entering the backlog **supersedes** whatever
        // unterminated tail was last published: that text is the head of *this*
        // line, so a timeout-salvage snapshot must not fold it in a second time.
        // Doing it here — under the same lock as the retention decision, rather
        // than leaving it to the pump's separate `set_partial_tail` call one
        // await later — is what makes "the line is in the backlog" and "the old
        // tail is no longer salvageable" one atomic step. Without it, a snapshot
        // landing in that gap took the stale tail *and* drained the full line,
        // repeating the prefix in the salvaged output.
        Self::supersede_partial_tail_locked(inner);
        // Whether the policy discarded this line.
        let mut policy_dropped = false;
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
                // turns `overflowed` into `ErrorReason::OutputTooLarge`.
                OverflowMode::Error => {
                    let over = match (inner.max_lines, inner.max_bytes) {
                        (None, None) => true,
                        (lines_cap, bytes_cap) => {
                            lines_cap.is_some_and(|n| total_lines > n)
                                || bytes_cap.is_some_and(|b| {
                                    // Raw pipe-byte total, plus the same
                                    // derived line-count bound `over_backlog`
                                    // uses. The latter keeps this cumulative
                                    // ceiling aligned with the retained
                                    // backlog's minimum per-line footprint.
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
                // "Head": keep a contiguous *prefix* of the output. Retain the
                // line only while the head is unsealed AND it fits both
                // ceilings; the first line that does not fit seals the head, so
                // every later line is dropped too. Without the seal an
                // over-budget long line would be dropped while a shorter line
                // after it still fit and got retained — the buffer would skip a
                // line and stop being a true prefix of the process's output.
                OverflowMode::DropNewest => {
                    if !inner.dropnewest_sealed && inner.would_fit(line.len()) {
                        inner.bytes += line.len();
                        inner.lines.push_back(line);
                    } else {
                        inner.dropnewest_sealed = true;
                        policy_dropped = true;
                    }
                }
            }
        }
        policy_dropped
    }

    /// The current retained-byte ceiling (`OutputBufferPolicy::max_bytes`). The
    /// pump re-reads it at every OS-read boundary because an adopting discard
    /// verb can lower an already-running stream sink's in-flight bound.
    pub(crate) fn byte_cap(&self) -> Option<usize> {
        self.inner.lock().expect("SharedLines poisoned").max_bytes
    }

    /// Record an over-cap line skipped by the pump: it is counted and never
    /// retained (it cannot fit the cap). Under [`OverflowMode::Error`]
    /// it trips the fail-loud ceiling; under the drop modes it sets the
    /// truncation signal. Raw bytes are accounted for at the read boundary in
    /// [`pump_lines_core`], before this decoded-line bookkeeping runs. A
    /// discarding sink instead only counts the line, skipping all retention and
    /// overflow bookkeeping like [`push`](Self::push). Mirrors the "cannot fit"
    /// accounting in [`push`](Self::push) for a line the pump never buffered (so
    /// it is also not delivered to the per-line handler or tee).
    pub(crate) fn record_oversized_line(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let policy_dropped = {
            let mut inner = self.inner.lock().expect("SharedLines poisoned");
            Self::record_oversized_locked(&mut inner)
        };
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.publish_change();
    }

    /// The locked half of
    /// [`record_oversized_line`](Self::record_oversized_line), mirroring
    /// [`retain_line_locked`](Self::retain_line_locked) for a line that is
    /// counted but never buffered. Returns whether the policy dropped it.
    fn record_oversized_locked(inner: &mut Inner) -> bool {
        // A skipped over-cap line still *completes* the line whose prefix was
        // last published, so the published tail stops being salvageable in the
        // same critical section — see `retain_line_locked`. Without it a
        // snapshot landing between this call and the pump's next
        // `set_partial_tail_state` would count the very same skipped line a
        // second time (inflating `count` and `dropped`).
        Self::supersede_partial_tail_locked(inner);
        // A discarded streaming consumer retains nothing and skips all
        // overflow bookkeeping, even for an over-cap line the pump skipped.
        if inner.discarding {
            return false;
        }
        match inner.mode {
            // The over-cap line trips the fail-loud ceiling.
            OverflowMode::Error => inner.overflowed = true,
            // An over-cap line can never fit the head, so it seals the
            // contiguous prefix: no later (shorter) line may be retained,
            // or the retained buffer would no longer be a prefix of the
            // output. Mirrors the seal in [`push`](Self::push).
            OverflowMode::DropNewest => inner.dropnewest_sealed = true,
            OverflowMode::DropOldest => {}
        }
        true
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
        self.publish_change();
    }

    /// Mark the buffer finished without a pump (e.g. a second `stdout_lines`
    /// call has no pipe left to drain), so a streaming consumer ends promptly.
    pub(crate) fn close_now(&self) {
        self.close();
    }

    /// Switch the sink to retain nothing, apply `in_flight_cap`, and drop its
    /// current backlog. A discard
    /// verb (`wait`/`profile`) calls this when it adopts a sink a **dropped**
    /// stream left populated under the caller's `OutputBufferPolicy`, so the
    /// still-running pump stops accumulating lines nobody will read. The line
    /// counter is untouched (it still reflects the total the pump has seen).
    /// Updating `max_bytes` is also load-bearing: the pump may have started under
    /// an unbounded streaming policy, so it must observe the discard verb's cap
    /// before decoding the next chunk of a newline-free flood.
    pub(crate) fn start_discarding(&self, in_flight_cap: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.discarding = true;
        inner.max_bytes = Some(in_flight_cap);
        inner.lines.clear();
        inner.bytes = 0;
        inner.partial_tail.clear();
        inner.partial_tail_pending = false;
        inner.partial_tail_oversized = false;
        inner.partial_tail_finalized = false;
    }

    /// Total lines seen by the pump (including dropped ones).
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Total raw bytes read by the pump (including dropped lines and line
    /// terminators), updated before decoding.
    pub(crate) fn seen_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .seen_bytes
    }

    /// Add bytes read directly from the pipe before decoding or line splitting.
    pub(crate) fn add_seen_bytes(&self, byte_count: usize) {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        inner.seen_bytes = inner.seen_bytes.saturating_add(byte_count);
        drop(inner);
        self.activity.record();
    }

    /// Publish the current **unterminated tail** — the decoded content the pump
    /// has read but not yet emitted as a complete line (`pending` in
    /// [`pump_lines_core`]) — so a
    /// [`wait_for_output`](crate::RunningProcess::wait_for_output) observer can
    /// match a prompt the child wrote without a trailing newline. The pump calls
    /// this once per read, after splitting out every complete line.
    ///
    /// Deliberately a **side channel**: it writes only [`Inner::partial_tail`]
    /// and never the `seen_bytes` ([[K-059]]) / `count` / `dropped` /
    /// `dropnewest_sealed` ([[K-054]]) state on the same lock, so surfacing the
    /// tail cannot re-run a retention/overflow decision or shift an accounting
    /// unit an existing consumer depends on. Notifies waiters **only on an actual
    /// change**, so a stream of complete lines (whose tail stays empty every
    /// read) does not spuriously wake — and a live prompt that stops changing
    /// (the child now blocking on input) is published exactly once and then left
    /// stable for the observer to match.
    pub(crate) fn set_partial_tail(&self, tail: &str) {
        self.set_partial_tail_state(tail, false);
    }

    /// Publish the current partial-line state, including an over-cap line that
    /// cannot be retained. The separate marker keeps timeout salvage from
    /// mistaking an empty, intentionally skipped tail for a line boundary.
    pub(crate) fn set_partial_tail_state(&self, tail: &str, oversized: bool) {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        let pending = oversized || !tail.is_empty();
        let changed = inner.partial_tail != tail
            || inner.partial_tail_pending != pending
            || inner.partial_tail_oversized != oversized;
        if changed {
            inner.partial_tail.clear();
            inner.partial_tail.push_str(tail);
            inner.partial_tail_pending = pending;
            inner.partial_tail_oversized = oversized;
        }
        // Publishing always clears the "already emitted" seal, even when the text
        // is byte-identical to what is already there. Once a completed line
        // supersedes the tail (see `retain_line_locked`), the pump's next publish
        // is a genuinely *new* live tail — and it may legitimately repeat the
        // previous text (a repeated line prefix, `a\na…`), which the change check
        // above cannot tell apart from "nothing happened". Leaving the seal on
        // would silently cost that new tail its timeout salvage.
        inner.partial_tail_finalized = false;
        if changed {
            drop(inner);
            self.publish_change();
            #[cfg(all(test, feature = "process-control"))]
            publish_partial_tail_for_test(tail);
        }
    }

    /// Mark the currently published partial tail as already emitted: its text
    /// stays visible to [`partial_tail_snapshot`](Self::partial_tail_snapshot)
    /// (so a final un-terminated prompt remains matchable by `wait_for_output`
    /// right up to close), but a timeout-salvage snapshot no longer folds it into
    /// the backlog.
    ///
    /// Called from every completed-line path
    /// ([`retain_line_locked`](Self::retain_line_locked) /
    /// [`record_oversized_locked`](Self::record_oversized_locked)) *inside* that
    /// path's own critical section, so "the line is recorded" and "the tail it
    /// completes is no longer salvageable" are one atomic step. That covers the
    /// pump's EOF finalizer too — finalizing means emitting the tail as a line,
    /// which takes exactly those paths.
    fn supersede_partial_tail_locked(inner: &mut Inner) {
        if inner.partial_tail_pending {
            inner.partial_tail_finalized = true;
        }
    }

    /// Fold a still-pending partial tail into the backlog and take every retained
    /// line — the timeout-salvage snapshot's **single** sink operation
    /// (`LineCapture::snapshot` / `RawCapture::snapshot`).
    ///
    /// Salvage runs while the last stage's pump may still be alive: dropping the
    /// capture task only *requests* an abort, and the pump stops at its next
    /// await. So this cannot be take-tail → push → drain as three separate lock
    /// acquisitions. In the gap between the take and the drain a live pump can
    /// push the very line the taken tail is the prefix of, and the drain then
    /// returns both — the tail repeated ahead of its own completed line. Holding
    /// the lock across the whole fold closes that direction;
    /// [`retain_line_locked`](Self::retain_line_locked) superseding the published
    /// tail closes the other (a push that already happened leaves nothing stale to
    /// salvage). What remains is the accepted best-effort degradation of a
    /// torn-down capture: a push that lands *after* this returns is lost, never
    /// duplicated.
    ///
    /// The recovered tail is retained through the same
    /// [`retain_line_locked`](Self::retain_line_locked) /
    /// [`record_oversized_locked`](Self::record_oversized_locked) paths a
    /// completed line takes, so it obeys the buffer policy's ceilings and seal and
    /// updates `count`/`dropped` identically. Taking it is idempotent (a second
    /// snapshot recovers nothing), and `seen_bytes` stays untouched because the
    /// pump already accounted for the raw bytes at its read boundary.
    ///
    /// `shape` — the stream's capture shaping
    /// ([`StreamConfig::shape_capture_line`]) — runs with the lock held, so the
    /// tail is shaped exactly like a completed line yet cannot be overtaken
    /// between shaping and retention. It is panic-isolated (see
    /// [`apply_capture_policy`]) and has no route back into this sink, so the
    /// hold is bounded by one line's redaction.
    pub(crate) fn drain_with_partial_tail(
        &self,
        shape: impl FnOnce(String) -> String,
    ) -> Vec<String> {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        let mut salvaged = false;
        let mut policy_dropped = false;
        if inner.partial_tail_pending && !inner.partial_tail_finalized {
            inner.partial_tail_pending = false;
            inner.partial_tail_finalized = true;
            let oversized = inner.partial_tail_oversized;
            let tail = std::mem::take(&mut inner.partial_tail);
            let total_lines = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            salvaged = true;
            policy_dropped = if oversized {
                Self::record_oversized_locked(&mut inner)
            } else {
                Self::retain_line_locked(&mut inner, shape(tail), total_lines)
            };
        }
        let lines = Self::drain_locked(&mut inner);
        drop(inner);
        if policy_dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        if salvaged {
            self.publish_change();
        }
        lines
    }

    /// Whether this sink's state lock is currently held by *someone else* — a
    /// test-only probe. It lets a racing thread prove, without any wall-clock
    /// wait, that a critical section it was released from really is still holding
    /// the lock (see `the_salvage_fold_and_drain_are_one_critical_section`).
    #[cfg(test)]
    pub(crate) fn is_locked_by_another_thread(&self) -> bool {
        matches!(
            self.inner.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        )
    }

    /// Snapshot the current unterminated tail (cloned so the predicate runs off
    /// the lock, never blocking the pump or poisoning `Inner` on a panicking
    /// user predicate) and whether the pump has closed. `None` tail means there
    /// is no live partial line right now (the last content ended on a line
    /// boundary). Backs [`wait_for_output`](crate::RunningProcess::wait_for_output).
    pub(crate) fn partial_tail_snapshot(&self) -> (Option<String>, bool, ChangeToken) {
        let inner = self.inner.lock().expect("SharedLines poisoned");
        let tail = if inner.partial_tail.is_empty() {
            None
        } else {
            Some(inner.partial_tail.clone())
        };
        let generation = ChangeToken(self.generation.load(Ordering::Acquire));
        (tail, inner.closed, generation)
    }

    /// Lines discarded by the buffer policy (DropOldest/DropNewest/Error), not
    /// counting lines a streaming consumer popped. `> 0` iff output was actually
    /// truncated by the policy.
    pub(crate) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Record the first OS read error the pump hit while draining the stream —
    /// the incomplete-capture signal a consuming finisher turns into
    /// [`ErrorReason::Io`](crate::ErrorReason::Io). Only the *first* error is kept (a later
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
    /// [`ErrorReason::Io`](crate::ErrorReason::Io); `None` means the stream drained to a clean
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
    /// pump has finished). A capture salvaging a still-live pump's tail uses
    /// [`drain_with_partial_tail`](Self::drain_with_partial_tail) instead, which
    /// folds the tail in under this same single lock.
    pub(crate) fn drain(&self) -> Vec<String> {
        let mut inner = self.inner.lock().expect("SharedLines poisoned");
        Self::drain_locked(&mut inner)
    }

    /// Clone the retained backlog plus a still-live, not-yet-superseded partial
    /// tail without consuming or finalizing either. Pipeline teardown uses this
    /// immediately before a fallback kill: that kill can wake the stage finisher
    /// and let it drain the live sink before the chain-level error is assembled.
    ///
    /// The clone is folded under the sink's single critical section through the
    /// same retention policy as destructive salvage. The live `Inner`, counters,
    /// tail seal, and one-consumer backlog remain untouched. A later completed
    /// line may supersede the cloned tail, but the checkpoint is used only for an
    /// immediate failed-confirmation return and is never joined to that later
    /// line, so it cannot duplicate the prefix.
    pub(crate) fn retained_snapshot(&self, shape: impl FnOnce(String) -> String) -> Vec<String> {
        let inner = self.inner.lock().expect("SharedLines poisoned");
        let mut snapshot = inner.clone();
        if snapshot.partial_tail_pending && !snapshot.partial_tail_finalized {
            let oversized = snapshot.partial_tail_oversized;
            let tail = std::mem::take(&mut snapshot.partial_tail);
            let total_lines = self.count.load(Ordering::Relaxed).saturating_add(1);
            if oversized {
                Self::record_oversized_locked(&mut snapshot);
            } else {
                Self::retain_line_locked(&mut snapshot, shape(tail), total_lines);
            }
        }
        snapshot.lines.into_iter().collect()
    }

    /// The locked half of [`drain`](Self::drain).
    fn drain_locked(inner: &mut Inner) -> Vec<String> {
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
            let generation = ChangeToken(self.generation.load(Ordering::Acquire));
            Popped::Empty(generation)
        }
    }

    /// Publish one completed state transition to every currently parked
    /// observer. The generation is advanced before broadcasting so an observer
    /// that has not registered yet still detects the transition.
    fn publish_change(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Await a buffer change after `observed`. Owns the `Arc` so the returned
    /// future is `'static` and can be boxed by the `Stream` impl.
    ///
    /// Registering before the acquire load closes the other half of the race: a
    /// publication either advances the generation that this check sees or wakes
    /// the already-enabled waiter. `notify_waiters` then wakes every observer of
    /// the same sink rather than arbitrarily selecting one.
    pub(crate) async fn changed(self: Arc<Self>, observed: ChangeToken) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.generation.load(Ordering::Acquire) != observed.0 {
            return;
        }
        notified.await;
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

/// A per-stream async **raw byte** tee sink: each chunk is written to it exactly
/// as read from the child's pipe — *before* decoding and *before* line splitting,
/// with no CRLF normalization, no terminator handling, and no per-line framing —
/// [`Command::stdout_raw_tee`](crate::Command::stdout_raw_tee) /
/// [`stderr_raw_tee`](crate::Command::stderr_raw_tee). Structurally the same
/// `Arc<Mutex<…>>` boxed writer as [`TeeSink`]; a distinct alias because the
/// contract differs — [`TeeSink`] receives *decoded lines* (each plus a `\n`),
/// this one receives the child's *raw bytes* verbatim. The write is awaited on
/// the pump task, so a slow sink applies the same backpressure as the line tee
/// (the pump slows → the OS pipe fills → the child blocks on its next write)
/// rather than buffering without bound.
pub(crate) type RawTeeSink = TeeSink;

/// The per-stream pump knobs that differ between stdout and stderr — the decode
/// [`encoding`](Self::encoding), an optional per-line [`handler`](Self::handler),
/// an optional decoded-line [`tee`](Self::tee) sink, an optional
/// [`raw_tee`](Self::raw_tee) byte sink, the line
/// [`terminator`](Self::terminator) mode, plus the optional
/// [`buffer_policy`](Self::buffer_policy) redaction seam and which
/// [`stream`](Self::stream) this config drives — carried as one value.
///
/// Held one-per-stream by [`Command`](crate::Command),
/// [`Spawned`](crate::running::Spawned), and
/// [`RunningProcess`](crate::RunningProcess), and handed to [`pump_lines_core`]
/// whole. Folding the knobs into a single struct means a new per-stream knob
/// is threaded through *this* type instead of duplicated across every field pair
/// and pump call site. Cheap to clone: `handler`/`tee`/`raw_tee`/`buffer_policy`
/// are `Arc`s and the rest is `Copy`.
#[derive(Clone)]
pub(crate) struct StreamConfig {
    /// Decode bytes with this encoding (default UTF-8).
    pub encoding: &'static Encoding,
    /// Optional per-line callback invoked on the pump task.
    pub handler: Option<LineHandler>,
    /// Optional async sink each decoded line is also written to.
    pub tee: Option<TeeSink>,
    /// Optional async sink each raw chunk is written to, verbatim and *before*
    /// decoding — strictly additive to the decoded-line path (see
    /// [`RawTeeSink`] and [`pump_lines_core`]).
    pub raw_tee: Option<RawTeeSink>,
    /// Where the pump splits the stream into lines (default `\n`-only).
    pub terminator: LineTerminator,
    /// Optional consumer [`CapturePolicy`](crate::CapturePolicy): a redaction/
    /// transform seam applied to each decoded line **just before it is
    /// retained** in the backlog (see [`pump_lines_core`]). `None` (the default)
    /// leaves capture verbatim. A whole-command knob shared by both streams;
    /// [`stream`](Self::stream) tells it which one it is looking at.
    pub buffer_policy: Option<SharedCapturePolicy>,
    /// Opt-in VT/ANSI **sanitizer** for the capture backlog
    /// ([`Command::sanitize_vt`](crate::Command::sanitize_vt)): when `true`, each
    /// decoded line is stripped of escape sequences and lone control codes
    /// ([`strip_vt`]) **just before** the [`buffer_policy`](Self::buffer_policy)
    /// and the backlog — after the raw-observing handler/tee, so it shapes only
    /// what is retained (the same boundary as the capture policy). `false` (the
    /// default) leaves capture verbatim.
    pub sanitize_vt: bool,
    /// Which stream this config drives — handed to
    /// [`CapturePolicy::on_capture`](crate::CapturePolicy::on_capture) so one
    /// policy can distinguish stdout from stderr.
    pub stream: OutputStream,
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
            raw_tee: None,
            terminator: LineTerminator::Newline,
            buffer_policy: None,
            sanitize_vt: false,
            stream: OutputStream::Stdout,
        }
    }

    /// This config with the decode `encoding` replaced. Used by the scripted
    /// feeder, which forces UTF-8 (it writes the canned `String`'s UTF-8 bytes)
    /// while keeping the command's handler/tee/raw_tee/terminator.
    pub(crate) fn with_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Apply the same capture-only shaping used by a completed line. Timeout
    /// salvage calls this after recovering the pump's decoded pending tail;
    /// handlers and tees remain observation-only and are not replayed.
    pub(crate) fn shape_capture_line(&self, line: String) -> String {
        shape_capture_line(&self.buffer_policy, self.sanitize_vt, self.stream, line)
    }
}

fn shape_capture_line(
    buffer_policy: &Option<SharedCapturePolicy>,
    sanitize_vt: bool,
    stream: OutputStream,
    line: String,
) -> String {
    apply_capture_policy(buffer_policy, stream, apply_vt_sanitize(sanitize_vt, line))
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
            ..StreamConfig::new()
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
            terminator,
            ..StreamConfig::new()
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
/// surface the incomplete capture as [`ErrorReason::Io`](crate::ErrorReason::Io) rather than
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
/// **Raw byte tee (`raw_tee`):** strictly additive to everything above. When
/// set, each chunk is written to it *exactly as read from the pipe* — the same
/// `&chunk[..n]` the decoder is about to consume — **before** `decode_to_string`
/// and independent of line splitting, the buffer policy, and the over-cap skip.
/// So it observes every byte the child wrote, byte-for-byte and in FIFO order:
/// no lossy decode (non-UTF-8 bytes survive), no CRLF normalization, no
/// fabricated trailing newline, and no loss of a line the policy drops (an
/// over-cap line skipped from every decoded sink still reaches the raw tee
/// whole). A chunk with no line terminator reaches the raw tee immediately, not
/// held until EOF. The write is awaited on the pump task — the same backpressure
/// point as the line tee, so a slow raw sink can't grow memory without bound —
/// flushed once at stream end, and a write error disables it (with a `tracing`
/// warn) exactly like the line tee, leaving the decoded path untouched.
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
        raw_tee,
        terminator,
        buffer_policy,
        sanitize_vt,
        stream,
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
    let mut raw_tee = raw_tee;

    // Write one raw chunk to the byte tee (`raw_tee`), disabling it on a write
    // error the same way `emit` disables the line tee. Awaiting the write here is
    // the backpressure point: a slow raw sink slows the pump, the OS pipe fills,
    // and the child blocks on its next write — so a lagging consumer can never
    // grow unbounded in-flight memory. Independent of decoding and line framing;
    // the caller feeds it the exact `&chunk[..n]` just read, before the decoder
    // sees it.
    async fn tee_raw_chunk(raw_tee: &mut Option<RawTeeSink>, chunk: &[u8]) {
        if let Some(t) = raw_tee {
            use tokio::io::AsyncWriteExt;
            let mut w = t.lock().await;
            let wrote = w.write_all(chunk).await;
            drop(w);
            if wrote.is_err() {
                *raw_tee = None;
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    "raw tee writer errored; disabled for the rest of the run"
                );
            }
        }
    }

    // Emit one decoded line: run the (panic-isolated) handler, await the tee
    // (disabling it on a write error), then shape the backlog copy — the opt-in
    // VT sanitizer first, then the optional capture policy — and buffer the
    // result. The handler and tee — pure observation seams — see the *raw*
    // decoded line; the sanitizer and capture policy run last, in front of the
    // backlog only, so hygiene/redaction shape what is retained without changing
    // what those sinks observe. The sanitizer runs *before* the policy so a
    // secret-scrubbing policy matches on already-cleaned text.
    async fn emit(
        handler: &mut Option<LineHandler>,
        tee: &mut Option<TeeSink>,
        buffer_policy: &Option<SharedCapturePolicy>,
        sanitize_vt: bool,
        stream: OutputStream,
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
        sink.push(shape_capture_line(buffer_policy, sanitize_vt, stream, line));
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
    // rather than discarded in case it is the CR of a CRLF whose `\n` lands in
    // the next chunk. Deferring keeps terminator classification identical
    // regardless of read boundary; a `\r` that turns out to be mid-line content
    // is discarded by a subsequent split, skip pass, or the EOF finalizer.
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

    // The single bulk memmove for a chunk: drop exactly the consumed prefix,
    // leaving `pending` holding only the unconsumed remainder for the next
    // read to append to (same invariant the per-line drains used to maintain
    // one line at a time). `start == 0` (nothing consumed this read) is
    // guarded out purely as a micro-optimization: `String::drain(..0)` is
    // already a no-op (an empty range, always a valid char boundary), so a
    // `start > 0` mutated to `start >= 0` behaves identically — a `#[mutants::
    // skip]`-worthy equivalent mutant, hence skipped here rather than chased
    // with an unkillable test. (`#[mutants::skip]` only attaches to functions,
    // not to an inline `if`, hence this extraction.)
    #[mutants::skip]
    fn drain_consumed_prefix(pending: &mut String, start: usize) {
        if start > 0 {
            pending.drain(..start);
        }
    }

    // The OS read size.
    const CHUNK: usize = 8192;
    let mut decoder = encoding.new_decoder_with_bom_removal();
    let mut pending = String::new(); // decoded text not yet split into a line
    let mut chunk = [0u8; CHUNK];
    // True while skipping an over-cap line. Decoded content is discarded until
    // its terminator; raw bytes are accounted for at the read boundary below.
    let mut oversized = false;
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
        // Feed the raw byte tee the exact bytes just read, *before* decoding and
        // any line framing — so it sees the child's output verbatim (non-UTF-8
        // bytes, CRLF, an unterminated tail, an over-cap line the policy drops).
        // A clean EOF / read error / broken pipe read yields `n == 0`, so nothing
        // is written for them; only real data chunks reach the sink.
        if n > 0 {
            // Account at the read boundary: tee backpressure or lossy decoding
            // must not delay or change the raw-byte counter.
            sink.0.add_seen_bytes(n);
            tee_raw_chunk(&mut raw_tee, &chunk[..n]).await;
        }
        // Reserve the decoder's worst-case output up front so `decode_to_string`
        // (which uses the `String`'s spare capacity as its output limit, never
        // reallocating) consumes the whole chunk in one call.
        if let Some(need) = decoder.max_utf8_buffer_length(n) {
            pending.reserve(need);
        }
        let _ = decoder.decode_to_string(&chunk[..n], &mut pending, last);
        #[cfg(test)]
        observe_pending(&pending);

        // Re-read at each OS-read boundary rather than pinning the sink's launch
        // policy. `wait`/`drain`/`profile` may adopt a dropped stream and lower
        // an originally unbounded sink to their discard cap while this task is
        // alive. The bound is `cap + CHUNK` (checked after a whole read decodes),
        // not exactly `cap`.
        let cap = sink.0.byte_cap();

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
            if oversized {
                // Skipping an over-cap line: discard through its terminator.
                match next_terminator(sub, terminator, eof) {
                    Some(term) => {
                        start += term.resume;
                        oversized = false;
                        sink.0.record_oversized_line();
                    }
                    None => {
                        #[cfg(test)]
                        observe_skip_call();
                        let advance = skip_over_cap_len(sub);
                        start += advance;
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
                            emit(
                                &mut handler,
                                &mut tee,
                                &buffer_policy,
                                sanitize_vt,
                                stream,
                                &sink.0,
                                line,
                            )
                            .await;
                        } else {
                            // Over-cap line, terminator already here: drop it whole
                            // and record the skipped line once.
                            start += term.resume;
                            sink.0.record_oversized_line();
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
                        #[cfg(test)]
                        observe_guard_entry();
                        #[cfg(test)]
                        observe_skip_call();
                        let advance = skip_over_cap_len(sub);
                        start += advance;
                        oversized = true;
                        break;
                    }
                    None => break,
                }
            }
        }
        drain_consumed_prefix(&mut pending, start);

        // Publish the current unterminated tail for a `wait_for_output` observer,
        // AFTER every complete line for this read has already been emitted. This
        // is a pure side view of the pump's own `pending` remainder — it moves no
        // `seen_bytes`/`count`/`dropnewest_sealed` state (K-054/K-059), so it can
        // never re-decide retention. While skipping an over-cap line the tail is
        // being discarded (never retained, handed to no sink), so nothing is
        // matchable — publish an empty tail to match that bypass. At EOF this
        // still runs (the final `pending` is published) and is then left in place:
        // the finalizer below turns that content into a complete line, but the
        // last published tail stays visible so a final un-terminated prompt is
        // matchable right up to and including close.
        if oversized {
            sink.0.set_partial_tail_state("", true);
        } else {
            sink.0.set_partial_tail(&pending);
        }

        if eof {
            // Finalize a final line (or an un-terminated over-cap tail). At EOF the
            // split loop above ran with `eof = true`, so in `\r`-aware mode a
            // trailing `\r` was already resolved as a frame terminator; whatever
            // remains in `pending` here is pure content with no terminator.
            //
            // Turning the tail into a completed line (`emit`'s push, or
            // `record_oversized_line` for one the cap rejects) is itself what marks
            // the published tail as already emitted — `SharedLines` seals it inside
            // the very critical section that records the line
            // (`supersede_partial_tail_locked`). So no separate finalize call is
            // needed here, and a timeout snapshot racing this finalizer can never
            // append the same text twice: it either sees the tail still live (and
            // salvages it) or sees the completed line (and the tail sealed).
            if oversized {
                // An un-terminated tail: `pending` is all content (in `Newline`
                // mode a trailing `\r` is content; in `\r`-aware mode none remains).
                pending.clear();
                sink.0.record_oversized_line();
            } else if !pending.is_empty() {
                // An un-terminated final line: `pending` is all content (in
                // `Newline` mode a trailing `\r` is content). Re-apply the byte cap:
                // the enter-skip deferred a lone trailing `\r` in case a `\n`
                // followed, but at EOF none does, so an over-cap tail must be
                // dropped (counted, never handed to the handler/tee) like any
                // over-cap line — not emitted.
                let line = std::mem::take(&mut pending);
                if cap.is_some_and(|c| line.len() > c) {
                    sink.0.record_oversized_line();
                } else {
                    emit(
                        &mut handler,
                        &mut tee,
                        &buffer_policy,
                        sanitize_vt,
                        stream,
                        &sink.0,
                        line,
                    )
                    .await;
                }
            }
            // Flush the tee once at stream end (best-effort).
            if let Some(t) = &tee {
                use tokio::io::AsyncWriteExt;
                let _ = t.lock().await.flush().await;
            }
            // Flush the raw byte tee once at stream end too (best-effort), so a
            // buffering raw sink (e.g. a `BufWriter`) commits its tail.
            if let Some(t) = &raw_tee {
                use tokio::io::AsyncWriteExt;
                let _ = t.lock().await.flush().await;
            }
            if let Some(e) = read_err {
                // Record the OS read error AFTER flushing the partial tail (so the
                // already-decoded prefix is still delivered) and before the sink
                // closes, so a finisher that joins this pump surfaces the
                // incomplete capture as `ErrorReason::Io`.
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
            ..StreamConfig::new()
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
    use crate::buffer::{CapturePolicy, OutputBufferPolicy};

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
    async fn seen_bytes_counts_raw_input_before_decode_and_overflow() {
        let policy = OutputBufferPolicy::bounded(1)
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        let raw = b"\xff\xfe\nabcdef\nok\n";

        pump_lines(raw.as_slice(), encoding_rs::UTF_8, None, sink.clone()).await;

        assert_eq!(
            sink.seen_bytes(),
            raw.len(),
            "invalid bytes, terminators, and dropped/oversized lines are all counted"
        );
        assert!(sink.dropped() >= 1, "the oversized line is dropped");
    }

    /// The partial-tail side view (backing `wait_for_output`) exposes the
    /// un-terminated remainder — the content after the last line terminator —
    /// while a stream that ends exactly on a line boundary leaves no tail.
    #[tokio::test]
    async fn partial_tail_exposes_the_unterminated_remainder() {
        // A complete line, then an un-terminated prompt.
        let with_tail = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(
            &b"loading\nPassword: "[..],
            encoding_rs::UTF_8,
            None,
            with_tail.clone(),
        )
        .await;
        let (tail, closed, _) = with_tail.partial_tail_snapshot();
        assert_eq!(tail.as_deref(), Some("Password: "));
        assert!(closed, "the pump closed at EOF");
        // The tail is a side view: at EOF the same un-terminated content is ALSO
        // finalized into the backlog (the pump's existing final-line behavior), so
        // the backlog carries both lines — the side view never replaces it.
        assert_eq!(with_tail.drain(), vec!["loading", "Password: "]);

        // A stream ending on a terminator has no live tail.
        let no_tail = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines(&b"done\n"[..], encoding_rs::UTF_8, None, no_tail.clone()).await;
        let (tail, _, _) = no_tail.partial_tail_snapshot();
        assert_eq!(
            tail, None,
            "content ending on a newline leaves no partial tail"
        );
    }

    /// Publishing the tail is a pure side channel: it must not perturb the
    /// `seen_bytes` (K-059), `count`, or `dropped` accounting an existing consumer
    /// relies on. Same input, checked with the tail machinery live.
    #[tokio::test]
    async fn partial_tail_does_not_disturb_byte_or_line_accounting() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let raw = b"one\ntwo\ntail-without-newline";
        pump_lines(raw.as_slice(), encoding_rs::UTF_8, None, sink.clone()).await;

        assert_eq!(sink.seen_bytes(), raw.len(), "raw byte count is unchanged");
        assert_eq!(sink.count(), 3, "two lines plus the finalized tail line");
        assert_eq!(sink.dropped(), 0, "nothing was dropped");
        let (tail, _, _) = sink.partial_tail_snapshot();
        assert_eq!(tail.as_deref(), Some("tail-without-newline"));
    }

    /// `notify_waiters` deliberately stores no permit when nobody is registered.
    /// A consumer that inspected state just before a publication must therefore
    /// complete from the generation check even if it constructs and polls its
    /// wait future only after the broadcast has already happened.
    #[test]
    fn changed_detects_a_publication_before_waiter_registration() {
        use std::future::Future as _;

        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let (_, _, observed) = sink.partial_tail_snapshot();
        sink.set_partial_tail("prompt> ");

        let mut changed = Box::pin(sink.clone().changed(observed));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            changed.as_mut().poll(&mut cx).is_ready(),
            "the generation advance must cover the pre-registration window"
        );
    }

    /// A completed line and the pump's replacement tail are published under two
    /// *separate* locks, an await apart, so a timeout-salvage snapshot can land
    /// between them. Pushing the line seals the tail it completed inside the
    /// push's own critical section, so a snapshot in that window sees "the line,
    /// no live tail" instead of the stale prefix on top of its own finished line.
    #[test]
    fn a_pushed_line_seals_the_tail_it_completed() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        // The previous read published the un-terminated prefix…
        sink.set_partial_tail("ab");
        // …and this read completed it, pushing the whole line. The replacement
        // tail has not been published yet.
        sink.push("abcd".to_owned());

        let salvaged = sink.drain_with_partial_tail(|tail| tail);

        assert_eq!(
            salvaged,
            vec!["abcd".to_owned()],
            "the salvage must not repeat the prefix of the line it drained"
        );
        assert_eq!(sink.count(), 1, "the sealed tail is not counted as a line");
        assert_eq!(
            sink.partial_tail_snapshot().0.as_deref(),
            Some("ab"),
            "the seal blocks salvage only; the side view stays readable"
        );
    }

    /// The seal must not outlive the tail it sealed. After a completed line the
    /// pump publishes a genuinely *new* live tail, and it may repeat the sealed
    /// text verbatim (`ab` completing `abab`, then `ab` again) — which the
    /// publish's own change check cannot tell from "nothing happened", so the
    /// publish clears the seal unconditionally.
    #[test]
    fn a_republished_identical_tail_is_salvageable_again() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_partial_tail("ab");
        sink.push("abab".to_owned()); // seals the published tail
        sink.set_partial_tail("ab"); // the next read's tail, same text, new line

        assert_eq!(
            sink.drain_with_partial_tail(|tail| tail),
            vec!["abab".to_owned(), "ab".to_owned()],
        );
    }

    /// The failed-confirmation checkpoint is a side view, not a second
    /// consumer: it must shape and include the live tail while leaving backlog,
    /// tail seal, and accounting untouched for the eventual consuming salvage.
    #[test]
    fn retained_snapshot_includes_a_shaped_tail_without_consuming_it() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.push("complete".to_owned());
        sink.set_partial_tail("partial");

        let first = sink.retained_snapshot(|tail| format!("shaped:{tail}"));
        let second = sink.retained_snapshot(|tail| format!("shaped:{tail}"));
        assert_eq!(first, vec!["complete", "shaped:partial"]);
        assert_eq!(second, first, "a checkpoint does not consume the tail");
        assert_eq!(sink.count(), 1, "checkpointing does not count a line");
        assert_eq!(
            sink.dropped(),
            0,
            "checkpointing does not alter policy state"
        );

        assert_eq!(
            sink.drain_with_partial_tail(|tail| format!("shaped:{tail}")),
            first,
            "the sole consuming salvage still owns backlog and tail"
        );
        assert_eq!(sink.count(), 2, "only consuming salvage counts the tail");
    }

    /// The same seal covers an over-cap line the pump *skipped*: without it, a
    /// snapshot between `record_oversized_line` and the pump's next publish would
    /// count — and charge as dropped — the very same skipped line a second time.
    #[test]
    fn a_skipped_over_cap_line_is_not_counted_twice_by_salvage() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(2));
        // Mid-skip the tail is published as "empty but pending" (oversized), so a
        // salvage can still charge the unfinished over-cap line as dropped.
        sink.set_partial_tail_state("", true);
        // Its terminator arrived: counted and charged exactly once, here.
        sink.record_oversized_line();

        assert!(sink.drain_with_partial_tail(|tail| tail).is_empty());
        assert_eq!(sink.count(), 1, "one skipped line, counted once");
        assert_eq!(sink.dropped(), 1, "…and charged as dropped once");
    }

    /// Folding the pending tail in and draining the backlog is **one** critical
    /// section. A pump that concurrently pushes the tail's own completed line is
    /// therefore ordered either wholly before the fold (which then finds the tail
    /// sealed and salvages nothing) or wholly after the drain (its line stays in
    /// the sink, lost from the salvage — the accepted best-effort degradation of a
    /// torn-down capture). It can never land in between, where a split
    /// take-then-drain returned both and repeated the prefix.
    ///
    /// Deterministic without a wall-clock wait (K-017): the racing thread is
    /// released from *inside* the fold and hands back a lock observation the fold
    /// blocks on, so "the sink is locked while the racing push starts" is a
    /// checked fact rather than a scheduling hope — and the push is provably
    /// ordered after the drain. Split the fold and the drain again and the
    /// observation comes back `false`, failing the test outright.
    #[test]
    fn the_salvage_fold_and_drain_are_one_critical_section() {
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_partial_tail("ab");
        let (release, released) = std::sync::mpsc::channel::<()>();
        let (observed, observation) = std::sync::mpsc::channel::<bool>();
        let pump = {
            let sink = sink.clone();
            std::thread::spawn(move || {
                released.recv().expect("released from inside the fold");
                // Report the fold's lock as seen from the outside (a non-blocking
                // probe), then race it: this push blocks until the fold ends.
                observed
                    .send(sink.is_locked_by_another_thread())
                    .expect("the fold is waiting for the observation");
                sink.push("abcd".to_owned());
            })
        };

        let salvaged = sink.drain_with_partial_tail(|tail| {
            release.send(()).expect("the racing pump thread is alive");
            assert!(
                observation
                    .recv()
                    .expect("the racing pump thread reported its observation"),
                "the fold must still hold the sink lock while a racing push starts"
            );
            tail
        });
        pump.join().expect("racing pump thread");

        assert_eq!(salvaged, vec!["ab".to_owned()], "the tail, exactly once");
        assert_eq!(
            sink.drain(),
            vec!["abcd".to_owned()],
            "the racing line landed after the drain: lost from the salvage, never repeated in it"
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
        sink.start_discarding(3);
        assert!(sink.drain().is_empty(), "the buffered backlog is dropped");
        sink.push("c".into());
        assert!(
            sink.drain().is_empty(),
            "lines pushed after discarding are not retained"
        );
        assert_eq!(sink.count(), 3, "every line is still counted");
    }

    #[tokio::test]
    async fn adopted_unbounded_sink_observes_the_discard_in_flight_cap() {
        use tokio::io::AsyncWriteExt;

        // Model `stdout_lines()` -> drop -> `wait()`: the pump starts with the
        // default unbounded streaming sink, then a discard verb adopts that same
        // live sink while a newline-free stream is still arriving.
        let (mut writer, reader) = tokio::io::duplex(16 * 1024);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let probe = Arc::new(PumpTestProbe::default());
        let pump = PUMP_TEST_PROBE.scope(
            probe.clone(),
            pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()),
        );
        let feed = async {
            writer
                .write_all(&vec![b'x'; 4096])
                .await
                .expect("initial unbounded stream chunk");
            tokio::task::yield_now().await;

            sink.start_discarding(64);
            for _ in 0..256 {
                writer
                    .write_all(&vec![b'x'; 4096])
                    .await
                    .expect("newline-free flood chunk");
            }
            writer.shutdown().await.expect("close duplex writer");
        };

        tokio::join!(pump, feed);

        assert!(
            probe.max_pending_bytes() <= 16 * 1024,
            "the adopted sink must switch from unbounded assembly to cap + one read chunk (high-water: {} bytes)",
            probe.max_pending_bytes()
        );
        assert!(
            probe.guard_entries() >= 1,
            "the lowered discard cap must engage the over-cap guard"
        );
        assert!(sink.drain().is_empty(), "the adopted sink retains nothing");
        assert_eq!(
            sink.dropped(),
            0,
            "discarding skips user-policy truncation accounting"
        );
    }

    #[test]
    fn discarding_oversized_line_skips_overflow_bookkeeping() {
        let policy = OutputBufferPolicy::fail_loud(10).with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        sink.start_discarding(3);
        sink.record_oversized_line();

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

    // --- CapturePolicy: redaction-at-capture seam (T-186) --------------------
    //
    // The policy runs *in front of* the backlog: it shapes what is retained
    // (and thus what `drain`/streaming/`output_string` see), unlike the
    // observing per-line handler/tee which run alongside capture. These tests
    // pin that placement, the stream tag, the empty-line elision, the
    // fail-closed panic contract, and that it composes with the built-in
    // overflow modes rather than forking a second retention path.

    /// A policy that rewrites every `secret` occurrence, borrowing (no alloc)
    /// when a line has none. Records nothing — used to assert retained content.
    struct RedactSecrets;
    impl CapturePolicy for RedactSecrets {
        fn name(&self) -> &str {
            "redact-secrets"
        }
        fn on_capture<'a>(&self, _stream: OutputStream, line: &'a str) -> Cow<'a, str> {
            if line.contains("secret") {
                Cow::Owned(line.replace("secret", "[REDACTED]"))
            } else {
                Cow::Borrowed(line)
            }
        }
    }

    fn policy_config(policy: SharedCapturePolicy, stream: OutputStream) -> StreamConfig {
        StreamConfig {
            buffer_policy: Some(policy),
            stream,
            ..StreamConfig::new()
        }
    }

    #[tokio::test]
    async fn capture_policy_shapes_what_is_retained_not_just_observed() {
        // The redacting policy changes what actually lands in the backlog: the
        // retained lines carry `[REDACTED]`, not the raw `secret`, proving the
        // seam runs before retention (what `output_string`/`ProcessResult` and
        // the streaming verbs read).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"keep me\ntoken=secret-abc\nsecret and secret\nplain\n"[..],
            policy_config(Arc::new(RedactSecrets), OutputStream::Stdout),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 4, "every line is still counted");
        assert_eq!(
            sink.drain(),
            vec![
                "keep me",
                "token=[REDACTED]-abc",
                "[REDACTED] and [REDACTED]",
                "plain",
            ],
            "the backlog holds the redacted lines, not the raw ones"
        );
    }

    #[tokio::test]
    async fn capture_policy_shapes_backlog_only_handler_and_tee_see_raw() {
        // The boundary the reviewer must trust: the policy scopes to the backlog.
        // The observing handler and decoded tee — independent seams — still see
        // the RAW line; only the retained backlog is redacted. A consumer who
        // also tees to a log is responsible for that sink (documented contract).
        let teed = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"token=secret-xyz\n"[..],
            StreamConfig {
                handler: Some(handler),
                tee: Some(tee_of(VecSink(teed.clone()))),
                buffer_policy: Some(Arc::new(RedactSecrets)),
                ..StreamConfig::new()
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["token=[REDACTED]-xyz"],
            "the backlog is redacted"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["token=secret-xyz"],
            "the handler observes the raw line (separate, un-redacted seam)"
        );
        assert_eq!(
            String::from_utf8(teed.lock().unwrap().clone()).unwrap(),
            "token=secret-xyz\n",
            "the decoded tee observes the raw line too"
        );
    }

    #[tokio::test]
    async fn capture_policy_is_handed_the_stream_identity() {
        // One policy, applied to both streams, can tell them apart: it receives
        // the `OutputStream` the config drives.
        let seen = Arc::new(Mutex::new(Vec::new()));
        struct RecordStream(Arc<Mutex<Vec<OutputStream>>>);
        impl CapturePolicy for RecordStream {
            fn name(&self) -> &str {
                "record-stream"
            }
            fn on_capture<'a>(&self, stream: OutputStream, line: &'a str) -> Cow<'a, str> {
                self.0.lock().unwrap().push(stream);
                Cow::Borrowed(line)
            }
        }
        let policy: SharedCapturePolicy = Arc::new(RecordStream(seen.clone()));

        let out_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"a\n"[..],
            policy_config(policy.clone(), OutputStream::Stdout),
            out_sink.clone(),
        )
        .await;
        let err_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"b\n"[..],
            policy_config(policy, OutputStream::Stderr),
            err_sink.clone(),
        )
        .await;

        assert_eq!(
            *seen.lock().unwrap(),
            vec![OutputStream::Stdout, OutputStream::Stderr],
            "the policy saw each stream's identity"
        );
        // Borrowed lines are retained verbatim (no re-allocation, same content).
        assert_eq!(out_sink.drain(), vec!["a"]);
        assert_eq!(err_sink.drain(), vec!["b"]);
    }

    #[tokio::test]
    async fn capture_policy_can_blank_a_line_keeping_its_slot() {
        // Returning an empty string elides the *content* while keeping the line
        // (and the exact line counter) — the "drop the payload, not the row"
        // shape. Retention stays `OutputBufferPolicy`'s job.
        struct BlankMatches;
        impl CapturePolicy for BlankMatches {
            fn name(&self) -> &str {
                "blank-matches"
            }
            fn on_capture<'a>(&self, _stream: OutputStream, line: &'a str) -> Cow<'a, str> {
                if line.starts_with("drop") {
                    Cow::Borrowed("")
                } else {
                    Cow::Borrowed(line)
                }
            }
        }
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"keep1\ndrop-this\nkeep2\n"[..],
            policy_config(Arc::new(BlankMatches), OutputStream::Stdout),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 3, "the blanked line is still a counted line");
        assert_eq!(sink.drain(), vec!["keep1", "", "keep2"]);
    }

    #[tokio::test]
    async fn panicking_capture_policy_fails_closed_without_leaking() {
        // A policy that panics must NOT fall back to retaining the raw line (a
        // redactor that leaks the secret it was meant to scrub is the worst
        // outcome): the offending line is retained EMPTY, the line is still
        // counted, and the policy stays active so later lines are still shaped.
        struct PanicOnSecret;
        impl CapturePolicy for PanicOnSecret {
            fn name(&self) -> &str {
                "panic-on-secret"
            }
            fn on_capture<'a>(&self, _stream: OutputStream, line: &'a str) -> Cow<'a, str> {
                if line.contains("secret") {
                    panic!("policy blew up on a secret line");
                }
                Cow::Owned(format!("ok:{line}"))
            }
        }
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let task = tokio::spawn(pump_lines_core(
            &b"one\ntop-secret-value\ntwo\n"[..],
            policy_config(Arc::new(PanicOnSecret), OutputStream::Stdout),
            sink.clone(),
        ));
        task.await
            .expect("the pump task must survive a capture-policy panic");
        assert_eq!(sink.count(), 3, "every line is still counted");
        let retained = sink.drain();
        assert_eq!(
            retained,
            vec!["ok:one", "", "ok:two"],
            "the panicking line is blanked (never the raw secret), later lines still shaped"
        );
        assert!(
            !retained.iter().any(|l| l.contains("secret")),
            "the raw secret line must never reach the backlog"
        );
    }

    #[tokio::test]
    async fn capture_policy_composes_with_drop_oldest_on_transformed_length() {
        // The built-in overflow modes are a separate, unchanged fast path: they
        // evict on the policy's *returned* content. Here the policy shrinks each
        // line to one char, so a 2-line cap retains the last two shrunk lines —
        // retention ran on the transformed text, not the raw line.
        struct FirstChar;
        impl CapturePolicy for FirstChar {
            fn name(&self) -> &str {
                "first-char"
            }
            fn on_capture<'a>(&self, _stream: OutputStream, line: &'a str) -> Cow<'a, str> {
                Cow::Owned(line.chars().take(1).collect())
            }
        }
        let sink = SharedLines::new(&OutputBufferPolicy::bounded(2));
        pump_lines_core(
            &b"alpha\nbravo\ncharlie\ndelta\n"[..],
            policy_config(Arc::new(FirstChar), OutputStream::Stdout),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 4, "every line counted");
        assert_eq!(
            sink.drain(),
            vec!["c", "d"],
            "DropOldest kept the last two, each shaped by the policy"
        );
        assert!(sink.dropped() > 0, "the overflow drop signal still fires");
    }

    #[tokio::test]
    async fn no_capture_policy_leaves_capture_verbatim() {
        // The default (no policy) path is byte-for-byte unchanged.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"secret one\nsecret two\n"[..],
            StreamConfig::new(),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["secret one", "secret two"]);
    }

    // --- Opt-in VT/ANSI sanitizer (`sanitize_vt`) ----------------------------

    /// A stdout `StreamConfig` with the VT sanitizer on (all other knobs default).
    fn sanitize_config() -> StreamConfig {
        StreamConfig {
            sanitize_vt: true,
            ..StreamConfig::new()
        }
    }

    #[test]
    fn strip_vt_leaves_plain_text_borrowed_and_unchanged() {
        // The no-op fast path must return `Cow::Borrowed` OF THE INPUT so the
        // caller reuses its owned line with no re-allocation. A tab is content,
        // kept verbatim.
        for plain in ["", "hello world", "col\tumns", "üñîçödé keeps 8-bit bytes"] {
            match strip_vt(plain) {
                Cow::Borrowed(b) => {
                    assert!(std::ptr::eq(b.as_ptr(), plain.as_ptr()) && b.len() == plain.len());
                }
                Cow::Owned(_) => panic!("plain text must not allocate: {plain:?}"),
            }
        }
    }

    #[test]
    fn strip_vt_removes_csi_osc_and_control_codes() {
        // A representative sweep of what a terminal-driven child emits.
        let cases: &[(&str, &str)] = &[
            // SGR color set/reset around content.
            ("\x1b[31mred\x1b[0m", "red"),
            // Multi-parameter SGR (bold + fg).
            ("\x1b[1;32mok\x1b[m done", "ok done"),
            // Cursor move / erase-line.
            ("a\x1b[2Kb\x1b[10;5Hc", "abc"),
            // Alternate-screen enter/leave (private CSI with `?`).
            ("\x1b[?1049hscreen\x1b[?1049l", "screen"),
            // OSC window-title, BEL-terminated.
            ("\x1b]0;my title\x07visible", "visible"),
            // OSC hyperlink, ST-terminated (`ESC \`).
            ("\x1b]8;;https://x\x1b\\link\x1b]8;;\x1b\\", "link"),
            // DCS string escape, ST-terminated.
            ("pre\x1bPq#0;1;0\x1b\\post", "prepost"),
            // Charset-selection nF escape.
            ("\x1b(Btext", "text"),
            // RIS (two-byte `ESC c`).
            ("\x1bcreset", "reset"),
            // Lone C0 controls (BEL, backspace, form-feed) dropped; tab kept.
            ("a\x07b\x08\x0cc\td", "abc\td"),
            // A bare CR kept as content by `Newline` mode is dropped as noise.
            ("keep\rme", "keepme"),
            // Doubled ESC: the first is dropped, the CSI after it still stripped.
            ("\x1b\x1b[31mx", "x"),
        ];
        for (raw, want) in cases {
            assert_eq!(strip_vt(raw), *want, "sanitizing {raw:?}");
        }
    }

    #[test]
    fn strip_vt_drops_incomplete_trailing_escape() {
        // A sequence with no terminator before the line ends must be dropped
        // whole — never left as a mangled tail in the retained line.
        assert_eq!(strip_vt("value\x1b["), "value"); // dangling CSI intro
        assert_eq!(strip_vt("value\x1b[31"), "value"); // CSI params, no final
        assert_eq!(strip_vt("t\x1b]0;unterminated title"), "t"); // OSC, no BEL/ST
        assert_eq!(strip_vt("end\x1b"), "end"); // lone trailing ESC
    }

    #[test]
    fn strip_vt_keeps_multibyte_scalar_after_unrecognized_escape() {
        // R-01 regression. An `ESC` immediately before a multi-byte UTF-8 scalar
        // is a truncated/garbled escape a terminal-driven child can emit before a
        // glyph. The byte after `ESC` is that scalar's NON-ASCII lead byte, which
        // matches no escape introducer and used to hit `skip_escape`'s catch-all
        // `(start + 2).min(n)` — an index pointing at the scalar's SECOND byte,
        // i.e. NOT a char boundary — so the next `&line[..]` slice in `strip_vt`
        // panicked "byte index N is not a char boundary". Now only the `ESC` is
        // dropped and the scalar is kept as content. Covers 2-, 3- and 4-byte
        // scalars, plus the exact `ESC ©` / `ESC €` cases called out in review.
        let cases: &[(&str, &str)] = &[
            ("\u{1b}\u{a9}", "\u{a9}"),                   // ESC + © (2-byte)
            ("\u{1b}\u{20ac}", "\u{20ac}"),               // ESC + € (3-byte)
            ("\u{1b}\u{1f680}", "\u{1f680}"),             // ESC + 🚀 (4-byte)
            ("pre\u{1b}\u{20ac}post", "pre\u{20ac}post"), // scalar mid-line
            ("a\u{1b}\u{a9}b", "a\u{a9}b"),
            ("end\u{1b}\u{a9}", "end\u{a9}"), // scalar at line end
            // Doubled `ESC` before a multibyte scalar: the first `ESC` is dropped,
            // the second re-parsed (also unrecognized) and dropped, scalar kept.
            ("\u{1b}\u{1b}\u{20ac}", "\u{20ac}"),
            // A run of box-drawing glyphs (3-byte each) after `ESC`.
            ("\u{1b}\u{2500}\u{2502}\u{2514}", "\u{2500}\u{2502}\u{2514}"),
        ];
        for (raw, want) in cases {
            assert_eq!(strip_vt(raw), *want, "sanitizing {raw:?}");
        }
    }

    #[tokio::test]
    async fn sanitize_scrubs_backlog_only_handler_and_tee_see_raw() {
        // The boundary a reviewer must trust — identical in shape to the
        // `capture_policy` boundary test: sanitization scopes to the backlog; the
        // observing handler and decoded tee still see the RAW, escape-laden line.
        let teed = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"\x1b[32mbuilding\x1b[0m target\n"[..],
            StreamConfig {
                handler: Some(handler),
                tee: Some(tee_of(VecSink(teed.clone()))),
                sanitize_vt: true,
                ..StreamConfig::new()
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["building target"],
            "the backlog is sanitized"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["\u{1b}[32mbuilding\u{1b}[0m target"],
            "the handler observes the raw escape-laden line (un-sanitized seam)"
        );
        assert_eq!(
            String::from_utf8(teed.lock().unwrap().clone()).unwrap(),
            "\u{1b}[32mbuilding\u{1b}[0m target\n",
            "the decoded tee observes the raw line too"
        );
    }

    #[tokio::test]
    async fn sanitize_strips_escape_split_across_chunk_boundaries_whole() {
        // The chunk-split requirement: an escape sequence cut between pump reads
        // must be stripped in one piece, leaving no tail of garbage. The line is
        // reassembled in `pending` before it reaches the per-line sanitizer, so
        // EVERY split point of a fixed escape-laden line must yield the same clean
        // text.
        let raw = b"start\x1b[1;31mMID\x1b]0;title\x07END\n";
        let want = "startMIDEND";
        for split in 1..raw.len() {
            let reader = ChunkedReader::new([raw[..split].to_vec(), raw[split..].to_vec()]);
            let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
            pump_lines_core(reader, sanitize_config(), sink.clone()).await;
            assert_eq!(
                sink.drain(),
                vec![want],
                "split at byte {split} must still strip the whole sequence"
            );
        }
    }

    #[tokio::test]
    async fn sanitize_runs_before_capture_policy_so_the_policy_sees_clean_text() {
        // Documented ordering: the sanitizer runs BEFORE the capture policy, so a
        // secret-scrubbing policy matches on already-cleaned text — a token broken
        // up by a color escape (`sec\x1b[0mret`) is rejoined to `secret` and then
        // redacted. Were the order reversed, the policy would see the raw split
        // token and miss it.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"token=sec\x1b[0mret-abc\n"[..],
            StreamConfig {
                sanitize_vt: true,
                buffer_policy: Some(Arc::new(RedactSecrets)),
                ..StreamConfig::new()
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["token=[REDACTED]-abc"],
            "sanitize-then-redact: the color escape can't hide the token from the policy"
        );
    }

    #[tokio::test]
    async fn sanitize_off_leaves_capture_verbatim() {
        // Default off: escapes are retained byte-for-byte (opt-in only).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"\x1b[31mred\x1b[0m\n"[..],
            StreamConfig::new(),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["\u{1b}[31mred\u{1b}[0m"]);
    }

    #[tokio::test]
    async fn sanitize_does_not_disturb_raw_byte_or_line_accounting() {
        // K-059: `seen_bytes` is the RAW pre-decode pipe-byte count; sanitizing
        // content must not shift it. K-054-adjacent: every line is still counted
        // and nothing is dropped by the (content-only) transform.
        let raw = b"\x1b[31mred\x1b[0m\nplain\n\x1b[1mbold\x1b[0m\n";
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(&raw[..], sanitize_config(), sink.clone()).await;
        assert_eq!(
            sink.seen_bytes(),
            raw.len(),
            "seen_bytes still counts every raw pipe byte, escapes included"
        );
        assert_eq!(sink.count(), 3, "every line counted");
        assert_eq!(sink.dropped(), 0, "the content transform drops no line");
        assert_eq!(sink.drain(), vec!["red", "plain", "bold"]);
    }

    #[tokio::test]
    async fn sanitize_composes_with_cr_frames() {
        // Sanitization and `\r`-aware framing compose: each progress frame is a
        // line, and its in-frame color codes are stripped.
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"\x1b[32m50%\x1b[0m\r\x1b[32m100%\x1b[0m\n"[..],
            StreamConfig {
                sanitize_vt: true,
                terminator: LineTerminator::CarriageReturn,
                ..StreamConfig::new()
            },
            sink.clone(),
        )
        .await;
        assert_eq!(sink.drain(), vec!["50%", "100%"], "clean per-frame lines");
    }

    #[tokio::test]
    async fn sanitize_preserves_k054_dropnewest_seal() {
        // K-054 regression: the DropNewest seal-on-first-drop latch still holds
        // with sanitization on. The byte cap gates on the RAW (pre-sanitize) line
        // length — exactly like `capture_policy` (the cap is judged before the
        // transform) — so the retained line's escapes are kept within the cap
        // here; sanitizing then proves it is the CLEANED text that lands, while
        // the seal keeps the backlog a contiguous prefix. `\x1b[mok` is 5 raw
        // bytes (≤ 6, retained → "ok"); `toolongline` is 11 > 6 (over-cap → seals
        // the head via `record_oversized_line`); `hi` alone would fit but the seal
        // is latched, so it is dropped.
        let policy = OutputBufferPolicy::unbounded()
            .with_max_bytes(6)
            .with_overflow(OverflowMode::DropNewest);
        let sink = SharedLines::new(&policy);
        pump_lines_core(
            &b"\x1b[mok\ntoolongline\nhi\n"[..],
            sanitize_config(),
            sink.clone(),
        )
        .await;
        assert_eq!(sink.count(), 3, "every line counted");
        assert_eq!(
            sink.drain(),
            vec!["ok"],
            "the seal latched on the first over-cap line; the retained line is sanitized"
        );
        assert!(sink.dropped() > 0, "the truncation signal fires");
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
    async fn max_bytes_error_mode_counts_line_terminator_against_ceiling() {
        // Error-mode byte ceilings count raw pipe bytes: content that exactly
        // fits still trips once its trailing newline is read.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::Error)
            .with_max_bytes(2);
        let sink = SharedLines::new(&policy);
        let raw = b"ab\n";
        pump_lines(raw.as_slice(), encoding_rs::UTF_8, None, sink.clone()).await;

        assert!(
            sink.overflowed(),
            "the newline must count against the raw-byte Error ceiling"
        );
        assert_eq!(sink.seen_bytes(), raw.len());
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
        // A flood of empty lines still has one raw byte per line (the newline),
        // so the raw-byte ceiling trips while the pump drains the pipe. The
        // derived per-line minimum remains the retained-buffer anti-DoS guard.
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
            17,
            "all bytes read from the pipe, including the CRLF and final newline"
        );
        assert_eq!(split.drain(), vec!["tail"], "the over-cap line is dropped");
        assert_eq!(single.drain(), vec!["tail"]);
    }

    #[tokio::test]
    async fn over_cap_skip_keeps_a_lone_cr_as_content_across_reads() {
        // The deferral must not lose a `\r` that is real content: when the byte
        // after a held-back `\r` is NOT `\n` (a lone CR mid-line), it counts. An
        // over-cap line "XXXXXXXXXX\rYYYYY\n" split right after the `\r` records
        // all 17 raw bytes (the lone `\r` is data, and the final `\n` is read too).
        let mut first = vec![b'X'; 10];
        first.push(b'\r');
        let reader = ChunkedReader::new([first, b"YYYYY\n".to_vec()]);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(4));
        pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.seen_bytes(),
            17,
            "all raw bytes are counted, including the lone CR and final newline"
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
        // `ErrorReason::Io` — the non-regression guard against a false-positive read error.
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
        // reports `ErrorReason::Io`.
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
            16,
            "all bytes read from the pipe, including the CR and final newline"
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
                raw_tee: None,
                terminator: LineTerminator::CarriageReturn,
                ..StreamConfig::new()
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
                raw_tee: None,
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
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
                raw_tee: None,
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
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
                raw_tee: None,
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
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
                raw_tee: None,
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
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
                raw_tee: None,
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
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

    // --- Raw byte tee (T-170) ------------------------------------------------
    //
    // The raw tee receives every chunk exactly as read from the pipe, before any
    // decoding or line splitting: byte-for-byte identical to the child's output,
    // including non-UTF-8 bytes, CRLF, a missing final newline, an unterminated
    // prompt, and a line the buffer policy drops. Strictly additive — the
    // decoded-line path (buffer/handler/line-tee) is unchanged by its presence.

    /// A `StreamConfig` with only a raw tee wired (UTF-8 decode, `\n` framing,
    /// no line handler or line tee) — the common shape for the raw-tee tests.
    fn raw_only_config(raw_tee: RawTeeSink) -> StreamConfig {
        StreamConfig {
            raw_tee: Some(raw_tee),
            ..StreamConfig::new()
        }
    }

    #[tokio::test]
    async fn raw_tee_receives_non_utf8_bytes_verbatim() {
        // Bytes that are not valid UTF-8 (`0x80 0x81`, lone continuation bytes)
        // are lossily replaced on the *decoded* path (U+FFFD) but must reach the
        // raw tee unchanged — the whole point of a byte-accurate transparent tee.
        let raw = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &[0x80, 0x81, b'\n'][..],
            raw_only_config(tee_of(VecSink(raw.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            &[0x80, 0x81, b'\n'],
            "the raw tee gets the exact bytes, non-UTF-8 and terminator included"
        );
        // The decoded path lossily replaced the two invalid bytes with U+FFFD,
        // whose UTF-8 is 6 bytes — proving the tee is genuinely *pre-decode*: it
        // delivered the original 2 invalid bytes, not the decoded text re-encoded.
        let decoded = sink.drain();
        assert_eq!(
            decoded,
            vec!["\u{FFFD}\u{FFFD}"],
            "the decoded line is lossy replacement characters, not the raw bytes"
        );
        assert_ne!(
            &raw.lock().unwrap()[..2],
            decoded[0].as_bytes(),
            "the raw bytes are not the decoded text's UTF-8 re-encoding"
        );
    }

    #[tokio::test]
    async fn raw_tee_preserves_crlf_and_unterminated_tail() {
        // `a\r\nb` with no trailing newline: the decoded path strips the CRLF and
        // emits `b` as an un-terminated final line (no fabricated `\n`). The raw
        // tee must keep the CRLF un-normalized and lose no trailing byte.
        let raw = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"a\r\nb"[..],
            raw_only_config(tee_of(VecSink(raw.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            b"a\r\nb",
            "raw tee keeps CRLF and the un-terminated tail exactly"
        );
        assert_eq!(
            sink.drain(),
            vec!["a", "b"],
            "the decoded line path still strips CRLF and emits the tail as a line"
        );
    }

    #[tokio::test]
    async fn raw_tee_gets_oversized_line_the_byte_cap_drops() {
        // A line longer than the byte cap is skipped from every *decoded* sink
        // (counted only via `dropped()`), but its bytes must still reach the raw
        // tee whole — the raw sink is what a transparent wrapper hashes.
        let policy = OutputBufferPolicy::unbounded().with_max_bytes(3);
        let raw = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&policy);
        pump_lines_core(
            &b"toolongline\nok\n"[..],
            raw_only_config(tee_of(VecSink(raw.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            b"toolongline\nok\n",
            "the raw tee gets the over-cap line whole plus the retained line"
        );
        assert_eq!(sink.count(), 2, "both lines are counted");
        assert_eq!(
            sink.dropped(),
            1,
            "the over-cap line was dropped from capture"
        );
        assert_eq!(sink.drain(), vec!["ok"], "only the in-cap line is retained");
    }

    #[tokio::test]
    async fn raw_tee_gets_bytes_of_lines_dropped_by_dropnewest_seal() {
        // Under `DropNewest` + a line cap of 1, the first line seals the head and
        // every later line is dropped from the decoded path (K-054). The raw tee
        // must still receive all of them — it does not fork the drop accounting,
        // it is fed upstream of the policy entirely.
        let policy = OutputBufferPolicy::bounded(1).with_overflow(OverflowMode::DropNewest);
        let raw = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&policy);
        pump_lines_core(
            &b"a\nb\nc\n"[..],
            raw_only_config(tee_of(VecSink(raw.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            b"a\nb\nc\n",
            "the raw tee gets every byte even for policy-sealed dropped lines"
        );
        assert_eq!(sink.drain(), vec!["a"], "DropNewest retains only the head");
        assert_eq!(sink.dropped(), 2, "the two later lines were sealed off");
    }

    /// An in-memory `AsyncWrite` that records each `poll_write` call's bytes as a
    /// separate entry, so a test can prove chunks are teed *per read* rather than
    /// accumulated and written once at the end.
    #[derive(Clone)]
    struct RecordingSink(Arc<Mutex<Vec<Vec<u8>>>>);
    impl tokio::io::AsyncWrite for RecordingSink {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().unwrap().push(buf.to_vec());
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

    #[tokio::test]
    async fn raw_tee_streams_each_read_without_waiting_for_a_newline() {
        // Two newline-free chunks (`Passw` then `ord: `, an interactive prompt).
        // Each is written to the raw tee as its own read completes — proving a
        // chunk with no terminator reaches the sink immediately, not held in the
        // decode buffer until EOF the way a decoded line is. The per-`poll_write`
        // recording makes the "one write per read, in order" claim deterministic
        // without any wall-clock timing (see K-017).
        let writes = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            ChunkedReader::new([b"Passw".to_vec(), b"ord: ".to_vec()]),
            raw_only_config(tee_of(RecordingSink(writes.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*writes.lock().unwrap(),
            &[b"Passw".to_vec(), b"ord: ".to_vec()],
            "each newline-free chunk is teed as its own write, in read order"
        );
    }

    #[tokio::test]
    async fn raw_tee_preserves_chunk_order_and_content_across_reads() {
        // Split arbitrary bytes across several reads: the raw tee's concatenation
        // is the exact original byte stream, FIFO — no reordering, no loss, no
        // decoding artifact at a chunk boundary that splits a multibyte char.
        let original: Vec<u8> = "αβγ\nδ".bytes().collect(); // multibyte UTF-8
        let raw = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            ChunkedReader::new(to_chunks(&original, &[1, 2, 3])),
            raw_only_config(tee_of(VecSink(raw.clone()))),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            &original,
            "the raw tee reconstructs the exact byte stream in order"
        );
    }

    #[tokio::test]
    async fn raw_tee_and_decoded_sinks_fire_independently() {
        // A raw tee and a decoded-line tee set together: each sees its own view of
        // the same stream, and the buffer/line path is unchanged. Regression guard
        // that the raw sink is strictly additive.
        let raw = Arc::new(Mutex::new(Vec::new()));
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"one\r\ntwo\n"[..],
            StreamConfig {
                encoding: encoding_rs::UTF_8,
                handler: None,
                tee: Some(tee_of(VecSink(lines.clone()))),
                raw_tee: Some(tee_of(VecSink(raw.clone()))),
                terminator: LineTerminator::Newline,
                ..StreamConfig::new()
            },
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            b"one\r\ntwo\n",
            "raw tee: verbatim bytes with CRLF intact"
        );
        assert_eq!(
            String::from_utf8(lines.lock().unwrap().clone()).unwrap(),
            "one\ntwo\n",
            "line tee: decoded lines, CRLF normalized, each with a trailing \\n"
        );
        assert_eq!(
            sink.drain(),
            vec!["one", "two"],
            "the capture buffer is unaffected by the raw tee"
        );
    }

    #[tokio::test]
    async fn raw_tee_write_error_is_isolated_and_capture_continues() {
        // A raw sink that errors on write is disabled for the rest of the run and
        // must not poison the decoded capture — mirrors the line tee's isolation.
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
            raw_only_config(tee_of(ErrSink)),
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["a", "b", "c"],
            "the decoded capture survives a raw tee write error"
        );
    }

    #[tokio::test]
    async fn raw_tee_flushes_a_buffering_sink_at_stream_end() {
        // A raw sink wrapped in a `BufWriter` only commits once flushed; the pump
        // must flush the raw tee at stream end so a buffered tail is not lost.
        let raw = Arc::new(Mutex::new(Vec::new()));
        let buffered = tokio::io::BufWriter::with_capacity(1024, VecSink(raw.clone()));
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        pump_lines_core(
            &b"no-newline-tail"[..],
            raw_only_config(tee_of(buffered)),
            sink.clone(),
        )
        .await;
        assert_eq!(
            &*raw.lock().unwrap(),
            b"no-newline-tail",
            "the raw tee is flushed through its buffering writer at stream end"
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
    async fn drop_newest_seals_head_after_an_over_cap_line() {
        // T-165 continuous-prefix invariant: under DropNewest + a byte cap, an
        // over-cap ("overflowing") line seals the head, so a SHORTER later line
        // that would fit the remaining budget must NOT be retained — otherwise the
        // buffer would skip the dropped line and stop being a prefix of the output.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        // "aa" fits (2 <= 3); "bbbb" is over-cap (4 > 3), dropped whole via the
        // pump's oversized-skip path; "c" would fit the remaining budget but must
        // be dropped — retaining ["aa", "c"] would be a non-contiguous subset.
        pump_lines(
            &b"aa\nbbbb\nc\n"[..],
            encoding_rs::UTF_8,
            None,
            sink.clone(),
        )
        .await;
        assert_eq!(
            sink.drain(),
            vec!["aa"],
            "the over-cap line seals the head; the later short line is not retained"
        );
        assert_eq!(sink.count(), 3, "every line is still counted");
        assert!(
            sink.dropped() >= 2,
            "the over-cap line and the sealed-off tail are both dropped"
        );
    }

    #[tokio::test]
    async fn drop_newest_seals_head_after_an_over_budget_line() {
        // Same invariant when the sealing line is NOT itself over-cap but merely
        // over the *remaining* budget (so it routes through `push`, not the pump's
        // oversized-skip path): a shorter line after it must still be dropped.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(4);
        let sink = SharedLines::new(&policy);
        // "aaa" fits (3 <= 4); "bb" would push the sum to 5 > 4, so it is dropped
        // and seals the head; "c" would fit (3 + 1 = 4) but must be dropped too.
        pump_lines(&b"aaa\nbb\nc\n"[..], encoding_rs::UTF_8, None, sink.clone()).await;
        assert_eq!(
            sink.drain(),
            vec!["aaa"],
            "the first over-budget line seals the head; the later fitting line is not retained"
        );
        assert_eq!(sink.count(), 3);
        assert!(sink.dropped() >= 2);
    }

    #[tokio::test]
    async fn drop_newest_push_seals_head_and_stays_a_prefix() {
        // The seal lives in `push`, proven directly (no pump): a shorter line
        // pushed by hand after an over-budget one is not retained.
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(3);
        let sink = SharedLines::new(&policy);
        sink.push("aa".into()); // 2 bytes, fits
        sink.push("bb".into()); // 2 + 2 = 4 > 3, dropped -> seals the head
        sink.push("a".into()); // would fit (2 + 1 = 3) but the head is sealed
        assert_eq!(
            sink.drain(),
            vec!["aa"],
            "once the head is sealed, a later fitting line is still dropped"
        );
        assert_eq!(
            sink.dropped(),
            2,
            "both post-seal lines are counted as dropped"
        );
    }

    #[tokio::test]
    async fn max_bytes_zero_empty_stream_delivers_no_phantom_segment() {
        // T-165 scenario 1: at `max_bytes = 0` an empty stream must NOT deliver a
        // phantom empty segment to the handler, the buffer, or `seen_bytes` before
        // any real output. Already correct in Rust — the pump only emits a line
        // when a real terminator is decoded — pinned here as a regression.
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(0);
        let sink = SharedLines::new(&policy);
        pump_lines(&b""[..], encoding_rs::UTF_8, Some(handler), sink.clone()).await;
        assert!(
            seen.lock().unwrap().is_empty(),
            "no phantom line reaches the handler"
        );
        assert_eq!(sink.count(), 0, "no phantom line is counted");
        assert_eq!(sink.seen_bytes(), 0, "no phantom bytes are accounted");
        assert!(sink.drain().is_empty(), "nothing retained");
    }

    #[tokio::test]
    async fn max_bytes_zero_unterminated_output_is_not_a_phantom_segment() {
        // At `max_bytes = 0`, real content with no trailing newline is an over-cap
        // line (its 1 byte exceeds the 0-byte cap): it is counted and its bytes are
        // charged, but it is NOT delivered to the handler or retained — and it is
        // never fabricated into a phantom EMPTY segment. Pins that the cap-0 path
        // routes real output through the oversized-skip accounting.
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(0);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"a"[..], encoding_rs::UTF_8, Some(handler), sink.clone()).await;
        assert!(
            seen.lock().unwrap().is_empty(),
            "the over-cap tail is not delivered as a line"
        );
        assert_eq!(sink.count(), 1, "the real line is counted");
        assert_eq!(sink.seen_bytes(), 1, "its one raw byte is counted");
        assert!(sink.drain().is_empty(), "nothing fits a 0-byte cap");
    }

    #[tokio::test]
    async fn max_bytes_zero_real_empty_line_reaches_the_handler_but_is_not_retained() {
        // A genuine empty line (a real `\n` from the process) has 0 content bytes,
        // which fits a 0-byte cap (`0 <= 0`), so it IS delivered to the handler —
        // it is real output, not a phantom. It is still not *retained*: a 0-byte
        // budget holds zero lines (the derived per-line charge), so `would_fit`
        // rejects it. This distinguishes the cap-0 empty-line path from scenario
        // 1's phantom-before-real-output (which never happens; see the pins above).
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = seen.clone();
        let handler: LineHandler =
            Arc::new(move |line: &str| captured.lock().unwrap().push(line.to_owned()));
        let policy = OutputBufferPolicy::unbounded()
            .with_overflow(OverflowMode::DropNewest)
            .with_max_bytes(0);
        let sink = SharedLines::new(&policy);
        pump_lines(&b"\n"[..], encoding_rs::UTF_8, Some(handler), sink.clone()).await;
        assert_eq!(
            *seen.lock().unwrap(),
            vec![""],
            "a real empty line reaches the handler"
        );
        assert_eq!(sink.count(), 1);
        assert_eq!(
            sink.seen_bytes(),
            1,
            "the line terminator is a byte read from the pipe"
        );
        assert!(sink.drain().is_empty(), "a 0-byte budget retains nothing");
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
        at.add_seen_bytes(2);
        at.push("ab".into());
        at.add_seen_bytes(2);
        at.push("cd".into()); // 4 raw bytes -> exactly on the cap
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
        over.add_seen_bytes(2);
        over.push("ab".into());
        over.add_seen_bytes(3);
        over.push("cde".into()); // 5 raw bytes > 4 -> trips
        assert!(
            over.overflowed(),
            "one byte past the cap trips the fail-loud ceiling"
        );
    }

    #[tokio::test]
    async fn error_mode_derived_line_ceiling_trips_on_empty_lines() {
        // This direct SharedLines push seam has no pipe bytes to account for, so
        // empty lines exercise the derived `total_lines > b` guard directly.
        // Exactly `b` empty lines are within budget; the (b+1)-th trips it.
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
            16,
            "all raw bytes, including the final newline, are accounted for"
        );
    }

    #[tokio::test]
    async fn skip_over_cap_len_actually_advances_past_the_discarded_prefix() {
        // `seen_bytes` proves that the skipped prefix and final tail are charged
        // exactly once. The task-local probe additionally pins the reason this
        // helper exists: every skipped chunk must be drained from `pending`, so
        // its high-water mark remains one input chunk. A `0` return value leaves
        // the whole 20 MB line in `pending` and fails this deterministically.
        let chunks: Vec<Vec<u8>> = std::iter::repeat_with(|| vec![b'a'; 8000])
            .take(2500)
            .collect();
        let reader = ChunkedReader::new(chunks);
        // A 1-byte cap forces the very first chunk into the skip path, so every
        // one of the 2500 reads that follow stays in it (no terminator anywhere).
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(1));
        let probe = Arc::new(PumpTestProbe::default());
        PUMP_TEST_PROBE
            .scope(
                probe.clone(),
                pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()),
            )
            .await;
        assert!(
            probe.max_pending_bytes() <= 8000,
            "skipping each 8 KB chunk must keep pending bounded (high-water: {} bytes)",
            probe.max_pending_bytes()
        );
        assert!(
            probe.skip_calls() >= 2500,
            "every chunk must invoke skip_over_cap_len while discarding the flood"
        );
        assert!(
            sink.drain().is_empty(),
            "the over-cap line is never retained"
        );
        assert!(sink.dropped() >= 1, "the over-cap line is a truncation");
        assert_eq!(
            sink.seen_bytes(),
            20_000_000,
            "all 20M raw bytes are accounted for"
        );
    }

    #[tokio::test]
    async fn memory_bound_guard_engages_the_skip_path_for_a_newline_free_flood() {
        // The memory-bound guard (`cap.is_some_and(|c| sub.len() - ... > c)`)
        // is what enters the over-cap skip path in the first place. Pin both the
        // observed guard entry and the bounded pending buffer: replacing it with
        // `false` records no entry and retains the whole newline-free flood.
        let chunks: Vec<Vec<u8>> = std::iter::repeat_with(|| vec![b'x'; 8000])
            .take(3500)
            .collect();
        let reader = ChunkedReader::new(chunks);
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded().with_max_bytes(8));
        let probe = Arc::new(PumpTestProbe::default());
        PUMP_TEST_PROBE
            .scope(
                probe.clone(),
                pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()),
            )
            .await;
        assert_eq!(
            probe.guard_entries(),
            1,
            "the first over-cap chunk must engage the memory-bound guard"
        );
        assert!(
            probe.skip_calls() >= 3500,
            "guard engagement must move the flood into the skip path"
        );
        assert!(
            probe.max_pending_bytes() <= 8000,
            "the guard must keep pending bounded (high-water: {} bytes)",
            probe.max_pending_bytes()
        );
        assert!(
            sink.drain().is_empty(),
            "the over-cap flood is never retained"
        );
        assert!(
            sink.dropped() >= 1,
            "the over-cap flood is recorded as a truncation via record_oversized_line"
        );
        assert_eq!(
            sink.seen_bytes(),
            28_000_000,
            "all 28M raw bytes are accounted for"
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

        /// ASCII lines (byte length == char count) of length 0..=10, so a line's
        /// length straddles the small (1..=8) byte cap the DropNewest prefix
        /// proptest uses — over-cap (long) and short lines interleave frequently,
        /// which is exactly what exercises the head-sealing invariant.
        fn arb_prefix_line() -> impl Strategy<Value = String> {
            prop::collection::vec(prop::char::range('a', 'j'), 0..=10)
                .prop_map(|chars| chars.into_iter().collect())
        }

        /// A single "line" for fuzzing the VT sanitizer (`strip_vt`), heavily
        /// biased toward `ESC` and other control/escape introducers so they land
        /// directly before arbitrary — and frequently multi-byte — scalars. That
        /// `ESC`-then-multibyte adjacency is exactly what regressed in R-01
        /// (`skip_escape` returning a mid-scalar byte index). No `\n`/`\r`: a
        /// single decoded line never contains a line terminator, and the sanitizer
        /// operates strictly per line.
        fn arb_vt_fuzz_line() -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop_oneof![
                    3 => Just('\u{1b}'),                       // ESC introducer
                    1 => Just('\u{7f}'),                       // DEL (strippable)
                    1 => Just('\u{07}'),                       // BEL (OSC terminator)
                    1 => Just('['),                            // common CSI second byte
                    1 => Just(']'),                            // common OSC second byte
                    4 => any::<char>()
                        .prop_filter("no CR/LF", |c| !matches!(*c, '\n' | '\r')),
                ],
                0..24,
            )
            .prop_map(|chars| chars.into_iter().collect())
        }

        fn arb_printable_ascii_line() -> impl Strategy<Value = String> {
            prop::collection::vec(0x20u8..=0x7e, 0..128)
                .prop_map(|bytes| String::from_utf8(bytes).expect("ASCII is UTF-8"))
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

                let expected_bytes = bytes.len();
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

            /// T-165 continuous-prefix invariant: under `OverflowMode::DropNewest`
            /// with a byte cap, the retained buffer must ALWAYS be a contiguous
            /// prefix (head) of the process's actual line output — for any
            /// interleaving of long (over-cap) and short lines, at any chunk
            /// boundaries. Generated sequences of lines whose lengths span both
            /// sides of the cap, joined with `\n`, chunked arbitrarily, and pumped
            /// through DropNewest: `retained == lines[..retained.len()]`, never a
            /// subset that skipped a dropped line and kept a later shorter one.
            #[test]
            fn drop_newest_retains_a_contiguous_prefix_under_a_byte_cap(
                lines in prop::collection::vec(arb_prefix_line(), 0..16),
                max_bytes in 1usize..=8,
                max_lines in prop::option::of(1usize..=16),
                chunk_sizes in prop::collection::vec(1usize..=7, 1..20),
            ) {
                let mut text = String::new();
                for line in &lines {
                    text.push_str(line);
                    text.push('\n');
                }
                let bytes = text.into_bytes();
                let chunks = to_chunks(&bytes, &chunk_sizes);
                let reader = ChunkedReader::new(chunks);
                let mut policy = OutputBufferPolicy::unbounded()
                    .with_overflow(OverflowMode::DropNewest)
                    .with_max_bytes(max_bytes);
                policy.max_lines = max_lines;
                let sink = SharedLines::new(&policy);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("current-thread runtime");
                rt.block_on(pump_lines(reader, encoding_rs::UTF_8, None, sink.clone()));

                let retained = sink.drain();
                // The core invariant: the retained set is a true prefix of the
                // output — never a subset that skipped a dropped line and kept a
                // later shorter one.
                prop_assert!(
                    lines.starts_with(&retained),
                    "retained {:?} is not a contiguous prefix of output {:?}",
                    retained,
                    lines
                );
                // Every line is still counted, and each retained line fits the byte
                // cap (an over-cap line can never be part of the head).
                prop_assert_eq!(sink.count(), lines.len(), "every line counted");
                for r in &retained {
                    prop_assert!(
                        r.len() <= max_bytes,
                        "retained line {:?} exceeds the {}-byte cap",
                        r,
                        max_bytes
                    );
                }
                // A prefix shorter than the whole output means something was
                // dropped, and vice versa — the truncation signal is exact.
                prop_assert_eq!(retained.len() < lines.len(), sink.dropped() > 0);
            }

            /// R-01 as a property, at the sanitizer itself: `strip_vt` must never
            /// panic and must always yield valid UTF-8 for ANY line, however
            /// malformed its escapes — in particular an `ESC` directly before a
            /// multi-byte scalar, which used to make `skip_escape` return a
            /// mid-scalar byte index and panic the `&line[..]` slice. The input is
            /// biased toward `ESC`/control introducers so that adjacency is hit
            /// constantly. Idempotence (a second pass is a no-op) is asserted too:
            /// it can only hold if every escape was consumed on a char boundary,
            /// so it doubles as a boundary-correctness check.
            #[test]
            fn strip_vt_never_panics_on_arbitrary_malformed_escapes(
                line in arb_vt_fuzz_line(),
            ) {
                let cleaned = strip_vt(&line).into_owned();
                let twice = strip_vt(&cleaned).into_owned();
                prop_assert!(
                    cleaned.len() <= line.len(),
                    "strip_vt must never expand its input"
                );
                prop_assert!(
                    !cleaned
                        .bytes()
                        .any(|byte| byte == ESC || is_strippable_control(byte)),
                    "sanitized output retained ESC, C0, or DEL"
                );
                prop_assert_eq!(
                    twice,
                    cleaned,
                    "strip_vt must be idempotent (every escape consumed on a char boundary)"
                );
            }

            /// Printable text contains no terminal control bytes, so the
            /// sanitizer's zero-allocation fast path must preserve it exactly.
            #[test]
            fn strip_vt_preserves_clean_printable_text(line in arb_printable_ascii_line()) {
                prop_assert!(matches!(strip_vt(&line), Cow::Borrowed(_)));
                prop_assert_eq!(strip_vt(&line), line.as_str());
            }

            /// The sanitizer-enabled twin of
            /// `pump_never_panics_on_arbitrary_bytes_under_any_chunking` (R-01):
            /// that existing panic-freedom proptest runs the DEFAULT config, so the
            /// `sanitize_vt: true` path — the only one that reaches
            /// `strip_vt`/`skip_escape` — was never fuzzed, and the
            /// `ESC`-before-multibyte panic slipped through a green self-check.
            /// Arbitrary bytes (routinely decoding to multi-byte UTF-8 with
            /// interleaved `ESC`s), chunked at arbitrary read boundaries, pumped
            /// with the sanitizer on: reaching here without a panic — plus
            /// internally consistent counters — is the invariant.
            #[test]
            fn sanitizing_pump_never_panics_on_arbitrary_bytes_under_any_chunking(
                raw in prop::collection::vec(any::<u8>(), 0..512),
                chunk_sizes in prop::collection::vec(1usize..=9, 1..20),
            ) {
                let chunks = to_chunks(&raw, &chunk_sizes);
                let reader = ChunkedReader::new(chunks);
                let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("current-thread runtime");
                rt.block_on(pump_lines_core(reader, sanitize_config(), sink.clone()));

                let lines = sink.drain();
                prop_assert!(lines.len() <= sink.count());
            }
        }
    }
}
