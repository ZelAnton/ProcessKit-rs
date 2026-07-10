//! Standard-input sources and the interactive stdin writer.

use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio_stream::{Stream, StreamExt};

/// A boxed async reader, shared so [`Stdin`] stays `Clone` (one-shot: consumed
/// on first use). A plain [`Mutex`], not an async one: the only critical
/// sections — reserving the payload out of the cell, and restoring it on a
/// rolled-back reservation — are instant `Option::take`/store steps that never
/// span an `.await`, and a synchronous lock is what lets a dropped, uncommitted
/// reservation put the payload back from `Drop` (which cannot `.await`).
type SharedReader = Arc<Mutex<Option<Pin<Box<dyn AsyncRead + Send>>>>>;
/// A boxed async line stream, shared the same way.
type SharedLines = Arc<Mutex<Option<Pin<Box<dyn Stream<Item = String> + Send>>>>>;

/// Lock a shared one-shot cell, tolerating a poisoned mutex. The critical
/// section never panics while holding the lock (it only `take`s/stores an
/// `Option`), so poisoning cannot arise from this module — recovering the guard
/// rather than unwrapping keeps a rollback in `Drop` from turning an unrelated
/// panic into a double panic.
fn lock_cell<T>(cell: &Arc<Mutex<T>>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What to feed a child process on standard input.
///
/// When a command has no `Stdin` (or
/// [`Stdin::empty`]), stdin is closed at start so the child reads EOF
/// immediately. The streaming sources ([`from_reader`](Self::from_reader),
/// [`from_lines`](Self::from_lines)) are one-shot — their payload feeds the
/// first run that actually **starts a child** and is consumed then. Re-running
/// or retrying a [`Command`](crate::Command) that reuses a consumed one-shot
/// source **fails loud** (an [`Error::Io`](crate::Error::Io) at launch) rather
/// than silently feeding the next run empty stdin; use a reusable source
/// (`from_string`/`from_bytes`/`from_file`/`from_iter_lines`) to re-run.
///
/// Reservation is transactional: a launch that fails **before** a child exists
/// (program not found, a spawn error, an already-raised cancellation) leaves the
/// payload untouched, so the very same `Command` can be launched again and will
/// feed that child normally. Only the launch that reaches a live child consumes
/// the source — and it stays consumed even if the subsequent write to the
/// child's stdin fails (e.g. the child closed its read end early). Two concurrent
/// launches of one cloned source never both feed a child: exactly one reserves
/// the payload; the other fails loud.
#[derive(Clone)]
pub struct Stdin(Source);

#[derive(Clone)]
enum Source {
    Empty,
    Bytes(Vec<u8>),
    File(PathBuf),
    Reader(SharedReader),
    Lines(SharedLines),
}

impl Stdin {
    /// No input: stdin is closed at start so the child reads EOF immediately.
    pub fn empty() -> Self {
        Stdin(Source::Empty)
    }

    /// Feed `text` (UTF-8) to the child's stdin.
    pub fn from_string(text: impl Into<String>) -> Self {
        Stdin(Source::Bytes(text.into().into_bytes()))
    }

    /// Feed raw `bytes` to the child's stdin.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Stdin(Source::Bytes(bytes.into()))
    }

    /// Stream the contents of the file at `path` to the child's stdin.
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        Stdin(Source::File(path.as_ref().to_path_buf()))
    }

    /// Write each item (as a UTF-8 line, `\n`-terminated) to the child's stdin.
    /// Eagerly collected, so the resulting [`Stdin`] is fully reusable. (The
    /// async-stream analogue is [`from_lines`](Self::from_lines).)
    ///
    /// **Newline contract:** exactly one `\n` is appended after *every* item,
    /// including the last — so `["a", "b"]` sends `"a\nb\n"` (a trailing
    /// newline). Each item is written verbatim and is **not** re-split, so an
    /// item that already contains `\n` (or ends in one) is passed through as-is
    /// and yields a blank line. To send bytes without this per-item framing, use
    /// [`from_bytes`](Self::from_bytes) / [`from_string`](Self::from_string).
    pub fn from_iter_lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut buf = Vec::new();
        for line in lines {
            buf.extend_from_slice(line.as_ref().as_bytes());
            buf.push(b'\n');
        }
        Stdin(Source::Bytes(buf))
    }

    /// Stream an arbitrary async reader to the child's stdin. One-shot.
    pub fn from_reader<R>(reader: R) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Stdin(Source::Reader(Arc::new(Mutex::new(Some(Box::pin(reader))))))
    }

    /// Write each item of an async string stream as a `\n`-terminated line.
    /// One-shot.
    pub fn from_lines<S>(lines: S) -> Self
    where
        S: Stream<Item = String> + Send + 'static,
    {
        Stdin(Source::Lines(Arc::new(Mutex::new(Some(Box::pin(lines))))))
    }

    /// Whether this source closes stdin without writing anything.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.0, Source::Empty)
    }

    /// Whether this is a one-shot streaming source
    /// ([`from_reader`](Self::from_reader) / [`from_lines`](Self::from_lines)) —
    /// its payload feeds a *single* run and cannot be replayed. The retry path
    /// uses this to refuse retrying such a command (a retry could not re-feed the
    /// input), and the launch path reserves the payload transactionally (see
    /// [`take_for_run`](Self::take_for_run)), committing it only once a child
    /// exists.
    pub(crate) fn is_one_shot(&self) -> bool {
        matches!(self.0, Source::Reader(_) | Source::Lines(_))
    }

    /// A **stable** digest of the stdin *source identity* for cassette keying —
    /// the payload itself is never persisted (preserving the no-payload posture),
    /// only this hash, so two otherwise-identical invocations that differ only in
    /// their stdin no longer collide on replay. FNV-1a (not `DefaultHasher`, whose
    /// value can change between Rust releases) so a digest recorded today matches
    /// one computed tomorrow. Byte content is hashed verbatim; a file source
    /// hashes its *path* (the file is not read at key time). The one-shot
    /// streaming sources have no fixed content, so they hash a discriminant only
    /// — but the cassette runner rejects them outright before this is keyed, since
    /// that discriminant would collide distinct payloads.
    #[cfg(feature = "record")]
    pub(crate) fn content_digest(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        fn mix(mut h: u64, bytes: &[u8]) -> u64 {
            for &b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(PRIME);
            }
            h
        }
        let (tag, payload): (u8, &[u8]) = match &self.0 {
            Source::Empty => (0, &[]),
            Source::Bytes(b) => (1, b),
            Source::File(p) => (2, p.as_os_str().as_encoded_bytes()),
            Source::Reader(_) | Source::Lines(_) => (3, b"<stream>"),
        };
        mix(mix(OFFSET, &[tag]), payload)
    }

    /// The [`Stdio`] to configure on the spawn: `null` for [`Self::empty`] (EOF
    /// at start), `piped` otherwise (we write, then drop to send EOF).
    pub(crate) fn stdio(&self) -> Stdio {
        if self.is_empty() {
            Stdio::null()
        } else {
            Stdio::piped()
        }
    }

    /// Reserve this source's payload for a single run, or report a one-shot
    /// source already consumed (or currently reserved) by another run.
    ///
    /// One-shot sources ([`from_reader`](Self::from_reader)/
    /// [`from_lines`](Self::from_lines)) are removed from their shared cell here —
    /// **atomically**, under the lock — so the take and the "already consumed?"
    /// decision are a single step. This closes the TOCTOU where two concurrent
    /// runs of the same cloned source could each pass a separate
    /// `is_consumed`-style check and then have one silently feed the child empty
    /// stdin: a concurrent second run observes the source taken and fails loud at
    /// launch. Re-runnable sources (bytes/file/empty) reserve a fresh clone of
    /// their replayable payload.
    ///
    /// The take is a *reservation*, not yet a commitment. The returned
    /// [`StdinReservation`] holds the payload out of the cell for the duration of
    /// the launch. On success the caller [`commit`](StdinReservation::commit)s it
    /// (yielding the payload to feed the child and leaving a one-shot cell empty
    /// forever); if the launch fails before a child exists, dropping the
    /// reservation without committing returns the payload to the cell, so the same
    /// [`Command`](crate::Command) can be launched again. Call once per run, at
    /// launch.
    pub(crate) fn take_for_run(&self) -> Result<StdinReservation, OneShotConsumed> {
        let reserved = match &self.0 {
            Source::Empty => Reserved::Reusable(TakenStdin::Empty),
            Source::Bytes(bytes) => Reserved::Reusable(TakenStdin::Bytes(bytes.clone())),
            Source::File(path) => Reserved::Reusable(TakenStdin::File(path.clone())),
            Source::Reader(reader) => match lock_cell(reader).take() {
                Some(r) => Reserved::OneShotReader(r, Arc::clone(reader)),
                None => return Err(OneShotConsumed),
            },
            Source::Lines(lines) => match lock_cell(lines).take() {
                Some(s) => Reserved::OneShotLines(s, Arc::clone(lines)),
                None => return Err(OneShotConsumed),
            },
        };
        Ok(StdinReservation(Some(reserved)))
    }
}

/// A one-shot streaming stdin source ([`Stdin::from_reader`]/
/// [`Stdin::from_lines`]) whose payload was already consumed (or is currently
/// reserved) by another run — returned by [`Stdin::take_for_run`] so the launch
/// path can fail loud.
#[derive(Debug)]
pub(crate) struct OneShotConsumed;

/// A payload reserved for one run by [`Stdin::take_for_run`], held across the
/// launch's spawn attempt so the take can be **committed or rolled back**.
///
/// This is what makes one-shot reservation transactional. Reserving a one-shot
/// source removes its payload from the shared cell up front, so a concurrent
/// second run observes it taken and fails loud (never spawning a duplicate
/// child). Then:
///
/// - the launch reaches a live child → [`commit`](Self::commit): the payload is
///   yielded to feed the child and a one-shot cell is left empty for good, so the
///   source stays consumed even if the later write to the child's stdin fails;
/// - the launch fails first (program not found, spawn error, raised cancel) →
///   the reservation is dropped **without** committing, and its [`Drop`] returns
///   the payload to the cell so the same command can be launched again.
///
/// Re-runnable sources (bytes/file/empty) reserve a fresh clone and roll back to
/// nothing — commit and drop are equivalent for them.
pub(crate) struct StdinReservation(Option<Reserved>);

/// The reserved payload inside a [`StdinReservation`] — `None` once committed or
/// rolled back. The one-shot variants also carry a clone of their shared cell so
/// a drop-without-commit can restore the payload there.
enum Reserved {
    /// A re-runnable payload (bytes/file/empty): nothing to restore on rollback.
    Reusable(TakenStdin),
    /// A one-shot reader taken out of its cell; rollback puts it back.
    OneShotReader(Pin<Box<dyn AsyncRead + Send>>, SharedReader),
    /// A one-shot line stream taken out of its cell; rollback puts it back.
    OneShotLines(Pin<Box<dyn Stream<Item = String> + Send>>, SharedLines),
}

impl StdinReservation {
    /// Commit the reservation now that a child exists: yield the payload to write
    /// to the child and leave any one-shot cell permanently empty (the source is
    /// consumed). Consumes the reservation, so its [`Drop`] can no longer roll
    /// back — the source stays consumed even if writing the payload then fails.
    pub(crate) fn commit(mut self) -> TakenStdin {
        match self
            .0
            .take()
            .expect("a StdinReservation is committed at most once")
        {
            Reserved::Reusable(payload) => payload,
            // Drop the cell handle without restoring: the source is now consumed.
            Reserved::OneShotReader(reader, _cell) => TakenStdin::Reader(reader),
            Reserved::OneShotLines(lines, _cell) => TakenStdin::Lines(lines),
        }
    }
}

impl Drop for StdinReservation {
    fn drop(&mut self) {
        // An uncommitted one-shot reservation returns its payload to the source's
        // cell, so a later launch of the same command can take it again. A
        // re-runnable (or already-committed) reservation has nothing to restore.
        match self.0.take() {
            Some(Reserved::OneShotReader(reader, cell)) => {
                *lock_cell(&cell) = Some(reader);
            }
            Some(Reserved::OneShotLines(lines, cell)) => {
                *lock_cell(&cell) = Some(lines);
            }
            Some(Reserved::Reusable(_)) | None => {}
        }
    }
}

/// A stdin payload committed to one run — obtained from
/// [`StdinReservation::commit`] once a child exists. It owns its content (a
/// one-shot source has been removed from its shared cell for good), so writing it
/// is a plain move — there is no second take and so no empty-stdin footgun.
pub(crate) enum TakenStdin {
    Empty,
    Bytes(Vec<u8>),
    File(PathBuf),
    Reader(Pin<Box<dyn AsyncRead + Send>>),
    Lines(Pin<Box<dyn Stream<Item = String> + Send>>),
}

impl TakenStdin {
    /// Whether this payload writes nothing (stdin is closed at start → EOF).
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, TakenStdin::Empty)
    }

    /// Write the payload to the child's stdin pipe, then return so the caller can
    /// drop the sink to signal EOF.
    pub(crate) async fn write_to<W>(self, sink: &mut W) -> std::io::Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        match self {
            TakenStdin::Empty => Ok(()),
            TakenStdin::Bytes(bytes) => sink.write_all(&bytes).await,
            TakenStdin::File(path) => {
                let mut file = tokio::fs::File::open(&path).await?;
                tokio::io::copy(&mut file, sink).await.map(|_| ())
            }
            TakenStdin::Reader(mut r) => tokio::io::copy(&mut r, sink).await.map(|_| ()),
            TakenStdin::Lines(mut stream) => {
                while let Some(line) = stream.next().await {
                    sink.write_all(line.as_bytes()).await?;
                    sink.write_all(b"\n").await?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for Stdin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.0 {
            Source::Empty => "Empty",
            Source::Bytes(_) => "Bytes",
            Source::File(_) => "File",
            Source::Reader(_) => "Reader",
            Source::Lines(_) => "Lines",
        };
        f.debug_tuple("Stdin").field(&kind).finish()
    }
}

/// An interactive writer to a child's standard input.
///
/// Available from [`RunningProcess::take_stdin`](crate::RunningProcess::take_stdin)
/// when the command was built with
/// [`Command::keep_stdin_open`](crate::Command::keep_stdin_open). Write
/// incrementally, then call [`finish`](Self::finish) to send EOF — dropping the
/// writer (or the process handle) without finishing closes stdin too.
///
/// **Avoid the full-duplex deadlock.** A child's stdout pipe has a finite OS
/// buffer; once it fills, the child blocks on *writing* stdout until something
/// reads it. If you feed a large interactive stdin while nothing is draining the
/// child's stdout, the child stops reading stdin (it is blocked writing stdout),
/// your [`write`](Self::write) parks waiting for stdin buffer space, and neither
/// side makes progress. When you both write a sizable stdin **and** the child
/// produces output, drain its stdout concurrently — e.g. stream
/// [`stdout_lines`](crate::RunningProcess::stdout_lines) from one task while
/// writing stdin from another. (The non-interactive sources —
/// [`Stdin::from_bytes`]/[`Stdin::from_string`]/[`Stdin::from_file`]/
/// [`Stdin::from_reader`] — are safe: the crate writes them on a background task
/// that runs concurrently with the output pumps.)
pub struct ProcessStdin {
    sink: tokio::process::ChildStdin,
}

impl ProcessStdin {
    pub(crate) fn new(sink: tokio::process::ChildStdin) -> Self {
        Self { sink }
    }

    /// Write raw bytes to stdin.
    ///
    /// # Errors
    ///
    /// The underlying [`std::io::Error`] from writing to the child's stdin pipe —
    /// commonly [`BrokenPipe`](std::io::ErrorKind::BrokenPipe) once the child has
    /// closed stdin (read all it wanted) or exited.
    pub async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.sink.write_all(bytes).await
    }

    /// Write `line` followed by `\n` (UTF-8), flushing so the child sees it
    /// promptly.
    ///
    /// # Errors
    ///
    /// The underlying [`std::io::Error`] from writing the line, its newline, or
    /// the flush — commonly [`BrokenPipe`](std::io::ErrorKind::BrokenPipe) once
    /// the child has closed stdin or exited.
    pub async fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        self.sink.write_all(line.as_bytes()).await?;
        self.sink.write_all(b"\n").await?;
        self.sink.flush().await
    }

    /// Flush buffered bytes to the child.
    ///
    /// # Errors
    ///
    /// The underlying [`std::io::Error`] from flushing the pipe — commonly
    /// [`BrokenPipe`](std::io::ErrorKind::BrokenPipe) once the child has closed
    /// stdin or exited.
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush().await
    }

    /// Close stdin, signalling EOF to the child.
    ///
    /// # Errors
    ///
    /// The underlying [`std::io::Error`] from shutting the pipe down; a child
    /// that already closed its read end may surface
    /// [`BrokenPipe`](std::io::ErrorKind::BrokenPipe).
    pub async fn finish(mut self) -> std::io::Result<()> {
        self.sink.shutdown().await
    }

    /// Send a control character to the child's stdin — e.g. `send_control('c')`
    /// for Ctrl-C (`\x03`, ETX) or `send_control('d')` for Ctrl-D (`\x04`, EOT).
    ///
    /// Accepts the ASCII letters `'a'..='z'`/`'A'..='Z'` (mapped to their
    /// control-code equivalent, `(c.to_ascii_uppercase() as u8) & 0x1f`) plus a
    /// few control characters commonly reachable from a terminal driver:
    /// `'['`/`'\\'`/`']'`/`'^'`/`'_'` (Ctrl-`[`/Ctrl-`\`/Ctrl-`]`/Ctrl-`^`/Ctrl-`_`,
    /// the same `& 0x1f` mapping) and `'?'` (Ctrl-`?`, i.e. DEL/`0x7f`, which does
    /// not fit the `& 0x1f` pattern). Any other character is rejected with an
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) error rather than
    /// silently writing a meaningless byte.
    ///
    /// **This writes exactly one raw byte into the child's stdin pipe — it is
    /// not a real terminal signal.** A genuine SIGINT/SIGTSTP/etc. is something
    /// the OS terminal driver raises for a *foreground process group attached to
    /// a TTY*; here there is no TTY, only a plain pipe, so the child only
    /// "reacts" if the child itself reads its stdin and recognizes the byte
    /// (as many REPLs and line editors do — e.g. readline treats `\x04` as
    /// end-of-input). A child that doesn't inspect its stdin for control bytes
    /// will not be interrupted or signaled by this call. Real terminal-signal
    /// semantics (the kernel actually delivering `SIGINT` etc. to the child)
    /// require a pseudo-terminal, which this crate does not yet provide — see
    /// the (git-untracked) design note `ideas/later-pty-support.md` for that
    /// direction.
    ///
    /// # Errors
    ///
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if `c` is not one of
    /// the accepted characters above. Otherwise, the underlying
    /// [`std::io::Error`] from writing to the child's stdin pipe — commonly
    /// [`BrokenPipe`](std::io::ErrorKind::BrokenPipe) once the child has closed
    /// stdin or exited.
    pub async fn send_control(&mut self, c: char) -> std::io::Result<()> {
        let byte = control_byte(c)?;
        self.sink.write_all(&[byte]).await
    }
}

/// The single control byte that [`ProcessStdin::send_control`] would write for
/// `c`, or an `InvalidInput` error for an unrecognized character. Split out
/// from `send_control` so the validation/mapping logic is unit-testable
/// without a real child process.
fn control_byte(c: char) -> std::io::Result<u8> {
    match c {
        'a'..='z' | 'A'..='Z' | '[' | '\\' | ']' | '^' | '_' => {
            Ok((c.to_ascii_uppercase() as u8) & 0x1f)
        }
        '?' => Ok(0x7f),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "send_control: {c:?} is not a recognized control character \
                 (expected 'a'..='z'/'A'..='Z', '[', '\\\\', ']', '^', '_', or '?')"
            ),
        )),
    }
}

impl fmt::Debug for ProcessStdin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessStdin").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reserve the source for one run, commit it (as a successful launch would),
    /// and drive its payload into an in-memory sink, returning what was written
    /// (panics if the one-shot source was already consumed).
    async fn written(stdin: &Stdin) -> Vec<u8> {
        let mut sink = Vec::new();
        stdin
            .take_for_run()
            .unwrap_or_else(|_| panic!("source already consumed"))
            .commit()
            .write_to(&mut sink)
            .await
            .expect("write_to");
        sink
    }

    #[tokio::test]
    async fn reader_source_is_one_shot() {
        // A second take of the same (cloned) source reports it consumed rather
        // than silently feeding empty stdin.
        let stdin = Stdin::from_reader(&b"payload"[..]);
        assert_eq!(written(&stdin).await, b"payload");
        assert!(
            stdin.take_for_run().is_err(),
            "a consumed one-shot reader reports OneShotConsumed, not empty"
        );
    }

    #[tokio::test]
    async fn is_one_shot_classifies_streaming_sources() {
        assert!(Stdin::from_reader(&b"x"[..]).is_one_shot());
        assert!(Stdin::from_lines(tokio_stream::iter(vec!["x".to_owned()])).is_one_shot());
        // Re-runnable sources are not one-shot.
        assert!(!Stdin::from_bytes(b"abc".to_vec()).is_one_shot());
        assert!(!Stdin::from_iter_lines(["a", "b"]).is_one_shot());
        assert!(!Stdin::from_string("x").is_one_shot());
        assert!(!Stdin::empty().is_one_shot());
    }

    #[tokio::test]
    async fn lines_source_is_one_shot_and_newline_terminated() {
        let stdin = Stdin::from_lines(tokio_stream::iter(vec![
            "first".to_owned(),
            "second".to_owned(),
        ]));
        assert_eq!(written(&stdin).await, b"first\nsecond\n");
        assert!(
            stdin.take_for_run().is_err(),
            "the stream was consumed by the first run"
        );
    }

    #[tokio::test]
    async fn iter_lines_is_reusable_and_newline_terminated() {
        let stdin = Stdin::from_iter_lines(["a", "b"]);
        assert_eq!(written(&stdin).await, b"a\nb\n");
        assert_eq!(
            written(&stdin).await,
            b"a\nb\n",
            "eagerly-collected lines replay on every run"
        );
    }

    #[tokio::test]
    async fn iter_lines_appends_one_newline_per_item_verbatim() {
        // Contract: exactly one `\n` after every item, verbatim, no re-splitting.
        // An item ending in `\n` yields a blank line; an empty iterator yields
        // no bytes (not a lone newline).
        assert_eq!(
            written(&Stdin::from_iter_lines(["a\n", "b"])).await,
            b"a\n\nb\n"
        );
        assert_eq!(
            written(&Stdin::from_iter_lines(Vec::<&str>::new())).await,
            b""
        );
    }

    #[tokio::test]
    async fn missing_file_surfaces_not_found() {
        let stdin = Stdin::from_file("processkit-definitely-missing-424242.txt");
        let mut sink = Vec::new();
        let err = stdin
            .take_for_run()
            .unwrap_or_else(|_| panic!("file source is re-runnable"))
            .commit()
            .write_to(&mut sink)
            .await
            .expect_err("a missing stdin file must error, not feed silence");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn empty_source_writes_nothing() {
        assert!(written(&Stdin::empty()).await.is_empty());
    }

    #[test]
    fn a_reservation_holds_the_source_taken_until_it_is_committed() {
        // Reserving a one-shot source removes its payload up front, so a second
        // (concurrent) take observes it already reserved and fails loud — no
        // window where both reservations succeed and one then feeds empty stdin.
        // This models two concurrent launches racing one cloned source: exactly
        // one reserves the payload; the other errors instead of spawning a
        // duplicate child. The take is synchronous and instant, so the loser is
        // never blocked on the winner.
        let stdin = Stdin::from_reader(&b"payload"[..]);
        let stdin2 = stdin.clone();

        let reservation = stdin.take_for_run().expect("the first reservation wins");
        assert!(
            stdin2.take_for_run().is_err(),
            "a second concurrent take sees the source already reserved"
        );
        // Committing (a live child now exists) keeps it consumed for good.
        let _committed = reservation.commit();
        assert!(
            stdin.take_for_run().is_err(),
            "a committed one-shot source stays consumed"
        );
    }

    #[tokio::test]
    async fn dropping_a_reservation_without_committing_returns_the_payload() {
        // A launch that fails before a child exists drops its reservation
        // uncommitted; that rolls the payload back into the shared cell, so the
        // same source can be reserved and fed by a later run.
        let stdin = Stdin::from_reader(&b"payload"[..]);
        {
            let _reservation = stdin.take_for_run().expect("first reservation");
            assert!(
                stdin.take_for_run().is_err(),
                "while a reservation is outstanding the source is taken"
            );
            // `_reservation` drops here uncommitted → the payload is restored.
        }
        // The rolled-back payload feeds a later run exactly as if untouched.
        assert_eq!(written(&stdin).await, b"payload");
        // That later run committed the source, so it is now finally consumed.
        assert!(
            stdin.take_for_run().is_err(),
            "the committed re-run consumes the one-shot source"
        );
    }

    #[tokio::test]
    async fn dropping_a_lines_reservation_also_rolls_back() {
        // The rollback path applies to `from_lines` as much as `from_reader`.
        let stdin = Stdin::from_lines(tokio_stream::iter(vec!["a".to_owned(), "b".to_owned()]));
        drop(stdin.take_for_run().expect("reserve the line stream"));
        assert_eq!(written(&stdin).await, b"a\nb\n");
        assert!(
            stdin.take_for_run().is_err(),
            "the committed re-run consumes the line stream"
        );
    }

    #[tokio::test]
    async fn a_reusable_source_survives_reserve_commit_and_rollback() {
        // Re-runnable sources clone their payload per reservation, so neither a
        // rollback nor a commit ever consumes them.
        let stdin = Stdin::from_bytes(b"abc".to_vec());
        drop(stdin.take_for_run().expect("reserve")); // rollback: no-op for a clone
        assert_eq!(written(&stdin).await, b"abc"); // commit
        assert_eq!(
            written(&stdin).await,
            b"abc",
            "a re-runnable source replays on every run"
        );
    }

    #[test]
    fn control_byte_maps_letters_to_the_control_range() {
        // Ctrl-C / Ctrl-D, case-insensitive, both map to the same byte.
        assert_eq!(control_byte('c').expect("valid"), 0x03);
        assert_eq!(control_byte('C').expect("valid"), 0x03);
        assert_eq!(control_byte('d').expect("valid"), 0x04);
        assert_eq!(control_byte('D').expect("valid"), 0x04);
        // Ctrl-Z, common EOF-on-Windows control character.
        assert_eq!(control_byte('z').expect("valid"), 0x1a);
        // The special terminal-driver characters, including DEL/0x7f which does
        // not fit the `& 0x1f` mapping.
        assert_eq!(control_byte('[').expect("valid"), 0x1b);
        assert_eq!(control_byte('\\').expect("valid"), 0x1c);
        assert_eq!(control_byte(']').expect("valid"), 0x1d);
        assert_eq!(control_byte('^').expect("valid"), 0x1e);
        assert_eq!(control_byte('_').expect("valid"), 0x1f);
        assert_eq!(control_byte('?').expect("valid"), 0x7f);
    }

    #[test]
    fn control_byte_rejects_unrecognized_characters() {
        for bad in ['0', ' ', '!', '@', 'é', '\u{1}'] {
            let err = control_byte(bad).expect_err("must reject an unrecognized character");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}
