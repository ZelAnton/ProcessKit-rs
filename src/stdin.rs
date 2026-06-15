//! Standard-input sources and the interactive stdin writer.

use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::{Stream, StreamExt};

/// A boxed async reader, shared so [`Stdin`] stays `Clone` (one-shot: consumed
/// on first use).
type SharedReader = Arc<AsyncMutex<Option<Pin<Box<dyn AsyncRead + Send>>>>>;
/// A boxed async line stream, shared the same way.
type SharedLines = Arc<AsyncMutex<Option<Pin<Box<dyn Stream<Item = String> + Send>>>>>;

/// What to feed a child process on standard input.
///
/// When a command has no `Stdin` (or
/// [`Stdin::empty`]), stdin is closed at start so the child reads EOF
/// immediately. The streaming sources ([`from_reader`](Self::from_reader),
/// [`from_lines`](Self::from_lines)) are one-shot — their payload is consumed by
/// the first run. Re-running or retrying a [`Command`](crate::Command) that
/// reuses a consumed one-shot source **fails loud** (an
/// [`Error::Io`](crate::Error::Io) at launch, D10) rather than silently feeding
/// the next run empty stdin; use a reusable source
/// (`from_string`/`from_bytes`/`from_file`/`from_iter_lines`) to re-run.
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
        Stdin(Source::Reader(Arc::new(AsyncMutex::new(Some(Box::pin(
            reader,
        ))))))
    }

    /// Write each item of an async string stream as a `\n`-terminated line.
    /// One-shot.
    pub fn from_lines<S>(lines: S) -> Self
    where
        S: Stream<Item = String> + Send + 'static,
    {
        Stdin(Source::Lines(Arc::new(AsyncMutex::new(Some(Box::pin(
            lines,
        ))))))
    }

    /// Whether this source closes stdin without writing anything.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.0, Source::Empty)
    }

    /// D10: whether this is a one-shot streaming source
    /// ([`from_reader`](Self::from_reader) / [`from_lines`](Self::from_lines)) —
    /// its payload feeds a *single* run and cannot be replayed. The retry path
    /// uses this to refuse retrying such a command (a retry could not re-feed the
    /// input), and the launch path takes the payload atomically (see
    /// [`take_for_run`](Self::take_for_run)).
    pub(crate) fn is_one_shot(&self) -> bool {
        matches!(self.0, Source::Reader(_) | Source::Lines(_))
    }

    /// A **stable** digest of the stdin *source identity* for cassette keying
    /// (F12) — the payload itself is never persisted (preserving the no-payload
    /// posture), only this hash, so two otherwise-identical invocations that
    /// differ only in their stdin no longer collide on replay. FNV-1a (not `DefaultHasher`,
    /// whose value can change between Rust releases) so a digest recorded today
    /// matches one computed tomorrow. Byte content is hashed verbatim; a file
    /// source hashes its *path* (the file is not read at key time). The one-shot
    /// streaming sources have no fixed content, so they hash a discriminant only
    /// — but the cassette runner rejects them outright (L-1) before this is
    /// keyed, since that discriminant would collide distinct payloads.
    #[cfg(feature = "record")]
    pub(crate) fn content_digest(&self) -> u64 {
        // FNV-1a, 64-bit.
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

    /// Take this source's payload for a single run (D10/M4), or report a one-shot
    /// source already consumed by a previous run.
    ///
    /// One-shot sources ([`from_reader`](Self::from_reader)/
    /// [`from_lines`](Self::from_lines)) are removed from their shared cell here —
    /// **atomically**, under the async lock — so the take and the "already
    /// consumed?" decision are a single step. This closes the TOCTOU where two
    /// concurrent runs of the same cloned source could each pass a separate
    /// `is_consumed`-style check and then have one silently feed the child empty
    /// stdin: a concurrent second run now observes the source taken and fails
    /// loud at launch. Re-runnable sources (bytes/file/empty) clone their
    /// replayable payload. Call once per run, at launch.
    pub(crate) async fn take_for_run(&self) -> Result<TakenStdin, OneShotConsumed> {
        Ok(match &self.0 {
            Source::Empty => TakenStdin::Empty,
            Source::Bytes(bytes) => TakenStdin::Bytes(bytes.clone()),
            Source::File(path) => TakenStdin::File(path.clone()),
            Source::Reader(reader) => match reader.lock().await.take() {
                Some(r) => TakenStdin::Reader(r),
                None => return Err(OneShotConsumed),
            },
            Source::Lines(lines) => match lines.lock().await.take() {
                Some(s) => TakenStdin::Lines(s),
                None => return Err(OneShotConsumed),
            },
        })
    }
}

/// A one-shot streaming stdin source ([`Stdin::from_reader`]/
/// [`Stdin::from_lines`]) whose payload was already consumed by a previous run —
/// returned by [`Stdin::take_for_run`] so the launch path can fail loud (D10).
#[derive(Debug)]
pub(crate) struct OneShotConsumed;

/// A stdin payload taken for one run by [`Stdin::take_for_run`]. It owns its
/// content (a one-shot source has been removed from its shared cell), so writing
/// it is a plain move — there is no second take and so no empty-stdin footgun.
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
    pub async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.sink.write_all(bytes).await
    }

    /// Write `line` followed by `\n` (UTF-8), flushing so the child sees it
    /// promptly.
    pub async fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        self.sink.write_all(line.as_bytes()).await?;
        self.sink.write_all(b"\n").await?;
        self.sink.flush().await
    }

    /// Flush buffered bytes to the child.
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush().await
    }

    /// Close stdin, signalling EOF to the child.
    pub async fn finish(mut self) -> std::io::Result<()> {
        self.sink.shutdown().await
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

    /// Take the source for one run and drive its payload into an in-memory sink,
    /// returning what was written (panics if the one-shot source was consumed).
    async fn written(stdin: &Stdin) -> Vec<u8> {
        let mut sink = Vec::new();
        stdin
            .take_for_run()
            .await
            .unwrap_or_else(|_| panic!("source already consumed"))
            .write_to(&mut sink)
            .await
            .expect("write_to");
        sink
    }

    #[tokio::test]
    async fn reader_source_is_one_shot() {
        // D10/M4: the first `take_for_run` yields the payload; a second take of
        // the same (cloned) source reports it consumed (the launch path turns
        // that into a loud error) rather than silently feeding empty stdin.
        let stdin = Stdin::from_reader(&b"payload"[..]);
        assert_eq!(written(&stdin).await, b"payload");
        assert!(
            stdin.take_for_run().await.is_err(),
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
            stdin.take_for_run().await.is_err(),
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
        // L-3: the contract is "exactly one `\n` after every item, verbatim, no
        // re-splitting." An item that already ends in `\n` yields a blank line;
        // an empty iterator yields no bytes (not a lone newline).
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
            .await
            .unwrap_or_else(|_| panic!("file source is re-runnable"))
            .write_to(&mut sink)
            .await
            .expect_err("a missing stdin file must error, not feed silence");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn empty_source_writes_nothing() {
        assert!(written(&Stdin::empty()).await.is_empty());
    }

    #[tokio::test]
    async fn concurrent_reuse_of_a_one_shot_source_fails_the_loser_atomically() {
        // M4: the take is atomic — when two runs of the same (cloned) one-shot
        // source race, exactly one wins the payload and the other observes it
        // consumed. There is no window where both pass a check and one then
        // silently feeds empty stdin. (The slow copy no longer holds the source
        // lock, so the loser also returns promptly — superseding the old B17
        // "not blocked by a slow first run" guarantee.)
        use std::time::Duration;

        let (_tx, rx) = tokio::io::duplex(64);
        let stdin = Stdin::from_reader(rx);
        let stdin2 = stdin.clone();

        // Run 1 wins the take and parks in the copy (no data, no EOF).
        let run1 = tokio::spawn(async move {
            let taken = stdin.take_for_run().await.expect("run 1 wins the take");
            let mut sink = Vec::new();
            let _ = taken.write_to(&mut sink).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Run 2 must observe the consumed source and finish quickly with an error.
        let second = tokio::time::timeout(Duration::from_secs(2), stdin2.take_for_run()).await;
        let second = second.expect("the loser must not block on the slow winner's copy");
        assert!(
            second.is_err(),
            "the losing concurrent run sees the one-shot source already taken"
        );

        run1.abort();
    }
}
