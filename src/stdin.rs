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
/// [`from_lines`](Self::from_lines)) are one-shot: a cloned
/// [`Command`](crate::Command) reusing them sees an empty stdin on the second run.
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

    /// A **stable** digest of the stdin *content* for cassette keying (F12) —
    /// the content itself is never persisted (preserving the no-payload posture),
    /// only this hash, so two otherwise-identical invocations that differ only in
    /// their stdin no longer collide on replay. FNV-1a (not `DefaultHasher`,
    /// whose value can change between Rust releases) so a digest recorded today
    /// matches one computed tomorrow. Byte content is hashed verbatim; a file
    /// source hashes its *path* (the file is not read at key time); the one-shot
    /// streaming sources have no fixed content, so they hash a discriminant only
    /// (they cannot be faithfully recorded/replayed regardless).
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

    /// Write this source to the child's stdin pipe, then return so the caller
    /// can drop the sink to signal EOF. (Generic over the sink so the one-shot
    /// semantics are unit-testable against an in-memory writer.)
    pub(crate) async fn write_to<W>(&self, sink: &mut W) -> std::io::Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        match &self.0 {
            Source::Empty => Ok(()),
            Source::Bytes(bytes) => sink.write_all(bytes).await,
            Source::File(path) => {
                let mut file = tokio::fs::File::open(path).await?;
                tokio::io::copy(&mut file, sink).await.map(|_| ())
            }
            Source::Reader(reader) => {
                // B17: take the reader out and release the lock *before* the
                // copy — the guard temporary drops at the end of this statement.
                // A concurrent second run then sees `None` (consumed) and gets
                // prompt EOF instead of blocking for the whole copy.
                let taken = reader.lock().await.take();
                match taken {
                    Some(mut r) => tokio::io::copy(&mut r, sink).await.map(|_| ()),
                    None => Ok(()), // already consumed by an earlier run
                }
            }
            Source::Lines(lines) => {
                // B17: release the lock before draining the stream (same as
                // `Reader` above) so a concurrent run isn't held for the whole
                // stream lifetime.
                let taken = lines.lock().await.take();
                match taken {
                    Some(mut stream) => {
                        while let Some(line) = stream.next().await {
                            sink.write_all(line.as_bytes()).await?;
                            sink.write_all(b"\n").await?;
                        }
                        Ok(())
                    }
                    None => Ok(()),
                }
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
/// Available from [`RunningProcess::standard_input`](crate::RunningProcess::standard_input)
/// when the command was built with
/// [`Command::keep_stdin_open`](crate::Command::keep_stdin_open). Write
/// incrementally, then call [`finish`](Self::finish) to send EOF — dropping the
/// writer (or the process handle) without finishing closes stdin too.
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

    /// Drive `write_to` into an in-memory sink and return what was written.
    async fn written(stdin: &Stdin) -> Vec<u8> {
        let mut sink = Vec::new();
        stdin.write_to(&mut sink).await.expect("write_to");
        sink
    }

    #[tokio::test]
    async fn reader_source_is_one_shot() {
        let stdin = Stdin::from_reader(&b"payload"[..]);
        assert_eq!(written(&stdin).await, b"payload");
        assert!(
            written(&stdin).await.is_empty(),
            "a second run must see empty stdin — the reader was consumed"
        );
    }

    #[tokio::test]
    async fn lines_source_is_one_shot_and_newline_terminated() {
        let stdin = Stdin::from_lines(tokio_stream::iter(vec![
            "first".to_owned(),
            "second".to_owned(),
        ]));
        assert_eq!(written(&stdin).await, b"first\nsecond\n");
        assert!(
            written(&stdin).await.is_empty(),
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
    async fn missing_file_surfaces_not_found() {
        let stdin = Stdin::from_file("processkit-definitely-missing-424242.txt");
        let mut sink = Vec::new();
        let err = stdin
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
    async fn second_run_is_not_blocked_by_a_slow_first_run() {
        // B17: while one run is parked mid-copy on a slow reader, a concurrent
        // second run on the same (cloned) source must see it already taken and
        // return promptly — not block on the source mutex for the whole copy.
        // Before the fix, the guard was held across the copy and this would hang.
        use std::time::Duration;

        // A reader whose data never arrives and never EOFs: the writer parks.
        let (_tx, rx) = tokio::io::duplex(64);
        let stdin = Stdin::from_reader(rx);
        let stdin2 = stdin.clone();

        // Run 1 takes the reader and parks in the copy (no data, no EOF).
        let run1 = tokio::spawn(async move {
            let mut sink = Vec::new();
            let _ = stdin.write_to(&mut sink).await;
        });
        // Let run 1 win the take.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Run 2 must observe the consumed source and finish quickly.
        let mut sink2 = Vec::new();
        let second =
            tokio::time::timeout(Duration::from_secs(2), stdin2.write_to(&mut sink2)).await;
        assert!(
            second.is_ok(),
            "the second run must not block on the source mutex held by the slow first run"
        );
        assert!(
            sink2.is_empty(),
            "the second run sees the already-consumed source"
        );

        run1.abort();
    }
}
