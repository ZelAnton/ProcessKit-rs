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
    /// can drop the sink to signal EOF.
    pub(crate) async fn write_to(
        &self,
        sink: &mut tokio::process::ChildStdin,
    ) -> std::io::Result<()> {
        match &self.0 {
            Source::Empty => Ok(()),
            Source::Bytes(bytes) => sink.write_all(bytes).await,
            Source::File(path) => {
                let mut file = tokio::fs::File::open(path).await?;
                tokio::io::copy(&mut file, sink).await.map(|_| ())
            }
            Source::Reader(reader) => {
                let mut guard = reader.lock().await;
                match guard.take() {
                    Some(mut r) => tokio::io::copy(&mut r, sink).await.map(|_| ()),
                    None => Ok(()), // already consumed by an earlier run
                }
            }
            Source::Lines(lines) => {
                let mut guard = lines.lock().await;
                match guard.take() {
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
