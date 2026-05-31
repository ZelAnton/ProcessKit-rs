//! Standard-input sources for a [`Command`](crate::Command).

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::io::AsyncWriteExt;

/// What to feed a child process on standard input.
///
/// A subset of the .NET `StandardInput` hierarchy: the streaming/interactive
/// sources are deferred. When a command has no `Stdin` (or [`Stdin::empty`]),
/// stdin is closed immediately so the child sees EOF at start.
#[derive(Debug, Clone)]
pub struct Stdin(Source);

#[derive(Debug, Clone)]
enum Source {
    /// Stdin is closed at start (EOF).
    Empty,
    /// A fixed byte payload written to stdin, then EOF.
    Bytes(Vec<u8>),
    /// The contents of a file streamed to stdin, then EOF.
    File(PathBuf),
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
        }
    }
}
