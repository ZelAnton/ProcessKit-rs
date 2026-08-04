//! The parent-side reader for a `merge_stderr_in_pipe` stage's shared pipe.
//!
//! `runner::launch` hands such a stage two clones of one anonymous pipe's write
//! end (its stdout *and* its stderr) and keeps the read end; this module turns
//! that read end into the boxed [`OutputReader`](crate::running) the rest of
//! the crate consumes, so the choice of async primitive lives in one place per
//! platform.
//!
//! The read end must not be wrapped in [`tokio::fs::File`] on Unix. Every read
//! there runs on a thread of the runtime's **shared blocking pool**, and
//! dropping the future that awaits it cancels only the wait — the `read(2)`
//! itself keeps that thread until the pipe delivers data or EOF. A grandchild
//! that inherited the write end (a forking `sh -c '… &'`) holds the pipe open
//! after the direct child exits, so a torn-down run's abandoned read can park
//! one pool thread for as long as that grandchild lives. Unix therefore drives
//! the fd through tokio's reactor instead, mirroring what `AsyncPtyMaster`
//! (`sys/pty/unix.rs`) does for the PTY master fd. Windows keeps the previous
//! `tokio::fs::File` wrapper — see its `imp` below.

use std::io;

use crate::running::OutputReader;

#[cfg(unix)]
mod imp {
    use std::io::{self, Read};
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::unix::AsyncFd;
    use tokio::io::{AsyncRead, ReadBuf};

    /// Put the parent's read end into `O_NONBLOCK` so every `read(2)` on it
    /// either makes progress at once or returns `EWOULDBLOCK` — the
    /// precondition for driving it through the reactor rather than a
    /// blocking-pool thread. Only this end is touched: the write end the child
    /// (and anything it forks) inherits is a separate open file description and
    /// stays blocking, so the child's own writes behave exactly as before.
    fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
        let raw = fd.as_raw_fd();
        // SAFETY: `F_GETFL`/`F_SETFL` on a valid, open fd; the call reads/writes
        // only the fd's status flags, no memory through a pointer.
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: as above; `flags` is the value just read back from `F_GETFL`.
        if unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// The merge pipe's read end, driven through tokio's reactor via
    /// [`AsyncFd`] instead of the blocking pool.
    ///
    /// The fd is `O_NONBLOCK`, so each `read` either makes progress at once or
    /// returns `EWOULDBLOCK`; on the latter the reactor parks the task until
    /// the pipe is readable again, holding no thread meanwhile. Dropping this
    /// reader deregisters the fd and closes it there and then, with no
    /// in-flight `read(2)` left to outlive it — and that is exactly what
    /// aborting the task holding it comes to (the downstream stage's stdin
    /// relay, the one consumer the pipeline plumbing hands it to).
    ///
    /// `S` is the owned object the fd lives in. Production always instantiates
    /// it with [`std::fs::File`] (see [`AsyncPipeReader::new`], the only
    /// constructor outside tests), which owns the fd and supplies the actual
    /// `read(2)` through `&File`'s [`Read`] impl; [`AsyncFd`] contributes only
    /// the readiness gating. It is a type parameter rather than that one type
    /// so the unit tests below can substitute a source that fails its `read`:
    /// nothing outside this process can make a `read` on the pipe itself fail
    /// (a closed write end is EOF, not an error), and the error branch is worth
    /// a test all the same.
    #[derive(Debug)]
    struct AsyncPipeReader<S: AsRawFd> {
        source: AsyncFd<S>,
    }

    impl AsyncPipeReader<std::fs::File> {
        /// Wrap the parent end of a merge pipe for reactor-driven, non-blocking
        /// reads.
        ///
        /// Must run inside a tokio runtime — [`AsyncFd::new`] registers the fd
        /// with the current reactor. `launch` is async, so that context is
        /// always present.
        fn new(pipe: std::io::PipeReader) -> io::Result<Self> {
            // `std::io::PipeReader` deliberately exposes its owned OS object
            // rather than a `File`; both conversions are ownership-preserving
            // and add no duplicate fd that could delay EOF.
            let fd = OwnedFd::from(pipe);
            set_nonblocking(&fd)?;
            Ok(Self {
                source: AsyncFd::new(std::fs::File::from(fd))?,
            })
        }
    }

    impl<S> AsyncRead for AsyncPipeReader<S>
    where
        S: AsRawFd + Unpin,
        for<'a> &'a S: Read,
    {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            loop {
                let mut guard = match this.source.poll_read_ready(cx) {
                    Poll::Ready(Ok(guard)) => guard,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                };
                // `initialize_unfilled` zeroes the tail handed to `read(2)`, so
                // the read is sound without unsafe. A short read simply fills
                // less of it; a zero-length read is the pipe's EOF, which
                // `advance(0)` reports as the empty filled buffer `AsyncRead`
                // defines EOF to be.
                let unfilled = buf.initialize_unfilled();
                match guard.try_io(|inner| {
                    let mut source: &S = inner.get_ref();
                    source.read(unfilled)
                }) {
                    Ok(Ok(read)) => {
                        buf.advance(read);
                        return Poll::Ready(Ok(()));
                    }
                    // A genuine read error goes to the caller unchanged — never
                    // reported as a short read or as EOF.
                    Ok(Err(e)) => return Poll::Ready(Err(e)),
                    // `WouldBlock`: `try_io` consumed the readiness, so loop to
                    // re-arm the reactor wait — the next `poll_read_ready`
                    // returns `Pending`.
                    Err(_would_block) => continue,
                }
            }
        }
    }

    pub(super) fn reader(pipe: std::io::PipeReader) -> io::Result<crate::running::OutputReader> {
        Ok(Box::new(AsyncPipeReader::new(pipe)?))
    }

    #[cfg(test)]
    mod tests {
        use std::io::Write;
        use std::os::fd::{AsRawFd, OwnedFd, RawFd};
        use std::time::Duration;

        use tokio::io::AsyncReadExt;
        use tokio::io::unix::AsyncFd;

        use super::AsyncPipeReader;

        /// A single-threaded runtime whose blocking pool has exactly **one**
        /// thread: any test below that probes the pool proves the merge-pipe
        /// reader is not sitting in it.
        fn one_blocking_thread_runtime() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()
                .expect("test runtime")
        }

        /// A source whose fd is readable (a pipe read end with data waiting) but
        /// whose `read` always fails. Nothing this test could do to the pipe
        /// makes its own `read(2)` fail — closing the write end is EOF, not an
        /// error — so this stands in for a failure to prove the reader hands one
        /// to its caller instead of reporting a short read or EOF.
        #[derive(Debug)]
        struct FailingSource(std::fs::File);

        impl AsRawFd for FailingSource {
            fn as_raw_fd(&self) -> RawFd {
                self.0.as_raw_fd()
            }
        }

        impl std::io::Read for &FailingSource {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("merge pipe read failed"))
            }
        }

        #[test]
        fn a_pending_read_holds_no_blocking_pool_thread() {
            let runtime = one_blocking_thread_runtime();
            runtime.block_on(async {
                let (reader, _writer) = std::io::pipe().expect("pipe");
                let mut reader = super::reader(reader).expect("async reader");
                let read_task = tokio::spawn(async move {
                    let mut byte = [0u8; 1];
                    reader.read(&mut byte).await
                });

                // Let the read reach its pending state while the writer is still
                // open — the shape a grandchild holding the write end creates.
                // The sleep only yields to the single-threaded scheduler; it
                // waits on no external event.
                tokio::time::sleep(Duration::from_millis(50)).await;

                // The old `tokio::fs::File` reader had this read occupying the
                // one worker for as long as the pipe stayed open.
                tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(|| {}))
                    .await
                    .expect("a parked merge-pipe read must leave the blocking pool free")
                    .expect("blocking probe");

                read_task.abort();
            });
        }

        #[test]
        fn aborting_a_pending_read_closes_the_parent_end() {
            let runtime = one_blocking_thread_runtime();
            runtime.block_on(async {
                let (reader, mut writer) = std::io::pipe().expect("pipe");
                let mut reader = super::reader(reader).expect("async reader");
                let read_task = tokio::spawn(async move {
                    let mut byte = [0u8; 1];
                    reader.read(&mut byte).await
                });
                // As above: hand the scheduler the spawned task so its read is
                // genuinely parked before the abort.
                tokio::time::sleep(Duration::from_millis(50)).await;

                read_task.abort();
                assert!(
                    read_task
                        .await
                        .expect_err("the read task must be aborted")
                        .is_cancelled()
                );

                // The aborted future dropped the reader, which closed the fd:
                // the write end now has no reader at all. (Rust ignores SIGPIPE,
                // so this surfaces as `EPIPE` rather than killing the process.)
                let error = writer
                    .write_all(b"x")
                    .expect_err("the read end must be closed once the reader is dropped");
                assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);

                // The pool is free again too — nothing was left running there.
                tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(|| {}))
                    .await
                    .expect("an aborted merge-pipe read must leave the blocking pool free")
                    .expect("blocking probe");
            });
        }

        #[test]
        fn a_failing_read_reaches_the_caller() {
            let runtime = one_blocking_thread_runtime();
            runtime.block_on(async {
                let (reader, mut writer) = std::io::pipe().expect("pipe");
                let file = std::fs::File::from(OwnedFd::from(reader));
                let mut reader = AsyncPipeReader {
                    source: AsyncFd::new(FailingSource(file)).expect("register with the reactor"),
                };
                // Written after the registration, so the reactor sees a fresh
                // readable edge and the poll below reaches the `read` at all.
                writer.write_all(b"payload").expect("prime the pipe");

                let mut buf = [0u8; 8];
                let error = reader
                    .read(&mut buf)
                    .await
                    .expect_err("a failing read must not be reported as EOF");
                assert_eq!(error.to_string(), "merge pipe read failed");
            });
        }
    }
}

#[cfg(windows)]
mod imp {
    use std::io;
    use std::os::windows::io::OwnedHandle;

    /// Windows keeps the ownership-preserving [`tokio::fs::File`] wrapper this
    /// pipe has always used: an anonymous pipe handle cannot be registered with
    /// tokio's reactor, so cancelling a read on it needs a dedicated bridge
    /// thread that owns the handle and can be told to close it — a distinct
    /// ownership contract, deliberately not improvised here. Reads therefore
    /// still occupy a blocking-pool thread while the pipe is open, unchanged
    /// from before the Unix half moved to the reactor.
    ///
    /// `std::io::PipeReader` deliberately exposes its owned OS object rather
    /// than a `File`; both conversions are ownership-preserving and add no
    /// duplicate handle that could delay EOF.
    pub(super) fn reader(pipe: std::io::PipeReader) -> io::Result<crate::running::OutputReader> {
        let handle: OwnedHandle = pipe.into();
        Ok(Box::new(tokio::fs::File::from_std(std::fs::File::from(
            handle,
        ))))
    }
}

/// Turn the parent end of a merge pipe into the boxed async reader the crate
/// uses for child output.
pub(crate) fn reader(pipe: std::io::PipeReader) -> io::Result<OutputReader> {
    imp::reader(pipe)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tokio::io::AsyncReadExt;

    /// Both platforms' readers deliver the child's bytes verbatim and end at
    /// the pipe's EOF; the Unix-only behaviours (no blocking-pool thread, fd
    /// closed on abort, read errors surfaced) are covered in `imp`'s own tests.
    #[test]
    fn merged_pipe_reader_preserves_bytes_and_eof() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let (reader, mut writer) = std::io::pipe().expect("pipe");
            let mut reader = super::reader(reader).expect("async reader");
            writer
                .write_all(b"stdout\nstderr\n")
                .expect("write merged output");
            drop(writer);

            let mut output = Vec::new();
            reader
                .read_to_end(&mut output)
                .await
                .expect("read merged output");
            assert_eq!(output, b"stdout\nstderr\n");
        });
    }
}
