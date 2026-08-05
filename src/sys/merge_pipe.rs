//! The parent-side reader for a `merge_stderr_in_pipe` stage's shared pipe.
//!
//! `runner::launch` hands such a stage two clones of one anonymous pipe's write
//! end (its stdout *and* its stderr) and keeps the read end; this module turns
//! that read end into the boxed [`OutputReader`](crate::running) the rest of
//! the crate consumes, so the choice of async primitive lives in one place per
//! platform.
//!
//! The read end must not be wrapped in [`tokio::fs::File`] on either platform.
//! Every read there runs on a thread of the runtime's **shared blocking pool**,
//! and dropping the future that awaits it cancels only the wait — the read
//! itself keeps that thread until the pipe delivers data or EOF. A grandchild
//! that inherited the write end (a forking `sh -c '… &'` on Unix, anything the
//! child launched with inherited handles on Windows) holds the pipe open after
//! the direct child exits, so a torn-down run's abandoned read can park one
//! pool thread for as long as that grandchild lives.
//!
//! Both platforms therefore keep the read off that pool, by different means —
//! Windows cannot use the one Unix does:
//!
//! * Unix puts the fd in `O_NONBLOCK` and drives it through tokio's reactor,
//!   mirroring what `AsyncPtyMaster` (`sys/pty/unix.rs`) does for the PTY master
//!   fd. Dropping the reader deregisters the fd and closes it, leaving no read
//!   in flight.
//! * Windows cannot register an anonymous pipe handle with that reactor, so its
//!   read has to block *somewhere*. It blocks on a bridge thread private to this
//!   module — never a thread shared with the rest of the runtime — and dropping
//!   the reader interrupts that thread's `ReadFile` rather than waiting for the
//!   pipe to end.

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
    use std::io::{self, Read};
    use std::os::windows::io::{AsRawHandle, OwnedHandle};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    use std::task::{Context, Poll};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::sync::mpsc;
    use windows_sys::Win32::Foundation::{ERROR_NOT_FOUND, HANDLE};
    use windows_sys::Win32::System::IO::CancelSynchronousIo;
    use windows_sys::Win32::System::Threading::WaitForSingleObject;

    /// How much of the pipe the bridge thread takes per `ReadFile`.
    const CHUNK: usize = 8 * 1024;
    /// How many chunks may sit between the bridge thread and the async side.
    /// The channel is bounded so a child writing faster than the consumer reads
    /// still ends up blocked in its own write, as it was when the parent read
    /// the pipe directly; the bound adds 128 KiB of buffering ahead of that
    /// backpressure and no more.
    const CHUNKS_IN_FLIGHT: usize = 16;
    /// How many times [`Bridge::cancel`] re-attempts `CancelSynchronousIo` while
    /// the bridge thread reports a read it has not yet entered the kernel for
    /// (see that method), and how many of those attempts merely yield before the
    /// rest start sleeping a millisecond each — an upper bound of roughly 24 ms,
    /// spent only in that race, never in the case this cancellation exists for
    /// (a read parked with nothing to read, which the first attempt interrupts).
    const CANCEL_ATTEMPTS: u32 = 32;
    const CANCEL_SPINS: u32 = 8;
    /// How long a dropped reader waits for its bridge thread to leave. The wait
    /// ends as soon as the thread does, so this bound is paid only if the
    /// interrupt above did not take — where waiting longer would not help
    /// either, and blocking the caller for it certainly would not.
    const JOIN_BUDGET_MS: u32 = 100;

    /// The bridge thread is not inside a read: whatever it does next, it reads
    /// [`Bridge::cancelled`] before starting another one.
    const STATE_BETWEEN_READS: u8 = 0;
    /// The bridge thread has committed to a read — it has passed its
    /// cancellation check and is in, or on its way into, `ReadFile`.
    const STATE_READING: u8 = 1;
    /// The bridge thread has left its loop; the pipe's read end is closed.
    const STATE_FINISHED: u8 = 2;

    /// What the bridge thread forwards to the async side.
    #[derive(Debug)]
    enum Message {
        Chunk(Vec<u8>),
        Error(io::Error),
    }

    /// The cancellation contract between a [`BridgeReader`] and its bridge
    /// thread: the async side sets [`Bridge::cancelled`] and interrupts the read
    /// the thread is parked in; the thread publishes enough of its own state
    /// ([`Bridge::state`]) for that interrupt to be aimed correctly.
    ///
    /// Why interrupt the thread rather than close the handle under it: closing
    /// the read end from here would need two owners for one handle, and the
    /// instant `CloseHandle` returns the value is free for any other thread in
    /// the process to be handed for something else — a bridge thread that had
    /// not yet entered its `ReadFile` would then read from whatever took the
    /// value over. `CancelSynchronousIo` leaves the handle owned by exactly one
    /// party (the thread, which closes it on its way out) and is documented for
    /// precisely this job: cancelling a synchronous I/O operation issued by
    /// another thread.
    #[derive(Debug, Default)]
    struct Bridge {
        state: AtomicU8,
        cancelled: AtomicBool,
    }

    impl Bridge {
        /// The bridge thread's body: read the pipe until it ends (or until the
        /// async side is gone), forwarding what it reads over `tx`.
        ///
        /// Takes `source` by value so the read end is closed when this returns —
        /// the thread is its only owner.
        fn pump(&self, mut source: impl Read, tx: mpsc::Sender<Message>) {
            let mut buf = [0u8; CHUNK];
            loop {
                // Publish "about to read" *before* reading the cancel flag,
                // which `cancel` sets before it reads this state. Both orderings
                // are sequentially consistent, so at least one side sees the
                // other: a `cancel` that observes anything but `STATE_READING`
                // is one this thread is guaranteed to observe below, and a
                // `cancel` that observes `STATE_READING` interrupts the read
                // itself.
                self.state.store(STATE_READING, Ordering::SeqCst);
                if self.cancelled.load(Ordering::SeqCst) {
                    // Back to `STATE_BETWEEN_READS` before leaving, so a
                    // `cancel` still in its retry loop stops trying to interrupt
                    // a read that will never be issued.
                    self.state.store(STATE_BETWEEN_READS, Ordering::SeqCst);
                    break;
                }
                let read = source.read(&mut buf);
                self.state.store(STATE_BETWEEN_READS, Ordering::SeqCst);
                match read {
                    // No writer left: the pipe's EOF. (`ERROR_BROKEN_PIPE` is
                    // EOF too; `std` already reports it as `Ok(0)` for a `File`,
                    // and the arm below covers a source that does not.)
                    Ok(0) => break,
                    Ok(read) => {
                        if tx
                            .blocking_send(Message::Chunk(buf[..read].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) if crate::running::is_broken_pipe(&error) => break,
                    // A read interrupted by `cancel` (`ERROR_OPERATION_ABORTED`)
                    // is not a failure worth reporting: the reader that would
                    // have received it is the one being dropped.
                    Err(_) if self.cancelled.load(Ordering::SeqCst) => break,
                    Err(error) => {
                        let _ = tx.blocking_send(Message::Error(error));
                        break;
                    }
                }
            }
        }

        /// Tell the bridge thread to stop and interrupt the read it is parked
        /// in. Returns whether the thread is on its way out, i.e. whether
        /// waiting for it is worth the caller's time.
        ///
        /// The caller must have closed the channel first: a thread blocked in
        /// `blocking_send` is between reads, and only the closed channel ends
        /// that wait.
        ///
        /// `CancelSynchronousIo` reports `ERROR_NOT_FOUND` when the thread has
        /// no synchronous I/O pending, which covers two cases here: the read
        /// already ended (the state re-read at the top of the loop then settles
        /// it), or the thread has passed its cancellation check but has not yet
        /// reached the kernel. The second is a window of a few instructions in a
        /// runnable thread, so the retries below — yields first, then short
        /// sleeps — close it. Exhausting them leaves the thread reading until
        /// its own pipe ends, the behaviour this cancellation replaces, rather
        /// than anything worse.
        fn cancel(&self, thread: HANDLE) -> bool {
            self.cancelled.store(true, Ordering::SeqCst);
            for attempt in 0..CANCEL_ATTEMPTS {
                if self.state.load(Ordering::SeqCst) != STATE_READING {
                    // Between reads (the flag above cannot be missed, see
                    // `pump`) or already finished: nothing to interrupt.
                    return true;
                }
                // SAFETY: `thread` is the bridge thread's handle, borrowed from
                // the `JoinHandle` the caller owns, so it stays valid across
                // this call. `CancelSynchronousIo` only marks that thread's
                // pending I/O; it dereferences nothing of ours.
                if unsafe { CancelSynchronousIo(thread) } != 0 {
                    return true;
                }
                if io::Error::last_os_error().raw_os_error() != Some(ERROR_NOT_FOUND as i32) {
                    // Not the "nothing pending yet" race — retrying cannot help.
                    return false;
                }
                if attempt < CANCEL_SPINS {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            false
        }

        /// Whether the bridge thread has left its loop and closed the read end.
        #[cfg(test)]
        fn is_finished(&self) -> bool {
            self.state.load(Ordering::SeqCst) == STATE_FINISHED
        }
    }

    /// The parent end of a merge pipe: a bridge thread blocks on it, and this
    /// hands what that thread reads to the async caller one chunk at a time.
    ///
    /// The `Receiver` is wrapped in a `Mutex` purely so this is `Sync` (a tokio
    /// mpsc `Receiver` is `Send` but not `Sync`), which keeps the boxed
    /// [`OutputReader`](crate::running) — and thus
    /// [`RunningProcess`](crate::RunningProcess) — `Sync`. The lock is
    /// uncontended (only `poll_read` and `Drop` touch it, and a reader is polled
    /// from one task) and never held across an `.await`.
    #[derive(Debug)]
    struct BridgeReader {
        rx: std::sync::Mutex<mpsc::Receiver<Message>>,
        leftover: Vec<u8>,
        pos: usize,
        bridge: Arc<Bridge>,
        thread: JoinHandle<()>,
    }

    impl BridgeReader {
        /// Hand the parent's read end to a bridge thread of our own.
        ///
        /// `std::io::PipeReader` deliberately exposes its owned OS object rather
        /// than a `File`; both conversions are ownership-preserving and add no
        /// duplicate handle that could delay EOF.
        fn new(pipe: std::io::PipeReader) -> io::Result<Self> {
            let handle: OwnedHandle = pipe.into();
            Self::over(std::fs::File::from(handle))
        }

        /// The body of [`new`](Self::new), over any blocking source the bridge
        /// thread can own. Production passes the pipe's `File`; the tests below
        /// pass a source whose `read` fails, since nothing outside this process
        /// can make a read on the pipe itself fail (a closed write end is EOF,
        /// not an error) and the error branch is worth a test all the same.
        fn over(source: impl Read + Send + 'static) -> io::Result<Self> {
            let (tx, rx) = mpsc::channel(CHUNKS_IN_FLIGHT);
            let bridge = Arc::new(Bridge::default());
            let thread = std::thread::Builder::new()
                .name("processkit-merge-pipe".into())
                .spawn({
                    let bridge = Arc::clone(&bridge);
                    move || {
                        bridge.pump(source, tx);
                        // `pump` owned both the source and the sender, so the
                        // read end is closed and the channel ended by the time
                        // the state says so.
                        bridge.state.store(STATE_FINISHED, Ordering::SeqCst);
                    }
                })?;
            Ok(Self {
                rx: std::sync::Mutex::new(rx),
                leftover: Vec::new(),
                pos: 0,
                bridge,
                thread,
            })
        }
    }

    /// Dropping the reader interrupts the read its bridge thread is parked in,
    /// so the thread closes the pipe and goes instead of waiting out whoever
    /// still holds the write end — the whole reason for owning a thread of our
    /// own rather than borrowing one from the runtime.
    impl Drop for BridgeReader {
        fn drop(&mut self) {
            // Close the channel first: a thread parked in `blocking_send`
            // (a full channel the async side stopped draining) has to be let go
            // before anything waits for it.
            self.rx
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .close();
            let thread = self.thread.as_raw_handle();
            if self.bridge.cancel(thread) {
                // A bounded join: it returns as soon as the thread has closed
                // the read end and gone, so a caller that drops this reader
                // normally hands back a torn-down bridge rather than one still
                // winding up. `WaitForSingleObject` rather than
                // `JoinHandle::join` so a thread that somehow stayed in its read
                // costs a bounded wait instead of hanging the caller forever.
                //
                // SAFETY: as in `Bridge::cancel` — `thread` is borrowed from the
                // `JoinHandle` this reader still owns, and waiting on a thread
                // handle neither consumes nor closes it.
                let _ = unsafe { WaitForSingleObject(thread, JOIN_BUDGET_MS) };
            }
        }
    }

    impl AsyncRead for BridgeReader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pos < this.leftover.len() {
                let start = this.pos;
                let n = (this.leftover.len() - start).min(buf.remaining());
                buf.put_slice(&this.leftover[start..start + n]);
                this.pos += n;
                return Poll::Ready(Ok(()));
            }
            let poll = this
                .rx
                .get_mut()
                .expect("merge pipe reader mutex poisoned")
                .poll_recv(cx);
            match poll {
                Poll::Ready(Some(Message::Chunk(chunk))) => {
                    let n = chunk.len().min(buf.remaining());
                    buf.put_slice(&chunk[..n]);
                    if n < chunk.len() {
                        this.leftover = chunk;
                        this.pos = n;
                    } else {
                        this.leftover.clear();
                        this.pos = 0;
                    }
                    Poll::Ready(Ok(()))
                }
                // A genuine read error goes to the caller unchanged — never
                // reported as a short read or as EOF.
                Poll::Ready(Some(Message::Error(error))) => Poll::Ready(Err(error)),
                // The bridge thread ended: the pipe's EOF, or a teardown that
                // has no reader left to tell anyway.
                Poll::Ready(None) => Poll::Ready(Ok(())),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    pub(super) fn reader(pipe: std::io::PipeReader) -> io::Result<crate::running::OutputReader> {
        Ok(Box::new(BridgeReader::new(pipe)?))
    }

    #[cfg(test)]
    mod tests {
        use std::io::Write;
        use std::time::{Duration, Instant};

        use tokio::io::AsyncReadExt;

        use super::{Bridge, BridgeReader, STATE_READING};

        /// Wait for a fact to become true, up to a cap that is an upper bound on
        /// a broken implementation's patience rather than a guess about a
        /// working one's speed.
        fn poll_until(what: &str, mut ready: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                if ready() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            panic!("timed out waiting for {what}");
        }

        /// A source whose `read` always fails. Nothing a test could do to the
        /// pipe makes its own read fail — closing the write end is EOF, not an
        /// error — so this stands in for a failure to prove the reader hands one
        /// to its caller instead of reporting a short read or EOF.
        #[derive(Debug)]
        struct FailingSource;

        impl std::io::Read for FailingSource {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("merge pipe read failed"))
            }
        }

        /// The regression this file exists for, in miniature: a write end held
        /// open by someone who writes nothing (a grandchild that inherited it
        /// outliving the child we launched) used to pin the reading thread —
        /// and the pipe handle — for as long as that holder lived, however long
        /// ago the run was torn down.
        #[test]
        fn a_dropped_reader_ends_the_bridge_thread_and_closes_the_pipe() {
            let (pipe, mut writer) = std::io::pipe().expect("pipe");
            let reader = BridgeReader::new(pipe).expect("bridge reader");
            let bridge = std::sync::Arc::clone(&reader.bridge);

            // Wait until the thread has committed to a read, so the drop below
            // faces the hard case — a read of a pipe nothing will ever write to
            // — rather than a thread that has not started one.
            poll_until("the bridge thread to enter its read", || {
                bridge.state.load(std::sync::atomic::Ordering::SeqCst) == STATE_READING
            });

            // Drop on a thread of its own, so "the drop itself returns" is a
            // fact this test can assert rather than a hang it would share: the
            // wait inside it has to be bounded whether or not the read ever
            // ends, and the pipe here is one that never will.
            let (dropped, was_dropped) = std::sync::mpsc::channel();
            let dropping = std::thread::spawn(move || {
                drop(reader);
                let _ = dropped.send(());
            });
            was_dropped
                .recv_timeout(Duration::from_secs(10))
                .expect("dropping the reader must not wait on the pipe");
            dropping.join().expect("dropping thread");

            // The bridge thread left and closed the read end — while `writer`,
            // the stand-in for that grandchild, is still open and still silent.
            poll_until("the bridge thread to end", || bridge.is_finished());
            let error = writer
                .write_all(b"x")
                .expect_err("the read end must be closed once the reader is dropped");
            assert!(
                crate::running::is_broken_pipe(&error),
                "expected a broken pipe once the read end is closed, got {error:?}"
            );
        }

        /// The same teardown, raced against the bridge thread's own start rather
        /// than waited into position: the drop may land before that thread has
        /// read the cancel flag, after it has committed to a read, or anywhere
        /// between. Which side wins is not asserted — that every one of them
        /// still ends the thread and closes the pipe is.
        #[test]
        fn a_reader_dropped_at_once_still_ends_the_bridge_thread() {
            let (pipe, mut writer) = std::io::pipe().expect("pipe");
            let reader = BridgeReader::new(pipe).expect("bridge reader");
            let bridge = std::sync::Arc::clone(&reader.bridge);
            drop(reader);

            poll_until("the bridge thread to end", || bridge.is_finished());
            let error = writer
                .write_all(b"x")
                .expect_err("the read end must be closed once the reader is dropped");
            assert!(
                crate::running::is_broken_pipe(&error),
                "expected a broken pipe once the read end is closed, got {error:?}"
            );
        }

        /// The bridge thread is this module's own, so a read waiting on a pipe
        /// nobody writes to must leave the runtime's shared blocking pool free —
        /// the property the `tokio::fs::File` wrapper this replaced could not
        /// offer.
        #[test]
        fn a_pending_read_holds_no_blocking_pool_thread() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let (pipe, _writer) = std::io::pipe().expect("pipe");
                let mut reader = super::reader(pipe).expect("async reader");
                let read_task = tokio::spawn(async move {
                    let mut byte = [0u8; 1];
                    reader.read(&mut byte).await
                });

                // Let the scheduler run that task so its read is genuinely in
                // flight before the probe below is queued behind it. On this
                // single-threaded runtime yielding runs the one ready task; it
                // waits on nothing external, and it is what makes the assertion
                // discriminating — a `tokio::fs::File` reader dispatches its
                // read to the (one) pool thread on that first poll, so the probe
                // then never runs, while this reader's bridge thread is its own.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }

                // The old reader had this read occupying the one pool thread for
                // as long as the pipe stayed open.
                tokio::time::timeout(Duration::from_secs(30), tokio::task::spawn_blocking(|| {}))
                    .await
                    .expect("a parked merge-pipe read must leave the blocking pool free")
                    .expect("blocking probe");

                read_task.abort();
            });
        }

        /// A payload several chunks long arrives byte for byte and ends at the
        /// pipe's EOF — the bridge splits the stream into chunks of its own, so
        /// reassembly is its job to get right.
        #[test]
        fn a_multi_chunk_payload_arrives_verbatim() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let payload: Vec<u8> = (0..super::CHUNK * 3 + 17)
                    .map(|i| u8::try_from(i % 251).expect("byte"))
                    .collect();
                let (pipe, mut writer) = std::io::pipe().expect("pipe");
                let mut reader = super::reader(pipe).expect("async reader");
                let written = payload.clone();
                let writing = std::thread::spawn(move || {
                    writer.write_all(&written).expect("write payload");
                    // Dropping the last write end is the EOF the read below ends
                    // on.
                });

                let mut output = Vec::new();
                reader
                    .read_to_end(&mut output)
                    .await
                    .expect("read the payload");
                writing.join().expect("writer thread");
                assert_eq!(output, payload);
            });
        }

        /// A failing read reaches the caller as an error, not as EOF.
        #[test]
        fn a_failing_read_reaches_the_caller() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let mut reader = BridgeReader::over(FailingSource).expect("bridge reader");
                let mut buf = [0u8; 8];
                let error = reader
                    .read(&mut buf)
                    .await
                    .expect_err("a failing read must not be reported as EOF");
                assert_eq!(error.to_string(), "merge pipe read failed");
            });
        }

        /// A bridge that is not in a read is cancelled by the flag alone: it is
        /// reported as leaving, and no interrupt is issued at all. The null
        /// handle is what pins that second half — reaching the FFI with it would
        /// be a `CancelSynchronousIo` on nothing, whose failure would flip the
        /// answer this asserts.
        #[test]
        fn cancelling_a_bridge_between_reads_needs_no_interrupt() {
            let bridge = Bridge::default();
            assert!(bridge.cancel(std::ptr::null_mut()));
        }
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

    /// Both platforms' readers deliver the child's bytes verbatim and end at the
    /// pipe's EOF; what each one does with the thread the read would otherwise
    /// hold — and how a dropped reader ends it and closes the pipe, along with
    /// surfacing read errors — is covered by whichever `imp` is compiled, in its
    /// own tests.
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
