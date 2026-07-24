//! Unix PTY backend: `openpty` + the pty **slave** wired as the child's stdio,
//! spawned through the *existing* per-platform containment path (K-032), so the
//! child lands in the same cgroup / process group as any other run. The master
//! is retained (as the merged reader and the stdin writer); terminal echo is
//! disabled so a written secret is not echoed back into the merged output.

use std::io;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::io::AsRawFd;
use std::os::unix::process::CommandExt;
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::process::{Child, Command};

use crate::sys::SpawnOptions;
use crate::sys::pid_gate::PidGate;

use super::{EofOnEio, PtyExitStatus, PtyReader, PtySpawn, PtyWriter};

/// A PTY child on Unix is an ordinary [`tokio::process::Child`] (the `openpty`
/// slave is its stdio), so the whole reap/kill lifecycle is the same as a real
/// pipe-spawned child — only the I/O wiring differs.
pub(crate) struct PtyChild {
    child: Child,
    /// A dedicated dup of the pty **master** kept solely for the live-resize
    /// `TIOCSWINSZ` ioctl (see [`resize`](Self::resize)). It is a plain owned fd,
    /// never registered with the reactor — the ioctl needs only a valid master
    /// fd, not readiness gating, so it does not touch the `AsyncFd`-driven
    /// reader/writer (K-072) and cannot conflict with their read/write
    /// registration. Sharing the master's open file description with the
    /// reader/writer, it does not alter end-of-session semantics: the merged
    /// reader still sees `EIO`/EOF the instant the slave closes (child exit),
    /// which is independent of how many master dups remain open.
    resize_fd: OwnedFd,
}

impl PtyChild {
    /// The child's pid, or `None` once reaped.
    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// Resize the pseudo-terminal to `cols`×`rows` via `TIOCSWINSZ` on the master
    /// fd. The kernel updates the terminal's window size and delivers `SIGWINCH`
    /// to the child's foreground process group, so a size-aware child (a TUI, a
    /// pager) re-renders for the new geometry — the live-resize half of
    /// [`Command::pty_size`](crate::Command::pty_size).
    ///
    /// `&self`: the ioctl mutates no Rust-side state, only the kernel's tty. The
    /// caller ([`RunningProcess::resize_pty`](crate::RunningProcess)) has already
    /// gated on the child still running, so this never runs against a torn-down
    /// session.
    pub(crate) fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
        let winsize = libc::winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: `resize_fd` is a valid, owned pty master fd for the call's
        // duration; `TIOCSWINSZ` reads the window size through the trailing
        // `*const winsize`, which points at a fully-initialised local. The request
        // constant is coerced to whatever integer type this target's `ioctl`
        // signature expects (`c_ulong` on glibc/BSD/Apple, `c_int` on musl).
        let rc = unsafe {
            libc::ioctl(
                self.resize_fd.as_raw_fd(),
                libc::TIOCSWINSZ as _,
                &raw const winsize,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Gated reap: poll the child's exit under the [`PidGate`] so the pid-freeing
    /// reap and the gate's retire are one indivisible step — identical to the
    /// pipe path's `gated_reap`, so a detached watchdog's raw `kill(pid)` can
    /// never land on a pid this reap just freed and the OS recycled.
    pub(crate) async fn reap(&mut self, gate: &PidGate) -> io::Result<PtyExitStatus> {
        use std::future::Future;
        let mut wait = std::pin::pin!(self.child.wait());
        let status = std::future::poll_fn(|cx| {
            let mut out = std::task::Poll::Pending;
            gate.reap_under_lock(|| match wait.as_mut().poll(cx) {
                std::task::Poll::Ready(res) => {
                    out = std::task::Poll::Ready(res);
                    true
                }
                std::task::Poll::Pending => false,
            });
            out
        })
        .await?;
        Ok(PtyExitStatus::from_std(status))
    }

    /// A plain (non-gated) exit wait — used post-kill, where the caller already
    /// retired the gate.
    pub(crate) async fn wait(&mut self) -> io::Result<PtyExitStatus> {
        self.child.wait().await.map(PtyExitStatus::from_std)
    }

    /// Non-blocking exit poll.
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<PtyExitStatus>> {
        Ok(self.child.try_wait()?.map(PtyExitStatus::from_std))
    }

    /// Send `SIGKILL` to the direct child through the owned handle.
    pub(crate) fn start_kill(&mut self) -> io::Result<()> {
        self.child.start_kill()
    }
}

/// Disable terminal echo (and the related echo flags) on `fd` so a secret written
/// to the master's input side is not echoed back into the merged output. Applied
/// to the slave before the child inherits it, so the child sees echo-off from its
/// first read. The Windows ConPTY has no portable per-write echo control, so this
/// guarantee is Unix-only (documented on [`Command::use_pty`](crate::Command::use_pty)).
fn disable_echo(fd: &OwnedFd) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    // SAFETY: `termios` is a plain C struct; zeroed then fully populated by
    // `tcgetattr` before use. `raw` is a valid, open pty fd for the call's duration.
    let mut termios: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(raw, &mut termios) } != 0 {
        return Err(io::Error::last_os_error());
    }
    termios.c_lflag &= !(libc::ECHO | libc::ECHOE | libc::ECHOK | libc::ECHONL);
    // SAFETY: `termios` is a fully-initialised struct read back from `tcgetattr`
    // with only the echo bits cleared; `TCSANOW` applies it immediately.
    if unsafe { libc::tcsetattr(raw, libc::TCSANOW, &termios) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Open a pseudo-terminal sized `cols`×`rows`, returning the (master, slave) fds
/// as owned handles.
fn open_pty(cols: u16, rows: u16) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    // The child's initial window size ([`Command::pty_size`], default 80×24 — a
    // zero size makes some TUI tools misbehave). Live-resizable afterwards via
    // `TIOCSWINSZ` (see `PtyChild::resize`). `mut` because the `winp` parameter of
    // `openpty` is `*const winsize` only on glibc; on the BSD/Apple libc it is
    // `*mut winsize`, so a `&mut` is required to satisfy every target.
    let mut winsize = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: `openpty` writes the two fds through the out-pointers; the name
    // buffer is null (we don't want the slave name), the termios is null (default
    // line discipline), and a valid winsize is supplied.
    //
    // `&mut winsize` (not `&winsize`) because `openpty`'s `winp` is `*mut winsize`
    // on the BSD/Apple libc (macos/ios/*bsd) and only `*const winsize` on glibc.
    // On glibc that shared-only use trips clippy's `unnecessary_mut_passed`; the
    // targeted `allow` silences it there and is simply not needed (and does not
    // warn) on the platforms where the `mut` is load-bearing.
    #[allow(clippy::unnecessary_mut_passed)]
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut::<libc::termios>(),
            &mut winsize,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both fds were just created by `openpty` and are owned by us now.
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    Ok((master, slave))
}

/// Put `fd` into non-blocking mode (`O_NONBLOCK`) so every `read`/`write` on the
/// master either makes progress at once or returns `EWOULDBLOCK` — the
/// precondition for driving it through the reactor rather than a blocking-pool
/// thread. The flag lives on the shared *open file description*, so a dup (the
/// reader/writer split below) inherits it; setting it on each dup is idempotent.
/// Only the master is touched — the slave (the child's tty) stays blocking, so
/// the child sees an ordinary terminal.
fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    // SAFETY: `F_GETFL`/`F_SETFL` on a valid, open fd; the call reads/writes only
    // the fd's status flags, no memory through a pointer.
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

/// The Unix PTY master fd, driven through tokio's reactor via [`AsyncFd`] instead
/// of the blocking pool.
///
/// The fd is `O_NONBLOCK`, so each `read`/`write` either makes progress at once
/// or returns `EWOULDBLOCK`; on the latter the reactor parks the task until the
/// master is readable/writable again. This replaces the previous
/// `tokio::fs::File` wrapper, which drove every read/write through a
/// blocking-pool thread — acceptable for a single low-volume session, but a
/// thread-per-session tax under the dozens-of-concurrent-PTY orchestration
/// workload this mode targets.
///
/// The inner [`std::fs::File`] owns the fd (closed on drop — [`AsyncFd`]
/// deregisters it from the reactor first) and supplies the actual
/// `read(2)`/`write(2)` through `&File`'s [`Read`]/[`Write`] impls; `AsyncFd`
/// contributes only the readiness gating.
#[derive(Debug)]
struct AsyncPtyMaster {
    master: AsyncFd<std::fs::File>,
}

impl AsyncPtyMaster {
    /// Wrap an owned pty master fd for reactor-driven, non-blocking I/O.
    ///
    /// Must run inside a tokio runtime — [`AsyncFd::new`] registers the fd with
    /// the current reactor. `spawn_pty` is called from the async launch path, so
    /// that context is always present.
    fn new(fd: OwnedFd) -> io::Result<Self> {
        set_nonblocking(&fd)?;
        Ok(Self {
            master: AsyncFd::new(std::fs::File::from(fd))?,
        })
    }
}

impl AsyncRead for AsyncPtyMaster {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            let mut guard = match this.master.poll_read_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            // `initialize_unfilled` zeroes the tail we hand to `read(2)`, so the
            // read is sound without unsafe. A short read simply fills less of it.
            let unfilled = buf.initialize_unfilled();
            match guard.try_io(|inner| {
                let mut file: &std::fs::File = inner.get_ref();
                file.read(unfilled)
            }) {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                // A genuine read error (including the end-of-session `EIO` the
                // `EofOnEio` wrapper turns into a clean EOF) surfaces unchanged.
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                // `WouldBlock`: `try_io` consumed the readiness, so loop to re-arm
                // the reactor wait — the next `poll_read_ready` returns `Pending`.
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsyncWrite for AsyncPtyMaster {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            let mut guard = match this.master.poll_write_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|inner| {
                let mut file: &std::fs::File = inner.get_ref();
                file.write(data)
            }) {
                Ok(result) => return Poll::Ready(result),
                // `WouldBlock`: readiness consumed, loop to re-arm the wait.
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // A pty master holds no user-space write buffer — `write(2)` hands bytes
        // straight to the tty line discipline — so there is nothing to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // A pty master has no half-close: EOF to the child comes from *closing*
        // the writer's fd (on drop), not a shutdown gesture. Mirror the previous
        // `tokio::fs::File` writer, whose shutdown was likewise a no-op (the fd
        // closes on drop); `finish()` on a Unix PTY stays best-effort.
        Poll::Ready(Ok(()))
    }
}

/// Spawn `cmd` under a pseudo-terminal, wiring the slave as its stdio and running
/// the actual spawn through `spawn` (the platform's normal containment path), so
/// the child is contained identically to a pipe-spawned run.
///
/// `spawn` is the per-platform `Job::spawn` (cgroup / process-group join in
/// `pre_exec`), passed as a closure so this shared Unix code reuses each
/// backend's containment without forking a parallel structure.
pub(crate) fn spawn_pty<F>(cmd: &mut Command, opts: &SpawnOptions, spawn: F) -> io::Result<PtySpawn>
where
    F: FnOnce(&mut Command, &SpawnOptions) -> io::Result<Child>,
{
    let (cols, rows) = opts.pty_size.unwrap_or(super::DEFAULT_PTY_SIZE);
    let (master, slave) = open_pty(cols, rows)?;
    disable_echo(&slave)?;

    // The child needs the slave on all three of stdin/stdout/stderr, and
    // `Stdio::from` consumes one owned fd each, so dup the slave twice. All three
    // are moved into the child's stdio and closed in the parent after spawn, so
    // the parent retains no slave fd — essential, or a lingering slave would keep
    // the master from ever seeing EOF when the child exits.
    let slave_out = slave.try_clone()?;
    let slave_err = slave.try_clone()?;
    cmd.stdin(Stdio::from(slave));
    cmd.stdout(Stdio::from(slave_out));
    cmd.stderr(Stdio::from(slave_err));

    // A terminal sends SIGWINCH only to its foreground process group. Make this
    // child a session leader and acquire the slave as its controlling terminal;
    // the session's initial process group is then foreground by default. A normal
    // pipe spawn deliberately keeps its existing session behavior, so this is
    // local to PTY launches.
    let mut pty_opts = *opts;
    if !pty_opts.setsid {
        // SAFETY: the closure calls only setsid() and reads errno, both of which
        // are async-signal-safe in the post-fork child.
        unsafe {
            cmd.as_std_mut().pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    pty_opts.setsid = true;
    // `build_tokio` registers a requested setsid hook before this one; when PTY
    // mode supplied it above, that hook was likewise registered first. std runs
    // user hooks in registration order, so fd 0 is claimed only after setsid().
    // SAFETY: fd 0 is the pty slave after std has installed child stdio; ioctl
    // reads no Rust memory and is async-signal-safe.
    unsafe {
        cmd.as_std_mut().pre_exec(|| {
            if libc::ioctl(0, libc::TIOCSCTTY as _, 0) == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }

    // Spawn through the caller's containment path (cgroup/process-group join
    // happens in that path's pre_exec hooks); the forced setsid group has pgid
    // equal to pid, which the process-group tracker handles identically.
    let child = spawn(cmd, &pty_opts)?;
    let pid = child.id();

    // The reader and writer each own a dup of the master (same open description),
    // so the pump can read the merged output while stdin is written concurrently;
    // both dropping closes the master. Each dup is driven through the reactor via
    // `AsyncFd` in non-blocking mode (no blocking-pool thread per session), so
    // many concurrent PTY sessions scale on the reactor instead of taxing the
    // pool with a read+write thread apiece. The `EofOnEio` wrapper is unchanged —
    // the end-of-session `EIO` still surfaces from `poll_read` as a clean EOF.
    let master_w = master.try_clone()?;
    // A third dup, retained by `PtyChild` solely for the live-resize ioctl. It
    // stays a plain blocking-view owned fd (never wrapped in `AsyncFd`); sharing
    // the master's open file description it inherits the `O_NONBLOCK` the
    // reader/writer set, which is irrelevant to an ioctl.
    let master_resize = master.try_clone()?;
    let reader: PtyReader = Box::new(EofOnEio(AsyncPtyMaster::new(master)?));
    let writer: PtyWriter = Box::new(AsyncPtyMaster::new(master_w)?);

    Ok(PtySpawn {
        child: PtyChild {
            child,
            resize_fd: master_resize,
        },
        reader,
        writer,
        pid,
    })
}
