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

/// Read the terminal's configured canonical EOF character instead of assuming
/// the usual Ctrl-D byte; callers can customize termios defaults system-wide.
fn terminal_eof(fd: &OwnedFd) -> io::Result<u8> {
    let raw = fd.as_raw_fd();
    // SAFETY: `termios` is fully populated by `tcgetattr` before its VEOF slot is
    // read, and `raw` remains owned for the duration of the call.
    let mut termios: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(raw, &mut termios) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(termios.c_cc[libc::VEOF])
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
    eof_byte: Option<u8>,
    eof_written: usize,
}

/// Clone one of the PTY master's ownership-bearing fds. Keeping the operation
/// behind one helper gives tests a deterministic failure seam at a genuinely
/// fallible post-spawn step without changing the production ownership flow. See
/// the `sys::fault_injection` module (test builds only, hence the bare
/// reference — an intra-doc link to a `cfg(test)` item breaks the rustdoc build).
fn clone_master(fd: &OwnedFd, target: &'static str) -> io::Result<OwnedFd> {
    // The label only exists to name a fault; production never reads it.
    #[cfg(not(test))]
    let _ = target;
    #[cfg(test)]
    if let Some(error) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::PtyMasterClone,
        target,
    ) {
        return Err(error);
    }
    fd.try_clone()
}

impl AsyncPtyMaster {
    /// Wrap an owned pty master fd for reactor-driven, non-blocking I/O.
    ///
    /// Must run inside a tokio runtime — [`AsyncFd::new`] registers the fd with
    /// the current reactor. `spawn_pty` is called from the async launch path, so
    /// that context is always present.
    ///
    /// `target` names this registration (`reader` / `writer`) for the same
    /// test-only fault seam [`clone_master`] uses — the reactor registration is
    /// the other genuinely fallible step after the child already exists.
    fn new(fd: OwnedFd, eof_byte: Option<u8>, target: &'static str) -> io::Result<Self> {
        #[cfg(not(test))]
        let _ = target;
        set_nonblocking(&fd)?;
        // Take ownership before the seam so an injected failure closes the fd on
        // exactly the path the real `AsyncFd::new` failure does.
        let file = std::fs::File::from(fd);
        #[cfg(test)]
        if let Some(error) = crate::sys::fault_injection::check(
            crate::sys::fault_injection::Site::PtyAsyncFdRegistration,
            target,
        ) {
            return Err(error);
        }
        Ok(Self {
            master: AsyncFd::new(file)?,
            eof_byte,
            eof_written: 0,
        })
    }

    fn poll_write_raw(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = match self.master.poll_write_ready(cx) {
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
        if this.eof_written > 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pty stdin writer closed",
            )));
        }
        this.poll_write_raw(cx, data)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // A pty master holds no user-space write buffer — `write(2)` hands bytes
        // straight to the tty line discipline — so there is nothing to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(eof) = this.eof_byte else {
            return Poll::Ready(Ok(()));
        };
        // A PTY master has no half-close. Two configured VEOF characters cover
        // both canonical-mode states: the first flushes an unterminated final
        // line, and the second arrives at an empty line and yields EOF.
        let sequence = [eof, eof];
        while this.eof_written < sequence.len() {
            let offset = this.eof_written;
            match this.poll_write_raw(cx, &sequence[offset..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to deliver pty EOF",
                    )));
                }
                Poll::Ready(Ok(written)) => this.eof_written += written,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for AsyncPtyMaster {
    fn drop(&mut self) {
        let Some(eof) = self.eof_byte else {
            return;
        };
        if self.eof_written < 2 {
            // Drop cannot await readiness. A two-byte non-blocking write normally
            // succeeds immediately; `finish()`/bulk stdin use poll_shutdown for
            // the reliable path, while plain drop remains best-effort.
            let mut file: &std::fs::File = self.master.get_ref();
            let _ = file.write(&[eof, eof][self.eof_written..]);
        }
    }
}

/// Spawn `cmd` under a pseudo-terminal, wiring the slave as its stdio and running
/// the actual spawn through `spawn` (the platform's normal containment path), so
/// the child is contained identically to a pipe-spawned run.
///
/// `spawn` is the per-platform `Job::spawn` (cgroup / process-group join in
/// `pre_exec`), passed as a closure so this shared Unix code reuses each
/// backend's containment without forking a parallel structure.
///
/// `rollback` is that spawn's undo, called (once, with the child's pid) when a
/// step *after* the child exists fails. Its contract is **kill within the
/// containment first, and release only bookkeeping the job no longer needs**:
/// hard-kill everything this spawn owns that the backend's containment can reach,
/// and drop a registration only where dropping it costs no later reach — never a
/// reaper subtree root, which is the sole handle the job's own `kill_all`/`Drop`
/// has on anything that survived. It runs with the child still owned and un-reaped
/// by [`PtySpawnRollback`], so the pid it is handed cannot be a recycled alias and
/// needs no identity gate. See that guard for the honest per-mechanism scope this
/// buys.
pub(crate) fn spawn_pty<F, R>(
    cmd: &mut Command,
    opts: &SpawnOptions,
    spawn: F,
    rollback: R,
) -> io::Result<PtySpawn>
where
    F: FnOnce(&mut Command, &SpawnOptions) -> io::Result<Child>,
    R: FnOnce(u32),
{
    let (cols, rows) = opts.pty_size.unwrap_or(super::DEFAULT_PTY_SIZE);
    let (master, slave) = open_pty(cols, rows)?;
    disable_echo(&slave)?;
    let eof_byte = terminal_eof(&slave)?;

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
    // From here on the child exists and is contained, while the master wiring
    // below is still fallible — so it is owned by the rollback guard, which undoes
    // the spawn (containment-scoped kill, then bookkeeping, then a direct-handle
    // backstop kill) unless `disarm` hands the child on.
    let guard = PtySpawnRollback::new(spawn(cmd, &pty_opts)?, rollback);
    let pid = guard.pid();

    // The reader and writer each own a dup of the master (same open description),
    // so the pump can read the merged output while stdin is written concurrently;
    // both dropping closes the master. Each dup is driven through the reactor via
    // `AsyncFd` in non-blocking mode (no blocking-pool thread per session), so
    // many concurrent PTY sessions scale on the reactor instead of taxing the
    // pool with a read+write thread apiece. The `EofOnEio` wrapper is unchanged —
    // the end-of-session `EIO` still surfaces from `poll_read` as a clean EOF.
    let master_w = clone_master(&master, "writer")?;
    // A third dup, retained by `PtyChild` solely for the live-resize ioctl. It
    // stays a plain blocking-view owned fd (never wrapped in `AsyncFd`); sharing
    // the master's open file description it inherits the `O_NONBLOCK` the
    // reader/writer set, which is irrelevant to an ioctl.
    let master_resize = clone_master(&master, "resize")?;
    let reader: PtyReader = Box::new(EofOnEio(AsyncPtyMaster::new(master, None, "reader")?));
    let writer: PtyWriter = Box::new(AsyncPtyMaster::new(master_w, Some(eof_byte), "writer")?);

    Ok(PtySpawn {
        // Setup is complete: take the child out of the guard so no rollback can
        // run once `RunningProcess` owns its lifecycle.
        child: PtyChild {
            child: guard.disarm(),
            resize_fd: master_resize,
        },
        reader,
        writer,
        pid,
    })
}

/// Owns a freshly-contained child while the PTY master wiring is still fallible,
/// and undoes the spawn — **containment-scoped kill first, bookkeeping last** — if
/// it fails.
///
/// # Why this order
///
/// By the time this guard exists the containment callback has already registered
/// the child (cgroup member, tracked process group, reaper subtree root). That
/// registration is what every *later* whole-tree verb aims by: the job's own
/// `kill_all`/`Drop` sweeps the recorded groups and roots. So releasing it during a
/// rollback is not a tidy-up, it is the job forgetting a subtree — and if anything
/// under that subtree survived the rollback's own kill (a descendant that forked
/// and `setsid`'d during the microsecond-wide but real setup window, a member that
/// refused the signal), it is stranded *permanently*. A rollback that leaks a live
/// process out of the job breaks the crate's headline guarantee, which is strictly
/// worse than a rollback that kills less than it hoped to. Each backend therefore
/// releases only bookkeeping whose loss costs the job nothing it still needs (see
/// each `rollback_pty_spawn`), and this `Drop` puts the killing first:
///
/// 1. the backend callback first — its containment-scoped kill, then whatever
///    registration that backend is willing to release (the reaper releases none;
///    see `Reaper::hard_kill_subtree` in `sys::freebsd`);
/// 2. only then the direct `SIGKILL` through the `Child` handle we still own,
///    which depends on no bookkeeping whatsoever and is pure backstop.
///
/// Issuing that direct kill *first* — the shape this guard originally had — buys
/// nothing and costs a dependency. `Child::start_kill` is an asynchronous `kill(2)`,
/// so the kernel may retire the child on another core before this thread reaches
/// its next syscall, and the backend is then asked to tear down a tree whose root
/// has just become a corpse. Every mechanism here is in fact known to survive that
/// (a process group outlives its leader; the reaper's subtree tag outlives the
/// root — see the FreeBSD entry below), so this ordering closes no proven hole; it
/// declines, for free, to *rest* on three kernels' treatment of a dead root. The
/// direct kill loses nothing by going last, being a subset of every backend's own
/// reach and needing no bookkeeping of any kind.
///
/// The pid stays pinned throughout: the `Child` is still owned (and un-reaped) for
/// both steps, so no kill in this path can land on a recycled number — the same
/// argument `pgroup::UntrackedChildGuard` rests on.
///
/// # The scope this actually guarantees (honest per mechanism)
///
/// The guarantee is "**as much of this spawn as the mechanism's own teardown can
/// reach, and nothing of it left outside the job's**", not an absolute "no
/// descendant of it survives anywhere":
///
/// - **FreeBSD reaper** — `PROC_REAP_KILL` with `REAPER_KILL_SUBTREE` over this
///   spawn's root: the entire subtree, `setsid` escapees included. The mechanism's
///   maximum, and it does not rest on the root still being alive — the kernel tags
///   each descendant with its subtree's root pid and an orphan keeps that tag
///   (`groups::freebsd_reaper::a_setsid_escapee_stays_contained` executes exactly
///   that on FreeBSD CI). What is *not* claimed is that the kill can never be
///   refused, so the rollback additionally releases **no** root: anything the walk
///   leaves behind is still the job's to kill and dies with its `kill_all`/`Drop`.
///   See `Reaper::hard_kill_subtree` in `sys::freebsd`.
/// - **Linux cgroup** — `cgroup.kill` over the **per-spawn leaf sub-cgroup this
///   spawn was given**: that leaf's whole subtree at once, `setsid` escapees
///   included (cgroup membership is inherited across `fork` and untouched by
///   `setsid`), and nothing belonging to another spawn, each of which has a leaf of
///   its own. Writing that same file in the job's *own* cgroup is still deliberately
///   rejected: there it kills every member of the job, which a failed spawn has no
///   business doing. Two conditions gate the selective kill — that this spawn has a
///   leaf at all (a host can refuse the `mkdir`) and that the write is accepted (a
///   kernel < 5.14 has no `cgroup.kill`; a restricted delegated cgroup can refuse
///   it) — and failing either falls back to `killpg` over the pty child's session
///   plus the direct child. A descendant that `setsid`s in the setup window survives
///   *that* fallback, but it has not left the job's cgroup tree, so it stays
///   contained and the job's own `kill_all` still ends it while the job lives. See
///   `Job::rollback_pty_spawn` in `sys::linux` for what `Drop` does and does not add
///   to that.
/// - **POSIX process group** (macOS, the BSDs other than FreeBSD, and the Linux
///   cgroup fallback) — `killpg` over the session, which is this mechanism's own
///   whole-tree maximum. A descendant that `setsid`s away escapes it: the standing
///   [`Mechanism::ProcessGroup`](crate::Mechanism::ProcessGroup) limit that applies
///   to `kill_all`, `shutdown` and `signal` alike, not something this rollback
///   introduces.
///
/// In every case the reach is a **superset** of the single-child teardown the same
/// child would receive had the launch succeeded and later timed out
/// (`graceful::run_pid`, which reaches the direct child by pid and nothing else).
/// What each mechanism releases afterwards, and what becomes of anything its kill
/// did not reach, is stated in that mechanism's bullet above and in its own
/// `rollback_pty_spawn` — deliberately not summarized into one sentence here, since
/// the three do not share one answer.
struct PtySpawnRollback<R: FnOnce(u32)> {
    /// `None` only after [`disarm`](Self::disarm) has taken the child.
    child: Option<Child>,
    /// `None` once the guard has fired or been disarmed — a `FnOnce` runs once.
    rollback: Option<R>,
}

impl<R: FnOnce(u32)> PtySpawnRollback<R> {
    fn new(child: Child, rollback: R) -> Self {
        Self {
            child: Some(child),
            rollback: Some(rollback),
        }
    }

    /// The guarded child's pid, or `None` if it exited and was reaped already —
    /// exactly what `PtySpawn::pid` reports for a successful spawn.
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    /// Setup succeeded: stop guarding and hand the child back unharmed.
    fn disarm(mut self) -> Child {
        self.rollback.take();
        self.child
            .take()
            .expect("the guarded child is taken exactly once")
    }
}

impl<R: FnOnce(u32)> Drop for PtySpawnRollback<R> {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return; // disarmed — `RunningProcess` owns the lifecycle now.
        };

        if let Some(pid) = child.id() {
            // The backend's containment-scoped kill first, while this spawn is
            // still registered and — as far as this path can arrange it — while its
            // root is still alive: on the reaper backend that root is what the
            // subtree walk is aimed at, and `start_kill` below would turn it into a
            // zombie asynchronously, possibly before the callback's first syscall.
            // See the type docs for the per-mechanism reach.
            if let Some(rollback) = self.rollback.take() {
                rollback(pid);
            }
            // Then the direct child, through the handle we still own. It is pure
            // backstop — every backend above already covers this pid — and it is
            // the one kill that depends on no bookkeeping at all, which is exactly
            // why it is safe to leave until after the callback.
            let _ = child.start_kill();
        }

        // Dropping a killed tokio `Child` hands it to tokio's orphan reaper, which
        // completes the wait without blocking this synchronous rollback path.
        drop(child);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::Mechanism;
    use crate::runner::ProcessRunner;
    use crate::sys::fault_injection::{Faults, Site};
    use crate::sys::pgroup::ProcessGroup;
    use crate::sys::{DisplacedSpare, SpawnOptions};

    /// Marker env var set only on the re-exec'd escapee, so
    /// [`setsid_escapee_process`] is an immediate no-op pass in an ordinary
    /// `--include-ignored` run of this binary.
    const ESCAPEE_FLAG: &str = "PK_PTY_ROLLBACK_ESCAPEE";
    /// Where that escapee publishes its pid (or, on failure, why it could not).
    const ESCAPEE_PIDFILE: &str = "PK_PTY_ROLLBACK_PIDFILE";
    /// This binary's own path, handed to `sh` through the environment rather than
    /// interpolated into the script text.
    const ESCAPEE_EXE: &str = "PK_PTY_ROLLBACK_EXE";
    /// Carries the pid of the shell that started the escapee (`$$`), and by being
    /// set at all asks it to publish only once that shell is **gone**, so a harness
    /// can be sure — not merely confident — that the rollback it is about to run has
    /// a zombie for a subtree root.
    ///
    /// The pid has to be passed rather than read as "whatever my parent was when I
    /// started": the shell exits within microseconds of forking, long before the
    /// `exec` of this binary completes, so an escapee that sampled its own
    /// `getppid` would often sample the re-parented value and then wait forever for
    /// a change that had already happened.
    const ESCAPEE_SHELL_PID: &str = "PK_PTY_ROLLBACK_SHELL";
    /// The libtest name the harness re-execs (positional filter + `--exact`).
    /// Spelled as the **unit-test** binary sees it: this file is included as
    /// `sys::pty::imp` (the `#[cfg_attr(unix, path = "unix.rs")] mod imp` in
    /// `sys/pty/mod.rs`), not as `sys::pty::unix`. Keep in sync with the module
    /// path and `fn setsid_escapee_process` — a mismatch surfaces as the pid poll
    /// timing out with no pidfile written.
    const ESCAPEE_TEST: &str = "sys::pty::imp::tests::setsid_escapee_process";

    /// A pty spawn's options — the launch seam sets nothing else for these tests.
    fn pty_options() -> SpawnOptions {
        SpawnOptions {
            use_pty: true,
            ..SpawnOptions::default()
        }
    }

    /// The platform job this host actually selects (cgroup, POSIX process group,
    /// or the FreeBSD reaper) — the point of driving the rollback through
    /// `sys::Job` rather than a hand-built `ProcessGroup`.
    fn platform_job() -> crate::sys::Job {
        #[cfg(feature = "limits")]
        let job = crate::sys::Job::new(&crate::limits::ResourceLimits::default());
        #[cfg(not(feature = "limits"))]
        let job = crate::sys::Job::new();
        job.expect("create the platform job")
    }

    /// A child that stays alive until something kills it.
    fn idle_command() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "while :; do sleep 60; done"]);
        command
    }

    /// A unique-per-process temp path for a helper to publish a pid through.
    fn pidfile(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "processkit_pty_rollback_{tag}_{}.pid",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// Whether `pid` still names a live process — first reaping it if it happens
    /// to be *this* process's child.
    ///
    /// The targeted `WNOHANG` wait is what keeps the probe honest on FreeBSD: this
    /// process is the tree's reaper there, so a killed grandchild re-parents onto
    /// us and an unreaped corpse would answer a bare `kill(pid, 0)` as "alive"
    /// forever. Elsewhere (Linux, macOS) such an orphan re-parents to init and the
    /// wait is a benign `ECHILD` no-op.
    ///
    /// For the one pid that *is* a tokio child (the rolled-back pty child itself,
    /// handed to tokio's orphan reaper when the guard dropped it) this races that
    /// reaper for the corpse — benignly and by design: both use `WNOHANG` on that
    /// one pid, so whichever wins, the other sees `ECHILD` and stands down.
    fn is_alive(pid: u32) -> bool {
        let pid = pid as libc::pid_t;
        let mut status = 0;
        // SAFETY: a non-blocking wait for one specific pid, then the signal-`0`
        // existence probe; neither touches memory beyond `status`.
        unsafe {
            if libc::waitpid(pid, &raw mut status, libc::WNOHANG) == pid {
                return false; // just reaped by us — definitively gone
            }
            libc::kill(pid, 0) == 0
        }
    }

    /// Wait (bounded) for `pid` to disappear, or panic with `what`.
    async fn wait_until_gone(pid: u32, what: &str) {
        for _ in 0..600 {
            if !is_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Never leave a test process behind, whichever way the assertion goes.
        // SAFETY: a best-effort `SIGKILL` to the pid this test itself started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        panic!("{what}: pid {pid} was still alive after the bounded wait");
    }

    /// Whether the Linux cgroup backend can give the spawn `pid` belongs to the
    /// **selective, leaf-scoped** kill its rollback prefers — decided from host
    /// facts, not from the crate's own bookkeeping: the process lives in a per-spawn
    /// leaf (a `spawn-*` directory, the one `sys::linux` creates per launch) and the
    /// kernel provides that leaf's `cgroup.kill` (>= 5.14). The cgroup path is
    /// resolved the way the backend itself resolves one — `/proc/<pid>/cgroup`'s
    /// `0::` line joined onto the v2 mount root.
    ///
    /// Both conditions can genuinely be absent (a host that refuses the `mkdir`, an
    /// older kernel), and the rollback then falls back to `killpg`, whose reach a
    /// `setsid` escapee escapes — two different contracts, so the caller has to know
    /// which one this host is under.
    #[cfg(target_os = "linux")]
    fn selective_leaf_kill_available(pid: u32) -> bool {
        let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
            return false;
        };
        let Some(rel) = text.lines().find_map(|line| line.strip_prefix("0::")) else {
            return false;
        };
        let dir = Path::new("/sys/fs/cgroup").join(rel.trim().trim_start_matches('/'));
        dir.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("spawn-"))
            && dir.join("cgroup.kill").exists()
    }

    /// [`Mechanism::CgroupV2`] is a Linux-only verdict, so everywhere else that arm
    /// is unreachable and this is a constant.
    #[cfg(not(target_os = "linux"))]
    fn selective_leaf_kill_available(_pid: u32) -> bool {
        false
    }

    /// What the cgroup backend's rollback owes a `setsid` escapee of the spawn it
    /// just undid — one of two contracts, chosen by the host fact
    /// [`selective_leaf_kill_available`] sampled **before** the rollback ran:
    ///
    /// - with a leaf-scoped `cgroup.kill`, that escapee is this spawn's own to kill
    ///   and must be gone;
    /// - without one the rollback is session-scoped and the escapee survives it, so
    ///   what must hold instead is that it never left the job — still a member, and
    ///   ended by the job's own `kill_all`.
    async fn assert_cgroup_escapee_scope(job: &crate::sys::Job, escapee: u32, selective: bool) {
        if selective {
            wait_until_gone(
                escapee,
                "the rollback's leaf-scoped cgroup.kill must reach a setsid escapee of \
                 the spawn it is undoing",
            )
            .await;
            return;
        }
        assert!(
            is_alive(escapee),
            "without a per-spawn leaf the cgroup rollback is session-scoped by design; \
             the escapee is the job's to kill, not this spawn's"
        );
        #[cfg(feature = "process-control")]
        assert!(
            job.members()
                .expect("read the cgroup's members")
                .contains(&escapee),
            "the escapee must still be a cgroup member — neither setsid nor losing its \
             parent takes a process out of a cgroup, which is why no orphan escapes the job"
        );
        job.kill_all().expect("the job's own teardown");
        wait_until_gone(escapee, "the job's cgroup.kill must reach the escapee").await;
    }

    /// Wait (bounded) for a helper to publish its pid into `path`.
    async fn published_pid(path: &Path, what: &str) -> u32 {
        for _ in 0..600 {
            if let Ok(text) = std::fs::read_to_string(path) {
                let text = text.trim();
                if !text.is_empty() {
                    return text.parse().unwrap_or_else(|_| {
                        panic!("{what} reported a failure, not a pid: {text}")
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{what} never published its pid");
    }

    /// How long a helper gets to publish its pid while the rollback guard waits on
    /// it. Long enough for the escapee's re-`exec` of this whole test binary under a
    /// loaded CI runner; the tests only ever wait this long when they are about to
    /// fail, because on the contract's path the child is unsignalled and prompt.
    const PUBLISH_BUDGET: Duration = Duration::from_secs(20);

    /// [`published_pid`]'s synchronous twin, for the callers that run inside the
    /// rollback guard's `Drop` — a `Drop` cannot await, and blocking it until the
    /// child has reached the state under test is the whole point of those callers.
    ///
    /// Returns whatever the helper published (a pid, or the failure text it
    /// publishes instead), or `None` if it published nothing within `budget` —
    /// which in these tests is a diagnosis, not an accident: a child that has not
    /// been signalled has every chance to publish.
    fn published_blocking(path: &Path, budget: Duration) -> Option<String> {
        let deadline = Instant::now() + budget;
        loop {
            if let Ok(text) = std::fs::read_to_string(path) {
                let text = text.trim().to_owned();
                if !text.is_empty() {
                    return Some(text);
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The pid in what [`published_blocking`] saw, or a panic naming which of the
    /// two ways it went wrong.
    fn parse_published(published: Option<String>, what: &str) -> u32 {
        match published {
            None => panic!(
                "{what} published nothing before the backend rollback ran — with the \
                 guard's contract order nothing had signalled the pty child yet, so \
                 either the direct SIGKILL moved back ahead of the callback or the \
                 re-exec filter no longer matches"
            ),
            Some(text) => text
                .parse()
                .unwrap_or_else(|_| panic!("{what} reported a failure, not a pid: {text}")),
        }
    }

    /// Drive a **real** post-spawn failure through the production guard: `command`
    /// is launched as a pty child of `job`, the first master `dup` is faulted, and
    /// `PtySpawnRollback::drop` therefore runs exactly as it does on an fd-exhausted
    /// host — backend rollback and direct `SIGKILL` both, in the production order.
    ///
    /// `observe` runs inside the guard's rollback callback, immediately before
    /// `Job::rollback_pty_spawn`. That is the one moment a test can see the state
    /// the ordering contract is about, and blocking there is also what makes these
    /// tests deterministic: the child reaches the state under test *because* the
    /// contract's order has not signalled it yet, so a regression to "direct kill
    /// first" shows up as a child that never gets there.
    fn rolled_back_pty_spawn(
        job: &crate::sys::Job,
        command: &mut Command,
        observe: impl FnOnce(u32),
    ) {
        let _fault = Faults::new()
            .fail_every(Site::PtyMasterClone, Some("writer"), libc::EIO)
            .arm();
        let result = spawn_pty(
            command,
            &pty_options(),
            |cmd, opts| job.spawn(cmd, opts),
            |pid| {
                observe(pid);
                // These tests are about the guard's kills and their order, not the
                // kill-on-drop latch: nothing here armed a spare, so there is none
                // to restore (the production wiring threads the real token from its
                // own spawn closure — see `Job::spawn_pty`).
                job.rollback_pty_spawn(pid, DisplacedSpare::default());
            },
        );
        assert!(
            result.is_err(),
            "the injected master-clone fault must surface as an error"
        );
    }

    /// Every fallible step that runs *after* the containment registration must
    /// take the same rollback path: the child is killed and its tracker entry is
    /// gone, whichever of them failed. The fault sites are deterministic; the real
    /// child is what proves the guard's kill/unregister/drop path actually works.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns real Unix PTY children"]
    async fn post_spawn_pty_failures_kill_and_unregister_the_child() {
        let failures = [
            (Site::PtyMasterClone, Some("writer")),
            (Site::PtyMasterClone, Some("resize")),
            (Site::PtyAsyncFdRegistration, Some("reader")),
            (Site::PtyAsyncFdRegistration, Some("writer")),
        ];

        for (site, target) in failures {
            let group = ProcessGroup::new();
            let rollback_pid = Arc::new(AtomicU32::new(0));
            let rollback_pid_for_callback = Arc::clone(&rollback_pid);
            let _fault = Faults::new().fail_every(site, target, libc::EIO).arm();

            let mut command = idle_command();
            let result = spawn_pty(
                &mut command,
                &pty_options(),
                |cmd, opts| group.spawn(cmd, opts),
                |pid| {
                    rollback_pid_for_callback.store(pid, Ordering::SeqCst);
                    group.rollback_pty_spawn(pid, DisplacedSpare::default());
                },
            );
            assert!(result.is_err(), "fault at {site:?}/{target:?} must surface");

            let pid = rollback_pid.load(Ordering::SeqCst);
            assert_ne!(pid, 0, "the rollback must observe the spawned child's pid");
            // Read *before* waiting for the reap on purpose: an unreaped child still
            // probes alive, so an entry the rollback failed to remove would be
            // listed here rather than pruned as dead and pass by accident.
            #[cfg(feature = "process-control")]
            assert!(
                group.members().is_empty(),
                "fault at {site:?}/{target:?} left a stale process-group entry"
            );
            wait_until_gone(pid, &format!("fault at {site:?}/{target:?}")).await;
        }
    }

    /// T-270, end to end through the production wiring (`Job::spawn_pty`, which
    /// threads the spawn's displaced spare to its own rollback): a caller stops a
    /// group *without* escalating — survivors deliberately left running, the
    /// kill-on-drop backstop spared — and a later PTY launch then fails after its
    /// child already exists. The failed launch re-armed that backstop on the way in,
    /// as every spawn does; its rollback must put the spare back, or dropping the
    /// job hard-kills processes the caller had decided not to escalate against.
    ///
    /// Deterministic: the master `dup` is fault-injected, and the survivor ignores
    /// `SIGTERM` so the non-escalating shutdown really does leave it running.
    /// Whichever mechanism this host selects is the one exercised (cgroup, POSIX
    /// process group, or the FreeBSD reaper — each consults its own
    /// `skip_drop_kill` latch before killing on `Drop`).
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns real Unix PTY children"]
    async fn a_failed_pty_launch_leaves_a_non_escalated_spare_in_place() {
        let job = platform_job();
        let file = pidfile("spare_survivor");

        // A survivor that will not die of the graceful signal. It publishes its pid
        // only *after* installing the trap, and the wait below is on that: until the
        // trap exists the survivor would die of the graceful `SIGTERM` like any other
        // child, and "the survivor was spared" would read the wrong thing.
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "trap '' TERM; echo $$ > \"$PK_PIDFILE\"; while :; do sleep 60; done",
            ])
            .env("PK_PIDFILE", &file);
        let mut survivor = job
            .spawn(&mut command, &SpawnOptions::default())
            .expect("spawn the survivor");
        let survivor_pid = survivor.id().expect("the survivor reports a pid");
        assert_eq!(
            published_pid(&file, "the survivor").await,
            survivor_pid,
            "the trap-installing shell must be the child this group tracks"
        );

        // The caller stops without escalating: the survivor stays alive, and the
        // latch is what will keep `Drop` from killing it.
        job.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .expect("graceful shutdown");
        assert!(
            is_alive(survivor_pid),
            "a non-escalating shutdown must leave the survivor running"
        );

        // A PTY launch into the same job, failing after its child exists.
        {
            let _fault = Faults::new()
                .fail_every(Site::PtyMasterClone, Some("writer"), libc::EIO)
                .arm();
            let result = job.spawn_pty(&mut idle_command(), &pty_options(), None);
            assert!(
                result.is_err(),
                "the injected master-clone fault must surface as an error"
            );
        }

        drop(job);
        // A `Drop` that killed the survivor does so with an unblockable `SIGKILL`
        // issued before `drop` returned; give the kernel a moment to retire it, then
        // read the state that decides this test.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let spared = is_alive(survivor_pid);

        // Whichever way the assertion goes, take the survivor down with us.
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(survivor_pid as libc::pid_t, libc::SIGKILL) };
        let _ = survivor.wait().await;
        let _ = std::fs::remove_file(&file);

        assert!(
            spared,
            "a failed PTY launch must not undo a graceful_shutdown(escalate = false): \
             the rollback has to restore the spare its own spawn displaced"
        );
    }

    /// The runner reserves a one-shot stdin source *before* the PTY setup and
    /// commits it only once the spawn returns `Ok`. A post-spawn rollback must
    /// therefore leave that source available for the next launch.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns real Unix PTY children"]
    async fn post_spawn_pty_failure_releases_one_shot_stdin_reservation() {
        let command = crate::Command::new("sh")
            .args(["-c", "read value; test -z \"$value\""])
            .stdin(crate::Stdin::from_reader(tokio::io::empty()))
            .use_pty();

        let fault = Faults::new()
            .fail_every(Site::PtyMasterClone, Some("writer"), libc::EIO)
            .arm();
        let first = crate::JobRunner::new().start(&command).await;
        assert!(first.is_err(), "the injected PTY setup fault must surface");
        drop(fault);

        let second = crate::JobRunner::new()
            .output_string(&command)
            .await
            .expect("the restored one-shot stdin source must permit a retry");
        assert!(
            second.is_success(),
            "the retry must receive EOF from the restored empty source: {second:?}"
        );
    }

    /// R-02, the ordinary half: a descendant the pty child forked during the setup
    /// window is inside the child's session, so **every** unix mechanism's rollback
    /// reaches it — the guarantee that would be lost if the rollback dropped the
    /// registration first and then killed only the direct child.
    ///
    /// Driven against the backend rollback the guard calls rather than through an
    /// injected fault: the fault fires microseconds after the spawn, so only an
    /// explicit call can guarantee the descendant already exists when the rollback
    /// runs — which is exactly the state this contract is about.
    #[tokio::test]
    #[ignore = "spawns real Unix PTY children"]
    async fn rollback_kills_a_descendant_forked_in_the_setup_window() {
        let job = platform_job();
        let file = pidfile("descendant");

        let mut command = Command::new("sh");
        command
            .args(["-c", "sleep 300 & echo $! > \"$PK_PIDFILE\"; wait"])
            .env("PK_PIDFILE", &file);
        let spawn = job
            .spawn_pty(&mut command, &pty_options(), None)
            .expect("pty spawn");
        let pid = spawn.pid.expect("the pty child reports a pid");
        let descendant = published_pid(&file, "the forked descendant").await;
        assert!(is_alive(descendant), "the descendant must start out alive");

        // The guard's step, with the child still owned — so the pid cannot have
        // been recycled, exactly as in the real rollback.
        job.rollback_pty_spawn(pid, DisplacedSpare::default());

        wait_until_gone(
            descendant,
            "a descendant inside the pty child's session must not survive the rollback",
        )
        .await;
        drop(spawn);
        let _ = std::fs::remove_file(&file);
    }

    /// R-02, the escapee half: a descendant that forks **and calls `setsid`** in the
    /// setup window, i.e. the one case where the mechanisms genuinely differ. The
    /// contract asserted here is the honest one the guard documents — rollback
    /// reaches as far as this mechanism's own teardown does, no further:
    ///
    /// - **FreeBSD reaper** — dead: `PROC_REAP_KILL` walks the subtree across the
    ///   session boundary, which is what makes a selective per-spawn cleanup
    ///   possible here at all. The root is left recorded, so anything the walk does
    ///   not take is still the job's (the R-02 fix).
    /// - **Linux cgroup** — dead where this host grants the spawn a leaf sub-cgroup
    ///   whose `cgroup.kill` the kernel provides: that write kills the leaf's whole
    ///   subtree, across the session boundary, without touching another spawn.
    ///   Where it does not (see [`selective_leaf_kill_available`]), the rollback
    ///   falls back to `killpg` and the escapee is alive but never out of
    ///   containment: `setsid` does not change cgroup membership, so the job still
    ///   lists it and `kill_all` ends it. Firing `cgroup.kill` in the job's *own*
    ///   cgroup instead would take down every unrelated member of the same job.
    /// - **POSIX process group** — alive and gone from containment: the standing
    ///   `Mechanism::ProcessGroup` escape hatch, identical for `kill_all` /
    ///   `shutdown` / `signal`, not something the rollback introduces.
    #[tokio::test]
    #[ignore = "spawns real Unix PTY children and a setsid escapee"]
    async fn rollback_scope_for_a_setsid_escapee_matches_the_mechanism() {
        let job = platform_job();
        let mechanism = job.mechanism();
        let file = pidfile("escapee");
        let exe = std::env::current_exe().expect("locate the unit-test binary");

        // `sh` starts the escapee in the background (so it is not a group leader and
        // its own `setsid` succeeds) and then waits, staying alive as the pty child
        // the rollback guard would still own. The escapee's stdio goes to /dev/null
        // so it never holds this test's pty master open.
        let script = format!(
            "\"${ESCAPEE_EXE}\" {ESCAPEE_TEST} --exact --ignored </dev/null >/dev/null 2>&1 & wait"
        );
        let mut command = Command::new("sh");
        command
            .args(["-c", &script])
            .env(ESCAPEE_EXE, &exe)
            .env(ESCAPEE_FLAG, "1")
            .env(ESCAPEE_PIDFILE, &file);
        let spawn = job
            .spawn_pty(&mut command, &pty_options(), None)
            .expect("pty spawn");
        let pid = spawn.pid.expect("the pty child reports a pid");
        let escapee = published_pid(&file, "the setsid escapee").await;
        // SAFETY: `getsid` only reads the target's session id.
        assert_eq!(
            unsafe { libc::getsid(escapee as libc::pid_t) },
            escapee as libc::pid_t,
            "escapee {escapee} never became a session leader — the test would prove nothing"
        );

        // Sampled before the rollback: once it has run, the escapee's own cgroup is
        // no longer there to be read.
        let selective = selective_leaf_kill_available(escapee);

        job.rollback_pty_spawn(pid, DisplacedSpare::default());

        match mechanism {
            Mechanism::ProcessReaper => {
                wait_until_gone(
                    escapee,
                    "the reaper's subtree kill must reach a setsid escapee",
                )
                .await;
            }
            Mechanism::CgroupV2 => {
                assert_cgroup_escapee_scope(&job, escapee, selective).await;
            }
            Mechanism::ProcessGroup => {
                assert!(
                    is_alive(escapee),
                    "the documented process-group escape hatch: killpg cannot reach \
                     a new session, here as everywhere else on this mechanism"
                );
                // SAFETY: cleaning up the escapee this test deliberately created.
                unsafe { libc::kill(escapee as libc::pid_t, libc::SIGKILL) };
            }
            other => panic!("unexpected unix containment mechanism {other:?}"),
        }

        drop(spawn);
        // Belt and braces: no arm may leave the escapee running.
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(escapee as libc::pid_t, libc::SIGKILL) };
        let _ = std::fs::remove_file(&file);
    }

    /// The guard's own path, with a live descendant — the production sequence the
    /// two tests above deliberately bypass by calling the backend rollback
    /// directly. Both of the guard's kills run here, and their **order** is what
    /// this pins down: the containment-scoped rollback (which also decides what
    /// bookkeeping to release) must run before the direct `SIGKILL` through the
    /// still-owned handle — never after it.
    ///
    /// Determinism comes from making the guard wait: the rollback callback blocks
    /// until the pty child publishes the pid of a descendant it forked. On the
    /// contract's order nothing has signalled that child when the callback runs, so
    /// it publishes promptly and is still alive — both asserted below, and both
    /// facts about the *guard*, not about scheduling. Hoist the direct kill back in
    /// front of the callback and the `SIGKILL` lands on an `sh` that has not even
    /// finished `exec`ing: no pid is ever published, the wait runs out, and the
    /// backend rollback is left aiming at a corpse.
    #[tokio::test]
    #[ignore = "spawns real Unix PTY children"]
    async fn the_guard_rolls_the_backend_back_before_killing_the_child_itself() {
        let job = platform_job();
        let file = pidfile("guard");

        let mut command = Command::new("sh");
        command
            .args(["-c", "sleep 300 & echo $! > \"$PK_PIDFILE\"; wait"])
            .env("PK_PIDFILE", &file);

        let published = RefCell::new(None);
        let root_alive = Cell::new(false);
        rolled_back_pty_spawn(&job, &mut command, |pid| {
            *published.borrow_mut() = published_blocking(&file, PUBLISH_BUDGET);
            // After that wait, a `SIGKILL` sent before the callback has had
            // milliseconds to land — and `is_alive` reaps first, so the pty child's
            // own un-reaped corpse cannot answer this probe as "alive".
            root_alive.set(is_alive(pid));
        });

        let descendant = parse_published(published.into_inner(), "the forked descendant");
        assert!(
            root_alive.get(),
            "the pty child was already dead when the backend rollback ran: the \
             guard's direct SIGKILL must come last, or every mechanism's \
             containment-scoped teardown is handed a tree whose root is a corpse"
        );
        wait_until_gone(
            descendant,
            "a descendant forked inside the setup window must not survive the guard",
        )
        .await;
        let _ = std::fs::remove_file(&file);
    }

    /// The case no ordering can fix, exercised rather than assumed: the pty child
    /// exits *before* the guard fires, so the rollback aims at a root that is
    /// already a zombie, and what it left behind is a `setsid` escapee — the one
    /// descendant no `killpg` can reach.
    ///
    /// The escapee publishes only once it has been orphaned, which is what makes
    /// "the root is already dead" a fact of this test rather than a hope about
    /// scheduling. Per mechanism the expectation is the same as with a live root
    /// (see the test above), because a dead root narrows no mechanism's reach:
    ///
    /// - **FreeBSD reaper** — still dead. A subtree is not looked up through its
    ///   root: each descendant carries the root's pid as its own subtree tag and
    ///   keeps it when orphaned onto the reaper, so `PROC_REAP_KILL` walks it all
    ///   the same (`groups::freebsd_reaper::a_setsid_escapee_stays_contained` proves
    ///   the same property against a root that is not merely dead but reaped).
    /// - **Linux cgroup** — same as with a live root, and for the same reason: the
    ///   rollback's leaf-scoped `cgroup.kill` is aimed at a *directory*, not looked
    ///   up through the pty child, and a dead root leaves its leaf's other members
    ///   where they were. So dead where this host grants that leaf, and otherwise
    ///   alive and still a cgroup member — `setsid` does not leave a cgroup and
    ///   neither does losing a parent, so the job's `kill_all` ends it.
    /// - **POSIX process group** — alive and escaped, the standing
    ///   `Mechanism::ProcessGroup` limit.
    #[tokio::test]
    #[ignore = "spawns real Unix PTY children and a setsid escapee"]
    async fn the_guard_rolls_back_a_spawn_whose_child_died_first() {
        let job = platform_job();
        let mechanism = job.mechanism();
        let file = pidfile("orphaned_escapee");
        let exe = std::env::current_exe().expect("locate the unit-test binary");

        // `sh` starts the escapee in the background — so it is not a process-group
        // leader and its own `setsid` succeeds — and then exits at once, leaving the
        // guard a zombie for a subtree root.
        //
        // `trap '' HUP` first, and it is load-bearing: the pty child is this
        // terminal's *controlling process*, so the kernel hangs up its foreground
        // process group when it dies — which is where the escapee still is during
        // the `exec` of this binary, before its own `setsid` can run. An ignored
        // disposition survives both `fork` and `exec` (the `nohup` pattern), so the
        // escapee lives long enough to leave the session, which is the only state
        // that makes this test say anything. A descendant that dies of that `SIGHUP`
        // needs no containment; the one worth asserting about is the one that does
        // not.
        let script = format!(
            "trap '' HUP; {ESCAPEE_SHELL_PID}=$$ \"${ESCAPEE_EXE}\" {ESCAPEE_TEST} \
             --exact --ignored </dev/null >/dev/null 2>&1 & exit 0"
        );
        let mut command = Command::new("sh");
        command
            .args(["-c", &script])
            .env(ESCAPEE_EXE, &exe)
            .env(ESCAPEE_FLAG, "1")
            .env(ESCAPEE_PIDFILE, &file);

        let published = RefCell::new(None);
        let selective = Cell::new(false);
        rolled_back_pty_spawn(&job, &mut command, |_pid| {
            // Deliberately no liveness probe on the root here: `is_alive` reaps, and
            // reaping would unpin the very number the rollback is about to hand to
            // the kernel. The escapee's publication is the proof of the state.
            let text = published_blocking(&file, PUBLISH_BUDGET);
            // Which cgroup contract this host is under, sampled here — inside the
            // guard, before the backend rollback runs — because a leaf the rollback
            // kills and reclaims is no longer there to be read afterwards.
            if let Some(pid) = text.as_deref().and_then(|t| t.trim().parse::<u32>().ok()) {
                selective.set(selective_leaf_kill_available(pid));
            }
            *published.borrow_mut() = text;
        });

        let escapee = parse_published(published.into_inner(), "the orphaned setsid escapee");
        match mechanism {
            Mechanism::ProcessReaper => {
                wait_until_gone(
                    escapee,
                    "the reaper's subtree kill must reach a setsid escapee whose \
                     subtree root died before the rollback ran",
                )
                .await;
            }
            Mechanism::CgroupV2 => {
                assert_cgroup_escapee_scope(&job, escapee, selective.get()).await;
            }
            Mechanism::ProcessGroup => {
                assert!(
                    is_alive(escapee),
                    "the documented process-group escape hatch: killpg cannot reach \
                     a new session, here as everywhere else on this mechanism"
                );
                // SAFETY: cleaning up the escapee this test deliberately created.
                unsafe { libc::kill(escapee as libc::pid_t, libc::SIGKILL) };
            }
            other => panic!("unexpected unix containment mechanism {other:?}"),
        }

        // Belt and braces: no arm may leave the escapee running.
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(escapee as libc::pid_t, libc::SIGKILL) };
        let _ = std::fs::remove_file(&file);
    }

    /// Escapee mode: leave the spawning session entirely, publish the new session
    /// leader's pid, then idle until the test tears the tree down. A plain no-op
    /// unless re-exec'd by one of the escapee tests above, so an ordinary
    /// `--include-ignored` run of this binary just passes.
    ///
    /// With [`ESCAPEE_SHELL_PID`] set it publishes only after that shell has exited
    /// — detected as the shell no longer being its parent. That turns "the
    /// rollback's root is already a corpse" from a scheduling assumption into
    /// something the harness has observed before it acts.
    #[tokio::test]
    #[ignore = "helper process for the rollback-scope test; a no-op unless re-exec'd"]
    async fn setsid_escapee_process() {
        let (Ok(_), Ok(pidfile)) = (std::env::var(ESCAPEE_FLAG), std::env::var(ESCAPEE_PIDFILE))
        else {
            return;
        };
        // SAFETY: `setsid` takes no arguments and affects only the caller. It
        // succeeds here because this process is a *grandchild* of the job — the
        // `sh` above leads the process group, this process does not.
        let sid = unsafe { libc::setsid() };
        if sid == -1 {
            let error = std::io::Error::last_os_error();
            let _ = std::fs::write(&pidfile, format!("error: setsid failed: {error}"));
            return;
        }
        if let Ok(shell) = std::env::var(ESCAPEE_SHELL_PID)
            && !await_orphaning(shell.parse().unwrap_or(0)).await
        {
            let _ = std::fs::write(&pidfile, format!("error: shell {shell} never exited"));
            return;
        }
        let _ = std::fs::write(&pidfile, std::process::id().to_string());
        // Comfortably longer than the harness's own bounded polls, but still
        // bounded: a harness that fails early cannot reach a process that has, by
        // design, left its session.
        tokio::time::sleep(Duration::from_secs(300)).await;
    }

    /// Wait (bounded) until `shell` is no longer this process's parent — i.e. until
    /// the shell that forked it has exited and the kernel has re-parented it (onto
    /// init, or onto the reaper on FreeBSD). Returns immediately if that already
    /// happened, which is the common case: the shell exits while this binary is
    /// still `exec`ing. `false` means it never happened in time, which the caller
    /// publishes as a failure rather than letting the harness proceed on a premise
    /// that did not hold.
    ///
    /// Cheaper and more direct than any handshake through the filesystem: `getppid`
    /// is the kernel's own answer to "is that process still my parent", and it needs
    /// no cooperation from a shell that is, by construction, about to be gone.
    async fn await_orphaning(shell: libc::pid_t) -> bool {
        for _ in 0..600 {
            // SAFETY: `getppid` takes no arguments and only reads the caller's
            // parent id.
            if unsafe { libc::getppid() } != shell {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }
}
