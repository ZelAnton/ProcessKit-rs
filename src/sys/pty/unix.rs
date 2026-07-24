//! Unix PTY backend: `openpty` + the pty **slave** wired as the child's stdio,
//! spawned through the *existing* per-platform containment path (K-032), so the
//! child lands in the same cgroup / process group as any other run. The master
//! is retained (as the merged reader and the stdin writer); terminal echo is
//! disabled so a written secret is not echoed back into the merged output.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::io::AsRawFd;
use std::process::Stdio;

use tokio::process::{Child, Command};

use crate::sys::SpawnOptions;
use crate::sys::pid_gate::PidGate;

use super::{EofOnEio, PtyExitStatus, PtyReader, PtySpawn, PtyWriter};

/// A PTY child on Unix is an ordinary [`tokio::process::Child`] (the `openpty`
/// slave is its stdio), so the whole reap/kill lifecycle is the same as a real
/// pipe-spawned child — only the I/O wiring differs.
pub(crate) struct PtyChild {
    child: Child,
}

impl PtyChild {
    /// The child's pid, or `None` once reaped.
    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
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

/// Open a pseudo-terminal, returning the (master, slave) fds as owned handles.
fn open_pty() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    // A sane default window size so a size-querying child gets something usable
    // (a zero size makes some TUI tools misbehave). Purely cosmetic for the
    // minimal single-master-fd mode.
    let winsize = libc::winsize {
        ws_row: 24,
        ws_col: 80,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: `openpty` writes the two fds through the out-pointers; the name
    // buffer is null (we don't want the slave name), the termios is null (default
    // line discipline), and a valid winsize is supplied.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut::<libc::termios>(),
            &winsize,
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
    let (master, slave) = open_pty()?;
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

    // Spawn through the caller's containment path (cgroup/pgroup join happens in
    // that path's `pre_exec`, unchanged by the pty wiring above).
    let child = spawn(cmd, opts)?;
    let pid = child.id();

    // The reader and writer each own a dup of the master (same open description),
    // so the pump can read the merged output while stdin is written concurrently;
    // both closing closes the master. `tokio::fs::File` drives the fd through the
    // blocking pool — acceptable for the minimal, low-volume interactive mode.
    let master_w = master.try_clone()?;
    let reader: PtyReader = Box::new(EofOnEio(tokio::fs::File::from_std(std::fs::File::from(
        master,
    ))));
    let writer: PtyWriter = Box::new(tokio::fs::File::from_std(std::fs::File::from(master_w)));

    Ok(PtySpawn {
        child: PtyChild { child },
        reader,
        writer,
        pid,
    })
}
