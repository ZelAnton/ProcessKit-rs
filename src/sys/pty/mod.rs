//! Pseudo-terminal (PTY) launch backend for the opt-in
//! [`Command::use_pty`](crate::Command::use_pty) mode.
//!
//! A PTY replaces the three independent stdio pipes with a **single master
//! fd/handle** carrying the child's *merged* stdout+stderr, plus an input side
//! for stdin — `openpty` on Unix, `CreatePseudoConsole` (ConPTY) on Windows. It
//! exists so tools that demand a controlling terminal (an `isatty()`-gated
//! agentic CLI, an `ssh`/`sudo` password prompt) work; it is a *minimal*
//! single-master-fd mode, not a terminal emulator.
//!
//! # Containment is unchanged (K-032)
//!
//! The PTY spawn does **not** fork a parallel containment structure. On Unix it
//! wires the pty **slave** as the child's stdio and then spawns through the very
//! same per-platform [`Job::spawn`](crate::sys::Job) path (cgroup / process-group
//! join in `pre_exec`), so the child lands in the same job as any other. On
//! Windows the ConPTY child is created suspended, `AssignProcessToJobObject`'d to
//! the same Job Object, then resumed — identical containment to
//! [`Job::spawn`](crate::sys::Job). Kill-on-drop, timeouts, and cancellation are
//! therefore unaffected; only the I/O wiring changes.
//!
//! Exactly one platform module (`unix.rs` / `windows.rs`) compiles per target,
//! each exposing the same [`PtyChild`] shape and a `spawn_pty` entry point.

use std::io;

#[cfg_attr(unix, path = "unix.rs")]
#[cfg_attr(windows, path = "windows.rs")]
mod imp;

// The platform child-lifecycle handle and the platform spawn entry point. Only
// one `imp` compiles per target, so this re-export resolves to the Unix (tokio
// `Child` + `openpty` master) or Windows (raw `CreateProcessW` + ConPTY) form.
pub(crate) use imp::{PtyChild, spawn_pty};

/// The pseudo-terminal window size a PTY spawn falls back to when no explicit
/// [`Command::pty_size`](crate::Command::pty_size) was requested — `(cols, rows)`
/// = 80×24, the historical hard-coded default and the conventional terminal
/// dimensions a size-querying child expects. Shared by both platform backends so
/// the "unset → 80×24" rule lives in exactly one place.
pub(crate) const DEFAULT_PTY_SIZE: (u16, u16) = (80, 24);

/// Validate a PTY geometry before a backend creates or mutates an OS terminal.
///
/// A zero axis is not a usable terminal geometry on either backend. Windows has
/// the additional representational limit imposed by ConPTY's signed [`COORD`]
/// fields; Unix `winsize` fields are `u16`, so every non-zero `u16` remains valid
/// there. Public launch/resize seams and both backend entry points call this one
/// helper so an invalid request cannot be reported as a successful, differently
/// sized terminal.
pub(crate) fn validate_size(cols: u16, rows: u16) -> io::Result<()> {
    if cols == 0 || rows == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "PTY window size must have non-zero columns and rows (requested {cols}x{rows})"
            ),
        ));
    }

    #[cfg(windows)]
    if cols > i16::MAX as u16 || rows > i16::MAX as u16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Windows ConPTY window size cannot exceed {} columns or rows (requested {cols}x{rows})",
                i16::MAX
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod size_tests {
    use super::validate_size;

    #[test]
    fn zero_axes_are_invalid_on_every_backend() {
        for (cols, rows) in [(0, 1), (1, 0), (0, 0)] {
            let error = validate_size(cols, rows).expect_err("zero-sized PTY axis must fail");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }

    #[cfg(windows)]
    #[test]
    fn conpty_accepts_its_signed_coord_boundary_and_rejects_larger_values() {
        let max = i16::MAX as u16;
        assert!(validate_size(1, 1).is_ok());
        assert!(validate_size(max, max).is_ok());
        for (cols, rows) in [(max + 1, 1), (1, max + 1), (u16::MAX, u16::MAX)] {
            let error = validate_size(cols, rows).expect_err("ConPTY COORD overflow must fail");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_keeps_the_full_non_zero_u16_range() {
        assert!(validate_size(1, 1).is_ok());
        assert!(validate_size(i16::MAX as u16 + 1, i16::MAX as u16 + 1).is_ok());
        assert!(validate_size(u16::MAX, u16::MAX).is_ok());
    }
}

/// A boxed async reader over the PTY master's **merged** output (stdout+stderr).
/// Flows through the same [`pump_lines_core`](crate::pump) machinery a real
/// child's stdout does — the pump is generic over [`AsyncRead`](tokio::io::AsyncRead).
/// `+ Sync` keeps [`RunningProcess`](crate::RunningProcess) `Sync` when a PTY
/// backend stores it (see [`OutputReader`](crate::running)).
pub(crate) type PtyReader = Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>;

/// A boxed async writer over the PTY master's input side (the child's stdin).
/// `+ Sync` for the same auto-trait-preserving reason as [`PtyReader`].
pub(crate) type PtyWriter = Box<dyn tokio::io::AsyncWrite + Send + Sync + Unpin>;

/// The exit status of a PTY child, in the platform-agnostic shape the
/// [`RunningProcess`](crate::RunningProcess) reap paths consume: an exit code,
/// plus (Unix only) a terminating signal number — mirroring
/// [`std::process::ExitStatus`]'s `code()` / `signal()`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PtyExitStatus {
    code: Option<i32>,
    #[cfg(unix)]
    signal: Option<i32>,
}

impl PtyExitStatus {
    /// The exit code, or `None` when the child was terminated by a signal (Unix)
    /// or produced no numeric code.
    pub(crate) fn code(&self) -> Option<i32> {
        self.code
    }

    /// The terminating signal number, if the child was signalled (Unix only).
    #[cfg(unix)]
    pub(crate) fn signal(&self) -> Option<i32> {
        self.signal
    }

    /// Build from a raw exit code (the Windows `GetExitCodeProcess` value).
    #[cfg(windows)]
    pub(crate) fn from_code(code: Option<i32>) -> Self {
        Self { code }
    }

    /// Build from a reaped Unix [`ExitStatus`](std::process::ExitStatus),
    /// preserving both the code and the terminating signal.
    #[cfg(unix)]
    pub(crate) fn from_std(status: std::process::ExitStatus) -> Self {
        use std::os::unix::process::ExitStatusExt;
        Self {
            code: status.code(),
            signal: status.signal(),
        }
    }
}

/// Everything a PTY spawn hands back to the launch path: the child-lifecycle
/// handle ([`PtyChild`]), the merged output reader, the stdin writer, and the pid.
/// The master fd/handle (and, on Windows, the pseudoconsole and its pipes) are
/// owned by these fields and closed when they drop — the reader/writer own the
/// I/O ends, [`PtyChild`] owns the process handle (and the ConPTY handle on
/// Windows).
pub(crate) struct PtySpawn {
    /// The uniform process-lifecycle handle (wait / try_wait / start_kill).
    pub child: PtyChild,
    /// The child's merged stdout+stderr, read through the standard pump.
    pub reader: PtyReader,
    /// The child's stdin (the master input side).
    pub writer: PtyWriter,
    /// The child's pid, or `None` if it exited before it could be read.
    pub pid: Option<u32>,
}

/// Map an `EIO` read error on a Unix pty master to a clean end-of-stream.
///
/// When the slave side of a pty closes (the child exits), a subsequent `read` on
/// the master returns `EIO` on Linux rather than a `0`-length EOF read. The pump
/// treats a read error as an *incomplete capture* ([`ErrorReason::Io`](crate::ErrorReason::Io));
/// wrapping the master reader in this adapter turns that expected end-of-session
/// `EIO` into a normal EOF so a PTY run reads exactly like a pipe one at close.
/// A genuine mid-stream error of any other kind is passed through unchanged.
#[cfg(unix)]
pub(crate) struct EofOnEio<R>(pub R);

#[cfg(unix)]
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for EofOnEio<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match std::pin::Pin::new(&mut self.0).poll_read(cx, buf) {
            // `EIO` (5) after the slave closed == end of the pty session. Leave
            // `buf` unfilled so the caller sees a 0-byte read, i.e. clean EOF.
            std::task::Poll::Ready(Err(e)) if e.raw_os_error() == Some(libc::EIO) => {
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
