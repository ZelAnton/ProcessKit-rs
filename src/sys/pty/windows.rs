//! Windows PTY backend: a ConPTY pseudoconsole (`CreatePseudoConsole`) attached
//! to the child via a `STARTUPINFOEXW` proc-thread attribute.
//!
//! ConPTY cannot be expressed through `std`/`tokio`'s `Command` (there is no
//! seam to inject a `STARTUPINFOEXW` attribute list, and `raw_attribute` is
//! nightly-only), so the child is spawned with a raw `CreateProcessW`. Crucially
//! this does **not** fork a parallel containment structure (K-032): the child is
//! created `CREATE_SUSPENDED`, `AssignProcessToJobObject`'d to the *same* Job
//! Object every other child joins, then resumed — so kill-on-close reaps the PTY
//! child's whole tree exactly as for a pipe-spawned run.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::process::Command;
use tokio::sync::Notify;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::System::Console::{
    COORD, ClosePseudoConsole, CreatePseudoConsole, GetConsoleWindow, GetStdHandle, HPCON,
    ResizePseudoConsole, STD_ERROR_HANDLE, STD_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    SetStdHandle,
};
use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, INFINITE,
    InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES,
    STARTUPINFOEXW, TerminateProcess, UpdateProcThreadAttribute, WaitForSingleObject,
};

use crate::sys::SpawnOptions;
use crate::sys::pid_gate::PidGate;

use super::{PtyExitStatus, PtyReader, PtySpawn, PtyWriter};

const STILL_ACTIVE: u32 = 259;

/// Build a ConPTY [`COORD`] from a `(cols, rows)` window size. `COORD`'s fields
/// are signed 16-bit, so a size beyond `i16::MAX` (far past any real terminal) is
/// clamped rather than wrapped negative — a defensive cap, never hit in practice.
fn coord(cols: u16, rows: u16) -> COORD {
    let clamp = |v: u16| v.min(i16::MAX as u16) as i16;
    COORD {
        X: clamp(cols),
        Y: clamp(rows),
    }
}

// Conhost reports the client's final console writes asynchronously after its
// process handle becomes signalled. This is a drain quiescence, not a caller
// timeout: closing the pseudoconsole immediately can discard that final frame.
const CONPTY_OUTPUT_QUIESCENCE: Duration = Duration::from_millis(100);

/// Cross-thread observation of bytes the ConPTY render pipe has delivered.
///
/// The process handle can signal before conhost has forwarded the child's final
/// console write. Keeping a monotonic sequence alongside the notification avoids
/// a missed wake-up between the bridge thread's write and an async waiter arming.
#[derive(Default)]
struct OutputActivity {
    sequence: AtomicU64,
    changed: Notify,
}

impl OutputActivity {
    fn record_chunk(&self) {
        self.sequence.fetch_add(1, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Wait until the render pipe has been quiet long enough to close the
    /// pseudoconsole without racing conhost's final frame.
    async fn wait_for_quiescence(&self) {
        let mut sequence = self.sequence.load(Ordering::Acquire);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // `record_chunk` can run between the first load and `notified()`.
            // The second load makes that race visible even though `Notify` does
            // not retain a permit for `notify_waiters`.
            let current = self.sequence.load(Ordering::Acquire);
            if current != sequence {
                sequence = current;
                continue;
            }
            if tokio::time::timeout(CONPTY_OUTPUT_QUIESCENCE, changed)
                .await
                .is_err()
            {
                return;
            }
            sequence = self.sequence.load(Ordering::Acquire);
        }
    }
}

/// An owned Windows process `HANDLE`, closed on drop. Shared behind an `Arc` so a
/// detached exit-wait (a `spawn_blocking` `WaitForSingleObject`) keeps the handle
/// alive even if its `RunningProcess`-side future is dropped (cancelled) — the
/// handle closes only when the last owner drops, never mid-wait.
struct OwnedProcess(HANDLE);

// SAFETY: the handle is owned solely through this `Arc`; every Win32 handle API
// used on it is thread-safe.
unsafe impl Send for OwnedProcess {}
unsafe impl Sync for OwnedProcess {}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the handle was created by `CreateProcessW` and is closed
            // exactly once, when the last `Arc` owner drops.
            unsafe { CloseHandle(self.0) };
        }
    }
}

/// A ConPTY child's process-lifecycle handle: the process handle (shared for
/// cancel-safe waits), the primary thread handle, and the pseudoconsole handle.
/// Dropping it closes the thread handle and the pseudoconsole (which signals the
/// client that the console is gone); the process handle closes with its `Arc`.
pub(crate) struct PtyChild {
    process: Arc<OwnedProcess>,
    thread: HANDLE,
    hpc: HPCON,
    /// Keeps the ConPTY host-input pipe open independently of the public writer.
    /// Closing that pipe asks conhost to close the console; it is not child EOF.
    input_keepalive: Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
    /// Set once the pseudoconsole has been closed, so it is closed exactly once
    /// (by the reap that ends the run, or by `Drop`) and never double-closed.
    hpc_closed: bool,
    output_activity: Arc<OutputActivity>,
    pid: u32,
}

// SAFETY: this is the sole owner of `thread`/`hpc`; the process handle is behind
// a `Send`/`Sync` `Arc`. All the Win32 APIs used are thread-safe.
unsafe impl Send for PtyChild {}
unsafe impl Sync for PtyChild {}

impl PtyChild {
    /// The child's pid. Part of the uniform [`PtyChild`] surface; consumed only by
    /// the Unix shared-group teardown (which signals the direct child by pid),
    /// hence unused on Windows, where teardown kills through the process handle.
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> Option<u32> {
        Some(self.pid)
    }

    /// Wait for exit. The `gate` is not consulted here: a Windows pid is not
    /// recycled while we hold the process handle (which outlives this wait via the
    /// `Arc`), so there is no pid-freeing reap to fuse with a retire — the caller
    /// (`backend_wait`) retires the gate after this returns, before the handle is
    /// ever closed on drop, so a detached watchdog's raw kill can never race a
    /// freed pid.
    pub(crate) async fn reap(&mut self, _gate: &PidGate) -> io::Result<PtyExitStatus> {
        self.wait().await
    }

    /// Wait for the child to exit and read its exit code. The process handle is
    /// cloned into the blocking task, so cancelling this future never closes a
    /// handle the task is still waiting on.
    ///
    /// Closes the pseudoconsole as soon as the child exits: on Windows the output
    /// pipe does NOT reach EOF when the child dies (conhost holds the write end
    /// until the pseudoconsole is closed), so without this the merged-output pump
    /// would block until teardown and never flush the final line. Closing here
    /// flushes conhost's remaining output and EOFs the reader promptly, so a
    /// capture verb sees the whole output and finishes without waiting out the
    /// pump-teardown grace.
    pub(crate) async fn wait(&mut self) -> io::Result<PtyExitStatus> {
        let process = Arc::clone(&self.process);
        let code = tokio::task::spawn_blocking(move || {
            // SAFETY: `process.0` is a valid process handle held alive by the
            // cloned `Arc` for the whole wait; `INFINITE` blocks until exit.
            unsafe { WaitForSingleObject(process.0, INFINITE) };
            let mut code: u32 = 0;
            // SAFETY: `process.0` is still valid; `code` is an owned out-param.
            let ok = unsafe { GetExitCodeProcess(process.0, &mut code) };
            (ok != 0).then_some(code)
        })
        .await
        .map_err(io::Error::other)?;
        // The input pipe is a ConPTY session-lifetime resource. Only release our
        // keepalive once the client process is already gone; releasing it while
        // conhost is starting can turn the broken pipe into CTRL_CLOSE_EVENT.
        self.input_keepalive.take();
        // The child has exited, but conhost can still have its final rendered
        // frame in flight. Let the bridge drain to quiescence before requesting
        // EOF; the reader stays live throughout, as ConPTY requires for teardown.
        self.output_activity.wait_for_quiescence().await;
        // Now close the pseudoconsole so `output_read` drains to EOF (see above).
        self.close_pty();
        Ok(PtyExitStatus::from_code(code.map(|c| c as i32)))
    }

    /// Close the pseudoconsole exactly once (idempotent). Flushes conhost's
    /// remaining output and lets the merged-output reader see EOF.
    fn close_pty(&mut self) {
        if !self.hpc_closed {
            self.hpc_closed = true;
            // SAFETY: `hpc` is a live pseudoconsole handle, closed exactly once.
            unsafe { ClosePseudoConsole(self.hpc) };
        }
    }

    /// Resize the ConPTY pseudoconsole to `cols`×`rows` via `ResizePseudoConsole`.
    /// conhost reflows its screen buffer and the client observes the new geometry
    /// through the console API (`GetConsoleScreenBufferInfo`) — the live-resize
    /// half of [`Command::pty_size`](crate::Command::pty_size).
    ///
    /// `&self`: `ResizePseudoConsole` takes the console handle by value and mutates
    /// no Rust-side state. The caller
    /// ([`RunningProcess::resize_pty`](crate::RunningProcess)) gates on the child
    /// still running, so the pseudoconsole has not yet been closed by a reap.
    ///
    /// Windows note: unlike Unix `TIOCSWINSZ` there is no `SIGWINCH`; a console
    /// client learns of the change on its next console query, and conhost may
    /// reflow asynchronously — the resize is delivered best-effort, not
    /// synchronously observable at the returned instant.
    pub(crate) fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
        // SAFETY: `hpc` is a live pseudoconsole handle for the call's duration —
        // the caller gated on the child still running, so no reap/`Drop` has closed
        // it. `ResizePseudoConsole` returns an `HRESULT`; non-zero is a failure.
        let hr = unsafe { ResizePseudoConsole(self.hpc, coord(cols, rows)) };
        if hr != 0 {
            return Err(io::Error::from_raw_os_error(hr));
        }
        Ok(())
    }

    /// Non-blocking exit poll.
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<PtyExitStatus>> {
        // SAFETY: a valid process handle; a 0 timeout polls without blocking.
        let waited = unsafe { WaitForSingleObject(self.process.0, 0) };
        if waited != WAIT_OBJECT_0 {
            return Ok(None);
        }
        let mut code: u32 = 0;
        // SAFETY: valid handle; owned out-param.
        let ok = unsafe { GetExitCodeProcess(self.process.0, &mut code) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // A process that raced to exit at code 259 reads as still-active for one
        // more poll — the same rare ambiguity the rest of the crate accepts.
        if code == STILL_ACTIVE {
            return Ok(None);
        }
        Ok(Some(PtyExitStatus::from_code(Some(code as i32))))
    }

    /// Terminate the child through the owned handle (no raw pid kill needed).
    /// Best-effort and idempotent: terminating an already-exited process fails
    /// with access-denied, which is treated as a successful no-op.
    pub(crate) fn start_kill(&mut self) -> io::Result<()> {
        // SAFETY: a valid process handle; exit code 1 for a forced termination.
        let ok = unsafe { TerminateProcess(self.process.0, 1) };
        if ok == 0 {
            // Already gone → nothing to kill; any other failure is best-effort
            // here (teardown also reaps and the job kill-on-close backstops it).
            return Ok(());
        }
        Ok(())
    }
}

impl Drop for PtyChild {
    fn drop(&mut self) {
        // Close the pseudoconsole (unless a reap already did) — for a handle
        // dropped without a consuming reap (e.g. an own-group teardown that kills
        // via the job), this signals conhost to shut down. Then release the thread
        // handle. The process handle closes with its `Arc` (possibly after a
        // detached wait finishes).
        self.close_pty();
        // Close the input bridge only after requesting pseudoconsole teardown.
        // A separately-owned ProcessStdin may keep it alive until that writer is
        // dropped, just like an ordinary child-stdin handle.
        self.input_keepalive.take();
        // SAFETY: `thread` is owned solely here and closed exactly once.
        unsafe {
            if !self.thread.is_null() {
                CloseHandle(self.thread);
            }
        }
    }
}

/// Close a raw handle, tolerating a null.
///
/// # Safety
///
/// `handle` must be a valid, still-open handle (or null), closed at most once.
unsafe fn close(handle: HANDLE) {
    if !handle.is_null() {
        // SAFETY: forwarded from the caller's contract above.
        unsafe { CloseHandle(handle) };
    }
}

/// Append `arg` to a `CreateProcessW` command line using the MSVCRT quoting
/// rules `std` applies internally (which it does not expose): quote when the arg
/// is empty or holds whitespace, double any run of backslashes that precedes a
/// quote (or the closing quote), and escape embedded quotes.
fn append_arg(out: &mut Vec<u16>, arg: &OsStr, force_quote: bool) {
    let wide: Vec<u16> = arg.encode_wide().collect();
    let quote = force_quote
        || wide.is_empty()
        || wide
            .iter()
            .any(|&c| c == u16::from(b' ') || c == u16::from(b'\t'));
    if quote {
        out.push(u16::from(b'"'));
    }
    let mut backslashes = 0usize;
    for &c in &wide {
        if c == u16::from(b'\\') {
            backslashes += 1;
        } else {
            if c == u16::from(b'"') {
                // 2n+1 backslashes before an embedded quote: the n already
                // pushed in the loop, plus n+1 here.
                for _ in 0..=backslashes {
                    out.push(u16::from(b'\\'));
                }
            }
            backslashes = 0;
        }
        out.push(c);
    }
    if quote {
        // Double a trailing backslash run so it is not read as escaping the
        // closing quote.
        for _ in 0..backslashes {
            out.push(u16::from(b'\\'));
        }
        out.push(u16::from(b'"'));
    }
}

/// Build the NUL-terminated UTF-16 command line for the child from the resolved
/// program and its arguments (read back from the tokio `Command`).
fn build_command_line(cmd: &Command) -> Vec<u16> {
    let std_cmd = cmd.as_std();
    let mut line: Vec<u16> = Vec::new();
    append_arg(&mut line, std_cmd.get_program(), true);
    for arg in std_cmd.get_args() {
        line.push(u16::from(b' '));
        append_arg(&mut line, arg, false);
    }
    line.push(0);
    line
}

/// Build the double-NUL-terminated UTF-16 environment block, or `None` to inherit
/// the parent environment unchanged.
fn build_env_block(env: Option<Vec<(OsString, OsString)>>) -> Option<Vec<u16>> {
    let pairs = env?;
    let mut block: Vec<u16> = Vec::new();
    for (k, v) in pairs {
        block.extend(k.encode_wide());
        block.push(u16::from(b'='));
        block.extend(v.encode_wide());
        block.push(0);
    }
    // The block ends with an extra NUL; an empty block is just "\0\0".
    block.push(0);
    if block.len() == 1 {
        block.push(0);
    }
    Some(block)
}

/// A NUL-terminated wide string for a `PCWSTR` argument.
fn to_wide_nul(s: &OsStr) -> Vec<u16> {
    let mut v: Vec<u16> = s.encode_wide().collect();
    v.push(0);
    v
}

/// Whether the launching process is itself attached to a console.
///
/// `GetConsoleWindow` returns null exactly when this process has no console — the
/// *headless* case: a service-hosted CI runner step, or a windowed (GUI-subsystem)
/// parent. It is non-null under an interactive terminal (a developer's
/// `cargo test` from a console).
///
/// A ConPTY child must never take its standard handles from the launcher — those
/// must come from the pseudoconsole — but the two launch environments empirically
/// need **different** ways to guarantee that (a split this crate's own Windows CI
/// vs. local runs exposed), so this selects between them:
///   * **Console-attached launcher:** [`conpty_startup_info`] requests
///     `STARTF_USESTDHANDLES` with three null handles, severing the launcher's
///     console std handles so the child binds to the pseudoconsole instead of
///     rendering into the launcher's terminal.
///   * **Headless launcher:** `STARTF_USESTDHANDLES` is *not* requested (per the
///     Microsoft ConPTY sample, which lets the pseudoconsole own the handles);
///     the launcher's own (redirected) std handles are instead temporarily nulled
///     across `CreateProcessW` (see [`NulledLauncherStdio`]) so the child
///     propagates null defaults the pseudoconsole overrides. Requesting
///     `STARTF_USESTDHANDLES` here is what stranded the child with no console
///     output binding on headless CI (only conhost's own initial frame reached
///     the master, never the child's writes).
fn launcher_has_console() -> bool {
    // SAFETY: `GetConsoleWindow` takes no arguments and only reads this process's
    // console association; the returned `HWND` is inspected, never dereferenced.
    !unsafe { GetConsoleWindow() }.is_null()
}

/// Build startup information for the ConPTY child.
///
/// When `sever_console_std_handles` is set (a console-attached launcher — see
/// [`launcher_has_console`]), request `STARTF_USESTDHANDLES` with three null
/// standard handles: `bInheritHandles = FALSE` alone does not stop a
/// console-attached launcher (a debugger, test runner, or terminal) from
/// pre-populating the child's three standard-handle slots from its own console,
/// which would let the child keep writing to the *launcher's* console while the
/// pseudoconsole attaches; nulling the slots severs that so ConPTY installs its
/// own console handles during process initialization.
///
/// When it is **clear** (a headless launcher), leave `STARTF_USESTDHANDLES`
/// unset: the pseudoconsole then owns the child's standard handles (the Microsoft
/// ConPTY sample's arrangement). Setting it with null handles instead strands the
/// child with no console output binding on some Windows builds (the headless-CI
/// failure); the launcher-side null in [`NulledLauncherStdio`] severs inheritance
/// there without this flag.
fn conpty_startup_info(
    attr_list: LPPROC_THREAD_ATTRIBUTE_LIST,
    sever_console_std_handles: bool,
) -> STARTUPINFOEXW {
    let mut si = STARTUPINFOEXW::default();
    si.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    if sever_console_std_handles {
        si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        si.StartupInfo.hStdInput = std::ptr::null_mut();
        si.StartupInfo.hStdOutput = std::ptr::null_mut();
        si.StartupInfo.hStdError = std::ptr::null_mut();
    }
    si.lpAttributeList = attr_list;
    si
}

/// Temporarily replaces the *launcher's* own three standard handles with null for
/// the span of a `CreateProcessW`, restoring them on drop.
///
/// Used only on a headless launcher (see [`launcher_has_console`]), where the
/// ConPTY child is created **without** `STARTF_USESTDHANDLES` so the pseudoconsole
/// owns its handles. Without `STARTF_USESTDHANDLES`, `CreateProcessW` propagates
/// the launcher's `ProcessParameters` standard-handle *values* into the child, so
/// a launcher whose own stdio is redirected (a CI step capturing output to a pipe)
/// would have the child inherit that redirect and write past the pseudoconsole
/// entirely. Nulling the launcher's slots for the spawn makes the child propagate
/// null defaults, which the pseudoconsole then overrides with its own console
/// handles during initialization — the same end state the console-launcher path
/// reaches via `STARTF_USESTDHANDLES`, but without the flag that stranded the
/// child on headless CI.
///
/// A process-wide lock is held for the whole window because `SetStdHandle` mutates
/// process-global state; the swap is confined to the (fast, synchronous)
/// `CreateProcessW` call and never spans an `.await`.
struct NulledLauncherStdio {
    saved: [(STD_HANDLE, HANDLE); 3],
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl NulledLauncherStdio {
    fn install() -> Self {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let lock = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut saved = [
            (STD_INPUT_HANDLE, std::ptr::null_mut()),
            (STD_OUTPUT_HANDLE, std::ptr::null_mut()),
            (STD_ERROR_HANDLE, std::ptr::null_mut()),
        ];
        for (which, prev) in &mut saved {
            // SAFETY: `Get`/`SetStdHandle` only read/replace this process's own
            // standard-handle slots; the previous value is captured for restore.
            unsafe {
                *prev = GetStdHandle(*which);
                SetStdHandle(*which, std::ptr::null_mut());
            }
        }
        Self { saved, _lock: lock }
    }
}

impl Drop for NulledLauncherStdio {
    fn drop(&mut self) {
        for (which, prev) in self.saved {
            // SAFETY: restores the exact handle value captured in `install`, on the
            // same process-global slot, still under the held lock.
            unsafe { SetStdHandle(which, prev) };
        }
    }
}

/// Spawn `cmd` under a ConPTY, assigning the child to `job` for containment.
///
/// `env` is the child's resolved environment (see
/// [`Command::resolved_pty_env`](crate::Command)); `skip_drop_kill` is the job's
/// kill-on-close latch, cleared on successful containment so a prior
/// survivor-sparing shutdown does not spare this fresh child.
pub(crate) fn spawn_pty(
    cmd: &mut Command,
    opts: &SpawnOptions,
    env: Option<Vec<(OsString, OsString)>>,
    job: HANDLE,
    skip_drop_kill: &crate::sys::SkipDropKill,
) -> io::Result<PtySpawn> {
    // Two pipes: the child's input (we write `input_write`) and output (we read
    // `output_read`). The ConPTY takes ownership of `input_read`/`output_write`.
    let mut input_read: HANDLE = std::ptr::null_mut();
    let mut input_write: HANDLE = std::ptr::null_mut();
    let mut output_read: HANDLE = std::ptr::null_mut();
    let mut output_write: HANDLE = std::ptr::null_mut();
    let sec: *const SECURITY_ATTRIBUTES = std::ptr::null();
    // SAFETY: valid out-pointers; null attributes request default (non-inheritable)
    // pipes, which is correct — the child gets its console from the pseudoconsole
    // attribute, not from inherited handles.
    if unsafe { CreatePipe(&mut input_read, &mut input_write, sec, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { CreatePipe(&mut output_read, &mut output_write, sec, 0) } == 0 {
        let e = io::Error::last_os_error();
        unsafe {
            close(input_read);
            close(input_write);
        }
        return Err(e);
    }

    // Create the pseudoconsole over the child's ends. It duplicates them, but our
    // copies are kept open **through `CreateProcessW`**: conhost attaches to the
    // pseudoconsole at process-creation time (via the attribute), so closing the
    // child's ends before that would leave the render pipe with no writer and the
    // output empty. They are closed only on the success path below (and by the
    // error cleanups) — see `close_child_ends`.
    let (cols, rows) = opts.pty_size.unwrap_or(super::DEFAULT_PTY_SIZE);
    let size = coord(cols, rows);
    // `HPCON` is an `isize` handle in windows-sys, not a pointer.
    let mut hpc: HPCON = 0;
    // SAFETY: valid pipe handles and an out-pointer for the pseudoconsole handle.
    let hr = unsafe { CreatePseudoConsole(size, input_read, output_write, 0, &mut hpc) };
    if hr != 0 {
        // CreatePseudoConsole failed → it did not dup; we still own all four ends.
        unsafe {
            close(input_read);
            close(input_write);
            close(output_read);
            close(output_write);
        }
        return Err(io::Error::from_raw_os_error(hr));
    }
    // Close all resources created so far (pseudoconsole + every pipe end) — the
    // cleanup shared by the setup error paths between here and a successful spawn.
    let cleanup_all = || unsafe {
        ClosePseudoConsole(hpc);
        close(input_read);
        close(input_write);
        close(output_read);
        close(output_write);
    };

    // Build the STARTUPINFOEXW carrying the pseudoconsole attribute.
    let mut attr_size: usize = 0;
    // SAFETY: the documented size-probe call — a null list with the real count
    // writes the required byte size into `attr_size` (and "fails" by design).
    unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut attr_size) };
    let mut attr_buf = vec![0u8; attr_size];
    let attr_list = attr_buf.as_mut_ptr().cast();
    // SAFETY: `attr_buf` is `attr_size` bytes; initializing a 1-attribute list.
    if unsafe { InitializeProcThreadAttributeList(attr_list, 1, 0, &mut attr_size) } == 0 {
        let e = io::Error::last_os_error();
        cleanup_all();
        return Err(e);
    }
    // SAFETY: a valid attribute list; for `PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE` the
    // HPCON **value** itself is the `lpValue` (not a pointer to it), with size
    // `sizeof(HPCON)` — matching the Microsoft ConPTY sample. Passing `&hpc` here
    // instead silently leaves the child without a console (empty output).
    let updated = unsafe {
        UpdateProcThreadAttribute(
            attr_list,
            0,
            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
            hpc as *const std::ffi::c_void,
            std::mem::size_of::<HPCON>(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if updated == 0 {
        let e = io::Error::last_os_error();
        unsafe { DeleteProcThreadAttributeList(attr_list) };
        cleanup_all();
        return Err(e);
    }

    // Bind the child's standard handles to the pseudoconsole, never the launcher's.
    // A console-attached launcher and a headless one need different guarantees for
    // that (see `launcher_has_console`): the former severs its console std handles
    // via `STARTF_USESTDHANDLES`; the latter must NOT set that flag (it stranded the
    // child with no output on headless CI) and instead nulls its own std handles
    // across `CreateProcessW` below so the child propagates pseudoconsole-overridable
    // null defaults rather than inheriting the launcher's redirected stdio.
    let has_console = launcher_has_console();
    let si = conpty_startup_info(attr_list, has_console);

    let mut command_line = build_command_line(cmd);
    let env_block = build_env_block(env);
    let cwd_wide = cmd
        .as_std()
        .get_current_dir()
        .map(|p| to_wide_nul(p.as_os_str()));

    // Suspended containment: create suspended, assign to the job, resume — the
    // same race-free sequence `Job::spawn` uses. `EXTENDED_STARTUPINFO_PRESENT`
    // activates the pseudoconsole attribute list.
    //
    let mut flags = CREATE_SUSPENDED | EXTENDED_STARTUPINFO_PRESENT | opts.creation_flags;
    if opts.windows_new_process_group {
        flags |= CREATE_NEW_PROCESS_GROUP;
    }
    let (env_ptr, env_flag): (*const std::ffi::c_void, u32) = match &env_block {
        Some(block) => (block.as_ptr().cast(), CREATE_UNICODE_ENVIRONMENT),
        None => (std::ptr::null(), 0),
    };
    flags |= env_flag;
    let cwd_ptr = cwd_wide.as_ref().map_or(std::ptr::null(), |w| w.as_ptr());

    let mut pi: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let created = {
        // Headless launcher only: null the launcher's own (possibly redirected)
        // std handles across the spawn so the child does not propagate them and
        // instead binds to the pseudoconsole. Restored the instant this guard drops
        // (end of block); a no-op for a console launcher, which severs via the
        // startup info above instead.
        let _nulled_stdio = (!has_console).then(NulledLauncherStdio::install);
        // SAFETY: `command_line` is a mutable NUL-terminated wide buffer
        // (CreateProcessW may write to it); the startup info's `cb` and attribute
        // list are set; the env block (when present) is double-NUL terminated with
        // the matching flag.
        unsafe {
            CreateProcessW(
                std::ptr::null(),
                command_line.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                0, // no ordinary handle inheritance; the child's standard handles
                // come from the pseudoconsole (severed from the launcher above)
                flags,
                env_ptr,
                cwd_ptr,
                std::ptr::from_ref(&si.StartupInfo),
                &mut pi,
            )
        }
    };
    // The attribute list is no longer needed once the child is created.
    unsafe { DeleteProcThreadAttributeList(attr_list) };
    if created == 0 {
        let e = io::Error::last_os_error();
        cleanup_all();
        return Err(e);
    }
    // Now that the child (and its conhost) exist, release our copies of the child's
    // pipe ends: conhost holds its own duplicates, so the render pipe still has a
    // writer (giving `output_read` data, and EOF only once the pseudoconsole is
    // closed at teardown). Keeping them past here would keep `output_read` from
    // ever seeing EOF.
    unsafe {
        close(input_read);
        close(output_write);
    }

    // Contain before resuming: assign to the same job, then release the primary
    // thread. On assign failure the suspended child is terminated and everything
    // is cleaned up — never an uncontained leak.
    // SAFETY: `pi.hProcess` is the freshly-created (suspended) child; `job` is a
    // valid job handle for the caller's lifetime.
    if unsafe { AssignProcessToJobObject(job, pi.hProcess) } == 0 {
        let e = io::Error::last_os_error();
        unsafe {
            TerminateProcess(pi.hProcess, 1);
            close(pi.hProcess);
            close(pi.hThread);
            ClosePseudoConsole(hpc);
            close(input_write);
            close(output_read);
        }
        return Err(e);
    }
    // A fresh killable member joined the job — re-arm kill-on-close so a prior
    // survivor-sparing shutdown does not spare it (mirrors `Job::spawn`).
    skip_drop_kill.clear();
    // SAFETY: resuming the primary thread of the now-contained child.
    unsafe { ResumeThread(pi.hThread) };
    let pid = pi.dwProcessId;
    // `output_read` (merged stdout+stderr) and `input_write` (stdin) are
    // *synchronous* anonymous pipes; tokio has no async adapter for them, so each
    // is driven by a dedicated OS thread doing blocking `ReadFile`/`WriteFile` and
    // bridged to async over a channel (the established pattern for blocking Windows
    // pipes). Acceptable for the minimal, low-volume interactive PTY mode.
    let output_activity = Arc::new(OutputActivity::default());
    let reader: PtyReader = Box::new(bridge_reader(output_read, Arc::clone(&output_activity)));
    let (writer, input_keepalive) = bridge_writer(input_write);
    let writer: PtyWriter = Box::new(writer);

    Ok(PtySpawn {
        child: PtyChild {
            process: Arc::new(OwnedProcess(pi.hProcess)),
            thread: pi.hThread,
            hpc,
            input_keepalive: Some(input_keepalive),
            hpc_closed: false,
            output_activity,
            pid,
        },
        reader,
        writer,
        pid: Some(pid),
    })
}

/// A raw handle moved into a bridge thread that owns it exclusively.
struct SendHandle(HANDLE);
// SAFETY: the handle is owned solely by the bridge thread it is moved into.
unsafe impl Send for SendHandle {}

impl SendHandle {
    /// Take ownership of the handle as a blocking [`std::fs::File`] (which closes
    /// it on drop). Consuming `self` forces the bridge closure to capture the whole
    /// `Send` wrapper rather than the bare (non-`Send`) `*mut c_void` field.
    ///
    /// # Safety
    ///
    /// The wrapped handle must be a valid, open handle owned by the caller.
    unsafe fn into_file(self) -> std::fs::File {
        // SAFETY: forwarded from the caller's contract; ownership transfers to the
        // returned `File`.
        unsafe { std::fs::File::from_raw_handle(self.0 as _) }
    }
}

/// Bridge a synchronous read pipe to async: a dedicated OS thread blocking-reads
/// the pipe and forwards chunks over a bounded channel (whose fullness
/// backpressures the reader thread, and thus the child). A `0`-length read (EOF,
/// on pseudoconsole close) or an error ends the thread.
fn bridge_reader(handle: HANDLE, output_activity: Arc<OutputActivity>) -> ChannelReader {
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    let h = SendHandle(handle);
    std::thread::spawn(move || {
        use std::io::Read;
        // SAFETY: the handle is owned by this thread; the `File` closes it on drop.
        let mut file = unsafe { h.into_file() };
        let mut buf = [0u8; 8192];
        loop {
            match file.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    output_activity.record_chunk();
                    if tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    ChannelReader {
        rx: std::sync::Mutex::new(rx),
        leftover: Vec::new(),
        pos: 0,
    }
}

/// Bridge a synchronous write pipe to async: a dedicated OS thread blocking-writes
/// each chunk received over an unbounded channel.
///
/// The returned sender clone is owned by [`PtyChild`] for the session lifetime.
/// ConPTY treats a closed host-input pipe as a request to close the pseudoconsole,
/// so a public writer ending must not be what closes the underlying pipe.
fn bridge_writer(handle: HANDLE) -> (ChannelWriter, tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let h = SendHandle(handle);
    std::thread::spawn(move || {
        use std::io::Write;
        // SAFETY: the handle is owned by this thread; the `File` closes it on drop.
        let mut file = unsafe { h.into_file() };
        while let Some(chunk) = rx.blocking_recv() {
            if file.write_all(&chunk).is_err() {
                break;
            }
            let _ = file.flush();
        }
    });
    let keepalive = tx.clone();
    (
        ChannelWriter {
            tx,
            shutdown: false,
        },
        keepalive,
    )
}

/// The async read half of the pipe bridge: drains `Vec<u8>` chunks from the reader
/// thread's channel, carrying any partial chunk across polls.
///
/// The `Receiver` is wrapped in a `Mutex` purely so [`ChannelReader`] is `Sync`
/// (a `tokio` mpsc `Receiver` is `Send` but not `Sync`), which keeps the boxed
/// [`OutputReader`](crate::running) — and thus [`RunningProcess`](crate::RunningProcess) —
/// `Sync`. The lock is uncontended (only `poll_read` touches it, and a reader is
/// polled from one task) and never held across an `.await`.
struct ChannelReader {
    rx: std::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    leftover: Vec<u8>,
    pos: usize,
}

impl AsyncRead for ChannelReader {
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
            .expect("pty reader mutex poisoned")
            .poll_recv(cx);
        match poll {
            Poll::Ready(Some(chunk)) => {
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
            // The reader thread ended (pipe EOF) — clean end of stream.
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The async write half of the pipe bridge: hands each write to the writer
/// thread's channel. Writes are buffered by the channel (small interactive input),
/// so `poll_write` never blocks the runtime.
struct ChannelWriter {
    tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    shutdown: bool,
}

impl ChannelWriter {
    /// Deliver the Windows console EOF gesture without closing ConPTY's host
    /// input pipe. `Console.ReadLine` recognizes Ctrl-Z followed by Enter as EOF.
    fn send_eof(&mut self) -> io::Result<()> {
        if self.shutdown {
            return Ok(());
        }
        self.shutdown = true;
        self.tx
            .send(vec![0x1a, b'\r'])
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "pty stdin writer closed"))
    }
}

impl AsyncWrite for ChannelWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.shutdown {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pty stdin writer closed",
            )));
        }
        match self.tx.send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pty stdin writer closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.send_eof())
    }
}

impl Drop for ChannelWriter {
    fn drop(&mut self) {
        // Drop has no error channel; an exited child routinely closes its input
        // before its writer is released, so a failed best-effort EOF is benign.
        let _ = self.send_eof();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conpty_startup_info_severs_std_handles_only_when_requested() {
        // Console-attached launcher: sever the inherited console std handles.
        let severed = conpty_startup_info(std::ptr::null_mut(), true);
        assert_eq!(
            severed.StartupInfo.dwFlags & STARTF_USESTDHANDLES,
            STARTF_USESTDHANDLES
        );
        assert!(severed.StartupInfo.hStdInput.is_null());
        assert!(severed.StartupInfo.hStdOutput.is_null());
        assert!(severed.StartupInfo.hStdError.is_null());

        // Headless launcher: leave the std handles to the pseudoconsole — no
        // `STARTF_USESTDHANDLES`, so the null slots are not consulted at all.
        let inherited = conpty_startup_info(std::ptr::null_mut(), false);
        assert_eq!(inherited.StartupInfo.dwFlags & STARTF_USESTDHANDLES, 0);
    }

    #[test]
    fn coord_maps_a_window_size_and_clamps_beyond_i16() {
        // An ordinary size passes through unchanged.
        let c = coord(120, 40);
        assert_eq!((c.X, c.Y), (120, 40));

        // The default is the historical 80×24.
        let d = coord(
            super::super::DEFAULT_PTY_SIZE.0,
            super::super::DEFAULT_PTY_SIZE.1,
        );
        assert_eq!((d.X, d.Y), (80, 24));

        // A size past `i16::MAX` (never a real terminal) is clamped, not wrapped
        // negative — a defensive guard on the signed `COORD` fields.
        let big = coord(u16::MAX, 40_000);
        assert_eq!((big.X, big.Y), (i16::MAX, i16::MAX));
    }
}
