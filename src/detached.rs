//! [`DetachedChild`] — the handle returned by
//! [`Command::spawn_detached`](crate::Command::spawn_detached) for a child
//! deliberately released from this crate's kill-on-drop containment.

#[cfg(unix)]
mod reaper {
    use std::io;
    use std::process::Child;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::Duration;

    const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(10);
    const MAX_POLL_INTERVAL: Duration = Duration::from_secs(1);

    enum ReaperMessage {
        Probe,
        Child(Child),
    }

    struct ReaperState<T> {
        sender: Option<Sender<T>>,
    }

    struct ReaperSendError<T> {
        message: T,
        source: io::Error,
    }

    impl<T> ReaperState<T> {
        const fn new() -> Self {
            Self { sender: None }
        }

        fn start_with(
            &mut self,
            start: &mut impl FnMut(Receiver<T>) -> io::Result<()>,
        ) -> io::Result<()> {
            if self.sender.is_some() {
                return Ok(());
            }

            let (sender, receiver) = mpsc::channel();
            start(receiver)?;
            self.sender = Some(sender);
            Ok(())
        }

        /// Send through the current manager, replacing one receiver that closed
        /// after an earlier successful start. The returned error keeps ownership
        /// of the unsent message when even the replacement cannot accept it.
        fn send_with(
            &mut self,
            mut message: T,
            mut start: impl FnMut(Receiver<T>) -> io::Result<()>,
        ) -> Result<(), ReaperSendError<T>> {
            for attempt in 0..=1 {
                if let Err(source) = self.start_with(&mut start) {
                    return Err(ReaperSendError { message, source });
                }

                let sender = self.sender.as_ref().expect("reaper sender installed");
                match sender.send(message) {
                    Ok(()) => return Ok(()),
                    Err(mpsc::SendError(returned)) => {
                        message = returned;
                        self.sender = None;
                        if attempt == 1 {
                            return Err(ReaperSendError {
                                message,
                                source: io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    "detached reaper channel disconnected after restart",
                                ),
                            });
                        }
                    }
                }
            }

            unreachable!("reaper send performs one initial and one replacement attempt")
        }
    }

    // One manager owns every detached `Child`. Keeping the `Child` in this
    // process is what makes it the only path allowed to consume the child's
    // wait status; the caller only ever receives the stable spawn-time pid.
    static REAPER: OnceLock<Mutex<ReaperState<ReaperMessage>>> = OnceLock::new();

    fn reaper_state() -> &'static Mutex<ReaperState<ReaperMessage>> {
        REAPER.get_or_init(|| Mutex::new(ReaperState::new()))
    }

    fn lock_reaper<T>(reaper: &Mutex<ReaperState<T>>) -> MutexGuard<'_, ReaperState<T>> {
        // `handoff` may already own the sole `Child`; poison cannot abort reaping it.
        reaper
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn start_reaper(receiver: Receiver<ReaperMessage>) -> io::Result<()> {
        std::thread::Builder::new()
            .name("processkit-detached-reaper".into())
            .spawn(move || reap_loop(receiver))
            .map(|_| ())
            .map_err(|source| {
                io::Error::other(format!("could not start detached reaper: {source}"))
            })
    }

    /// Start the manager before spawning the child, so a thread-resource failure
    /// cannot leave an already-launched child without a reaping owner.
    pub(super) fn prepare() -> io::Result<()> {
        let mut state = lock_reaper(reaper_state());
        state
            .send_with(ReaperMessage::Probe, start_reaper)
            .map_err(|error| error.source)
    }

    fn is_already_reaped(source: &io::Error) -> bool {
        source.raw_os_error() == Some(libc::ECHILD)
    }

    /// Wait for a child without ever dropping it after a retryable wait attempt.
    ///
    /// `Child::wait` normally retries interruption internally, but retaining the
    /// handle here also covers a handoff failure that leaves this thread as the
    /// last possible owner. The child is dropped only after a successful wait.
    fn wait_until_reaped(mut child: Child) -> Option<io::Error> {
        let mut first_error = None;
        loop {
            match child.wait() {
                Ok(_) => return first_error,
                Err(source) if is_already_reaped(&source) => return first_error,
                Err(source) => {
                    first_error.get_or_insert(source);
                    std::thread::sleep(INITIAL_POLL_INTERVAL);
                }
            }
        }
    }

    fn reap_finished(children: &mut Vec<Child>) {
        let mut index = 0;
        while index < children.len() {
            match children[index].try_wait() {
                Ok(Some(_)) => {
                    // `try_wait` has already reaped the child on Unix. Calling
                    // `wait` on this same owner observes that completed status
                    // and makes the ownership handoff explicit to Clippy.
                    let _ = children.swap_remove(index).wait();
                }
                Err(source) if is_already_reaped(&source) => {
                    // A process-wide reaper or an auto-reaping SIGCHLD
                    // disposition has already consumed this status. Keeping the
                    // handle cannot make a later wait succeed.
                    drop(children.swap_remove(index));
                }
                Ok(None) | Err(_) => {
                    // A transient wait error must not transfer ownership or
                    // drop the handle; retrying keeps this manager the sole
                    // reaping path.
                    index += 1;
                }
            }
        }
    }

    /// Preserve a handoff error after synchronously reclaiming the wait owner.
    ///
    /// The fallback intentionally does not return until `child.wait()` succeeds or
    /// reports that no waitable child remains: returning after a retryable error
    /// while retaining no `Child` would make Unix process reaping a best-effort
    /// operation and could leave a zombie behind. A transient wait error is
    /// included in the returned error after the child reaches a terminal wait
    /// outcome, so callers do not mistake a failed handoff for a successful spawn.
    /// `ECHILD` is terminal because another reaper or the SIGCHLD disposition has
    /// already consumed the status.
    fn finish_failed_handoff(child: Child, handoff_error: io::Error) -> io::Error {
        match wait_until_reaped(child) {
            None => handoff_error,
            Some(wait_error) => io::Error::other(format!(
                "{handoff_error}; fallback wait initially failed: {wait_error}"
            )),
        }
    }

    /// Transfer the only `Child` handle to the manager. A receiver that stopped
    /// after `prepare` is replaced before the synchronous fallback is considered.
    pub(super) fn handoff(child: Child) -> io::Result<()> {
        let result = {
            let mut state = lock_reaper(reaper_state());
            state.send_with(ReaperMessage::Child(child), start_reaper)
        };

        match result {
            Ok(()) => Ok(()),
            Err(ReaperSendError {
                message: ReaperMessage::Child(child),
                source,
            }) => Err(finish_failed_handoff(child, source)),
            Err(ReaperSendError {
                message: ReaperMessage::Probe,
                ..
            }) => unreachable!("handoff always sends a child"),
        }
    }

    fn receive(message: ReaperMessage, children: &mut Vec<Child>) -> bool {
        match message {
            ReaperMessage::Child(child) => {
                children.push(child);
                true
            }
            ReaperMessage::Probe => false,
        }
    }

    fn next_poll_interval(current: Duration) -> Duration {
        current.saturating_mul(2).min(MAX_POLL_INTERVAL)
    }

    fn reap_loop(receiver: Receiver<ReaperMessage>) {
        reap_loop_with(receiver, reap_finished);
    }

    fn reap_loop_with(receiver: Receiver<ReaperMessage>, mut reap: impl FnMut(&mut Vec<Child>)) {
        let mut children = Vec::new();
        let mut poll_interval = INITIAL_POLL_INTERVAL;
        loop {
            if children.is_empty() {
                poll_interval = INITIAL_POLL_INTERVAL;
                match receiver.recv() {
                    Ok(message) => {
                        receive(message, &mut children);
                    }
                    Err(_) => return,
                }
            } else {
                match receiver.recv_timeout(poll_interval) {
                    Ok(message) => {
                        if receive(message, &mut children) {
                            // A new child needs the short first interval so a
                            // just-launched process cannot sit unobserved at
                            // the current long backoff.
                            poll_interval = INITIAL_POLL_INTERVAL;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        reap(&mut children);
                        poll_interval = next_poll_interval(poll_interval);
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // The sender is process-global and normally never drops;
                        // if that invariant changes, finish every owned child
                        // before the manager exits.
                        for child in children {
                            let _ = wait_until_reaped(child);
                        }
                        return;
                    }
                }
            }

            reap(&mut children);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{
            INITIAL_POLL_INTERVAL, MAX_POLL_INTERVAL, ReaperMessage, ReaperState,
            is_already_reaped, lock_reaper, next_poll_interval, reap_finished, reap_loop_with,
            wait_until_reaped,
        };
        use std::io;
        use std::process::{Child, Command, Stdio};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex, mpsc};
        use std::thread;
        use std::time::{Duration, Instant};

        const HELPER_ENV: &str = "PROCESSKIT_DETACHED_ECHILD_HELPER";
        const PROACTIVE_HELPER: &str = "detached::reaper::tests::proactive_echild_helper_process";
        const FALLBACK_HELPER: &str = "detached::reaper::tests::fallback_echild_helper_process";

        #[derive(Debug, PartialEq, Eq)]
        enum TestMessage {
            Probe,
            Child(u64),
        }

        struct SigchldDisposition {
            original: libc::sigaction,
            armed: bool,
        }

        impl SigchldDisposition {
            fn ignore() -> Self {
                // SAFETY: both structs are initialized before the kernel reads
                // them, and this helper runs alone in a re-executed test process.
                let mut ignored = unsafe { std::mem::zeroed::<libc::sigaction>() };
                ignored.sa_sigaction = libc::SIG_IGN;
                let mask_result = unsafe { libc::sigemptyset(&mut ignored.sa_mask) };
                assert_eq!(mask_result, 0, "initialize the SIGCHLD mask");

                // SAFETY: `original` is an out-pointer for a full sigaction value.
                let mut original = unsafe { std::mem::zeroed::<libc::sigaction>() };
                let result = unsafe { libc::sigaction(libc::SIGCHLD, &ignored, &mut original) };
                assert_eq!(
                    result,
                    0,
                    "install SIGCHLD=SIG_IGN: {}",
                    io::Error::last_os_error()
                );
                Self {
                    original,
                    armed: true,
                }
            }

            fn restore(mut self) {
                let result = self.restore_inner();
                assert_eq!(
                    result,
                    0,
                    "restore the original SIGCHLD disposition: {}",
                    io::Error::last_os_error()
                );
            }

            fn restore_inner(&mut self) -> libc::c_int {
                // SAFETY: `original` was filled by the successful installation.
                let result =
                    unsafe { libc::sigaction(libc::SIGCHLD, &self.original, std::ptr::null_mut()) };
                if result == 0 {
                    self.armed = false;
                }
                result
            }
        }

        impl Drop for SigchldDisposition {
            fn drop(&mut self) {
                if self.armed {
                    // Best effort during unwind; the re-exec boundary still
                    // prevents a failed test from contaminating another test.
                    let _ = self.restore_inner();
                }
            }
        }

        fn exited_child() -> Child {
            Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn a short-lived child")
        }

        fn wait_for_echild(child: &mut Child) {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match child.try_wait() {
                    Err(source) if is_already_reaped(&source) => return,
                    Err(source) => panic!("try_wait failed before ECHILD: {source}"),
                    Ok(Some(status)) => {
                        panic!("SIGCHLD=SIG_IGN unexpectedly retained status {status}")
                    }
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(None) => panic!("short-lived child did not exit before the deadline"),
                }
            }
        }

        fn run_isolated(helper: &str) {
            let mut child = Command::new(std::env::current_exe().expect("current test binary"))
                .args([helper, "--exact", "--ignored", "--nocapture"])
                .env(HELPER_ENV, helper)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("re-exec isolated SIGCHLD helper");
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match child.try_wait().expect("poll isolated SIGCHLD helper") {
                    Some(status) => {
                        assert!(status.success(), "isolated helper failed with {status}");
                        return;
                    }
                    None if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    None => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("isolated SIGCHLD helper exceeded its 5s deadline");
                    }
                }
            }
        }

        #[test]
        fn only_echild_is_terminal() {
            assert!(is_already_reaped(&io::Error::from_raw_os_error(
                libc::ECHILD
            )));
            assert!(!is_already_reaped(&io::Error::from_raw_os_error(
                libc::EINTR
            )));
            assert!(!is_already_reaped(&io::Error::other("retry")));
        }

        #[test]
        fn transient_reaper_start_failure_is_retried() {
            let mut state = ReaperState::new();
            let mut starts = 0;
            let first = state.send_with(TestMessage::Probe, |_| {
                starts += 1;
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "injected thread-resource exhaustion",
                ))
            });
            let first = first.expect_err("the injected first start must fail");
            assert_eq!(first.source.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(first.message, TestMessage::Probe);
            assert!(
                state.sender.is_none(),
                "a failed start must not install a permanently failed sender"
            );

            let mut receiver = None;
            assert!(
                state
                    .send_with(TestMessage::Probe, |started| {
                        starts += 1;
                        receiver = Some(started);
                        Ok(())
                    })
                    .is_ok(),
                "the next call must retry and use the replacement manager"
            );
            assert_eq!(starts, 2, "one failed and one successful start are enough");
            let receiver = receiver.expect("capture the successful receiver");
            assert_eq!(
                receiver.recv().expect("receive the retry probe"),
                TestMessage::Probe
            );

            assert!(
                state
                    .send_with(TestMessage::Probe, |_| {
                        panic!("an open manager must be reused")
                    })
                    .is_ok()
            );
            assert_eq!(
                receiver.recv().expect("receive the reuse probe"),
                TestMessage::Probe
            );
        }

        #[test]
        fn closed_receiver_is_replaced_without_losing_ownership() {
            let (closed_sender, closed_receiver) = std::sync::mpsc::channel();
            drop(closed_receiver);
            let mut state = ReaperState {
                sender: Some(closed_sender),
            };
            let mut replacement = None;
            let mut starts = 0;

            assert!(
                state
                    .send_with(TestMessage::Child(41), |receiver| {
                        starts += 1;
                        replacement = Some(receiver);
                        Ok(())
                    })
                    .is_ok(),
                "the returned sole owner must be sent to a replacement manager"
            );
            assert_eq!(starts, 1, "only the disconnected receiver is replaced");
            assert_eq!(
                replacement
                    .expect("replacement receiver")
                    .recv()
                    .expect("replacement receives the sole owner"),
                TestMessage::Child(41)
            );
        }

        #[test]
        fn poll_interval_grows_and_stays_bounded() {
            let mut interval = INITIAL_POLL_INTERVAL;
            let expected = [
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
                Duration::from_millis(80),
                Duration::from_millis(160),
                Duration::from_millis(320),
                Duration::from_millis(640),
                Duration::from_secs(1),
            ];

            for expected in expected {
                assert_eq!(interval, expected);
                interval = next_poll_interval(interval);
            }
            assert_eq!(interval, MAX_POLL_INTERVAL);
            assert_eq!(next_poll_interval(MAX_POLL_INTERVAL), MAX_POLL_INTERVAL);
        }

        #[test]
        fn live_child_polling_uses_backoff() {
            let (sender, receiver) = mpsc::channel();
            let polls = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let killed = Arc::new(AtomicBool::new(false));
            let worker_polls = Arc::clone(&polls);
            let worker_stop = Arc::clone(&stop);
            let worker_killed = Arc::clone(&killed);
            let worker = thread::spawn(move || {
                reap_loop_with(receiver, move |children| {
                    worker_polls.fetch_add(1, Ordering::Relaxed);
                    if worker_stop.load(Ordering::Relaxed)
                        && let Some(child) = children.first_mut()
                    {
                        let _ = child.kill();
                        worker_killed.store(true, Ordering::Relaxed);
                    }
                    reap_finished(children);
                });
            });

            let child = Command::new("sleep")
                .arg("5")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn a live child");
            sender
                .send(ReaperMessage::Child(child))
                .expect("send the live child to the test reaper");

            // The fixed 10ms loop would make roughly one hundred reap attempts;
            // the bounded schedule reaches only its seventh attempt here.
            thread::sleep(Duration::from_secs(1));
            let observed = polls.load(Ordering::Relaxed);
            assert!(
                !killed.load(Ordering::Relaxed),
                "the child must still be live during sampling"
            );

            stop.store(true, Ordering::Relaxed);
            sender
                .send(ReaperMessage::Probe)
                .expect("wake the test reaper");
            drop(sender);
            worker.join().expect("test reaper thread must exit");
            assert!(
                killed.load(Ordering::Relaxed),
                "the test must kill the child after sampling"
            );

            assert!(
                observed >= 5,
                "a live child should be sampled repeatedly, only observed {observed} polls"
            );
            assert!(
                observed < 20,
                "polling should back off for a live child, observed {observed} polls"
            );
        }

        #[test]
        fn poisoned_reaper_state_still_accepts_a_handoff() {
            let state = Mutex::new(ReaperState::<TestMessage>::new());
            let poison = std::panic::catch_unwind(|| {
                let _held = state.lock().expect("lock fresh reaper state");
                panic!("poison the reaper state");
            });
            assert!(poison.is_err(), "the setup panic must be caught");
            assert!(state.is_poisoned(), "the mutex must exercise recovery");

            let mut receiver = None;
            let mut state = lock_reaper(&state);
            assert!(
                state
                    .send_with(TestMessage::Child(73), |started| {
                        receiver = Some(started);
                        Ok(())
                    })
                    .is_ok(),
                "poison must not discard the sole child owner"
            );
            drop(state);
            assert_eq!(
                receiver
                    .expect("receiver after poison recovery")
                    .recv()
                    .expect("receive the handoff after poison recovery"),
                TestMessage::Child(73)
            );
        }

        #[test]
        #[ignore = "re-execs a Unix subprocess with a process-wide SIGCHLD disposition"]
        fn proactive_echild_removes_the_manager_entry() {
            run_isolated(PROACTIVE_HELPER);
        }

        #[test]
        #[ignore = "re-execs a Unix subprocess with a process-wide SIGCHLD disposition"]
        fn fallback_echild_finishes_within_a_bound() {
            run_isolated(FALLBACK_HELPER);
        }

        #[test]
        #[ignore = "isolated helper; a no-op unless re-executed by the driver test"]
        fn proactive_echild_helper_process() {
            if std::env::var(HELPER_ENV).as_deref() != Ok(PROACTIVE_HELPER) {
                return;
            }
            let disposition = SigchldDisposition::ignore();
            let mut child = exited_child();
            wait_for_echild(&mut child);

            let mut children = vec![child];
            reap_finished(&mut children);
            assert!(
                children.is_empty(),
                "a permanent ECHILD must release the manager entry"
            );
            disposition.restore();
        }

        #[test]
        #[ignore = "isolated helper; a no-op unless re-executed by the driver test"]
        fn fallback_echild_helper_process() {
            if std::env::var(HELPER_ENV).as_deref() != Ok(FALLBACK_HELPER) {
                return;
            }
            let disposition = SigchldDisposition::ignore();
            let mut child = exited_child();
            wait_for_echild(&mut child);

            assert!(
                wait_until_reaped(child).is_none(),
                "ECHILD is completion, not a fallback wait failure"
            );
            disposition.restore();
        }
    }
}

#[cfg(unix)]
pub(crate) fn prepare_reaper() -> std::io::Result<()> {
    reaper::prepare()
}

#[cfg(unix)]
pub(crate) fn handoff_reaper(child: std::process::Child) -> std::io::Result<()> {
    reaper::handoff(child)
}

/// A minimal handle to a child spawned **outside** this crate's kill-on-drop
/// containment via [`Command::spawn_detached`](crate::Command::spawn_detached).
///
/// # Warning — this inverts the crate's headline guarantee
///
/// Everywhere else, `processkit` guarantees a child — and everything it spawns —
/// dies with its owner (kill-on-drop). A `DetachedChild` is the crate's **one**
/// deliberate exception: the child was launched into its own session (Unix
/// `setsid`) and is **not** assigned to this crate's containment (no Windows Job
/// Object / cgroup / process-group tracking), so **dropping this handle does
/// nothing to the child** — it keeps running, and its lifetime is now entirely
/// yours to manage.
///
/// That is why this is a **separate, non-interchangeable type**, not a
/// [`RunningProcess`](crate::RunningProcess): it deliberately exposes **no**
/// public `kill`, `wait`, timeout, output capture, or teardown/control verbs. All
/// it carries is the child's [`pid`](Self::pid). On Unix, a private background
/// reaper owns the OS child handle and collects its exit status; this public
/// handle still exposes none of those operations. If you need any of those, you
/// need containment — use [`Command::start`](crate::Command::start) instead and
/// keep the handle.
///
/// **Not contained by *this crate* — but a *host* container may still bind it.**
/// [`spawn_detached`](crate::Command::spawn_detached) deliberately does **not**
/// break out of an external Job Object / cgroup that already contains *your*
/// process (a CI runner, a `systemd` scope, this crate's own supervisor); doing
/// so would be hostile to whoever set that containment up. So a detached child
/// escapes only *this crate's* per-run containment, not a broader host one it
/// inherits.
#[derive(Debug)]
pub struct DetachedChild {
    pid: u32,
}

impl DetachedChild {
    /// Wrap the pid of a just-spawned detached child. Constructed only by
    /// [`Command::spawn_detached`](crate::Command::spawn_detached).
    pub(crate) fn new(pid: u32) -> Self {
        Self { pid }
    }

    /// The OS process id of the detached child, as reported at spawn.
    ///
    /// The pid can be recycled by the OS once the child exits and the Unix
    /// background reaper (or the platform's normal process cleanup) has
    /// collected it. Treat it as a spawn-time identifier, not a durable handle
    /// to a still-live process.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }
}
