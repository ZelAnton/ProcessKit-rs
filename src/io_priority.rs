//! [`IoPriority`] — a Linux I/O-scheduling priority.

/// An I/O-scheduling priority for a child process (see
/// [`Command::io_priority`](crate::Command::io_priority)).
///
/// ProcessKit applies this through Linux `ioprio_set(2)` in the child's
/// `pre_exec` hook, before the program begins its own disk work. It is Linux-only:
/// requesting it on Windows, macOS, BSD, or another Unix target fails with
/// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported), never a silent
/// no-op.
///
/// Lower data values have higher priority. `BestEffort(7)` is therefore a useful
/// choice for polite batch work; [`Idle`](Self::Idle) runs only when the block
/// device is otherwise idle. `RealTime` can starve other I/O and normally needs
/// `CAP_SYS_ADMIN`; when the kernel rejects a request, spawning fails with
/// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IoPriority {
    /// I/O runs only when the block device is otherwise idle.
    Idle,
    /// The ordinary Linux best-effort class, with a data value from `0` (highest)
    /// through `7` (lowest).
    BestEffort(u8),
    /// The Linux real-time I/O class, with a data value from `0` (highest)
    /// through `7` (lowest).
    ///
    /// This class can starve other disk users and normally requires
    /// `CAP_SYS_ADMIN`; prefer [`BestEffort`](Self::BestEffort) or
    /// [`Idle`](Self::Idle) for background work.
    RealTime(u8),
}

#[cfg(target_os = "linux")]
impl IoPriority {
    /// Encode this priority for `ioprio_set(2)`, validating the kernel's three-bit
    /// priority data range before the forked child runs any hook.
    pub(crate) fn linux_value(self) -> std::io::Result<libc::c_int> {
        const CLASS_SHIFT: libc::c_int = 13;
        const CLASS_REALTIME: libc::c_int = 1;
        const CLASS_BEST_EFFORT: libc::c_int = 2;
        const CLASS_IDLE: libc::c_int = 3;

        let (class, data) = match self {
            IoPriority::Idle => (CLASS_IDLE, 0),
            IoPriority::BestEffort(data) => (CLASS_BEST_EFFORT, data),
            IoPriority::RealTime(data) => (CLASS_REALTIME, data),
        };
        if data > 7 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Linux I/O priority data must be in 0..=7, got {data}"),
            ));
        }
        Ok((class << CLASS_SHIFT) | libc::c_int::from(data))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::IoPriority;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_value_encodes_classes_and_validates_data() {
        assert_eq!(IoPriority::Idle.linux_value().unwrap(), 3 << 13);
        assert_eq!(
            IoPriority::BestEffort(7).linux_value().unwrap(),
            (2 << 13) | 7
        );
        assert_eq!(IoPriority::RealTime(0).linux_value().unwrap(), 1 << 13);
        assert_eq!(
            IoPriority::BestEffort(8)
                .linux_value()
                .expect_err("values beyond Linux's three-bit range are invalid")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
