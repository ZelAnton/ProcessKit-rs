//! [`RlimitResource`] — typed Unix per-process `setrlimit(2)` resources.

#[cfg(unix)]
use std::io;

/// A Unix per-process resource controlled by
/// [`Command::rlimit`](crate::Command::rlimit).
///
/// These limits complement the `limits` feature's whole-tree
/// `ResourceLimits`: an rlimit applies to the direct child before `exec` and is
/// inherited by descendants, but each descendant may lower its own limits (and
/// may raise them again up to its hard limit). On non-Unix platforms,
/// requesting any variant fails with
/// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RlimitResource {
    /// Maximum CPU time in seconds (`RLIMIT_CPU`).
    Cpu,
    /// Maximum size in bytes of a core dump (`RLIMIT_CORE`). Use `0, 0` to
    /// disable core dumps for a child handling secrets.
    Core,
    /// Maximum process data-segment size in bytes (`RLIMIT_DATA`).
    Data,
    /// Maximum size in bytes of a file the process may create (`RLIMIT_FSIZE`).
    FileSize,
    /// Maximum number of simultaneously open file descriptors (`RLIMIT_NOFILE`).
    NoFile,
    /// Maximum process stack size in bytes (`RLIMIT_STACK`).
    Stack,
}

impl RlimitResource {
    /// This resource's stable lowercase identifier.
    pub fn name(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Core => "core",
            Self::Data => "data",
            Self::FileSize => "file_size",
            Self::NoFile => "no_file",
            Self::Stack => "stack",
        }
    }

    #[cfg(unix)]
    pub(crate) fn prepare(self, soft: u64, hard: u64) -> io::Result<libc::rlimit> {
        if soft > hard {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} rlimit soft value ({soft}) exceeds hard value ({hard})",
                    self.name()
                ),
            ));
        }
        let convert = |value: u64| {
            libc::rlim_t::try_from(value).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} rlimit value {value} exceeds this target's range",
                        self.name()
                    ),
                )
            })
        };
        Ok(libc::rlimit {
            rlim_cur: convert(soft)?,
            rlim_max: convert(hard)?,
        })
    }

    /// Apply a parent-prepared native value in the post-fork child.
    #[cfg(unix)]
    pub(crate) unsafe fn apply(self, limit: &libc::rlimit) -> libc::c_int {
        // SAFETY: the caller owns the post-fork pre_exec boundary and passes a
        // valid pointer to a fully initialized `rlimit`. Keeping each constant
        // in its call arm lets libc expose its target-correct resource type.
        unsafe {
            match self {
                Self::Cpu => libc::setrlimit(libc::RLIMIT_CPU, limit),
                Self::Core => libc::setrlimit(libc::RLIMIT_CORE, limit),
                Self::Data => libc::setrlimit(libc::RLIMIT_DATA, limit),
                Self::FileSize => libc::setrlimit(libc::RLIMIT_FSIZE, limit),
                Self::NoFile => libc::setrlimit(libc::RLIMIT_NOFILE, limit),
                Self::Stack => libc::setrlimit(libc::RLIMIT_STACK, limit),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RlimitResource;

    #[test]
    fn stable_names_cover_every_resource() {
        assert_eq!(RlimitResource::Cpu.name(), "cpu");
        assert_eq!(RlimitResource::Core.name(), "core");
        assert_eq!(RlimitResource::Data.name(), "data");
        assert_eq!(RlimitResource::FileSize.name(), "file_size");
        assert_eq!(RlimitResource::NoFile.name(), "no_file");
        assert_eq!(RlimitResource::Stack.name(), "stack");
    }

    #[cfg(unix)]
    #[test]
    fn prepare_rejects_soft_above_hard_before_fork() {
        let error = match RlimitResource::NoFile.prepare(65, 64) {
            Ok(_) => panic!("soft cannot exceed hard"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
