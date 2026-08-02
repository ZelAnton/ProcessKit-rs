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
    /// This resource's **stable machine identifier**: a short, lowercase
    /// `snake_case` string, part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's config schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    pub fn name(self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            Self::Cpu => "cpu",
            Self::Core => "core",
            Self::Data => "data",
            Self::FileSize => "file_size",
            Self::NoFile => "no_file",
            Self::Stack => "stack",
        }
    }

    /// Parse a stable [`name`](Self::name) identifier back into a resource.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `RlimitResource::from_name(v.name()) == Some(v)`
    /// for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        // Add an explicit branch here for every new variant; `_` rejects only
        // unknown input and must not stand in for a resource's stable name.
        match name {
            "cpu" => Some(Self::Cpu),
            "core" => Some(Self::Core),
            "data" => Some(Self::Data),
            "file_size" => Some(Self::FileSize),
            "no_file" => Some(Self::NoFile),
            "stack" => Some(Self::Stack),
            _ => None,
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
        for resource in [
            RlimitResource::Cpu,
            RlimitResource::Core,
            RlimitResource::Data,
            RlimitResource::FileSize,
            RlimitResource::NoFile,
            RlimitResource::Stack,
        ] {
            assert_eq!(RlimitResource::from_name(resource.name()), Some(resource));
        }
        assert_eq!(RlimitResource::from_name("nofile"), None);
    }

    #[test]
    fn name_from_name_round_trips_every_resource() {
        let resources = [
            RlimitResource::Cpu,
            RlimitResource::Core,
            RlimitResource::Data,
            RlimitResource::FileSize,
            RlimitResource::NoFile,
            RlimitResource::Stack,
        ];

        for resource in resources {
            assert_eq!(RlimitResource::from_name(resource.name()), Some(resource));
        }
    }

    #[test]
    fn from_name_rejects_unknown_and_bad_case() {
        assert_eq!(RlimitResource::from_name("Cpu"), None);
        assert_eq!(RlimitResource::from_name("unknown"), None);
        assert_eq!(RlimitResource::from_name(""), None);
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
