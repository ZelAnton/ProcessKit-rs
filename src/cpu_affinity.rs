//! Validation and native representations for `Command::cpu_affinity`.

#[cfg(any(target_os = "linux", windows))]
use std::io;

/// Canonicalize a caller-provided CPU set so debug output, tests, and each
/// platform lowering see one stable representation.
pub(crate) fn normalize(cpus: impl IntoIterator<Item = usize>) -> Vec<usize> {
    let mut cpus: Vec<_> = cpus.into_iter().collect();
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

#[cfg(any(target_os = "linux", windows))]
fn require_non_empty(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CPU affinity must contain at least one CPU index",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn linux_set(cpus: &[usize]) -> io::Result<libc::cpu_set_t> {
    require_non_empty(cpus)?;
    // Build the fixed-size set before fork. The pre-exec hook then performs only
    // one syscall over this copied value and cannot allocate in the child.
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe { libc::CPU_ZERO(&mut set) };
    for &cpu in cpus {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Linux CPU index {cpu} exceeds cpu_set_t capacity {}",
                    libc::CPU_SETSIZE
                ),
            ));
        }
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    Ok(set)
}

#[cfg(windows)]
pub(crate) fn windows_mask(cpus: &[usize]) -> io::Result<usize> {
    require_non_empty(cpus)?;
    let mut mask = 0usize;
    for &cpu in cpus {
        if cpu >= usize::BITS as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Windows CPU index {cpu} cannot be represented by a {}-bit process affinity mask",
                    usize::BITS
                ),
            ));
        }
        mask |= 1usize << cpu;
    }
    Ok(mask)
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn normalization_sorts_and_deduplicates() {
        assert_eq!(normalize([3, 1, 3, 2, 1]), [1, 2, 3]);
    }

    #[cfg(windows)]
    #[test]
    fn windows_mask_encodes_and_validates_indices() {
        assert_eq!(super::windows_mask(&[0, 2, 5]).unwrap(), 0b100101);
        assert_eq!(
            super::windows_mask(&[]).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            super::windows_mask(&[usize::BITS as usize])
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_set_encodes_and_validates_indices() {
        let set = super::linux_set(&[0, 2]).unwrap();
        assert!(unsafe { libc::CPU_ISSET(0, &set) });
        assert!(unsafe { libc::CPU_ISSET(2, &set) });
        assert!(!unsafe { libc::CPU_ISSET(1, &set) });
        assert!(super::linux_set(&[]).is_err());
        assert!(super::linux_set(&[libc::CPU_SETSIZE as usize]).is_err());
    }
}
