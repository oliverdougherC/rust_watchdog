use std::fmt::Write;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// An exclusive file lock that prevents concurrent instances.
/// Uses `flock(LOCK_EX | LOCK_NB)` which is automatically released when the
/// process exits (even on SIGKILL or crash). No stale lock files to clean up.
pub struct InstanceLock {
    _lock: nix::fcntl::Flock<File>,
    path: PathBuf,
}

impl InstanceLock {
    /// Try to acquire an exclusive lock on the given file path.
    /// Returns `Ok(lock)` if acquired, or an error if another instance holds it.
    pub fn acquire(lock_path: &Path) -> io::Result<Self> {
        use nix::fcntl::{Flock, FlockArg};

        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(lock_path)?;

        // Write our PID for diagnostics before locking
        {
            use io::Write;
            let mut f = &file;
            let _ = writeln!(f, "{}", std::process::id());
        }

        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => Ok(Self {
                _lock: lock,
                path: lock_path.to_path_buf(),
            }),
            Err((_, nix::Error::EWOULDBLOCK)) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "Another watchdog instance is already running (lock: {})",
                    lock_path.display()
                ),
            )),
            Err((_, e)) => Err(io::Error::other(format!(
                "Failed to acquire lock on {}: {}",
                lock_path.display(),
                e
            ))),
        }
    }

    /// Path of the lock file (for logging).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // flock is released automatically when the Flock is dropped,
        // but remove the lock file for cleanliness.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Format a byte count into human-friendly binary units (KiB, MiB, GiB, TiB).
pub fn format_bytes(num_bytes: u64) -> String {
    const UNITS: &[&str] = &["bytes", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut num = num_bytes as f64;
    for unit in UNITS {
        if num.abs() < 1024.0 || *unit == "PiB" {
            if *unit == "bytes" {
                return format!("{} {}", num_bytes, unit);
            }
            return format!("{:.2} {}", num, unit);
        }
        num /= 1024.0;
    }
    format!("{} bytes", num_bytes)
}

/// Format a byte count as signed (for space saved which can theoretically be negative).
pub fn format_bytes_signed(num_bytes: i64) -> String {
    let sign = if num_bytes < 0 { "-" } else { "" };
    let formatted = format_bytes(num_bytes.unsigned_abs());
    format!("{}{}", sign, formatted)
}

/// Format a duration in seconds into a human-friendly string like "2h 15m 30s".
pub fn format_duration(seconds: f64) -> String {
    let total_secs = seconds as u64;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    let mut result = String::new();
    if hours > 0 {
        let _ = write!(result, "{}h ", hours);
    }
    if minutes > 0 || hours > 0 {
        let _ = write!(result, "{}m ", minutes);
    }
    let _ = write!(result, "{}s", secs);
    result
}

/// Check that required CLI dependencies are available in PATH.
/// Returns a list of missing tool names.
pub fn check_dependencies(tools: &[&str]) -> Vec<String> {
    tools
        .iter()
        .filter(|tool| which::which(tool).is_err())
        .map(|s| s.to_string())
        .collect()
}

/// Verify all required dependencies. Returns Ok(()) or Err with the missing list.
pub fn verify_dependencies() -> std::result::Result<(), Vec<String>> {
    let required = &["ffprobe", "HandBrakeCLI", "rsync"];
    let missing = check_dependencies(required);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(2_500_000_000), "2.33 GiB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0.0), "0s");
        assert_eq!(format_duration(59.0), "59s");
        assert_eq!(format_duration(60.0), "1m 0s");
        assert_eq!(format_duration(3661.0), "1h 1m 1s");
        assert_eq!(format_duration(7200.0), "2h 0m 0s");
    }
}
