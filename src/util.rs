use std::fmt::Write;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

/// An exclusive file lock that prevents concurrent instances.
/// Uses `flock(LOCK_EX | LOCK_NB)` which is automatically released when the
/// process exits (even on SIGKILL or crash). No stale lock files to clean up.
pub struct InstanceLock {
    lock: Option<nix::fcntl::Flock<File>>,
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
                lock: Some(lock),
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
        // Release the flock before removing the path so a new process cannot
        // race in via a freshly-created lock file while we still hold the old lock.
        let _ = self.lock.take();
        let _ = std::fs::remove_file(&self.path);
    }
}

pub fn pid_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        kill(Pid::from_raw(pid as i32), None).is_ok()
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

pub fn wait_for_pid_exit(pid: u32, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !pid_is_running(pid) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    !pid_is_running(pid)
}

pub fn copy_to_clipboard(text: &str) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        copy_with_command("pbcopy", &[], text)
    }

    #[cfg(target_os = "windows")]
    {
        copy_with_command("clip", &[], text)
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let candidates: [(&str, &[&str]); 3] = [
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ];
        for (program, args) in candidates {
            if which::which(program).is_ok() {
                return copy_with_command(program, args, text);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No supported clipboard command found (tried wl-copy, xclip, xsel)",
        ))
    }
}

fn copy_with_command(program: &str, args: &[&str], text: &str) -> io::Result<()> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        stdin.write_all(text.as_bytes())?;
    }

    let status = child.wait()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "Clipboard command '{}' exited with {}",
            program, status
        )))
    }
}

#[cfg(unix)]
fn configure_detached_command(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;

    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // SAFETY: pre_exec runs in the child immediately before exec.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_detached_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
}

pub fn spawn_detached_command(mut cmd: Command) -> io::Result<Child> {
    configure_detached_command(&mut cmd);
    cmd.spawn()
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
    use std::io::ErrorKind;

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

    #[test]
    fn instance_lock_blocks_reentry_until_drop() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("watchdog.lock");

        let first = InstanceLock::acquire(&lock_path).unwrap();
        let err = match InstanceLock::acquire(&lock_path) {
            Ok(_) => panic!("second lock acquisition unexpectedly succeeded"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);

        drop(first);

        let second = InstanceLock::acquire(&lock_path).unwrap();
        assert_eq!(second.path(), lock_path.as_path());
    }
}
