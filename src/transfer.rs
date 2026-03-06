use crate::error::{Result, WatchdogError};
use crate::process::{run_command, RunOptions};
use crate::traits::{FileSystem, FileTransfer, TransferResult};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tracing::{error, info, warn};

pub const WATCHDOG_TMP_SUFFIX: &str = ".watchdog.tmp";
pub const WATCHDOG_OLD_SUFFIX: &str = ".watchdog.old";

/// Real rsync-based file transfer.
pub struct RsyncTransfer;

impl FileTransfer for RsyncTransfer {
    fn transfer(&self, source: &Path, dest: &Path, timeout_secs: u64) -> Result<TransferResult> {
        let cmd_str = format!("rsync -avh {} {}", source.display(), dest.display());
        info!("Running: {} (timeout: {}s)", cmd_str, timeout_secs);
        let mut cmd = Command::new("rsync");
        cmd.args(["-avh", "--progress", "--checksum"])
            .arg(source)
            .arg(dest);
        let output = run_command(
            cmd,
            RunOptions {
                timeout: if timeout_secs > 0 {
                    Some(Duration::from_secs(timeout_secs))
                } else {
                    None
                },
                stdout_limit: 64 * 1024,
                stderr_limit: 256 * 1024,
                ..RunOptions::default()
            },
        )
        .map_err(|e| WatchdogError::Transfer {
            path: source.to_path_buf(),
            reason: format!("Failed to execute rsync: {}", e),
        })?;

        if output.timed_out {
            warn!(
                "rsync timed out after {}s for {}",
                timeout_secs,
                source.display()
            );
            return Ok(TransferResult { success: false });
        }
        if !output.status.success() {
            warn!(
                "rsync failed (rc={:?}): {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr_tail).trim()
            );
            return Ok(TransferResult { success: false });
        }
        Ok(TransferResult { success: true })
    }
}

/// Perform a safe replace operation: rsync new file to temp on remote,
/// rename original to .watchdog.old, rename temp to original, delete backup.
/// Rolls back on failure. Cleans up stale artifacts from previous crashes.
pub fn safe_replace(
    fs: &dyn FileSystem,
    transfer: &dyn FileTransfer,
    source_path: &Path,
    new_local_path: &Path,
) -> Result<bool> {
    let source_dir = source_path.parent().unwrap_or(Path::new("."));
    let source_filename = source_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let temp_remote = source_dir.join(format!("{}{}", source_filename, WATCHDOG_TMP_SUFFIX));
    let old_remote = source_dir.join(format!("{}{}", source_filename, WATCHDOG_OLD_SUFFIX));
    let legacy_temp_remote = source_dir.join(format!("{}.tmp", source_filename));
    let legacy_old_remote = source_dir.join(format!("{}.old", source_filename));

    // Pre-flight: verify the original file still exists on the NFS share.
    if !fs.exists(source_path) {
        error!(
            "Original file no longer exists at {}; aborting replace",
            source_path.display()
        );
        return Ok(false);
    }

    // Pre-flight: clean up stale artifacts from previous crashes.
    // If .watchdog.tmp exists, it's a partial transfer from a previous run — safe to remove.
    if fs.exists(&temp_remote) {
        warn!(
            "Removing stale watchdog temp from previous run: {}",
            temp_remote.display()
        );
        let _ = fs.remove(&temp_remote);
    }
    // If .watchdog.old exists AND source_path exists, it is from a previous successful
    // replace where cleanup (step 4) failed — safe to remove.
    if fs.exists(&old_remote) {
        warn!(
            "Removing stale watchdog backup from previous run: {}",
            old_remote.display()
        );
        let _ = fs.remove(&old_remote);
    }
    // Legacy suffixes from older versions are detected but never mutated automatically.
    if fs.exists(&legacy_temp_remote) {
        warn!(
            "Legacy temp artifact detected (left untouched): {}",
            legacy_temp_remote.display()
        );
    }
    if fs.exists(&legacy_old_remote) {
        warn!(
            "Legacy backup artifact detected (left untouched): {}",
            legacy_old_remote.display()
        );
    }

    // Step 1: rsync new file to temp path on remote (with --checksum for integrity)
    let file_size = fs.file_size(new_local_path).unwrap_or(0);
    let timeout = if file_size > 0 {
        (file_size / (1024 * 1024)).max(300)
    } else {
        3600
    };

    let result = transfer.transfer(new_local_path, &temp_remote, timeout)?;
    if !result.success {
        error!("rsync to temp failed for {}", source_path.display());
        let _ = fs.remove(&temp_remote);
        return Ok(false);
    }

    // Verify temp file landed and size matches the local output
    if !fs.exists(&temp_remote) {
        error!("rsync reported success but temp file missing on remote");
        return Ok(false);
    }
    let remote_size = fs.file_size(&temp_remote).unwrap_or(0);
    let local_size = fs.file_size(new_local_path).unwrap_or(0);
    if remote_size != local_size {
        error!(
            "Size mismatch after rsync: local={} remote={} — aborting replace to prevent corruption",
            local_size, remote_size
        );
        let _ = fs.remove(&temp_remote);
        return Ok(false);
    }

    // Step 2: move original aside
    if let Err(e) = fs.rename(source_path, &old_remote) {
        error!("Failed to rename original to .watchdog.old: {}", e);
        let _ = fs.remove(&temp_remote);
        return Ok(false);
    }

    // Step 3: move new file into place
    if let Err(e) = fs.rename(&temp_remote, source_path) {
        error!(
            "Rename .watchdog.tmp->source failed ({}); rolling back from .watchdog.old",
            e
        );
        match fs.rename(&old_remote, source_path) {
            Ok(_) => info!("Rollback succeeded — original file restored"),
            Err(rb_err) => {
                error!(
                    "CRITICAL ROLLBACK FAILED — your original file is safe at: {}",
                    old_remote.display()
                );
                error!(
                    "To recover manually: mv \"{}\" \"{}\"",
                    old_remote.display(),
                    source_path.display()
                );
                error!("Rollback error: {}", rb_err);
            }
        }
        return Ok(false);
    }

    // Step 4: remove old file (non-fatal — the old original is no longer needed)
    if let Err(e) = fs.remove(&old_remote) {
        warn!(
            "Non-critical: failed to remove .watchdog.old backup ({}): {} — will be cleaned up next run",
            old_remote.display(),
            e
        );
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::FileEntry;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    struct MockFs {
        files: Mutex<HashMap<std::path::PathBuf, u64>>,
        fail_rename_to: Mutex<HashSet<std::path::PathBuf>>,
    }

    impl MockFs {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                fail_rename_to: Mutex::new(HashSet::new()),
            }
        }

        fn insert(&self, path: &Path, size: u64) {
            self.files.lock().unwrap().insert(path.to_path_buf(), size);
        }

        fn fail_rename_to(&self, path: &Path) {
            self.fail_rename_to
                .lock()
                .unwrap()
                .insert(path.to_path_buf());
        }
    }

    impl FileSystem for MockFs {
        fn walk_share(
            &self,
            _share_name: &str,
            _root: &Path,
            _extensions: &[String],
        ) -> Result<Vec<FileEntry>> {
            Ok(Vec::new())
        }

        fn file_size(&self, path: &Path) -> Result<u64> {
            self.files
                .lock()
                .unwrap()
                .get(path)
                .copied()
                .ok_or_else(|| {
                    WatchdogError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "not found",
                    ))
                })
        }

        fn file_mtime(&self, _path: &Path) -> Result<f64> {
            Ok(0.0)
        }

        fn exists(&self, path: &Path) -> bool {
            self.files.lock().unwrap().contains_key(path)
        }

        fn is_dir(&self, _path: &Path) -> bool {
            true
        }

        fn rename(&self, from: &Path, to: &Path) -> Result<()> {
            if self.fail_rename_to.lock().unwrap().contains(to) {
                return Err(WatchdogError::Io(std::io::Error::other(
                    "forced rename failure",
                )));
            }
            let size = self.files.lock().unwrap().remove(from).ok_or_else(|| {
                WatchdogError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "rename source missing",
                ))
            })?;
            self.files.lock().unwrap().insert(to.to_path_buf(), size);
            Ok(())
        }

        fn remove(&self, path: &Path) -> Result<()> {
            self.files.lock().unwrap().remove(path);
            Ok(())
        }

        fn free_space(&self, _path: &Path) -> Result<u64> {
            Ok(u64::MAX / 2)
        }

        fn create_dir_all(&self, _path: &Path) -> Result<()> {
            Ok(())
        }

        fn list_dir(&self, _path: &Path) -> Result<Vec<std::path::PathBuf>> {
            Ok(Vec::new())
        }

        fn walk_files_with_suffix(
            &self,
            _root: &Path,
            _suffix: &str,
        ) -> Result<Vec<std::path::PathBuf>> {
            Ok(Vec::new())
        }
    }

    struct MockTransfer {
        fs: Arc<MockFs>,
        succeed: bool,
        create_dest: bool,
        force_dest_size: Option<u64>,
    }

    impl FileTransfer for MockTransfer {
        fn transfer(
            &self,
            source: &Path,
            dest: &Path,
            _timeout_secs: u64,
        ) -> Result<TransferResult> {
            if !self.succeed {
                return Ok(TransferResult { success: false });
            }
            if self.create_dest {
                let source_size = self.fs.file_size(source).unwrap_or(0);
                let size = self.force_dest_size.unwrap_or(source_size);
                self.fs.insert(dest, size);
            }
            Ok(TransferResult { success: true })
        }
    }

    #[test]
    fn test_safe_replace_success() {
        let fs = Arc::new(MockFs::new());
        let source = Path::new("/media/movie.mkv");
        let local_new = Path::new("/tmp/new.mkv");
        fs.insert(source, 100);
        fs.insert(local_new, 50);
        let transfer = MockTransfer {
            fs: fs.clone(),
            succeed: true,
            create_dest: true,
            force_dest_size: None,
        };

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new).unwrap();
        assert!(ok);
        assert_eq!(fs.file_size(source).unwrap(), 50);
        assert!(!fs.exists(Path::new("/media/movie.mkv.watchdog.old")));
        assert!(!fs.exists(Path::new("/media/movie.mkv.watchdog.tmp")));
    }

    #[test]
    fn test_safe_replace_fails_when_temp_missing() {
        let fs = Arc::new(MockFs::new());
        let source = Path::new("/media/movie.mkv");
        let local_new = Path::new("/tmp/new.mkv");
        fs.insert(source, 100);
        fs.insert(local_new, 50);
        let transfer = MockTransfer {
            fs: fs.clone(),
            succeed: true,
            create_dest: false,
            force_dest_size: None,
        };

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new).unwrap();
        assert!(!ok);
        assert_eq!(fs.file_size(source).unwrap(), 100);
    }

    #[test]
    fn test_safe_replace_fails_on_size_mismatch() {
        let fs = Arc::new(MockFs::new());
        let source = Path::new("/media/movie.mkv");
        let local_new = Path::new("/tmp/new.mkv");
        let tmp_remote = Path::new("/media/movie.mkv.watchdog.tmp");
        fs.insert(source, 100);
        fs.insert(local_new, 50);
        let transfer = MockTransfer {
            fs: fs.clone(),
            succeed: true,
            create_dest: true,
            force_dest_size: Some(999),
        };

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new).unwrap();
        assert!(!ok);
        assert!(!fs.exists(tmp_remote));
    }

    #[test]
    fn test_safe_replace_rollback_failure_leaves_backup() {
        let fs = Arc::new(MockFs::new());
        let source = Path::new("/media/movie.mkv");
        let local_new = Path::new("/tmp/new.mkv");
        let old_remote = Path::new("/media/movie.mkv.watchdog.old");
        fs.insert(source, 100);
        fs.insert(local_new, 50);
        fs.fail_rename_to(source);
        let transfer = MockTransfer {
            fs: fs.clone(),
            succeed: true,
            create_dest: true,
            force_dest_size: None,
        };

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new).unwrap();
        assert!(!ok);
        assert!(!fs.exists(source));
        assert!(fs.exists(old_remote));
    }
}
