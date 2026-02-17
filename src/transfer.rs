use crate::error::{Result, WatchdogError};
use crate::traits::{FileSystem, FileTransfer, TransferResult};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Real rsync-based file transfer.
pub struct RsyncTransfer;

impl FileTransfer for RsyncTransfer {
    fn transfer(&self, source: &Path, dest: &Path, timeout_secs: u64) -> Result<TransferResult> {
        let cmd_str = format!(
            "rsync -avh {} {}",
            source.display(),
            dest.display()
        );
        info!("Running: {} (timeout: {}s)", cmd_str, timeout_secs);

        let mut child = Command::new("rsync")
            .args(["-avh", "--progress", "--checksum"])
            .arg(source)
            .arg(dest)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| WatchdogError::Transfer {
                path: source.to_path_buf(),
                reason: format!("Failed to execute rsync: {}", e),
            })?;

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        warn!("rsync failed (rc={:?})", status.code());
                        return Ok(TransferResult { success: false });
                    }
                    return Ok(TransferResult { success: true });
                }
                Ok(None) => {
                    if timeout_secs > 0 && start.elapsed() > timeout {
                        warn!(
                            "rsync timed out after {}s for {}",
                            timeout_secs,
                            source.display()
                        );
                        let _ = child.kill();
                        let _ = child.wait();
                        return Ok(TransferResult { success: false });
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
                Err(e) => {
                    return Err(WatchdogError::Transfer {
                        path: source.to_path_buf(),
                        reason: format!("Failed to wait for rsync: {}", e),
                    });
                }
            }
        }
    }
}

/// Perform a safe replace operation: rsync new file to temp on remote,
/// rename original to .old, rename temp to original, delete .old.
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
    let temp_remote = source_dir.join(format!("{}.tmp", source_filename));
    let old_remote = source_dir.join(format!("{}.old", source_filename));

    // Pre-flight: verify the original file still exists on the NFS share.
    if !fs.exists(source_path) {
        error!(
            "Original file no longer exists at {}; aborting replace",
            source_path.display()
        );
        return Ok(false);
    }

    // Pre-flight: clean up stale artifacts from previous crashes.
    // If .tmp exists, it's a partial transfer from a previous run — safe to remove.
    if fs.exists(&temp_remote) {
        warn!(
            "Removing stale .tmp from previous run: {}",
            temp_remote.display()
        );
        let _ = fs.remove(&temp_remote);
    }
    // If .old exists AND source_path exists, the .old is from a previous successful
    // replace where cleanup (step 4) failed — safe to remove.
    if fs.exists(&old_remote) {
        warn!(
            "Removing stale .old from previous run: {}",
            old_remote.display()
        );
        let _ = fs.remove(&old_remote);
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
        error!("Failed to rename original to .old: {}", e);
        let _ = fs.remove(&temp_remote);
        return Ok(false);
    }

    // Step 3: move new file into place
    if let Err(e) = fs.rename(&temp_remote, source_path) {
        error!("Rename temp->source failed ({}); rolling back from .old", e);
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
            "Non-critical: failed to remove .old backup ({}): {} — will be cleaned up next run",
            old_remote.display(),
            e
        );
    }

    Ok(true)
}
