use crate::error::{Result, WatchdogError};
use crate::process::{run_command, RunOptions};
use crate::traits::MountManager;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tracing::{error, info, warn};

/// Real NFS mount manager using system commands.
pub struct SystemMountManager;

impl MountManager for SystemMountManager {
    fn is_healthy(&self, mount_point: &Path) -> bool {
        // Check if the path is a mount point
        if !mount_point.exists() {
            return false;
        }

        // Try to read the directory to verify it's accessible
        match std::fs::read_dir(mount_point) {
            Ok(mut entries) => {
                // Try to read at least one entry to verify the mount is responsive
                match entries.next() {
                    Some(Ok(_)) | None => true,
                    Some(Err(_)) => false,
                }
            }
            Err(_) => false,
        }
    }

    fn remount(
        &self,
        server: &str,
        remote_path: &str,
        local_mount: &Path,
        share_name: &str,
    ) -> Result<bool> {
        // Ensure mount point directory exists
        if !local_mount.exists() {
            std::fs::create_dir_all(local_mount)?;
        }

        let remote = format!("{}:{}", server, remote_path);
        let mut delay = 2u64;
        let max_attempts = 3;

        for attempt in 1..=max_attempts {
            warn!(
                "[{}] Remount attempt {}/{} using mount_nfs {} {}",
                share_name,
                attempt,
                max_attempts,
                remote,
                local_mount.display()
            );

            let mut cmd = Command::new("mount_nfs");
            cmd.arg(&remote).arg(local_mount);
            let output = run_command(
                cmd,
                RunOptions {
                    timeout: Some(Duration::from_secs(30)),
                    stdout_limit: 16 * 1024,
                    stderr_limit: 64 * 1024,
                    ..RunOptions::default()
                },
            );

            match output {
                Ok(result) if !result.timed_out && result.status.success() => {
                    if self.is_healthy(local_mount) {
                        info!(
                            "[{}] Remount succeeded at {}",
                            share_name,
                            local_mount.display()
                        );
                        return Ok(true);
                    }
                }
                Ok(result) => {
                    if result.timed_out {
                        warn!("[{}] mount_nfs timed out after 30s", share_name);
                        continue;
                    }
                    let stderr = String::from_utf8_lossy(&result.stderr_tail);
                    warn!(
                        "[{}] mount_nfs failed (rc={}): {}",
                        share_name,
                        result.status.code().unwrap_or(-1),
                        stderr.trim()
                    );
                }
                Err(e) => {
                    error!("[{}] Failed to execute mount_nfs: {}", share_name, e);
                }
            }

            if attempt < max_attempts {
                std::thread::sleep(std::time::Duration::from_secs(delay));
                delay = (delay * 2).min(30);
            }
        }

        error!(
            "[{}] Remount attempts exhausted for {}",
            share_name,
            local_mount.display()
        );
        Ok(false)
    }
}

/// Ensure all configured NFS shares are mounted and healthy.
/// Returns Ok(()) if all shares are healthy after potential remounting.
pub fn ensure_all_mounts(
    manager: &dyn MountManager,
    server: &str,
    shares: &[(String, String, String)], // (name, remote_path, local_mount)
) -> Result<()> {
    for (name, remote_path, local_mount) in shares {
        let mount_path = Path::new(local_mount);
        if manager.is_healthy(mount_path) {
            continue;
        }

        warn!(
            "Mount '{}' at {} is unavailable; attempting remount",
            name, local_mount
        );

        let success = manager.remount(server, remote_path, mount_path, name)?;
        if !success {
            return Err(WatchdogError::NfsMount {
                share: name.clone(),
                reason: format!(
                    "Failed to remount share '{}' ({}) after multiple attempts",
                    name, local_mount
                ),
            });
        }
    }
    Ok(())
}
