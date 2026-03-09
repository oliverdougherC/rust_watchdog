use crate::error::{Result, WatchdogError};
use crate::process::{
    describe_exit_status, format_command_for_log, infer_failure_hint, run_command,
    summarize_output_tail, RunOptions,
};
use crate::traits::MountManager;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tracing::{error, info, warn};

fn allow_local_mounts_for_testing() -> bool {
    std::env::var("WATCHDOG_ALLOW_LOCAL_MOUNTS")
        .ok()
        .is_some_and(|v| v == "1")
}

/// Real NFS mount manager using system commands.
pub struct SystemMountManager;

impl SystemMountManager {
    fn filesystem_type(&self, mount_point: &Path) -> Option<String> {
        let mut cmd = Command::new("stat");
        cmd.args(["-f", "%T"]).arg(mount_point);
        let command_repr = format_command_for_log(&cmd);
        let output = match run_command(
            cmd,
            RunOptions {
                timeout: Some(Duration::from_secs(3)),
                stdout_limit: 1024,
                stderr_limit: 1024,
                ..RunOptions::default()
            },
        ) {
            Ok(out) => out,
            Err(e) => {
                warn!(
                    "Filesystem-type probe execution failed for {}: {} (command={})",
                    mount_point.display(),
                    e,
                    command_repr
                );
                return None;
            }
        };

        if output.timed_out {
            warn!(
                "Filesystem-type probe timed out for {} after {:.1}s: command={}",
                mount_point.display(),
                output.elapsed.as_secs_f64(),
                command_repr
            );
            return None;
        }
        if !output.status.success() {
            warn!(
                "Filesystem-type probe failed for {}: status={} stderr={}",
                mount_point.display(),
                describe_exit_status(&output.status),
                summarize_output_tail(&output.stderr_tail, 200)
            );
            return None;
        }

        let fs_type = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .map(str::trim)
            .unwrap_or_default()
            .to_lowercase();
        if fs_type.is_empty() {
            None
        } else {
            Some(fs_type)
        }
    }
}

impl MountManager for SystemMountManager {
    fn is_healthy(&self, mount_point: &Path) -> bool {
        if !mount_point.exists() || !mount_point.is_dir() {
            return false;
        }

        let readable = match std::fs::read_dir(mount_point) {
            Ok(mut entries) => match entries.next() {
                Some(Ok(_)) | None => true,
                Some(Err(_)) => false,
            },
            Err(_) => false,
        };
        if !readable {
            return false;
        }

        if allow_local_mounts_for_testing() {
            return true;
        }

        matches!(
            self.filesystem_type(mount_point).as_deref(),
            Some("nfs") | Some("nfs4")
        )
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
            let command_repr = format_command_for_log(&cmd);
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
                    warn!(
                        "[{}] mount_nfs command succeeded but mount is still unhealthy at {} (elapsed={:.1}s)",
                        share_name,
                        local_mount.display(),
                        result.elapsed.as_secs_f64()
                    );
                }
                Ok(result) => {
                    if result.timed_out {
                        warn!(
                            "[{}] mount_nfs timed out after 30s (elapsed={:.1}s): remote={} local={} command={}",
                            share_name,
                            result.elapsed.as_secs_f64(),
                            remote,
                            local_mount.display(),
                            command_repr
                        );
                        continue;
                    }
                    let stderr_summary = summarize_output_tail(&result.stderr_tail, 320);
                    let hint_suffix = infer_failure_hint(&stderr_summary)
                        .map(|hint| format!(" likely_cause={}", hint))
                        .unwrap_or_default();
                    warn!(
                        "[{}] mount_nfs failed: remote={} local={} status={} elapsed={:.1}s stderr={}{}",
                        share_name,
                        remote,
                        local_mount.display(),
                        describe_exit_status(&result.status),
                        result.elapsed.as_secs_f64(),
                        stderr_summary,
                        hint_suffix
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_mount_is_unhealthy() {
        let manager = SystemMountManager;
        assert!(!manager.is_healthy(Path::new("/definitely/not/a/real/watchdog/mount")));
    }

    #[test]
    fn local_directory_is_not_treated_as_nfs_mount() {
        let manager = SystemMountManager;
        let temp = tempfile::tempdir().unwrap();
        assert!(!manager.is_healthy(temp.path()));
    }
}
