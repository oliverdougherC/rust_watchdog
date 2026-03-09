use crate::error::{Result, WatchdogError};
use crate::process::{
    configure_subprocess_group, describe_exit_status, format_command_for_log, infer_failure_hint,
    summarize_output_tail, terminate_subprocess,
};
use crate::traits::{FileSystem, FileTransfer, TransferProgress, TransferResult, TransferStage};
use regex::Regex;
use std::io::{self, Read};
use std::path::Path;
use std::process::Command;
use std::process::Stdio;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub const WATCHDOG_TMP_SUFFIX: &str = ".watchdog.tmp";
pub const WATCHDOG_OLD_SUFFIX: &str = ".watchdog.old";

static RSYNC_PROGRESS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*[0-9,]+\s+(\d{1,3})%\s+([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z/]+)\s+([0-9:]{4,8})")
        .unwrap()
});

#[derive(Default)]
struct CapturedOutput {
    bytes: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
}

#[derive(Debug)]
struct TransferProgressSignals {
    max_parsed_percent: f64,
    last_parsed_at: Option<Instant>,
}

impl Default for TransferProgressSignals {
    fn default() -> Self {
        Self {
            max_parsed_percent: -1.0,
            last_parsed_at: None,
        }
    }
}

fn push_tail(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    if limit == 0 {
        return false;
    }
    buf.extend_from_slice(chunk);
    if buf.len() > limit {
        let drop_len = buf.len() - limit;
        buf.drain(0..drop_len);
        return true;
    }
    false
}

fn normalize_rate_to_mib(value: f64, unit: &str) -> Option<f64> {
    let normalized = unit.trim().to_ascii_lowercase();
    if !normalized.ends_with("/s") {
        return None;
    }
    let unit = normalized.trim_end_matches("/s");
    let mib = match unit {
        "b" => value / (1024.0 * 1024.0),
        "kb" | "kib" => value / 1024.0,
        "mb" | "mib" => value,
        "gb" | "gib" => value * 1024.0,
        "tb" | "tib" => value * 1024.0 * 1024.0,
        _ => return None,
    };
    Some(mib.max(0.0))
}

fn parse_rsync_progress(line: &str) -> Option<(f64, f64, String)> {
    let clean = line.replace(',', "");
    let caps = RSYNC_PROGRESS_RE.captures(&clean)?;
    let percent: f64 = caps.get(1)?.as_str().parse().ok()?;
    let rate: f64 = caps.get(2)?.as_str().parse().ok()?;
    let unit = caps.get(3)?.as_str();
    let eta = caps.get(4)?.as_str().to_string();
    let rate_mib = normalize_rate_to_mib(rate, unit)?;
    Some((percent.clamp(0.0, 100.0), rate_mib, eta))
}

fn format_eta_clock(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

fn estimate_percent_from_sizes(source_size: u64, dest_size: u64) -> Option<f64> {
    if source_size == 0 {
        return None;
    }
    let clamped_dest = dest_size.min(source_size);
    Some(((clamped_dest as f64 / source_size as f64) * 100.0).clamp(0.0, 99.9))
}

fn should_emit_preparing_heartbeat(
    start: Instant,
    now: Instant,
    last_emit: Instant,
    saw_any_progress: bool,
) -> bool {
    !saw_any_progress
        && now.duration_since(start) >= Duration::from_secs(1)
        && now.duration_since(last_emit) >= Duration::from_secs(1)
}

fn build_rsync_command(source: &Path, dest: &Path) -> Command {
    let mut cmd = Command::new("rsync");
    configure_subprocess_group(&mut cmd);
    cmd.args(["-avh", "--progress", "--ignore-times"])
        .arg(source)
        .arg(dest)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn spawn_rsync_progress_thread<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
    stage: TransferStage,
    progress_tx: Option<mpsc::Sender<TransferProgress>>,
    signals: Arc<Mutex<TransferProgressSignals>>,
) -> thread::JoinHandle<CapturedOutput> {
    thread::spawn(move || {
        let mut out = CapturedOutput::default();
        let mut tmp = [0u8; 4096];
        let mut line_buf = Vec::new();
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        let mut last_percent = -1.0f64;

        let mut process_line = |buf: &[u8]| {
            if buf.is_empty() {
                return;
            }
            out.truncated |= push_tail(&mut out.bytes, buf, limit);
            out.truncated |= push_tail(&mut out.bytes, b"\n", limit);
            if let Some(ref tx) = progress_tx {
                let line = String::from_utf8_lossy(buf);
                if let Some((percent, rate_mib_per_sec, eta)) = parse_rsync_progress(&line) {
                    let now = Instant::now();
                    if let Ok(mut shared) = signals.lock() {
                        if percent > shared.max_parsed_percent {
                            shared.max_parsed_percent = percent;
                        }
                        shared.last_parsed_at = Some(now);
                    }
                    let should_emit = percent >= 100.0
                        || (percent - last_percent).abs() >= 0.5
                        || now.duration_since(last_emit) >= Duration::from_millis(250);
                    if should_emit {
                        let _ = tx.try_send(TransferProgress {
                            stage,
                            percent,
                            rate_mib_per_sec,
                            eta,
                        });
                        last_emit = now;
                        last_percent = percent;
                    }
                }
            }
        };

        loop {
            match reader.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    out.total_bytes = out.total_bytes.saturating_add(n);
                    for &byte in &tmp[..n] {
                        if byte == b'\n' || byte == b'\r' {
                            process_line(&line_buf);
                            line_buf.clear();
                        } else {
                            line_buf.push(byte);
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        if !line_buf.is_empty() {
            process_line(&line_buf);
        }
        out
    })
}

/// Real rsync-based file transfer.
pub struct RsyncTransfer;

impl FileTransfer for RsyncTransfer {
    fn transfer(
        &self,
        source: &Path,
        dest: &Path,
        timeout_secs: u64,
        stage: TransferStage,
        progress_tx: Option<mpsc::Sender<TransferProgress>>,
    ) -> Result<TransferResult> {
        let mut cmd = build_rsync_command(source, dest);
        let command_repr = format_command_for_log(&cmd);
        info!(
            "Starting rsync transfer: stage={} source={} dest={} timeout={}s",
            stage.as_str(),
            source.display(),
            dest.display(),
            timeout_secs
        );
        let mut child = cmd.spawn().map_err(|e| WatchdogError::Transfer {
            path: source.to_path_buf(),
            reason: format!("Failed to execute rsync command `{}`: {}", command_repr, e),
        })?;

        let stdout = child.stdout.take().ok_or_else(|| WatchdogError::Transfer {
            path: source.to_path_buf(),
            reason: "Failed to capture rsync stdout".to_string(),
        })?;
        let stderr = child.stderr.take().ok_or_else(|| WatchdogError::Transfer {
            path: source.to_path_buf(),
            reason: "Failed to capture rsync stderr".to_string(),
        })?;

        let progress_signals = Arc::new(Mutex::new(TransferProgressSignals::default()));
        let stderr_progress_tx = progress_tx.clone();
        let stdout_handle = spawn_rsync_progress_thread(
            stdout,
            64 * 1024,
            stage,
            progress_tx.clone(),
            Arc::clone(&progress_signals),
        );
        let stderr_handle = spawn_rsync_progress_thread(
            stderr,
            256 * 1024,
            stage,
            stderr_progress_tx,
            Arc::clone(&progress_signals),
        );

        let start = Instant::now();
        let source_size = std::fs::metadata(source).map(|m| m.len()).unwrap_or(0);
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        let mut last_fallback_percent = -1.0f64;
        let mut last_dest_size = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
        let mut last_dest_growth_at = start;
        let mut last_rate_sample: Option<(Instant, u64)> = None;
        let mut estimated_rate_mib_per_sec = 0.0f64;
        let mut saw_any_progress = false;
        let mut timed_out = false;
        let status = loop {
            if timeout_secs > 0 && start.elapsed().as_secs() > timeout_secs {
                timed_out = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transfer {
                    path: source.to_path_buf(),
                    reason: format!("Failed to wait for timed out rsync process: {}", e),
                })?;
            }

            let now = Instant::now();
            let dest_size = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
            if dest_size > last_dest_size {
                if let Some((sample_at, sample_size)) = last_rate_sample {
                    let delta_bytes = dest_size.saturating_sub(sample_size);
                    let delta_secs = now.duration_since(sample_at).as_secs_f64();
                    if delta_secs > 0.0 && delta_bytes > 0 {
                        estimated_rate_mib_per_sec =
                            (delta_bytes as f64 / (1024.0 * 1024.0)) / delta_secs;
                    }
                }
                last_dest_growth_at = now;
                last_dest_size = dest_size;
                last_rate_sample = Some((now, dest_size));
                saw_any_progress = true;
            } else if last_rate_sample.is_none() && dest_size > 0 {
                last_rate_sample = Some((now, dest_size));
                last_dest_size = dest_size;
                saw_any_progress = true;
            }

            if let Some(ref tx) = progress_tx {
                let (parsed_percent, parsed_recent) = progress_signals
                    .lock()
                    .ok()
                    .map(|shared| {
                        let recent = shared
                            .last_parsed_at
                            .is_some_and(|t| now.duration_since(t) <= Duration::from_secs(1));
                        (shared.max_parsed_percent, recent)
                    })
                    .unwrap_or((-1.0, false));
                if parsed_percent >= 0.0 {
                    saw_any_progress = true;
                }

                if !parsed_recent
                    && source_size > 0
                    && now.duration_since(last_dest_growth_at) <= Duration::from_secs(2)
                {
                    if let Some(percent) = estimate_percent_from_sizes(source_size, dest_size) {
                        let baseline = parsed_percent.max(last_fallback_percent);
                        let should_emit = percent >= 100.0
                            || (percent - baseline).abs() >= 0.5
                            || now.duration_since(last_emit) >= Duration::from_secs(1);
                        if should_emit {
                            let eta = if estimated_rate_mib_per_sec > 0.0 && dest_size < source_size
                            {
                                let remaining = source_size - dest_size;
                                let eta_secs = ((remaining as f64 / (1024.0 * 1024.0))
                                    / estimated_rate_mib_per_sec)
                                    .max(0.0)
                                    .round() as u64;
                                format_eta_clock(eta_secs)
                            } else {
                                "preparing".to_string()
                            };
                            let _ = tx.try_send(TransferProgress {
                                stage,
                                percent,
                                rate_mib_per_sec: estimated_rate_mib_per_sec.max(0.0),
                                eta,
                            });
                            last_emit = now;
                            last_fallback_percent = percent;
                        }
                    }
                }

                if should_emit_preparing_heartbeat(start, now, last_emit, saw_any_progress) {
                    let _ = tx.try_send(TransferProgress {
                        stage,
                        percent: 0.0,
                        rate_mib_per_sec: 0.0,
                        eta: "preparing".to_string(),
                    });
                    last_emit = now;
                }
            }

            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(e) => {
                    return Err(WatchdogError::Transfer {
                        path: source.to_path_buf(),
                        reason: format!("Failed while waiting for rsync process: {}", e),
                    });
                }
            }
        };

        let stdout_capture = stdout_handle.join().unwrap_or_default();
        let stderr_capture = stderr_handle.join().unwrap_or_default();
        let elapsed = start.elapsed().as_secs_f64();

        if timed_out {
            warn!(
                "rsync transfer timed out: stage={} source={} dest={} timeout={}s elapsed={:.1}s command={}",
                stage.as_str(),
                source.display(),
                dest.display(),
                timeout_secs,
                elapsed,
                command_repr
            );
            return Ok(TransferResult { success: false });
        }
        if !status.success() {
            let stderr_summary = summarize_output_tail(&stderr_capture.bytes, 400);
            let stdout_summary = summarize_output_tail(&stdout_capture.bytes, 240);
            let details = if stderr_summary == "(empty)" && stdout_summary != "(empty)" {
                format!("stdout={}", stdout_summary)
            } else {
                format!("stderr={}", stderr_summary)
            };
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            let trunc_suffix = if stderr_capture.truncated {
                format!(
                    " [stderr tail truncated; captured {} of {} bytes]",
                    stderr_capture.bytes.len(),
                    stderr_capture.total_bytes
                )
            } else {
                String::new()
            };
            warn!(
                "rsync transfer failed: stage={} source={} dest={} status={} elapsed={:.1}s {}{}{}",
                stage.as_str(),
                source.display(),
                dest.display(),
                describe_exit_status(&status),
                elapsed,
                details,
                trunc_suffix,
                hint_suffix
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
    progress_tx: Option<mpsc::Sender<TransferProgress>>,
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
        if let Err(e) = fs.remove(&temp_remote) {
            error!(
                "Failed to remove stale watchdog temp {}: {}; aborting replace to fail closed",
                temp_remote.display(),
                e
            );
            return Ok(false);
        }
    }
    // If .watchdog.old exists AND source_path exists, it is from a previous successful
    // replace where cleanup (step 4) failed — safe to remove.
    if fs.exists(&old_remote) {
        warn!(
            "Removing stale watchdog backup from previous run: {}",
            old_remote.display()
        );
        if let Err(e) = fs.remove(&old_remote) {
            error!(
                "Failed to remove stale watchdog backup {}: {}; aborting replace to fail closed",
                old_remote.display(),
                e
            );
            return Ok(false);
        }
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

    // Step 1: rsync new file to temp path on remote.
    let file_size = fs.file_size(new_local_path).unwrap_or(0);
    let timeout = if file_size > 0 {
        (file_size / (1024 * 1024)).max(300)
    } else {
        3600
    };

    let result = transfer.transfer(
        new_local_path,
        &temp_remote,
        timeout,
        TransferStage::Export,
        progress_tx,
    )?;
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
            _stage: TransferStage,
            _progress_tx: Option<mpsc::Sender<TransferProgress>>,
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

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new, None).unwrap();
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

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new, None).unwrap();
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

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new, None).unwrap();
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

        let ok = safe_replace(fs.as_ref(), &transfer, source, local_new, None).unwrap();
        assert!(!ok);
        assert!(!fs.exists(source));
        assert!(fs.exists(old_remote));
    }

    #[test]
    fn test_parse_rsync_progress_openrsync() {
        let line = "       14385152  98%  992.11KB/s   00:00:00";
        let (percent, rate_mib, eta) = parse_rsync_progress(line).unwrap();
        assert!((percent - 98.0).abs() < 0.01);
        assert!(rate_mib > 0.0);
        assert_eq!(eta, "00:00:00");
    }

    #[test]
    fn test_parse_rsync_progress_gnu_with_suffix() {
        let line = "       2097152 100%  296.33MB/s   00:00:00 (xfer#1, to-check=0/1)";
        let (percent, rate_mib, eta) = parse_rsync_progress(line).unwrap();
        assert!((percent - 100.0).abs() < 0.01);
        assert!(rate_mib > 0.0);
        assert_eq!(eta, "00:00:00");
    }

    #[test]
    fn test_parse_rsync_progress_ignores_non_progress_lines() {
        assert!(parse_rsync_progress("Transfer starting: 1 files").is_none());
        assert!(parse_rsync_progress("sent 13309k bytes  received 42 bytes").is_none());
    }

    #[test]
    fn test_preparing_heartbeat_when_no_progress_yet() {
        let start = Instant::now();
        let now = start + Duration::from_secs(2);
        let last_emit = start + Duration::from_millis(500);
        assert!(should_emit_preparing_heartbeat(
            start, now, last_emit, false
        ));
        assert!(!should_emit_preparing_heartbeat(
            start, now, last_emit, true
        ));
    }

    #[test]
    fn test_estimate_percent_from_dest_size_growth() {
        let percent = estimate_percent_from_sizes(1000, 550).unwrap();
        assert!((percent - 55.0).abs() < 0.01);
        let capped = estimate_percent_from_sizes(1000, 2000).unwrap();
        assert!(capped < 100.0);
    }

    #[test]
    fn test_rsync_command_uses_ignore_times_not_checksum() {
        let cmd = build_rsync_command(Path::new("/src.mkv"), Path::new("/dst.mkv"));
        let args = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(args.iter().any(|a| a == "--ignore-times"));
        assert!(!args.iter().any(|a| a == "--checksum"));
    }
}
