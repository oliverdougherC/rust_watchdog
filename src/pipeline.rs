use crate::config::Config;
use crate::db::WatchdogDb;
use crate::error::{Result, WatchdogError};
use crate::nfs::ensure_all_mounts;
use crate::probe::{evaluate_transcode_need, verify_transcode, TranscodeEval};
use crate::state::{PipelinePhase, StateManager};
use crate::stats::RunStats;
use crate::traits::*;
use crate::transfer::safe_replace;
use crate::util::{format_bytes, format_bytes_signed};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

/// Log a formatted message to both tracing and the TUI state log.
fn tui_log(state: &StateManager, level: &str, msg: &str) {
    let ts = chrono::Local::now().format("%H:%M:%S");
    state.append_log(format!("{} | {} | {}", ts, level, msg));
}

/// All injectable dependencies for the pipeline.
pub struct PipelineDeps {
    pub fs: Box<dyn FileSystem>,
    pub prober: Box<dyn Prober>,
    pub transcoder: Box<dyn Transcoder>,
    pub transfer: Box<dyn FileTransfer>,
    pub mount_manager: Box<dyn MountManager>,
}

/// Run a single watchdog pass: scan, filter, transcode, replace.
pub async fn run_watchdog_pass(
    config: &Config,
    base_dir: &Path,
    deps: &PipelineDeps,
    db: &Arc<WatchdogDb>,
    state: &StateManager,
    dry_run: bool,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<RunStats> {
    let mut stats = RunStats::default();

    // Close stale transcodes from previous crash
    let stale = db.close_stale_transcodes();
    if stale > 0 {
        info!("Cleaned up {} stale transcode row(s) from previous run", stale);
    }

    state.set_phase(PipelinePhase::Scanning);
    state.set_current_file(None);
    state.set_transcode_progress(0.0, 0.0, 0.0, String::new());
    state.update(|s| {
        s.run_inspected = 0;
        s.run_transcoded = 0;
        s.run_failures = 0;
        s.run_space_saved = 0;
    });
    tui_log(state, "INFO", "Starting watchdog scan pass");

    // Ensure NFS mounts
    let shares: Vec<(String, String, String)> = config
        .shares
        .iter()
        .map(|s| (s.name.clone(), s.remote_path.clone(), s.local_mount.clone()))
        .collect();

    ensure_all_mounts(deps.mount_manager.as_ref(), &config.nfs.server, &shares)?;
    state.set_nfs_healthy(true);
    tui_log(state, "INFO", &format!("NFS mounts verified ({} shares)", shares.len()));

    // Recover orphaned .old files from previous crashes.
    // If a crash occurred between rename(original→.old) and rename(.tmp→original),
    // the original file is stranded at .old while the source path is gone.
    // Scan each share for .old files whose original path is missing and restore them.
    recover_orphaned_files(deps.fs.as_ref(), config, state);

    // Prepare paths
    let preset_path = config.resolve_path(base_dir, &config.transcode.preset_file);
    let temp_dir = config.resolve_path(base_dir, &config.paths.transcode_temp);
    deps.fs.create_dir_all(&temp_dir)?;

    // Clean up stale temp files from previous crashes (e.g., leftover .av1.* or source copies).
    // Only clean files we own (prefixed with a share name + underscore).
    if let Ok(entries) = std::fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let is_ours = config.shares.iter().any(|s| name.starts_with(&format!("{}_", s.name)));
            if is_ours {
                info!("Cleaning stale temp file from previous run: {}", name);
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    let inspected_count = db.get_inspected_count();
    info!("Using database for inspected files ({} entries)", inspected_count);

    // Scan all shares
    let mut all_entries = Vec::new();
    let mut available_shares = 0usize;
    for share in &config.shares {
        let mount_path = Path::new(&share.local_mount);
        if !deps.fs.is_dir(mount_path) {
            warn!(
                "Media directory unavailable for share '{}': {}",
                share.name, share.local_mount
            );
            continue;
        }

        available_shares += 1;
        let entries = deps.fs.walk_share(
            &share.name,
            mount_path,
            &config.scan.video_extensions,
        )?;
        all_entries.extend(entries);
    }

    if all_entries.is_empty() {
        if available_shares == 0 {
            return Err(WatchdogError::NoMediaDirectories);
        }
        info!("No media files found across healthy shares; ending pass");
        tui_log(state, "INFO", "No media files found this pass");
        state.set_phase(PipelinePhase::Idle);
        state.set_last_pass_time();
        state.set_current_file(None);
        state.set_transcode_progress(0.0, 0.0, 0.0, String::new());
        return Ok(stats);
    }

    // Filter out already-inspected files and build transcode queue
    let mut transcode_queue: Vec<(FileEntry, TranscodeEval)> = Vec::new();
    let discovered_count = all_entries.len();

    for entry in &all_entries {
        // Check shutdown
        if shutdown_rx.try_recv().is_ok() {
            return Err(WatchdogError::Shutdown);
        }

        let path_str = entry.path.to_string_lossy();
        if db.is_inspected(&path_str, entry.size, entry.mtime) {
            continue;
        }

        // Probe the file
        let probe_result = tokio::task::block_in_place(|| {
            deps.prober.probe(&entry.path)
        })?;

        stats.files_inspected += 1;
        state.update(|s| {
            s.total_inspected += 1;
            s.run_inspected += 1;
        });

        match probe_result {
            None => {
                warn!(
                    "Skipping file (probe failed, cannot read metadata): {}",
                    path_str
                );
                tui_log(state, "WARN", &format!("Probe failed, skipping: {}", path_str));
            }
            Some(ref probe) => {
                let eval = evaluate_transcode_need(probe, &config.transcode);
                if eval.needs_transcode {
                    info!(
                        "QUEUE: {} (reasons: {})",
                        path_str,
                        eval.reasons.join(", ")
                    );
                    stats.files_queued += 1;
                    transcode_queue.push((entry.clone(), eval));
                } else {
                    let bitrate_text = if eval.bitrate_mbps > 0.0 {
                        format!("{:.2} Mbps", eval.bitrate_mbps)
                    } else {
                        "unknown bitrate".to_string()
                    };
                    info!(
                        "PASS: {} (codec={}, bitrate={} <= {:.2} Mbps)",
                        path_str,
                        eval.video_codec.as_deref().unwrap_or("unknown"),
                        bitrate_text,
                        config.transcode.max_average_bitrate_mbps
                    );
                    db.mark_inspected(&path_str, entry.size, entry.mtime);
                }
            }
        }
    }

    // Sort queue: largest files first
    transcode_queue.sort_by(|a, b| b.0.size.cmp(&a.0.size));

    info!("Discovered {} candidate files before filtering", discovered_count);
    info!("Queue length: {}", transcode_queue.len());
    state.set_queue_info(0, transcode_queue.len() as u32);
    tui_log(
        state,
        "INFO",
        &format!(
            "Scan complete: {} files discovered, {} inspected, {} queued for transcode",
            discovered_count, stats.files_inspected, transcode_queue.len()
        ),
    );

    // Dry-run: just report the queue
    if dry_run {
        info!("=== DRY RUN MODE ===");
        for (idx, (entry, eval)) in transcode_queue.iter().enumerate() {
            info!(
                "[DRY-RUN {}/{}] {} | share={} codec={} bitrate={:.2} Mbps size={}",
                idx + 1,
                transcode_queue.len(),
                entry.path.display(),
                entry.share_name,
                eval.video_codec.as_deref().unwrap_or("unknown"),
                eval.bitrate_mbps,
                format_bytes(entry.size)
            );
        }
        return Ok(stats);
    }

    // Transcode loop
    state.set_phase(PipelinePhase::Transcoding);

    let mut attempt_counts: std::collections::HashMap<PathBuf, u32> = std::collections::HashMap::new();
    let initial_queue_length = transcode_queue.len();
    let mut processed = 0u32;
    let mut idx = 0;

    while idx < transcode_queue.len() {
        // Check shutdown
        if shutdown_rx.try_recv().is_ok() {
            info!("Shutdown requested, stopping pipeline");
            return Err(WatchdogError::Shutdown);
        }

        let (entry, eval) = &transcode_queue[idx];
        idx += 1;

        let path = &entry.path;
        let share_name = &entry.share_name;
        let path_str = path.to_string_lossy().to_string();

        let attempt_num = {
            let count = attempt_counts.entry(path.clone()).or_insert(0);
            *count += 1;
            *count
        };
        processed += 1;

        let progress_total =
            initial_queue_length.max((processed as usize) + (transcode_queue.len() - idx));

        info!(
            "------ Beginning analysis on item {}/{}: {} (attempt {}/{}) ------",
            processed,
            progress_total,
            path_str,
            attempt_num,
            config.transcode.max_retries + 1
        );
        let display_filename = path.file_name().unwrap_or_default().to_string_lossy();
        tui_log(
            state,
            "INFO",
            &format!("Transcoding {}/{}: {}", processed, progress_total, display_filename),
        );
        state.set_current_file(Some(path_str.clone()));
        state.set_queue_info(processed, progress_total as u32);
        state.set_transcode_progress(0.0, 0.0, 0.0, String::new());

        let transcode_start = Instant::now();
        let original_size = deps.fs.file_size(path).unwrap_or(entry.size) as i64;

        // Record in DB
        let db_row_id = db.record_transcode_start(
            &path_str,
            share_name,
            eval.video_codec.as_deref(),
            eval.bitrate_bps as i64,
            original_size,
        );

        // Check disk space
        let required = (original_size as f64 * config.transcode.min_free_space_multiplier) as u64;
        if let Ok(free) = deps.fs.free_space(&temp_dir) {
            if free < required {
                warn!(
                    "[{}] Skipping {}: insufficient temp space (need {}, have {})",
                    share_name,
                    path_str,
                    format_bytes(required),
                    format_bytes(free)
                );
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "WARN", &format!("Skipped {}: insufficient disk space", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some("insufficient temp space"),
                );
                continue;
            }
        }

        // Build temp file paths (prefix with share name to avoid collisions
        // when different shares contain files with identical names)
        let source_name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let local_source = temp_dir.join(format!("{}_{}", share_name, source_name));
        let name_no_ext = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let original_ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("mkv");
        let local_output = temp_dir.join(format!("{}_{}.av1.{}", share_name, name_no_ext, original_ext));

        // Step 1: rsync source to temp
        let rsync_timeout = if original_size > 0 {
            (original_size as u64 / (1024 * 1024)).max(300)
        } else {
            3600
        };

        let transfer_result = tokio::task::block_in_place(|| {
            deps.transfer.transfer(path, &local_source, rsync_timeout)
        });

        match transfer_result {
            Ok(r) if r.success => {}
            Ok(_) => {
                error!("[{}] Failed to rsync source: {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Transfer failed: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("rsync failed"));
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(e) => {
                error!("[{}] rsync error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Transfer error: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("rsync error"));
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
        }

        // Step 2: Transcode
        let (progress_tx, mut progress_rx) = mpsc::channel::<TranscodeProgress>(32);

        let transcode_result = tokio::task::block_in_place(|| {
            let mut result: Option<Result<TranscodeResult>> = None;
            std::thread::scope(|s| {
                // Drain progress in a scoped thread so we can borrow `state`
                s.spawn(|| {
                    while let Some(p) = progress_rx.blocking_recv() {
                        state.set_transcode_progress(p.percent, p.fps, p.avg_fps, p.eta);
                    }
                });

                result = Some(deps.transcoder.transcode(
                    &local_source,
                    &local_output,
                    &preset_path,
                    &config.transcode.preset_name,
                    config.transcode.timeout_seconds,
                    progress_tx,
                ));
            });
            result.unwrap()
        });

        match transcode_result {
            Err(WatchdogError::TranscodeTimeout { .. }) => {
                warn!(
                    "[{}] Transcode timed out after {}s: {}",
                    share_name, config.transcode.timeout_seconds, path_str
                );

                if attempt_num <= config.transcode.max_retries {
                    info!(
                        "[{}] Requeueing after timeout (attempt {}/{}): {}",
                        share_name,
                        attempt_num,
                        config.transcode.max_retries + 1,
                        path_str
                    );
                    tui_log(state, "WARN", &format!("Timeout, requeuing: {} (attempt {})", display_filename, attempt_num));
                    transcode_queue.push((entry.clone(), TranscodeEval {
                        needs_transcode: true,
                        reasons: vec!["retry after timeout".to_string()],
                        bitrate_mbps: eval.bitrate_mbps,
                        video_codec: eval.video_codec.clone(),
                        bitrate_bps: eval.bitrate_bps,
                    }));
                    let dur = transcode_start.elapsed().as_secs_f64();
                    db.record_transcode_end(
                        db_row_id,
                        false,
                        0,
                        0,
                        dur,
                        Some("timeout (requeued)"),
                    );
                } else {
                    error!(
                        "[{}] Timeout retries exhausted for {}",
                        share_name, path_str
                    );
                    stats.transcode_failures += 1;
                    state.update(|s| s.run_failures += 1);
                    tui_log(state, "ERROR", &format!("Timeout retries exhausted: {}", display_filename));
                    let dur = transcode_start.elapsed().as_secs_f64();
                    db.record_transcode_end(
                        db_row_id,
                        false,
                        0,
                        0,
                        dur,
                        Some("timeout exhausted"),
                    );
                }
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(e) => {
                error!("[{}] Transcode error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Transcode error: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some(&e.to_string()),
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Ok(ref r) if !r.success || !r.output_exists => {
                error!("[{}] Transcode failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Transcode failed: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some("handbrake failed"),
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Ok(_) => {} // Success, continue to verification
        }

        // Step 3: Pre-verify output file exists and is non-zero
        if !deps.fs.exists(&local_output) {
            error!("[{}] Transcoded output file does not exist: {}", share_name, local_output.display());
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            tui_log(state, "ERROR", &format!("Output missing after transcode: {}", display_filename));
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("output file missing"));
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }
        let output_file_size = deps.fs.file_size(&local_output).unwrap_or(0);
        if output_file_size == 0 {
            error!("[{}] Transcoded output file is 0 bytes: {}", share_name, local_output.display());
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            tui_log(state, "ERROR", &format!("Output is 0 bytes: {}", display_filename));
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("output 0 bytes"));
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 3b: Verify transcode metadata (duration, streams, health)
        let verified = tokio::task::block_in_place(|| {
            verify_transcode(deps.prober.as_ref(), &local_source, &local_output)
        });

        match verified {
            Ok(true) => {
                tui_log(state, "INFO", &format!("Verification passed: {}", display_filename));
            }
            Ok(false) => {
                error!("[{}] Verification failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Verification failed: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some("verification failed"),
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(e) => {
                error!("[{}] Verification error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Verification error: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some("verification error"),
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
        }

        // Step 4: Size sanity checks
        let new_size = deps.fs.file_size(&local_output).unwrap_or(0) as i64;

        // Reject suspiciously tiny output (< 1MB or < 1% of original).
        // This catches truncated/garbage transcodes that somehow passed verification.
        let min_size = (original_size as f64 * 0.01).max(1_000_000.0) as i64;
        if new_size < min_size {
            error!(
                "[{}] Output file suspiciously small ({} vs min {}); rejecting: {}",
                share_name,
                format_bytes(new_size as u64),
                format_bytes(min_size as u64),
                path_str
            );
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            tui_log(state, "ERROR", &format!("Output too small, rejecting: {}", display_filename));
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("output suspiciously small"));
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        if new_size >= original_size {
            info!(
                "[{}] Not space-efficient (new {} >= orig {}); skipping replace",
                share_name,
                format_bytes(new_size as u64),
                format_bytes(original_size as u64)
            );
            tui_log(state, "INFO", &format!("Skipped (no space savings): {}", display_filename));
            db.mark_inspected(&path_str, entry.size, entry.mtime);
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(db_row_id, true, new_size, 0, dur, None);
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 5: Verify NFS share has enough free space for the replacement file.
        // We need at least the output file size free on the target share.
        let share_path = path.parent().unwrap_or(path);
        if let Ok(share_free) = deps.fs.free_space(share_path) {
            if share_free < new_size as u64 {
                warn!(
                    "[{}] Insufficient space on NFS share for replacement (need {}, have {}): {}",
                    share_name,
                    format_bytes(new_size as u64),
                    format_bytes(share_free),
                    path_str
                );
                tui_log(state, "WARN", &format!("Insufficient NFS space, skipping: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("NFS share space insufficient"));
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
        }

        // Step 6: Verify the original file hasn't changed since we started.
        // If it was modified (e.g., by Jellyfin, the user, or another tool),
        // we must NOT replace it — we'd overwrite the newer version.
        let current_size = deps.fs.file_size(path).unwrap_or(0);
        let current_mtime = deps.fs.file_mtime(path).unwrap_or(0.0);
        if current_size != entry.size || (current_mtime - entry.mtime).abs() > 1.0 {
            warn!(
                "[{}] Original file changed during transcode (size: {} -> {}, mtime delta: {:.1}s); aborting replace: {}",
                share_name,
                entry.size,
                current_size,
                (current_mtime - entry.mtime).abs(),
                path_str
            );
            tui_log(state, "WARN", &format!("File changed during transcode, skipping replace: {}", display_filename));
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(db_row_id, false, 0, 0, dur, Some("original file changed"));
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 7: Safe replace
        let replaced = tokio::task::block_in_place(|| {
            safe_replace(
                deps.fs.as_ref(),
                deps.transfer.as_ref(),
                path,
                &local_output,
            )
        });

        match replaced {
            Ok(true) => {
                let space_saved = original_size - new_size;
                stats.files_transcoded += 1;
                stats.space_saved_bytes += space_saved;

                // Update DB
                if let Ok(new_mtime) = deps.fs.file_mtime(path) {
                    let new_file_size = deps.fs.file_size(path).unwrap_or(0);
                    db.mark_inspected(&path_str, new_file_size, new_mtime);
                }
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(db_row_id, true, new_size, space_saved, dur, None);

                // Log space saved to timeseries
                let cumulative = db.get_total_space_saved() + space_saved;
                db.log_space_saved(cumulative);

                info!(
                    "[{}] SUCCESS: Replaced {} | Saved {}",
                    share_name, path_str, format_bytes_signed(space_saved)
                );
                tui_log(
                    state,
                    "SUCCESS",
                    &format!("Replaced {} | Saved {}", display_filename, format_bytes_signed(space_saved)),
                );

                // Update state
                state.update(|s| {
                    s.total_transcoded += 1;
                    s.total_space_saved += space_saved;
                    s.run_transcoded += 1;
                    s.run_space_saved += space_saved;
                });
            }
            Ok(false) => {
                error!("[{}] Safe replace failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Replace failed: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some("safe replace failed"),
                );
            }
            Err(e) => {
                error!("[{}] Replace error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(state, "ERROR", &format!("Replace error: {}", display_filename));
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    false,
                    0,
                    0,
                    dur,
                    Some(&e.to_string()),
                );
            }
        }

        cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
    }

    state.set_phase(PipelinePhase::Idle);
    state.set_last_pass_time();
    state.set_current_file(None);
    state.set_transcode_progress(0.0, 0.0, 0.0, String::new());
    tui_log(
        state,
        "INFO",
        &format!(
            "Pass complete: {} transcoded, {} failures, {} saved",
            stats.files_transcoded, stats.transcode_failures, format_bytes_signed(stats.space_saved_bytes)
        ),
    );

    Ok(stats)
}

/// Scan NFS shares for orphaned .old files from interrupted safe_replace operations.
/// If the original file is missing (crash between rename steps), restore from .old.
/// If both the original and .old exist (.old cleanup failed), remove the stale .old.
fn recover_orphaned_files(fs: &dyn FileSystem, config: &Config, state: &StateManager) {
    for share in &config.shares {
        let mount_path = Path::new(&share.local_mount);
        if !fs.is_dir(mount_path) {
            continue;
        }

        let walker = walkdir::WalkDir::new(mount_path)
            .follow_links(false)
            .into_iter();

        for entry in walker.filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };

            // Look for files ending in .old (e.g., "Movie.mkv.old")
            if !name.ends_with(".old") {
                continue;
            }

            // Derive the original path by stripping the .old suffix
            let original_name = &name[..name.len() - 4]; // Remove ".old"
            let original_path = path.parent().unwrap_or(Path::new(".")).join(original_name);

            if fs.exists(&original_path) {
                // Original exists — .old is stale from a completed replace. Safe to remove.
                warn!(
                    "[{}] Removing stale .old (original exists): {}",
                    share.name,
                    path.display()
                );
                let _ = fs.remove(path);
            } else {
                // Original is MISSING — crash between rename steps. Restore from .old.
                error!(
                    "[{}] RECOVERING orphaned file: {} → {}",
                    share.name,
                    path.display(),
                    original_path.display()
                );
                match fs.rename(path, &original_path) {
                    Ok(_) => {
                        info!(
                            "[{}] Successfully recovered: {}",
                            share.name,
                            original_path.display()
                        );
                        tui_log(
                            state,
                            "WARN",
                            &format!("Recovered orphaned file: {}", original_name),
                        );
                    }
                    Err(e) => {
                        error!(
                            "[{}] FAILED to recover orphaned file {} → {}: {}",
                            share.name,
                            path.display(),
                            original_path.display(),
                            e
                        );
                        tui_log(
                            state,
                            "ERROR",
                            &format!("FAILED to recover orphaned file: {} (manual intervention needed)", name),
                        );
                    }
                }
            }

            // Also clean up any corresponding .tmp file
            let tmp_name = format!("{}.tmp", original_name);
            let tmp_path = path.parent().unwrap_or(Path::new(".")).join(tmp_name);
            if fs.exists(&tmp_path) {
                warn!(
                    "[{}] Removing stale .tmp: {}",
                    share.name,
                    tmp_path.display()
                );
                let _ = fs.remove(&tmp_path);
            }
        }
    }
}

/// Clean up temporary files, logging warnings on failure.
fn cleanup_temp_files(fs: &dyn FileSystem, paths: &[&Path]) {
    for path in paths {
        if fs.exists(path) {
            if let Err(e) = fs.remove(path) {
                warn!("Failed to clean up temp file {}: {}", path.display(), e);
            }
        }
    }
}

/// Run the pipeline in a loop with configurable interval.
pub async fn run_pipeline_loop(
    config: Config,
    base_dir: PathBuf,
    deps: PipelineDeps,
    db: Arc<WatchdogDb>,
    state: StateManager,
    dry_run: bool,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut retry_delay = 30u64;

    loop {
        match run_watchdog_pass(
            &config,
            &base_dir,
            &deps,
            &db,
            &state,
            dry_run,
            shutdown_rx.resubscribe(),
        )
        .await
        {
            Ok(stats) => {
                info!(
                    "Pass complete: inspected={} queued={} transcoded={} failures={} saved={}",
                    stats.files_inspected,
                    stats.files_queued,
                    stats.files_transcoded,
                    stats.transcode_failures,
                    format_bytes_signed(stats.space_saved_bytes)
                );
                retry_delay = 30;
            }
            Err(WatchdogError::Shutdown) => {
                info!("Pipeline shutting down");
                return;
            }
            Err(e) => {
                error!("Watchdog pass failed: {}", e);
                state.set_nfs_healthy(false);
                tui_log(&state, "ERROR", &format!("Pass failed: {}", e));
                tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay)).await;
                retry_delay = (retry_delay * 2).min(300);
                continue;
            }
        }

        if dry_run {
            return;
        }

        // Wait for next scan interval or shutdown
        state.set_phase(PipelinePhase::Waiting);
        state.set_current_file(None);
        state.set_transcode_progress(0.0, 0.0, 0.0, String::new());
        tui_log(
            &state,
            "INFO",
            &format!("Waiting {}s until next scan", config.scan.interval_seconds),
        );

        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(config.scan.interval_seconds)) => {}
            _ = shutdown_rx.recv() => {
                info!("Pipeline shutting down during wait");
                return;
            }
        }
    }
}
