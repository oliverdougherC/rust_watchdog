use crate::config::Config;
use crate::db::{TranscodeOutcome, WatchdogDb};
use crate::error::{Result, WatchdogError};
use crate::event_journal::{append_event, resolve_event_journal_path};
use crate::nfs::ensure_all_mounts;
use crate::notify::{send_webhook, NotifyEvent};
use crate::probe::{evaluate_transcode_need, verify_transcode, TranscodeEval};
use crate::state::{PipelinePhase, ProgressStage, StateManager};
use crate::stats::RunStats;
use crate::traits::*;
use crate::transfer::{safe_replace, WATCHDOG_OLD_SUFFIX, WATCHDOG_TMP_SUFFIX};
use crate::util::{format_bytes, format_bytes_signed};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

const CANCEL_NONE: u8 = 0;
const CANCEL_SHUTDOWN: u8 = 1;
const CANCEL_PAUSE: u8 = 2;
const FAILURE_CODE_QUARANTINED_FILE: &str = "quarantined_file";
const FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP: &str = "auto_paused_safety_trip";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    Inspected,
    Young,
    Cooldown,
    Filtered,
    InUse,
    Quarantined,
}

#[derive(Debug, Clone, Copy)]
enum FailureCode {
    TransferFailedRequeued,
    TransferErrorRequeued,
    TransferFailed,
    TransferError,
    InsufficientTempSpace,
    TempSpaceProbeError,
    TimeoutRequeued,
    TimeoutExhausted,
    StalledRequeued,
    StalledExhausted,
    ScanTimeout,
    InterruptedShutdown,
    InterruptedPause,
    Interrupted,
    TranscodeError,
    TranscodeFailed,
    OutputMissing,
    OutputZeroBytes,
    VerificationFailed,
    VerificationError,
    OutputSuspiciouslySmall,
    InsufficientNfsSpace,
    NfsSpaceProbeError,
    SourceChangedDuringTranscode,
    FileInUse,
    SafeReplaceFailed,
    SafeReplaceError,
    QuarantinedFile,
}

impl FailureCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::TransferFailedRequeued => "transfer_failed_requeued",
            Self::TransferErrorRequeued => "transfer_error_requeued",
            Self::TransferFailed => "transfer_failed",
            Self::TransferError => "transfer_error",
            Self::InsufficientTempSpace => "insufficient_temp_space",
            Self::TempSpaceProbeError => "temp_space_probe_error",
            Self::TimeoutRequeued => "timeout_requeued",
            Self::TimeoutExhausted => "timeout_exhausted",
            Self::StalledRequeued => "stalled_requeued",
            Self::StalledExhausted => "stalled_exhausted",
            Self::ScanTimeout => "scan_timeout",
            Self::InterruptedShutdown => "interrupted_shutdown",
            Self::InterruptedPause => "interrupted_pause",
            Self::Interrupted => "interrupted",
            Self::TranscodeError => "transcode_error",
            Self::TranscodeFailed => "transcode_failed",
            Self::OutputMissing => "output_missing",
            Self::OutputZeroBytes => "output_zero_bytes",
            Self::VerificationFailed => "verification_failed",
            Self::VerificationError => "verification_error",
            Self::OutputSuspiciouslySmall => "output_suspiciously_small",
            Self::InsufficientNfsSpace => "insufficient_nfs_space",
            Self::NfsSpaceProbeError => "nfs_space_probe_error",
            Self::SourceChangedDuringTranscode => "source_changed_during_transcode",
            Self::FileInUse => "file_in_use",
            Self::SafeReplaceFailed => "safe_replace_failed",
            Self::SafeReplaceError => "safe_replace_error",
            Self::QuarantinedFile => FAILURE_CODE_QUARANTINED_FILE,
        }
    }
}

struct GlobMatcher {
    path_set: Option<GlobSet>,
    basename_set: Option<GlobSet>,
}

#[derive(Debug)]
struct QueueCandidate {
    size: u64,
    seq: u64,
    entry: FileEntry,
    eval: TranscodeEval,
}

impl PartialEq for QueueCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.seq == other.seq
    }
}

impl Eq for QueueCandidate {}

impl PartialOrd for QueueCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.size.cmp(&other.size).then(self.seq.cmp(&other.seq))
    }
}

fn stable_path_hash(path: &Path) -> u64 {
    // Deterministic FNV-1a hash for stable temp naming across runs.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in path.to_string_lossy().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0001_0000_01b3);
    }
    hash
}

fn build_local_temp_paths(
    temp_dir: &Path,
    share_name: &str,
    source_path: &Path,
) -> (PathBuf, PathBuf) {
    let hash = format!("{:016x}", stable_path_hash(source_path));
    let source_name = source_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let name_no_ext = source_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let original_ext = source_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("mkv");

    let local_source = temp_dir.join(format!("{}_{}__{}", share_name, hash, source_name));
    let local_output = temp_dir.join(format!(
        "{}_{}__{}.av1.{}",
        share_name, hash, name_no_ext, original_ext
    ));
    (local_source, local_output)
}

/// Log a formatted message to both tracing and the TUI state log.
fn tui_log(state: &StateManager, level: &str, msg: &str) {
    let ts = chrono::Local::now().format("%H:%M:%S");
    state.append_log(format!("{} | {} | {}", ts, level, msg));
}

/// All injectable dependencies for the pipeline.
pub struct PipelineDeps {
    pub fs: Arc<dyn FileSystem>,
    pub prober: Box<dyn Prober>,
    pub transcoder: Box<dyn Transcoder>,
    pub transfer: Box<dyn FileTransfer>,
    pub mount_manager: Box<dyn MountManager>,
    pub in_use_detector: Box<dyn InUseDetector>,
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

    if !dry_run {
        // Close stale transcodes from previous crash
        let stale = db.close_stale_transcodes();
        if stale > 0 {
            info!(
                "Cleaned up {} stale transcode row(s) from previous run",
                stale
            );
        }
    }

    state.set_phase(PipelinePhase::Scanning);
    state.set_current_file(None);
    state.reset_file_progress();
    state.update(|s| {
        s.run_inspected = 0;
        s.run_transcoded = 0;
        s.run_failures = 0;
        s.run_space_saved = 0;
        s.run_skipped_inspected = 0;
        s.run_skipped_young = 0;
        s.run_skipped_cooldown = 0;
        s.run_skipped_filtered = 0;
        s.run_skipped_in_use = 0;
        s.run_skipped_quarantined = 0;
        s.quarantined_files = db.quarantine_count().max(0) as u64;
    });
    tui_log(state, "INFO", "Starting watchdog scan pass");
    emit_event(
        config,
        base_dir,
        "pass_start",
        json!({
            "dry_run": dry_run,
            "share_count": config.shares.len(),
        }),
    );

    if is_pause_requested(config, base_dir) {
        state.set_phase(PipelinePhase::Paused);
        tui_log(state, "WARN", "Pause file present; pausing pipeline");
        return Err(WatchdogError::Paused);
    }

    // Ensure NFS mounts (dry-run is read-only: do not perform remount attempts)
    let shares: Vec<(String, String, String)> = config
        .shares
        .iter()
        .map(|s| (s.name.clone(), s.remote_path.clone(), s.local_mount.clone()))
        .collect();

    if dry_run {
        let unhealthy: Vec<String> = shares
            .iter()
            .filter_map(|(name, _remote, local_mount)| {
                (!deps.mount_manager.is_healthy(Path::new(local_mount))).then_some(name.clone())
            })
            .collect();
        if let Some(first) = unhealthy.first() {
            return Err(WatchdogError::NfsMount {
                share: first.clone(),
                reason: format!(
                    "Dry-run mode is read-only and will not remount unhealthy shares ({} unhealthy)",
                    unhealthy.len()
                ),
            });
        }
    } else {
        ensure_all_mounts(deps.mount_manager.as_ref(), &config.nfs.server, &shares)?;
    }
    let share_health = config
        .shares
        .iter()
        .map(|share| {
            let healthy = deps.mount_manager.is_healthy(Path::new(&share.local_mount));
            (share.name.clone(), healthy)
        })
        .collect::<Vec<_>>();
    state.set_share_health(share_health.clone());
    state.set_nfs_healthy(share_health.iter().all(|(_, healthy)| *healthy));
    tui_log(
        state,
        "INFO",
        &format!("NFS mounts verified ({} shares)", shares.len()),
    );

    // Prepare paths
    let preset_path = config.resolve_path(base_dir, &config.transcode.preset_file);
    let temp_dir = config.resolve_path(base_dir, &config.paths.transcode_temp);
    if !dry_run {
        deps.fs.create_dir_all(&temp_dir)?;
    }

    // Clean up stale temp files from previous crashes (e.g., leftover .av1.* or source copies).
    // Only clean files we own (prefixed with a share name + underscore).
    if !dry_run {
        if let Ok(entries) = deps.fs.list_dir(&temp_dir) {
            for path in entries {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let is_ours = config
                    .shares
                    .iter()
                    .any(|s| name.starts_with(&format!("{}_", s.name)));
                if is_ours {
                    info!("Cleaning stale temp file from previous run: {}", name);
                    if let Err(e) = deps.fs.remove(&path) {
                        error!(
                            "Failed to remove stale temp artifact {}: {}; aborting pass to fail closed",
                            path.display(),
                            e
                        );
                        return Err(e);
                    }
                }
            }
        }
    }

    let inspected_count = db.get_inspected_count();
    info!(
        "Using database for inspected files ({} entries)",
        inspected_count
    );

    // Scan all healthy shares in parallel.
    let mut share_scan_jobs = Vec::new();
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
        share_scan_jobs.push((share.name.clone(), mount_path.to_path_buf()));
    }

    let include_globs = build_glob_matcher(&config.scan.include_globs)?;
    let exclude_globs = build_glob_matcher(&config.scan.exclude_globs)?;
    let pause_path = config.resolve_path(base_dir, &config.safety.pause_file);
    let now_ts = chrono::Utc::now().timestamp();
    let queue_cap = if config.scan.max_files_per_pass > 0 {
        Some(config.scan.max_files_per_pass as usize)
    } else {
        None
    };

    // Filter out already-inspected files and build transcode queue
    let mut transcode_queue: Vec<(FileEntry, TranscodeEval)> = Vec::new();
    let mut capped_queue: Option<BinaryHeap<Reverse<QueueCandidate>>> =
        queue_cap.map(|_| BinaryHeap::new());
    let mut queue_seq: u64 = 0;
    let mut total_transcode_candidates = 0u64;
    let mut pending_inspected: Vec<(String, u64, f64)> = Vec::new();
    let mut discovered_count = 0usize;
    let mut duplicate_count = 0usize;
    let mut last_scan_log = Instant::now();
    let mut probe_batch: Vec<FileEntry> = Vec::new();
    let mut seen_paths = HashSet::<PathBuf>::new();

    let worker_count = if config.scan.probe_workers > 0 {
        config.scan.probe_workers as usize
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }
    .max(1);
    info!("Probing with {} parallel worker(s)", worker_count);
    tui_log(
        state,
        "INFO",
        &format!("Probing with {} parallel worker(s)", worker_count),
    );

    const PROBE_BATCH_SIZE: usize = 512;
    let scanned_shares = scan_shares_parallel(
        deps.fs.clone(),
        &share_scan_jobs,
        &config.scan.video_extensions,
        Duration::from_secs(config.safety.share_scan_timeout_seconds.max(1)),
        &mut shutdown_rx,
    )?;
    for (_share_name, entries) in scanned_shares {
        for entry in entries {
            discovered_count += 1;
            let scanned = discovered_count;
            state.set_queue_info(scanned as u32, scanned as u32);
            if scanned == 1 || last_scan_log.elapsed().as_secs() >= 2 {
                tui_log(
                    state,
                    "INFO",
                    &format!(
                        "Scanning: {} files (pending probe: {}, inspected this pass: {})",
                        scanned,
                        probe_batch.len(),
                        stats.files_inspected
                    ),
                );
                last_scan_log = Instant::now();
            }

            if shutdown_requested(&mut shutdown_rx) {
                return Err(WatchdogError::Shutdown);
            }
            if is_pause_requested(config, base_dir) {
                state.set_phase(PipelinePhase::Paused);
                tui_log(state, "WARN", "Pause file detected during scan; pausing");
                return Err(WatchdogError::Paused);
            }

            if !seen_paths.insert(entry.path.clone()) {
                duplicate_count += 1;
                continue;
            }

            if !path_matches_filters(&entry.path, include_globs.as_ref(), exclude_globs.as_ref()) {
                increment_skip_counter(state, SkipReason::Filtered);
                continue;
            }

            let file_age_secs = (now_ts as f64 - entry.mtime).max(0.0);
            if file_age_secs < config.safety.min_file_age_seconds as f64 {
                increment_skip_counter(state, SkipReason::Young);
                continue;
            }

            let path_string = entry.path.to_string_lossy().to_string();
            if db.is_quarantined(&path_string) {
                increment_skip_counter(state, SkipReason::Quarantined);
                if !dry_run {
                    db.record_quarantine_skip(&path_string, "quarantined_file");
                }
                let quarantined_count = db.quarantine_count().max(0) as u64;
                state.update(|s| {
                    s.last_failure_code = Some(FailureCode::QuarantinedFile.as_str().to_string());
                    s.quarantined_files = quarantined_count;
                });
                continue;
            }
            if let Some(failure_state) = db.get_file_failure_state(&path_string) {
                if failure_state.next_eligible_at > now_ts {
                    increment_skip_counter(state, SkipReason::Cooldown);
                    continue;
                }
            }

            let path_str = entry.path.to_string_lossy();
            if db.is_inspected(&path_str, entry.size, entry.mtime) {
                increment_skip_counter(state, SkipReason::Inspected);
                continue;
            }

            probe_batch.push(entry);
            if probe_batch.len() >= PROBE_BATCH_SIZE {
                process_probe_batch(
                    &mut probe_batch,
                    deps.prober.as_ref(),
                    config,
                    state,
                    &mut shutdown_rx,
                    worker_count,
                    &mut stats,
                    &mut transcode_queue,
                    &mut capped_queue,
                    &mut queue_seq,
                    &mut total_transcode_candidates,
                    &mut pending_inspected,
                    queue_cap,
                )?;
            }
        }
    }

    if !probe_batch.is_empty() {
        process_probe_batch(
            &mut probe_batch,
            deps.prober.as_ref(),
            config,
            state,
            &mut shutdown_rx,
            worker_count,
            &mut stats,
            &mut transcode_queue,
            &mut capped_queue,
            &mut queue_seq,
            &mut total_transcode_candidates,
            &mut pending_inspected,
            queue_cap,
        )?;
    }

    if discovered_count == 0 {
        if available_shares == 0 {
            return Err(WatchdogError::NoMediaDirectories);
        }
        info!("No media files found across healthy shares; ending pass");
        tui_log(state, "INFO", "No media files found this pass");
        let top_reasons = db.get_top_failure_reasons(3);
        state.update(|s| {
            s.top_failure_reasons = top_reasons
                .iter()
                .map(|(reason, count)| (reason.clone(), *count as u64))
                .collect();
        });
        state.set_phase(PipelinePhase::Idle);
        state.set_last_pass_time();
        state.set_current_file(None);
        state.reset_file_progress();
        emit_event(
            config,
            base_dir,
            "pass_end",
            json!({
                "result": "ok",
                "discovered_files": discovered_count,
                "inspected_files": stats.files_inspected,
                "queued_files": stats.files_queued,
                "transcoded_files": stats.files_transcoded,
                "failed_files": stats.transcode_failures,
                "space_saved_bytes": stats.space_saved_bytes,
            }),
        );
        return Ok(stats);
    }

    if duplicate_count > 0 {
        warn!(
            "Skipped {} duplicate discovered path(s) during scan",
            duplicate_count
        );
        tui_log(
            state,
            "WARN",
            &format!("Skipped {} duplicate discovered path(s)", duplicate_count),
        );
    }

    if let Some(heap) = capped_queue.take() {
        transcode_queue = heap
            .into_iter()
            .map(|Reverse(candidate)| (candidate.entry, candidate.eval))
            .collect();
    }

    if !dry_run {
        db.mark_inspected_batch(&pending_inspected);
    }

    // Sort queue: largest files first
    transcode_queue.sort_by(|a, b| b.0.size.cmp(&a.0.size));
    stats.files_queued = transcode_queue.len() as u64;

    if queue_cap.is_some() && total_transcode_candidates > transcode_queue.len() as u64 {
        tui_log(
            state,
            "INFO",
            &format!(
                "Queue capped at {} files this pass (from {})",
                config.scan.max_files_per_pass, total_transcode_candidates
            ),
        );
    }

    info!(
        "Discovered {} candidate files before filtering",
        discovered_count
    );
    info!("Queue length: {}", transcode_queue.len());
    state.set_queue_info(0, transcode_queue.len() as u32);
    tui_log(
        state,
        "INFO",
        &format!(
            "Scan complete: {} files discovered, {} inspected, {} queued for transcode",
            discovered_count,
            stats.files_inspected,
            transcode_queue.len()
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

    // Per-file processing loop (import -> transcode -> export)

    let mut attempt_counts: std::collections::HashMap<PathBuf, u32> =
        std::collections::HashMap::new();
    let initial_queue_length = transcode_queue.len();
    let mut processed = 0u32;
    let mut idx = 0;

    while idx < transcode_queue.len() {
        // Check shutdown
        if shutdown_requested(&mut shutdown_rx) {
            info!("Shutdown requested, stopping pipeline");
            return Err(WatchdogError::Shutdown);
        }
        if is_pause_requested(config, base_dir) {
            info!("Pause file present, stopping pipeline before next item");
            state.set_phase(PipelinePhase::Paused);
            return Err(WatchdogError::Paused);
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
            &format!(
                "Processing {}/{}: {}",
                processed, progress_total, display_filename
            ),
        );
        state.set_current_file(Some(path_str.clone()));
        state.set_queue_info(processed, progress_total as u32);
        state.reset_file_progress();

        let transcode_start = Instant::now();
        let original_size = deps.fs.file_size(path).unwrap_or(entry.size) as i64;

        // Record in DB
        let db_row_id = match db.record_transcode_start(
            &path_str,
            share_name,
            eval.video_codec.as_deref(),
            eval.bitrate_bps as i64,
            original_size,
        ) {
            Ok(id) => id,
            Err(e) => {
                error!(
                    "[{}] Failed to record transcode start for {}: {}",
                    share_name, path_str, e
                );
                tui_log(
                    state,
                    "ERROR",
                    "Database write failed (transcode start); aborting pass",
                );
                return Err(WatchdogError::Database(e));
            }
        };

        if check_file_in_use_best_effort(
            config,
            base_dir,
            deps,
            state,
            path,
            &path_str,
            &display_filename,
            "pre_transfer",
            db.as_ref(),
            db_row_id,
            transcode_start.elapsed().as_secs_f64(),
        ) {
            continue;
        }

        // Check disk space
        let required = (original_size as f64 * config.transcode.min_free_space_multiplier) as u64;
        match deps.fs.free_space(&temp_dir) {
            Ok(free) if free < required => {
                warn!(
                    "[{}] Skipping {}: insufficient temp space (need {}, have {})",
                    share_name,
                    path_str,
                    format_bytes(required),
                    format_bytes(free)
                );
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "WARN",
                    &format!("Skipped {}: insufficient disk space", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::InsufficientTempSpace,
                    "insufficient_temp_space",
                    true,
                );
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    "[{}] Skipping {}: failed to determine temp free space ({}), failing closed",
                    share_name, path_str, e
                );
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "WARN",
                    &format!("Skipped {}: temp-space probe failed", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::TempSpaceProbeError,
                    "temp_space_probe_error",
                    true,
                );
                continue;
            }
        }

        // Build collision-resistant temp paths using a deterministic hash of full source path.
        let (local_source, local_output) = build_local_temp_paths(&temp_dir, share_name, path);

        // Step 1: rsync source to temp
        state.set_phase(PipelinePhase::Transferring);
        tui_log(
            state,
            "INFO",
            &format!("Rsync source -> temp: {}", display_filename),
        );
        state.set_progress_stage(ProgressStage::Import);
        let rsync_timeout = if original_size > 0 {
            (original_size as u64 / (1024 * 1024)).max(300)
        } else {
            3600
        };

        let transfer_result = tokio::task::block_in_place(|| {
            let mut result: Option<Result<TransferResult>> = None;
            let (transfer_tx, mut transfer_rx) = mpsc::channel::<TransferProgress>(32);
            std::thread::scope(|s| {
                s.spawn(|| {
                    while let Some(p) = transfer_rx.blocking_recv() {
                        state.set_import_progress(p.percent, p.rate_mib_per_sec, p.eta);
                    }
                });

                result = Some(deps.transfer.transfer(
                    path,
                    &local_source,
                    rsync_timeout,
                    TransferStage::Import,
                    Some(transfer_tx),
                ));
            });
            result.unwrap()
        });

        match transfer_result {
            Ok(r) if r.success => {
                state.set_import_progress(100.0, 0.0, String::new());
                tui_log(
                    state,
                    "INFO",
                    &format!("Rsync complete: {}", display_filename),
                );
            }
            Ok(_) => {
                error!("[{}] Failed to rsync source: {}", share_name, path_str);
                let dur = transcode_start.elapsed().as_secs_f64();
                if attempt_num <= config.transcode.max_retries {
                    tui_log(
                        state,
                        "WARN",
                        &format!("Transfer failed, requeuing: {}", display_filename),
                    );
                    transcode_queue.push((entry.clone(), eval.clone()));
                    db.record_transcode_end_with_code(
                        db_row_id,
                        TranscodeOutcome::Failed,
                        0,
                        0,
                        dur,
                        Some("rsync failed (requeued)"),
                        Some(FailureCode::TransferFailedRequeued.as_str()),
                    );
                } else {
                    stats.transcode_failures += 1;
                    state.update(|s| s.run_failures += 1);
                    tui_log(
                        state,
                        "ERROR",
                        &format!("Transfer failed (retries exhausted): {}", display_filename),
                    );
                    record_failure(
                        state,
                        db.as_ref(),
                        config,
                        base_dir,
                        &path_str,
                        db_row_id,
                        dur,
                        FailureCode::TransferFailed,
                        "transfer_failed",
                        true,
                    );
                }
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(e) => {
                error!("[{}] rsync error: {}", share_name, e);
                let dur = transcode_start.elapsed().as_secs_f64();
                if attempt_num <= config.transcode.max_retries {
                    tui_log(
                        state,
                        "WARN",
                        &format!("Transfer error, requeuing: {}", display_filename),
                    );
                    transcode_queue.push((entry.clone(), eval.clone()));
                    db.record_transcode_end_with_code(
                        db_row_id,
                        TranscodeOutcome::Failed,
                        0,
                        0,
                        dur,
                        Some("rsync error (requeued)"),
                        Some(FailureCode::TransferErrorRequeued.as_str()),
                    );
                } else {
                    stats.transcode_failures += 1;
                    state.update(|s| s.run_failures += 1);
                    tui_log(
                        state,
                        "ERROR",
                        &format!("Transfer error (retries exhausted): {}", display_filename),
                    );
                    record_failure(
                        state,
                        db.as_ref(),
                        config,
                        base_dir,
                        &path_str,
                        db_row_id,
                        dur,
                        FailureCode::TransferError,
                        "transfer_error",
                        true,
                    );
                }
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
        }

        // Step 2: Transcode
        state.set_phase(PipelinePhase::Transcoding);
        tui_log(
            state,
            "INFO",
            &format!("Starting HandBrake encode: {}", display_filename),
        );
        state.set_progress_stage(ProgressStage::Transcode);
        let (progress_tx, mut progress_rx) = mpsc::channel::<TranscodeProgress>(32);
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let cancel_reason = Arc::new(AtomicU8::new(CANCEL_NONE));
        let monitor_cancel_flag = Arc::clone(&cancel_flag);
        let monitor_cancel_reason = Arc::clone(&cancel_reason);
        let monitor_pause_path = pause_path.clone();
        let mut monitor_shutdown_rx = shutdown_rx.resubscribe();
        let monitor_task = tokio::spawn(async move {
            loop {
                if monitor_cancel_flag.load(Ordering::Relaxed) {
                    break;
                }
                if monitor_pause_path.exists() {
                    let _ = monitor_cancel_reason.compare_exchange(
                        CANCEL_NONE,
                        CANCEL_PAUSE,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                    monitor_cancel_flag.store(true, Ordering::Relaxed);
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(250)) => {}
                    _ = monitor_shutdown_rx.recv() => {
                        let _ = monitor_cancel_reason.compare_exchange(
                            CANCEL_NONE,
                            CANCEL_SHUTDOWN,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                        monitor_cancel_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        });

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
                    config.transcode.stall_timeout_seconds,
                    progress_tx,
                    Arc::clone(&cancel_flag),
                ));
            });
            result.unwrap()
        });
        cancel_flag.store(true, Ordering::Relaxed);
        let _ = monitor_task.await;

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
                    tui_log(
                        state,
                        "WARN",
                        &format!(
                            "Timeout, requeuing: {} (attempt {})",
                            display_filename, attempt_num
                        ),
                    );
                    transcode_queue.push((
                        entry.clone(),
                        TranscodeEval {
                            needs_transcode: true,
                            reasons: vec!["retry after timeout".to_string()],
                            bitrate_mbps: eval.bitrate_mbps,
                            video_codec: eval.video_codec.clone(),
                            bitrate_bps: eval.bitrate_bps,
                        },
                    ));
                    let dur = transcode_start.elapsed().as_secs_f64();
                    db.record_transcode_end_with_code(
                        db_row_id,
                        TranscodeOutcome::Failed,
                        0,
                        0,
                        dur,
                        Some("timeout (requeued)"),
                        Some(FailureCode::TimeoutRequeued.as_str()),
                    );
                } else {
                    error!(
                        "[{}] Timeout retries exhausted for {}",
                        share_name, path_str
                    );
                    stats.transcode_failures += 1;
                    state.update(|s| s.run_failures += 1);
                    tui_log(
                        state,
                        "ERROR",
                        &format!("Timeout retries exhausted: {}", display_filename),
                    );
                    let dur = transcode_start.elapsed().as_secs_f64();
                    record_failure(
                        state,
                        db.as_ref(),
                        config,
                        base_dir,
                        &path_str,
                        db_row_id,
                        dur,
                        FailureCode::TimeoutExhausted,
                        "timeout_exhausted",
                        true,
                    );
                }
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(WatchdogError::TranscodeStalled {
                stall_timeout_secs, ..
            }) => {
                warn!(
                    "[{}] Transcode stalled after {}s with no progress: {}",
                    share_name, stall_timeout_secs, path_str
                );

                if attempt_num <= config.transcode.max_retries {
                    info!(
                        "[{}] Requeueing after stall (attempt {}/{}): {}",
                        share_name,
                        attempt_num,
                        config.transcode.max_retries + 1,
                        path_str
                    );
                    tui_log(
                        state,
                        "WARN",
                        &format!(
                            "Stalled, requeuing: {} (attempt {})",
                            display_filename, attempt_num
                        ),
                    );
                    transcode_queue.push((
                        entry.clone(),
                        TranscodeEval {
                            needs_transcode: true,
                            reasons: vec!["retry after stall".to_string()],
                            bitrate_mbps: eval.bitrate_mbps,
                            video_codec: eval.video_codec.clone(),
                            bitrate_bps: eval.bitrate_bps,
                        },
                    ));
                    let dur = transcode_start.elapsed().as_secs_f64();
                    db.record_transcode_end_with_code(
                        db_row_id,
                        TranscodeOutcome::Failed,
                        0,
                        0,
                        dur,
                        Some("stalled (requeued)"),
                        Some(FailureCode::StalledRequeued.as_str()),
                    );
                } else {
                    error!("[{}] Stall retries exhausted for {}", share_name, path_str);
                    stats.transcode_failures += 1;
                    state.update(|s| s.run_failures += 1);
                    tui_log(
                        state,
                        "ERROR",
                        &format!("Stall retries exhausted: {}", display_filename),
                    );
                    let dur = transcode_start.elapsed().as_secs_f64();
                    record_failure(
                        state,
                        db.as_ref(),
                        config,
                        base_dir,
                        &path_str,
                        db_row_id,
                        dur,
                        FailureCode::StalledExhausted,
                        "stalled_exhausted",
                        true,
                    );
                }
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(WatchdogError::TranscodeCancelled { .. }) => {
                let dur = transcode_start.elapsed().as_secs_f64();
                let reason = match cancel_reason.load(Ordering::Relaxed) {
                    CANCEL_SHUTDOWN => "interrupted_by_shutdown",
                    CANCEL_PAUSE => "interrupted_by_pause",
                    _ => "interrupted",
                };
                // Operator-driven interruptions are recorded in history but should not
                // increment per-file cooldown counters.
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    match cancel_reason.load(Ordering::Relaxed) {
                        CANCEL_SHUTDOWN => FailureCode::InterruptedShutdown,
                        CANCEL_PAUSE => FailureCode::InterruptedPause,
                        _ => FailureCode::Interrupted,
                    },
                    reason,
                    false,
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                if cancel_reason.load(Ordering::Relaxed) == CANCEL_PAUSE {
                    state.set_phase(PipelinePhase::Paused);
                    return Err(WatchdogError::Paused);
                }
                return Err(WatchdogError::Shutdown);
            }
            Err(e) => {
                error!("[{}] Transcode error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Transcode error: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::TranscodeError,
                    "transcode_error",
                    true,
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Ok(ref r) if !r.success || !r.output_exists => {
                error!("[{}] Transcode failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Transcode failed: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::TranscodeFailed,
                    "transcode_failed",
                    true,
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Ok(_) => {
                state.update(|s| {
                    s.progress_stage = ProgressStage::Transcode;
                    s.transcode_percent = s.transcode_percent.max(100.0);
                });
            } // Success, continue to verification
        }

        // Step 3: Pre-verify output file exists and is non-zero
        if !deps.fs.exists(&local_output) {
            error!(
                "[{}] Transcoded output file does not exist: {}",
                share_name,
                local_output.display()
            );
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            tui_log(
                state,
                "ERROR",
                &format!("Output missing after transcode: {}", display_filename),
            );
            let dur = transcode_start.elapsed().as_secs_f64();
            record_failure(
                state,
                db.as_ref(),
                config,
                base_dir,
                &path_str,
                db_row_id,
                dur,
                FailureCode::OutputMissing,
                "output_missing",
                true,
            );
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }
        let output_file_size = deps.fs.file_size(&local_output).unwrap_or(0);
        if output_file_size == 0 {
            error!(
                "[{}] Transcoded output file is 0 bytes: {}",
                share_name,
                local_output.display()
            );
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            tui_log(
                state,
                "ERROR",
                &format!("Output is 0 bytes: {}", display_filename),
            );
            let dur = transcode_start.elapsed().as_secs_f64();
            record_failure(
                state,
                db.as_ref(),
                config,
                base_dir,
                &path_str,
                db_row_id,
                dur,
                FailureCode::OutputZeroBytes,
                "output_zero_bytes",
                true,
            );
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 3b: Verify transcode metadata (duration, streams, health)
        let verified = tokio::task::block_in_place(|| {
            verify_transcode(deps.prober.as_ref(), &local_source, &local_output)
        });

        match verified {
            Ok(true) => {
                tui_log(
                    state,
                    "INFO",
                    &format!("Verification passed: {}", display_filename),
                );
            }
            Ok(false) => {
                error!("[{}] Verification failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Verification failed: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::VerificationFailed,
                    "verification_failed",
                    true,
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Err(e) => {
                error!("[{}] Verification error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Verification error: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::VerificationError,
                    "verification_error",
                    true,
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
            tui_log(
                state,
                "ERROR",
                &format!("Output too small, rejecting: {}", display_filename),
            );
            let dur = transcode_start.elapsed().as_secs_f64();
            record_failure(
                state,
                db.as_ref(),
                config,
                base_dir,
                &path_str,
                db_row_id,
                dur,
                FailureCode::OutputSuspiciouslySmall,
                "output_suspiciously_small",
                true,
            );
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
            tui_log(
                state,
                "INFO",
                &format!("Skipped (no space savings): {}", display_filename),
            );
            db.mark_inspected(&path_str, entry.size, entry.mtime);
            let dur = transcode_start.elapsed().as_secs_f64();
            db.record_transcode_end(
                db_row_id,
                TranscodeOutcome::SkippedNoSavings,
                new_size,
                0,
                dur,
                None,
            );
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 5: Verify NFS share has enough free space for the replacement file.
        // We need at least the output file size free on the target share.
        let share_path = path.parent().unwrap_or(path);
        match deps.fs.free_space(share_path) {
            Ok(share_free) if share_free < new_size as u64 => {
                warn!(
                    "[{}] Insufficient space on NFS share for replacement (need {}, have {}): {}",
                    share_name,
                    format_bytes(new_size as u64),
                    format_bytes(share_free),
                    path_str
                );
                tui_log(
                    state,
                    "WARN",
                    &format!("Insufficient NFS space, skipping: {}", display_filename),
                );
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::InsufficientNfsSpace,
                    "insufficient_nfs_space",
                    true,
                );
                cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    "[{}] Skipping {}: failed to determine NFS free space ({}), failing closed",
                    share_name, path_str, e
                );
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "WARN",
                    &format!("Skipped {}: NFS-space probe failed", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::NfsSpaceProbeError,
                    "nfs_space_probe_error",
                    true,
                );
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
            tui_log(
                state,
                "WARN",
                &format!(
                    "File changed during transcode, skipping replace: {}",
                    display_filename
                ),
            );
            stats.transcode_failures += 1;
            state.update(|s| s.run_failures += 1);
            let dur = transcode_start.elapsed().as_secs_f64();
            record_failure(
                state,
                db.as_ref(),
                config,
                base_dir,
                &path_str,
                db_row_id,
                dur,
                FailureCode::SourceChangedDuringTranscode,
                "source_changed_during_transcode",
                true,
            );
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        // Step 7: Safe replace
        state.set_phase(PipelinePhase::Transferring);
        if check_file_in_use_best_effort(
            config,
            base_dir,
            deps,
            state,
            path,
            &path_str,
            &display_filename,
            "pre_replace",
            db.as_ref(),
            db_row_id,
            transcode_start.elapsed().as_secs_f64(),
        ) {
            cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
            continue;
        }

        state.set_progress_stage(ProgressStage::Export);
        let replaced = tokio::task::block_in_place(|| {
            let mut result: Option<Result<bool>> = None;
            let (transfer_tx, mut transfer_rx) = mpsc::channel::<TransferProgress>(32);
            std::thread::scope(|s| {
                s.spawn(|| {
                    while let Some(p) = transfer_rx.blocking_recv() {
                        state.set_export_progress(p.percent, p.rate_mib_per_sec, p.eta);
                    }
                });
                result = Some(safe_replace(
                    deps.fs.as_ref(),
                    deps.transfer.as_ref(),
                    path,
                    &local_output,
                    Some(transfer_tx),
                ));
            });
            result.unwrap()
        });

        match replaced {
            Ok(true) => {
                state.set_export_progress(100.0, 0.0, String::new());
                let space_saved = original_size - new_size;
                stats.files_transcoded += 1;
                stats.space_saved_bytes += space_saved;

                // Update DB
                if let Ok(new_mtime) = deps.fs.file_mtime(path) {
                    let new_file_size = deps.fs.file_size(path).unwrap_or(0);
                    db.mark_inspected(&path_str, new_file_size, new_mtime);
                }
                let dur = transcode_start.elapsed().as_secs_f64();
                db.record_transcode_end(
                    db_row_id,
                    TranscodeOutcome::Replaced,
                    new_size,
                    space_saved,
                    dur,
                    None,
                );
                db.clear_file_failure_state(&path_str);

                // Log space saved to timeseries
                let cumulative = db.get_total_space_saved() + space_saved;
                db.log_space_saved(cumulative);

                info!(
                    "[{}] SUCCESS: Replaced {} | Saved {}",
                    share_name,
                    path_str,
                    format_bytes_signed(space_saved)
                );
                tui_log(
                    state,
                    "SUCCESS",
                    &format!(
                        "Replaced {} | Saved {}",
                        display_filename,
                        format_bytes_signed(space_saved)
                    ),
                );

                // Update state
                state.update(|s| {
                    s.total_transcoded += 1;
                    s.total_space_saved += space_saved;
                    s.run_transcoded += 1;
                    s.run_space_saved += space_saved;
                });

                send_webhook(
                    &config.notify,
                    NotifyEvent::ReplacementSummary,
                    &json!({
                        "path": path_str,
                        "share": share_name,
                        "original_size": original_size,
                        "new_size": new_size,
                        "space_saved": space_saved,
                    }),
                );
            }
            Ok(false) => {
                error!("[{}] Safe replace failed for {}", share_name, path_str);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Replace failed: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::SafeReplaceFailed,
                    "safe_replace_failed",
                    true,
                );
            }
            Err(e) => {
                error!("[{}] Replace error: {}", share_name, e);
                stats.transcode_failures += 1;
                state.update(|s| s.run_failures += 1);
                tui_log(
                    state,
                    "ERROR",
                    &format!("Replace error: {}", display_filename),
                );
                let dur = transcode_start.elapsed().as_secs_f64();
                record_failure(
                    state,
                    db.as_ref(),
                    config,
                    base_dir,
                    &path_str,
                    db_row_id,
                    dur,
                    FailureCode::SafeReplaceError,
                    "safe_replace_error",
                    true,
                );
            }
        }

        cleanup_temp_files(deps.fs.as_ref(), &[&local_source, &local_output]);
    }

    state.set_phase(PipelinePhase::Idle);
    let top_reasons = db.get_top_failure_reasons(3);
    state.update(|s| {
        s.top_failure_reasons = top_reasons
            .iter()
            .map(|(reason, count)| (reason.clone(), *count as u64))
            .collect();
    });
    state.set_last_pass_time();
    state.set_current_file(None);
    state.reset_file_progress();
    tui_log(
        state,
        "INFO",
        &format!(
            "Pass complete: {} transcoded, {} failures, {} saved",
            stats.files_transcoded,
            stats.transcode_failures,
            format_bytes_signed(stats.space_saved_bytes)
        ),
    );
    emit_event(
        config,
        base_dir,
        "pass_end",
        json!({
            "result": "ok",
            "discovered_files": discovered_count,
            "inspected_files": stats.files_inspected,
            "queued_files": stats.files_queued,
            "transcoded_files": stats.files_transcoded,
            "failed_files": stats.transcode_failures,
            "space_saved_bytes": stats.space_saved_bytes,
        }),
    );

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn process_probe_batch(
    batch: &mut Vec<FileEntry>,
    prober: &dyn Prober,
    config: &Config,
    state: &StateManager,
    shutdown_rx: &mut broadcast::Receiver<()>,
    worker_count: usize,
    stats: &mut RunStats,
    transcode_queue: &mut Vec<(FileEntry, TranscodeEval)>,
    capped_queue: &mut Option<BinaryHeap<Reverse<QueueCandidate>>>,
    queue_seq: &mut u64,
    total_transcode_candidates: &mut u64,
    pending_inspected: &mut Vec<(String, u64, f64)>,
    queue_cap: Option<usize>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let probe_results = probe_candidates_parallel(prober, batch, worker_count, shutdown_rx, state)?;
    let entries = std::mem::take(batch);
    for (entry, probe_result) in entries.into_iter().zip(probe_results.into_iter()) {
        let path_str = entry.path.to_string_lossy().to_string();
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
                tui_log(
                    state,
                    "WARN",
                    &format!("Probe failed, skipping: {}", path_str),
                );
            }
            Some(ref probe) => {
                let eval = evaluate_transcode_need(probe, &config.transcode);
                if eval.needs_transcode {
                    info!("QUEUE: {} (reasons: {})", path_str, eval.reasons.join(", "));
                    *total_transcode_candidates += 1;
                    if let Some(cap) = queue_cap {
                        let candidate = QueueCandidate {
                            size: entry.size,
                            seq: *queue_seq,
                            entry,
                            eval,
                        };
                        *queue_seq = queue_seq.saturating_add(1);
                        let heap = capped_queue
                            .as_mut()
                            .expect("capped queue should exist when queue_cap is set");
                        if heap.len() < cap {
                            heap.push(Reverse(candidate));
                        } else if let Some(Reverse(min)) = heap.peek() {
                            if candidate.size > min.size {
                                let _ = heap.pop();
                                heap.push(Reverse(candidate));
                            }
                        }
                    } else {
                        transcode_queue.push((entry, eval));
                    }
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
                    pending_inspected.push((path_str, entry.size, entry.mtime));
                }
            }
        }
    }
    Ok(())
}

/// Walk healthy shares concurrently and return per-share entries.
fn scan_shares_parallel(
    fs: Arc<dyn FileSystem>,
    jobs: &[(String, PathBuf)],
    extensions: &[String],
    timeout: Duration,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<Vec<(String, Vec<FileEntry>)>> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let extensions = extensions.to_vec();
    let mut per_share: Vec<(String, Result<Vec<FileEntry>>)> = Vec::new();
    let cancel_scan = Arc::new(AtomicBool::new(false));
    let pending = Arc::new(std::sync::Mutex::new(
        jobs.iter()
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>(),
    ));
    let (tx, rx) = std::sync::mpsc::channel::<(String, Result<Vec<FileEntry>>)>();
    let mut handles = Vec::with_capacity(jobs.len());
    for (share_name, mount_path) in jobs {
        let tx = tx.clone();
        let share_name = share_name.clone();
        let mount_path = mount_path.clone();
        let extensions = extensions.clone();
        let fs = Arc::clone(&fs);
        let pending = Arc::clone(&pending);
        let cancel_scan = Arc::clone(&cancel_scan);
        handles.push(std::thread::spawn(move || {
            let result = fs.walk_share_cancellable(
                &share_name,
                &mount_path,
                &extensions,
                Some(cancel_scan.as_ref()),
            );
            if let Ok(mut pending) = pending.lock() {
                pending.remove(&share_name);
            }
            let _ = tx.send((share_name, result));
        }));
    }
    drop(tx);

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let mut shutdown = false;
    while per_share.len() < jobs.len() {
        if shutdown_requested(shutdown_rx) {
            shutdown = true;
            cancel_scan.store(true, Ordering::Relaxed);
            break;
        }

        let now = Instant::now();
        if now >= deadline {
            timed_out = true;
            cancel_scan.store(true, Ordering::Relaxed);
            break;
        }

        let wait = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(200));
        match rx.recv_timeout(wait) {
            Ok(received) => per_share.push(received),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    cancel_scan.store(true, Ordering::Relaxed);
    while per_share.len() < jobs.len() {
        match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(received) => per_share.push(received),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    for handle in handles {
        let _ = handle.join();
    }

    if shutdown {
        return Err(WatchdogError::Shutdown);
    }

    if timed_out {
        let (pending_count, pending_list) = pending
            .lock()
            .map(|s| (s.len(), s.iter().cloned().collect::<Vec<_>>()))
            .unwrap_or_default();
        error!(
            "Share scan timed out after {}s; pending shares: {}",
            timeout.as_secs().max(1),
            if pending_list.is_empty() {
                "unknown".to_string()
            } else {
                pending_list.join(", ")
            }
        );
        return Err(WatchdogError::ScanTimeout {
            timeout_secs: timeout.as_secs().max(1),
            pending_shares: pending_count.max(1),
        });
    }

    if per_share.len() < jobs.len() {
        let pending_count = jobs.len().saturating_sub(per_share.len()).max(1);
        error!(
            "Share scan worker ended early; treating as timeout with {} pending share(s)",
            pending_count
        );
        return Err(WatchdogError::ScanTimeout {
            timeout_secs: timeout.as_secs().max(1),
            pending_shares: pending_count,
        });
    }

    let mut out = Vec::with_capacity(per_share.len());
    for (share_name, result) in per_share {
        match result {
            Ok(entries) => out.push((share_name, entries)),
            Err(e) => {
                error!("Share scan failed for '{}': {}", share_name, e);
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Probe files in parallel using a bounded worker pool.
fn probe_candidates_parallel(
    prober: &dyn Prober,
    candidates: &[FileEntry],
    worker_count: usize,
    shutdown_rx: &mut broadcast::Receiver<()>,
    state: &StateManager,
) -> Result<Vec<Option<ProbeResult>>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut aborted = false;
    let mut ordered_results = tokio::task::block_in_place(|| {
        let next_index = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel::<(usize, Result<Option<ProbeResult>>)>();
        let mut completed = 0usize;
        let mut last_progress_log = Instant::now();
        let mut results: Vec<Option<Result<Option<ProbeResult>>>> = std::iter::repeat_with(|| None)
            .take(candidates.len())
            .collect();

        std::thread::scope(|scope| {
            for _ in 0..worker_count.min(candidates.len()) {
                let tx = tx.clone();
                let next_index = Arc::clone(&next_index);
                let cancelled = Arc::clone(&cancelled);
                scope.spawn(move || loop {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = next_index.fetch_add(1, Ordering::Relaxed);
                    if idx >= candidates.len() {
                        break;
                    }
                    let result = prober.probe(&candidates[idx].path);
                    let _ = tx.send((idx, result));
                });
            }
            drop(tx);

            while completed < candidates.len() {
                if shutdown_requested(shutdown_rx) {
                    cancelled.store(true, Ordering::Relaxed);
                    aborted = true;
                }

                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok((idx, result)) => {
                        if idx < results.len() && results[idx].is_none() {
                            results[idx] = Some(result);
                            completed += 1;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }

                state.set_queue_info(completed as u32, candidates.len() as u32);
                if completed == candidates.len() || last_progress_log.elapsed().as_secs() >= 2 {
                    tui_log(
                        state,
                        "INFO",
                        &format!("Probing progress: {}/{} files", completed, candidates.len()),
                    );
                    last_progress_log = Instant::now();
                }
            }
        });
        results
    });

    if aborted {
        return Err(WatchdogError::Shutdown);
    }

    let mut final_results = Vec::with_capacity(ordered_results.len());
    for (idx, item) in ordered_results.drain(..).enumerate() {
        let probe_result = item.unwrap_or_else(|| {
            Err(WatchdogError::Probe {
                path: candidates[idx].path.clone(),
                reason: "probe worker exited without returning a result".to_string(),
            })
        })?;
        final_results.push(probe_result);
    }

    Ok(final_results)
}

fn shutdown_requested(shutdown_rx: &mut broadcast::Receiver<()>) -> bool {
    matches!(
        shutdown_rx.try_recv(),
        Ok(_)
            | Err(broadcast::error::TryRecvError::Closed)
            | Err(broadcast::error::TryRecvError::Lagged(_))
    )
}

/// Scan NFS shares for orphaned watchdog backup files from interrupted safe_replace operations.
/// If the original file is missing (crash between rename steps), restore from .watchdog.old.
/// If both the original and backup exist (cleanup failed), remove the stale backup.
/// Returns false if shutdown is requested during recovery.
fn recover_orphaned_files(
    fs: &dyn FileSystem,
    config: &Config,
    base_dir: &Path,
    state: &StateManager,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> bool {
    for share in &config.shares {
        if shutdown_requested(shutdown_rx) {
            return false;
        }
        let mount_path = Path::new(&share.local_mount);
        if !fs.is_dir(mount_path) {
            continue;
        }

        // Only recover watchdog-owned artifacts.
        let old_files = match fs.walk_files_with_suffix(mount_path, WATCHDOG_OLD_SUFFIX) {
            Ok(files) => files,
            Err(e) => {
                warn!(
                    "[{}] Could not scan for watchdog recovery artifacts: {}",
                    share.name, e
                );
                continue;
            }
        };

        for path in old_files {
            if shutdown_requested(shutdown_rx) {
                return false;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let original_name = name.trim_end_matches(WATCHDOG_OLD_SUFFIX);
            let original_path = path.parent().unwrap_or(Path::new(".")).join(original_name);

            if fs.exists(&original_path) {
                warn!(
                    "[{}] Removing stale watchdog backup (original exists): {}",
                    share.name,
                    path.display()
                );
                let _ = fs.remove(&path);
                emit_event(
                    config,
                    base_dir,
                    "orphan_recovery",
                    json!({
                        "share": share.name,
                        "action": "remove_stale_backup",
                        "path": path,
                    }),
                );
            } else {
                error!(
                    "[{}] RECOVERING orphaned file: {} -> {}",
                    share.name,
                    path.display(),
                    original_path.display()
                );
                match fs.rename(&path, &original_path) {
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
                        emit_event(
                            config,
                            base_dir,
                            "orphan_recovery",
                            json!({
                                "share": share.name,
                                "action": "restore_orphaned_original",
                                "from": path,
                                "to": original_path,
                            }),
                        );
                    }
                    Err(e) => {
                        error!(
                            "[{}] FAILED to recover orphaned file {} -> {}: {}",
                            share.name,
                            path.display(),
                            original_path.display(),
                            e
                        );
                        tui_log(
                            state,
                            "ERROR",
                            &format!(
                                "FAILED to recover orphaned file: {} (manual intervention needed)",
                                name
                            ),
                        );
                    }
                }
            }

            let tmp_name = format!("{}{}", original_name, WATCHDOG_TMP_SUFFIX);
            let tmp_path = path.parent().unwrap_or(Path::new(".")).join(tmp_name);
            if fs.exists(&tmp_path) {
                warn!(
                    "[{}] Removing stale watchdog temp artifact: {}",
                    share.name,
                    tmp_path.display()
                );
                let _ = fs.remove(&tmp_path);
                emit_event(
                    config,
                    base_dir,
                    "orphan_recovery",
                    json!({
                        "share": share.name,
                        "action": "remove_stale_temp",
                        "path": tmp_path,
                    }),
                );
            }
        }

        // Legacy .old files are reported only, never auto-mutated.
        if let Ok(legacy_old_files) = fs.walk_files_with_suffix(mount_path, ".old") {
            for legacy in legacy_old_files {
                if shutdown_requested(shutdown_rx) {
                    return false;
                }
                if legacy
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(WATCHDOG_OLD_SUFFIX))
                {
                    continue;
                }
                warn!(
                    "[{}] Legacy .old file detected (left untouched): {}",
                    share.name,
                    legacy.display()
                );
            }
        }
    }
    true
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

#[allow(clippy::too_many_arguments)]
fn record_failure(
    state: &StateManager,
    db: &WatchdogDb,
    config: &Config,
    base_dir: &Path,
    path: &str,
    row_id: i64,
    duration_seconds: f64,
    code: FailureCode,
    reason: &str,
    apply_cooldown: bool,
) {
    let failure_code = code.as_str();
    let now_ts = chrono::Utc::now().timestamp();
    let summary = format!(
        "Recording file failure: code={} path={} reason={} duration={:.1}s cooldown_tracking={}",
        failure_code, path, reason, duration_seconds, apply_cooldown
    );
    match code {
        FailureCode::InterruptedShutdown
        | FailureCode::InterruptedPause
        | FailureCode::Interrupted => {
            info!("{}", summary);
        }
        _ => warn!("{}", summary),
    }

    state.update(|s| {
        s.last_failure_code = Some(failure_code.to_string());
    });
    db.record_transcode_end_with_code(
        row_id,
        TranscodeOutcome::Failed,
        0,
        0,
        duration_seconds,
        Some(reason),
        Some(failure_code),
    );
    if apply_cooldown {
        if let Some(cooldown) = db.record_file_failure(
            path,
            reason,
            failure_code,
            config.safety.max_failures_before_cooldown,
            config.safety.cooldown_base_seconds,
            config.safety.cooldown_max_seconds,
        ) {
            let should_quarantine = config
                .safety
                .quarantine_failure_codes
                .iter()
                .any(|c| c == failure_code)
                && cooldown.consecutive_failures >= config.safety.quarantine_after_failures.max(1);
            let retry_in_seconds = cooldown.next_eligible_at.saturating_sub(now_ts).max(0);
            warn!(
                "Failure cooldown state: code={} path={} consecutive_failures={} retry_in={}s quarantine_candidate={}",
                failure_code,
                path,
                cooldown.consecutive_failures,
                retry_in_seconds,
                should_quarantine
            );
            if should_quarantine {
                let was_quarantined = db.is_quarantined(path);
                db.quarantine_file(path, failure_code, reason);
                if !was_quarantined {
                    tui_log(
                        state,
                        "ERROR",
                        &format!("File quarantined after repeated failures: {}", path),
                    );
                    emit_event(
                        config,
                        base_dir,
                        "quarantine_add",
                        json!({
                            "path": path,
                            "failure_code": failure_code,
                            "failure_reason": reason,
                            "consecutive_failures": cooldown.consecutive_failures,
                        }),
                    );
                }
                let quarantined_count = db.quarantine_count().max(0) as u64;
                state.update(|s| {
                    s.quarantined_files = quarantined_count;
                });
            }
            if cooldown.next_eligible_at > chrono::Utc::now().timestamp() {
                send_webhook(
                    &config.notify,
                    NotifyEvent::CooldownAlert,
                    &json!({
                        "path": path,
                        "failure_reason": reason,
                        "failure_code": failure_code,
                        "consecutive_failures": cooldown.consecutive_failures,
                        "next_eligible_at": cooldown.next_eligible_at,
                    }),
                );
            }
        } else {
            warn!(
                "Failed to persist cooldown state for {} (code={} reason={}); DB error should be logged separately",
                path, failure_code, reason
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn check_file_in_use_best_effort(
    config: &Config,
    base_dir: &Path,
    deps: &PipelineDeps,
    state: &StateManager,
    path: &Path,
    path_str: &str,
    display_filename: &str,
    stage: &str,
    db: &WatchdogDb,
    row_id: i64,
    duration_seconds: f64,
) -> bool {
    if !config.safety.in_use_guard_enabled {
        return false;
    }

    match deps.in_use_detector.check_in_use(path) {
        Ok(InUseStatus::NotInUse) => false,
        Ok(InUseStatus::InUse) => {
            warn!(
                "Skipping {} at {}: file appears to be in use by another process",
                path.display(),
                stage
            );
            tui_log(
                state,
                "WARN",
                &format!("Skipped (in use): {} [{}]", display_filename, stage),
            );
            record_failure(
                state,
                db,
                config,
                base_dir,
                path_str,
                row_id,
                duration_seconds,
                FailureCode::FileInUse,
                "file_in_use",
                false,
            );
            increment_skip_counter(state, SkipReason::InUse);
            true
        }
        Err(e) => {
            warn!(
                "In-use guard probe failed for {} at {} (best-effort continue): {}",
                path.display(),
                stage,
                e
            );
            tui_log(
                state,
                "WARN",
                &format!("In-use probe failed (continuing): {}", display_filename),
            );
            false
        }
    }
}

fn increment_skip_counter(state: &StateManager, reason: SkipReason) {
    state.update(|s| match reason {
        SkipReason::Inspected => s.run_skipped_inspected += 1,
        SkipReason::Young => s.run_skipped_young += 1,
        SkipReason::Cooldown => s.run_skipped_cooldown += 1,
        SkipReason::Filtered => s.run_skipped_filtered += 1,
        SkipReason::InUse => s.run_skipped_in_use += 1,
        SkipReason::Quarantined => s.run_skipped_quarantined += 1,
    });
}

fn build_glob_matcher(globs: &[String]) -> Result<Option<GlobMatcher>> {
    if globs.is_empty() {
        return Ok(None);
    }

    let mut path_builder = GlobSetBuilder::new();
    let mut basename_builder = GlobSetBuilder::new();
    let mut has_path = false;
    let mut has_basename = false;

    for glob in globs {
        let parsed = Glob::new(glob).map_err(|e| WatchdogError::Config(e.to_string()))?;
        if glob.contains('/') || glob.contains('\\') {
            path_builder.add(parsed);
            has_path = true;
        } else {
            basename_builder.add(parsed);
            has_basename = true;
        }
    }

    let path_set = if has_path {
        Some(
            path_builder
                .build()
                .map_err(|e| WatchdogError::Config(e.to_string()))?,
        )
    } else {
        None
    };
    let basename_set = if has_basename {
        Some(
            basename_builder
                .build()
                .map_err(|e| WatchdogError::Config(e.to_string()))?,
        )
    } else {
        None
    };

    Ok(Some(GlobMatcher {
        path_set,
        basename_set,
    }))
}

fn matches_glob_matcher(path: &Path, matcher: &GlobMatcher) -> bool {
    if let Some(path_set) = matcher.path_set.as_ref() {
        if path_set.is_match(path) {
            return true;
        }
    }
    if let Some(name_set) = matcher.basename_set.as_ref() {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name_set.is_match(name) {
                return true;
            }
        }
    }
    false
}

fn path_matches_filters(
    path: &Path,
    include: Option<&GlobMatcher>,
    exclude: Option<&GlobMatcher>,
) -> bool {
    if let Some(exclude_set) = exclude {
        if matches_glob_matcher(path, exclude_set) {
            return false;
        }
    }
    if let Some(include_set) = include {
        matches_glob_matcher(path, include_set)
    } else {
        true
    }
}

fn is_pause_requested(config: &Config, base_dir: &Path) -> bool {
    let pause_path = config.resolve_path(base_dir, &config.safety.pause_file);
    pause_path.exists()
}

fn sync_service_state(state: &StateManager, db: &WatchdogDb) {
    let service = db.get_service_state();
    let quarantined = db.quarantine_count().max(0) as u64;
    state.update(|s| {
        s.consecutive_pass_failures = service.consecutive_pass_failures;
        s.auto_paused = service.auto_paused_at.is_some();
        s.auto_pause_reason = service.auto_pause_reason.clone();
        s.auto_paused_at = service.auto_paused_at.clone();
        s.quarantined_files = quarantined;
    });
}

fn emit_event(config: &Config, base_dir: &Path, event: &str, payload: serde_json::Value) {
    let path = resolve_event_journal_path(&config.paths.event_journal, base_dir);
    append_event(path.as_deref(), event, payload);
}

fn write_pause_marker(config: &Config, base_dir: &Path, reason: &str) -> std::io::Result<PathBuf> {
    let pause_path = config.resolve_path(base_dir, &config.safety.pause_file);
    if let Some(parent) = pause_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let marker = format!(
        "paused_at={}\nreason={}\nsource=auto_safety_trip\n",
        chrono::Utc::now().to_rfc3339(),
        reason
    );
    std::fs::write(&pause_path, marker.as_bytes())?;
    Ok(pause_path)
}

async fn wait_while_paused(
    config: &Config,
    base_dir: &Path,
    state: &StateManager,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> bool {
    loop {
        if !is_pause_requested(config, base_dir) {
            tui_log(state, "INFO", "Pause lifted, resuming pipeline");
            return true;
        }
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            _ = shutdown_rx.recv() => {
                info!("Pipeline shutting down while paused");
                return false;
            }
        }
    }
}

fn classify_pass_failure_code(err: &WatchdogError) -> &'static str {
    match err {
        WatchdogError::ScanTimeout { .. } => FailureCode::ScanTimeout.as_str(),
        WatchdogError::NfsMount { .. } => "nfs_mount_failed",
        WatchdogError::NoMediaDirectories => "no_media_directories",
        _ => "pass_error",
    }
}

/// Run the pipeline in a loop with configurable interval.
#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline_loop(
    config: Config,
    base_dir: PathBuf,
    deps: PipelineDeps,
    db: Arc<WatchdogDb>,
    state: StateManager,
    dry_run: bool,
    once: bool,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let mut retry_delay = 30u64;
    let recovery_interval =
        Duration::from_secs(config.safety.recovery_scan_interval_seconds.max(1));
    let mut last_recovery_run: Option<Instant> = None;
    sync_service_state(&state, db.as_ref());

    loop {
        if shutdown_requested(&mut shutdown_rx) {
            info!("Pipeline shutting down");
            emit_event(
                &config,
                &base_dir,
                "shutdown",
                json!({"phase": "loop_top_shutdown"}),
            );
            return Ok(());
        }

        if !dry_run
            && last_recovery_run
                .as_ref()
                .is_none_or(|last| last.elapsed() >= recovery_interval)
        {
            if !recover_orphaned_files(
                deps.fs.as_ref(),
                &config,
                &base_dir,
                &state,
                &mut shutdown_rx,
            ) {
                info!("Pipeline shutting down during recovery scan");
                emit_event(
                    &config,
                    &base_dir,
                    "shutdown",
                    json!({"phase": "recovery_scan"}),
                );
                return Ok(());
            }
            last_recovery_run = Some(Instant::now());
        }

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
                if !dry_run {
                    db.note_pass_success();
                    sync_service_state(&state, db.as_ref());
                }
                retry_delay = 30;
                if once {
                    return Ok(());
                }
            }
            Err(WatchdogError::Shutdown) => {
                info!("Pipeline shutting down");
                emit_event(
                    &config,
                    &base_dir,
                    "shutdown",
                    json!({"phase": "pass_shutdown"}),
                );
                return Ok(());
            }
            Err(WatchdogError::Paused) => {
                if dry_run || once {
                    return Err(WatchdogError::Paused);
                }
                state.set_phase(PipelinePhase::Paused);
                tui_log(&state, "WARN", "Pipeline paused");
                if !wait_while_paused(&config, &base_dir, &state, &mut shutdown_rx).await {
                    emit_event(
                        &config,
                        &base_dir,
                        "shutdown",
                        json!({"phase": "paused_wait"}),
                    );
                    return Ok(());
                }
                if db.get_service_state().auto_paused_at.is_some() {
                    db.clear_auto_paused();
                    db.note_pass_success();
                    sync_service_state(&state, db.as_ref());
                    emit_event(
                        &config,
                        &base_dir,
                        "auto_pause_cleared",
                        json!({"source": "manual_resume"}),
                    );
                }
                continue;
            }
            Err(WatchdogError::ScanTimeout {
                timeout_secs,
                pending_shares,
            }) => {
                if dry_run || once {
                    return Err(WatchdogError::ScanTimeout {
                        timeout_secs,
                        pending_shares,
                    });
                }
                error!(
                    "Share scan timed out after {}s ({} share(s) pending)",
                    timeout_secs, pending_shares
                );
                state.set_nfs_healthy(false);
                state.update(|s| {
                    s.scan_timeout_count += 1;
                    s.last_failure_code = Some(FailureCode::ScanTimeout.as_str().to_string());
                });
                db.record_pipeline_failure("scan_timeout", FailureCode::ScanTimeout.as_str());
                let service_state = db.note_pass_failure(FailureCode::ScanTimeout.as_str());
                state.update(|s| {
                    s.consecutive_pass_failures = service_state.consecutive_pass_failures;
                });
                emit_event(
                    &config,
                    &base_dir,
                    "scan_timeout",
                    json!({
                        "timeout_seconds": timeout_secs,
                        "pending_shares": pending_shares,
                        "consecutive_pass_failures": service_state.consecutive_pass_failures,
                    }),
                );
                if config.safety.auto_pause_on_pass_failures
                    && service_state.consecutive_pass_failures
                        >= config.safety.max_consecutive_pass_failures.max(1)
                {
                    let reason = format!(
                        "tripwire: {} consecutive pass failures (latest: {})",
                        service_state.consecutive_pass_failures,
                        FailureCode::ScanTimeout.as_str()
                    );
                    match write_pause_marker(&config, &base_dir, &reason) {
                        Ok(path) => info!("Auto-pause marker created at {}", path.display()),
                        Err(e) => warn!("Failed to write auto-pause marker: {}", e),
                    }
                    db.mark_auto_paused(&reason);
                    db.record_pipeline_failure(
                        FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP,
                        FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP,
                    );
                    sync_service_state(&state, db.as_ref());
                    state.update(|s| {
                        s.last_failure_code =
                            Some(FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP.to_string());
                    });
                    emit_event(
                        &config,
                        &base_dir,
                        "auto_pause",
                        json!({
                            "reason": reason,
                            "trigger_failure_code": FailureCode::ScanTimeout.as_str(),
                            "consecutive_pass_failures": service_state.consecutive_pass_failures,
                        }),
                    );
                    state.set_phase(PipelinePhase::Paused);
                    tui_log(
                        &state,
                        "ERROR",
                        "Safety tripwire triggered; pipeline auto-paused until --resume",
                    );
                    if !wait_while_paused(&config, &base_dir, &state, &mut shutdown_rx).await {
                        emit_event(
                            &config,
                            &base_dir,
                            "shutdown",
                            json!({"phase": "auto_pause_wait"}),
                        );
                        return Ok(());
                    }
                    db.clear_auto_paused();
                    db.note_pass_success();
                    sync_service_state(&state, db.as_ref());
                    emit_event(
                        &config,
                        &base_dir,
                        "auto_pause_cleared",
                        json!({"source": "manual_resume"}),
                    );
                    retry_delay = 30;
                    continue;
                }
                tui_log(
                    &state,
                    "ERROR",
                    &format!(
                        "Pass failed: scan timeout after {}s ({} pending share(s))",
                        timeout_secs, pending_shares
                    ),
                );
                send_webhook(
                    &config.notify,
                    NotifyEvent::PassFailureSummary,
                    &json!({
                        "error": format!(
                            "share scan timed out after {}s ({} share(s) pending)",
                            timeout_secs, pending_shares
                        ),
                        "failure_code": FailureCode::ScanTimeout.as_str(),
                        "retry_delay_seconds": retry_delay,
                    }),
                );
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay)) => {}
                    _ = shutdown_rx.recv() => {
                        info!("Pipeline shutting down during retry backoff");
                        emit_event(
                            &config,
                            &base_dir,
                            "shutdown",
                            json!({"phase": "retry_backoff_scan_timeout"}),
                        );
                        return Ok(());
                    }
                }
                retry_delay = (retry_delay * 2).min(300);
                continue;
            }
            Err(e) => {
                if dry_run || once {
                    return Err(e);
                }
                let pass_failure_code = classify_pass_failure_code(&e).to_string();
                error!("Watchdog pass failed: {}", e);
                state.set_nfs_healthy(false);
                state.update(|s| {
                    s.last_failure_code = Some(pass_failure_code.clone());
                });
                db.record_pipeline_failure(&pass_failure_code, &pass_failure_code);
                let service_state = db.note_pass_failure(&pass_failure_code);
                state.update(|s| {
                    s.consecutive_pass_failures = service_state.consecutive_pass_failures;
                });
                tui_log(&state, "ERROR", &format!("Pass failed: {}", e));
                emit_event(
                    &config,
                    &base_dir,
                    "pass_end",
                    json!({
                        "result": "failed",
                        "failure_code": pass_failure_code,
                        "error": e.to_string(),
                        "consecutive_pass_failures": service_state.consecutive_pass_failures,
                    }),
                );
                if config.safety.auto_pause_on_pass_failures
                    && service_state.consecutive_pass_failures
                        >= config.safety.max_consecutive_pass_failures.max(1)
                {
                    let reason = format!(
                        "tripwire: {} consecutive pass failures (latest: {})",
                        service_state.consecutive_pass_failures, pass_failure_code
                    );
                    match write_pause_marker(&config, &base_dir, &reason) {
                        Ok(path) => info!("Auto-pause marker created at {}", path.display()),
                        Err(err) => warn!("Failed to write auto-pause marker: {}", err),
                    }
                    db.mark_auto_paused(&reason);
                    db.record_pipeline_failure(
                        FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP,
                        FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP,
                    );
                    sync_service_state(&state, db.as_ref());
                    state.update(|s| {
                        s.last_failure_code =
                            Some(FAILURE_CODE_AUTO_PAUSED_SAFETY_TRIP.to_string());
                    });
                    emit_event(
                        &config,
                        &base_dir,
                        "auto_pause",
                        json!({
                            "reason": reason,
                            "trigger_failure_code": pass_failure_code,
                            "consecutive_pass_failures": service_state.consecutive_pass_failures,
                        }),
                    );
                    state.set_phase(PipelinePhase::Paused);
                    tui_log(
                        &state,
                        "ERROR",
                        "Safety tripwire triggered; pipeline auto-paused until --resume",
                    );
                    if !wait_while_paused(&config, &base_dir, &state, &mut shutdown_rx).await {
                        emit_event(
                            &config,
                            &base_dir,
                            "shutdown",
                            json!({"phase": "auto_pause_wait"}),
                        );
                        return Ok(());
                    }
                    db.clear_auto_paused();
                    db.note_pass_success();
                    sync_service_state(&state, db.as_ref());
                    emit_event(
                        &config,
                        &base_dir,
                        "auto_pause_cleared",
                        json!({"source": "manual_resume"}),
                    );
                    retry_delay = 30;
                    continue;
                }
                send_webhook(
                    &config.notify,
                    NotifyEvent::PassFailureSummary,
                    &json!({
                        "error": e.to_string(),
                        "failure_code": pass_failure_code,
                        "retry_delay_seconds": retry_delay,
                    }),
                );
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay)) => {}
                    _ = shutdown_rx.recv() => {
                        info!("Pipeline shutting down during retry backoff");
                        emit_event(
                            &config,
                            &base_dir,
                            "shutdown",
                            json!({"phase": "retry_backoff_pass_error"}),
                        );
                        return Ok(());
                    }
                }
                retry_delay = (retry_delay * 2).min(300);
                continue;
            }
        }

        if dry_run || once {
            return Ok(());
        }

        if is_pause_requested(&config, &base_dir) {
            state.set_phase(PipelinePhase::Paused);
            tui_log(&state, "WARN", "Pipeline paused");
            if !wait_while_paused(&config, &base_dir, &state, &mut shutdown_rx).await {
                emit_event(
                    &config,
                    &base_dir,
                    "shutdown",
                    json!({"phase": "manual_pause_wait"}),
                );
                return Ok(());
            }
            if db.get_service_state().auto_paused_at.is_some() {
                db.clear_auto_paused();
                db.note_pass_success();
                sync_service_state(&state, db.as_ref());
                emit_event(
                    &config,
                    &base_dir,
                    "auto_pause_cleared",
                    json!({"source": "manual_resume"}),
                );
            }
            continue;
        }

        // Wait for next scan interval or shutdown
        state.set_phase(PipelinePhase::Waiting);
        state.set_current_file(None);
        state.reset_file_progress();
        tui_log(
            &state,
            "INFO",
            &format!("Waiting {}s until next scan", config.scan.interval_seconds),
        );

        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(config.scan.interval_seconds)) => {}
            _ = shutdown_rx.recv() => {
                info!("Pipeline shutting down during wait");
                emit_event(
                    &config,
                    &base_dir,
                    "shutdown",
                    json!({"phase": "interval_wait"}),
                );
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_local_temp_paths_hashes_full_path_to_avoid_collisions() {
        let temp_dir = Path::new("/tmp/watchdog");
        let a = Path::new("/mnt/movies/A/Movie.mkv");
        let b = Path::new("/mnt/movies/B/Movie.mkv");
        let (a_source, a_output) = build_local_temp_paths(temp_dir, "movies", a);
        let (b_source, b_output) = build_local_temp_paths(temp_dir, "movies", b);

        assert_ne!(a_source, b_source);
        assert_ne!(a_output, b_output);
        assert!(a_source
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("movies_")));
    }

    #[test]
    fn shutdown_requested_false_when_no_signal() {
        let (_tx, mut rx) = broadcast::channel::<()>(1);
        assert!(!shutdown_requested(&mut rx));
    }

    #[test]
    fn shutdown_requested_true_when_signal_arrives() {
        let (tx, mut rx) = broadcast::channel::<()>(1);
        let _ = tx.send(());
        assert!(shutdown_requested(&mut rx));
    }

    #[test]
    fn shutdown_requested_true_when_channel_lagged() {
        let (tx, mut rx) = broadcast::channel::<()>(1);
        let _ = tx.send(());
        let _ = tx.send(());
        assert!(shutdown_requested(&mut rx));
    }

    #[test]
    fn shutdown_requested_true_when_channel_closed() {
        let (tx, mut rx) = broadcast::channel::<()>(1);
        drop(tx);
        assert!(shutdown_requested(&mut rx));
    }
}
