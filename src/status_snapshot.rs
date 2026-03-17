use crate::config::Config;
use crate::db::{LatestFailureRecord, WatchdogDb};
use crate::state::AppState;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::warn;

#[derive(Debug, Serialize)]
struct SnapshotPayload<'a> {
    timestamp: String,
    phase: String,
    paused: bool,
    nfs_healthy: bool,
    simulate_mode: bool,
    run_mode: &'a str,
    queue_position: u32,
    queue_total: u32,
    current_file: Option<&'a str>,
    progress_stage: String,
    import_percent: f64,
    transcode_percent: f64,
    export_percent: f64,
    composite_percent: f64,
    transfer_rate_mib_per_sec: f64,
    transfer_eta: &'a str,
    transcode_fps: f64,
    transcode_avg_fps: f64,
    transcode_eta: &'a str,
    totals: SnapshotTotals,
    run: SnapshotRun,
    reliability: SnapshotReliability<'a>,
    cooldown_files: i64,
    top_failure_reasons: &'a [(String, u64)],
    share_health: Vec<SnapshotShareHealth>,
    unhealthy_shares: Vec<String>,
    latest_failure: Option<SnapshotLatestFailure>,
}

#[derive(Debug, Serialize)]
struct SnapshotTotals {
    transcoded: u64,
    inspected: u64,
    space_saved_bytes: i64,
}

#[derive(Debug, Serialize)]
struct SnapshotRun {
    inspected: u64,
    transcoded: u64,
    failures: u64,
    retries_scheduled: u64,
    space_saved_bytes: i64,
    skipped_inspected: u64,
    skipped_young: u64,
    skipped_cooldown: u64,
    skipped_filtered: u64,
    skipped_in_use: u64,
    skipped_quarantined: u64,
}

#[derive(Debug, Serialize)]
struct SnapshotReliability<'a> {
    scan_timeouts: u64,
    retries_scheduled_total: u64,
    last_failure_code: Option<&'a str>,
    consecutive_pass_failures: u32,
    auto_paused: bool,
    auto_pause_reason: Option<&'a str>,
    auto_paused_at: Option<&'a str>,
    quarantined_files: u64,
}

#[derive(Debug, Serialize)]
struct SnapshotShareHealth {
    name: String,
    healthy: bool,
}

#[derive(Debug, Serialize)]
struct SnapshotLatestFailure {
    source_path: String,
    failure_reason: Option<String>,
    failure_code: Option<String>,
    completed_at: Option<String>,
}

pub fn resolve_status_snapshot_path(config: &Config, base_dir: &Path) -> Option<PathBuf> {
    if config.paths.status_snapshot.trim().is_empty() {
        None
    } else {
        Some(config.resolve_path(base_dir, &config.paths.status_snapshot))
    }
}

pub fn read_snapshot_file(path: &Path) -> Option<serde_json::Value> {
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

pub fn write_snapshot(
    path: &Path,
    state: &AppState,
    cooldown_files: i64,
    latest_failure: Option<&LatestFailureRecord>,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let payload = SnapshotPayload {
        timestamp: chrono::Utc::now().to_rfc3339(),
        phase: state.phase.to_string(),
        paused: matches!(state.phase, crate::state::PipelinePhase::Paused),
        nfs_healthy: state.nfs_healthy,
        simulate_mode: state.simulate_mode,
        run_mode: state.run_mode.as_str(),
        queue_position: state.queue_position,
        queue_total: state.queue_total,
        current_file: state.current_file.as_deref(),
        progress_stage: state.progress_stage.to_string(),
        import_percent: state.import_percent,
        transcode_percent: state.transcode_percent,
        export_percent: state.export_percent,
        composite_percent: (state.import_percent * 0.20)
            + (state.transcode_percent * 0.60)
            + (state.export_percent * 0.20),
        transfer_rate_mib_per_sec: state.transfer_rate_mib_per_sec,
        transfer_eta: &state.transfer_eta,
        transcode_fps: state.transcode_fps,
        transcode_avg_fps: state.transcode_avg_fps,
        transcode_eta: &state.transcode_eta,
        totals: SnapshotTotals {
            transcoded: state.total_transcoded,
            inspected: state.total_inspected,
            space_saved_bytes: state.total_space_saved,
        },
        run: SnapshotRun {
            inspected: state.run_inspected,
            transcoded: state.run_transcoded,
            failures: state.run_failures,
            retries_scheduled: state.run_retries_scheduled,
            space_saved_bytes: state.run_space_saved,
            skipped_inspected: state.run_skipped_inspected,
            skipped_young: state.run_skipped_young,
            skipped_cooldown: state.run_skipped_cooldown,
            skipped_filtered: state.run_skipped_filtered,
            skipped_in_use: state.run_skipped_in_use,
            skipped_quarantined: state.run_skipped_quarantined,
        },
        reliability: SnapshotReliability {
            scan_timeouts: state.scan_timeout_count,
            retries_scheduled_total: state.total_retries_scheduled,
            last_failure_code: state.last_failure_code.as_deref(),
            consecutive_pass_failures: state.consecutive_pass_failures,
            auto_paused: state.auto_paused,
            auto_pause_reason: state.auto_pause_reason.as_deref(),
            auto_paused_at: state.auto_paused_at.as_deref(),
            quarantined_files: state.quarantined_files,
        },
        cooldown_files,
        top_failure_reasons: &state.top_failure_reasons,
        share_health: state
            .share_health
            .iter()
            .map(|(name, healthy)| SnapshotShareHealth {
                name: name.clone(),
                healthy: *healthy,
            })
            .collect(),
        unhealthy_shares: state
            .share_health
            .iter()
            .filter_map(|(name, healthy)| (!*healthy).then_some(name.clone()))
            .collect(),
        latest_failure: latest_failure.map(|failure| SnapshotLatestFailure {
            source_path: failure.source_path.clone(),
            failure_reason: failure.failure_reason.clone(),
            failure_code: failure.failure_code.clone(),
            completed_at: failure.completed_at.clone(),
        }),
    };

    let bytes = serde_json::to_vec_pretty(&payload)?;
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("status_snapshot");
    let tmp_path = path.with_file_name(format!("{}.tmp", file_name));
    std::fs::write(&tmp_path, bytes)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub async fn run_status_snapshot_task(
    mut state_rx: watch::Receiver<AppState>,
    db: Arc<WatchdogDb>,
    snapshot_path: PathBuf,
    mut shutdown_rx: broadcast::Receiver<()>,
    throttle: Duration,
) {
    let mut dirty = true;
    let mut ticker = tokio::time::interval(throttle);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                let state = state_rx.borrow().clone();
                let cooldown = db.get_cooldown_active_count(chrono::Utc::now().timestamp());
                let latest_failure = db.get_latest_failure();
                if let Err(e) = write_snapshot(&snapshot_path, &state, cooldown, latest_failure.as_ref()) {
                    warn!("Failed to write final status snapshot: {}", e);
                }
                break;
            }
            changed = state_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                dirty = true;
            }
            _ = ticker.tick() => {
                if dirty {
                    let state = state_rx.borrow().clone();
                    let cooldown = db.get_cooldown_active_count(chrono::Utc::now().timestamp());
                    let latest_failure = db.get_latest_failure();
                    if let Err(e) = write_snapshot(&snapshot_path, &state, cooldown, latest_failure.as_ref()) {
                        warn!("Failed to write status snapshot: {}", e);
                    }
                    dirty = false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::state::StateManager;

    #[test]
    fn resolve_snapshot_path_disabled_when_empty() {
        let cfg = Config::default_config();
        assert!(resolve_status_snapshot_path(&cfg, Path::new(".")).is_none());
    }

    #[test]
    fn write_snapshot_outputs_valid_json() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let (state, _) = StateManager::new();
        state.set_phase(crate::state::PipelinePhase::AwaitingSelection);
        state.set_run_mode(crate::state::RunMode::Precision);
        state.update(|s| {
            s.run_skipped_in_use = 2;
            s.total_transcoded = 3;
            s.share_health = vec![("movies".to_string(), true), ("tv".to_string(), false)];
        });

        write_snapshot(&path, &state.snapshot(), 5, None).unwrap();
        let content = std::fs::read_to_string(path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(json["cooldown_files"], 5);
        assert_eq!(json["phase"], "Awaiting Selection");
        assert_eq!(json["run_mode"], "precision");
        assert_eq!(json["run"]["skipped_in_use"], 2);
        assert_eq!(json["totals"]["transcoded"], 3);
        assert_eq!(json["reliability"]["scan_timeouts"], 0);
        assert_eq!(json["run"]["retries_scheduled"], 0);
        assert_eq!(json["reliability"]["retries_scheduled_total"], 0);
        assert!(json["reliability"]["last_failure_code"].is_null());
        assert_eq!(json["share_health"][0]["name"], "movies");
        assert_eq!(json["share_health"][1]["healthy"], false);
        assert_eq!(json["unhealthy_shares"][0], "tv");
        assert!(json["latest_failure"].is_null());
    }

    #[tokio::test]
    async fn snapshot_task_coalesces_updates_and_writes_latest() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
        let (state, rx) = StateManager::new();
        let (shutdown_tx, _) = broadcast::channel(1);

        let task = tokio::spawn(run_status_snapshot_task(
            rx,
            db,
            path.clone(),
            shutdown_tx.subscribe(),
            Duration::from_millis(200),
        ));

        state.update(|s| s.queue_position = 1);
        state.update(|s| s.queue_position = 2);
        state.update(|s| s.queue_position = 3);
        tokio::time::sleep(Duration::from_millis(350)).await;

        let content = std::fs::read_to_string(&path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(json["queue_position"], 3);

        let _ = shutdown_tx.send(());
        let _ = task.await;
    }

    #[tokio::test]
    async fn snapshot_task_write_failures_are_non_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf(); // directory path forces write failure
        let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
        let (state, rx) = StateManager::new();
        let (shutdown_tx, _) = broadcast::channel(1);

        let task = tokio::spawn(run_status_snapshot_task(
            rx,
            db,
            path,
            shutdown_tx.subscribe(),
            Duration::from_millis(100),
        ));

        state.update(|s| s.queue_position = 9);
        tokio::time::sleep(Duration::from_millis(180)).await;
        let _ = shutdown_tx.send(());
        assert!(task.await.is_ok());
    }
}
