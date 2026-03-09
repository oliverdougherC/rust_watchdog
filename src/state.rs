use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use tokio::sync::watch;

/// The current phase of the watchdog pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelinePhase {
    Idle,
    Scanning,
    Paused,
    Transcoding,
    Waiting,
}

impl std::fmt::Display for PipelinePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelinePhase::Idle => write!(f, "Idle"),
            PipelinePhase::Scanning => write!(f, "Scanning"),
            PipelinePhase::Paused => write!(f, "Paused"),
            PipelinePhase::Transcoding => write!(f, "Transcoding"),
            PipelinePhase::Waiting => write!(f, "Waiting"),
        }
    }
}

/// Fine-grained stage for the per-file progress bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressStage {
    Idle,
    Import,
    Transcode,
    Export,
}

impl std::fmt::Display for ProgressStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgressStage::Idle => write!(f, "Idle"),
            ProgressStage::Import => write!(f, "Import"),
            ProgressStage::Transcode => write!(f, "Transcode"),
            ProgressStage::Export => write!(f, "Export"),
        }
    }
}

/// Snapshot of the entire application state, shared between pipeline and TUI.
#[derive(Debug, Clone)]
pub struct AppState {
    pub phase: PipelinePhase,
    pub nfs_healthy: bool,
    pub simulate_mode: bool,

    // Current transcode info
    pub current_file: Option<String>,
    pub queue_position: u32,
    pub queue_total: u32,
    pub progress_stage: ProgressStage,
    pub import_percent: f64,
    pub transcode_percent: f64,
    pub export_percent: f64,
    pub transfer_rate_mib_per_sec: f64,
    pub transfer_eta: String,
    pub transcode_fps: f64,
    pub transcode_avg_fps: f64,
    pub transcode_eta: String,

    // Cumulative stats
    pub total_transcoded: u64,
    pub total_space_saved: i64,
    pub total_inspected: u64,

    // Run stats
    pub run_inspected: u64,
    pub run_transcoded: u64,
    pub run_failures: u64,
    pub run_space_saved: i64,
    pub run_skipped_inspected: u64,
    pub run_skipped_young: u64,
    pub run_skipped_cooldown: u64,
    pub run_skipped_filtered: u64,
    pub run_skipped_in_use: u64,
    pub run_skipped_quarantined: u64,
    pub scan_timeout_count: u64,
    pub consecutive_pass_failures: u32,
    pub auto_paused: bool,
    pub auto_pause_reason: Option<String>,
    pub auto_paused_at: Option<String>,
    pub quarantined_files: u64,

    // Failure insights
    pub top_failure_reasons: Vec<(String, u64)>,
    pub last_failure_code: Option<String>,
    pub share_health: Vec<(String, bool)>,

    // Timing
    pub last_pass_time: Option<DateTime<Utc>>,

    // Log lines for the TUI log viewer
    pub log_lines: VecDeque<String>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            phase: PipelinePhase::Idle,
            nfs_healthy: true,
            simulate_mode: false,
            current_file: None,
            queue_position: 0,
            queue_total: 0,
            progress_stage: ProgressStage::Idle,
            import_percent: 0.0,
            transcode_percent: 0.0,
            export_percent: 0.0,
            transfer_rate_mib_per_sec: 0.0,
            transfer_eta: String::new(),
            transcode_fps: 0.0,
            transcode_avg_fps: 0.0,
            transcode_eta: String::new(),
            total_transcoded: 0,
            total_space_saved: 0,
            total_inspected: 0,
            run_inspected: 0,
            run_transcoded: 0,
            run_failures: 0,
            run_space_saved: 0,
            run_skipped_inspected: 0,
            run_skipped_young: 0,
            run_skipped_cooldown: 0,
            run_skipped_filtered: 0,
            run_skipped_in_use: 0,
            run_skipped_quarantined: 0,
            scan_timeout_count: 0,
            consecutive_pass_failures: 0,
            auto_paused: false,
            auto_pause_reason: None,
            auto_paused_at: None,
            quarantined_files: 0,
            top_failure_reasons: Vec::new(),
            last_failure_code: None,
            share_health: Vec::new(),
            last_pass_time: None,
            log_lines: VecDeque::with_capacity(500),
        }
    }
}

const MAX_LOG_LINES: usize = 500;

/// Manages shared state between the pipeline task and the TUI task
/// using a `tokio::sync::watch` channel.
#[derive(Clone)]
pub struct StateManager {
    tx: watch::Sender<AppState>,
}

impl StateManager {
    /// Create a new StateManager and return it along with a watch Receiver.
    pub fn new() -> (Self, watch::Receiver<AppState>) {
        let (tx, rx) = watch::channel(AppState::default());
        (Self { tx }, rx)
    }

    /// Update the state by applying a closure. The closure receives a mutable
    /// reference to the current state.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut AppState),
    {
        self.tx.send_modify(f);
    }

    /// Append a log line to the state.
    pub fn append_log(&self, line: String) {
        self.tx.send_modify(|state| {
            if state.log_lines.len() >= MAX_LOG_LINES {
                state.log_lines.pop_front();
            }
            state.log_lines.push_back(line);
        });
    }

    /// Set the pipeline phase.
    pub fn set_phase(&self, phase: PipelinePhase) {
        self.tx.send_modify(|state| {
            state.phase = phase;
        });
    }

    /// Set the current file being transcoded.
    pub fn set_current_file(&self, file: Option<String>) {
        self.tx.send_modify(|state| {
            state.current_file = file;
        });
    }

    /// Set queue progress.
    pub fn set_queue_info(&self, position: u32, total: u32) {
        self.tx.send_modify(|state| {
            state.queue_position = position;
            state.queue_total = total;
        });
    }

    /// Reset all per-file progress fields.
    pub fn reset_file_progress(&self) {
        self.tx.send_modify(|state| {
            state.progress_stage = ProgressStage::Idle;
            state.import_percent = 0.0;
            state.transcode_percent = 0.0;
            state.export_percent = 0.0;
            state.transfer_rate_mib_per_sec = 0.0;
            state.transfer_eta.clear();
            state.transcode_fps = 0.0;
            state.transcode_avg_fps = 0.0;
            state.transcode_eta.clear();
        });
    }

    /// Set import transfer progress.
    pub fn set_import_progress(&self, percent: f64, rate_mib_per_sec: f64, eta: String) {
        self.tx.send_modify(|state| {
            state.progress_stage = ProgressStage::Import;
            state.import_percent = percent.clamp(0.0, 100.0);
            state.transfer_rate_mib_per_sec = rate_mib_per_sec.max(0.0);
            state.transfer_eta = eta;
        });
    }

    /// Set export transfer progress.
    pub fn set_export_progress(&self, percent: f64, rate_mib_per_sec: f64, eta: String) {
        self.tx.send_modify(|state| {
            state.progress_stage = ProgressStage::Export;
            state.export_percent = percent.clamp(0.0, 100.0);
            state.transfer_rate_mib_per_sec = rate_mib_per_sec.max(0.0);
            state.transfer_eta = eta;
        });
    }

    /// Set the active per-file progress stage without changing percentages.
    pub fn set_progress_stage(&self, stage: ProgressStage) {
        self.tx.send_modify(|state| {
            state.progress_stage = stage;
        });
    }

    /// Set transcode progress.
    pub fn set_transcode_progress(&self, percent: f64, fps: f64, avg_fps: f64, eta: String) {
        self.tx.send_modify(|state| {
            state.progress_stage = ProgressStage::Transcode;
            state.transcode_percent = percent.clamp(0.0, 100.0);
            state.transcode_fps = fps;
            state.transcode_avg_fps = avg_fps;
            state.transcode_eta = eta;
        });
    }

    /// Set NFS health status.
    pub fn set_nfs_healthy(&self, healthy: bool) {
        self.tx.send_modify(|state| {
            state.nfs_healthy = healthy;
        });
    }

    /// Set per-share health snapshot.
    pub fn set_share_health(&self, share_health: Vec<(String, bool)>) {
        self.tx.send_modify(|state| {
            state.share_health = share_health;
        });
    }

    /// Record the completion of a pass.
    pub fn set_last_pass_time(&self) {
        self.tx.send_modify(|state| {
            state.last_pass_time = Some(Utc::now());
        });
    }

    /// Subscribe to state changes.
    pub fn subscribe(&self) -> watch::Receiver<AppState> {
        self.tx.subscribe()
    }

    /// Get a snapshot of the current state.
    pub fn snapshot(&self) -> AppState {
        self.tx.borrow().clone()
    }
}
