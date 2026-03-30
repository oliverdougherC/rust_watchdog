pub mod cooldown_tab;
pub mod dashboard_tab;
pub mod history_tab;
pub mod logs_tab;
pub mod queue_tab;
pub mod widgets;

use crate::config::{bundled_preset_dir, Config};
use crate::db::{ServiceState, WatchdogDb};
use crate::event_journal::{
    append_event, append_log_event, read_recent_log_lines_for_current_session,
};
use crate::process::describe_exit_status;
use crate::scanner::RealFileSystem;
use crate::selection::{
    enqueue_manual_paths_with_progress, ManualSelectionPhase, ManualSelectionProgress,
    ManualSelectionSummary,
};
use crate::state::{AppState, PipelinePhase, ProgressStage, RunMode};
use crate::status_snapshot::read_snapshot_file;
use crate::traits::FileSystem;
use crate::transcode::{
    load_preset_catalog, load_preset_payload_text, PresetCatalogEntry, PresetSnapshot,
};
use crate::util::{copy_to_clipboard, pid_is_running};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, MouseButton,
        MouseEvent, MouseEventKind,
    },
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Tabs},
    Frame, Terminal,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, VecDeque};
use std::io::stdout;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Dashboard,
    Queue,
    Logs,
    History,
    Cooldown,
}

impl Tab {
    fn titles() -> Vec<&'static str> {
        vec![
            "1 Dashboard",
            "2 Queue",
            "3 Logs",
            "4 History",
            "5 Cooldown",
        ]
    }

    fn index(self) -> usize {
        match self {
            Tab::Dashboard => 0,
            Tab::Queue => 1,
            Tab::Logs => 2,
            Tab::History => 3,
            Tab::Cooldown => 4,
        }
    }

    fn next(self) -> Tab {
        match self {
            Tab::Dashboard => Tab::Queue,
            Tab::Queue => Tab::Logs,
            Tab::Logs => Tab::History,
            Tab::History => Tab::Cooldown,
            Tab::Cooldown => Tab::Dashboard,
        }
    }
}

#[derive(Debug, Clone)]
struct BrowserEntry {
    path: PathBuf,
    label: String,
    is_dir: bool,
    available: bool,
}

#[derive(Debug, Clone)]
struct BrowserState {
    current_dir: Option<PathBuf>,
    entries: Vec<BrowserEntry>,
    selected_index: usize,
    scroll_offset: usize,
    selected_targets: BTreeSet<PathBuf>,
}

impl BrowserState {
    fn new(config: &Config) -> Self {
        let mut browser = Self {
            current_dir: None,
            entries: Vec::new(),
            selected_index: 0,
            scroll_offset: 0,
            selected_targets: BTreeSet::new(),
        };
        browser.refresh_entries(config);
        browser
    }

    fn refresh_entries(&mut self, config: &Config) {
        let fs = RealFileSystem;
        self.entries = if let Some(dir) = self.current_dir.as_ref() {
            let mut entries = fs.list_dir(dir).unwrap_or_default();
            entries.sort_by(|a, b| {
                let a_is_dir = fs.is_dir(a);
                let b_is_dir = fs.is_dir(b);
                b_is_dir
                    .cmp(&a_is_dir)
                    .then_with(|| a.file_name().cmp(&b.file_name()))
            });
            entries
                .into_iter()
                .filter_map(|path| {
                    if fs.is_dir(&path) {
                        let name = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string();
                        Some(BrowserEntry {
                            label: format!("{}/", name),
                            path,
                            is_dir: true,
                            available: true,
                        })
                    } else if path_matches_video_extension(&path, &config.scan.video_extensions) {
                        let name = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string();
                        Some(BrowserEntry {
                            label: name,
                            path,
                            is_dir: false,
                            available: true,
                        })
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            config
                .shares
                .iter()
                .map(|share| {
                    let path = PathBuf::from(&share.local_mount);
                    let available = fs.is_dir(&path);
                    BrowserEntry {
                        label: format!("{} [{}]", share.name, share.local_mount),
                        path,
                        is_dir: true,
                        available,
                    }
                })
                .collect()
        };

        if self.selected_index >= self.entries.len() {
            self.selected_index = self.entries.len().saturating_sub(1);
        }
        if self.scroll_offset >= self.entries.len() {
            self.scroll_offset = self
                .selected_index
                .min(self.entries.len().saturating_sub(1));
        }
    }

    fn move_down(&mut self) {
        if self.selected_index + 1 < self.entries.len() {
            self.selected_index += 1;
        }
    }

    fn move_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    fn current_entry(&self) -> Option<&BrowserEntry> {
        self.entries.get(self.selected_index)
    }

    fn toggle_current_target(&mut self) {
        let Some(entry) = self.current_entry().cloned() else {
            return;
        };
        if !entry.available {
            return;
        }
        if !self.selected_targets.insert(entry.path.clone()) {
            self.selected_targets.remove(&entry.path);
        }
    }

    fn descend_or_toggle(&mut self, config: &Config) {
        let Some(entry) = self.current_entry().cloned() else {
            return;
        };
        if entry.is_dir {
            if entry.available {
                self.current_dir = Some(entry.path);
                self.selected_index = 0;
                self.scroll_offset = 0;
                self.refresh_entries(config);
            }
        } else {
            self.toggle_current_target();
        }
    }

    fn go_up(&mut self, config: &Config) {
        let Some(current) = self.current_dir.as_ref() else {
            return;
        };
        let Some(share_root) = share_root_for_path(config, current) else {
            self.current_dir = None;
            self.selected_index = 0;
            self.refresh_entries(config);
            return;
        };
        if current == &share_root {
            self.current_dir = None;
        } else {
            self.current_dir = current.parent().map(Path::to_path_buf);
            if self.current_dir.as_ref().is_some_and(|dir| {
                dir == &share_root.parent().unwrap_or(Path::new("/")).to_path_buf()
                    && !dir.starts_with(&share_root)
            }) {
                self.current_dir = Some(share_root);
            }
        }
        self.selected_index = 0;
        self.scroll_offset = 0;
        self.refresh_entries(config);
    }

    fn ensure_visible(&mut self, viewport_rows: usize) {
        if viewport_rows == 0 {
            self.scroll_offset = self.selected_index;
            return;
        }

        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        } else {
            let visible_end = self.scroll_offset.saturating_add(viewport_rows);
            if self.selected_index >= visible_end {
                self.scroll_offset = self.selected_index + 1 - viewport_rows;
            }
        }
    }

    fn visible_entries(&self, viewport_rows: usize) -> &[BrowserEntry] {
        if viewport_rows == 0 || self.entries.is_empty() {
            return &[];
        }
        let start = self.scroll_offset.min(self.entries.len().saturating_sub(1));
        let end = start.saturating_add(viewport_rows).min(self.entries.len());
        &self.entries[start..end]
    }
}

#[derive(Debug, Clone)]
struct PresetSelectorState {
    entries: Vec<PresetCatalogEntry>,
    selected_index: usize,
    scroll_offset: usize,
}

impl PresetSelectorState {
    fn new(base_dir: &Path, current: &PresetSnapshot) -> Self {
        let entries = load_preset_catalog(base_dir);
        let selected_index = entries
            .iter()
            .position(|entry| entry.snapshot().as_ref() == Some(current))
            .unwrap_or(0);
        Self {
            entries,
            selected_index,
            scroll_offset: selected_index,
        }
    }

    fn move_down(&mut self) {
        if self.selected_index + 1 < self.entries.len() {
            self.selected_index += 1;
        }
    }

    fn move_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    fn scroll_to_top(&mut self) {
        self.selected_index = 0;
    }

    fn scroll_to_bottom(&mut self) {
        if !self.entries.is_empty() {
            self.selected_index = self.entries.len() - 1;
        }
    }

    fn current_entry(&self) -> Option<&PresetCatalogEntry> {
        self.entries.get(self.selected_index)
    }

    fn ensure_visible(&mut self, viewport_rows: usize) {
        if viewport_rows == 0 {
            self.scroll_offset = self.selected_index;
            return;
        }

        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        } else {
            let visible_end = self.scroll_offset.saturating_add(viewport_rows);
            if self.selected_index >= visible_end {
                self.scroll_offset = self.selected_index + 1 - viewport_rows;
            }
        }
    }

    fn visible_entries(&self, viewport_rows: usize) -> &[PresetCatalogEntry] {
        if viewport_rows == 0 || self.entries.is_empty() {
            return &[];
        }
        let start = self.scroll_offset.min(self.entries.len().saturating_sub(1));
        let end = start.saturating_add(viewport_rows).min(self.entries.len());
        &self.entries[start..end]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuitChoice {
    Background,
    Kill,
    Copy,
    Cancel,
}

impl QuitChoice {
    fn next(self) -> Self {
        match self {
            Self::Background => Self::Kill,
            Self::Kill => Self::Copy,
            Self::Copy => Self::Cancel,
            Self::Cancel => Self::Background,
        }
    }

    fn prev(self) -> Self {
        match self {
            Self::Background => Self::Cancel,
            Self::Kill => Self::Background,
            Self::Copy => Self::Kill,
            Self::Cancel => Self::Copy,
        }
    }
}

struct QuitKillState {
    pid: u32,
    started_at: Instant,
    followup_signal_sent: bool,
}

enum SelectionTaskUpdate {
    Progress(ManualSelectionProgress),
    Finished(ManualSelectionSummary),
}

struct SelectionTaskState {
    rx: Receiver<SelectionTaskUpdate>,
    latest_progress: ManualSelectionProgress,
    started_at: Instant,
}

struct TuiApp {
    current_tab: Tab,
    logs_state: logs_tab::LogsTabState,
    history_state: history_tab::HistoryTabState,
    cooldown_state: cooldown_tab::CooldownTabState,
    queue_state: queue_tab::QueueTabState,
    base_dir: PathBuf,
    last_db_refresh: Instant,
    last_snapshot_refresh: Instant,
    current_state: AppState,
    browser: Option<BrowserState>,
    preset_selector: Option<PresetSelectorState>,
    quit_choice: QuitChoice,
    show_quit_modal: bool,
    quit_kill_state: Option<QuitKillState>,
    worker_pid: Option<u32>,
    owned_worker: Option<Child>,
    auto_opened_browser: bool,
    status_message: Option<String>,
    attach_hint: String,
    selection_task: Option<SelectionTaskState>,
    default_preset: PresetSnapshot,
    selected_precision_preset: PresetSnapshot,
}

impl TuiApp {
    fn new(config: &Config, base_dir: PathBuf, attach_hint: String) -> Self {
        let default_preset = PresetSnapshot::normalized(
            &base_dir,
            &config.transcode.preset_file,
            &config.transcode.preset_name,
            &config.transcode.target_codec,
        );
        Self {
            current_tab: Tab::Dashboard,
            logs_state: logs_tab::LogsTabState::default(),
            history_state: history_tab::HistoryTabState::default(),
            cooldown_state: cooldown_tab::CooldownTabState::default(),
            queue_state: queue_tab::QueueTabState::default(),
            base_dir,
            last_db_refresh: Instant::now() - Duration::from_secs(10),
            last_snapshot_refresh: Instant::now() - Duration::from_secs(10),
            current_state: AppState::default(),
            browser: None,
            preset_selector: None,
            quit_choice: QuitChoice::Background,
            show_quit_modal: false,
            quit_kill_state: None,
            worker_pid: None,
            owned_worker: None,
            auto_opened_browser: false,
            status_message: None,
            attach_hint,
            selection_task: None,
            default_preset: default_preset.clone(),
            selected_precision_preset: default_preset,
        }
    }
}

pub async fn run_tui(
    config: Config,
    base_dir: PathBuf,
    db: Arc<WatchdogDb>,
    snapshot_path: PathBuf,
    event_journal_path: PathBuf,
    attach_hint: String,
    owned_worker: Option<Child>,
) -> anyhow::Result<()> {
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = stdout().execute(DisableMouseCapture);
        let _ = stdout().execute(LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    stdout().execute(EnableMouseCapture)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp::new(&config, base_dir, attach_hint);
    app.owned_worker = owned_worker;
    let tick_rate = Duration::from_millis(100);

    let result = run_tui_loop(
        &mut terminal,
        &mut app,
        &config,
        &db,
        &snapshot_path,
        &event_journal_path,
        tick_rate,
    )
    .await;

    disable_raw_mode()?;
    stdout().execute(DisableMouseCapture)?;
    stdout().execute(LeaveAlternateScreen)?;
    result
}

async fn run_tui_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    app: &mut TuiApp,
    config: &Config,
    db: &Arc<WatchdogDb>,
    snapshot_path: &Path,
    event_journal_path: &Path,
    tick_rate: Duration,
) -> anyhow::Result<()> {
    loop {
        refresh_runtime_state(app, db.as_ref(), snapshot_path, event_journal_path);
        poll_selection_task(app, db.as_ref(), event_journal_path);
        if finalize_requested_kill(app, db.as_ref(), event_journal_path) {
            return Ok(());
        }

        if app.last_db_refresh.elapsed() > Duration::from_secs(2) {
            app.history_state.refresh(db.as_ref());
            app.cooldown_state.refresh(db.as_ref());
            app.queue_state.refresh(db.as_ref());
            app.last_db_refresh = Instant::now();
        }

        if !app.auto_opened_browser
            && matches!(app.current_state.run_mode, RunMode::Precision)
            && db.get_queue_count() == 0
            && app.selection_task.is_none()
        {
            app.browser = Some(BrowserState::new(config));
            app.auto_opened_browser = true;
        }

        terminal.draw(|f| draw_ui(f, app))?;

        if event::poll(tick_rate)? {
            match event::read()? {
                Event::Key(key) => {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    if handle_key_event(app, config, db, event_journal_path, key.code)? {
                        return Ok(());
                    }
                }
                Event::Mouse(mouse) => {
                    if handle_mouse_event(
                        app,
                        db,
                        event_journal_path,
                        mouse,
                        terminal.size()?.into(),
                    )? {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }
}

fn refresh_runtime_state(
    app: &mut TuiApp,
    db: &WatchdogDb,
    snapshot_path: &Path,
    event_journal_path: &Path,
) {
    if app.last_snapshot_refresh.elapsed() < Duration::from_millis(300) {
        return;
    }
    app.last_snapshot_refresh = Instant::now();

    let service_state = db.get_service_state();
    let owned_worker_pid = poll_owned_worker(app, db, &service_state);
    let worker_pid =
        owned_worker_pid.or_else(|| service_state.worker_pid.filter(|pid| pid_is_running(*pid)));
    if owned_worker_pid.is_none() && worker_pid.is_none() && service_state.worker_pid.is_some() {
        db.clear_worker_state();
    }
    app.worker_pid = worker_pid;

    app.current_state = state_from_runtime(
        read_snapshot_file(snapshot_path),
        &service_state,
        db,
        event_journal_path,
        worker_pid,
    );
}

fn poll_owned_worker(
    app: &mut TuiApp,
    db: &WatchdogDb,
    service_state: &ServiceState,
) -> Option<u32> {
    let mut child = app.owned_worker.take()?;
    let pid = child.id();
    match child.try_wait() {
        Ok(Some(status)) => {
            if service_state
                .worker_pid
                .is_some_and(|worker_pid| worker_pid == pid)
            {
                db.clear_worker_state();
            }
            if app.quit_kill_state.is_none() {
                app.status_message = Some(format!(
                    "Worker {} exited ({})",
                    pid,
                    describe_exit_status(&status)
                ));
            }
            None
        }
        Ok(None) => {
            app.owned_worker = Some(child);
            Some(pid)
        }
        Err(error) => {
            app.status_message = Some(format!("Failed to poll worker {}: {}", pid, error));
            app.owned_worker = Some(child);
            Some(pid)
        }
    }
}

fn poll_selection_task(app: &mut TuiApp, db: &WatchdogDb, event_journal_path: &Path) {
    let mut latest_progress = None;
    let mut completion = None;
    let mut disconnected = false;

    if let Some(task) = app.selection_task.as_mut() {
        loop {
            match task.rx.try_recv() {
                Ok(SelectionTaskUpdate::Progress(progress)) => latest_progress = Some(progress),
                Ok(SelectionTaskUpdate::Finished(summary)) => completion = Some(summary),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if let Some(progress) = latest_progress {
            task.latest_progress = progress;
        }
    }

    if let Some(summary) = completion {
        let message = format_manual_selection_message(&summary);
        append_client_log(event_journal_path, "INFO", &message);
        append_event(
            Some(event_journal_path),
            "manual_selection",
            json!({
                "selected_roots": summary.selected_roots,
                "discovered_files": summary.discovered_files,
                "enqueued_files": summary.enqueued_files,
                "skipped_duplicates": summary.skipped_duplicates,
                "skipped_ineligible": summary.skipped_ineligible,
                "skipped_cooldown": summary.skipped_cooldown,
                "skipped_quarantined": summary.skipped_quarantined,
                "skipped_young": summary.skipped_young,
                "skipped_missing": summary.skipped_missing,
                "skipped_non_video": summary.skipped_non_video,
                "skipped_probe_failed": summary.skipped_probe_failed,
            }),
        );
        app.status_message = Some(message);
        app.browser = None;
        app.queue_state.refresh(db);
        app.last_db_refresh = Instant::now();
        app.selection_task = None;
        return;
    }

    if disconnected {
        app.status_message = Some("Manual selection stopped unexpectedly".to_string());
        app.selection_task = None;
    }
}

fn state_from_runtime(
    snapshot: Option<Value>,
    service_state: &ServiceState,
    db: &WatchdogDb,
    event_journal_path: &Path,
    worker_pid: Option<u32>,
) -> AppState {
    let live_queue_total = db.get_queue_count().max(0) as u32;
    let mut state = AppState {
        run_mode: if worker_pid.is_some()
            && service_state.worker_run_mode.as_deref() == Some("precision")
        {
            RunMode::Precision
        } else {
            RunMode::Watchdog
        },
        queue_total: live_queue_total,
        consecutive_pass_failures: service_state.consecutive_pass_failures,
        auto_paused: service_state.auto_paused_at.is_some(),
        auto_pause_reason: service_state.auto_pause_reason.clone(),
        auto_paused_at: service_state.auto_paused_at.clone(),
        quarantined_files: db.quarantine_count().max(0) as u64,
        ..AppState::default()
    };

    if let Some(snapshot) = snapshot.as_ref() {
        if let Some(phase) = snapshot.get("phase").and_then(Value::as_str) {
            state.phase = parse_phase(phase);
        }
        state.nfs_healthy = snapshot
            .get("nfs_healthy")
            .and_then(Value::as_bool)
            .unwrap_or(state.nfs_healthy);
        state.local_mode = snapshot
            .get("local_mode")
            .and_then(Value::as_bool)
            .unwrap_or(state.local_mode);
        state.simulate_mode = snapshot
            .get("simulate_mode")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if worker_pid.is_some() {
            if let Some(run_mode) = snapshot.get("run_mode").and_then(Value::as_str) {
                state.run_mode = if run_mode.eq_ignore_ascii_case("precision") {
                    RunMode::Precision
                } else {
                    RunMode::Watchdog
                };
            }
        }
        state.queue_position = snapshot
            .get("queue_position")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        state.queue_total = live_queue_total.max(state.queue_position);
        state.current_file = snapshot
            .get("current_file")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if let Some(stage) = snapshot.get("progress_stage").and_then(Value::as_str) {
            state.progress_stage = parse_progress_stage(stage);
        }
        state.import_percent = snapshot
            .get("import_percent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.transcode_percent = snapshot
            .get("transcode_percent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.export_percent = snapshot
            .get("export_percent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.transfer_rate_mib_per_sec = snapshot
            .get("transfer_rate_mib_per_sec")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.transfer_eta = snapshot
            .get("transfer_eta")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        state.transcode_fps = snapshot
            .get("transcode_fps")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.transcode_avg_fps = snapshot
            .get("transcode_avg_fps")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        state.transcode_eta = snapshot
            .get("transcode_eta")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if let Some(totals) = snapshot.get("totals") {
            state.total_transcoded = totals
                .get("transcoded")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.total_inspected = totals.get("inspected").and_then(Value::as_u64).unwrap_or(0);
            state.total_space_saved = totals
                .get("space_saved_bytes")
                .and_then(Value::as_i64)
                .unwrap_or(0);
        }
        if let Some(run) = snapshot.get("run") {
            state.run_inspected = run.get("inspected").and_then(Value::as_u64).unwrap_or(0);
            state.run_transcoded = run.get("transcoded").and_then(Value::as_u64).unwrap_or(0);
            state.run_failures = run.get("failures").and_then(Value::as_u64).unwrap_or(0);
            state.run_retries_scheduled = run
                .get("retries_scheduled")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_space_saved = run
                .get("space_saved_bytes")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            state.run_skipped_inspected = run
                .get("skipped_inspected")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_skipped_young = run
                .get("skipped_young")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_skipped_cooldown = run
                .get("skipped_cooldown")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_skipped_filtered = run
                .get("skipped_filtered")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_skipped_in_use = run
                .get("skipped_in_use")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.run_skipped_quarantined = run
                .get("skipped_quarantined")
                .and_then(Value::as_u64)
                .unwrap_or(0);
        }
        if let Some(reliability) = snapshot.get("reliability") {
            state.scan_timeout_count = reliability
                .get("scan_timeouts")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.total_retries_scheduled = reliability
                .get("retries_scheduled_total")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            state.last_failure_code = reliability
                .get("last_failure_code")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            state.consecutive_pass_failures = reliability
                .get("consecutive_pass_failures")
                .and_then(Value::as_u64)
                .unwrap_or(state.consecutive_pass_failures as u64)
                as u32;
            state.auto_paused = reliability
                .get("auto_paused")
                .and_then(Value::as_bool)
                .unwrap_or(state.auto_paused);
            state.auto_pause_reason = reliability
                .get("auto_pause_reason")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or(state.auto_pause_reason);
            state.auto_paused_at = reliability
                .get("auto_paused_at")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or(state.auto_paused_at);
            state.quarantined_files = reliability
                .get("quarantined_files")
                .and_then(Value::as_u64)
                .unwrap_or(state.quarantined_files);
        }
        state.top_failure_reasons = snapshot
            .get("top_failure_reasons")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let pair = item.as_array()?;
                        Some((pair.first()?.as_str()?.to_string(), pair.get(1)?.as_u64()?))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        state.share_health = snapshot
            .get("share_health")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        Some((
                            item.get("name")?.as_str()?.to_string(),
                            item.get("healthy")?.as_bool()?,
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    if worker_pid.is_some()
        && matches!(state.run_mode, RunMode::Precision)
        && state.queue_total == 0
        && state.current_file.is_none()
    {
        state.phase = PipelinePhase::AwaitingSelection;
    }
    if worker_pid.is_none() {
        state.phase = PipelinePhase::Idle;
        state.queue_position = 0;
        state.current_file = None;
        state.progress_stage = ProgressStage::Idle;
        state.import_percent = 0.0;
        state.transcode_percent = 0.0;
        state.export_percent = 0.0;
        state.transfer_rate_mib_per_sec = 0.0;
        state.transfer_eta.clear();
        state.transcode_fps = 0.0;
        state.transcode_avg_fps = 0.0;
        state.transcode_eta.clear();
    }

    state.log_lines = read_recent_log_lines_for_current_session(event_journal_path, 500)
        .into_iter()
        .collect::<VecDeque<_>>();

    state
}

fn parse_phase(phase: &str) -> PipelinePhase {
    match phase.to_ascii_lowercase().as_str() {
        "scanning" => PipelinePhase::Scanning,
        "rsyncing" | "transferring" => PipelinePhase::Transferring,
        "paused" => PipelinePhase::Paused,
        "transcoding" => PipelinePhase::Transcoding,
        "waiting" => PipelinePhase::Waiting,
        "awaiting selection" | "awaiting_selection" => PipelinePhase::AwaitingSelection,
        _ => PipelinePhase::Idle,
    }
}

fn parse_progress_stage(stage: &str) -> ProgressStage {
    match stage.to_ascii_lowercase().as_str() {
        "import" => ProgressStage::Import,
        "transcode" => ProgressStage::Transcode,
        "export" => ProgressStage::Export,
        _ => ProgressStage::Idle,
    }
}

fn share_root_for_path(config: &Config, path: &Path) -> Option<PathBuf> {
    config
        .shares
        .iter()
        .map(|share| PathBuf::from(&share.local_mount))
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
}

fn path_matches_video_extension(path: &Path, extensions: &[String]) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext.to_ascii_lowercase()))
        .is_some_and(|ext| extensions.iter().any(|candidate| candidate == &ext))
}

fn append_client_log(event_journal_path: &Path, level: &str, message: &str) {
    let ts = chrono::Local::now().format("%H:%M:%S");
    let line = format!("{} | {} | {}", ts, level, message);
    append_log_event(Some(event_journal_path), level, message, &line);
}

fn start_manual_selection_task(
    app: &mut TuiApp,
    config: &Config,
    db: &Arc<WatchdogDb>,
    selected: Vec<PathBuf>,
) {
    let preset = app.selected_precision_preset.clone();
    let preset_payload_json = match load_preset_payload_text(&preset.resolve_path(&app.base_dir)) {
        Ok(payload) => payload,
        Err(error) => {
            app.status_message = Some(format!("Failed to load selected preset: {}", error));
            return;
        }
    };
    let (tx, rx) = mpsc::channel();
    let config = config.clone();
    let db = db.clone();
    let initial_progress = ManualSelectionProgress {
        phase: ManualSelectionPhase::Discovering,
        selected_roots: selected.len(),
        root_index: 1,
        current_root: selected.first().cloned(),
        current_path: None,
        discovered_files: 0,
        processed_files: 0,
        queued_candidates: 0,
    };

    std::thread::spawn(move || {
        let tx_progress = tx.clone();
        let summary = enqueue_manual_paths_with_progress(
            &config,
            &RealFileSystem,
            db.as_ref(),
            &selected,
            &preset,
            &preset_payload_json,
            |progress| {
                let _ = tx_progress.send(SelectionTaskUpdate::Progress(progress));
            },
        );
        let _ = tx.send(SelectionTaskUpdate::Finished(summary));
    });

    app.selection_task = Some(SelectionTaskState {
        rx,
        latest_progress: initial_progress,
        started_at: Instant::now(),
    });
}

fn open_preset_selector(app: &mut TuiApp) {
    if !matches!(app.current_state.run_mode, RunMode::Precision) {
        app.status_message =
            Some("Codec selector is only available in precision mode.".to_string());
        return;
    }

    let selector = PresetSelectorState::new(&app.base_dir, &app.selected_precision_preset);
    if selector.entries.is_empty() {
        app.status_message = Some(format!(
            "No preset files found in {}",
            bundled_preset_dir(&app.base_dir).display()
        ));
    } else {
        app.preset_selector = Some(selector);
    }
}

fn handle_key_event(
    app: &mut TuiApp,
    config: &Config,
    db: &Arc<WatchdogDb>,
    event_journal_path: &Path,
    code: KeyCode,
) -> anyhow::Result<bool> {
    if app.show_quit_modal {
        return handle_quit_modal_key(app, db.as_ref(), event_journal_path, code);
    }

    if app.preset_selector.is_some() {
        return handle_preset_selector_key(app, code);
    }

    if app.browser.is_some() {
        return handle_browser_key(app, config, db, event_journal_path, code);
    }

    match code {
        KeyCode::Char('q') | KeyCode::Esc => {
            if app.selection_task.is_some() {
                app.status_message =
                    Some("Manual selection is still scanning; wait for it to finish.".to_string());
                return Ok(false);
            }
            if app.worker_pid.is_some() {
                app.show_quit_modal = true;
                app.quit_choice = QuitChoice::Background;
                return Ok(false);
            } else {
                return Ok(true);
            }
        }
        KeyCode::Char('1') => app.current_tab = Tab::Dashboard,
        KeyCode::Char('2') => app.current_tab = Tab::Queue,
        KeyCode::Char('3') => app.current_tab = Tab::Logs,
        KeyCode::Char('4') => app.current_tab = Tab::History,
        KeyCode::Char('5') => app.current_tab = Tab::Cooldown,
        KeyCode::Tab => app.current_tab = app.current_tab.next(),
        KeyCode::Char('b') => {
            if app.selection_task.is_some() {
                app.status_message = Some("Manual selection is already scanning.".to_string());
            } else {
                app.browser = Some(BrowserState::new(config));
            }
        }
        KeyCode::Char('c') => open_preset_selector(app),
        KeyCode::Char('d') => {
            if matches!(app.current_tab, Tab::Queue) {
                if !matches!(app.current_state.run_mode, RunMode::Precision) {
                    app.status_message = Some(
                        "Deleting queue rows is only available in precision mode.".to_string(),
                    );
                } else if let Some(idx) = app.queue_state.table_state.selected() {
                    if let Some(record) = app.queue_state.records.get(idx) {
                        let source_path = record.source_path.clone();
                        let started = record.started_at.is_some();
                        if started {
                            app.status_message = Some(
                                "Active transcodes cannot be deleted from the queue.".to_string(),
                            );
                        } else if db.remove_pending_queue_item(&source_path) {
                            append_client_log(
                                event_journal_path,
                                "INFO",
                                &format!("Removed queued item: {}", source_path),
                            );
                            app.status_message = Some(format!("Removed {}", source_path));
                            app.queue_state.refresh(db.as_ref());
                            app.current_state.queue_total = db.get_queue_count().max(0) as u32;
                            app.current_state.queue_total = app
                                .current_state
                                .queue_total
                                .max(app.current_state.queue_position);
                            app.last_db_refresh = Instant::now();
                        } else {
                            app.status_message =
                                Some("Selected queue row is no longer pending.".to_string());
                            app.queue_state.refresh(db.as_ref());
                            app.current_state.queue_total = db.get_queue_count().max(0) as u32;
                            app.current_state.queue_total = app
                                .current_state
                                .queue_total
                                .max(app.current_state.queue_position);
                            app.last_db_refresh = Instant::now();
                        }
                    }
                } else {
                    app.status_message = Some("Select a queue row first.".to_string());
                }
            }
        }
        KeyCode::Char('j') | KeyCode::Down => match app.current_tab {
            Tab::Logs => app.logs_state.scroll_down(),
            Tab::History => app.history_state.scroll_down(),
            Tab::Cooldown => app.cooldown_state.scroll_down(),
            Tab::Queue => app.queue_state.scroll_down(),
            Tab::Dashboard => {}
        },
        KeyCode::Char('k') | KeyCode::Up => match app.current_tab {
            Tab::Logs => app.logs_state.scroll_up(),
            Tab::History => app.history_state.scroll_up(),
            Tab::Cooldown => app.cooldown_state.scroll_up(),
            Tab::Queue => app.queue_state.scroll_up(),
            Tab::Dashboard => {}
        },
        KeyCode::Char('f') => {
            if matches!(app.current_tab, Tab::Logs) {
                app.logs_state.cycle_filter();
            }
        }
        KeyCode::Home => match app.current_tab {
            Tab::Logs => app.logs_state.scroll_to_top(),
            Tab::History => app.history_state.scroll_to_top(),
            Tab::Cooldown => app.cooldown_state.scroll_to_top(),
            Tab::Queue => app.queue_state.scroll_to_top(),
            Tab::Dashboard => {}
        },
        KeyCode::End => match app.current_tab {
            Tab::Logs => app.logs_state.scroll_to_bottom(),
            Tab::History => app.history_state.scroll_to_bottom(),
            Tab::Cooldown => app.cooldown_state.scroll_to_bottom(),
            Tab::Queue => app.queue_state.scroll_to_bottom(),
            Tab::Dashboard => {}
        },
        _ => {}
    }

    Ok(false)
}

fn handle_browser_key(
    app: &mut TuiApp,
    config: &Config,
    db: &Arc<WatchdogDb>,
    _event_journal_path: &Path,
    code: KeyCode,
) -> anyhow::Result<bool> {
    if app.selection_task.is_some() {
        if matches!(code, KeyCode::Esc) {
            app.browser = None;
        }
        return Ok(false);
    }

    match code {
        KeyCode::Esc => app.browser = None,
        KeyCode::Char('c') => open_preset_selector(app),
        KeyCode::Char('j') | KeyCode::Down => {
            if let Some(browser) = app.browser.as_mut() {
                browser.move_down();
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if let Some(browser) = app.browser.as_mut() {
                browser.move_up();
            }
        }
        KeyCode::Enter => {
            if let Some(browser) = app.browser.as_mut() {
                browser.descend_or_toggle(config);
            }
        }
        KeyCode::Char(' ') => {
            if let Some(browser) = app.browser.as_mut() {
                browser.toggle_current_target();
            }
        }
        KeyCode::Backspace => {
            if let Some(browser) = app.browser.as_mut() {
                browser.go_up(config);
            }
        }
        KeyCode::Char('a') => {
            let selected = app
                .browser
                .as_ref()
                .map(|browser| browser.selected_targets.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            if selected.is_empty() {
                app.status_message =
                    Some("Select at least one file or folder before adding.".to_string());
            } else {
                start_manual_selection_task(app, config, db, selected);
            }
        }
        _ => {}
    }

    Ok(false)
}

fn handle_preset_selector_key(app: &mut TuiApp, code: KeyCode) -> anyhow::Result<bool> {
    match code {
        KeyCode::Esc => {
            app.preset_selector = None;
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if let Some(selector) = app.preset_selector.as_mut() {
                selector.move_down();
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if let Some(selector) = app.preset_selector.as_mut() {
                selector.move_up();
            }
        }
        KeyCode::Home => {
            if let Some(selector) = app.preset_selector.as_mut() {
                selector.scroll_to_top();
            }
        }
        KeyCode::End => {
            if let Some(selector) = app.preset_selector.as_mut() {
                selector.scroll_to_bottom();
            }
        }
        KeyCode::Enter => {
            let Some(selector) = app.preset_selector.as_ref() else {
                return Ok(false);
            };
            let Some(entry) = selector.current_entry() else {
                app.status_message = Some("No preset selected.".to_string());
                return Ok(false);
            };
            if let Some(snapshot) = entry.snapshot() {
                app.selected_precision_preset = snapshot;
                app.status_message = Some(format!(
                    "Precision preset selected: {}",
                    app.selected_precision_preset.short_label()
                ));
                app.preset_selector = None;
            } else {
                app.status_message = Some(
                    entry
                        .error_summary
                        .clone()
                        .unwrap_or_else(|| "This preset entry is not selectable.".to_string()),
                );
            }
        }
        _ => {}
    }

    Ok(false)
}

fn handle_quit_modal_key(
    app: &mut TuiApp,
    db: &WatchdogDb,
    event_journal_path: &Path,
    code: KeyCode,
) -> anyhow::Result<bool> {
    match code {
        KeyCode::Esc => {
            if app.quit_kill_state.is_some() {
                app.status_message = Some(
                    "Worker stop is already in progress. Wait for shutdown to finish.".to_string(),
                );
            } else {
                app.show_quit_modal = false;
            }
            return Ok(false);
        }
        KeyCode::Left | KeyCode::Char('h') => app.quit_choice = app.quit_choice.prev(),
        KeyCode::Right | KeyCode::Char('l') | KeyCode::Tab => {
            app.quit_choice = app.quit_choice.next()
        }
        KeyCode::Char('b') => {
            return handle_quit_modal_choice(app, db, event_journal_path, QuitChoice::Background)
        }
        KeyCode::Char('k') => {
            return handle_quit_modal_choice(app, db, event_journal_path, QuitChoice::Kill)
        }
        KeyCode::Char('y') => {
            return handle_quit_modal_choice(app, db, event_journal_path, QuitChoice::Copy)
        }
        KeyCode::Char('c') => {
            return handle_quit_modal_choice(app, db, event_journal_path, QuitChoice::Cancel)
        }
        KeyCode::Enter | KeyCode::Char('q') => {
            return handle_quit_modal_choice(app, db, event_journal_path, app.quit_choice)
        }
        _ => return Ok(false),
    }

    Ok(false)
}

fn handle_quit_modal_choice(
    app: &mut TuiApp,
    db: &WatchdogDb,
    event_journal_path: &Path,
    choice: QuitChoice,
) -> anyhow::Result<bool> {
    app.quit_choice = choice;

    if app.quit_kill_state.is_some() {
        match choice {
            QuitChoice::Copy => return copy_attach_hint(app),
            QuitChoice::Kill => {
                app.status_message = Some(
                    "Worker stop is already in progress. This window will close automatically."
                        .to_string(),
                );
            }
            QuitChoice::Background | QuitChoice::Cancel => {
                app.status_message = Some(
                    "Worker stop is already in progress. Wait for shutdown to finish.".to_string(),
                );
            }
        }
        return Ok(false);
    }

    match choice {
        QuitChoice::Background => Ok(true),
        QuitChoice::Cancel => {
            app.show_quit_modal = false;
            Ok(false)
        }
        QuitChoice::Copy => copy_attach_hint(app),
        QuitChoice::Kill => request_worker_stop(app, db, event_journal_path),
    }
}

fn copy_attach_hint(app: &mut TuiApp) -> anyhow::Result<bool> {
    match copy_to_clipboard(&app.attach_hint) {
        Ok(()) => {
            app.status_message = Some("Reattach command copied to the clipboard.".to_string());
        }
        Err(error) => {
            app.status_message = Some(format!("Failed to copy reattach command: {}", error));
        }
    }
    Ok(false)
}

fn request_worker_stop(
    app: &mut TuiApp,
    _db: &WatchdogDb,
    _event_journal_path: &Path,
) -> anyhow::Result<bool> {
    let Some(pid) = app.worker_pid else {
        app.show_quit_modal = false;
        return Ok(true);
    };

    send_sigterm(pid)?;

    app.quit_choice = QuitChoice::Kill;
    app.quit_kill_state = Some(QuitKillState {
        pid,
        started_at: Instant::now(),
        followup_signal_sent: false,
    });
    app.status_message = Some(format!(
        "Stopping worker {}. Rsync operations can take a moment to unwind safely.",
        pid
    ));
    Ok(false)
}

fn send_sigterm(pid: u32) -> anyhow::Result<()> {
    match kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn finalize_requested_kill(app: &mut TuiApp, db: &WatchdogDb, event_journal_path: &Path) -> bool {
    let Some(kill_state) = app.quit_kill_state.as_mut() else {
        return false;
    };

    if !pid_is_running(kill_state.pid) {
        let pid = kill_state.pid;
        db.clear_worker_state();
        let cleared = db.clear_queue_items();
        append_client_log(
            event_journal_path,
            "WARN",
            &format!(
                "Worker {} stopped by client; cleared {} queued item(s)",
                pid, cleared
            ),
        );
        append_event(
            Some(event_journal_path),
            "worker_stopped",
            json!({"pid": pid, "cleared_queue_items": cleared, "source": "tui_kill"}),
        );
        app.worker_pid = None;
        app.quit_kill_state = None;
        app.show_quit_modal = false;
        app.status_message = Some(format!(
            "Worker {} stopped; cleared {} queued item(s).",
            pid, cleared
        ));
        return true;
    }

    if !kill_state.followup_signal_sent
        && kill_state.started_at.elapsed() >= Duration::from_millis(150)
    {
        let _ = kill(Pid::from_raw(kill_state.pid as i32), Signal::SIGTERM);
        kill_state.followup_signal_sent = true;
    }

    false
}

fn handle_mouse_event(
    app: &mut TuiApp,
    db: &Arc<WatchdogDb>,
    event_journal_path: &Path,
    mouse: MouseEvent,
    area: Rect,
) -> anyhow::Result<bool> {
    if app.show_quit_modal {
        if !matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
            return Ok(false);
        }

        let layout = quit_modal_layout(area);
        for (choice, rect, _) in quit_button_rects(app, layout.buttons) {
            if rect_contains(rect, mouse.column, mouse.row) {
                return handle_quit_modal_choice(app, db.as_ref(), event_journal_path, choice);
            }
        }
        return Ok(false);
    }

    if let Some(selector) = app.preset_selector.as_mut() {
        let popup = centered_rect(84, 72, area);
        if rect_contains(popup, mouse.column, mouse.row) {
            match mouse.kind {
                MouseEventKind::ScrollDown => selector.move_down(),
                MouseEventKind::ScrollUp => selector.move_up(),
                _ => {}
            }
        }
    }

    if let Some(browser) = app.browser.as_mut() {
        let layout = browser_modal_layout(area, app.selection_task.is_some());
        if rect_contains(layout.list, mouse.column, mouse.row) {
            match mouse.kind {
                MouseEventKind::ScrollDown => {
                    browser.move_down();
                    browser.ensure_visible(layout.list.height as usize);
                }
                MouseEventKind::ScrollUp => {
                    browser.move_up();
                    browser.ensure_visible(layout.list.height as usize);
                }
                _ => {}
            }
        }
    }

    Ok(false)
}

fn format_manual_selection_message(summary: &crate::selection::ManualSelectionSummary) -> String {
    format!(
        "Manual selection: selected={} discovered={} added={} duplicates={} cooldown={} quarantined={} young={} missing={} non_video={}",
        summary.selected_roots,
        summary.discovered_files,
        summary.enqueued_files,
        summary.skipped_duplicates,
        summary.skipped_cooldown,
        summary.skipped_quarantined,
        summary.skipped_young,
        summary.skipped_missing,
        summary.skipped_non_video,
    )
}

fn manual_selection_progress_message(progress: &ManualSelectionProgress, spinner: &str) -> String {
    match progress.phase {
        ManualSelectionPhase::Discovering => format!(
            "{} Scanning selection {}/{}: {}",
            spinner,
            progress.root_index.max(1),
            progress.selected_roots.max(1),
            progress
                .current_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "pending".to_string())
        ),
        ManualSelectionPhase::Evaluating => format!(
            "{} Reviewing selected files: {} processed / {} discovered / {} queued",
            spinner,
            progress.processed_files,
            progress.discovered_files,
            progress.queued_candidates
        ),
        ManualSelectionPhase::Enqueueing => format!(
            "{} Adding {} file(s) to the queue...",
            spinner, progress.queued_candidates
        ),
    }
}

fn manual_selection_browser_line(progress: &ManualSelectionProgress, spinner: &str) -> String {
    match progress.phase {
        ManualSelectionPhase::Discovering => format!(
            "{} scanning {}/{} {}",
            spinner,
            progress.root_index.max(1),
            progress.selected_roots.max(1),
            progress
                .current_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "pending".to_string())
        ),
        ManualSelectionPhase::Evaluating => format!(
            "{} checked {}/{} queued={} {}",
            spinner,
            progress.processed_files,
            progress.discovered_files.max(progress.processed_files),
            progress.queued_candidates,
            progress
                .current_path
                .as_ref()
                .and_then(|path| path.file_name())
                .unwrap_or_default()
                .to_string_lossy()
        ),
        ManualSelectionPhase::Enqueueing => {
            format!(
                "{} adding {} file(s) to queue",
                spinner, progress.queued_candidates
            )
        }
    }
}

fn activity_spinner(started_at: Instant) -> &'static str {
    const FRAMES: [&str; 4] = ["|", "/", "-", "\\"];
    let frame = ((started_at.elapsed().as_millis() / 125) % FRAMES.len() as u128) as usize;
    FRAMES[frame]
}

fn draw_ui(f: &mut Frame, app: &mut TuiApp) {
    let size = f.area();
    let outer_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(2),
        ])
        .split(size);

    render_title_bar(f, outer_chunks[0], app);
    match app.current_tab {
        Tab::Dashboard => dashboard_tab::render_dashboard(f, outer_chunks[1], &app.current_state),
        Tab::Queue => queue_tab::render_queue(
            f,
            outer_chunks[1],
            &mut app.queue_state,
            &app.default_preset,
        ),
        Tab::Logs => {
            logs_tab::render_logs(f, outer_chunks[1], &app.current_state, &mut app.logs_state)
        }
        Tab::History => history_tab::render_history(f, outer_chunks[1], &mut app.history_state),
        Tab::Cooldown => cooldown_tab::render_cooldown(f, outer_chunks[1], &mut app.cooldown_state),
    }
    render_footer(f, outer_chunks[2], app);

    let browser_progress = app
        .selection_task
        .as_ref()
        .map(|task| (task.latest_progress.clone(), task.started_at));
    if let Some(browser) = app.browser.as_mut() {
        render_browser_modal(
            f,
            size,
            browser,
            browser_progress.as_ref(),
            &app.selected_precision_preset,
        );
    }
    if let Some(selector) = app.preset_selector.as_mut() {
        render_preset_selector_modal(f, size, selector, &app.selected_precision_preset);
    }
    if app.show_quit_modal {
        render_quit_modal(f, size, app);
    }
}

fn render_title_bar(f: &mut Frame, area: Rect, app: &TuiApp) {
    let block = Block::default()
        .title(" Jellyfin Transcoding Watchdog ")
        .title_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let titles: Vec<Line> = Tab::titles()
        .iter()
        .map(|title| Line::from(Span::raw(*title)))
        .collect();
    let tabs = Tabs::new(titles)
        .select(app.current_tab.index())
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider(Span::raw(" | "));
    f.render_widget(tabs, inner);
}

fn render_footer(f: &mut Frame, area: Rect, app: &TuiApp) {
    let help = if app.preset_selector.is_some() {
        preset_selector_help_line(area.width)
    } else if app.browser.is_some() && app.selection_task.is_some() {
        selection_help_line(area.width)
    } else if app.browser.is_some() {
        browser_help_line(app, area.width)
    } else {
        main_help_line(app, area.width)
    };

    let footer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);
    f.render_widget(Paragraph::new(help), footer[0]);

    let status_line = if let Some(task) = app.selection_task.as_ref() {
        manual_selection_progress_message(&task.latest_progress, activity_spinner(task.started_at))
    } else {
        let base_line = app
            .status_message
            .clone()
            .unwrap_or_else(|| format!("Reattach with {}", app.attach_hint));
        if matches!(app.current_state.run_mode, RunMode::Precision) {
            format!(
                "Preset: {} | {}",
                app.selected_precision_preset.short_label(),
                base_line
            )
        } else {
            base_line
        }
    };
    f.render_widget(
        Paragraph::new(status_line).style(Style::default().fg(Color::DarkGray)),
        footer[1],
    );
}

fn browser_help_line(app: &TuiApp, width: u16) -> Line<'static> {
    if width < 72 {
        if matches!(app.current_state.run_mode, RunMode::Precision) {
            Line::from("j/k move  Enter open  Space select  a add  c codec  Esc close")
        } else {
            Line::from("j/k move  Enter open  Space select  a add  Esc close")
        }
    } else {
        let mut spans = vec![
            Span::styled(
                " j/k",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":move  "),
            Span::styled(
                "Enter",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":open/toggle  "),
            Span::styled(
                "Space",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":select  "),
            Span::styled(
                "Backspace",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":up  "),
            Span::styled(
                "a",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":add queue  "),
        ];
        if matches!(app.current_state.run_mode, RunMode::Precision) {
            spans.extend([
                Span::styled(
                    "c",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(":codec  "),
            ]);
        }
        spans.extend([
            Span::styled(
                "Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":close"),
        ]);
        Line::from(spans)
    }
}

fn selection_help_line(width: u16) -> Line<'static> {
    if width < 72 {
        Line::from("Manual selection scan running  Esc hide")
    } else {
        Line::from(vec![
            Span::styled(
                " Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":hide browser  "),
            Span::raw("Manual selection scan is still running"),
        ])
    }
}

fn preset_selector_help_line(width: u16) -> Line<'static> {
    if width < 72 {
        Line::from("j/k move  Enter select  Esc close")
    } else {
        Line::from(vec![
            Span::styled(
                " j/k",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":move  "),
            Span::styled(
                "Enter",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":select  "),
            Span::styled(
                "Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":close"),
        ])
    }
}

fn main_help_line(app: &TuiApp, width: u16) -> Line<'static> {
    if width < 72 {
        if matches!(app.current_state.run_mode, RunMode::Precision) {
            Line::from("q quit  1-5 tabs  b browse  c codec  d delete  j/k scroll")
        } else {
            Line::from("q quit  1-5 tabs  b browse  j/k scroll  f log-filter")
        }
    } else {
        let mut spans = vec![
            Span::styled(
                " q",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":quit  "),
            Span::styled(
                "1-5",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":tabs  "),
            Span::styled(
                "b",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":browse  "),
        ];
        if matches!(app.current_state.run_mode, RunMode::Precision) {
            spans.extend([
                Span::styled(
                    "c",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(":codec  "),
                Span::styled(
                    "d",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(":delete queue row  "),
            ]);
        }
        spans.extend([
            Span::styled(
                "j/k",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":scroll  "),
        ]);
        if !matches!(app.current_state.run_mode, RunMode::Precision) {
            spans.extend([
                Span::styled(
                    "f",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(":log-filter"),
            ]);
        }
        Line::from(spans)
    }
}

fn render_browser_modal(
    f: &mut Frame,
    area: Rect,
    browser: &mut BrowserState,
    selection_progress: Option<&(ManualSelectionProgress, Instant)>,
    selected_preset: &PresetSnapshot,
) {
    let modal_layout = browser_modal_layout(area, selection_progress.is_some());
    let popup = modal_layout.popup;
    f.render_widget(Clear, popup);

    let title = if let Some(dir) = browser.current_dir.as_ref() {
        format!(
            " Browse {} ",
            truncate_left(
                &dir.display().to_string(),
                popup.width.saturating_sub(14) as usize
            )
        )
    } else {
        " Browse Shares ".to_string()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::LightBlue));
    let inner = block.inner(popup);
    f.render_widget(block, popup);
    let sections = if selection_progress.is_some() && inner.height > 2 {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(inner)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(inner)
    };
    let list_area = modal_layout.list;
    let progress_area = if selection_progress.is_some() && sections.len() > 2 {
        Some(sections[1])
    } else {
        None
    };
    let preset_area = *sections.last().unwrap_or(&list_area);
    let visible_rows = list_area.height as usize;
    browser.ensure_visible(visible_rows);

    let items = browser
        .visible_entries(visible_rows)
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let absolute_idx = browser.scroll_offset + idx;
            let selected = browser.selected_targets.contains(&entry.path);
            let marker = if selected { "[+]" } else { "[ ]" };
            let mut style = if entry.available {
                Style::default()
            } else {
                Style::default().fg(Color::DarkGray)
            };
            if absolute_idx == browser.selected_index {
                style = style
                    .fg(Color::White)
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD);
            }
            ListItem::new(Line::from(Span::styled(
                truncate_right(
                    &format!("{} {}", marker, entry.label),
                    list_area.width as usize,
                ),
                style,
            )))
        })
        .collect::<Vec<_>>();
    f.render_widget(List::new(items), list_area);

    if let (Some((progress, started_at)), Some(progress_area)) = (selection_progress, progress_area)
    {
        let line = manual_selection_browser_line(progress, activity_spinner(*started_at));
        f.render_widget(
            Paragraph::new(truncate_right(&line, progress_area.width as usize))
                .style(Style::default().fg(Color::DarkGray)),
            progress_area,
        );
    }

    let preset_line = format!("Preset: {}", selected_preset.short_label());
    f.render_widget(
        Paragraph::new(truncate_right(&preset_line, preset_area.width as usize))
            .style(Style::default().fg(Color::DarkGray)),
        preset_area,
    );
}

fn render_preset_selector_modal(
    f: &mut Frame,
    area: Rect,
    selector: &mut PresetSelectorState,
    selected_preset: &PresetSnapshot,
) {
    let popup = centered_rect(84, 72, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(" Codec Selector ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(2)])
        .split(inner);
    let content = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
        .split(chunks[0]);

    let list_area = content[0];
    let details_area = content[1];
    let footer_area = chunks[1];
    let visible_rows = list_area.height as usize;
    selector.ensure_visible(visible_rows);

    let items = selector
        .visible_entries(visible_rows)
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let absolute_idx = selector.scroll_offset + idx;
            let mut style = if entry.selectable {
                Style::default()
            } else {
                Style::default().fg(Color::DarkGray)
            };
            if absolute_idx == selector.selected_index {
                style = style
                    .fg(Color::White)
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD);
            }
            let marker = if entry.snapshot().as_ref() == Some(selected_preset) {
                "*"
            } else {
                " "
            };
            ListItem::new(Line::from(Span::styled(
                truncate_right(
                    &format!("{} {}", marker, entry.label()),
                    list_area.width as usize,
                ),
                style,
            )))
        })
        .collect::<Vec<_>>();
    f.render_widget(List::new(items), list_area);

    let details = selector
        .current_entry()
        .map(|entry| {
            let mut lines = vec![
                format!("File: {}", entry.preset_file),
                format!("Preset: {}", entry.preset_name),
            ];
            if let Some(target_codec) = entry.target_codec.as_deref() {
                lines.push(format!("Target codec: {}", target_codec));
            }
            lines.push(format!("Encoder: {}", entry.encoder));
            lines.push(format!("Quality/CRF: {}", entry.quality_summary));
            lines.push(format!("Speed preset: {}", entry.speed_preset));
            lines.push(format!("Subtitles: {}", entry.subtitle_summary));
            lines.push(format!("Audio: {}", entry.audio_summary));
            if let Some(error) = entry.error_summary.as_deref() {
                lines.push(format!("Status: disabled ({})", error));
            } else {
                lines.push("Status: selectable".to_string());
            }
            lines.join("\n")
        })
        .unwrap_or_else(|| "No preset entries found.".to_string());
    f.render_widget(
        Paragraph::new(details)
            .block(
                Block::default()
                    .title(" Details ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray)),
            )
            .wrap(ratatui::widgets::Wrap { trim: true }),
        details_area,
    );

    let footer = format!(
        "Selected preset: {}",
        truncate_right(&selected_preset.short_label(), footer_area.width as usize)
    );
    f.render_widget(
        Paragraph::new(footer).style(Style::default().fg(Color::DarkGray)),
        footer_area,
    );
}

fn render_quit_modal(f: &mut Frame, area: Rect, app: &TuiApp) {
    let layout = quit_modal_layout(area);
    let popup = layout.popup;
    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(" Quit ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    f.render_widget(block, popup);

    let pid_text = app
        .quit_kill_state
        .as_ref()
        .map(|state| {
            format!(
                "Stopping worker {}. Rsync can take time to flush and exit cleanly; this window will close automatically when shutdown finishes.",
                state.pid
            )
        })
        .or_else(|| {
            app.worker_pid.map(|pid| {
                format!(
                    "Worker {} is still running. Background leaves transcoding active; Kill stops the worker and clears the queue after it exits.",
                    pid
                )
            })
        })
        .unwrap_or_else(|| "Worker status is unavailable.".to_string());
    f.render_widget(
        Paragraph::new(pid_text).wrap(ratatui::widgets::Wrap { trim: true }),
        layout.message,
    );

    for (choice, rect, label) in quit_button_rects(app, layout.buttons) {
        let selected = app.quit_choice == choice;
        let disabled = app.quit_kill_state.is_some()
            && matches!(
                choice,
                QuitChoice::Background | QuitChoice::Kill | QuitChoice::Cancel
            );
        let border_style = if selected {
            Style::default().fg(Color::Yellow)
        } else if disabled {
            Style::default().fg(Color::DarkGray)
        } else {
            Style::default().fg(Color::Gray)
        };
        let text_style = if selected {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else if disabled {
            Style::default().fg(Color::Gray)
        } else {
            Style::default().fg(Color::White)
        };
        let button_block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);
        let button_inner = button_block.inner(rect);
        f.render_widget(button_block, rect);
        if button_inner.width > 0 && button_inner.height > 0 {
            f.render_widget(
                Paragraph::new(label)
                    .style(text_style)
                    .alignment(Alignment::Center),
                button_inner,
            );
        }
    }

    f.render_widget(
        Paragraph::new(app.attach_hint.as_str())
            .block(
                Block::default()
                    .title(" Reattach ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray)),
            )
            .wrap(ratatui::widgets::Wrap { trim: true }),
        layout.command,
    );

    let footer_text = if app.quit_kill_state.is_some() {
        "Keys: y copy command. Waiting for the worker to exit."
    } else {
        "Keys: b background, k kill, y copy command, c cancel, Enter confirm."
    };
    f.render_widget(
        Paragraph::new(footer_text)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center),
        layout.footer,
    );
}

struct QuitModalLayout {
    popup: Rect,
    message: Rect,
    buttons: Rect,
    command: Rect,
    footer: Rect,
}

struct BrowserModalLayout {
    popup: Rect,
    list: Rect,
}

fn quit_modal_layout(area: Rect) -> QuitModalLayout {
    let popup = centered_rect(78, 44, area);
    let inner = Block::default().borders(Borders::ALL).inner(popup);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(3),
            Constraint::Min(4),
            Constraint::Length(2),
        ])
        .split(inner);

    QuitModalLayout {
        popup,
        message: chunks[0],
        buttons: chunks[1],
        command: chunks[2],
        footer: chunks[3],
    }
}

fn browser_modal_layout(area: Rect, show_progress: bool) -> BrowserModalLayout {
    let popup = centered_rect(80, 80, area);
    let inner = Block::default().borders(Borders::ALL).inner(popup);
    let sections = if show_progress && inner.height > 2 {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(inner)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(inner)
    };

    BrowserModalLayout {
        popup,
        list: sections[0],
    }
}

fn quit_button_rects(app: &TuiApp, area: Rect) -> Vec<(QuitChoice, Rect, &'static str)> {
    let buttons = [
        (QuitChoice::Background, "Background"),
        (
            QuitChoice::Kill,
            if app.quit_kill_state.is_some() {
                "Stopping"
            } else {
                "Kill"
            },
        ),
        (QuitChoice::Copy, "Copy Cmd"),
        (QuitChoice::Cancel, "Cancel"),
    ];
    let gap = 1u16;
    let total_width = buttons.iter().fold(0u16, |acc, (_, label)| {
        acc.saturating_add(label.len() as u16 + 4)
    }) + gap.saturating_mul(buttons.len().saturating_sub(1) as u16);
    let start_x = area.x + area.width.saturating_sub(total_width) / 2;
    let mut cursor_x = start_x;

    buttons
        .into_iter()
        .map(|(choice, label)| {
            let width = label.len() as u16 + 4;
            let rect = Rect {
                x: cursor_x,
                y: area.y,
                width,
                height: area.height,
            };
            cursor_x = cursor_x.saturating_add(width + gap);
            (choice, rect, label)
        })
        .collect()
}

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x
        && x < rect.x.saturating_add(rect.width)
        && y >= rect.y
        && y < rect.y.saturating_add(rect.height)
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn truncate_right(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let len = text.chars().count();
    if len <= width {
        return text.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut out = text.chars().take(width - 3).collect::<String>();
    out.push_str("...");
    out
}

fn truncate_left(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let len = text.chars().count();
    if len <= width {
        return text.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let suffix = text.chars().skip(len - (width - 3)).collect::<String>();
    format!("...{}", suffix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::NewQueueItem;
    use crate::state::RunMode;
    use ratatui::{backend::TestBackend, Terminal};
    use std::process::Command;

    #[test]
    fn browser_state_scrolls_to_keep_selection_visible() {
        let mut browser = BrowserState {
            current_dir: None,
            entries: (0..10)
                .map(|idx| BrowserEntry {
                    path: PathBuf::from(format!("/tmp/{}", idx)),
                    label: format!("Item {}", idx),
                    is_dir: true,
                    available: true,
                })
                .collect(),
            selected_index: 0,
            scroll_offset: 0,
            selected_targets: BTreeSet::new(),
        };

        browser.selected_index = 6;
        browser.ensure_visible(4);
        assert_eq!(browser.scroll_offset, 3);

        browser.selected_index = 2;
        browser.ensure_visible(4);
        assert_eq!(browser.scroll_offset, 2);
        assert_eq!(browser.visible_entries(4).len(), 4);
    }

    #[test]
    fn truncate_helpers_add_ellipsis_when_needed() {
        assert_eq!(truncate_right("abcdef", 4), "a...");
        assert_eq!(truncate_left("abcdef", 4), "...f");
        assert_eq!(truncate_right("abc", 8), "abc");
        assert_eq!(truncate_left("abc", 8), "abc");
    }

    #[test]
    fn open_preset_selector_rejects_non_precision_mode() {
        let mut app = TuiApp::new(
            &Config::default_config(),
            PathBuf::from("/tmp/watchdog"),
            String::new(),
        );
        app.current_state.run_mode = RunMode::Watchdog;

        open_preset_selector(&mut app);

        assert!(app.preset_selector.is_none());
        assert_eq!(
            app.status_message.as_deref(),
            Some("Codec selector is only available in precision mode.")
        );
    }

    #[test]
    fn browser_help_line_only_shows_codec_shortcut_in_precision_mode() {
        let mut app = TuiApp::new(
            &Config::default_config(),
            PathBuf::from("/tmp/watchdog"),
            String::new(),
        );

        app.current_state.run_mode = RunMode::Watchdog;
        let watchdog_help = browser_help_line(&app, 120);
        let watchdog_text = watchdog_help
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(!watchdog_text.contains(":codec"));

        app.current_state.run_mode = RunMode::Precision;
        let precision_help = browser_help_line(&app, 120);
        let precision_text = precision_help
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(precision_text.contains(":codec"));
    }

    #[test]
    fn quit_modal_renders_button_labels() {
        let backend = TestBackend::new(160, 48);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = TuiApp::new(
            &Config::default_config(),
            PathBuf::from("/tmp/watchdog"),
            "watchdog --attach --config .watchdog/watchdog.toml".to_string(),
        );
        app.show_quit_modal = true;
        app.worker_pid = Some(88529);
        app.quit_choice = QuitChoice::Kill;
        app.quit_kill_state = Some(QuitKillState {
            pid: 88529,
            started_at: Instant::now(),
            followup_signal_sent: false,
        });

        terminal
            .draw(|f| render_quit_modal(f, f.area(), &app))
            .unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>()
            .join("");
        assert!(rendered.contains("Stopping"));
        assert!(rendered.contains("Copy Cmd"));
        assert!(rendered.contains("Cancel"));
    }

    #[test]
    fn mouse_scroll_moves_browser_selection() {
        let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
        let mut app = TuiApp::new(
            &Config::default_config(),
            PathBuf::from("/tmp/watchdog"),
            String::new(),
        );
        app.browser = Some(BrowserState {
            current_dir: None,
            entries: (0..8)
                .map(|idx| BrowserEntry {
                    path: PathBuf::from(format!("/tmp/{}", idx)),
                    label: format!("Item {}", idx),
                    is_dir: true,
                    available: true,
                })
                .collect(),
            selected_index: 0,
            scroll_offset: 0,
            selected_targets: BTreeSet::new(),
        });

        let area = Rect::new(0, 0, 120, 40);
        let list = browser_modal_layout(area, false).list;
        let pointer_x = list.x.saturating_add(1);
        let pointer_y = list.y.saturating_add(1);

        handle_mouse_event(
            &mut app,
            &db,
            Path::new("/tmp/watchdog.events.ndjson"),
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: pointer_x,
                row: pointer_y,
                modifiers: event::KeyModifiers::NONE,
            },
            area,
        )
        .unwrap();

        assert_eq!(app.browser.as_ref().unwrap().selected_index, 1);

        handle_mouse_event(
            &mut app,
            &db,
            Path::new("/tmp/watchdog.events.ndjson"),
            MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: pointer_x,
                row: pointer_y,
                modifiers: event::KeyModifiers::NONE,
            },
            area,
        )
        .unwrap();

        assert_eq!(app.browser.as_ref().unwrap().selected_index, 0);
    }

    #[test]
    fn poll_owned_worker_reaps_exited_child_and_clears_worker_state() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let child = Command::new("sh").args(["-c", "exit 0"]).spawn().unwrap();
        let pid = child.id();
        let mut app = TuiApp::new(
            &Config::default_config(),
            PathBuf::from("/tmp/watchdog"),
            String::new(),
        );
        db.set_worker_state(pid, "precision");
        app.owned_worker = Some(child);

        std::thread::sleep(Duration::from_millis(50));

        let service_state = db.get_service_state();
        let worker_pid = poll_owned_worker(&mut app, &db, &service_state);
        let cleared_state = db.get_service_state();

        assert!(worker_pid.is_none());
        assert!(app.owned_worker.is_none());
        assert!(cleared_state.worker_pid.is_none());
        assert!(cleared_state.worker_run_mode.is_none());
        assert!(app
            .status_message
            .as_deref()
            .is_some_and(|message| message.contains("exited")));
    }

    #[test]
    fn state_from_runtime_prefers_live_queue_total_after_queue_edits() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.enqueue_queue_items(&[
            NewQueueItem {
                source_path: "/mnt/movies/A.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: "{}".to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/B.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: "{}".to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/C.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: "{}".to_string(),
            },
        ]);

        let state = state_from_runtime(
            Some(json!({
                "queue_position": 4,
                "queue_total": 10,
            })),
            &db.get_service_state(),
            &db,
            Path::new("/tmp/watchdog.events.ndjson"),
            Some(12345),
        );

        assert_eq!(state.queue_position, 4);
        assert_eq!(state.queue_total, 4);
    }
}
