pub mod cooldown_tab;
pub mod dashboard_tab;
pub mod history_tab;
pub mod logs_tab;
pub mod queue_tab;
pub mod widgets;

use crate::config::Config;
use crate::db::{ServiceState, WatchdogDb};
use crate::event_journal::{append_event, append_log_event, read_recent_log_lines};
use crate::probe::FfprobeProber;
use crate::scanner::RealFileSystem;
use crate::selection::enqueue_manual_paths;
use crate::state::{AppState, PipelinePhase, ProgressStage, RunMode};
use crate::status_snapshot::read_snapshot_file;
use crate::traits::FileSystem;
use crate::util::{pid_is_running, wait_for_pid_exit};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
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
    selected_targets: BTreeSet<PathBuf>,
}

impl BrowserState {
    fn new(config: &Config) -> Self {
        let mut browser = Self {
            current_dir: None,
            entries: Vec::new(),
            selected_index: 0,
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
            if self.current_dir.as_ref().is_some_and(|dir| dir == &share_root.parent().unwrap_or(Path::new("/")).to_path_buf() && !dir.starts_with(&share_root)) {
                self.current_dir = Some(share_root);
            }
        }
        self.selected_index = 0;
        self.refresh_entries(config);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuitChoice {
    Background,
    Kill,
    Cancel,
}

impl QuitChoice {
    fn next(self) -> Self {
        match self {
            Self::Background => Self::Kill,
            Self::Kill => Self::Cancel,
            Self::Cancel => Self::Background,
        }
    }

    fn prev(self) -> Self {
        match self {
            Self::Background => Self::Cancel,
            Self::Kill => Self::Background,
            Self::Cancel => Self::Kill,
        }
    }
}

struct TuiApp {
    current_tab: Tab,
    logs_state: logs_tab::LogsTabState,
    history_state: history_tab::HistoryTabState,
    cooldown_state: cooldown_tab::CooldownTabState,
    queue_state: queue_tab::QueueTabState,
    last_db_refresh: Instant,
    last_snapshot_refresh: Instant,
    current_state: AppState,
    browser: Option<BrowserState>,
    quit_choice: QuitChoice,
    show_quit_modal: bool,
    worker_pid: Option<u32>,
    auto_opened_browser: bool,
    status_message: Option<String>,
    attach_hint: String,
}

impl TuiApp {
    fn new(attach_hint: String) -> Self {
        Self {
            current_tab: Tab::Dashboard,
            logs_state: logs_tab::LogsTabState::default(),
            history_state: history_tab::HistoryTabState::default(),
            cooldown_state: cooldown_tab::CooldownTabState::default(),
            queue_state: queue_tab::QueueTabState::default(),
            last_db_refresh: Instant::now() - Duration::from_secs(10),
            last_snapshot_refresh: Instant::now() - Duration::from_secs(10),
            current_state: AppState::default(),
            browser: None,
            quit_choice: QuitChoice::Background,
            show_quit_modal: false,
            worker_pid: None,
            auto_opened_browser: false,
            status_message: None,
            attach_hint,
        }
    }
}

pub async fn run_tui(
    config: Config,
    db: Arc<WatchdogDb>,
    snapshot_path: PathBuf,
    event_journal_path: PathBuf,
    attach_hint: String,
) -> anyhow::Result<()> {
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = stdout().execute(LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp::new(attach_hint);
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
    stdout().execute(LeaveAlternateScreen)?;
    result
}

async fn run_tui_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    app: &mut TuiApp,
    config: &Config,
    db: &WatchdogDb,
    snapshot_path: &Path,
    event_journal_path: &Path,
    tick_rate: Duration,
) -> anyhow::Result<()> {
    loop {
        refresh_runtime_state(app, db, snapshot_path, event_journal_path);

        if app.last_db_refresh.elapsed() > Duration::from_secs(2) {
            app.history_state.refresh(db);
            app.cooldown_state.refresh(db);
            app.queue_state.refresh(db);
            app.last_db_refresh = Instant::now();
        }

        if !app.auto_opened_browser
            && matches!(app.current_state.run_mode, RunMode::Precision)
            && db.get_queue_count() == 0
        {
            app.browser = Some(BrowserState::new(config));
            app.auto_opened_browser = true;
        }

        terminal.draw(|f| draw_ui(f, app))?;

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if handle_key_event(app, config, db, event_journal_path, key.code)? {
                    return Ok(());
                }
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
    let worker_pid = service_state.worker_pid.filter(|pid| pid_is_running(*pid));
    if worker_pid.is_none() && service_state.worker_pid.is_some() {
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

fn state_from_runtime(
    snapshot: Option<Value>,
    service_state: &ServiceState,
    db: &WatchdogDb,
    event_journal_path: &Path,
    worker_pid: Option<u32>,
) -> AppState {
    let mut state = AppState::default();
    state.run_mode = match service_state.worker_run_mode.as_deref() {
        Some("precision") => RunMode::Precision,
        _ => RunMode::Watchdog,
    };
    state.queue_total = db.get_queue_count().max(0) as u32;
    state.consecutive_pass_failures = service_state.consecutive_pass_failures;
    state.auto_paused = service_state.auto_paused_at.is_some();
    state.auto_pause_reason = service_state.auto_pause_reason.clone();
    state.auto_paused_at = service_state.auto_paused_at.clone();
    state.quarantined_files = db.quarantine_count().max(0) as u64;

    if let Some(snapshot) = snapshot.as_ref() {
        if let Some(phase) = snapshot.get("phase").and_then(Value::as_str) {
            state.phase = parse_phase(phase);
        }
        state.nfs_healthy = snapshot
            .get("nfs_healthy")
            .and_then(Value::as_bool)
            .unwrap_or(state.nfs_healthy);
        state.simulate_mode = snapshot
            .get("simulate_mode")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if let Some(run_mode) = snapshot.get("run_mode").and_then(Value::as_str) {
            state.run_mode = if run_mode.eq_ignore_ascii_case("precision") {
                RunMode::Precision
            } else {
                RunMode::Watchdog
            };
        }
        state.queue_position = snapshot
            .get("queue_position")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        state.queue_total = snapshot
            .get("queue_total")
            .and_then(Value::as_u64)
            .unwrap_or(state.queue_total as u64) as u32;
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
            state.total_inspected = totals
                .get("inspected")
                .and_then(Value::as_u64)
                .unwrap_or(0);
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
                .unwrap_or(state.consecutive_pass_failures as u64) as u32;
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
                items.iter()
                    .filter_map(|item| {
                        let pair = item.as_array()?;
                        Some((
                            pair.first()?.as_str()?.to_string(),
                            pair.get(1)?.as_u64()?,
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        state.share_health = snapshot
            .get("share_health")
            .and_then(Value::as_array)
            .map(|items| {
                items.iter()
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

    state.log_lines = read_recent_log_lines(event_journal_path, 500)
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

fn handle_key_event(
    app: &mut TuiApp,
    config: &Config,
    db: &WatchdogDb,
    event_journal_path: &Path,
    code: KeyCode,
) -> anyhow::Result<bool> {
    if app.show_quit_modal {
        return handle_quit_modal_key(app, db, event_journal_path, code);
    }

    if app.browser.is_some() {
        return handle_browser_key(app, config, db, event_journal_path, code);
    }

    match code {
        KeyCode::Char('q') | KeyCode::Esc => {
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
        KeyCode::Char('b') => app.browser = Some(BrowserState::new(config)),
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
    db: &WatchdogDb,
    event_journal_path: &Path,
    code: KeyCode,
) -> anyhow::Result<bool> {
    let Some(browser) = app.browser.as_mut() else {
        return Ok(false);
    };

    match code {
        KeyCode::Esc => app.browser = None,
        KeyCode::Char('j') | KeyCode::Down => browser.move_down(),
        KeyCode::Char('k') | KeyCode::Up => browser.move_up(),
        KeyCode::Enter => browser.descend_or_toggle(config),
        KeyCode::Char(' ') => browser.toggle_current_target(),
        KeyCode::Backspace => browser.go_up(config),
        KeyCode::Char('a') => {
            let selected = browser
                .selected_targets
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            let summary = enqueue_manual_paths(config, &RealFileSystem, &FfprobeProber, db, &selected);
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
            app.show_quit_modal = false;
            return Ok(false);
        }
        KeyCode::Left | KeyCode::Char('h') => app.quit_choice = app.quit_choice.prev(),
        KeyCode::Right | KeyCode::Char('l') | KeyCode::Tab => {
            app.quit_choice = app.quit_choice.next()
        }
        KeyCode::Char('b') => app.quit_choice = QuitChoice::Background,
        KeyCode::Char('k') => app.quit_choice = QuitChoice::Kill,
        KeyCode::Char('c') => app.quit_choice = QuitChoice::Cancel,
        KeyCode::Enter | KeyCode::Char('q') => {}
        _ => return Ok(false),
    }

    if !matches!(code, KeyCode::Enter | KeyCode::Char('q') | KeyCode::Char('b') | KeyCode::Char('k') | KeyCode::Char('c')) {
        return Ok(false);
    }

    match app.quit_choice {
        QuitChoice::Background => Ok(true),
        QuitChoice::Cancel => {
            app.show_quit_modal = false;
            Ok(false)
        }
        QuitChoice::Kill => {
            if let Some(pid) = app.worker_pid {
                if pid_is_running(pid) {
                    kill(Pid::from_raw(pid as i32), Signal::SIGTERM)?;
                    if !wait_for_pid_exit(pid, Duration::from_secs(15)) {
                        app.status_message =
                            Some(format!("Timed out waiting for worker {} to exit", pid));
                        app.show_quit_modal = false;
                        return Ok(false);
                    }
                }
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
            }
            Ok(true)
        }
    }
}

fn format_manual_selection_message(
    summary: &crate::selection::ManualSelectionSummary,
) -> String {
    format!(
        "Manual selection: added={} duplicates={} ineligible={} cooldown={} quarantined={} young={} missing={} non_video={} probe_failed={}",
        summary.enqueued_files,
        summary.skipped_duplicates,
        summary.skipped_ineligible,
        summary.skipped_cooldown,
        summary.skipped_quarantined,
        summary.skipped_young,
        summary.skipped_missing,
        summary.skipped_non_video,
        summary.skipped_probe_failed,
    )
}

fn draw_ui(f: &mut Frame, app: &mut TuiApp) {
    let size = f.area();
    let outer_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(10), Constraint::Length(2)])
        .split(size);

    render_title_bar(f, outer_chunks[0], app);
    match app.current_tab {
        Tab::Dashboard => dashboard_tab::render_dashboard(f, outer_chunks[1], &app.current_state),
        Tab::Queue => queue_tab::render_queue(f, outer_chunks[1], &mut app.queue_state),
        Tab::Logs => logs_tab::render_logs(f, outer_chunks[1], &app.current_state, &mut app.logs_state),
        Tab::History => history_tab::render_history(f, outer_chunks[1], &mut app.history_state),
        Tab::Cooldown => cooldown_tab::render_cooldown(f, outer_chunks[1], &mut app.cooldown_state),
    }
    render_footer(f, outer_chunks[2], app);

    if let Some(browser) = app.browser.as_ref() {
        render_browser_modal(f, size, app, browser);
    }
    if app.show_quit_modal {
        render_quit_modal(f, size, app);
    }
}

fn render_title_bar(f: &mut Frame, area: Rect, app: &TuiApp) {
    let block = Block::default()
        .title(" Jellyfin AV1 Transcoding Watchdog ")
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
    let help = if app.browser.is_some() {
        Line::from(vec![
            Span::styled(" j/k", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":move  "),
            Span::styled("Enter", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":open/toggle  "),
            Span::styled("Space", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":select  "),
            Span::styled("Backspace", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":up  "),
            Span::styled("a", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":add queue  "),
            Span::styled("Esc", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":close"),
        ])
    } else {
        Line::from(vec![
            Span::styled(" q", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":quit  "),
            Span::styled("1-5", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":tabs  "),
            Span::styled("b", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":browse  "),
            Span::styled("j/k", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":scroll  "),
            Span::styled("f", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(":log-filter"),
        ])
    };

    let footer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);
    f.render_widget(Paragraph::new(help), footer[0]);

    let status_line = app
        .status_message
        .clone()
        .unwrap_or_else(|| format!("Reattach with {}", app.attach_hint));
    f.render_widget(
        Paragraph::new(status_line).style(Style::default().fg(Color::DarkGray)),
        footer[1],
    );
}

fn render_browser_modal(f: &mut Frame, area: Rect, app: &TuiApp, browser: &BrowserState) {
    let popup = centered_rect(80, 80, area);
    f.render_widget(Clear, popup);

    let title = if let Some(dir) = browser.current_dir.as_ref() {
        format!(" Browse {} ", dir.display())
    } else {
        " Browse Shares ".to_string()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::LightBlue));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    let items = browser
        .entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let selected = browser.selected_targets.contains(&entry.path);
            let marker = if selected { "[+]" } else { "[ ]" };
            let mut style = if entry.available {
                Style::default()
            } else {
                Style::default().fg(Color::DarkGray)
            };
            if idx == browser.selected_index {
                style = style
                    .fg(Color::White)
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD);
            }
            ListItem::new(Line::from(Span::styled(
                format!("{} {}", marker, entry.label),
                style,
            )))
        })
        .collect::<Vec<_>>();
    f.render_widget(List::new(items), inner);

    let hint = Paragraph::new(
        "Enter: open dir / toggle file   Space: select file or folder   Backspace: up   a: add to queue   Esc: cancel",
    )
    .alignment(Alignment::Center)
    .style(Style::default().fg(Color::DarkGray));
    let hint_area = Rect {
        x: popup.x + 1,
        y: popup.y + popup.height.saturating_sub(2),
        width: popup.width.saturating_sub(2),
        height: 1,
    };
    f.render_widget(hint, hint_area);

    let _ = app;
}

fn render_quit_modal(f: &mut Frame, area: Rect, app: &TuiApp) {
    let popup = centered_rect(60, 30, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(" Quit ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    f.render_widget(block, popup);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Length(2)])
        .split(inner);

    let pid_text = app
        .worker_pid
        .map(|pid| format!("Worker {} is still running.", pid))
        .unwrap_or_else(|| "Worker status is unavailable.".to_string());
    f.render_widget(
        Paragraph::new(format!(
            "{} Background leaves transcoding running. Kill stops the worker and clears the queue.",
            pid_text
        ))
        .wrap(ratatui::widgets::Wrap { trim: true }),
        chunks[0],
    );

    let buttons = [
        (QuitChoice::Background, "Background"),
        (QuitChoice::Kill, "Kill"),
        (QuitChoice::Cancel, "Cancel"),
    ]
    .iter()
    .map(|(choice, label)| {
        let selected = app.quit_choice == *choice;
        Span::styled(
            format!(" {} ", label),
            if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            },
        )
    })
    .collect::<Vec<_>>();
    f.render_widget(
        Paragraph::new(Line::from(buttons)).alignment(Alignment::Center),
        chunks[1],
    );

    f.render_widget(
        Paragraph::new(format!(
            "Reopen later with {}. Keys: b background, k kill, c cancel, Enter confirm.",
            app.attach_hint
        ))
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Center),
        chunks[2],
    );
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
