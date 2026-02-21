pub mod dashboard_tab;
pub mod history_tab;
pub mod logs_tab;
pub mod widgets;

use crate::db::WatchdogDb;
use crate::state::AppState;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
    Frame, Terminal,
};
use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Dashboard,
    Logs,
    History,
}

impl Tab {
    fn titles() -> Vec<&'static str> {
        vec!["1 Dashboard", "2 Logs", "3 History"]
    }

    fn index(&self) -> usize {
        match self {
            Tab::Dashboard => 0,
            Tab::Logs => 1,
            Tab::History => 2,
        }
    }

    fn next(&self) -> Tab {
        match self {
            Tab::Dashboard => Tab::Logs,
            Tab::Logs => Tab::History,
            Tab::History => Tab::Dashboard,
        }
    }
}

struct TuiApp {
    current_tab: Tab,
    logs_state: logs_tab::LogsTabState,
    history_state: history_tab::HistoryTabState,
    last_history_refresh: Instant,
}

impl TuiApp {
    fn new() -> Self {
        Self {
            current_tab: Tab::Dashboard,
            logs_state: logs_tab::LogsTabState::default(),
            history_state: history_tab::HistoryTabState::default(),
            last_history_refresh: Instant::now() - Duration::from_secs(10), // Force initial refresh
        }
    }
}

/// Run the TUI event loop.
pub async fn run_tui(
    mut state_rx: watch::Receiver<AppState>,
    db: Arc<WatchdogDb>,
    shutdown_tx: broadcast::Sender<()>,
) -> anyhow::Result<()> {
    // Install a panic hook that restores the terminal before printing the panic.
    // Without this, a panic leaves the terminal in raw mode with no echo.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = stdout().execute(LeaveAlternateScreen);
        original_hook(info);
    }));

    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp::new();
    let tick_rate = Duration::from_millis(50); // ~20fps

    let result = run_tui_loop(&mut terminal, &mut app, &mut state_rx, &db, tick_rate).await;

    // Restore terminal
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;

    // Signal shutdown
    let _ = shutdown_tx.send(());

    result
}

async fn run_tui_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    app: &mut TuiApp,
    state_rx: &mut watch::Receiver<AppState>,
    db: &WatchdogDb,
    tick_rate: Duration,
) -> anyhow::Result<()> {
    loop {
        // Get latest state
        let state = state_rx.borrow().clone();

        // Refresh history periodically
        if app.last_history_refresh.elapsed() > Duration::from_secs(5) {
            app.history_state.refresh(db);
            app.last_history_refresh = Instant::now();
        }

        // Draw
        terminal.draw(|f| draw_ui(f, app, &state))?;

        // Handle events with timeout
        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('1') => app.current_tab = Tab::Dashboard,
                    KeyCode::Char('2') => app.current_tab = Tab::Logs,
                    KeyCode::Char('3') => app.current_tab = Tab::History,
                    KeyCode::Tab => app.current_tab = app.current_tab.next(),
                    KeyCode::Char('j') | KeyCode::Down => match app.current_tab {
                        Tab::Logs => app.logs_state.scroll_down(),
                        Tab::History => app.history_state.scroll_down(),
                        _ => {}
                    },
                    KeyCode::Char('k') | KeyCode::Up => match app.current_tab {
                        Tab::Logs => app.logs_state.scroll_up(),
                        Tab::History => app.history_state.scroll_up(),
                        _ => {}
                    },
                    KeyCode::Home => match app.current_tab {
                        Tab::Logs => app.logs_state.scroll_to_top(),
                        Tab::History => app.history_state.scroll_to_top(),
                        _ => {}
                    },
                    KeyCode::End => match app.current_tab {
                        Tab::Logs => app.logs_state.scroll_to_bottom(),
                        Tab::History => app.history_state.scroll_to_bottom(),
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
    }
}

fn draw_ui(f: &mut Frame, app: &mut TuiApp, state: &AppState) {
    let size = f.area();

    let outer_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title + tabs
            Constraint::Min(10),   // Content
            Constraint::Length(1), // Footer
        ])
        .split(size);

    // Title bar with tabs
    render_title_bar(f, outer_chunks[0], app);

    // Main content area
    match app.current_tab {
        Tab::Dashboard => dashboard_tab::render_dashboard(f, outer_chunks[1], state),
        Tab::Logs => logs_tab::render_logs(f, outer_chunks[1], state, &mut app.logs_state),
        Tab::History => history_tab::render_history(f, outer_chunks[1], &mut app.history_state),
    }

    // Footer help
    render_footer(f, outer_chunks[2]);
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
        .map(|t| Line::from(Span::raw(*t)))
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

fn render_footer(f: &mut Frame, area: Rect) {
    let help = Line::from(vec![
        Span::styled(
            " q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":quit  "),
        Span::styled(
            "1-3",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":tabs  "),
        Span::styled(
            "Tab",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":next  "),
        Span::styled(
            "j/k",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":scroll  "),
        Span::styled(
            "Home/End",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(":top/bottom"),
    ]);

    f.render_widget(Paragraph::new(help), area);
}
