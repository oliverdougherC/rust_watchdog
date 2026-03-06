use crate::state::AppState;
use crate::tui::widgets::log_level_color;
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
    Frame,
};

pub struct LogsTabState {
    pub list_state: ListState,
    pub auto_scroll: bool,
    pub total_lines: usize,
    pub filter: LogFilter,
}

#[derive(Debug, Clone, Copy)]
pub enum LogFilter {
    All,
    WarnAndError,
    ErrorOnly,
}

impl LogFilter {
    fn next(self) -> Self {
        match self {
            Self::All => Self::WarnAndError,
            Self::WarnAndError => Self::ErrorOnly,
            Self::ErrorOnly => Self::All,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::WarnAndError => "warn+error",
            Self::ErrorOnly => "error",
        }
    }

    fn keep(self, line: &str) -> bool {
        match self {
            Self::All => true,
            Self::WarnAndError => line.contains("WARN") || line.contains("ERROR"),
            Self::ErrorOnly => line.contains("ERROR") || line.contains("CRITICAL"),
        }
    }
}

impl Default for LogsTabState {
    fn default() -> Self {
        Self {
            list_state: ListState::default(),
            auto_scroll: true,
            total_lines: 0,
            filter: LogFilter::All,
        }
    }
}

impl LogsTabState {
    pub fn scroll_up(&mut self) {
        self.auto_scroll = false;
        let current = self.list_state.selected().unwrap_or(0);
        if current > 0 {
            self.list_state.select(Some(current - 1));
        }
    }

    pub fn scroll_down(&mut self) {
        let current = self.list_state.selected().unwrap_or(0);
        if current + 1 < self.total_lines {
            self.list_state.select(Some(current + 1));
        }
        // Re-enable auto-scroll if we're at the bottom
        if self.list_state.selected().unwrap_or(0) + 1 >= self.total_lines {
            self.auto_scroll = true;
        }
    }

    pub fn scroll_to_top(&mut self) {
        self.auto_scroll = false;
        self.list_state.select(Some(0));
    }

    pub fn scroll_to_bottom(&mut self) {
        self.auto_scroll = true;
        if self.total_lines > 0 {
            self.list_state.select(Some(self.total_lines - 1));
        }
    }

    pub fn cycle_filter(&mut self) {
        self.filter = self.filter.next();
        self.auto_scroll = true;
    }
}

pub fn render_logs(f: &mut Frame, area: Rect, state: &AppState, tab_state: &mut LogsTabState) {
    let block = Block::default()
        .title(format!(" Logs [{} | f:filter] ", tab_state.filter.label()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let filtered: Vec<String> = state
        .log_lines
        .iter()
        .filter(|line| tab_state.filter.keep(line))
        .cloned()
        .collect();
    tab_state.total_lines = filtered.len();

    // Auto-scroll to bottom if enabled
    if tab_state.auto_scroll && !filtered.is_empty() {
        tab_state.list_state.select(Some(filtered.len() - 1));
    }

    let items: Vec<ListItem> = filtered
        .iter()
        .map(|line| {
            let color = log_level_color(line);
            ListItem::new(Line::from(Span::styled(
                line.clone(),
                Style::default().fg(color),
            )))
        })
        .collect();

    let list = List::new(items).block(block).highlight_style(
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    );

    f.render_stateful_widget(list, area, &mut tab_state.list_state);
}
