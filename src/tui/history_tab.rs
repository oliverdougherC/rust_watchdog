use crate::db::{TranscodeRecord, WatchdogDb};
use crate::tui::widgets::status_color;
use crate::util::{format_bytes, format_bytes_signed};
use ratatui::{
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, Cell, Row, Table, TableState},
    Frame,
};

pub struct HistoryTabState {
    pub table_state: TableState,
    pub records: Vec<TranscodeRecord>,
    pub total_records: usize,
}

impl Default for HistoryTabState {
    fn default() -> Self {
        Self {
            table_state: TableState::default(),
            records: Vec::new(),
            total_records: 0,
        }
    }
}

impl HistoryTabState {
    pub fn scroll_up(&mut self) {
        let current = self.table_state.selected().unwrap_or(0);
        if current > 0 {
            self.table_state.select(Some(current - 1));
        }
    }

    pub fn scroll_down(&mut self) {
        let current = self.table_state.selected().unwrap_or(0);
        if current + 1 < self.total_records {
            self.table_state.select(Some(current + 1));
        }
    }

    pub fn scroll_to_top(&mut self) {
        self.table_state.select(Some(0));
    }

    pub fn scroll_to_bottom(&mut self) {
        if self.total_records > 0 {
            self.table_state.select(Some(self.total_records - 1));
        }
    }

    pub fn refresh(&mut self, db: &WatchdogDb) {
        self.records = db.get_recent_transcodes(100);
        self.total_records = self.records.len();
        if self.table_state.selected().is_none() && !self.records.is_empty() {
            self.table_state.select(Some(0));
        }
    }
}

pub fn render_history(f: &mut Frame, area: Rect, tab_state: &mut HistoryTabState) {
    let header_cells = [
        "Status", "File", "Codec", "Original", "Output", "Saved", "Duration", "Time",
    ]
    .iter()
    .map(|h| {
        Cell::from(Span::styled(
            *h,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))
    });
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = tab_state
        .records
        .iter()
        .map(|record| {
            let status_str = if record.success { "OK" } else { "FAIL" };
            let color = status_color(record.success);

            let filename = std::path::Path::new(&record.source_path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let codec = record
                .original_codec
                .clone()
                .unwrap_or_else(|| "?".to_string());
            let orig_size = record
                .original_size
                .map(|s| format_bytes(s as u64))
                .unwrap_or_else(|| "-".to_string());
            let out_size = record
                .output_size
                .map(|s| format_bytes(s as u64))
                .unwrap_or_else(|| "-".to_string());
            let saved = record
                .space_saved
                .map(|s| format_bytes_signed(s))
                .unwrap_or_else(|| "-".to_string());
            let duration = record
                .duration_seconds
                .map(|d| crate::util::format_duration(d))
                .unwrap_or_else(|| "-".to_string());
            let time = record
                .completed_at
                .as_deref()
                .or(record.started_at.as_deref())
                .unwrap_or("-");

            // Show only time portion if it's an ISO timestamp
            let time_short = if time.len() > 11 {
                &time[11..time.len().min(19)]
            } else {
                time
            };

            let failure_info = if !record.success {
                record.failure_reason.as_deref().unwrap_or("unknown")
            } else {
                ""
            };

            let file_display = if !record.success && !failure_info.is_empty() {
                format!("{} ({})", filename, failure_info)
            } else {
                filename
            };

            Row::new(vec![
                Cell::from(Span::styled(
                    status_str,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                )),
                Cell::from(file_display),
                Cell::from(codec),
                Cell::from(orig_size),
                Cell::from(out_size),
                Cell::from(saved),
                Cell::from(duration),
                Cell::from(time_short.to_string()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(6),
        Constraint::Min(20),
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(" Transcode History ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .row_highlight_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::REVERSED),
        );

    f.render_stateful_widget(table, area, &mut tab_state.table_state);
}
