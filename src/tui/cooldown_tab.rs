use crate::db::{CooldownRecord, WatchdogDb};
use ratatui::{
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, Cell, Row, Table, TableState},
    Frame,
};

#[derive(Default)]
pub struct CooldownTabState {
    pub table_state: TableState,
    pub records: Vec<CooldownRecord>,
    pub total_records: usize,
}

impl CooldownTabState {
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
        self.records = db.get_cooldown_files(chrono::Utc::now().timestamp(), 200);
        self.total_records = self.records.len();
        if self.table_state.selected().is_none() && !self.records.is_empty() {
            self.table_state.select(Some(0));
        }
    }
}

pub fn render_cooldown(f: &mut Frame, area: Rect, tab_state: &mut CooldownTabState) {
    let header_cells = ["File", "Failures", "Reason", "Retry At (UTC)"]
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
            let retry_at =
                chrono::DateTime::<chrono::Utc>::from_timestamp(record.next_eligible_at, 0)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "-".to_string());
            Row::new(vec![
                Cell::from(record.file_path.clone()),
                Cell::from(record.consecutive_failures.to_string()),
                Cell::from(
                    record
                        .last_failure_reason
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ),
                Cell::from(retry_at),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(20),
        Constraint::Length(8),
        Constraint::Length(24),
        Constraint::Length(21),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(" Cooldown Files ")
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
