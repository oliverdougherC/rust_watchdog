use crate::db::{QueueRecord, WatchdogDb};
use crate::transcode::PresetSnapshot;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Frame,
};

#[derive(Default)]
pub struct QueueTabState {
    pub table_state: TableState,
    pub records: Vec<QueueRecord>,
    pub total_records: usize,
}

impl QueueTabState {
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
        self.records = db.list_queue_items(500);
        self.total_records = self.records.len();
        if let Some(selected) = self.table_state.selected() {
            if self.records.is_empty() {
                self.table_state.select(None);
            } else if selected >= self.records.len() {
                self.table_state.select(Some(self.records.len() - 1));
            }
        } else if !self.records.is_empty() {
            self.table_state.select(Some(0));
        }
    }
}

fn effective_preset(record: &QueueRecord, default_preset: &PresetSnapshot) -> PresetSnapshot {
    PresetSnapshot {
        preset_file: record
            .preset_file
            .clone()
            .unwrap_or_else(|| default_preset.preset_file.clone()),
        preset_name: record
            .preset_name
            .clone()
            .unwrap_or_else(|| default_preset.preset_name.clone()),
        target_codec: record
            .target_codec
            .clone()
            .unwrap_or_else(|| default_preset.target_codec.clone()),
    }
}

pub fn render_queue(
    f: &mut Frame,
    area: Rect,
    tab_state: &mut QueueTabState,
    default_preset: &PresetSnapshot,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(5)])
        .split(area);

    let header = Row::new(
        ["Status", "Source", "File", "Preset", "Share"]
            .iter()
            .map(|h| {
                Cell::from(Span::styled(
                    *h,
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ))
            }),
    )
    .height(1);

    let rows = tab_state.records.iter().map(|record| {
        let status = if record.started_at.is_some() {
            Span::styled(
                "ACTIVE",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled("PENDING", Style::default().fg(Color::Magenta))
        };
        let file_name = std::path::Path::new(&record.source_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let preset = effective_preset(record, default_preset);

        Row::new(vec![
            Cell::from(status),
            Cell::from(record.enqueue_source.clone()),
            Cell::from(file_name),
            Cell::from(preset.short_label()),
            Cell::from(record.share_name.clone()),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Min(20),
            Constraint::Length(24),
            Constraint::Length(14),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" Queue ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    )
    .row_highlight_style(
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::REVERSED),
    );
    f.render_stateful_widget(table, chunks[0], &mut tab_state.table_state);

    let details = tab_state
        .table_state
        .selected()
        .and_then(|idx| tab_state.records.get(idx))
        .map(|record| {
            let preset = effective_preset(record, default_preset);
            format!(
                "Path: {}\nPreset: {} [{}] codec={}\nQueued: {}  Started: {}  Order: {}",
                record.source_path,
                preset.preset_name,
                preset.preset_file,
                preset.target_codec,
                record.queued_at,
                record.started_at.as_deref().unwrap_or("-"),
                record.order_key,
            )
        })
        .unwrap_or_else(|| "Queue is empty".to_string());

    f.render_widget(
        Paragraph::new(details).block(
            Block::default()
                .title(" Details ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        ),
        chunks[1],
    );
}
