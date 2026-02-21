use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Widget},
};

/// A stat card widget showing a label and a value.
pub struct StatCard<'a> {
    pub title: &'a str,
    pub value: String,
    pub style: Style,
}

impl<'a> StatCard<'a> {
    pub fn new(title: &'a str, value: String) -> Self {
        Self {
            title,
            value,
            style: Style::default(),
        }
    }

    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

impl Widget for StatCard<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .title(format!(" {} ", self.title))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        let inner = block.inner(area);
        block.render(area, buf);

        let value_line = Line::from(Span::styled(
            self.value,
            self.style.add_modifier(Modifier::BOLD),
        ));

        // Center the value vertically and horizontally
        if inner.height > 0 && inner.width > 0 {
            let y = inner.y + inner.height / 2;
            let text_len = value_line.width() as u16;
            let x = inner.x + inner.width.saturating_sub(text_len) / 2;
            buf.set_line(x, y, &value_line, inner.width);
        }
    }
}

/// A status badge with a colored dot and label.
pub struct StatusBadge<'a> {
    pub label: &'a str,
    pub color: Color,
}

impl<'a> StatusBadge<'a> {
    pub fn new(label: &'a str, color: Color) -> Self {
        Self { label, color }
    }

    pub fn to_spans(&self) -> Vec<Span<'_>> {
        vec![
            Span::styled("● ", Style::default().fg(self.color)),
            Span::styled(
                self.label,
                Style::default().fg(self.color).add_modifier(Modifier::BOLD),
            ),
        ]
    }
}

/// Color for log level text.
pub fn log_level_color(line: &str) -> Color {
    if line.contains("ERROR") || line.contains("CRITICAL") {
        Color::Red
    } else if line.contains("WARN") {
        Color::Yellow
    } else if line.contains("SUCCESS") {
        Color::Green
    } else if line.contains("INFO") {
        Color::Cyan
    } else {
        Color::White
    }
}

/// Color for transcode history status.
pub fn status_color(success: bool) -> Color {
    if success {
        Color::Green
    } else {
        Color::Red
    }
}
