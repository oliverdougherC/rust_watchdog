use crate::state::AppState;
use crate::tui::widgets::{StatCard, StatusBadge};
use crate::util::format_bytes_signed;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
    Frame,
};
use std::path::Path;

pub fn render_dashboard(f: &mut Frame, area: Rect, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Status bar (with border)
            Constraint::Length(5),  // Stat cards (taller)
            Constraint::Length(5),  // Current transcode
            Constraint::Min(6),    // Recent activity
        ])
        .split(area);

    render_status_bar(f, chunks[0], state);
    render_stat_cards(f, chunks[1], state);
    render_current_transcode(f, chunks[2], state);
    render_recent_activity(f, chunks[3], state);
}

fn render_status_bar(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default()
        .title(" Status ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let phase_color = match state.phase {
        crate::state::PipelinePhase::Idle => Color::Gray,
        crate::state::PipelinePhase::Scanning => Color::Yellow,
        crate::state::PipelinePhase::Transcoding => Color::Green,
        crate::state::PipelinePhase::Waiting => Color::Blue,
    };

    let nfs_color = if state.nfs_healthy {
        Color::Green
    } else {
        Color::Red
    };

    let mode_label = if state.simulate_mode {
        "Simulate"
    } else {
        "Live"
    };
    let mode_color = if state.simulate_mode {
        Color::Magenta
    } else {
        Color::Green
    };

    let phase_str = state.phase.to_string();
    let phase_badge = StatusBadge::new(&phase_str, phase_color);
    let nfs_label = if state.nfs_healthy { "Healthy" } else { "Unhealthy" };
    let nfs_badge = StatusBadge::new(nfs_label, nfs_color);

    let mut spans = vec![Span::raw(" State: ")];
    spans.extend(phase_badge.to_spans());
    spans.push(Span::raw("    NFS: "));
    spans.extend(nfs_badge.to_spans());
    spans.push(Span::raw("    Mode: "));
    spans.push(Span::styled(
        mode_label,
        Style::default().fg(mode_color).add_modifier(Modifier::BOLD),
    ));

    let status_line = Line::from(spans);
    f.render_widget(Paragraph::new(status_line), inner);
}

fn render_stat_cards(f: &mut Frame, area: Rect, state: &AppState) {
    let card_areas = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

    let transcoded = StatCard::new("Transcoded", format!("{}", state.total_transcoded))
        .style(Style::default().fg(Color::Green));
    f.render_widget(transcoded, card_areas[0]);

    let saved = StatCard::new("Space Saved", format_bytes_signed(state.total_space_saved))
        .style(Style::default().fg(Color::Cyan));
    f.render_widget(saved, card_areas[1]);

    let failures_str = if state.run_failures > 0 {
        format!("{}", state.run_failures)
    } else {
        "0".to_string()
    };
    let failures_color = if state.run_failures > 0 {
        Color::Red
    } else {
        Color::DarkGray
    };
    let failures = StatCard::new("Failures", failures_str)
        .style(Style::default().fg(failures_color));
    f.render_widget(failures, card_areas[2]);

    let queue_str = if state.queue_total > 0 {
        format!("{}/{}", state.queue_position, state.queue_total)
    } else {
        "0".to_string()
    };
    let queue = StatCard::new("Queue", queue_str)
        .style(Style::default().fg(Color::Magenta));
    f.render_widget(queue, card_areas[3]);
}

fn render_current_transcode(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default()
        .title(" Current Transcode ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    if let Some(ref file) = state.current_file {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(inner);

        // Show just the filename, not the full path
        let display_name = Path::new(file)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| file.clone());
        let display_name = if display_name.len() > inner.width as usize {
            let start = display_name.len().saturating_sub(inner.width as usize - 3);
            format!("...{}", &display_name[start..])
        } else {
            display_name
        };
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                display_name,
                Style::default().fg(Color::White),
            ))),
            chunks[0],
        );

        // Progress bar with stats
        let eta_str = if state.transcode_eta.is_empty() {
            String::new()
        } else {
            format!("  ETA {}", state.transcode_eta)
        };
        let label = format!(
            "{:.1}%  {:.1}fps  avg {:.1}{}",
            state.transcode_percent, state.transcode_fps, state.transcode_avg_fps, eta_str
        );

        let gauge = Gauge::default()
            .gauge_style(
                Style::default()
                    .fg(Color::Green)
                    .bg(Color::DarkGray),
            )
            .ratio((state.transcode_percent / 100.0).clamp(0.0, 1.0))
            .label(label);

        f.render_widget(gauge, chunks[1]);
    } else {
        let msg = match state.phase {
            crate::state::PipelinePhase::Idle => "Idle - no active transcode",
            crate::state::PipelinePhase::Scanning => "Scanning media libraries...",
            crate::state::PipelinePhase::Waiting => "Waiting for next scan interval...",
            _ => "Preparing...",
        };
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                msg,
                Style::default().fg(Color::DarkGray),
            )))
            .alignment(Alignment::Center),
            inner,
        );
    }
}

fn render_recent_activity(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default()
        .title(" Recent Activity ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let max_lines = inner.height as usize;
    let start = state.log_lines.len().saturating_sub(max_lines);

    let items: Vec<ListItem> = state
        .log_lines
        .iter()
        .skip(start)
        .map(|line| {
            let color = crate::tui::widgets::log_level_color(line);
            ListItem::new(Line::from(Span::styled(
                line.clone(),
                Style::default().fg(color),
            )))
        })
        .collect();

    f.render_widget(List::new(items), inner);
}
