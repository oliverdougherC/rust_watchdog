use crate::state::{AppState, PipelinePhase, ProgressStage};
use crate::tui::widgets::{StatCard, StatusBadge};
use crate::util::format_bytes_signed;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};
use std::path::Path;

pub fn render_dashboard(f: &mut Frame, area: Rect, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Status bar (with border)
            Constraint::Length(5), // Stat cards (taller)
            Constraint::Length(8), // Current transcode hero panel
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
        PipelinePhase::Idle => Color::Gray,
        PipelinePhase::Scanning => Color::Yellow,
        PipelinePhase::Transferring => Color::Cyan,
        PipelinePhase::Paused => Color::LightYellow,
        PipelinePhase::Transcoding => Color::Green,
        PipelinePhase::Waiting => Color::Blue,
        PipelinePhase::AwaitingSelection => Color::LightBlue,
    };

    let nfs_color = if state.nfs_healthy {
        Color::Green
    } else {
        Color::Red
    };

    let env_label = if state.simulate_mode {
        "Simulate"
    } else {
        "Live"
    };
    let env_color = if state.simulate_mode {
        Color::Magenta
    } else {
        Color::Green
    };

    let phase_str = state.phase.to_string();
    let phase_badge = StatusBadge::new(&phase_str, phase_color);
    let nfs_label = if state.nfs_healthy {
        "Healthy"
    } else {
        "Unhealthy"
    };
    let nfs_badge = StatusBadge::new(nfs_label, nfs_color);

    let mut spans = vec![Span::raw(" State: ")];
    spans.extend(phase_badge.to_spans());
    spans.push(Span::raw("    NFS: "));
    spans.extend(nfs_badge.to_spans());
    spans.push(Span::raw("    Mode: "));
    spans.push(Span::styled(
        env_label,
        Style::default().fg(env_color).add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::raw("    Run: "));
    spans.push(Span::styled(
        state.run_mode.to_string(),
        Style::default()
            .fg(Color::LightBlue)
            .add_modifier(Modifier::BOLD),
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
    let failures =
        StatCard::new("Failures", failures_str).style(Style::default().fg(failures_color));
    f.render_widget(failures, card_areas[2]);

    let queue_str = if state.queue_total > 0 {
        format!("{}/{}", state.queue_position, state.queue_total)
    } else {
        "0".to_string()
    };
    let queue = StatCard::new("Queue", queue_str).style(Style::default().fg(Color::Magenta));
    f.render_widget(queue, card_areas[3]);
}

fn render_current_transcode(f: &mut Frame, area: Rect, state: &AppState) {
    const HERO_TITLE_COLOR: Color = Color::Rgb(128, 236, 255);
    const HERO_BORDER_COLOR: Color = Color::Rgb(70, 120, 140);
    const HERO_TEXT_COLOR: Color = Color::Rgb(227, 244, 255);
    const HERO_SUBTEXT_COLOR: Color = Color::Rgb(161, 201, 222);
    const HERO_MUTED_COLOR: Color = Color::Rgb(104, 130, 144);

    let block = Block::default()
        .title(Line::from(vec![Span::styled(
            " Current Transcode ",
            Style::default()
                .fg(HERO_TITLE_COLOR)
                .add_modifier(Modifier::BOLD),
        )]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(HERO_BORDER_COLOR));

    let inner = block.inner(area);
    f.render_widget(block, area);

    if let Some(ref file) = state.current_file {
        // Show just the filename, not the full path
        let display_name = Path::new(file)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| file.clone());
        let display_name = truncate_from_left(&display_name, inner.width);

        let show_bar_top = inner.height >= 3;
        let show_bar_bottom = inner.height >= 4;
        let show_detail = inner.height >= 5;

        let mut constraints = vec![Constraint::Length(1), Constraint::Length(1)];
        if show_bar_top {
            constraints.push(Constraint::Length(1));
        }
        if show_bar_bottom {
            constraints.push(Constraint::Length(1));
        }
        if show_detail {
            constraints.push(Constraint::Length(1));
        }
        if inner.height > constraints.len() as u16 {
            constraints.push(Constraint::Min(0));
        }
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(inner);

        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                display_name,
                Style::default()
                    .fg(HERO_TEXT_COLOR)
                    .add_modifier(Modifier::BOLD),
            ))),
            rows[0],
        );

        let composite_percent = composite_percent(
            state.import_percent,
            state.transcode_percent,
            state.export_percent,
        );
        let stage_line = format!(
            "{}  |  Import {:.0}%  Transcode {:.0}%  Export {:.0}%  |  Overall {:.1}%",
            state.progress_stage,
            state.import_percent,
            state.transcode_percent,
            state.export_percent,
            composite_percent
        );
        let stage_line = truncate_to_width(&stage_line, rows[1].width);
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                stage_line,
                Style::default().fg(HERO_SUBTEXT_COLOR),
            ))),
            rows[1],
        );

        if show_bar_top {
            render_composite_bar_row(f, rows[2], state);
        }
        if show_bar_bottom {
            render_composite_bar_row(f, rows[3], state);
        }

        let detail = match state.progress_stage {
            ProgressStage::Import | ProgressStage::Export => transfer_detail_text(state),
            ProgressStage::Transcode => {
                let eta = if state.transcode_eta.is_empty() {
                    "-".to_string()
                } else {
                    state.transcode_eta.clone()
                };
                format!(
                    "Transcode {:.1} fps  avg {:.1} fps  ETA {}",
                    state.transcode_fps, state.transcode_avg_fps, eta
                )
            }
            ProgressStage::Idle => "Preparing...".to_string(),
        };
        if show_detail {
            let detail = truncate_to_width(&detail, rows[4].width);
            f.render_widget(
                Paragraph::new(Line::from(Span::styled(
                    detail,
                    Style::default().fg(HERO_MUTED_COLOR),
                ))),
                rows[4],
            );
        }
    } else {
        let msg = match state.phase {
            PipelinePhase::Idle => "Idle - no active transcode",
            PipelinePhase::Scanning => "Scanning media libraries...",
            PipelinePhase::Transferring => "Transferring media file...",
            PipelinePhase::Paused => "Paused (pause file present)",
            PipelinePhase::Transcoding => "Transcoding media file...",
            PipelinePhase::Waiting => "Waiting for next scan interval...",
            PipelinePhase::AwaitingSelection => "Waiting for manual selection...",
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

fn transfer_detail_text(state: &AppState) -> String {
    let transfer_percent = match state.progress_stage {
        ProgressStage::Import => state.import_percent,
        ProgressStage::Export => state.export_percent,
        _ => 0.0,
    };

    let eta = state.transfer_eta.trim();
    let eta_is_preparing = eta.is_empty() || eta.eq_ignore_ascii_case("preparing");
    if transfer_percent <= 0.0 && eta_is_preparing {
        return "Rsync preparing/checking...".to_string();
    }

    let eta_display = if eta.is_empty() { "-" } else { eta };
    format!(
        "Transfer {:.2} MiB/s  ETA {}",
        state.transfer_rate_mib_per_sec, eta_display
    )
}

fn composite_percent(import_percent: f64, transcode_percent: f64, export_percent: f64) -> f64 {
    (import_percent * 0.20) + (transcode_percent * 0.60) + (export_percent * 0.20)
}

fn truncate_to_width(text: &str, width: u16) -> String {
    let width = width as usize;
    if width == 0 {
        return String::new();
    }
    let mut out = String::new();
    for ch in text.chars().take(width) {
        out.push(ch);
    }
    out
}

fn truncate_from_left(text: &str, width: u16) -> String {
    let width = width as usize;
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
    let keep = width - 3;
    let suffix: String = text.chars().skip(len.saturating_sub(keep)).collect();
    format!("...{}", suffix)
}

fn compute_segment_widths(total_width: u16, gap_width: u16) -> Option<[u16; 3]> {
    let required_gaps = gap_width.saturating_mul(2);
    if total_width <= required_gaps {
        return None;
    }
    let available = total_width - required_gaps;
    if available < 3 {
        return None;
    }

    let mut widths = [
        ((available as u32 * 20) / 100) as u16,
        ((available as u32 * 60) / 100) as u16,
        ((available as u32 * 20) / 100) as u16,
    ];
    let mut remainders = [
        (available as u32 * 20) % 100,
        (available as u32 * 60) % 100,
        (available as u32 * 20) % 100,
    ];
    let used = widths[0] + widths[1] + widths[2];
    let mut leftover = available.saturating_sub(used);

    while leftover > 0 {
        let mut pick = 0usize;
        for idx in 1..3 {
            if remainders[idx] > remainders[pick]
                || (remainders[idx] == remainders[pick] && widths[idx] < widths[pick])
            {
                pick = idx;
            }
        }
        widths[pick] += 1;
        remainders[pick] = 0;
        leftover -= 1;
    }

    for idx in 0..3 {
        if widths[idx] == 0 {
            let mut donor = 0usize;
            for candidate in 1..3 {
                if widths[candidate] > widths[donor] {
                    donor = candidate;
                }
            }
            if widths[donor] <= 1 {
                return None;
            }
            widths[donor] -= 1;
            widths[idx] = 1;
        }
    }

    Some(widths)
}

fn render_composite_bar_row(f: &mut Frame, row: Rect, state: &AppState) {
    const GAP_WIDTH: u16 = 1;
    const EMPTY_BG: Color = Color::Rgb(35, 52, 64);
    const GAP_BG: Color = Color::Rgb(16, 29, 38);

    if let Some([import_w, transcode_w, export_w]) = compute_segment_widths(row.width, GAP_WIDTH) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(import_w),
                Constraint::Length(GAP_WIDTH),
                Constraint::Length(transcode_w),
                Constraint::Length(GAP_WIDTH),
                Constraint::Length(export_w),
            ])
            .split(row);

        f.render_widget(
            Block::default().style(Style::default().bg(GAP_BG)),
            chunks[1],
        );
        f.render_widget(
            Block::default().style(Style::default().bg(GAP_BG)),
            chunks[3],
        );

        render_stage_segment(
            f,
            chunks[0],
            state.progress_stage,
            ProgressStage::Import,
            state.import_percent,
            Color::Rgb(76, 192, 255),
            Color::Rgb(44, 88, 116),
            EMPTY_BG,
        );
        render_stage_segment(
            f,
            chunks[2],
            state.progress_stage,
            ProgressStage::Transcode,
            state.transcode_percent,
            Color::Rgb(119, 224, 124),
            Color::Rgb(51, 111, 58),
            EMPTY_BG,
        );
        render_stage_segment(
            f,
            chunks[4],
            state.progress_stage,
            ProgressStage::Export,
            state.export_percent,
            Color::Rgb(255, 190, 84),
            Color::Rgb(132, 88, 38),
            EMPTY_BG,
        );
    } else {
        let overall = composite_percent(
            state.import_percent,
            state.transcode_percent,
            state.export_percent,
        );
        render_solid_progress_bar(row, f, overall, Color::Rgb(119, 224, 124), EMPTY_BG);
    }
}

#[allow(clippy::too_many_arguments)]
fn render_stage_segment(
    f: &mut Frame,
    area: Rect,
    active_stage: ProgressStage,
    segment_stage: ProgressStage,
    segment_percent: f64,
    active_fill: Color,
    completed_fill: Color,
    empty_bg: Color,
) {
    let fill_color = if active_stage == segment_stage {
        active_fill
    } else if segment_percent > 0.0 {
        completed_fill
    } else {
        empty_bg
    };

    render_solid_progress_bar(area, f, segment_percent, fill_color, empty_bg);
}

fn render_solid_progress_bar(
    area: Rect,
    f: &mut Frame,
    percent: f64,
    fill_color: Color,
    empty_bg: Color,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    f.render_widget(Block::default().style(Style::default().bg(empty_bg)), area);

    let pct = percent.clamp(0.0, 100.0);
    if pct <= 0.0 {
        return;
    }

    let mut fill_width = ((area.width as f64) * (pct / 100.0)).round() as u16;
    if fill_width == 0 {
        fill_width = 1;
    }
    fill_width = fill_width.min(area.width);

    let fill_area = Rect {
        x: area.x,
        y: area.y,
        width: fill_width,
        height: area.height,
    };
    f.render_widget(
        Block::default().style(Style::default().bg(fill_color)),
        fill_area,
    );
}

fn render_recent_activity(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default()
        .title(" Recent Activity ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let max_lines = inner.height as usize;
    let mut header_lines = Vec::new();
    let show_pass_summary = matches!(
        state.phase,
        PipelinePhase::Idle | PipelinePhase::Waiting | PipelinePhase::Paused
    );
    if show_pass_summary {
        header_lines.push(format!(
            "Skipped this pass: inspected={} young={} cooldown={} filtered={} in_use={} quarantined={}",
            state.run_skipped_inspected,
            state.run_skipped_young,
            state.run_skipped_cooldown,
            state.run_skipped_filtered,
            state.run_skipped_in_use,
            state.run_skipped_quarantined
        ));
        if !state.top_failure_reasons.is_empty() {
            let summary = state
                .top_failure_reasons
                .iter()
                .map(|(reason, count)| format!("{} ({})", reason, count))
                .collect::<Vec<_>>()
                .join(", ");
            header_lines.push(format!("Top historical failure reasons: {}", summary));
        }
        if state.scan_timeout_count > 0 {
            header_lines.push(format!(
                "Scan timeouts (runtime): {}",
                state.scan_timeout_count
            ));
        }
        if state.run_retries_scheduled > 0 {
            header_lines.push(format!(
                "Retries scheduled this pass: {} (lifetime: {})",
                state.run_retries_scheduled, state.total_retries_scheduled
            ));
        }
        if let Some(code) = state.last_failure_code.as_deref() {
            header_lines.push(format!("Last failure code: {}", code));
        }
        if state.consecutive_pass_failures > 0 {
            header_lines.push(format!(
                "Consecutive pass failures: {}",
                state.consecutive_pass_failures
            ));
        }
        if state.auto_paused {
            header_lines.push(format!(
                "Auto-paused: {}",
                state
                    .auto_pause_reason
                    .as_deref()
                    .unwrap_or("safety tripwire")
            ));
        }
        if state.quarantined_files > 0 {
            header_lines.push(format!("Quarantined files: {}", state.quarantined_files));
        }
    }

    let reserved = header_lines.len();
    let log_capacity = max_lines.saturating_sub(reserved);
    let start = state.log_lines.len().saturating_sub(log_capacity);

    let mut items: Vec<ListItem> = header_lines
        .into_iter()
        .map(|line| {
            ListItem::new(Line::from(Span::styled(
                line,
                Style::default().fg(Color::Yellow),
            )))
        })
        .collect();

    items.extend(state.log_lines.iter().skip(start).map(|line| {
        let color = crate::tui::widgets::log_level_color(line);
        ListItem::new(Line::from(Span::styled(
            line.clone(),
            Style::default().fg(color),
        )))
    }));

    f.render_widget(List::new(items), inner);
}

#[cfg(test)]
mod tests {
    use super::{
        composite_percent, compute_segment_widths, transfer_detail_text, truncate_to_width,
    };
    use crate::state::{AppState, ProgressStage};

    #[test]
    fn truncate_to_width_respects_bounds() {
        assert_eq!(truncate_to_width("abcdef", 4), "abcd");
        assert_eq!(truncate_to_width("abcdef", 0), "");
        assert_eq!(truncate_to_width("abc", 8), "abc");
    }

    #[test]
    fn segment_widths_sum_with_gaps() {
        for width in [5u16, 24, 80, 160] {
            let seg = compute_segment_widths(width, 1).unwrap();
            assert_eq!(seg[0] + seg[1] + seg[2] + 2, width);
            assert!(seg.iter().all(|w| *w >= 1));
        }
    }

    #[test]
    fn composite_percent_uses_20_60_20() {
        assert!((composite_percent(100.0, 100.0, 0.0) - 80.0).abs() < 0.0001);
        assert!((composite_percent(50.0, 0.0, 50.0) - 20.0).abs() < 0.0001);
        assert!((composite_percent(0.0, 50.0, 0.0) - 30.0).abs() < 0.0001);
    }

    #[test]
    fn transfer_detail_shows_preparing_message_at_zero_percent() {
        let state = AppState {
            progress_stage: ProgressStage::Import,
            import_percent: 0.0,
            transfer_eta: "preparing".to_string(),
            ..AppState::default()
        };
        assert_eq!(transfer_detail_text(&state), "Rsync preparing/checking...");
    }
}
