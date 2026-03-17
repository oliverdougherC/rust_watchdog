use chrono::Utc;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use tracing::warn;

pub fn resolve_event_journal_path(config_path: &str, base_dir: &Path) -> Option<PathBuf> {
    let trimmed = config_path.trim();
    if trimmed.is_empty() {
        None
    } else {
        let path = Path::new(trimmed);
        if path.is_absolute() {
            Some(path.to_path_buf())
        } else {
            Some(base_dir.join(path))
        }
    }
}

pub fn append_event(path: Option<&Path>, event: &str, payload: Value) {
    let Some(path) = path else {
        return;
    };
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            warn!(
                "Failed to create event journal directory {}: {}",
                parent.display(),
                e
            );
            return;
        }
    }

    let row = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "event": event,
        "payload": payload,
    });
    let line = match serde_json::to_string(&row) {
        Ok(line) => line,
        Err(e) => {
            warn!(
                "Failed to serialize event journal row for event '{}': {}",
                event, e
            );
            return;
        }
    };

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path);
    let mut file = match file {
        Ok(file) => file,
        Err(e) => {
            warn!("Failed to open event journal {}: {}", path.display(), e);
            return;
        }
    };
    if let Err(e) = writeln!(file, "{}", line) {
        warn!("Failed to write event journal {}: {}", path.display(), e);
    }
}

pub fn append_log_event(path: Option<&Path>, level: &str, message: &str, line: &str) {
    append_event(
        path,
        "log",
        json!({
            "level": level,
            "message": message,
            "line": line,
        }),
    );
}

pub fn read_recent_log_lines(path: &Path, limit: usize) -> Vec<String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(limit.max(1));

    for raw_line in reader.lines().map_while(Result::ok) {
        let Ok(value) = serde_json::from_str::<Value>(&raw_line) else {
            continue;
        };
        if value.get("event").and_then(Value::as_str) != Some("log") {
            continue;
        }
        let rendered = value
            .get("payload")
            .and_then(|payload| payload.get("line"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let Some(rendered) = rendered else {
            continue;
        };
        if lines.len() >= limit.max(1) {
            lines.pop_front();
        }
        lines.push_back(rendered);
    }

    lines.into_iter().collect()
}
