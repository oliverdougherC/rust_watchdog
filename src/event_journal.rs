use chrono::Utc;
use serde_json::{json, Value};
use std::io::Write;
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
