use crate::config::Config;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub database: PathBuf,
    pub status_snapshot: PathBuf,
    pub event_journal: PathBuf,
}

fn sibling_path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|v| v.to_str())
        .filter(|v| !v.is_empty())
        .unwrap_or("watchdog");
    parent.join(format!("{}.{}", stem, suffix))
}

pub fn resolve_runtime_paths(config: &Config, base_dir: &Path) -> RuntimePaths {
    let database = config.resolve_path(base_dir, &config.paths.database);
    let status_snapshot = if config.paths.status_snapshot.trim().is_empty() {
        sibling_path_with_suffix(&database, "status.json")
    } else {
        config.resolve_path(base_dir, &config.paths.status_snapshot)
    };
    let event_journal = if config.paths.event_journal.trim().is_empty() {
        sibling_path_with_suffix(&database, "events.ndjson")
    } else {
        config.resolve_path(base_dir, &config.paths.event_journal)
    };

    RuntimePaths {
        database,
        status_snapshot,
        event_journal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_runtime_paths_derives_internal_paths_when_empty() {
        let mut cfg = Config::default_config();
        cfg.paths.database = "state/watchdog.db".to_string();
        cfg.paths.status_snapshot.clear();
        cfg.paths.event_journal.clear();

        let paths = resolve_runtime_paths(&cfg, Path::new("/tmp/run"));
        assert_eq!(paths.database, Path::new("/tmp/run/state/watchdog.db"));
        assert_eq!(
            paths.status_snapshot,
            Path::new("/tmp/run/state/watchdog.status.json")
        );
        assert_eq!(
            paths.event_journal,
            Path::new("/tmp/run/state/watchdog.events.ndjson")
        );
    }
}
