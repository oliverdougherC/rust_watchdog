use crate::config::{Config, ShareConfig};
use crate::db::{NewQueueItem, WatchdogDb};
use crate::traits::{FileEntry, FileSystem};
use crate::transcode::PresetSnapshot;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualSelectionPhase {
    Discovering,
    Evaluating,
    Enqueueing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualSelectionProgress {
    pub phase: ManualSelectionPhase,
    pub selected_roots: usize,
    pub root_index: usize,
    pub current_root: Option<PathBuf>,
    pub current_path: Option<PathBuf>,
    pub discovered_files: usize,
    pub processed_files: usize,
    pub queued_candidates: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ManualSelectionSummary {
    pub selected_roots: usize,
    pub discovered_files: usize,
    pub enqueued_files: usize,
    pub skipped_duplicates: usize,
    pub skipped_ineligible: usize,
    pub skipped_cooldown: usize,
    pub skipped_quarantined: usize,
    pub skipped_young: usize,
    pub skipped_missing: usize,
    pub skipped_non_video: usize,
    pub skipped_probe_failed: usize,
}

fn path_matches_video_extension(path: &Path, extensions: &[String]) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext.to_ascii_lowercase()))
        .is_some_and(|ext| extensions.iter().any(|candidate| candidate == &ext))
}

fn share_for_path<'a>(config: &'a Config, path: &Path) -> Option<&'a ShareConfig> {
    config
        .shares
        .iter()
        .filter(|share| path.starts_with(Path::new(&share.local_mount)))
        .max_by_key(|share| share.local_mount.len())
}

fn manual_file_entry(fs: &dyn FileSystem, share: &ShareConfig, path: &Path) -> Option<FileEntry> {
    let size = fs.file_size(path).ok()?;
    let mtime = fs.file_mtime(path).ok()?;
    Some(FileEntry {
        path: path.to_path_buf(),
        size,
        mtime,
        share_name: share.name.clone(),
    })
}

pub fn enqueue_manual_paths(
    config: &Config,
    fs: &dyn FileSystem,
    db: &WatchdogDb,
    paths: &[PathBuf],
    preset: &PresetSnapshot,
    preset_payload_json: &str,
) -> ManualSelectionSummary {
    enqueue_manual_paths_with_progress(config, fs, db, paths, preset, preset_payload_json, |_| {})
}

pub fn enqueue_manual_paths_with_progress<F>(
    config: &Config,
    fs: &dyn FileSystem,
    db: &WatchdogDb,
    paths: &[PathBuf],
    preset: &PresetSnapshot,
    preset_payload_json: &str,
    mut on_progress: F,
) -> ManualSelectionSummary
where
    F: FnMut(ManualSelectionProgress),
{
    let mut summary = ManualSelectionSummary {
        selected_roots: paths.len(),
        ..ManualSelectionSummary::default()
    };
    let mut seen_paths = HashSet::<PathBuf>::new();
    let mut queue_items = Vec::new();
    let now_ts = chrono::Utc::now().timestamp() as f64;
    let mut processed_files = 0usize;

    for (idx, path) in paths.iter().enumerate() {
        on_progress(ManualSelectionProgress {
            phase: ManualSelectionPhase::Discovering,
            selected_roots: paths.len(),
            root_index: idx + 1,
            current_root: Some(path.clone()),
            current_path: None,
            discovered_files: summary.discovered_files,
            processed_files,
            queued_candidates: queue_items.len(),
        });

        let Some(share) = share_for_path(config, path) else {
            summary.skipped_missing += 1;
            continue;
        };

        let mut candidates = if fs.is_dir(path) {
            fs.walk_share(&share.name, path, &config.scan.video_extensions)
                .unwrap_or_default()
        } else if !fs.exists(path) {
            summary.skipped_missing += 1;
            continue;
        } else if !path_matches_video_extension(path, &config.scan.video_extensions) {
            summary.skipped_non_video += 1;
            continue;
        } else {
            match manual_file_entry(fs, share, path) {
                Some(entry) => vec![entry],
                None => {
                    summary.skipped_missing += 1;
                    continue;
                }
            }
        };

        candidates.sort_by(|a, b| a.path.cmp(&b.path));
        for entry in candidates {
            if !seen_paths.insert(entry.path.clone()) {
                summary.skipped_duplicates += 1;
                continue;
            }
            summary.discovered_files += 1;
            let path_string = entry.path.to_string_lossy().to_string();

            let file_age_secs = (now_ts - entry.mtime).max(0.0);
            if file_age_secs < config.safety.min_file_age_seconds as f64 {
                summary.skipped_young += 1;
            } else if db.is_quarantined(&path_string) {
                summary.skipped_quarantined += 1;
            } else {
                let in_cooldown = db
                    .get_file_failure_state(&path_string)
                    .is_some_and(|failure_state| failure_state.next_eligible_at > now_ts as i64);
                if in_cooldown {
                    summary.skipped_cooldown += 1;
                } else {
                    queue_items.push(NewQueueItem {
                        source_path: path_string,
                        share_name: entry.share_name.clone(),
                        enqueue_source: "manual".to_string(),
                        preset_file: preset.preset_file.clone(),
                        preset_name: preset.preset_name.clone(),
                        target_codec: preset.target_codec.clone(),
                        preset_payload_json: preset_payload_json.to_string(),
                    });
                }
            }
            processed_files += 1;
            on_progress(ManualSelectionProgress {
                phase: ManualSelectionPhase::Evaluating,
                selected_roots: paths.len(),
                root_index: idx + 1,
                current_root: Some(path.clone()),
                current_path: Some(entry.path.clone()),
                discovered_files: summary.discovered_files,
                processed_files,
                queued_candidates: queue_items.len(),
            });
        }
    }

    on_progress(ManualSelectionProgress {
        phase: ManualSelectionPhase::Enqueueing,
        selected_roots: paths.len(),
        root_index: paths.len(),
        current_root: paths.last().cloned(),
        current_path: None,
        discovered_files: summary.discovered_files,
        processed_files,
        queued_candidates: queue_items.len(),
    });

    let inserted = db.enqueue_queue_items(&queue_items);
    summary.enqueued_files = inserted;
    summary.skipped_duplicates += queue_items.len().saturating_sub(inserted);
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShareConfig;
    use crate::db::WatchdogDb;
    use crate::error::{Result, WatchdogError};
    use crate::traits::FileSystem;
    use crate::transcode::PresetSnapshot;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct TestFs {
        files: Mutex<HashMap<PathBuf, (u64, f64)>>,
    }

    impl TestFs {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
            }
        }

        fn insert(&self, path: &str, size: u64, mtime: f64) {
            self.files
                .lock()
                .unwrap()
                .insert(PathBuf::from(path), (size, mtime));
        }
    }

    impl FileSystem for TestFs {
        fn walk_share(
            &self,
            share_name: &str,
            root: &Path,
            extensions: &[String],
        ) -> Result<Vec<FileEntry>> {
            let mut entries = Vec::new();
            for (path, (size, mtime)) in self.files.lock().unwrap().iter() {
                if !path.starts_with(root) || !path_matches_video_extension(path, extensions) {
                    continue;
                }
                entries.push(FileEntry {
                    path: path.clone(),
                    size: *size,
                    mtime: *mtime,
                    share_name: share_name.to_string(),
                });
            }
            Ok(entries)
        }

        fn file_size(&self, path: &Path) -> Result<u64> {
            self.files
                .lock()
                .unwrap()
                .get(path)
                .map(|(size, _)| *size)
                .ok_or_else(|| {
                    WatchdogError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"))
                })
        }

        fn file_mtime(&self, path: &Path) -> Result<f64> {
            self.files
                .lock()
                .unwrap()
                .get(path)
                .map(|(_, mtime)| *mtime)
                .ok_or_else(|| {
                    WatchdogError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"))
                })
        }

        fn link_count(&self, path: &Path) -> Result<u64> {
            if self.files.lock().unwrap().contains_key(path) {
                Ok(1)
            } else {
                Err(WatchdogError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "missing",
                )))
            }
        }

        fn exists(&self, path: &Path) -> bool {
            self.files.lock().unwrap().contains_key(path)
        }

        fn is_dir(&self, path: &Path) -> bool {
            self.files
                .lock()
                .unwrap()
                .keys()
                .any(|candidate| candidate.starts_with(path) && candidate != path)
        }

        fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
            Ok(())
        }

        fn remove(&self, _path: &Path) -> Result<()> {
            Ok(())
        }

        fn free_space(&self, _path: &Path) -> Result<u64> {
            Ok(u64::MAX)
        }

        fn create_dir_all(&self, _path: &Path) -> Result<()> {
            Ok(())
        }

        fn list_dir(&self, _path: &Path) -> Result<Vec<PathBuf>> {
            Ok(Vec::new())
        }

        fn walk_files_with_suffix(&self, _root: &Path, _suffix: &str) -> Result<Vec<PathBuf>> {
            Ok(Vec::new())
        }
    }

    fn config() -> Config {
        let mut cfg = Config::default_config();
        cfg.shares = vec![ShareConfig {
            name: "movies".to_string(),
            remote_path: "/remote/movies".to_string(),
            local_mount: "/mnt/movies".to_string(),
        }];
        cfg.scan.video_extensions = vec![".mkv".to_string()];
        cfg
    }

    fn preset() -> PresetSnapshot {
        PresetSnapshot::normalized(
            std::path::Path::new("/"),
            "presets/AV1_MKV.json",
            "AV1_MKV",
            "av1",
        )
    }

    fn preset_payload_json() -> &'static str {
        r#"{"PresetList":[]}"#
    }

    #[test]
    fn manual_selection_expands_folders_and_queues_already_compliant_files() {
        let cfg = config();
        let fs = TestFs::new();
        fs.insert("/mnt/movies/needs-a.mkv", 10, 1.0);
        fs.insert("/mnt/movies/already-target.mkv", 10, 1.0);
        let db = WatchdogDb::open_in_memory().unwrap();

        let summary = enqueue_manual_paths(
            &cfg,
            &fs,
            &db,
            &[PathBuf::from("/mnt/movies")],
            &preset(),
            preset_payload_json(),
        );

        assert_eq!(summary.discovered_files, 2);
        assert_eq!(summary.enqueued_files, 2);
        assert_eq!(summary.skipped_ineligible, 0);
        assert_eq!(db.get_queue_count(), 2);
    }

    #[test]
    fn manual_selection_bypasses_inspected_cache_but_dedupes_active_queue() {
        let cfg = config();
        let fs = TestFs::new();
        fs.insert("/mnt/movies/needs-a.mkv", 10, 1.0);
        let db = WatchdogDb::open_in_memory().unwrap();
        db.mark_inspected("/mnt/movies/needs-a.mkv", 10, 1.0);

        let first = enqueue_manual_paths(
            &cfg,
            &fs,
            &db,
            &[PathBuf::from("/mnt/movies/needs-a.mkv")],
            &preset(),
            preset_payload_json(),
        );
        let second = enqueue_manual_paths(
            &cfg,
            &fs,
            &db,
            &[PathBuf::from("/mnt/movies/needs-a.mkv")],
            &preset(),
            preset_payload_json(),
        );

        assert_eq!(first.enqueued_files, 1);
        assert_eq!(second.enqueued_files, 0);
        assert!(second.skipped_duplicates >= 1);
    }

    #[test]
    fn manual_selection_reports_progress_for_folder_expansion() {
        let cfg = config();
        let fs = TestFs::new();
        fs.insert("/mnt/movies/needs-a.mkv", 10, 1.0);
        fs.insert("/mnt/movies/needs-b.mkv", 10, 1.0);
        let db = WatchdogDb::open_in_memory().unwrap();
        let mut progress = Vec::new();

        let summary = enqueue_manual_paths_with_progress(
            &cfg,
            &fs,
            &db,
            &[PathBuf::from("/mnt/movies")],
            &preset(),
            preset_payload_json(),
            |update| progress.push(update),
        );

        assert_eq!(summary.enqueued_files, 2);
        assert!(progress
            .iter()
            .any(|update| update.phase == ManualSelectionPhase::Discovering));
        assert!(progress.iter().any(|update| {
            update.phase == ManualSelectionPhase::Evaluating
                && update.discovered_files == 2
                && update.processed_files == 2
        }));
        assert!(progress
            .iter()
            .any(|update| update.phase == ManualSelectionPhase::Enqueueing));
    }
}
