use crate::error::Result;
use crate::traits::{FileEntry, FileSystem};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, trace, warn};
use walkdir::WalkDir;

/// Real filesystem implementation using actual OS calls.
pub struct RealFileSystem;

impl RealFileSystem {
    fn walk_share_impl(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
        cancel: Option<&AtomicBool>,
    ) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        let ext_set: HashSet<String> = extensions
            .iter()
            .map(|ext| ext.trim().to_lowercase())
            .collect();

        info!("Scanning share '{}' at {}", share_name, root.display());

        for entry in WalkDir::new(root).follow_links(false).into_iter() {
            if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                break;
            }

            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("[{}] Error walking directory: {}", share_name, e);
                    continue;
                }
            };

            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();

            // Skip hidden/sidecar files (AppleDouble, etc.)
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with('.') {
                    trace!(
                        "[{}] SKIP hidden/sidecar file: {}",
                        share_name,
                        path.display()
                    );
                    continue;
                }
            }

            // Check extension
            let ext_match = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_lowercase()))
                .map(|dot_ext| ext_set.contains(&dot_ext))
                .unwrap_or(false);

            if !ext_match {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        "[{}] Failed to read metadata for {}: {}",
                        share_name,
                        path.display(),
                        e
                    );
                    continue;
                }
            };

            let mtime = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            entries.push(FileEntry {
                path: path.to_path_buf(),
                size: metadata.len(),
                mtime,
                share_name: share_name.to_string(),
            });
        }

        info!("[{}] Found {} video files", share_name, entries.len());
        Ok(entries)
    }
}

impl FileSystem for RealFileSystem {
    fn walk_share(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
    ) -> Result<Vec<FileEntry>> {
        self.walk_share_impl(share_name, root, extensions, None)
    }

    fn walk_share_cancellable(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
        cancel: Option<&AtomicBool>,
    ) -> Result<Vec<FileEntry>> {
        self.walk_share_impl(share_name, root, extensions, cancel)
    }

    fn file_size(&self, path: &Path) -> Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn file_mtime(&self, path: &Path) -> Result<f64> {
        let meta = std::fs::metadata(path)?;
        let mtime = meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        Ok(mtime)
    }

    fn link_count(&self, path: &Path) -> Result<u64> {
        use std::os::unix::fs::MetadataExt;
        Ok(std::fs::metadata(path)?.nlink())
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn free_space(&self, path: &Path) -> Result<u64> {
        let stat = nix::sys::statvfs::statvfs(path)?;
        Ok(stat.blocks_available() as u64 * stat.fragment_size() as u64)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<std::path::PathBuf>> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            entries.push(entry.path());
        }
        Ok(entries)
    }

    fn walk_files_with_suffix(&self, root: &Path, suffix: &str) -> Result<Vec<std::path::PathBuf>> {
        let mut matches = Vec::new();
        for entry in WalkDir::new(root).follow_links(false).into_iter() {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Error walking directory {}: {}", root.display(), e);
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            if entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.ends_with(suffix))
            {
                matches.push(entry.path().to_path_buf());
            }
        }
        Ok(matches)
    }
}
