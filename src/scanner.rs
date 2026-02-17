use crate::error::Result;
use crate::traits::{FileEntry, FileSystem};
use std::path::Path;
use tracing::{info, trace, warn};
use walkdir::WalkDir;

/// Real filesystem implementation using actual OS calls.
pub struct RealFileSystem;

impl FileSystem for RealFileSystem {
    fn walk_share(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
    ) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();

        info!("Scanning share '{}' at {}", share_name, root.display());

        for entry in WalkDir::new(root).follow_links(false).into_iter() {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!(
                        "[{}] Error walking directory: {}",
                        share_name, e
                    );
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
                .map(|e| {
                    let dot_ext = format!(".{}", e.to_lowercase());
                    extensions.iter().any(|ext| ext.to_lowercase() == dot_ext)
                })
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

        info!(
            "[{}] Found {} video files",
            share_name,
            entries.len()
        );
        Ok(entries)
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
}
