use crate::error::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Metadata about a discovered media file.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: f64,
    pub share_name: String,
}

/// Parsed ffprobe metadata for a media file.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProbeResult {
    pub video_codec: Option<String>,
    pub stream_bitrate_bps: u64,
    pub format_bitrate_bps: u64,
    pub size_bytes: u64,
    pub duration_seconds: f64,
    pub video_stream_count: u32,
    pub audio_stream_count: u32,
    pub subtitle_stream_count: u32,
    pub raw_json: serde_json::Value,
}

/// Progress update from a running transcode.
#[derive(Debug, Clone)]
pub struct TranscodeProgress {
    pub percent: f64,
    pub fps: f64,
    pub avg_fps: f64,
    pub eta: String,
}

/// The pipeline stage a file transfer belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStage {
    Import,
    Export,
}

impl TransferStage {
    pub fn as_str(self) -> &'static str {
        match self {
            TransferStage::Import => "import",
            TransferStage::Export => "export",
        }
    }
}

/// Progress update from a running rsync transfer.
#[derive(Debug, Clone)]
pub struct TransferProgress {
    pub stage: TransferStage,
    pub percent: f64,
    pub rate_mib_per_sec: f64,
    pub eta: String,
}

/// Result of a transcode operation.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TranscodeResult {
    pub success: bool,
    pub timed_out: bool,
    pub output_exists: bool,
}

/// Result of a file transfer operation.
#[derive(Debug)]
pub struct TransferResult {
    pub success: bool,
}

/// Result of checking whether a media file is currently in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InUseStatus {
    InUse,
    NotInUse,
}

/// Abstracts filesystem operations: walking directories, stat, rename, remove, free space.
pub trait FileSystem: Send + Sync {
    /// Walk a directory and return all video file entries for a given share.
    fn walk_share(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
    ) -> Result<Vec<FileEntry>>;

    /// Walk a directory and return video entries, with optional cooperative cancellation.
    /// Default implementation falls back to `walk_share` for implementations that do not
    /// support cancellation-aware walking.
    fn walk_share_cancellable(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
        cancel: Option<&AtomicBool>,
    ) -> Result<Vec<FileEntry>> {
        let _ = cancel;
        self.walk_share(share_name, root, extensions)
    }

    /// Get file size in bytes.
    fn file_size(&self, path: &Path) -> Result<u64>;

    /// Get file modification time as fractional seconds since epoch.
    fn file_mtime(&self, path: &Path) -> Result<f64>;

    /// Check if a path exists.
    fn exists(&self, path: &Path) -> bool;

    /// Check if a path is a directory.
    fn is_dir(&self, path: &Path) -> bool;

    /// Rename a file (used for safe_replace atomic swap).
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Remove a file.
    fn remove(&self, path: &Path) -> Result<()>;

    /// Get available free space in bytes at the given path.
    fn free_space(&self, path: &Path) -> Result<u64>;

    /// Create directories recursively.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// List direct entries in a directory.
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Recursively find files ending in the given suffix.
    fn walk_files_with_suffix(&self, root: &Path, suffix: &str) -> Result<Vec<PathBuf>>;
}

/// Abstracts ffprobe execution.
pub trait Prober: Send + Sync {
    /// Run ffprobe on a file and return parsed metadata, or None if the probe fails.
    fn probe(&self, path: &Path) -> Result<Option<ProbeResult>>;

    /// Run a quick health check (ffprobe -v error) on a file.
    fn health_check(&self, path: &Path) -> Result<bool>;
}

/// Abstracts HandBrakeCLI execution.
pub trait Transcoder: Send + Sync {
    /// Run HandBrakeCLI with the given input/output paths.
    /// Progress updates are sent via the provided channel.
    /// Returns when the transcode completes or times out.
    #[allow(clippy::too_many_arguments)]
    fn transcode(
        &self,
        input: &Path,
        output: &Path,
        preset_file: &Path,
        preset_name: &str,
        timeout_secs: u64,
        progress_tx: mpsc::Sender<TranscodeProgress>,
        cancel: Arc<AtomicBool>,
    ) -> Result<TranscodeResult>;
}

/// Abstracts rsync file transfer.
pub trait FileTransfer: Send + Sync {
    /// Transfer a file from source to destination using rsync.
    fn transfer(
        &self,
        source: &Path,
        dest: &Path,
        timeout_secs: u64,
        stage: TransferStage,
        progress_tx: Option<mpsc::Sender<TransferProgress>>,
    ) -> Result<TransferResult>;
}

/// Abstracts NFS mount health checking and remounting.
pub trait MountManager: Send + Sync {
    /// Check if a mount point is healthy (mounted and readable).
    fn is_healthy(&self, mount_point: &Path) -> bool;

    /// Attempt to remount an NFS share. Returns true on success.
    fn remount(
        &self,
        server: &str,
        remote_path: &str,
        local_mount: &Path,
        share_name: &str,
    ) -> Result<bool>;
}

/// Detect whether a file is currently opened by another process.
pub trait InUseDetector: Send + Sync {
    fn check_in_use(&self, path: &Path) -> Result<InUseStatus>;
}
