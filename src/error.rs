use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum WatchdogError {
    #[error("Config error: {0}")]
    Config(String),

    #[error("Config file not found: {0}")]
    ConfigNotFound(PathBuf),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("NFS mount failed for share '{share}': {reason}")]
    NfsMount { share: String, reason: String },

    #[error("Share scan timed out after {timeout_secs}s ({pending_shares} share(s) pending)")]
    ScanTimeout {
        timeout_secs: u64,
        pending_shares: usize,
    },

    #[error("Transcode failed for {path}: {reason}")]
    Transcode { path: PathBuf, reason: String },

    #[error("Transcode timed out after {timeout_secs}s for {path}")]
    TranscodeTimeout { path: PathBuf, timeout_secs: u64 },

    #[error("Transcode stalled after {stall_timeout_secs}s with no progress for {path}")]
    TranscodeStalled {
        path: PathBuf,
        stall_timeout_secs: u64,
    },

    #[error("Transcode cancelled for {path}: {reason}")]
    TranscodeCancelled { path: PathBuf, reason: String },

    #[error("Verification failed for {path}: {reason}")]
    Verification { path: PathBuf, reason: String },

    #[error("Transfer failed for {path}: {reason}")]
    Transfer { path: PathBuf, reason: String },

    #[error("Probe failed for {path}: {reason}")]
    Probe { path: PathBuf, reason: String },

    #[error("In-use detection failed for {path}: {reason}")]
    InUse { path: PathBuf, reason: String },

    #[error("Missing dependency: {0}")]
    MissingDependency(String),

    #[error("No media directories available")]
    NoMediaDirectories,

    #[error("Insufficient disk space on {path}: need {needed}, have {available}")]
    InsufficientSpace {
        path: PathBuf,
        needed: String,
        available: String,
    },

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Pipeline paused")]
    Paused,

    #[error("Nix error: {0}")]
    Nix(#[from] nix::Error),
}

pub type Result<T> = std::result::Result<T, WatchdogError>;
