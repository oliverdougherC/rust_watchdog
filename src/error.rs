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

    #[error("Share access failed for share '{share}': {reason}")]
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

impl WatchdogError {
    /// Stable machine-readable error code for diagnostics.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "config_error",
            Self::ConfigNotFound(_) => "config_not_found",
            Self::Database(_) => "database_error",
            Self::Io(_) => "io_error",
            Self::Json(_) => "json_error",
            Self::Toml(_) => "toml_error",
            Self::NfsMount { .. } => "nfs_mount_error",
            Self::ScanTimeout { .. } => "scan_timeout",
            Self::Transcode { .. } => "transcode_error",
            Self::TranscodeTimeout { .. } => "transcode_timeout",
            Self::TranscodeStalled { .. } => "transcode_stalled",
            Self::TranscodeCancelled { .. } => "transcode_cancelled",
            Self::Verification { .. } => "verification_error",
            Self::Transfer { .. } => "transfer_error",
            Self::Probe { .. } => "probe_error",
            Self::InUse { .. } => "in_use_error",
            Self::MissingDependency(_) => "missing_dependency",
            Self::NoMediaDirectories => "no_media_directories",
            Self::InsufficientSpace { .. } => "insufficient_space",
            Self::Shutdown => "shutdown",
            Self::Paused => "paused",
            Self::Nix(_) => "nix_error",
        }
    }

    /// High-level grouping useful for operators scanning logs quickly.
    pub fn category(&self) -> &'static str {
        match self {
            Self::Config(_)
            | Self::ConfigNotFound(_)
            | Self::Json(_)
            | Self::Toml(_)
            | Self::MissingDependency(_) => "configuration",
            Self::Database(_) => "database",
            Self::Io(_) | Self::Nix(_) => "system",
            Self::NfsMount { .. } => "nfs",
            Self::ScanTimeout { .. } => "scan",
            Self::Transcode { .. }
            | Self::TranscodeTimeout { .. }
            | Self::TranscodeStalled { .. }
            | Self::TranscodeCancelled { .. } => "transcode",
            Self::Verification { .. } => "verification",
            Self::Transfer { .. } => "transfer",
            Self::Probe { .. } => "probe",
            Self::InUse { .. } => "in_use_detection",
            Self::NoMediaDirectories => "discovery",
            Self::InsufficientSpace { .. } => "capacity",
            Self::Shutdown | Self::Paused => "control_flow",
        }
    }

    /// Human hint for common remediation actions.
    pub fn operator_hint(&self) -> Option<&'static str> {
        match self {
            Self::Config(_) | Self::ConfigNotFound(_) | Self::Toml(_) => Some(
                "Validate watchdog.toml syntax/paths and run `watchdog --doctor --config <path>`.",
            ),
            Self::MissingDependency(_) => Some(
                "Install missing CLI tools and re-run `watchdog --doctor` to verify detection.",
            ),
            Self::Database(_) => Some(
                "Verify database path permissions and free disk space; inspect sqlite file ownership.",
            ),
            Self::NfsMount { .. } => Some(
                "Check configured share roots; in NFS mode also verify server reachability and mount credentials, then re-run doctor/status checks.",
            ),
            Self::ScanTimeout { .. } => Some(
                "Reduce scan scope or increase `safety.share_scan_timeout_seconds` for slow/unhealthy shares.",
            ),
            Self::TranscodeTimeout { .. } => Some(
                "Increase `transcode.timeout_seconds` or inspect HandBrakeCLI logs for stalled jobs.",
            ),
            Self::TranscodeStalled { .. } => Some(
                "Inspect source integrity and disk throughput; consider increasing stall timeout.",
            ),
            Self::InsufficientSpace { .. } => Some(
                "Free destination/temp disk space or tune free-space multiplier thresholds.",
            ),
            Self::Transfer { .. } => Some(
                "Check rsync connectivity/permissions and verify destination mount stability.",
            ),
            Self::Verification { .. } => Some(
                "Inspect ffprobe output for source/transcoded files and check preset compatibility.",
            ),
            Self::InUse { .. } => Some(
                "Confirm in-use guard command behavior (`lsof`) and check for active file handles.",
            ),
            Self::NoMediaDirectories => Some(
                "Verify share mount paths exist and are readable; run with `--doctor` for mount diagnostics.",
            ),
            Self::Shutdown | Self::Paused => None,
            _ => Some("Re-run with `RUST_LOG=watchdog=debug` and inspect the full error chain."),
        }
    }

    /// Structured fields for rich logging and support tickets.
    pub fn diagnostic_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::Config(message) => vec![("message", message.clone())],
            Self::ConfigNotFound(path) => vec![("path", path.display().to_string())],
            Self::NfsMount { share, reason } => {
                vec![("share", share.clone()), ("reason", reason.clone())]
            }
            Self::ScanTimeout {
                timeout_secs,
                pending_shares,
            } => vec![
                ("timeout_secs", timeout_secs.to_string()),
                ("pending_shares", pending_shares.to_string()),
            ],
            Self::Transcode { path, reason }
            | Self::Verification { path, reason }
            | Self::Transfer { path, reason }
            | Self::Probe { path, reason }
            | Self::InUse { path, reason }
            | Self::TranscodeCancelled { path, reason } => vec![
                ("path", path.display().to_string()),
                ("reason", reason.clone()),
            ],
            Self::TranscodeTimeout { path, timeout_secs } => vec![
                ("path", path.display().to_string()),
                ("timeout_secs", timeout_secs.to_string()),
            ],
            Self::TranscodeStalled {
                path,
                stall_timeout_secs,
            } => vec![
                ("path", path.display().to_string()),
                ("stall_timeout_secs", stall_timeout_secs.to_string()),
            ],
            Self::MissingDependency(tool) => vec![("tool", tool.clone())],
            Self::InsufficientSpace {
                path,
                needed,
                available,
            } => vec![
                ("path", path.display().to_string()),
                ("needed", needed.clone()),
                ("available", available.clone()),
            ],
            Self::Database(db_err) => vec![("db_error", db_err.to_string())],
            Self::Io(io_err) => vec![("io_error", io_err.to_string())],
            Self::Json(json_err) => vec![("json_error", json_err.to_string())],
            Self::Toml(toml_err) => vec![("toml_error", toml_err.to_string())],
            Self::Nix(nix_err) => vec![("nix_error", nix_err.to_string())],
            Self::NoMediaDirectories | Self::Shutdown | Self::Paused => Vec::new(),
        }
    }
}

pub type Result<T> = std::result::Result<T, WatchdogError>;
