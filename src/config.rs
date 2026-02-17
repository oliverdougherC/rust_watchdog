use crate::error::{Result, WatchdogError};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub nfs: NfsConfig,

    #[serde(default)]
    pub shares: Vec<ShareConfig>,

    #[serde(default)]
    pub transcode: TranscodeConfig,

    #[serde(default)]
    pub scan: ScanConfig,

    #[serde(default)]
    pub paths: PathsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NfsConfig {
    #[serde(default = "default_nfs_server")]
    pub server: String,
}

impl Default for NfsConfig {
    fn default() -> Self {
        Self {
            server: default_nfs_server(),
        }
    }
}

fn default_nfs_server() -> String {
    "192.168.1.244".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShareConfig {
    pub name: String,
    pub remote_path: String,
    pub local_mount: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscodeConfig {
    #[serde(default = "default_max_bitrate")]
    pub max_average_bitrate_mbps: f64,

    #[serde(default = "default_target_codec")]
    pub target_codec: String,

    #[serde(default = "default_preset_file")]
    pub preset_file: String,

    #[serde(default = "default_preset_name")]
    pub preset_name: String,

    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_min_free_space_multiplier")]
    pub min_free_space_multiplier: f64,
}

impl Default for TranscodeConfig {
    fn default() -> Self {
        Self {
            max_average_bitrate_mbps: default_max_bitrate(),
            target_codec: default_target_codec(),
            preset_file: default_preset_file(),
            preset_name: default_preset_name(),
            timeout_seconds: default_timeout(),
            max_retries: default_max_retries(),
            min_free_space_multiplier: default_min_free_space_multiplier(),
        }
    }
}

fn default_max_bitrate() -> f64 {
    25.0
}
fn default_target_codec() -> String {
    "av1".to_string()
}
fn default_preset_file() -> String {
    "AV1_MKV.json".to_string()
}
fn default_preset_name() -> String {
    "AV1_MKV".to_string()
}
fn default_timeout() -> u64 {
    18000
}
fn default_max_retries() -> u32 {
    1
}
fn default_min_free_space_multiplier() -> f64 {
    2.0
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScanConfig {
    #[serde(default = "default_extensions")]
    pub video_extensions: Vec<String>,

    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            video_extensions: default_extensions(),
            interval_seconds: default_interval(),
        }
    }
}

fn default_extensions() -> Vec<String> {
    vec![
        ".mkv".to_string(),
        ".mp4".to_string(),
        ".avi".to_string(),
        ".mov".to_string(),
        ".webm".to_string(),
    ]
}

fn default_interval() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    #[serde(default = "default_transcode_temp")]
    pub transcode_temp: String,

    #[serde(default = "default_database")]
    pub database: String,

    #[allow(dead_code)]
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            transcode_temp: default_transcode_temp(),
            database: default_database(),
            log_dir: default_log_dir(),
        }
    }
}

fn default_transcode_temp() -> String {
    "/Volumes/External/tmp/".to_string()
}
fn default_database() -> String {
    "watchdog.db".to_string()
}
fn default_log_dir() -> String {
    "logs".to_string()
}

impl Config {
    /// Load config from a TOML file. Relative paths in the config are resolved
    /// relative to the config file's parent directory.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(WatchdogError::ConfigNotFound(path.to_path_buf()));
        }
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Create a default config (useful for simulation mode).
    pub fn default_config() -> Self {
        Config {
            nfs: NfsConfig::default(),
            shares: vec![
                ShareConfig {
                    name: "movies".to_string(),
                    remote_path: "/mnt/DataStore/share/jellyfin/jfmedia/Movies".to_string(),
                    local_mount: "/Volumes/JellyfinMovies".to_string(),
                },
                ShareConfig {
                    name: "tv".to_string(),
                    remote_path: "/mnt/DataStore/share/jellyfin/jfmedia/TV".to_string(),
                    local_mount: "/Volumes/JellyfinTV".to_string(),
                },
            ],
            transcode: TranscodeConfig::default(),
            scan: ScanConfig::default(),
            paths: PathsConfig::default(),
        }
    }

    /// Resolve a potentially relative path against a base directory.
    pub fn resolve_path(&self, base_dir: &Path, path: &str) -> PathBuf {
        let p = Path::new(path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            base_dir.join(p)
        }
    }

    /// Validate the config and return any errors.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.shares.is_empty() {
            errors.push("No shares configured".to_string());
        }
        for share in &self.shares {
            if share.name.is_empty() {
                errors.push("Share has empty name".to_string());
            }
            if share.local_mount.is_empty() {
                errors.push(format!("Share '{}' has empty local_mount", share.name));
            }
        }
        if self.transcode.max_average_bitrate_mbps <= 0.0 {
            errors.push("max_average_bitrate_mbps must be positive".to_string());
        }
        if self.scan.video_extensions.is_empty() {
            errors.push("No video extensions configured".to_string());
        }

        errors
    }
}
