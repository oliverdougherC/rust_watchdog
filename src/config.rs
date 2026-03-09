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

    #[serde(default)]
    pub safety: SafetyConfig,

    #[serde(default)]
    pub notify: NotifyConfig,
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

    #[serde(default)]
    pub include_globs: Vec<String>,

    #[serde(default)]
    pub exclude_globs: Vec<String>,

    #[serde(default)]
    pub max_files_per_pass: u32,

    #[serde(default)]
    pub probe_workers: u32,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            video_extensions: default_extensions(),
            interval_seconds: default_interval(),
            include_globs: Vec::new(),
            exclude_globs: Vec::new(),
            max_files_per_pass: 0,
            probe_workers: 0,
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
pub struct SafetyConfig {
    #[serde(default = "default_min_file_age_seconds")]
    pub min_file_age_seconds: u64,

    #[serde(default = "default_pause_file")]
    pub pause_file: String,

    #[serde(default = "default_max_failures_before_cooldown")]
    pub max_failures_before_cooldown: u32,

    #[serde(default = "default_cooldown_base_seconds")]
    pub cooldown_base_seconds: u64,

    #[serde(default = "default_cooldown_max_seconds")]
    pub cooldown_max_seconds: u64,

    #[serde(default)]
    pub in_use_guard_enabled: bool,

    #[serde(default = "default_in_use_guard_command")]
    pub in_use_guard_command: String,

    #[serde(default = "default_recovery_scan_interval_seconds")]
    pub recovery_scan_interval_seconds: u64,

    #[serde(default = "default_share_scan_timeout_seconds")]
    pub share_scan_timeout_seconds: u64,

    #[serde(default = "default_max_consecutive_pass_failures")]
    pub max_consecutive_pass_failures: u32,

    #[serde(default = "default_auto_pause_on_pass_failures")]
    pub auto_pause_on_pass_failures: bool,

    #[serde(default = "default_quarantine_after_failures")]
    pub quarantine_after_failures: u32,

    #[serde(default = "default_quarantine_failure_codes")]
    pub quarantine_failure_codes: Vec<String>,

    #[serde(default = "default_status_snapshot_stale_seconds")]
    pub status_snapshot_stale_seconds: u64,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            min_file_age_seconds: default_min_file_age_seconds(),
            pause_file: default_pause_file(),
            max_failures_before_cooldown: default_max_failures_before_cooldown(),
            cooldown_base_seconds: default_cooldown_base_seconds(),
            cooldown_max_seconds: default_cooldown_max_seconds(),
            in_use_guard_enabled: false,
            in_use_guard_command: default_in_use_guard_command(),
            recovery_scan_interval_seconds: default_recovery_scan_interval_seconds(),
            share_scan_timeout_seconds: default_share_scan_timeout_seconds(),
            max_consecutive_pass_failures: default_max_consecutive_pass_failures(),
            auto_pause_on_pass_failures: default_auto_pause_on_pass_failures(),
            quarantine_after_failures: default_quarantine_after_failures(),
            quarantine_failure_codes: default_quarantine_failure_codes(),
            status_snapshot_stale_seconds: default_status_snapshot_stale_seconds(),
        }
    }
}

fn default_min_file_age_seconds() -> u64 {
    0
}
fn default_pause_file() -> String {
    "watchdog.pause".to_string()
}
fn default_max_failures_before_cooldown() -> u32 {
    3
}
fn default_cooldown_base_seconds() -> u64 {
    300
}
fn default_cooldown_max_seconds() -> u64 {
    86_400
}
fn default_in_use_guard_command() -> String {
    "lsof".to_string()
}
fn default_recovery_scan_interval_seconds() -> u64 {
    43_200
}

fn default_share_scan_timeout_seconds() -> u64 {
    180
}

fn default_max_consecutive_pass_failures() -> u32 {
    3
}

fn default_auto_pause_on_pass_failures() -> bool {
    true
}

fn default_quarantine_after_failures() -> u32 {
    8
}

fn default_quarantine_failure_codes() -> Vec<String> {
    vec![
        "transcode_failed".to_string(),
        "transcode_error".to_string(),
        "verification_failed".to_string(),
        "verification_error".to_string(),
        "safe_replace_failed".to_string(),
        "safe_replace_error".to_string(),
        "source_changed_during_transcode".to_string(),
    ]
}

fn default_status_snapshot_stale_seconds() -> u64 {
    30
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

    #[serde(default)]
    pub status_snapshot: String,

    #[serde(default)]
    pub event_journal: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotifyConfig {
    #[serde(default)]
    pub webhook_url: String,

    #[serde(default = "default_notify_events")]
    pub events: Vec<String>,

    #[serde(default = "default_notify_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            webhook_url: String::new(),
            events: default_notify_events(),
            timeout_seconds: default_notify_timeout_seconds(),
        }
    }
}

fn default_notify_events() -> Vec<String> {
    vec![
        "pass_failure_summary".to_string(),
        "replacement_summary".to_string(),
        "cooldown_alert".to_string(),
    ]
}

fn default_notify_timeout_seconds() -> u64 {
    5
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            transcode_temp: default_transcode_temp(),
            database: default_database(),
            log_dir: default_log_dir(),
            status_snapshot: String::new(),
            event_journal: String::new(),
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
        let mut config: Config = toml::from_str(&content)?;
        config.normalize();
        Ok(config)
    }

    /// Create a default config (useful for simulation mode).
    pub fn default_config() -> Self {
        let mut config = Config {
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
            safety: SafetyConfig::default(),
            notify: NotifyConfig::default(),
        };
        config.normalize();
        config
    }

    /// Normalize values to make matching deterministic across user styles.
    pub fn normalize(&mut self) {
        self.transcode.target_codec = self.transcode.target_codec.trim().to_lowercase();
        self.scan.video_extensions = self
            .scan
            .video_extensions
            .iter()
            .map(|ext| {
                let trimmed = ext.trim().to_lowercase();
                if trimmed.starts_with('.') {
                    trimmed
                } else {
                    format!(".{}", trimmed)
                }
            })
            .collect();
        self.scan.include_globs = self
            .scan
            .include_globs
            .iter()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .collect();
        self.scan.exclude_globs = self
            .scan
            .exclude_globs
            .iter()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .collect();
        self.safety.pause_file = self.safety.pause_file.trim().to_string();
        self.safety.in_use_guard_command = self.safety.in_use_guard_command.trim().to_string();
        self.safety.quarantine_failure_codes = self
            .safety
            .quarantine_failure_codes
            .iter()
            .map(|code| code.trim().to_lowercase())
            .filter(|code| !code.is_empty())
            .collect();
        self.paths.status_snapshot = self.paths.status_snapshot.trim().to_string();
        self.paths.event_journal = self.paths.event_journal.trim().to_string();
        self.notify.webhook_url = self.notify.webhook_url.trim().to_string();
        self.notify.events = self
            .notify
            .events
            .iter()
            .map(|e| e.trim().to_lowercase())
            .filter(|e| !e.is_empty())
            .collect();
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
            if share.remote_path.is_empty() {
                errors.push(format!("Share '{}' has empty remote_path", share.name));
            }
            if share.local_mount.is_empty() {
                errors.push(format!("Share '{}' has empty local_mount", share.name));
            } else if !std::path::Path::new(&share.local_mount).is_absolute() {
                errors.push(format!(
                    "Share '{}' local_mount must be an absolute path",
                    share.name
                ));
            }
        }
        {
            let mut seen_names = std::collections::HashSet::new();
            for share in &self.shares {
                if !seen_names.insert(share.name.clone()) {
                    errors.push(format!("Duplicate share name '{}'", share.name));
                }
            }
        }
        {
            let mut seen_mounts = std::collections::HashSet::new();
            for share in &self.shares {
                if !seen_mounts.insert(share.local_mount.clone()) {
                    errors.push(format!(
                        "Duplicate share local_mount '{}'",
                        share.local_mount
                    ));
                }
            }
        }
        if self.transcode.max_average_bitrate_mbps <= 0.0 {
            errors.push("max_average_bitrate_mbps must be positive".to_string());
        }
        if self.transcode.target_codec.trim().is_empty() {
            errors.push("target_codec must not be empty".to_string());
        }
        if self.scan.video_extensions.is_empty() {
            errors.push("No video extensions configured".to_string());
        }
        if self.scan.interval_seconds == 0 {
            errors.push("scan.interval_seconds must be >= 1".to_string());
        }
        if self.scan.video_extensions.iter().any(|e| e.trim() == ".") {
            errors.push("video_extensions must not contain empty extension entries".to_string());
        }
        if self.safety.pause_file.is_empty() {
            errors.push("safety.pause_file must not be empty".to_string());
        }
        if self.safety.max_failures_before_cooldown == 0 {
            errors.push("safety.max_failures_before_cooldown must be >= 1".to_string());
        }
        if self.safety.cooldown_base_seconds == 0 {
            errors.push("safety.cooldown_base_seconds must be >= 1".to_string());
        }
        if self.safety.cooldown_max_seconds < self.safety.cooldown_base_seconds {
            errors.push("safety.cooldown_max_seconds must be >= cooldown_base_seconds".to_string());
        }
        if self.safety.recovery_scan_interval_seconds == 0 {
            errors.push("safety.recovery_scan_interval_seconds must be >= 1".to_string());
        }
        if self.safety.share_scan_timeout_seconds == 0 {
            errors.push("safety.share_scan_timeout_seconds must be >= 1".to_string());
        }
        if self.safety.max_consecutive_pass_failures == 0 {
            errors.push("safety.max_consecutive_pass_failures must be >= 1".to_string());
        }
        if self.safety.quarantine_after_failures == 0 {
            errors.push("safety.quarantine_after_failures must be >= 1".to_string());
        }
        if self.safety.status_snapshot_stale_seconds < 5 {
            errors.push("safety.status_snapshot_stale_seconds must be >= 5".to_string());
        }
        if self.safety.in_use_guard_enabled && self.safety.in_use_guard_command.is_empty() {
            errors.push(
                "safety.in_use_guard_command must not be empty when in_use_guard_enabled=true"
                    .to_string(),
            );
        }
        for code in &self.safety.quarantine_failure_codes {
            let valid = code
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !valid {
                errors.push(format!(
                    "safety.quarantine_failure_codes contains invalid code '{}'",
                    code
                ));
            }
        }
        if !(self.notify.webhook_url.is_empty()
            || self.notify.webhook_url.starts_with("http://")
            || self.notify.webhook_url.starts_with("https://"))
        {
            errors.push("notify.webhook_url must start with http:// or https://".to_string());
        }
        if self.notify.timeout_seconds == 0 {
            errors.push("notify.timeout_seconds must be >= 1".to_string());
        }
        let allowed_events = [
            "pass_failure_summary",
            "replacement_summary",
            "cooldown_alert",
        ];
        for event in &self.notify.events {
            if !allowed_events.contains(&event.as_str()) {
                errors.push(format!(
                    "notify.events contains unsupported event '{}'",
                    event
                ));
            }
        }
        for glob in &self.scan.include_globs {
            if let Err(e) = globset::Glob::new(glob) {
                errors.push(format!("Invalid include_glob '{}': {}", glob, e));
            }
        }
        for glob in &self.scan.exclude_globs {
            if let Err(e) = globset::Glob::new(glob) {
                errors.push(format!("Invalid exclude_glob '{}': {}", glob, e));
            }
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_codec_and_extensions() {
        let mut cfg = Config::default_config();
        cfg.transcode.target_codec = "  AV1  ".to_string();
        cfg.scan.video_extensions = vec!["MKV".to_string(), ".Mp4".to_string()];
        cfg.scan.include_globs = vec!["  *.mkv  ".to_string()];
        cfg.safety.in_use_guard_command = "  lsof  ".to_string();
        cfg.safety.quarantine_failure_codes =
            vec!["  TRANSCode_Failed ".to_string(), " ".to_string()];
        cfg.paths.status_snapshot = "  status.json  ".to_string();
        cfg.paths.event_journal = "  events.ndjson  ".to_string();
        cfg.normalize();

        assert_eq!(cfg.transcode.target_codec, "av1");
        assert_eq!(cfg.scan.video_extensions, vec![".mkv", ".mp4"]);
        assert_eq!(cfg.scan.include_globs, vec!["*.mkv"]);
        assert_eq!(cfg.safety.in_use_guard_command, "lsof");
        assert_eq!(
            cfg.safety.quarantine_failure_codes,
            vec!["transcode_failed".to_string()]
        );
        assert_eq!(cfg.paths.status_snapshot, "status.json");
        assert_eq!(cfg.paths.event_journal, "events.ndjson");
    }

    #[test]
    fn test_validate_duplicate_shares() {
        let mut cfg = Config::default_config();
        cfg.shares = vec![
            ShareConfig {
                name: "movies".to_string(),
                remote_path: "/a".to_string(),
                local_mount: "/x".to_string(),
            },
            ShareConfig {
                name: "movies".to_string(),
                remote_path: "/b".to_string(),
                local_mount: "/x".to_string(),
            },
        ];

        let errs = cfg.validate();
        assert!(errs.iter().any(|e| e.contains("Duplicate share name")));
        assert!(errs
            .iter()
            .any(|e| e.contains("Duplicate share local_mount")));
    }

    #[test]
    fn test_validate_cooldown_constraints() {
        let mut cfg = Config::default_config();
        cfg.safety.max_failures_before_cooldown = 0;
        cfg.safety.cooldown_base_seconds = 100;
        cfg.safety.cooldown_max_seconds = 10;
        cfg.safety.recovery_scan_interval_seconds = 0;
        cfg.safety.share_scan_timeout_seconds = 0;
        cfg.safety.max_consecutive_pass_failures = 0;
        cfg.safety.quarantine_after_failures = 0;
        cfg.safety.status_snapshot_stale_seconds = 4;
        let errs = cfg.validate();
        assert!(errs
            .iter()
            .any(|e| e.contains("max_failures_before_cooldown")));
        assert!(errs.iter().any(|e| e.contains("cooldown_max_seconds")));
        assert!(errs
            .iter()
            .any(|e| e.contains("recovery_scan_interval_seconds")));
        assert!(errs
            .iter()
            .any(|e| e.contains("share_scan_timeout_seconds")));
        assert!(errs
            .iter()
            .any(|e| e.contains("max_consecutive_pass_failures")));
        assert!(errs.iter().any(|e| e.contains("quarantine_after_failures")));
        assert!(errs
            .iter()
            .any(|e| e.contains("status_snapshot_stale_seconds")));
    }

    #[test]
    fn test_validate_quarantine_failure_code_format() {
        let mut cfg = Config::default_config();
        cfg.safety.quarantine_failure_codes = vec!["bad-code".to_string()];
        let errs = cfg.validate();
        assert!(errs.iter().any(|e| e.contains("quarantine_failure_codes")));
    }

    #[test]
    fn test_validate_invalid_glob() {
        let mut cfg = Config::default_config();
        cfg.scan.include_globs = vec!["[".to_string()];
        let errs = cfg.validate();
        assert!(errs.iter().any(|e| e.contains("Invalid include_glob")));
    }

    #[test]
    fn test_validate_in_use_guard_command_when_enabled() {
        let mut cfg = Config::default_config();
        cfg.safety.in_use_guard_enabled = true;
        cfg.safety.in_use_guard_command = String::new();
        let errs = cfg.validate();
        assert!(errs
            .iter()
            .any(|e| e.contains("in_use_guard_command must not be empty")));
    }

    #[test]
    fn test_validate_notify_settings() {
        let mut cfg = Config::default_config();
        cfg.notify.webhook_url = "ftp://example.com".to_string();
        cfg.notify.events = vec!["unknown".to_string()];
        cfg.notify.timeout_seconds = 0;
        let errs = cfg.validate();
        assert!(errs.iter().any(|e| e.contains("notify.webhook_url")));
        assert!(errs.iter().any(|e| e.contains("notify.events")));
        assert!(errs.iter().any(|e| e.contains("notify.timeout_seconds")));
    }

    #[test]
    fn test_validate_scan_interval_and_mount_path() {
        let mut cfg = Config::default_config();
        cfg.scan.interval_seconds = 0;
        cfg.shares[0].local_mount = "relative/mount".to_string();

        let errs = cfg.validate();
        assert!(errs.iter().any(|e| e.contains("scan.interval_seconds")));
        assert!(errs
            .iter()
            .any(|e| e.contains("local_mount must be an absolute path")));
    }
}
