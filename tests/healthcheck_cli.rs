use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_watchdog")
}

fn parse_json_output(output: &std::process::Output) -> serde_json::Value {
    fn try_parse(text: &str) -> Option<serde_json::Value> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            return Some(v);
        }
        let start = trimmed.find('{')?;
        let end = trimmed.rfind('}')?;
        if end < start {
            return None;
        }
        serde_json::from_str::<serde_json::Value>(&trimmed[start..=end]).ok()
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(v) = try_parse(&stdout) {
        return v;
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if let Some(v) = try_parse(&stderr) {
        return v;
    }

    panic!(
        "Failed to parse JSON output\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout,
        stderr
    );
}

struct TestConfigFile {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

fn write_basic_config() -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let config_path = dir.path().join("watchdog.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(
        file,
        r#"[nfs]
server = "127.0.0.1"

[[shares]]
name = "movies"
remote_path = "/remote/movies"
local_mount = "/tmp/nonexistent-mount"

[transcode]
max_average_bitrate_mbps = 25.0
target_codec = "av1"
preset_file = "AV1_MKV.json"
preset_name = "AV1_MKV"
timeout_seconds = 100
max_retries = 1
min_free_space_multiplier = 2.0

[scan]
video_extensions = [".mkv"]
interval_seconds = 300

[safety]
min_file_age_seconds = 0
pause_file = "watchdog.pause"
max_failures_before_cooldown = 3
cooldown_base_seconds = 300
cooldown_max_seconds = 86400

[paths]
transcode_temp = "/tmp"
database = "{}"
log_dir = "logs"
status_snapshot = ""
"#,
        db_path.display()
    )
    .unwrap();
    TestConfigFile {
        _dir: dir,
        path: config_path,
    }
}

fn write_invalid_config() -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let config_path = dir.path().join("watchdog.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(
        file,
        r#"[nfs]
server = "127.0.0.1"

[[shares]]
name = "movies"
remote_path = ""
local_mount = "/tmp/nonexistent-mount"

[paths]
database = "{}"
"#,
        db_path.display()
    )
    .unwrap();
    TestConfigFile {
        _dir: dir,
        path: config_path,
    }
}

fn write_status_config(with_snapshot: bool) -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let snapshot_path = dir.path().join("status.json");
    if with_snapshot {
        std::fs::write(
            &snapshot_path,
            r#"{
  "phase": "Idle",
  "nfs_healthy": true,
  "queue_position": 0,
  "queue_total": 0,
  "current_file": null,
  "unhealthy_shares": [],
  "reliability": {
    "scan_timeouts": 0,
    "last_failure_code": null
  }
}"#,
        )
        .unwrap();
    }
    let config_path = dir.path().join("watchdog.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(
        file,
        r#"[nfs]
server = "127.0.0.1"

[[shares]]
name = "movies"
remote_path = "/remote/movies"
local_mount = "/tmp/nonexistent-mount"

[scan]
video_extensions = [".mkv"]
interval_seconds = 300

[safety]
status_snapshot_stale_seconds = 30
pause_file = "watchdog.pause"
max_failures_before_cooldown = 3
cooldown_base_seconds = 300
cooldown_max_seconds = 86400
max_consecutive_pass_failures = 3

[paths]
database = "{}"
transcode_temp = "/tmp"
status_snapshot = "{}"
"#,
        db_path.display(),
        if with_snapshot {
            snapshot_path.to_string_lossy().to_string()
        } else {
            String::new()
        },
    )
    .unwrap();
    TestConfigFile {
        _dir: dir,
        path: config_path,
    }
}

#[test]
fn healthcheck_healthy_simulate_returns_zero() {
    let output = Command::new(bin_path())
        .args(["--simulate", "--healthcheck"])
        .output()
        .unwrap();
    assert!(output.status.success());
}

#[test]
fn healthcheck_json_contains_exit_code_and_checks() {
    let output = Command::new(bin_path())
        .args(["--simulate", "--healthcheck-json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json = parse_json_output(&output);
    assert_eq!(json["exit_code"], 0);
    assert_eq!(json["checks"]["config"], true);
    assert_eq!(json["checks"]["db"], true);
}

#[test]
fn healthcheck_missing_deps_returns_22() {
    let cfg = write_basic_config();
    let output = Command::new(bin_path())
        .args(["--healthcheck", "--config", cfg.path.to_str().unwrap()])
        .env("WATCHDOG_HEALTHCHECK_FORCE_MISSING_DEPS", "ffprobe")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(22));
}

#[test]
fn healthcheck_unhealthy_mount_returns_23() {
    let cfg = write_basic_config();
    let output = Command::new(bin_path())
        .args(["--healthcheck", "--config", cfg.path.to_str().unwrap()])
        .env("WATCHDOG_HEALTHCHECK_FORCE_MISSING_DEPS", "")
        .env("WATCHDOG_HEALTHCHECK_FORCE_UNHEALTHY_SHARES", "movies")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(23));
}

#[test]
fn healthcheck_invalid_config_returns_20() {
    let output = Command::new(bin_path())
        .args([
            "--healthcheck",
            "--config",
            "/definitely/not/found/watchdog.toml",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(20));
}

#[test]
fn once_simulate_returns_zero() {
    let output = Command::new(bin_path())
        .args(["--simulate", "--once", "--headless"])
        .env("WATCHDOG_SIM_MAX_FILES_PER_SHARE", "1")
        .output()
        .unwrap();
    assert!(output.status.success());
}

#[test]
fn doctor_simulate_returns_zero() {
    let output = Command::new(bin_path())
        .args(["--simulate", "--doctor"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("watchdog_doctor"));
}

#[test]
fn doctor_with_invalid_config_still_runs_diagnostics() {
    let cfg = write_invalid_config();
    let output = Command::new(bin_path())
        .args(["--doctor", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));

    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("watchdog_doctor"));
    assert!(text.contains("config_errors"));
    assert!(text.contains("empty remote_path"));
}

#[test]
fn status_json_fresh_snapshot_returns_zero() {
    let cfg = write_status_config(true);
    let output = Command::new(bin_path())
        .args(["--status-json", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    let json = parse_json_output(&output);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["status_freshness"], "fresh");
}

#[test]
fn status_json_missing_snapshot_returns_degraded() {
    let cfg = write_status_config(false);
    let output = Command::new(bin_path())
        .args(["--status-json", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(30));
    let json = parse_json_output(&output);
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["status_freshness"], "missing");
}
