use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_watchdog")
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
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
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
