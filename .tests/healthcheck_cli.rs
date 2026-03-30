use rusqlite::Connection;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt};

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

fn add_mock_tools(cmd: &mut Command) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for tool in ["ffprobe", "HandBrakeCLI", "rsync"] {
        let path = dir.path().join(tool);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo \"{tool} test-stub 1.0\"\nelse\n  echo \"{tool} test-stub\"\nfi\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).unwrap();
        }
    }

    let existing_path = std::env::var("PATH").unwrap_or_default();
    let combined_path = if existing_path.is_empty() {
        dir.path().display().to_string()
    } else {
        format!("{}:{}", dir.path().display(), existing_path)
    };
    cmd.env("PATH", combined_path);
    dir
}

fn write_basic_config() -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let config_path = dir.path().join("watchdog.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(
        file,
        r#"local_mode = false

[nfs]
server = "127.0.0.1"

[[shares]]
name = "movies"
remote_path = "/remote/movies"
local_mount = "/tmp/nonexistent-mount"

[transcode]
max_average_bitrate_mbps = 25.0
target_codec = "hevc"
preset_file = "presets/HEVC_MKV.json"
preset_name = "HEVC_MKV"
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

fn inspected_count(config_path: &std::path::Path) -> i64 {
    let cfg_text = std::fs::read_to_string(config_path).unwrap();
    let value: toml::Value = toml::from_str(&cfg_text).unwrap();
    let db_path = value
        .get("paths")
        .and_then(|v| v.get("database"))
        .and_then(|v| v.as_str())
        .unwrap();
    let conn = Connection::open(db_path).unwrap();
    conn.query_row("SELECT COUNT(*) FROM inspected_files", [], |row| row.get(0))
        .unwrap()
}

fn write_invalid_config() -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let config_path = dir.path().join("watchdog.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(
        file,
        r#"local_mode = false

[nfs]
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
        r#"local_mode = false

[nfs]
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

fn write_local_mode_config(with_snapshot: bool) -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let media_dir = dir.path().join("media");
    std::fs::create_dir_all(&media_dir).unwrap();
    let snapshot_path = dir.path().join("status.json");
    if with_snapshot {
        std::fs::write(
            &snapshot_path,
            r#"{
  "phase": "Idle",
  "nfs_healthy": true,
  "local_mode": true,
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
        r#"local_mode = true

[nfs]
server = ""

[[shares]]
name = "movies"
remote_path = ""
local_mount = "{}"

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
        media_dir.display(),
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

fn write_nfs_download_area_config(with_snapshot: bool) -> TestConfigFile {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watchdog.db");
    let share_root = dir.path().join("downloads").join("movies");
    std::fs::create_dir_all(&share_root).unwrap();
    let snapshot_path = dir.path().join("status.json");
    if with_snapshot {
        std::fs::write(
            &snapshot_path,
            r#"{
  "phase": "Idle",
  "nfs_healthy": true,
  "local_mode": false,
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
        r#"local_mode = false

[nfs]
server = "127.0.0.1"

[[shares]]
name = "movies"
remote_path = "/remote/movies"
local_mount = "{}"

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
        share_root.display(),
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

fn seed_stale_worker_run_mode(config_path: &std::path::Path, run_mode: &str) {
    let db_path = config_path.parent().unwrap().join("watchdog.db");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS service_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            consecutive_pass_failures INTEGER NOT NULL DEFAULT 0,
            last_pass_failure_code TEXT,
            auto_paused_at TEXT,
            auto_pause_reason TEXT,
            worker_pid INTEGER,
            worker_run_mode TEXT
        );
        INSERT INTO service_state (id, consecutive_pass_failures, worker_pid, worker_run_mode)
        VALUES (1, 0, NULL, 'watchdog')
        ON CONFLICT(id) DO NOTHING;
        ",
    )
    .unwrap();
    conn.execute(
        "UPDATE service_state SET worker_pid = NULL, worker_run_mode = ?1 WHERE id = 1",
        [run_mode],
    )
    .unwrap();
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
fn healthcheck_local_mode_succeeds_without_nfs_overrides() {
    let cfg = write_local_mode_config(false);
    let mut cmd = Command::new(bin_path());
    let _tools = add_mock_tools(&mut cmd);
    let output = cmd
        .args(["--healthcheck", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("storage_mode: local"));
    assert!(text.contains("share_roots=true"));
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
fn doctor_local_mode_reports_share_roots() {
    let cfg = write_local_mode_config(true);
    let mut cmd = Command::new(bin_path());
    let _tools = add_mock_tools(&mut cmd);
    let output = cmd
        .args(["--doctor", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("storage_mode: local"));
    assert!(text.contains("share_roots:"));
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

#[test]
fn status_json_local_mode_reports_local_storage_mode() {
    let cfg = write_local_mode_config(true);
    let output = Command::new(bin_path())
        .args(["--status-json", "--config", cfg.path.to_str().unwrap()])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let json = parse_json_output(&output);
    assert_eq!(json["status"], "ok");
    assert_eq!(json["local_mode"], true);
}

#[test]
fn status_json_warns_about_download_area_in_nfs_mode() {
    let cfg = write_nfs_download_area_config(true);
    let output = Command::new(bin_path())
        .args(["--status-json", "--config", cfg.path.to_str().unwrap()])
        .env("WATCHDOG_ALLOW_LOCAL_MOUNTS", "1")
        .env("WATCHDOG_RUNTIME_MODE", "local_test")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let json = parse_json_output(&output);
    let warnings = json["local_fs_warnings"].as_array().unwrap();
    assert!(warnings.iter().any(|warning| {
        warning
            .as_str()
            .is_some_and(|text| text.contains("downloads/incomplete area"))
    }));
}

#[test]
fn status_json_ignores_stale_worker_run_mode_without_fresh_snapshot() {
    let cfg = write_status_config(false);
    seed_stale_worker_run_mode(&cfg.path, "precision");

    let output = Command::new(bin_path())
        .args([
            "--simulate",
            "--status-json",
            "--config",
            cfg.path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let json = parse_json_output(&output);
    assert_eq!(json["run_mode"], "watchdog");
}

#[test]
fn healthcheck_json_ignores_stale_worker_run_mode_without_fresh_snapshot() {
    let cfg = write_basic_config();
    seed_stale_worker_run_mode(&cfg.path, "precision");

    let output = Command::new(bin_path())
        .args([
            "--simulate",
            "--healthcheck-json",
            "--config",
            cfg.path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let json = parse_json_output(&output);
    assert_eq!(json["run_mode"], "watchdog");
}

#[test]
fn clear_scan_cache_flag_clears_inspected_files() {
    let cfg = write_basic_config();
    let db_path = cfg.path.parent().unwrap().join("watchdog.db");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS inspected_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_size INTEGER,
                file_mtime REAL,
                inspected_at TEXT NOT NULL
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO inspected_files (file_path, file_size, file_mtime, inspected_at)
             VALUES ('/a.mkv', 1, 1.0, '2026-01-01T00:00:00')",
            [],
        )
        .unwrap();
    }

    assert_eq!(inspected_count(&cfg.path), 1);
    let output = Command::new(bin_path())
        .args([
            "--config",
            cfg.path.to_str().unwrap(),
            "--clear-scan-cache",
            "--quarantine-list",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(inspected_count(&cfg.path), 0);
}

#[test]
fn clear_scan_cache_rejected_in_read_only_modes() {
    let output = Command::new(bin_path())
        .args(["--simulate", "--dry-run", "--clear-scan-cache"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--clear-scan-cache"));
}
