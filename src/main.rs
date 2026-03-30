use anyhow::Context;
use clap::Parser;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use watchdog::config::{
    path_is_same_or_nested, write_metrics_to_path, Config, MetricsConfig, DEFAULT_CONFIG_PATH,
    LEGACY_DEFAULT_CONFIG_PATH,
};
use watchdog::db::WatchdogDb;
use watchdog::diagnostics::{log_anyhow_error, print_anyhow_error_report};
use watchdog::event_journal::{append_event, resolve_event_journal_path};
use watchdog::in_use::CommandInUseDetector;
use watchdog::metrics::{
    flush_global_metrics_now, set_global_metrics_persistence, sync_cumulative_metrics_state,
    CumulativeMetrics, MetricsPersistence,
};
use watchdog::nfs::SystemMountManager;
use watchdog::pipeline::{run_pipeline_loop, set_tui_log_event_path, PipelineDeps};
use watchdog::probe::FfprobeProber;
use watchdog::process::terminate_registered_subprocesses;
use watchdog::process::{
    describe_exit_status, format_command_for_log, run_command, summarize_output_tail,
    terminate_subprocess, RunOptions,
};
use watchdog::runtime::resolve_runtime_paths;
use watchdog::scanner::RealFileSystem;
use watchdog::simulate::create_simulated_deps;
use watchdog::state::{RunMode, StateManager};
use watchdog::status_snapshot::{resolve_status_snapshot_path, run_status_snapshot_task};
use watchdog::traits::MountManager;
use watchdog::transcode::{resolve_preset_path, HandBrakeTranscoder, PresetContract};
use watchdog::transfer::RsyncTransfer;
use watchdog::{tui, util};

const HC_OK: i32 = 0;
const HC_CONFIG_FAIL: i32 = 20;
const HC_DB_FAIL: i32 = 21;
const HC_DEPS_FAIL: i32 = 22;
const HC_NFS_FAIL: i32 = 23;
const HC_INTERNAL_FAIL: i32 = 24;
const STATUS_OK: i32 = 0;
const STATUS_DEGRADED: i32 = 30;
const STATUS_UNHEALTHY: i32 = 31;
const STATUS_INTERNAL_FAIL: i32 = 32;

#[derive(Debug, serde::Serialize)]
struct HealthcheckChecks {
    config: bool,
    db: bool,
    dependencies: bool,
    nfs: bool,
}

#[derive(Debug, serde::Serialize)]
struct HealthcheckResult {
    status: String,
    exit_code: i32,
    local_mode: bool,
    checks: HealthcheckChecks,
    paused: bool,
    cooldown_files: i64,
    mode: String,
    run_mode: String,
    missing_dependencies: Vec<String>,
    unhealthy_shares: Vec<String>,
    db_query_ok: bool,
    dependency_diagnostics: Vec<HealthcheckDependency>,
    mount_checks: Vec<HealthcheckMountCheck>,
    latest_failure_code: Option<String>,
    snapshot_age_seconds: Option<u64>,
    status_freshness: String,
    consecutive_pass_failures: u32,
    auto_pause_reason: Option<String>,
    quarantine_count: i64,
    skipped_temporary: u64,
    skipped_unstable: u64,
    skipped_hardlinked: u64,
    local_fs_warnings: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct HealthcheckDependency {
    name: String,
    found: bool,
    version: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct HealthcheckMountCheck {
    share: String,
    healthy: bool,
    latency_ms: u128,
}

#[derive(Debug)]
struct DoctorDependency {
    name: String,
    found: bool,
    version: Option<String>,
}

#[derive(Debug)]
struct DoctorMountCheck {
    share: String,
    mount: String,
    healthy: bool,
    latency_ms: u128,
}

#[derive(Debug, serde::Serialize)]
struct StatusResult {
    status: String,
    exit_code: i32,
    local_mode: bool,
    mode: String,
    run_mode: String,
    paused: bool,
    auto_paused: bool,
    auto_pause_reason: Option<String>,
    nfs_healthy: Option<bool>,
    unhealthy_shares: Vec<String>,
    phase: Option<String>,
    queue_position: Option<u64>,
    queue_total: Option<u64>,
    current_file: Option<String>,
    cooldown_files: i64,
    scan_timeouts: u64,
    last_failure_code: Option<String>,
    quarantine_count: i64,
    consecutive_pass_failures: u32,
    status_freshness: String,
    snapshot_age_seconds: Option<u64>,
    snapshot_path: Option<String>,
    skipped_temporary: u64,
    skipped_unstable: u64,
    skipped_hardlinked: u64,
    local_fs_warnings: Vec<String>,
}

#[derive(Debug)]
struct SnapshotDiagnostics {
    freshness: String,
    age_seconds: Option<u64>,
    snapshot: Option<Value>,
    snapshot_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct LocalDeploymentDiagnostics {
    resolved_temp_dir: PathBuf,
    temp_writable: bool,
    temp_inside_share_root: bool,
    temp_fs_type: Option<String>,
    share_fs_types: Vec<(String, PathBuf, Option<String>)>,
    local_fs_warnings: Vec<String>,
}

fn storage_mode_name(local_mode: bool) -> &'static str {
    if local_mode {
        "local"
    } else {
        "nfs"
    }
}

fn share_health_check_label(local_mode: bool) -> &'static str {
    if local_mode {
        "share_roots"
    } else {
        "nfs"
    }
}

fn share_health_text_label(local_mode: bool) -> &'static str {
    if local_mode {
        "local_roots_healthy"
    } else {
        "nfs_healthy"
    }
}

fn share_health_subject_label(local_mode: bool) -> &'static str {
    if local_mode {
        "share_root"
    } else {
        "mount"
    }
}

fn share_health_section_label(local_mode: bool) -> &'static str {
    if local_mode {
        "share_roots"
    } else {
        "mounts"
    }
}

fn host_share_root_is_healthy(path: &Path) -> bool {
    if !path.exists() || !path.is_dir() {
        return false;
    }

    match std::fs::read_dir(path) {
        Ok(mut entries) => match entries.next() {
            Some(Ok(_)) | None => true,
            Some(Err(_)) => false,
        },
        Err(_) => false,
    }
}

fn parse_mount_entry(line: &str) -> Option<(PathBuf, String)> {
    if let Some((_, right)) = line.split_once(" on ") {
        if let Some((mount, rest)) = right.split_once(" (") {
            let fs_type = rest
                .split([',', ')'])
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !fs_type.is_empty() {
                return Some((PathBuf::from(mount.trim()), fs_type));
            }
        }
        if let Some((mount, rest)) = right.split_once(" type ") {
            let fs_type = rest
                .split([' ', '('])
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !fs_type.is_empty() {
                return Some((PathBuf::from(mount.trim()), fs_type));
            }
        }
    }
    None
}

fn filesystem_type_for_path(path: &Path) -> Option<String> {
    let cmd = Command::new("mount");
    let out = run_command(
        cmd,
        RunOptions {
            timeout: Some(Duration::from_secs(3)),
            stdout_limit: 256 * 1024,
            stderr_limit: 4096,
            ..RunOptions::default()
        },
    )
    .ok()?;
    if out.timed_out || !out.status.success() {
        return None;
    }
    let normalized = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(parse_mount_entry)
        .filter(|(mount_point, _)| normalized.starts_with(mount_point))
        .max_by_key(|(mount_point, _)| mount_point.components().count())
        .map(|(_, fs_type)| fs_type)
}

fn is_network_filesystem(fs_type: &str) -> bool {
    matches!(
        fs_type,
        "nfs" | "nfs4" | "smbfs" | "cifs" | "afpfs" | "webdav" | "fuse.sshfs"
    )
}

fn path_suggests_download_area(path: &Path) -> bool {
    let lowered = path.to_string_lossy().to_ascii_lowercase();
    [
        "download",
        "incomplete",
        "torrent",
        "qbittorrent",
        "sabnzbd",
        "usenet",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
}

fn nearest_existing_path(path: &Path) -> Option<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn path_is_writable(path: &Path) -> bool {
    let Some(target) = nearest_existing_path(path) else {
        return false;
    };
    nix::unistd::access(&target, nix::unistd::AccessFlags::W_OK).is_ok()
}

fn collect_local_deployment_diagnostics(
    config: &Config,
    base_dir: &Path,
) -> LocalDeploymentDiagnostics {
    let resolved_temp_dir = config.resolve_path(base_dir, &config.paths.transcode_temp);
    let temp_fs_type = filesystem_type_for_path(&resolved_temp_dir);
    let temp_writable = path_is_writable(&resolved_temp_dir);
    let mut local_fs_warnings = Vec::new();
    let mut share_fs_types = Vec::new();
    let temp_inside_share_root = config.shares.iter().any(|share| {
        let root = Path::new(&share.local_mount);
        path_is_same_or_nested(root, &resolved_temp_dir)
    });

    for share in &config.shares {
        let root = PathBuf::from(&share.local_mount);
        let fs_type = filesystem_type_for_path(&root);
        if path_suggests_download_area(&root) {
            local_fs_warnings.push(format!(
                "share '{}' path looks like a downloads/incomplete area ({})",
                share.name,
                root.display()
            ));
        }
        if config.local_mode {
            if let Some(ref fs_type) = fs_type {
                if is_network_filesystem(fs_type) {
                    local_fs_warnings.push(format!(
                        "local share '{}' is on network filesystem '{}' ({})",
                        share.name,
                        fs_type,
                        root.display()
                    ));
                }
            }
        }
        share_fs_types.push((share.name.clone(), root, fs_type));
    }

    if temp_inside_share_root {
        local_fs_warnings.push(format!(
            "transcode temp directory is inside a scanned share root ({})",
            resolved_temp_dir.display()
        ));
    }
    if config.local_mode {
        if let Some(ref fs_type) = temp_fs_type {
            if is_network_filesystem(fs_type) {
                local_fs_warnings.push(format!(
                    "local temp directory is on network filesystem '{}' ({})",
                    fs_type,
                    resolved_temp_dir.display()
                ));
            }
        }
    }

    LocalDeploymentDiagnostics {
        resolved_temp_dir,
        temp_writable,
        temp_inside_share_root,
        temp_fs_type,
        share_fs_types,
        local_fs_warnings,
    }
}

fn snapshot_run_counter(snapshot: Option<&Value>, key: &str) -> u64 {
    snapshot
        .and_then(|s| s.get("run"))
        .and_then(|run| run.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn snapshot_local_fs_warnings(snapshot: Option<&Value>) -> Vec<String> {
    snapshot
        .and_then(|s| s.get("local_fs_warnings"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

#[derive(Parser)]
#[command(name = "watchdog", about = "Jellyfin Transcoding Watchdog", version)]
struct Cli {
    /// Run in simulation mode with fake data
    #[arg(long)]
    simulate: bool,

    /// Scan and report queue, then exit
    #[arg(long)]
    dry_run: bool,

    /// Run one live pass then exit
    #[arg(long)]
    once: bool,

    /// No TUI, log to stdout (for running as a service)
    #[arg(long)]
    headless: bool,

    /// Run in precision/manual-selection mode instead of automatic watchdog scanning
    #[arg(long)]
    precision: bool,

    /// Attach the TUI to an already-running worker without spawning a new one
    #[arg(long)]
    attach: bool,

    /// Custom config file path
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,

    /// Override log verbosity (error|warn|info|debug|trace)
    #[arg(long, value_parser = ["error", "warn", "info", "debug", "trace"])]
    log_level: Option<String>,

    /// Run read-only health checks and exit
    #[arg(long)]
    healthcheck: bool,

    /// Emit healthcheck output as JSON (implies --healthcheck)
    #[arg(long)]
    healthcheck_json: bool,

    /// Create pause file and exit (service will pause safely)
    #[arg(long)]
    pause: bool,

    /// Remove pause file and exit (service will resume on next cycle)
    #[arg(long)]
    resume: bool,

    /// Guided diagnostics mode (config, deps, mounts, DB)
    #[arg(long)]
    doctor: bool,

    /// Read-only service status for SSH operators
    #[arg(long)]
    status: bool,

    /// Emit service status output as JSON (implies --status)
    #[arg(long)]
    status_json: bool,

    /// List quarantined files and exit
    #[arg(long)]
    quarantine_list: bool,

    /// Clear one quarantined file path and exit
    #[arg(long)]
    quarantine_clear: Option<String>,

    /// Clear all quarantined files and exit
    #[arg(long)]
    quarantine_clear_all: bool,

    /// Clear cached inspected-file scan state on startup
    #[arg(long)]
    clear_scan_cache: bool,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        log_anyhow_error("Fatal watchdog error", &err);
        print_anyhow_error_report("watchdog failed", &err);
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let mut cli = Cli::parse();
    let cwd = std::env::current_dir().context("Failed to determine current working directory")?;
    if !cli.simulate {
        let resolved_config = resolve_config_path(&cli.config, &cwd);
        if resolved_config != cli.config {
            info!(
                "Default hidden config missing; falling back to legacy config path {}",
                resolved_config.display()
            );
            cli.config = resolved_config;
        }
    }
    let requested_run_mode = if cli.precision {
        RunMode::Precision
    } else {
        RunMode::Watchdog
    };
    if cli.pause && cli.resume {
        anyhow::bail!("Use only one of --pause or --resume");
    }
    if cli.pause && cli.clear_scan_cache {
        anyhow::bail!("--clear-scan-cache cannot be combined with --pause");
    }
    if cli.once && cli.dry_run {
        anyhow::bail!("Use only one of --once or --dry-run");
    }
    let run_status_mode = cli.status || cli.status_json;
    let run_healthcheck_mode = cli.healthcheck || cli.healthcheck_json;
    if run_status_mode && run_healthcheck_mode {
        anyhow::bail!("Use only one of --status/--status-json or --healthcheck/--healthcheck-json");
    }
    if cli.doctor && (run_healthcheck_mode || run_status_mode || cli.pause || cli.resume) {
        anyhow::bail!("--doctor cannot be combined with healthcheck/status/pause/resume flags");
    }
    if cli.pause
        && (run_status_mode
            || cli.quarantine_list
            || cli.quarantine_clear.is_some()
            || cli.quarantine_clear_all)
    {
        anyhow::bail!("--pause cannot be combined with status/quarantine commands");
    }
    if cli.resume
        && (run_status_mode
            || cli.quarantine_list
            || cli.quarantine_clear.is_some()
            || cli.quarantine_clear_all)
    {
        anyhow::bail!("--resume cannot be combined with status/quarantine commands");
    }
    let quarantine_action_count = (cli.quarantine_list as u8)
        + (cli.quarantine_clear.is_some() as u8)
        + (cli.quarantine_clear_all as u8);
    if quarantine_action_count > 1 {
        anyhow::bail!(
            "Use only one of --quarantine-list, --quarantine-clear, --quarantine-clear-all"
        );
    }
    if run_status_mode && quarantine_action_count > 0 {
        anyhow::bail!("--status/--status-json cannot be combined with quarantine mutation flags");
    }
    if cli.clear_scan_cache && (cli.dry_run || run_status_mode || run_healthcheck_mode) {
        anyhow::bail!(
            "--clear-scan-cache cannot be combined with --dry-run, --status/--status-json, or --healthcheck/--healthcheck-json"
        );
    }
    if cli.attach
        && (cli.headless
            || cli.dry_run
            || cli.once
            || cli.doctor
            || run_status_mode
            || run_healthcheck_mode
            || cli.pause
            || cli.resume
            || cli.quarantine_list
            || cli.quarantine_clear.is_some()
            || cli.quarantine_clear_all)
    {
        anyhow::bail!("--attach is only valid for interactive TUI sessions");
    }

    // Set up tracing — suppress stderr output in TUI mode to avoid corrupting the display
    let tui_mode = !cli.headless
        && !cli.dry_run
        && !cli.once
        && !cli.doctor
        && !run_status_mode
        && !cli.quarantine_list
        && !cli.quarantine_clear_all
        && cli.quarantine_clear.is_none()
        && !run_healthcheck_mode
        && !cli.pause
        && !cli.resume;
    let default_level = cli
        .log_level
        .as_deref()
        .unwrap_or(if tui_mode { "warn" } else { "info" });
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .with_target(false)
        .init();

    info!(
        "Starting Jellyfin Transcoding Watchdog v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!(
        "Runtime platform: os={} arch={} pid={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::process::id()
    );
    if let Ok(cwd) = std::env::current_dir() {
        info!("Current working directory: {}", cwd.display());
    }
    debug!("Raw argv: {:?}", std::env::args().collect::<Vec<_>>());
    info!(
        "Runtime flags: simulate={} headless={} dry_run={} once={} doctor={} status_mode={} healthcheck_mode={} clear_scan_cache={} tui_mode={} log_level={}",
        cli.simulate,
        cli.headless,
        cli.dry_run,
        cli.once,
        cli.doctor,
        run_status_mode,
        run_healthcheck_mode,
        cli.clear_scan_cache,
        tui_mode,
        default_level
    );

    // Load or generate config
    let mut config = if cli.simulate {
        info!("Simulation mode: using default config with in-memory database");
        Config::default_config()
    } else {
        match Config::load(&cli.config) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Failed to load config from {}: {} (code={} category={})",
                    cli.config.display(),
                    e,
                    e.code(),
                    e.category()
                );
                if let Some(hint) = e.operator_hint() {
                    warn!("Config remediation hint: {}", hint);
                }
                if run_healthcheck_mode {
                    std::process::exit(HC_CONFIG_FAIL);
                }
                if run_status_mode {
                    std::process::exit(STATUS_INTERNAL_FAIL);
                }
                return Err(anyhow::Error::new(e)
                    .context(format!("Config load failed from {}", cli.config.display())));
            }
        }
    };
    if cli.dry_run {
        // Enforce strict read-only dry-run execution.
        config.paths.event_journal.clear();
        config.paths.status_snapshot.clear();
    }

    // Determine base directory (config file's parent, or cwd)
    let base_dir = if cli.simulate {
        cwd
    } else {
        cli.config
            .canonicalize()
            .unwrap_or_else(|_| cli.config.clone())
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf()
    };
    info!("Resolved base directory: {}", base_dir.display());

    let runtime_paths = resolve_runtime_paths(&config, &base_dir);
    if !cli.dry_run {
        config.paths.database = runtime_paths.database.to_string_lossy().to_string();
        config.paths.status_snapshot = runtime_paths.status_snapshot.to_string_lossy().to_string();
        config.paths.event_journal = runtime_paths.event_journal.to_string_lossy().to_string();
    }

    let needs_preset_contract = !run_status_mode
        && !run_healthcheck_mode
        && !cli.pause
        && !cli.resume
        && !cli.quarantine_list
        && cli.quarantine_clear.is_none()
        && !cli.quarantine_clear_all
        && !cli.doctor;
    if needs_preset_contract {
        let preset_path = resolve_preset_path(&base_dir, &config.transcode.preset_file);
        let preset_contract = PresetContract::resolve(&preset_path, &config.transcode.preset_name)?;
        preset_contract.ensure_target_codec(&config.transcode.target_codec)?;
        info!(
            "Resolved preset contract: codec={} container=.{} preset={} file={}",
            preset_contract.target_codec,
            preset_contract.container_extension,
            config.transcode.preset_name,
            preset_path.display()
        );
    }

    let mut resume_requested = false;
    if cli.pause || cli.resume {
        let pause_path = config.resolve_path(&base_dir, &config.safety.pause_file);
        info!("Pause marker path: {}", pause_path.display());
        if cli.pause {
            if let Some(parent) = pause_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Failed to create pause marker parent directory {}",
                        parent.display()
                    )
                })?;
            }
            let marker = format!(
                "paused_at={}\nreason=manual_cli\nsource=operator\n",
                chrono::Utc::now().to_rfc3339()
            );
            std::fs::write(&pause_path, marker.as_bytes()).with_context(|| {
                format!("Failed to write pause marker file {}", pause_path.display())
            })?;
            println!("pause file created: {}", pause_path.display());
        } else {
            if pause_path.exists() {
                std::fs::remove_file(&pause_path).with_context(|| {
                    format!("Failed to remove pause marker {}", pause_path.display())
                })?;
                println!("pause file removed: {}", pause_path.display());
            } else {
                println!("pause file already absent: {}", pause_path.display());
            }
            resume_requested = true;
        }
        if cli.pause {
            return Ok(());
        }
    }

    let validation_errors = config.validate_with_base_dir(&base_dir);
    if run_healthcheck_mode && !cli.simulate && !validation_errors.is_empty() {
        for err in &validation_errors {
            error!("Config validation error: {}", err);
        }
        std::process::exit(HC_CONFIG_FAIL);
    }
    if run_status_mode && !cli.simulate && !validation_errors.is_empty() {
        for err in &validation_errors {
            error!("Config validation error: {}", err);
        }
        std::process::exit(STATUS_INTERNAL_FAIL);
    }

    let status_snapshot_path = if cli.dry_run {
        None
    } else {
        Some(runtime_paths.status_snapshot.clone())
    };
    if let Some(path) = status_snapshot_path.as_ref() {
        info!("Status snapshot path: {}", path.display());
    } else {
        info!("Status snapshot path: disabled");
    }

    // Open database
    let db = if cli.simulate {
        Arc::new(WatchdogDb::open_in_memory()?)
    } else {
        let db_path = runtime_paths.database.clone();
        info!("Database: {}", db_path.display());
        if cli.dry_run {
            if db_path.exists() {
                match WatchdogDb::open_read_only(&db_path) {
                    Ok(db) => Arc::new(db),
                    Err(e) => {
                        error!(
                            "Failed to open database read-only at {}: {}",
                            db_path.display(),
                            e
                        );
                        return Err(anyhow::Error::new(e).context(format!(
                            "Failed to open read-only database at {}",
                            db_path.display()
                        )));
                    }
                }
            } else {
                warn!(
                    "Dry-run database does not exist at {}; using in-memory read-only state",
                    db_path.display()
                );
                Arc::new(WatchdogDb::open_in_memory()?)
            }
        } else {
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Failed to create database parent directory {}",
                        parent.display()
                    )
                })?;
            }
            match WatchdogDb::open(&db_path) {
                Ok(db) => Arc::new(db),
                Err(e) => {
                    error!("Failed to open database at {}: {}", db_path.display(), e);
                    if run_healthcheck_mode {
                        std::process::exit(HC_DB_FAIL);
                    }
                    if run_status_mode {
                        std::process::exit(STATUS_INTERNAL_FAIL);
                    }
                    return Err(anyhow::Error::new(e)
                        .context(format!("Failed to open database at {}", db_path.display())));
                }
            }
        }
    };

    if let Some(pid) = db.get_service_state().worker_pid {
        if !util::pid_is_running(pid) {
            db.clear_worker_state();
        }
    }

    if cli.clear_scan_cache {
        let cleared = db.clear_inspected_files();
        info!(
            "Cleared {} inspected-file cache entr{} via --clear-scan-cache",
            cleared,
            if cleared == 1 { "y" } else { "ies" }
        );
        let event_path = resolve_event_journal_path(&config.paths.event_journal, &base_dir);
        append_event(
            event_path.as_deref(),
            "scan_cache_cleared",
            json!({
                "source": "clear_scan_cache_flag",
                "cleared_inspected_rows": cleared,
            }),
        );
    }

    if resume_requested {
        db.clear_auto_paused();
        db.note_pass_success();
        let event_path = resolve_event_journal_path(&config.paths.event_journal, &base_dir);
        append_event(
            event_path.as_deref(),
            "auto_pause_cleared",
            json!({"source": "manual_resume"}),
        );
        return Ok(());
    }

    if cli.quarantine_list {
        let records = db.list_quarantined_files(500);
        if records.is_empty() {
            println!("No quarantined files");
        } else {
            println!("quarantined_files: {}", records.len());
            for rec in records {
                println!(
                    "- {} | at={} | code={} | reason={}",
                    rec.file_path,
                    rec.quarantined_at,
                    rec.last_failure_code.as_deref().unwrap_or("-"),
                    rec.reason.as_deref().unwrap_or("-")
                );
            }
        }
        return Ok(());
    }

    if let Some(path) = cli.quarantine_clear.as_deref() {
        let removed = db.clear_quarantine_file(path);
        if removed {
            db.clear_file_failure_state(path);
            let event_path = resolve_event_journal_path(&config.paths.event_journal, &base_dir);
            append_event(
                event_path.as_deref(),
                "quarantine_clear",
                json!({
                    "path": path,
                    "scope": "single",
                }),
            );
            println!("cleared quarantine: {}", path);
        } else {
            println!("path was not quarantined: {}", path);
        }
        return Ok(());
    }

    if cli.quarantine_clear_all {
        let removed = db.clear_all_quarantine_files();
        let event_path = resolve_event_journal_path(&config.paths.event_journal, &base_dir);
        append_event(
            event_path.as_deref(),
            "quarantine_clear",
            json!({
                "scope": "all",
                "cleared": removed,
            }),
        );
        println!("cleared quarantined entries: {}", removed);
        return Ok(());
    }

    if run_status_mode {
        let exit_code = match run_status(&config, &base_dir, &db, cli.simulate, cli.status_json) {
            Ok(code) => code,
            Err(e) => {
                log_anyhow_error("Status internal error", &e);
                STATUS_INTERNAL_FAIL
            }
        };
        std::process::exit(exit_code);
    }

    if run_healthcheck_mode {
        let exit_code =
            match run_healthcheck(&config, &base_dir, &db, cli.simulate, cli.healthcheck_json) {
                Ok(code) => code,
                Err(e) => {
                    log_anyhow_error("Healthcheck internal error", &e);
                    HC_INTERNAL_FAIL
                }
            };
        std::process::exit(exit_code);
    }

    if cli.doctor {
        let exit_code = run_doctor(&config, &base_dir, &db, cli.simulate, &validation_errors)?;
        std::process::exit(exit_code);
    }

    maybe_bootstrap_persisted_metrics(
        &mut config,
        &cli.config,
        db.as_ref(),
        cli.simulate,
        cli.dry_run,
    );

    // Validate config (doctor mode handles diagnostics itself)
    if !validation_errors.is_empty() && !cli.simulate {
        for err in &validation_errors {
            error!("Config validation error: {}", err);
        }
        warn!(
            "Configuration validation failed; run `watchdog --doctor --config {}` for a full diagnostic report",
            cli.config.display()
        );
        anyhow::bail!(
            "Configuration validation failed with {} error(s)",
            validation_errors.len()
        );
    }

    // Verify dependencies (skip in simulation mode)
    if !cli.simulate {
        if let Err(missing) = util::verify_dependencies() {
            error!("Missing required tools: {}", missing.join(", "));
            debug!(
                "Dependency check PATH snapshot: {}",
                std::env::var("PATH").unwrap_or_else(|_| "<unavailable>".to_string())
            );
            anyhow::bail!(
                "Dependency check failed. Install: {}. Run `watchdog --doctor --config {}` for detailed diagnostics.",
                missing.join(", "),
                cli.config.display()
            );
        }
        let ffprobe_ver = tool_version("ffprobe").unwrap_or_else(|| "unknown".to_string());
        let handbrake_ver = tool_version("HandBrakeCLI").unwrap_or_else(|| "unknown".to_string());
        let rsync_ver = tool_version("rsync").unwrap_or_else(|| "unknown".to_string());
        info!(
            "All required CLI tools found: ffprobe='{}' HandBrakeCLI='{}' rsync='{}'",
            ffprobe_ver, handbrake_ver, rsync_ver
        );
    }
    if cli.headless || cli.dry_run || cli.once {
        let _instance_lock = if !cli.simulate && !cli.dry_run {
            let lock_path = runtime_paths.database.with_extension("lock");
            match util::InstanceLock::acquire(&lock_path) {
                Ok(lock) => {
                    info!("Instance lock acquired: {}", lock.path().display());
                    Some(lock)
                }
                Err(e) => {
                    error!("{}", e);
                    anyhow::bail!(
                        "Failed to acquire instance lock at {}: {}",
                        lock_path.display(),
                        e
                    );
                }
            }
        } else {
            None
        };

        let (state, _state_rx) = StateManager::new();
        if cli.simulate || cli.dry_run {
            set_global_metrics_persistence(None);
        } else {
            set_global_metrics_persistence(Some(Arc::new(MetricsPersistence::new(
                cli.config.clone(),
                CumulativeMetrics::from(&config.metrics),
                Duration::from_secs(config.metrics.flush_interval_seconds.max(5)),
            ))));
        }
        seed_state_from_config(&state, &db, &config, cli.simulate);
        state.set_local_fs_warnings(
            collect_local_deployment_diagnostics(&config, &base_dir).local_fs_warnings,
        );
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let deps = if cli.simulate {
            create_simulated_deps(&config)
        } else {
            PipelineDeps {
                fs: Arc::new(RealFileSystem),
                prober: Box::new(FfprobeProber),
                transcoder: Box::new(HandBrakeTranscoder),
                transfer: Box::new(RsyncTransfer),
                mount_manager: Box::new(SystemMountManager),
                in_use_detector: Box::new(CommandInUseDetector::new(
                    config.safety.in_use_guard_command.clone(),
                )),
            }
        };

        if !cli.dry_run {
            db.reset_stale_queue_items();
            db.set_worker_state(std::process::id(), requested_run_mode.as_str());
            set_tui_log_event_path(Some(runtime_paths.event_journal.clone()));
            append_event(
                Some(&runtime_paths.event_journal),
                "session_start",
                json!({
                    "pid": std::process::id(),
                    "run_mode": requested_run_mode.as_str(),
                }),
            );
        }

        let snapshot_handle = if !cli.dry_run {
            status_snapshot_path.clone().map(|snapshot_path| {
                tokio::spawn(run_status_snapshot_task(
                    state.subscribe(),
                    db.clone(),
                    snapshot_path,
                    shutdown_tx.subscribe(),
                    Duration::from_secs(2),
                ))
            })
        } else {
            None
        };

        let signal_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(sig) => Some(sig),
                    Err(e) => {
                        warn!(
                            "Failed to register SIGTERM handler; continuing with SIGINT only: {}",
                            e
                        );
                        None
                    }
                };
            if let Some(sigterm) = sigterm.as_mut() {
                let mut shutdown_signalled = false;
                loop {
                    tokio::select! {
                        _ = sigterm.recv() => {
                            if shutdown_signalled {
                                info!("Received follow-up SIGTERM force-stop request");
                                terminate_registered_subprocesses(Duration::from_millis(250));
                                break;
                            }
                            info!("Received SIGTERM");
                            info!("Signalling pipeline shutdown...");
                            let _ = signal_shutdown_tx.send(());
                            shutdown_signalled = true;
                        },
                        _ = tokio::signal::ctrl_c() => {
                            info!("Received SIGINT");
                            info!("Signalling pipeline shutdown...");
                            let _ = signal_shutdown_tx.send(());
                            break;
                        },
                    }
                }
            } else {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => info!("Received SIGINT"),
                    Err(e) => {
                        warn!("Failed to register SIGINT handler: {}", e);
                        return;
                    }
                }
                info!("Signalling pipeline shutdown...");
                let _ = signal_shutdown_tx.send(());
            }
        });

        let pipeline_result = run_pipeline_loop(
            config,
            base_dir,
            deps,
            db.clone(),
            state.clone(),
            requested_run_mode,
            cli.dry_run,
            cli.once,
            shutdown_tx.subscribe(),
        )
        .await;

        if let Some(metrics) = flush_global_metrics_now() {
            sync_cumulative_metrics_state(&state, metrics);
        }
        let _ = shutdown_tx.send(());
        if !cli.dry_run {
            db.clear_worker_state();
        }

        if let Some(handle) = snapshot_handle {
            match handle.await {
                Ok(()) => {}
                Err(e) => warn!("Status snapshot task join error: {}", e),
            }
        }

        pipeline_result.with_context(|| {
            format!(
                "Pipeline loop failed (headless={}, dry_run={}, once={}, run_mode={})",
                cli.headless, cli.dry_run, cli.once, requested_run_mode
            )
        })?;
    } else {
        let service_state = db.get_service_state();
        let live_worker_pid = service_state
            .worker_pid
            .filter(|pid| util::pid_is_running(*pid));
        let active_run_mode = match service_state.worker_run_mode.as_deref() {
            Some("precision") => RunMode::Precision,
            _ => RunMode::Watchdog,
        };

        let mut owned_worker = None;
        let attach_run_mode = if cli.attach {
            if live_worker_pid.is_none() {
                anyhow::bail!(
                    "No active worker is running. Start one with `watchdog{}` first.",
                    if matches!(requested_run_mode, RunMode::Precision) {
                        " --precision"
                    } else {
                        ""
                    }
                );
            }
            if cli.precision && !matches!(active_run_mode, RunMode::Precision) {
                anyhow::bail!("The active worker is not running in precision mode");
            }
            active_run_mode
        } else {
            if let Some(_pid) = live_worker_pid {
                if active_run_mode != requested_run_mode {
                    anyhow::bail!(
                        "A {} worker is already running. Reattach with `{}` or stop it before starting {} mode.",
                        active_run_mode,
                        build_attach_hint(&cli, active_run_mode),
                        requested_run_mode
                    );
                }
            } else {
                let mut worker = spawn_worker_process(&cli, requested_run_mode)?;
                wait_for_worker_ready(&db, &mut worker, Duration::from_secs(5))?;
                owned_worker = Some(worker);
            }
            requested_run_mode
        };

        let attach_hint = build_attach_hint(&cli, attach_run_mode);
        if let Err(e) = tui::run_tui(
            config,
            base_dir.clone(),
            db.clone(),
            runtime_paths.status_snapshot.clone(),
            runtime_paths.event_journal.clone(),
            attach_hint,
            owned_worker,
        )
        .await
        {
            log_anyhow_error("TUI error", &e);
            print_anyhow_error_report("tui failed", &e);
        }
    }

    info!("Watchdog shut down cleanly");
    Ok(())
}

fn maybe_bootstrap_persisted_metrics(
    config: &mut Config,
    config_path: &Path,
    db: &WatchdogDb,
    simulate: bool,
    dry_run: bool,
) {
    if simulate || dry_run || config.metrics_loaded_from_file {
        return;
    }

    let migrated = MetricsConfig {
        space_saved_bytes: db.get_total_space_saved(),
        transcoded_files: db.get_transcode_count().max(0) as u64,
        inspected_files: db.get_inspected_count().max(0) as u64,
        retries_scheduled: config.metrics.retries_scheduled,
        flush_interval_seconds: config.metrics.flush_interval_seconds.max(5),
    };

    match write_metrics_to_path(config_path, &migrated) {
        Ok(()) => {
            info!(
                "Seeded [metrics] in {} from historical database totals",
                config_path.display()
            );
            config.metrics = migrated;
            config.metrics_loaded_from_file = true;
        }
        Err(err) => {
            warn!(
                "Failed to seed [metrics] in {}: {}",
                config_path.display(),
                err
            );
        }
    }
}

fn seed_state_from_config(state: &StateManager, db: &WatchdogDb, config: &Config, simulate: bool) {
    let top_reasons = db.get_top_failure_reasons(3);
    let service_state = db.get_service_state();
    let quarantined = db.quarantine_count();
    let metrics = CumulativeMetrics::from(&config.metrics);
    state.update(|s| {
        s.total_space_saved = metrics.space_saved_bytes;
        s.total_transcoded = metrics.transcoded_files;
        s.total_inspected = metrics.inspected_files;
        s.total_retries_scheduled = metrics.retries_scheduled;
        s.local_mode = config.local_mode;
        s.simulate_mode = simulate;
        s.consecutive_pass_failures = service_state.consecutive_pass_failures;
        s.auto_paused = service_state.auto_paused_at.is_some();
        s.auto_paused_at = service_state.auto_paused_at.clone();
        s.auto_pause_reason = service_state.auto_pause_reason.clone();
        s.quarantined_files = quarantined.max(0) as u64;
        s.top_failure_reasons = top_reasons
            .iter()
            .map(|(reason, count)| (reason.clone(), *count as u64))
            .collect();
    });
}

fn build_attach_hint(cli: &Cli, run_mode: RunMode) -> String {
    let mut parts = vec!["watchdog".to_string(), "--attach".to_string()];
    if matches!(run_mode, RunMode::Precision) {
        parts.push("--precision".to_string());
    }
    if cli.simulate {
        parts.push("--simulate".to_string());
    } else {
        parts.push("--config".to_string());
        parts.push(shell_quote(&display_attach_config_path(cli)));
    }
    parts.join(" ")
}

fn display_attach_config_path(cli: &Cli) -> String {
    if !cli.config.is_absolute() {
        return cli.config.to_string_lossy().to_string();
    }

    let canonical = cli
        .config
        .canonicalize()
        .unwrap_or_else(|_| cli.config.clone());
    if let Ok(cwd) = std::env::current_dir() {
        if let Ok(relative) = canonical.strip_prefix(&cwd) {
            return relative.to_string_lossy().to_string();
        }
    }
    canonical.to_string_lossy().to_string()
}

fn resolve_config_path(requested: &Path, cwd: &Path) -> PathBuf {
    if requested.is_absolute() || cwd.join(requested).exists() {
        return requested.to_path_buf();
    }
    if requested == Path::new(DEFAULT_CONFIG_PATH) {
        let legacy = PathBuf::from(LEGACY_DEFAULT_CONFIG_PATH);
        if cwd.join(&legacy).exists() {
            return legacy;
        }
    }
    requested.to_path_buf()
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn spawn_worker_process(cli: &Cli, run_mode: RunMode) -> anyhow::Result<Child> {
    let exe = std::env::current_exe().context("Failed to resolve current executable")?;
    let mut cmd = Command::new(exe);
    cmd.arg("--headless");
    if cli.simulate {
        cmd.arg("--simulate");
    } else {
        cmd.arg("--config").arg(&cli.config);
    }
    if matches!(run_mode, RunMode::Precision) {
        cmd.arg("--precision");
    }
    if let Some(level) = cli.log_level.as_deref() {
        cmd.arg("--log-level").arg(level);
    }
    util::spawn_detached_command(cmd).context("Failed to spawn detached worker process")
}

fn wait_for_worker_ready(
    db: &WatchdogDb,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<()> {
    let spawned_pid = child.id();
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("Failed to poll worker process {}", spawned_pid))?
        {
            anyhow::bail!(
                "Worker process {} exited during startup with {}",
                spawned_pid,
                describe_exit_status(&status)
            );
        }
        if db
            .get_service_state()
            .worker_pid
            .is_some_and(|pid| pid == spawned_pid)
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    terminate_subprocess(child, Duration::from_millis(100));
    let _ = child.wait();
    anyhow::bail!(
        "Timed out waiting for worker {} to register itself",
        spawned_pid
    )
}

fn live_worker_run_mode(service_state: &watchdog::db::ServiceState) -> Option<String> {
    service_state
        .worker_pid
        .filter(|pid| util::pid_is_running(*pid))
        .and(service_state.worker_run_mode.clone())
}

fn fresh_snapshot_run_mode(snapshot_diag: &SnapshotDiagnostics) -> Option<String> {
    if snapshot_diag.freshness != "fresh" {
        return None;
    }
    snapshot_diag
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.get("run_mode"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn run_healthcheck(
    config: &Config,
    base_dir: &std::path::Path,
    db: &WatchdogDb,
    simulate: bool,
    json_output: bool,
) -> anyhow::Result<i32> {
    let result = evaluate_healthcheck(config, base_dir, db, simulate);

    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("watchdog_healthcheck");
        println!("  status: {}", result.status);
        println!("  exit_code: {}", result.exit_code);
        println!("  storage_mode: {}", storage_mode_name(result.local_mode));
        println!("  mode: {}", result.mode);
        println!("  run_mode: {}", result.run_mode);
        println!("  paused: {}", result.paused);
        println!("  cooldown_files: {}", result.cooldown_files);
        println!(
            "  checks: config={} db={} dependencies={} {}={}",
            result.checks.config,
            result.checks.db,
            result.checks.dependencies,
            share_health_check_label(result.local_mode),
            result.checks.nfs
        );
        println!("  db_query_ok: {}", result.db_query_ok);
        if !result.missing_dependencies.is_empty() {
            println!(
                "  missing_dependencies: {}",
                result.missing_dependencies.join(", ")
            );
        }
        if !result.unhealthy_shares.is_empty() {
            println!("  unhealthy_shares: {}", result.unhealthy_shares.join(", "));
        }
        if !result.dependency_diagnostics.is_empty() {
            for dep in &result.dependency_diagnostics {
                println!(
                    "  dependency: {} found={} version={}",
                    dep.name,
                    dep.found,
                    dep.version.as_deref().unwrap_or("unknown")
                );
            }
        }
        if !result.mount_checks.is_empty() {
            for mount in &result.mount_checks {
                println!(
                    "  {}: {} healthy={} latency_ms={}",
                    share_health_subject_label(result.local_mode),
                    mount.share,
                    mount.healthy,
                    mount.latency_ms
                );
            }
        }
        if let Some(code) = result.latest_failure_code.as_deref() {
            println!("  latest_failure_code: {}", code);
        }
        println!("  status_freshness: {}", result.status_freshness);
        println!(
            "  snapshot_age_seconds: {}",
            result
                .snapshot_age_seconds
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
        println!(
            "  consecutive_pass_failures: {}",
            result.consecutive_pass_failures
        );
        if let Some(reason) = result.auto_pause_reason.as_deref() {
            println!("  auto_pause_reason: {}", reason);
        }
        println!("  quarantine_count: {}", result.quarantine_count);
        println!("  skipped_temporary: {}", result.skipped_temporary);
        println!("  skipped_unstable: {}", result.skipped_unstable);
        println!("  skipped_hardlinked: {}", result.skipped_hardlinked);
        for warning in &result.local_fs_warnings {
            println!("  local_fs_warning: {}", warning);
        }
    }

    Ok(result.exit_code)
}

fn evaluate_healthcheck(
    config: &Config,
    base_dir: &std::path::Path,
    db: &WatchdogDb,
    simulate: bool,
) -> HealthcheckResult {
    let pause_path = config.resolve_path(base_dir, &config.safety.pause_file);
    let paused = pause_path.exists();
    let cooldown_files = db.get_cooldown_active_count(chrono::Utc::now().timestamp());
    let db_query_ok = db.healthcheck_query_ok();
    let latest_failure = db.get_latest_failure();
    let service_state = db.get_service_state();
    let quarantine_count = db.quarantine_count();
    let snapshot_diag = collect_snapshot_diagnostics(config, base_dir);
    let deployment_diag = collect_local_deployment_diagnostics(config, base_dir);
    let snapshot = snapshot_diag.snapshot.as_ref();
    let run_mode = fresh_snapshot_run_mode(&snapshot_diag)
        .or_else(|| live_worker_run_mode(&service_state))
        .unwrap_or_else(|| "watchdog".to_string());

    let dep_names = ["ffprobe", "HandBrakeCLI", "rsync"];
    let forced_missing = read_list_env("WATCHDOG_HEALTHCHECK_FORCE_MISSING_DEPS");
    let dependency_diagnostics: Vec<HealthcheckDependency> = dep_names
        .iter()
        .map(|name| {
            let found = if simulate {
                true
            } else if let Some(forced) = forced_missing.as_ref() {
                !forced.iter().any(|f| f == name)
            } else {
                which::which(name).is_ok()
            };
            let version = if found && !simulate {
                tool_version(name)
            } else {
                None
            };
            HealthcheckDependency {
                name: (*name).to_string(),
                found,
                version,
            }
        })
        .collect();
    let missing_dependencies = dependency_diagnostics
        .iter()
        .filter(|d| !d.found)
        .map(|d| d.name.clone())
        .collect::<Vec<_>>();

    let forced_unhealthy = read_list_env("WATCHDOG_HEALTHCHECK_FORCE_UNHEALTHY_SHARES");
    let mount_manager = SystemMountManager;
    let mount_checks = config
        .shares
        .iter()
        .map(|share| {
            let started = std::time::Instant::now();
            let healthy = if simulate {
                true
            } else if let Some(forced) = forced_unhealthy.as_ref() {
                !forced.iter().any(|s| s == &share.name)
            } else if config.local_mode {
                host_share_root_is_healthy(std::path::Path::new(&share.local_mount))
            } else {
                mount_manager.is_healthy(std::path::Path::new(&share.local_mount))
            };
            HealthcheckMountCheck {
                share: share.name.clone(),
                healthy,
                latency_ms: started.elapsed().as_millis(),
            }
        })
        .collect::<Vec<_>>();
    let unhealthy_shares = mount_checks
        .iter()
        .filter(|s| !s.healthy)
        .map(|s| s.share.clone())
        .collect::<Vec<_>>();
    let temp_safe = deployment_diag.temp_writable && !deployment_diag.temp_inside_share_root;
    let local_fs_warnings = if snapshot.is_some() {
        let from_snapshot = snapshot_local_fs_warnings(snapshot);
        if from_snapshot.is_empty() {
            deployment_diag.local_fs_warnings.clone()
        } else {
            from_snapshot
        }
    } else {
        deployment_diag.local_fs_warnings.clone()
    };

    let checks = HealthcheckChecks {
        config: true,
        db: db_query_ok,
        dependencies: missing_dependencies.is_empty(),
        nfs: unhealthy_shares.is_empty() && temp_safe,
    };
    let exit_code = if !checks.config {
        HC_CONFIG_FAIL
    } else if !checks.db {
        HC_DB_FAIL
    } else if !checks.dependencies {
        HC_DEPS_FAIL
    } else if !checks.nfs {
        HC_NFS_FAIL
    } else {
        HC_OK
    };

    HealthcheckResult {
        status: if exit_code == HC_OK {
            "ok".to_string()
        } else {
            "failed".to_string()
        },
        exit_code,
        local_mode: config.local_mode,
        checks,
        paused,
        cooldown_files,
        mode: if simulate {
            "simulate".to_string()
        } else {
            "live".to_string()
        },
        run_mode,
        missing_dependencies,
        unhealthy_shares,
        db_query_ok,
        dependency_diagnostics,
        mount_checks,
        latest_failure_code: latest_failure.and_then(|f| f.failure_code),
        snapshot_age_seconds: snapshot_diag.age_seconds,
        status_freshness: snapshot_diag.freshness,
        consecutive_pass_failures: service_state.consecutive_pass_failures,
        auto_pause_reason: service_state.auto_pause_reason,
        quarantine_count,
        skipped_temporary: snapshot_run_counter(snapshot, "skipped_temporary"),
        skipped_unstable: snapshot_run_counter(snapshot, "skipped_unstable"),
        skipped_hardlinked: snapshot_run_counter(snapshot, "skipped_hardlinked"),
        local_fs_warnings,
    }
}

fn collect_snapshot_diagnostics(config: &Config, base_dir: &Path) -> SnapshotDiagnostics {
    let snapshot_path = resolve_status_snapshot_path(config, base_dir);
    let Some(path) = snapshot_path else {
        return SnapshotDiagnostics {
            freshness: "missing".to_string(),
            age_seconds: None,
            snapshot: None,
            snapshot_path: None,
        };
    };
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(e) => {
            debug!(
                "Status snapshot metadata unavailable at {}: {}",
                path.display(),
                e
            );
            return SnapshotDiagnostics {
                freshness: "missing".to_string(),
                age_seconds: None,
                snapshot: None,
                snapshot_path: Some(path),
            };
        }
    };
    let age_seconds = metadata
        .modified()
        .ok()
        .and_then(|mtime| std::time::SystemTime::now().duration_since(mtime).ok())
        .map(|age| age.as_secs());
    let freshness = match age_seconds {
        Some(age) if age <= config.safety.status_snapshot_stale_seconds => "fresh",
        Some(_) => "stale",
        None => "stale",
    }
    .to_string();

    let snapshot = match std::fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                warn!(
                    "Status snapshot at {} is invalid JSON: {}",
                    path.display(),
                    e
                );
                None
            }
        },
        Err(e) => {
            warn!(
                "Failed to read status snapshot at {}: {}",
                path.display(),
                e
            );
            None
        }
    };
    SnapshotDiagnostics {
        freshness,
        age_seconds,
        snapshot,
        snapshot_path: Some(path),
    }
}

fn read_list_env(key: &str) -> Option<Vec<String>> {
    std::env::var(key).ok().map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    })
}

fn tool_version(tool: &str) -> Option<String> {
    let mut cmd = Command::new(tool);
    cmd.arg("--version");
    let command_repr = format_command_for_log(&cmd);
    let out = match run_command(
        cmd,
        RunOptions {
            timeout: Some(Duration::from_secs(3)),
            stdout_limit: 4096,
            stderr_limit: 4096,
            ..RunOptions::default()
        },
    ) {
        Ok(out) => out,
        Err(e) => {
            debug!(
                "Tool version probe failed to execute: tool={} command={} error={}",
                tool, command_repr, e
            );
            return None;
        }
    };
    if out.timed_out || !out.status.success() {
        debug!(
            "Tool version probe failed: tool={} command={} status={} timed_out={} stderr={}",
            tool,
            command_repr,
            describe_exit_status(&out.status),
            out.timed_out,
            summarize_output_tail(&out.stderr_tail, 200)
        );
        return None;
    }
    let text = if !out.stdout.is_empty() {
        String::from_utf8_lossy(&out.stdout).to_string()
    } else {
        String::from_utf8_lossy(&out.stderr_tail).to_string()
    };
    text.lines().next().map(|line| line.trim().to_string())
}

fn run_status(
    config: &Config,
    base_dir: &Path,
    db: &WatchdogDb,
    simulate: bool,
    json_output: bool,
) -> anyhow::Result<i32> {
    if !db.healthcheck_query_ok() {
        warn!("Status request degraded: database healthcheck query failed");
        let result = StatusResult {
            status: "error".to_string(),
            exit_code: STATUS_INTERNAL_FAIL,
            local_mode: config.local_mode,
            mode: if simulate {
                "simulate".to_string()
            } else {
                "live".to_string()
            },
            run_mode: "watchdog".to_string(),
            paused: false,
            auto_paused: false,
            auto_pause_reason: None,
            nfs_healthy: None,
            unhealthy_shares: Vec::new(),
            phase: None,
            queue_position: None,
            queue_total: None,
            current_file: None,
            cooldown_files: 0,
            scan_timeouts: 0,
            last_failure_code: None,
            quarantine_count: 0,
            consecutive_pass_failures: 0,
            status_freshness: "missing".to_string(),
            snapshot_age_seconds: None,
            snapshot_path: None,
            skipped_temporary: 0,
            skipped_unstable: 0,
            skipped_hardlinked: 0,
            local_fs_warnings: Vec::new(),
        };
        if json_output {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("watchdog_status");
            println!("  status: error");
            println!("  exit_code: {}", STATUS_INTERNAL_FAIL);
            println!("  storage_mode: {}", config.storage_mode_name());
            println!("  db_query_ok: false");
            println!("  hint: run watchdog --doctor for database diagnostics");
        }
        return Ok(STATUS_INTERNAL_FAIL);
    }

    let pause_path = config.resolve_path(base_dir, &config.safety.pause_file);
    let paused = pause_path.exists();
    let cooldown_files = db.get_cooldown_active_count(chrono::Utc::now().timestamp());
    let latest_failure = db.get_latest_failure();
    let service_state = db.get_service_state();
    let quarantine_count = db.quarantine_count();
    let snapshot_diag = collect_snapshot_diagnostics(config, base_dir);
    let deployment_diag = collect_local_deployment_diagnostics(config, base_dir);

    let snapshot = snapshot_diag.snapshot.as_ref();
    let phase = snapshot
        .and_then(|s| s.get("phase"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let queue_position = snapshot
        .and_then(|s| s.get("queue_position"))
        .and_then(Value::as_u64);
    let queue_total = snapshot
        .and_then(|s| s.get("queue_total"))
        .and_then(Value::as_u64);
    let current_file = snapshot
        .and_then(|s| s.get("current_file"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let nfs_healthy = snapshot
        .and_then(|s| s.get("nfs_healthy"))
        .and_then(Value::as_bool);
    let local_mode = snapshot
        .and_then(|s| s.get("local_mode"))
        .and_then(Value::as_bool)
        .unwrap_or(config.local_mode);
    let run_mode = fresh_snapshot_run_mode(&snapshot_diag)
        .or_else(|| live_worker_run_mode(&service_state))
        .unwrap_or_else(|| "watchdog".to_string());
    let unhealthy_shares = snapshot
        .and_then(|s| s.get("unhealthy_shares"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let scan_timeouts = snapshot
        .and_then(|s| s.get("reliability"))
        .and_then(|r| r.get("scan_timeouts"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let last_failure_code = snapshot
        .and_then(|s| s.get("reliability"))
        .and_then(|r| r.get("last_failure_code"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| latest_failure.and_then(|f| f.failure_code));
    let local_fs_warnings = {
        let from_snapshot = snapshot_local_fs_warnings(snapshot);
        if from_snapshot.is_empty() {
            deployment_diag.local_fs_warnings.clone()
        } else {
            from_snapshot
        }
    };
    let temp_safe = deployment_diag.temp_writable && !deployment_diag.temp_inside_share_root;

    let auto_paused = service_state.auto_paused_at.is_some();
    let unhealthy = auto_paused
        || service_state.consecutive_pass_failures
            >= config.safety.max_consecutive_pass_failures.max(1)
        || nfs_healthy == Some(false)
        || !temp_safe
        || (config.local_mode
            && config
                .shares
                .iter()
                .any(|share| !host_share_root_is_healthy(Path::new(&share.local_mount))));
    let degraded = snapshot_diag.freshness == "stale" || snapshot_diag.freshness == "missing";
    let exit_code = if unhealthy {
        STATUS_UNHEALTHY
    } else if degraded {
        STATUS_DEGRADED
    } else {
        STATUS_OK
    };
    let status = if exit_code == STATUS_OK {
        "ok"
    } else if exit_code == STATUS_DEGRADED {
        "degraded"
    } else {
        "unhealthy"
    }
    .to_string();

    let result = StatusResult {
        status,
        exit_code,
        local_mode,
        mode: if simulate {
            "simulate".to_string()
        } else {
            "live".to_string()
        },
        run_mode,
        paused,
        auto_paused,
        auto_pause_reason: service_state.auto_pause_reason,
        nfs_healthy,
        unhealthy_shares,
        phase,
        queue_position,
        queue_total,
        current_file,
        cooldown_files,
        scan_timeouts,
        last_failure_code,
        quarantine_count,
        consecutive_pass_failures: service_state.consecutive_pass_failures,
        status_freshness: snapshot_diag.freshness,
        snapshot_age_seconds: snapshot_diag.age_seconds,
        snapshot_path: snapshot_diag
            .snapshot_path
            .map(|p| p.to_string_lossy().to_string()),
        skipped_temporary: snapshot_run_counter(snapshot, "skipped_temporary"),
        skipped_unstable: snapshot_run_counter(snapshot, "skipped_unstable"),
        skipped_hardlinked: snapshot_run_counter(snapshot, "skipped_hardlinked"),
        local_fs_warnings,
    };

    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("watchdog_status");
        println!("  status: {}", result.status);
        println!("  exit_code: {}", result.exit_code);
        println!("  storage_mode: {}", storage_mode_name(result.local_mode));
        println!("  mode: {}", result.mode);
        println!("  run_mode: {}", result.run_mode);
        println!("  paused: {}", result.paused);
        println!("  auto_paused: {}", result.auto_paused);
        if let Some(reason) = result.auto_pause_reason.as_deref() {
            println!("  auto_pause_reason: {}", reason);
        }
        println!("  phase: {}", result.phase.as_deref().unwrap_or("unknown"));
        println!(
            "  queue: {}/{}",
            result.queue_position.unwrap_or(0),
            result.queue_total.unwrap_or(0)
        );
        if let Some(file) = result.current_file.as_deref() {
            println!("  current_file: {}", file);
        }
        println!(
            "  {}: {}",
            share_health_text_label(result.local_mode),
            result
                .nfs_healthy
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );
        if !result.unhealthy_shares.is_empty() {
            println!("  unhealthy_shares: {}", result.unhealthy_shares.join(", "));
        }
        println!("  cooldown_files: {}", result.cooldown_files);
        println!("  scan_timeouts: {}", result.scan_timeouts);
        println!("  quarantine_count: {}", result.quarantine_count);
        println!(
            "  consecutive_pass_failures: {}",
            result.consecutive_pass_failures
        );
        if let Some(code) = result.last_failure_code.as_deref() {
            println!("  last_failure_code: {}", code);
        }
        println!("  status_freshness: {}", result.status_freshness);
        println!(
            "  snapshot_age_seconds: {}",
            result
                .snapshot_age_seconds
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
        println!("  skipped_temporary: {}", result.skipped_temporary);
        println!("  skipped_unstable: {}", result.skipped_unstable);
        println!("  skipped_hardlinked: {}", result.skipped_hardlinked);
        for warning in &result.local_fs_warnings {
            println!("  local_fs_warning: {}", warning);
        }
    }

    Ok(exit_code)
}

fn run_doctor(
    config: &Config,
    base_dir: &std::path::Path,
    db: &WatchdogDb,
    simulate: bool,
    validation_errors: &[String],
) -> anyhow::Result<i32> {
    let db_ok = db.healthcheck_query_ok();
    let deployment_diag = collect_local_deployment_diagnostics(config, base_dir);
    let deps = ["ffprobe", "HandBrakeCLI", "rsync"];
    let dep_results: Vec<DoctorDependency> = deps
        .iter()
        .map(|name| {
            let found = if simulate {
                true
            } else {
                which::which(name).is_ok()
            };
            let version = if found && !simulate {
                tool_version(name)
            } else {
                None
            };
            DoctorDependency {
                name: (*name).to_string(),
                found,
                version,
            }
        })
        .collect();

    let mount_manager = SystemMountManager;
    let mut mount_checks = Vec::new();
    for share in &config.shares {
        let started = std::time::Instant::now();
        let healthy = if simulate {
            true
        } else if config.local_mode {
            host_share_root_is_healthy(std::path::Path::new(&share.local_mount))
        } else {
            mount_manager.is_healthy(std::path::Path::new(&share.local_mount))
        };
        mount_checks.push(DoctorMountCheck {
            share: share.name.clone(),
            mount: share.local_mount.clone(),
            healthy,
            latency_ms: started.elapsed().as_millis(),
        });
    }
    let snapshot_diag = collect_snapshot_diagnostics(config, base_dir);
    let service_state = db.get_service_state();
    let quarantine_count = db.quarantine_count();

    println!("watchdog_doctor");
    println!("  mode: {}", if simulate { "simulate" } else { "live" });
    println!("  storage_mode: {}", config.storage_mode_name());
    println!(
        "  config: {}",
        if validation_errors.is_empty() {
            "ok"
        } else {
            "failed"
        }
    );
    println!("  db_query: {}", if db_ok { "ok" } else { "failed" });
    println!(
        "  base_dir: {}",
        base_dir
            .canonicalize()
            .unwrap_or_else(|_| base_dir.to_path_buf())
            .display()
    );
    println!(
        "  resolved_temp_dir: {}",
        deployment_diag.resolved_temp_dir.display()
    );
    println!("  temp_dir_writable: {}", deployment_diag.temp_writable);
    println!(
        "  temp_dir_inside_share_root: {}",
        deployment_diag.temp_inside_share_root
    );
    println!(
        "  temp_dir_fs_type: {}",
        deployment_diag.temp_fs_type.as_deref().unwrap_or("unknown")
    );
    println!("  dependencies:");
    for dep in &dep_results {
        println!(
            "    - {}: {}{}",
            dep.name,
            if dep.found { "found" } else { "missing" },
            dep.version
                .as_ref()
                .map(|v| format!(" ({})", v))
                .unwrap_or_default()
        );
    }
    println!("  {}:", share_health_section_label(config.local_mode));
    for mount in &mount_checks {
        let fs_type = deployment_diag
            .share_fs_types
            .iter()
            .find(|(name, _, _)| name == &mount.share)
            .and_then(|(_, _, fs_type)| fs_type.as_deref())
            .unwrap_or("unknown");
        println!(
            "    - {} [{}]: {} ({} ms, fs_type={})",
            mount.share,
            mount.mount,
            if mount.healthy {
                "healthy"
            } else {
                "unhealthy"
            },
            mount.latency_ms,
            fs_type
        );
    }
    println!("  status_freshness: {}", snapshot_diag.freshness);
    println!(
        "  snapshot_age_seconds: {}",
        snapshot_diag
            .age_seconds
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  consecutive_pass_failures: {}",
        service_state.consecutive_pass_failures
    );
    if let Some(reason) = service_state.auto_pause_reason.as_deref() {
        println!("  auto_pause_reason: {}", reason);
    }
    println!("  quarantine_count: {}", quarantine_count);
    if !validation_errors.is_empty() {
        println!("  config_errors:");
        for err in validation_errors {
            println!("    - {}", err);
        }
    }
    if !deployment_diag.local_fs_warnings.is_empty() {
        println!("  local_fs_warnings:");
        for warning in &deployment_diag.local_fs_warnings {
            println!("    - {}", warning);
        }
    }

    let has_missing_deps = dep_results.iter().any(|d| !d.found);
    let has_unhealthy_mounts = mount_checks.iter().any(|m| !m.healthy);
    let temp_unsafe = !deployment_diag.temp_writable || deployment_diag.temp_inside_share_root;
    let snapshot_degraded =
        !simulate && snapshot_diag.snapshot_path.is_some() && snapshot_diag.freshness != "fresh";
    let safety_trip = service_state.auto_paused_at.is_some()
        || service_state.consecutive_pass_failures
            >= config.safety.max_consecutive_pass_failures.max(1);
    let failed = !validation_errors.is_empty()
        || !db_ok
        || has_missing_deps
        || has_unhealthy_mounts
        || temp_unsafe
        || snapshot_degraded
        || safety_trip;
    println!("  status: {}", if failed { "failed" } else { "ok" });

    Ok(if failed { 1 } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_with_config(config: PathBuf) -> Cli {
        Cli {
            simulate: false,
            dry_run: false,
            once: false,
            headless: false,
            precision: false,
            attach: false,
            config,
            log_level: None,
            healthcheck: false,
            healthcheck_json: false,
            pause: false,
            resume: false,
            doctor: false,
            status: false,
            status_json: false,
            quarantine_list: false,
            quarantine_clear: None,
            quarantine_clear_all: false,
            clear_scan_cache: false,
        }
    }

    #[test]
    fn display_attach_config_path_keeps_relative_paths() {
        let cli = cli_with_config(PathBuf::from("configs/watchdog.toml"));
        assert_eq!(display_attach_config_path(&cli), "configs/watchdog.toml");
    }

    #[test]
    fn display_attach_config_path_keeps_absolute_paths_outside_cwd_absolute() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("watchdog.toml");
        std::fs::write(&config_path, b"").unwrap();
        let cli = cli_with_config(config_path.clone());

        assert_eq!(
            display_attach_config_path(&cli),
            config_path.canonicalize().unwrap().to_string_lossy()
        );
    }

    #[test]
    fn resolve_config_path_falls_back_to_legacy_root_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let legacy = temp_dir.path().join("watchdog.toml");
        std::fs::write(&legacy, b"").unwrap();

        assert_eq!(
            resolve_config_path(Path::new(DEFAULT_CONFIG_PATH), temp_dir.path()),
            PathBuf::from(LEGACY_DEFAULT_CONFIG_PATH)
        );
    }
}
