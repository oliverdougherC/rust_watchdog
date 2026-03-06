use clap::Parser;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use watchdog::config::Config;
use watchdog::db::WatchdogDb;
use watchdog::in_use::CommandInUseDetector;
use watchdog::nfs::SystemMountManager;
use watchdog::pipeline::{run_pipeline_loop, PipelineDeps};
use watchdog::probe::FfprobeProber;
use watchdog::process::{run_command, RunOptions};
use watchdog::scanner::RealFileSystem;
use watchdog::simulate::create_simulated_deps;
use watchdog::state::StateManager;
use watchdog::status_snapshot::{
    resolve_status_snapshot_path, run_status_snapshot_task, write_snapshot,
};
use watchdog::traits::MountManager;
use watchdog::transcode::HandBrakeTranscoder;
use watchdog::transfer::RsyncTransfer;
use watchdog::{tui, util};

const HC_OK: i32 = 0;
const HC_CONFIG_FAIL: i32 = 20;
const HC_DB_FAIL: i32 = 21;
const HC_DEPS_FAIL: i32 = 22;
const HC_NFS_FAIL: i32 = 23;
const HC_INTERNAL_FAIL: i32 = 24;

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
    checks: HealthcheckChecks,
    paused: bool,
    cooldown_files: i64,
    mode: String,
    missing_dependencies: Vec<String>,
    unhealthy_shares: Vec<String>,
    db_query_ok: bool,
    dependency_diagnostics: Vec<HealthcheckDependency>,
    mount_checks: Vec<HealthcheckMountCheck>,
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

#[derive(Parser)]
#[command(
    name = "watchdog",
    about = "Jellyfin AV1 Transcoding Watchdog",
    version
)]
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

    /// Custom config file path
    #[arg(long, default_value = "watchdog.toml")]
    config: PathBuf,

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.pause && cli.resume {
        anyhow::bail!("Use only one of --pause or --resume");
    }
    if cli.once && cli.dry_run {
        anyhow::bail!("Use only one of --once or --dry-run");
    }
    let run_healthcheck_mode = cli.healthcheck || cli.healthcheck_json;
    if cli.doctor && (run_healthcheck_mode || cli.pause || cli.resume) {
        anyhow::bail!("--doctor cannot be combined with healthcheck/pause/resume flags");
    }

    // Set up tracing — suppress stderr output in TUI mode to avoid corrupting the display
    let tui_mode = !cli.headless
        && !cli.dry_run
        && !cli.once
        && !cli.doctor
        && !run_healthcheck_mode
        && !cli.pause
        && !cli.resume;
    let default_level = if tui_mode { "warn" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .with_target(false)
        .init();

    info!("Starting Jellyfin AV1 Transcoding Watchdog");

    // Load or generate config
    let config = if cli.simulate {
        info!("Simulation mode: using default config with in-memory database");
        Config::default_config()
    } else {
        match Config::load(&cli.config) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load config from {}: {}", cli.config.display(), e);
                if run_healthcheck_mode {
                    std::process::exit(HC_CONFIG_FAIL);
                }
                anyhow::bail!("Config load failed: {}", e);
            }
        }
    };

    // Determine base directory (config file's parent, or cwd)
    let base_dir = if cli.simulate {
        std::env::current_dir()?
    } else {
        cli.config
            .canonicalize()
            .unwrap_or_else(|_| cli.config.clone())
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf()
    };

    if cli.pause || cli.resume {
        let pause_path = config.resolve_path(&base_dir, &config.safety.pause_file);
        if cli.pause {
            if let Some(parent) = pause_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let marker = format!("paused_at={}\n", chrono::Utc::now().to_rfc3339());
            std::fs::write(&pause_path, marker.as_bytes())?;
            println!("pause file created: {}", pause_path.display());
        } else if pause_path.exists() {
            std::fs::remove_file(&pause_path)?;
            println!("pause file removed: {}", pause_path.display());
        } else {
            println!("pause file already absent: {}", pause_path.display());
        }
        return Ok(());
    }

    // Validate config
    let validation_errors = config.validate();
    if !validation_errors.is_empty() && !cli.simulate {
        for err in &validation_errors {
            error!("Config validation error: {}", err);
        }
        if run_healthcheck_mode {
            std::process::exit(HC_CONFIG_FAIL);
        }
        anyhow::bail!(
            "Configuration validation failed with {} error(s)",
            validation_errors.len()
        );
    }

    let status_snapshot_path = resolve_status_snapshot_path(&config, &base_dir);

    // Open database
    let db = if cli.simulate {
        Arc::new(WatchdogDb::open_in_memory()?)
    } else {
        let db_path = config.resolve_path(&base_dir, &config.paths.database);
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        info!("Database: {}", db_path.display());
        match WatchdogDb::open(&db_path) {
            Ok(db) => Arc::new(db),
            Err(e) => {
                error!("Failed to open database at {}: {}", db_path.display(), e);
                if run_healthcheck_mode {
                    std::process::exit(HC_DB_FAIL);
                }
                return Err(e.into());
            }
        }
    };

    if run_healthcheck_mode {
        let exit_code =
            match run_healthcheck(&config, &base_dir, &db, cli.simulate, cli.healthcheck_json) {
                Ok(code) => code,
                Err(e) => {
                    error!("Healthcheck internal error: {}", e);
                    HC_INTERNAL_FAIL
                }
            };
        std::process::exit(exit_code);
    }

    if cli.doctor {
        let exit_code = run_doctor(&config, &base_dir, &db, cli.simulate)?;
        std::process::exit(exit_code);
    }

    // Acquire instance lock (skip in simulation mode)
    let _instance_lock = if !cli.simulate {
        let lock_path = config
            .resolve_path(&base_dir, &config.paths.database)
            .with_extension("lock");
        match util::InstanceLock::acquire(&lock_path) {
            Ok(lock) => {
                info!("Instance lock acquired: {}", lock.path().display());
                Some(lock)
            }
            Err(e) => {
                error!("{}", e);
                anyhow::bail!("{}", e);
            }
        }
    } else {
        None
    };

    // Verify dependencies (skip in simulation mode)
    if !cli.simulate {
        if let Err(missing) = util::verify_dependencies() {
            error!("Missing required tools: {}", missing.join(", "));
            anyhow::bail!("Dependency check failed. Install: {}", missing.join(", "));
        }
        info!("All required CLI tools found: ffprobe, HandBrakeCLI, rsync");
    }

    // Create state manager and seed cumulative stats from DB
    let (state, _state_rx) = StateManager::new();

    {
        let total_saved = db.get_total_space_saved();
        let total_transcoded = db.get_transcode_count();
        let total_inspected = db.get_inspected_count();
        let top_reasons = db.get_top_failure_reasons(3);
        state.update(|s| {
            s.total_space_saved = total_saved;
            s.total_transcoded = total_transcoded as u64;
            s.total_inspected = total_inspected as u64;
            s.simulate_mode = cli.simulate;
            s.top_failure_reasons = top_reasons
                .iter()
                .map(|(reason, count)| (reason.clone(), *count as u64))
                .collect();
        });
    }

    // Create shutdown channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Build dependencies
    let deps = if cli.simulate {
        create_simulated_deps(&config)
    } else {
        PipelineDeps {
            fs: Box::new(RealFileSystem),
            prober: Box::new(FfprobeProber),
            transcoder: Box::new(HandBrakeTranscoder),
            transfer: Box::new(RsyncTransfer),
            mount_manager: Box::new(SystemMountManager),
            in_use_detector: Box::new(CommandInUseDetector::new(
                config.safety.in_use_guard_command.clone(),
            )),
        }
    };

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

    // Run based on mode
    if cli.headless || cli.dry_run || cli.once {
        // Headless mode: run pipeline with signal handling for graceful shutdown
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
                tokio::select! {
                    _ = sigterm.recv() => info!("Received SIGTERM"),
                    _ = tokio::signal::ctrl_c() => info!("Received SIGINT"),
                }
            } else {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => info!("Received SIGINT"),
                    Err(e) => {
                        warn!("Failed to register SIGINT handler: {}", e);
                        return;
                    }
                }
            }
            info!("Signalling pipeline shutdown...");
            let _ = signal_shutdown_tx.send(());
        });

        let pipeline_result = run_pipeline_loop(
            config,
            base_dir,
            deps,
            db.clone(),
            state.clone(),
            cli.dry_run,
            cli.once,
            shutdown_tx.subscribe(),
        )
        .await;

        let _ = shutdown_tx.send(());
        pipeline_result?;
    } else {
        // TUI mode: run pipeline in background, TUI in foreground
        let pipeline_config = config.clone();
        let pipeline_base_dir = base_dir.clone();
        let pipeline_db = db.clone();
        let pipeline_shutdown_rx = shutdown_tx.subscribe();

        // The state manager needs to be shared between pipeline and TUI.
        // Pipeline writes via StateManager, TUI reads via watch::Receiver.
        // We'll give the pipeline the StateManager and the TUI the Receiver.
        let tui_state_rx = state.subscribe();
        let pipeline_state = state.clone();

        let pipeline_handle = tokio::spawn(async move {
            run_pipeline_loop(
                pipeline_config,
                pipeline_base_dir,
                deps,
                pipeline_db,
                pipeline_state,
                false,
                false,
                pipeline_shutdown_rx,
            )
            .await
        });

        // Run TUI on the main thread (crossterm needs it)
        let tui_result = tui::run_tui(tui_state_rx, db.clone(), shutdown_tx.clone()).await;

        // Signal shutdown to pipeline
        let _ = shutdown_tx.send(());

        // Wait for pipeline to finish
        match pipeline_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("Pipeline error: {}", e),
            Err(e) => error!("Pipeline task join error: {}", e),
        }

        if let Err(e) = tui_result {
            error!("TUI error: {}", e);
        }
    }

    if cli.dry_run {
        if let Some(snapshot_path) = status_snapshot_path.as_ref() {
            let cooldown = db.get_cooldown_active_count(chrono::Utc::now().timestamp());
            let latest_failure = db.get_latest_failure();
            if let Err(e) = write_snapshot(
                snapshot_path,
                &state.snapshot(),
                cooldown,
                latest_failure.as_ref(),
            ) {
                warn!("Failed to write dry-run status snapshot: {}", e);
            }
        }
    }

    if let Some(handle) = snapshot_handle {
        match handle.await {
            Ok(()) => {}
            Err(e) => warn!("Status snapshot task join error: {}", e),
        }
    }

    info!("Watchdog shut down cleanly");
    Ok(())
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
        println!("  mode: {}", result.mode);
        println!("  paused: {}", result.paused);
        println!("  cooldown_files: {}", result.cooldown_files);
        println!(
            "  checks: config={} db={} dependencies={} nfs={}",
            result.checks.config, result.checks.db, result.checks.dependencies, result.checks.nfs
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
                    "  mount: {} healthy={} latency_ms={}",
                    mount.share, mount.healthy, mount.latency_ms
                );
            }
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

    let checks = HealthcheckChecks {
        config: true,
        db: db_query_ok,
        dependencies: missing_dependencies.is_empty(),
        nfs: unhealthy_shares.is_empty(),
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
        checks,
        paused,
        cooldown_files,
        mode: if simulate {
            "simulate".to_string()
        } else {
            "live".to_string()
        },
        missing_dependencies,
        unhealthy_shares,
        db_query_ok,
        dependency_diagnostics,
        mount_checks,
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
    let out = run_command(
        cmd,
        RunOptions {
            timeout: Some(Duration::from_secs(3)),
            stdout_limit: 4096,
            stderr_limit: 4096,
            ..RunOptions::default()
        },
    )
    .ok()?;
    if !out.status.success() || out.timed_out {
        return None;
    }
    let text = if !out.stdout.is_empty() {
        String::from_utf8_lossy(&out.stdout).to_string()
    } else {
        String::from_utf8_lossy(&out.stderr_tail).to_string()
    };
    text.lines().next().map(|line| line.trim().to_string())
}

fn run_doctor(
    config: &Config,
    base_dir: &std::path::Path,
    db: &WatchdogDb,
    simulate: bool,
) -> anyhow::Result<i32> {
    let validation_errors = config.validate();
    let db_ok = db.healthcheck_query_ok();
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

    println!("watchdog_doctor");
    println!("  mode: {}", if simulate { "simulate" } else { "live" });
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
    println!("  mounts:");
    for mount in &mount_checks {
        println!(
            "    - {} [{}]: {} ({} ms)",
            mount.share,
            mount.mount,
            if mount.healthy {
                "healthy"
            } else {
                "unhealthy"
            },
            mount.latency_ms
        );
    }
    if !validation_errors.is_empty() {
        println!("  config_errors:");
        for err in &validation_errors {
            println!("    - {}", err);
        }
    }

    let has_missing_deps = dep_results.iter().any(|d| !d.found);
    let has_unhealthy_mounts = mount_checks.iter().any(|m| !m.healthy);
    let failed =
        !validation_errors.is_empty() || !db_ok || has_missing_deps || has_unhealthy_mounts;
    println!("  status: {}", if failed { "failed" } else { "ok" });

    Ok(if failed { 1 } else { 0 })
}
