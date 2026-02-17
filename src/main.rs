mod config;
mod db;
mod error;
mod nfs;
mod pipeline;
mod probe;
mod scanner;
mod simulate;
mod state;
mod stats;
mod traits;
mod transcode;
mod transfer;
mod tui;
mod util;

use crate::config::Config;
use crate::db::WatchdogDb;
use crate::nfs::SystemMountManager;
use crate::pipeline::{PipelineDeps, run_pipeline_loop};
use crate::probe::FfprobeProber;
use crate::scanner::RealFileSystem;
use crate::simulate::create_simulated_deps;
use crate::state::StateManager;
use crate::transcode::HandBrakeTranscoder;
use crate::transfer::RsyncTransfer;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "watchdog", about = "Jellyfin AV1 Transcoding Watchdog", version)]
struct Cli {
    /// Run in simulation mode with fake data
    #[arg(long)]
    simulate: bool,

    /// Scan and report queue, then exit
    #[arg(long)]
    dry_run: bool,

    /// No TUI, log to stdout (for running as a service)
    #[arg(long)]
    headless: bool,

    /// Custom config file path
    #[arg(long, default_value = "watchdog.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Set up tracing — suppress stderr output in TUI mode to avoid corrupting the display
    let tui_mode = !cli.headless && !cli.dry_run;
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
                anyhow::bail!("Config load failed: {}", e);
            }
        }
    };

    // Validate config
    let validation_errors = config.validate();
    if !validation_errors.is_empty() && !cli.simulate {
        for err in &validation_errors {
            error!("Config validation error: {}", err);
        }
        anyhow::bail!(
            "Configuration validation failed with {} error(s)",
            validation_errors.len()
        );
    }

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

    // Open database
    let db = if cli.simulate {
        Arc::new(WatchdogDb::open_in_memory()?)
    } else {
        let db_path = config.resolve_path(&base_dir, &config.paths.database);
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        info!("Database: {}", db_path.display());
        Arc::new(WatchdogDb::open(&db_path)?)
    };

    // Acquire instance lock (skip in simulation mode)
    let _instance_lock = if !cli.simulate {
        let lock_path = config.resolve_path(&base_dir, &config.paths.database)
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
        state.update(|s| {
            s.total_space_saved = total_saved;
            s.total_transcoded = total_transcoded as u64;
            s.total_inspected = total_inspected as u64;
            s.simulate_mode = cli.simulate;
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
        }
    };

    // Run based on mode
    if cli.headless || cli.dry_run {
        // Headless mode: run pipeline with signal handling for graceful shutdown
        let signal_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            let sigint = tokio::signal::ctrl_c();
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = sigint => info!("Received SIGINT"),
            }
            info!("Signalling pipeline shutdown...");
            let _ = signal_shutdown_tx.send(());
        });

        run_pipeline_loop(
            config,
            base_dir,
            deps,
            db,
            state,
            cli.dry_run,
            shutdown_tx.subscribe(),
        )
        .await;
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

        let pipeline_handle = tokio::spawn(async move {
            run_pipeline_loop(
                pipeline_config,
                pipeline_base_dir,
                deps,
                pipeline_db,
                state,
                false,
                pipeline_shutdown_rx,
            )
            .await;
        });

        // Run TUI on the main thread (crossterm needs it)
        let tui_result = tui::run_tui(tui_state_rx, db.clone(), shutdown_tx.clone()).await;

        // Signal shutdown to pipeline
        let _ = shutdown_tx.send(());

        // Wait for pipeline to finish
        let _ = pipeline_handle.await;

        if let Err(e) = tui_result {
            error!("TUI error: {}", e);
        }
    }

    info!("Watchdog shut down cleanly");
    Ok(())
}
