use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use watchdog::config::{Config, ShareConfig};
use watchdog::error::WatchdogError;
use watchdog::simulate::{SimulatedFileSystem, SimulatedTranscoder};
use watchdog::traits::{FileSystem, Transcoder};

fn sim_config() -> Config {
    let mut cfg = Config::default_config();
    cfg.shares = vec![ShareConfig {
        name: "movies".to_string(),
        remote_path: "/remote/movies".to_string(),
        local_mount: "/mnt/movies".to_string(),
    }];
    cfg.scan.video_extensions = vec![".mkv".to_string(), ".mp4".to_string(), ".avi".to_string()];
    cfg.normalize();
    cfg
}

#[test]
fn rename_updates_state_in_simulation() {
    let cfg = sim_config();
    let fs = SimulatedFileSystem::new_with_seed(&cfg, 42);
    let entries = fs
        .walk_share(
            "movies",
            Path::new("/mnt/movies"),
            &cfg.scan.video_extensions,
        )
        .unwrap();
    let source = entries.first().unwrap().path.clone();
    let backup = PathBuf::from(format!("{}.watchdog.old", source.display()));

    fs.rename(&source, &backup).unwrap();
    assert!(!fs.exists(&source));
    assert!(fs.exists(&backup));
}

#[test]
fn remove_and_suffix_walk_reflect_current_state() {
    let cfg = sim_config();
    let fs = SimulatedFileSystem::new_with_seed(&cfg, 7);
    let entries = fs
        .walk_share(
            "movies",
            Path::new("/mnt/movies"),
            &cfg.scan.video_extensions,
        )
        .unwrap();
    let source = entries.first().unwrap().path.clone();
    let orphan = PathBuf::from(format!("{}.watchdog.old", source.display()));

    fs.rename(&source, &orphan).unwrap();
    let found = fs
        .walk_files_with_suffix(Path::new("/mnt/movies"), ".watchdog.old")
        .unwrap();
    assert!(found.contains(&orphan));

    fs.remove(&orphan).unwrap();
    assert!(!fs.exists(&orphan));
}

#[test]
fn list_dir_returns_direct_children_only() {
    let cfg = sim_config();
    let fs = SimulatedFileSystem::new_with_seed(&cfg, 99);
    let root = Path::new("/mnt/movies");
    let direct = fs.list_dir(root).unwrap();

    assert!(direct.iter().all(|p| p.parent() == Some(root)));
}

#[test]
fn simulated_transcoder_honors_cancel_signal() {
    let cfg = sim_config();
    let fs = Arc::new(SimulatedFileSystem::new_with_seed(&cfg, 5));
    let entries = fs
        .walk_share(
            "movies",
            Path::new("/mnt/movies"),
            &cfg.scan.video_extensions,
        )
        .unwrap();
    let input = entries.first().unwrap().path.clone();
    let output = PathBuf::from("/tmp/out.mkv");
    let transcoder = SimulatedTranscoder::new(fs);
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let cancel = Arc::new(AtomicBool::new(true));
    let result = transcoder.transcode(
        &input,
        &output,
        Path::new("preset.json"),
        "preset",
        10,
        tx,
        cancel,
    );
    assert!(matches!(
        result,
        Err(WatchdogError::TranscodeCancelled { .. })
    ));
}
