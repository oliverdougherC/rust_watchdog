use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use watchdog::config::{Config, ShareConfig};
use watchdog::db::{NewQueueItem, TranscodeOutcome, WatchdogDb};
use watchdog::error::{Result, WatchdogError};
use watchdog::pipeline::{run_pipeline_loop, run_watchdog_pass, PipelineDeps};
use watchdog::state::{PipelinePhase, RunMode, StateManager};
use watchdog::traits::*;

#[derive(Clone)]
struct TestMediaFile {
    size: u64,
    mtime: f64,
    codec: String,
    format_name: String,
    duration_seconds: f64,
    video_stream_count: u32,
    audio_stream_count: u32,
    subtitle_stream_count: u32,
}

fn format_name_for_path(path: &Path) -> String {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "mkv" | "webm" => "matroska,webm".to_string(),
        "mp4" | "mov" => "mov,mp4,m4a,3gp,3g2,mj2".to_string(),
        "avi" => "avi".to_string(),
        _ => "matroska,webm".to_string(),
    }
}

fn default_media_file(path: &Path, size: u64, mtime: f64) -> TestMediaFile {
    let codec = if path.to_string_lossy().contains(".av1.") {
        "av1"
    } else {
        "h264"
    };
    TestMediaFile {
        size,
        mtime,
        codec: codec.to_string(),
        format_name: format_name_for_path(path),
        duration_seconds: 100.0,
        video_stream_count: 1,
        audio_stream_count: 1,
        subtitle_stream_count: 0,
    }
}

struct TestFs {
    files: Mutex<HashMap<PathBuf, TestMediaFile>>,
    share_roots: HashMap<String, PathBuf>,
    free_space_default: u64,
    free_space_sequence: Mutex<Vec<u64>>,
    free_space_error: AtomicBool,
    export_mutation: Mutex<Option<(PathBuf, u64)>>,
    recovery_walk_calls: AtomicUsize,
    walk_delay: Mutex<Option<Duration>>,
    active_walks: AtomicUsize,
    max_concurrent_walks: AtomicUsize,
    create_dir_calls: AtomicUsize,
    remove_calls: AtomicUsize,
    rename_calls: AtomicUsize,
}

#[derive(Clone)]
struct FsHandle(Arc<TestFs>);

impl TestFs {
    fn new(config: &Config) -> Self {
        let mut share_roots = HashMap::new();
        for s in &config.shares {
            share_roots.insert(s.name.clone(), PathBuf::from(&s.local_mount));
        }
        Self {
            files: Mutex::new(HashMap::new()),
            share_roots,
            free_space_default: u64::MAX / 2,
            free_space_sequence: Mutex::new(Vec::new()),
            free_space_error: AtomicBool::new(false),
            export_mutation: Mutex::new(None),
            recovery_walk_calls: AtomicUsize::new(0),
            walk_delay: Mutex::new(None),
            active_walks: AtomicUsize::new(0),
            max_concurrent_walks: AtomicUsize::new(0),
            create_dir_calls: AtomicUsize::new(0),
            remove_calls: AtomicUsize::new(0),
            rename_calls: AtomicUsize::new(0),
        }
    }

    fn insert(&self, path: &Path, size: u64, mtime: f64) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), default_media_file(path, size, mtime));
    }

    fn insert_transcoded(&self, path: &Path, size: u64, mtime: f64) {
        let mut file = default_media_file(path, size, mtime);
        file.codec = "av1".to_string();
        self.files.lock().unwrap().insert(path.to_path_buf(), file);
    }

    fn get_size(&self, path: &Path) -> Option<u64> {
        self.files.lock().unwrap().get(path).map(|v| v.size)
    }

    fn get_media(&self, path: &Path) -> Option<TestMediaFile> {
        self.files.lock().unwrap().get(path).cloned()
    }

    fn has_path(&self, path: &Path) -> bool {
        self.files.lock().unwrap().contains_key(path)
    }

    fn recovery_walk_calls(&self) -> usize {
        self.recovery_walk_calls.load(Ordering::Relaxed)
    }

    fn set_walk_delay(&self, delay: Duration) {
        *self.walk_delay.lock().unwrap() = Some(delay);
    }

    fn set_free_space_error(&self, enabled: bool) {
        self.free_space_error.store(enabled, Ordering::Relaxed);
    }

    fn set_free_space_sequence(&self, sequence: Vec<u64>) {
        *self.free_space_sequence.lock().unwrap() = sequence;
    }

    fn set_export_mutation(&self, path: &Path, size: u64) {
        *self.export_mutation.lock().unwrap() = Some((path.to_path_buf(), size));
    }

    fn max_concurrent_walks(&self) -> usize {
        self.max_concurrent_walks.load(Ordering::Relaxed)
    }

    fn mutation_calls(&self) -> usize {
        self.create_dir_calls.load(Ordering::Relaxed)
            + self.remove_calls.load(Ordering::Relaxed)
            + self.rename_calls.load(Ordering::Relaxed)
    }

    fn bump_first_share_file_mtime(&self, delta: f64) {
        let mut files = self.files.lock().unwrap();
        for (path, media) in files.iter_mut() {
            if self.share_roots.values().any(|root| path.starts_with(root)) {
                media.mtime += delta;
                break;
            }
        }
    }
}

impl FileSystem for FsHandle {
    fn walk_share(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
    ) -> Result<Vec<FileEntry>> {
        self.walk_share_cancellable(share_name, root, extensions, None)
    }

    fn walk_share_cancellable(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
        cancel: Option<&AtomicBool>,
    ) -> Result<Vec<FileEntry>> {
        let active_now = self.0.active_walks.fetch_add(1, Ordering::Relaxed) + 1;
        loop {
            let observed = self.0.max_concurrent_walks.load(Ordering::Relaxed);
            if active_now <= observed {
                break;
            }
            if self
                .0
                .max_concurrent_walks
                .compare_exchange(observed, active_now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        if let Some(delay) = *self.0.walk_delay.lock().unwrap() {
            let start = Instant::now();
            while start.elapsed() < delay {
                if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                    self.0.active_walks.fetch_sub(1, Ordering::Relaxed);
                    return Ok(Vec::new());
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        let files = self.0.files.lock().unwrap();
        let mut out = Vec::new();
        for (path, media) in files.iter() {
            if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                break;
            }
            if !path.starts_with(root) {
                continue;
            }
            let ext_match = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_lowercase()))
                .is_some_and(|e| extensions.contains(&e));
            if !ext_match {
                continue;
            }
            out.push(FileEntry {
                path: path.clone(),
                size: media.size,
                mtime: media.mtime,
                share_name: share_name.to_string(),
            });
        }
        drop(files);
        self.0.active_walks.fetch_sub(1, Ordering::Relaxed);
        Ok(out)
    }

    fn file_size(&self, path: &Path) -> Result<u64> {
        self.0
            .files
            .lock()
            .unwrap()
            .get(path)
            .map(|v| v.size)
            .ok_or_else(|| {
                WatchdogError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "not found",
                ))
            })
    }

    fn file_mtime(&self, path: &Path) -> Result<f64> {
        self.0
            .files
            .lock()
            .unwrap()
            .get(path)
            .map(|v| v.mtime)
            .ok_or_else(|| {
                WatchdogError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "not found",
                ))
            })
    }

    fn exists(&self, path: &Path) -> bool {
        self.0.files.lock().unwrap().contains_key(path)
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.0.share_roots.values().any(|r| r == path)
            || self
                .0
                .files
                .lock()
                .unwrap()
                .keys()
                .any(|p| p.starts_with(path))
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.rename_calls.fetch_add(1, Ordering::Relaxed);
        let mut files = self.0.files.lock().unwrap();
        let value = files.remove(from).ok_or_else(|| {
            WatchdogError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "rename source missing",
            ))
        })?;
        let mut renamed = value;
        renamed.mtime += 1.0;
        files.insert(to.to_path_buf(), renamed);
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.0.remove_calls.fetch_add(1, Ordering::Relaxed);
        self.0.files.lock().unwrap().remove(path);
        Ok(())
    }

    fn free_space(&self, _path: &Path) -> Result<u64> {
        if self.0.free_space_error.load(Ordering::Relaxed) {
            return Err(WatchdogError::Io(std::io::Error::other(
                "free space probe failed",
            )));
        }
        let mut sequence = self.0.free_space_sequence.lock().unwrap();
        if !sequence.is_empty() {
            return Ok(sequence.remove(0));
        }
        Ok(self.0.free_space_default)
    }

    fn create_dir_all(&self, _path: &Path) -> Result<()> {
        self.0.create_dir_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        Ok(self
            .0
            .files
            .lock()
            .unwrap()
            .keys()
            .filter(|p| p.parent() == Some(path))
            .cloned()
            .collect())
    }

    fn walk_files_with_suffix(&self, root: &Path, suffix: &str) -> Result<Vec<PathBuf>> {
        self.0.recovery_walk_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .0
            .files
            .lock()
            .unwrap()
            .keys()
            .filter(|p| p.starts_with(root))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(suffix))
            })
            .cloned()
            .collect())
    }
}

struct TestProber {
    fs: Arc<TestFs>,
}

impl Prober for TestProber {
    fn probe(&self, path: &Path) -> Result<Option<ProbeResult>> {
        let media = match self.fs.get_media(path) {
            Some(media) => media,
            None => return Ok(None),
        };
        Ok(Some(ProbeResult {
            video_codec: Some(media.codec.clone()),
            stream_bitrate_bps: 0,
            format_bitrate_bps: ((media.size as f64 * 8.0) / media.duration_seconds) as u64,
            size_bytes: media.size,
            duration_seconds: media.duration_seconds,
            video_stream_count: media.video_stream_count,
            audio_stream_count: media.audio_stream_count,
            subtitle_stream_count: media.subtitle_stream_count,
            raw_json: serde_json::json!({
                "format": {
                    "format_name": media.format_name
                }
            }),
        }))
    }

    fn health_check(&self, _path: &Path) -> Result<bool> {
        Ok(true)
    }
}

enum TranscodeMode {
    TimeoutAlways,
    StalledAlways,
    SuccessWithOutputSize(u64),
    WaitForCancel,
    MutateSourceDuringTranscode { output_size: u64, mtime_delta: f64 },
}

struct TestTranscoder {
    fs: Arc<TestFs>,
    mode: TranscodeMode,
    calls: AtomicUsize,
}

impl TestTranscoder {
    fn new(fs: Arc<TestFs>, mode: TranscodeMode) -> Self {
        Self {
            fs,
            mode,
            calls: AtomicUsize::new(0),
        }
    }
}

impl Transcoder for TestTranscoder {
    fn transcode(
        &self,
        input: &Path,
        output: &Path,
        _preset_file: &Path,
        _preset_name: &str,
        timeout_secs: u64,
        _stall_timeout_secs: u64,
        _progress_tx: mpsc::Sender<TranscodeProgress>,
        cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<TranscodeResult> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.mode {
            TranscodeMode::TimeoutAlways => Err(WatchdogError::TranscodeTimeout {
                path: input.to_path_buf(),
                timeout_secs,
            }),
            TranscodeMode::StalledAlways => Err(WatchdogError::TranscodeStalled {
                path: input.to_path_buf(),
                stall_timeout_secs: 10,
            }),
            TranscodeMode::SuccessWithOutputSize(size) => {
                self.fs.insert_transcoded(output, size, 1000.0);
                Ok(TranscodeResult {
                    success: true,
                    timed_out: false,
                    output_exists: true,
                })
            }
            TranscodeMode::WaitForCancel => {
                for _ in 0..200 {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(WatchdogError::TranscodeCancelled {
                            path: input.to_path_buf(),
                            reason: "cancel".to_string(),
                        });
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Ok(TranscodeResult {
                    success: false,
                    timed_out: false,
                    output_exists: false,
                })
            }
            TranscodeMode::MutateSourceDuringTranscode {
                output_size,
                mtime_delta,
            } => {
                self.fs.bump_first_share_file_mtime(mtime_delta);
                self.fs.insert_transcoded(output, output_size, 1000.0);
                Ok(TranscodeResult {
                    success: true,
                    timed_out: false,
                    output_exists: true,
                })
            }
        }
    }
}

struct TestTransfer {
    fs: Arc<TestFs>,
}

impl FileTransfer for TestTransfer {
    fn transfer(
        &self,
        source: &Path,
        dest: &Path,
        _timeout_secs: u64,
        stage: TransferStage,
        progress_tx: Option<mpsc::Sender<TransferProgress>>,
    ) -> Result<TransferResult> {
        let media = self
            .fs
            .get_media(source)
            .ok_or_else(|| WatchdogError::Transfer {
                path: source.to_path_buf(),
                reason: "source missing".to_string(),
            })?;
        self.fs
            .files
            .lock()
            .unwrap()
            .insert(dest.to_path_buf(), media);
        if stage == TransferStage::Export {
            if let Some((path, size)) = self.fs.export_mutation.lock().unwrap().clone() {
                self.fs.insert(&path, size, 1000.0);
            }
        }
        if let Some(tx) = progress_tx {
            let _ = tx.try_send(TransferProgress {
                stage,
                percent: 100.0,
                rate_mib_per_sec: 0.0,
                eta: String::new(),
            });
        }
        Ok(TransferResult { success: true })
    }
}

struct TestMountManager {
    healthy: bool,
    remount_success: bool,
}

impl MountManager for TestMountManager {
    fn is_healthy(&self, _mount_point: &Path) -> bool {
        self.healthy
    }

    fn remount(
        &self,
        _server: &str,
        _remote_path: &str,
        _local_mount: &Path,
        _share_name: &str,
    ) -> Result<bool> {
        Ok(self.remount_success)
    }
}

enum InUseMode {
    Never,
    Always,
    Error,
    OnSecondCheck,
}

struct TestInUseDetector {
    mode: InUseMode,
    calls: AtomicUsize,
}

impl TestInUseDetector {
    fn never() -> Self {
        Self::with_mode(InUseMode::Never)
    }

    fn with_mode(mode: InUseMode) -> Self {
        Self {
            mode,
            calls: AtomicUsize::new(0),
        }
    }
}

impl InUseDetector for TestInUseDetector {
    fn check_in_use(&self, _path: &Path) -> Result<InUseStatus> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        match self.mode {
            InUseMode::Never => Ok(InUseStatus::NotInUse),
            InUseMode::Always => Ok(InUseStatus::InUse),
            InUseMode::Error => Err(WatchdogError::InUse {
                path: PathBuf::from("/test"),
                reason: "detector failure".to_string(),
            }),
            InUseMode::OnSecondCheck => {
                if call >= 2 {
                    Ok(InUseStatus::InUse)
                } else {
                    Ok(InUseStatus::NotInUse)
                }
            }
        }
    }
}

fn base_config() -> Config {
    let mut cfg = Config::default_config();
    cfg.transcode.preset_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("presets")
        .join("AV1_MKV.json")
        .display()
        .to_string();
    cfg.shares = vec![ShareConfig {
        name: "movies".to_string(),
        remote_path: "/remote/movies".to_string(),
        local_mount: "/mnt/movies".to_string(),
    }];
    cfg.scan.video_extensions = vec![".mkv".to_string()];
    cfg.scan.interval_seconds = 1;
    cfg.transcode.max_retries = 2;
    cfg.transcode.timeout_seconds = 1;
    cfg.normalize();
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_success_exits_once() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let result = run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        true,
        false,
        tx.subscribe(),
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_error_returns_immediately() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: false,
            remount_success: false,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let started = Instant::now();
    let result = run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        true,
        false,
        tx.subscribe(),
    )
    .await;

    assert!(result.is_err());
    assert!(started.elapsed().as_secs() < 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_does_not_mutate_db_or_filesystem() {
    let mut cfg = base_config();
    cfg.paths.event_journal.clear();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Quarantined.DryRun.mkv");
    let wd_old = PathBuf::from("/mnt/movies/Recovered.DryRun.mkv.watchdog.old");
    fs.insert(&source, 50_000_000, 1000.0);
    fs.insert(&wd_old, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    db.quarantine_file(
        &source.to_string_lossy(),
        "manual_quarantine",
        "test quarantine",
    );
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let result = run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db.clone(),
        state,
        RunMode::Watchdog,
        true,
        false,
        tx.subscribe(),
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(db.get_inspected_count(), 0);
    assert!(db.get_recent_transcodes(10).is_empty());
    assert!(fs.has_path(&wd_old));
    assert_eq!(fs.mutation_calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_retries_up_to_max_retries() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let transcoder = TestTranscoder::new(fs.clone(), TranscodeMode::TimeoutAlways);
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(transcoder),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let recent = db.get_recent_transcodes(10);
    assert_eq!(stats.transcode_failures, 0);
    assert_eq!(stats.retries_scheduled, 3);
    assert_eq!(state.snapshot().run_failures, 0);
    assert_eq!(state.snapshot().run_retries_scheduled, 3);
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].outcome, TranscodeOutcome::RetryScheduled);
    assert_eq!(recent[0].failure_code.as_deref(), Some("timeout_exhausted"));
}

#[tokio::test(flavor = "multi_thread")]
async fn stalled_retries_up_to_max_retries() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Stalled.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let transcoder = TestTranscoder::new(fs.clone(), TranscodeMode::StalledAlways);
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(transcoder),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let recent = db.get_recent_transcodes(10);
    assert_eq!(stats.transcode_failures, 0);
    assert_eq!(stats.retries_scheduled, 3);
    assert_eq!(state.snapshot().run_failures, 0);
    assert_eq!(state.snapshot().run_retries_scheduled, 3);
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].outcome, TranscodeOutcome::RetryScheduled);
    assert_eq!(recent[0].failure_code.as_deref(), Some("stalled_exhausted"));
}

#[tokio::test(flavor = "multi_thread")]
async fn no_space_savings_recorded_as_skipped() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(50_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.files_transcoded, 0);
    assert_eq!(db.get_transcode_count(), 0);
    assert_eq!(db.get_outcome_count(TranscodeOutcome::SkippedNoSavings), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn orphan_recovery_only_touches_watchdog_suffix() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let wd_old = PathBuf::from("/mnt/movies/Recovered.mkv.watchdog.old");
    let legacy_old = PathBuf::from("/mnt/movies/Legacy.mkv.old");
    fs.insert(&wd_old, 1000, 10.0);
    fs.insert(&legacy_old, 1000, 10.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(100),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        false,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert!(fs.has_path(Path::new("/mnt/movies/Recovered.mkv")));
    assert!(!fs.has_path(&wd_old));
    assert!(fs.has_path(&legacy_old));
    assert!(fs.recovery_walk_calls() >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn orphan_recovery_runs_on_startup() {
    let mut cfg = base_config();
    cfg.safety.recovery_scan_interval_seconds = 3600;
    let fs = Arc::new(TestFs::new(&cfg));
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(100),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        false,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert!(fs.recovery_walk_calls() >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn orphan_recovery_runs_periodically() {
    let mut cfg = base_config();
    cfg.safety.recovery_scan_interval_seconds = 1;
    cfg.scan.interval_seconds = 1;
    let fs = Arc::new(TestFs::new(&cfg));
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(100),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);

    let handle = tokio::spawn(run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        false,
        false,
        shutdown_tx.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(2400)).await;
    let _ = shutdown_tx.send(());
    let result = handle.await.unwrap();
    assert!(result.is_ok());
    assert!(
        fs.recovery_walk_calls() >= 4,
        "expected recovery scan at startup and at least one periodic run"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn precision_mode_waits_for_manual_queue_and_processes_it() {
    let mut cfg = base_config();
    cfg.scan.interval_seconds = 60;
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Precision.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);

    let handle = tokio::spawn(run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db.clone(),
        state.clone(),
        RunMode::Precision,
        false,
        false,
        shutdown_tx.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(state.snapshot().phase, PipelinePhase::AwaitingSelection);
    assert!(db.get_recent_transcodes(10).is_empty());

    db.enqueue_queue_items(&[NewQueueItem {
        source_path: source.to_string_lossy().to_string(),
        share_name: "movies".to_string(),
        enqueue_source: "manual".to_string(),
        preset_file: "presets/AV1_MKV.json".to_string(),
        preset_name: "AV1_MKV".to_string(),
        target_codec: "av1".to_string(),
    }]);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let _ = shutdown_tx.send(());
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let recent = db.get_recent_transcodes(10);
    assert!(
        recent
            .iter()
            .any(|record| record.source_path == source.to_string_lossy()),
        "expected precision-mode queue item to be processed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn precision_mode_processes_manual_override_for_already_compliant_file() {
    let mut cfg = base_config();
    cfg.scan.interval_seconds = 60;
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Precision.Movie.av1.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);

    let handle = tokio::spawn(run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db.clone(),
        state,
        RunMode::Precision,
        false,
        false,
        shutdown_tx.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(300)).await;
    db.enqueue_queue_items(&[NewQueueItem {
        source_path: source.to_string_lossy().to_string(),
        share_name: "movies".to_string(),
        enqueue_source: "manual".to_string(),
        preset_file: "presets/AV1_MKV.json".to_string(),
        preset_name: "AV1_MKV".to_string(),
        target_codec: "av1".to_string(),
    }]);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let _ = shutdown_tx.send(());
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let recent = db.get_recent_transcodes(10);
    assert!(
        recent
            .iter()
            .any(|record| record.source_path == source.to_string_lossy()),
        "expected already-compliant manual queue item to be processed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_file_blocks_scan_and_returns_paused() {
    let mut cfg = base_config();
    cfg.safety.pause_file = "watchdog.pause".to_string();
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("watchdog.pause"), b"paused").unwrap();

    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let err = run_watchdog_pass(&cfg, temp.path(), &deps, &db, &state, false, tx.subscribe())
        .await
        .unwrap_err();
    assert!(matches!(err, WatchdogError::Paused));
}

#[tokio::test(flavor = "multi_thread")]
async fn cooldown_skips_file_before_probe() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);

    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let _ = db.record_file_failure(
        &source.to_string_lossy(),
        "verification_failed",
        "verification_failed",
        1,
        600,
        600,
    );

    let (state, mut rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();
    assert_eq!(stats.files_inspected, 0);
    let _ = rx.changed().await;
    assert!(rx.borrow().run_skipped_cooldown >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_files_per_pass_caps_queue() {
    let mut cfg = base_config();
    cfg.scan.max_files_per_pass = 1;
    let fs = Arc::new(TestFs::new(&cfg));
    fs.insert(Path::new("/mnt/movies/A.mkv"), 50_000_000, 1000.0);
    fs.insert(Path::new("/mnt/movies/B.mkv"), 51_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();
    assert_eq!(stats.files_queued, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_during_transcode_records_interrupted_by_pause() {
    let mut cfg = base_config();
    cfg.safety.pause_file = "watchdog.pause".to_string();
    let temp = tempfile::tempdir().unwrap();
    let pause_path = temp.path().join("watchdog.pause");

    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Test.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::WaitForCancel,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    std::thread::spawn({
        let pause_path = pause_path.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(120));
            let _ = std::fs::write(pause_path, b"pause");
        }
    });

    let err = run_watchdog_pass(&cfg, temp.path(), &deps, &db, &state, false, tx.subscribe())
        .await
        .unwrap_err();
    assert!(matches!(err, WatchdogError::Paused));

    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_reason.as_deref(),
        Some("interrupted_by_pause")
    );
    assert!(db
        .get_file_failure_state(&source.to_string_lossy())
        .is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn include_exclude_filters_apply_with_exclude_precedence() {
    let mut cfg = base_config();
    cfg.scan.include_globs = vec!["/mnt/movies/*.mkv".to_string()];
    cfg.scan.exclude_globs = vec!["/mnt/movies/B*.mkv".to_string()];
    cfg.normalize();

    let fs = Arc::new(TestFs::new(&cfg));
    fs.insert(Path::new("/mnt/movies/A.mkv"), 50_000_000, 1000.0);
    fs.insert(Path::new("/mnt/movies/B.mkv"), 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, mut rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let _ = rx.changed().await;
    assert!(rx.borrow().run_skipped_filtered >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn include_glob_without_path_matches_basename() {
    let mut cfg = base_config();
    cfg.scan.include_globs = vec!["A*.mkv".to_string()];
    cfg.normalize();

    let fs = Arc::new(TestFs::new(&cfg));
    fs.insert(Path::new("/mnt/movies/A.Movie.mkv"), 50_000_000, 1000.0);
    fs.insert(Path::new("/mnt/movies/B.Movie.mkv"), 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, mut rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        true,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let _ = rx.changed().await;
    assert_eq!(stats.files_queued, 1);
    assert!(rx.borrow().run_skipped_filtered >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn in_use_guard_skips_before_transfer_without_cooldown() {
    let mut cfg = base_config();
    cfg.safety.in_use_guard_enabled = true;
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/InUse.Movie.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();

    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::with_mode(InUseMode::Always)),
    };
    let (tx, _) = broadcast::channel(1);
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let recent = db.get_recent_transcodes(1);
    assert_eq!(recent[0].failure_reason.as_deref(), Some("file_in_use"));
    assert!(db
        .get_file_failure_state(&source.to_string_lossy())
        .is_none());
    assert!(state.snapshot().run_skipped_in_use >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn in_use_guard_skips_before_replace_and_cleans_temp() {
    let mut cfg = base_config();
    cfg.safety.in_use_guard_enabled = true;
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/InUse.BeforeReplace.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();

    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::with_mode(InUseMode::OnSecondCheck)),
    };
    let (tx, _) = broadcast::channel(1);
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    let recent = db.get_recent_transcodes(1);
    assert_eq!(recent[0].failure_reason.as_deref(), Some("file_in_use"));
    assert_eq!(fs.get_size(&source), Some(50_000_000));
    assert!(db
        .get_file_failure_state(&source.to_string_lossy())
        .is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn in_use_detector_error_is_best_effort_continue() {
    let mut cfg = base_config();
    cfg.safety.in_use_guard_enabled = true;
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Error.BestEffort.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();

    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::with_mode(InUseMode::Error)),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.files_transcoded, 1);
    assert_eq!(db.get_transcode_count(), 1);
    assert_eq!(state.snapshot().run_skipped_in_use, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_timeout_aborts_pass_before_transcode() {
    let mut cfg = base_config();
    cfg.safety.share_scan_timeout_seconds = 1;
    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_walk_delay(Duration::from_millis(1500));
    fs.insert(Path::new("/mnt/movies/Slow.Scan.mkv"), 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let err = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WatchdogError::ScanTimeout { .. }));
    assert!(db.get_recent_transcodes(10).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_scan_timeouts_do_not_overlap_workers() {
    let mut cfg = base_config();
    cfg.safety.share_scan_timeout_seconds = 1;
    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_walk_delay(Duration::from_millis(1500));
    fs.insert(
        Path::new("/mnt/movies/Timeout.Overlap.Guard.mkv"),
        50_000_000,
        1000.0,
    );
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let first = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap_err();
    assert!(matches!(first, WatchdogError::ScanTimeout { .. }));

    let second = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap_err();
    assert!(matches!(second, WatchdogError::ScanTimeout { .. }));
    assert_eq!(fs.max_concurrent_walks(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn temp_space_probe_error_is_fail_closed_and_persisted() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_free_space_error(true);
    let source = PathBuf::from("/mnt/movies/Space.Probe.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);
    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.files_transcoded, 0);
    assert_eq!(db.get_transcode_count(), 0);
    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_code.as_deref(),
        Some("temp_space_probe_error")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn insufficient_nfs_space_increments_failure_counters() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_free_space_sequence(vec![u64::MAX / 2, 1]);
    let source = PathBuf::from("/mnt/movies/Nfs.Space.Low.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.transcode_failures, 1);
    assert_eq!(state.snapshot().run_failures, 1);
    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_code.as_deref(),
        Some("insufficient_nfs_space")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_changed_during_transcode_increments_failure_counters() {
    let cfg = base_config();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Source.Changed.During.Transcode.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::MutateSourceDuringTranscode {
                output_size: 10_000_000,
                mtime_delta: 5.0,
            },
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.transcode_failures, 1);
    assert_eq!(state.snapshot().run_failures, 1);
    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_code.as_deref(),
        Some("source_changed_during_transcode")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_changed_during_export_preserves_remote_temp_and_skips_swap() {
    let mut cfg = base_config();
    cfg.scan.video_extensions = vec![".mp4".to_string(), ".mkv".to_string()];
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Source.Changed.During.Export.mp4");
    let preserved_temp = PathBuf::from("/mnt/movies/Source.Changed.During.Export.mkv.watchdog.tmp");
    fs.insert(&source, 50_000_000, 1000.0);
    fs.set_export_mutation(&source, 51_000_000);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let stats = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();

    assert_eq!(stats.transcode_failures, 1);
    assert!(fs.has_path(&source));
    assert!(fs.has_path(&preserved_temp));
    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_code.as_deref(),
        Some("source_changed_during_transcode")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_transcode_cleans_temp_and_finalizes_row() {
    let mut cfg = base_config();
    cfg.paths.transcode_temp = "/tmp/watchdog-test-temp".to_string();
    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Shutdown.Cancel.mkv");
    fs.insert(&source, 50_000_000, 1000.0);
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::WaitForCancel,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);

    std::thread::spawn({
        let shutdown_tx = shutdown_tx.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(120));
            let _ = shutdown_tx.send(());
        }
    });

    let err = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps,
        &db,
        &state,
        false,
        shutdown_tx.subscribe(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WatchdogError::Shutdown));

    let temp_entries = deps
        .fs
        .list_dir(Path::new(&cfg.paths.transcode_temp))
        .unwrap();
    assert!(
        temp_entries.is_empty(),
        "temp files not cleaned: {temp_entries:?}"
    );

    let recent = db.get_recent_transcodes(1);
    assert_eq!(
        recent[0].failure_reason.as_deref(),
        Some("interrupted_by_shutdown")
    );
    assert!(recent[0].completed_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_loop_records_scan_timeout_in_state() {
    let mut cfg = base_config();
    cfg.safety.share_scan_timeout_seconds = 1;
    cfg.scan.interval_seconds = 1;
    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_walk_delay(Duration::from_millis(1500));
    fs.insert(
        Path::new("/mnt/movies/State.Timeout.mkv"),
        50_000_000,
        1000.0,
    );
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let assert_db = db.clone();
    let (state, _rx) = StateManager::new();
    let assert_state = state.clone();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);
    let handle = tokio::spawn(run_pipeline_loop(
        cfg,
        PathBuf::from("."),
        deps,
        db,
        state,
        RunMode::Watchdog,
        false,
        false,
        shutdown_tx.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(1300)).await;
    let _ = shutdown_tx.send(());
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let snapshot = assert_state.snapshot();
    assert!(snapshot.scan_timeout_count >= 1);
    assert_eq!(snapshot.last_failure_code.as_deref(), Some("scan_timeout"));
    let recent = assert_db.get_recent_transcodes(1);
    assert_eq!(recent[0].source_path, "[pipeline]");
    assert_eq!(recent[0].failure_code.as_deref(), Some("scan_timeout"));
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_scan_timeouts_trigger_auto_pause_tripwire() {
    let mut cfg = base_config();
    cfg.safety.share_scan_timeout_seconds = 1;
    cfg.safety.max_consecutive_pass_failures = 1;
    cfg.safety.auto_pause_on_pass_failures = true;
    cfg.scan.interval_seconds = 1;
    cfg.safety.pause_file = "watchdog.pause".to_string();
    let base = tempfile::tempdir().unwrap();
    let pause_path = base.path().join("watchdog.pause");

    let fs = Arc::new(TestFs::new(&cfg));
    fs.set_walk_delay(Duration::from_millis(1500));
    fs.insert(
        Path::new("/mnt/movies/Tripwire.Timeout.mkv"),
        50_000_000,
        1000.0,
    );
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let assert_db = db.clone();
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (shutdown_tx, _) = broadcast::channel(1);

    let handle = tokio::spawn(run_pipeline_loop(
        cfg,
        base.path().to_path_buf(),
        deps,
        db,
        state,
        RunMode::Watchdog,
        false,
        false,
        shutdown_tx.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        pause_path.exists(),
        "expected auto-pause marker to be created"
    );
    let _ = shutdown_tx.send(());
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let recent = assert_db.get_recent_transcodes(20);
    assert!(recent
        .iter()
        .any(|r| r.failure_code.as_deref() == Some("auto_paused_safety_trip")));
    let service = assert_db.get_service_state();
    assert!(service.auto_paused_at.is_some());
    assert_eq!(assert_db.get_transcode_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn quarantined_file_is_skipped_until_cleared() {
    let mut cfg = base_config();
    cfg.transcode.max_retries = 0;
    cfg.safety.cooldown_base_seconds = 1;
    cfg.safety.cooldown_max_seconds = 1;
    cfg.safety.quarantine_after_failures = 2;
    cfg.safety.quarantine_failure_codes = vec!["timeout_exhausted".to_string()];

    let fs = Arc::new(TestFs::new(&cfg));
    let source = PathBuf::from("/mnt/movies/Quarantine.Target.mkv");
    fs.insert(&source, 50_000_000, 1000.0);

    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state_a, _rx_a) = StateManager::new();
    let deps_a = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps_a,
        &db,
        &state_a,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();
    assert!(!db.is_quarantined(&source.to_string_lossy()));
    let first_pass_latest = db.get_recent_transcodes(1);
    assert_eq!(
        first_pass_latest[0].outcome,
        TranscodeOutcome::RetryScheduled
    );
    assert_eq!(
        first_pass_latest[0].failure_code.as_deref(),
        Some("timeout_exhausted")
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let (state_b, _rx_b) = StateManager::new();
    let deps_b = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps_b,
        &db,
        &state_b,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();
    assert!(db.is_quarantined(&source.to_string_lossy()));
    let second_pass_latest = db.get_recent_transcodes(1);
    assert_eq!(second_pass_latest[0].outcome, TranscodeOutcome::Failed);
    assert_eq!(
        second_pass_latest[0].failure_code.as_deref(),
        Some("quarantined_file")
    );

    let (state_c, _rx_c) = StateManager::new();
    let deps_c = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps_c,
        &db,
        &state_c,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();
    let recent = db.get_recent_transcodes(5);
    assert!(recent
        .iter()
        .any(|r| r.failure_code.as_deref() == Some("quarantined_file")));

    assert!(db.clear_quarantine_file(&source.to_string_lossy()));
    db.clear_file_failure_state(&source.to_string_lossy());
    let (state_d, _rx_d) = StateManager::new();
    let deps_d = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::TimeoutAlways,
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let _ = run_watchdog_pass(
        &cfg,
        Path::new("."),
        &deps_d,
        &db,
        &state_d,
        false,
        tx.subscribe(),
    )
    .await
    .unwrap();
    let latest = db.get_recent_transcodes(1);
    assert_ne!(latest[0].failure_code.as_deref(), Some("quarantined_file"));
}

#[tokio::test(flavor = "multi_thread")]
async fn event_journal_writes_pass_start_and_end_rows() {
    let mut cfg = base_config();
    cfg.paths.event_journal = "events.ndjson".to_string();
    let base = tempfile::tempdir().unwrap();
    let fs = Arc::new(TestFs::new(&cfg));
    let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
    let (state, _rx) = StateManager::new();
    let deps = PipelineDeps {
        fs: Arc::new(FsHandle(fs.clone())),
        prober: Box::new(TestProber { fs: fs.clone() }),
        transcoder: Box::new(TestTranscoder::new(
            fs.clone(),
            TranscodeMode::SuccessWithOutputSize(10_000_000),
        )),
        transfer: Box::new(TestTransfer { fs: fs.clone() }),
        mount_manager: Box::new(TestMountManager {
            healthy: true,
            remount_success: true,
        }),
        in_use_detector: Box::new(TestInUseDetector::never()),
    };
    let (tx, _) = broadcast::channel(1);

    let _ = run_watchdog_pass(&cfg, base.path(), &deps, &db, &state, true, tx.subscribe())
        .await
        .unwrap();

    let journal = std::fs::read_to_string(base.path().join("events.ndjson")).unwrap();
    let mut events = Vec::new();
    for line in journal.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        events.push(
            value
                .get("event")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
        );
    }
    assert!(events.iter().any(|e| e == "pass_start"));
    assert!(events.iter().any(|e| e == "pass_end"));
}
