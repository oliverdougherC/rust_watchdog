use crate::config::Config;
use crate::error::Result;
use crate::in_use::SimulatedInUseDetector;
use crate::pipeline::PipelineDeps;
use crate::traits::*;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;

/// Simulated VFS entry.
#[derive(Debug, Clone)]
struct SimFile {
    path: PathBuf,
    size: u64,
    mtime: f64,
    codec: String,
    bitrate_mbps: f64,
    duration_secs: f64,
    audio_stream_count: u32,
    subtitle_stream_count: u32,
}

/// Simulated filesystem with in-memory virtual files.
pub struct SimulatedFileSystem {
    files: Mutex<HashMap<String, Vec<SimFile>>>,
    temp_files: Mutex<HashMap<PathBuf, SimFile>>,
    share_roots: HashMap<String, PathBuf>,
    directories: Mutex<HashSet<PathBuf>>,
}

impl SimulatedFileSystem {
    pub fn new(config: &Config) -> Self {
        let seed = std::env::var("WATCHDOG_SIM_SEED")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(rand::random);
        Self::new_with_seed(config, seed)
    }

    pub fn new_with_seed(config: &Config, seed: u64) -> Self {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut files = HashMap::new();
        let mut share_roots = HashMap::new();
        let mut directories = HashSet::new();
        let fixed_files_per_share = std::env::var("WATCHDOG_SIM_MAX_FILES_PER_SHARE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0);

        let codecs = ["h264", "hevc", "mpeg4", "av1"];
        let movie_names = [
            "The.Matrix.1999",
            "Inception.2010",
            "Interstellar.2014",
            "Blade.Runner.2049",
            "Dune.Part.Two.2024",
            "Oppenheimer.2023",
            "The.Dark.Knight.2008",
            "Pulp.Fiction.1994",
            "Fight.Club.1999",
            "The.Shawshank.Redemption.1994",
            "Parasite.2019",
            "Whiplash.2014",
            "Mad.Max.Fury.Road.2015",
            "Arrival.2016",
            "The.Godfather.1972",
            "Goodfellas.1990",
            "No.Country.for.Old.Men.2007",
            "There.Will.Be.Blood.2007",
            "The.Grand.Budapest.Hotel.2014",
            "Moonlight.2016",
            "La.La.Land.2016",
            "Joker.2019",
            "The.Social.Network.2010",
            "Drive.2011",
            "Sicario.2015",
        ];
        let tv_shows = [
            "Breaking.Bad",
            "Better.Call.Saul",
            "The.Wire",
            "Chernobyl",
            "True.Detective",
            "Severance",
            "The.Bear",
            "Succession",
            "The.Last.of.Us",
            "Shogun",
            "House.of.the.Dragon",
            "Andor",
            "Arcane",
            "The.Penguin",
            "Slow.Horses",
        ];
        let extensions = [".mkv", ".mp4", ".avi"];

        for share in &config.shares {
            let mut share_files = Vec::new();
            let share_root = PathBuf::from(&share.local_mount);
            directories.insert(share_root.clone());
            share_roots.insert(share.name.clone(), share_root);
            let num_files = fixed_files_per_share.unwrap_or_else(|| rng.gen_range(50..=200));

            for i in 0..num_files {
                let (name, subdir) = if share.name == "tv" {
                    let show = tv_shows[i % tv_shows.len()];
                    let season = rng.gen_range(1..=5);
                    let episode = rng.gen_range(1..=10);
                    let name = format!("{}.S{:02}E{:02}", show, season, episode);
                    let subdir = format!("{}/Season {:02}", show, season);
                    (name, subdir)
                } else {
                    let movie = if i < movie_names.len() {
                        movie_names[i].to_string()
                    } else {
                        format!("Movie.{}.{}", rng.gen_range(2000..=2025), i)
                    };
                    let subdir = movie.clone();
                    (movie, subdir)
                };

                let ext = extensions[rng.gen_range(0..extensions.len())];
                let codec = codecs[rng.gen_range(0..codecs.len())].to_string();
                let size = rng.gen_range(500_000_000u64..=30_000_000_000); // 500MB-30GB
                let bitrate = rng.gen_range(2.0..=50.0); // 2-50 Mbps
                let duration = if bitrate > 0.0 {
                    (size as f64 * 8.0) / (bitrate * 1_000_000.0)
                } else {
                    7200.0
                };
                let mtime = 1700000000.0 + rng.gen_range(0.0..=10000000.0);

                let path = PathBuf::from(&share.local_mount)
                    .join(&subdir)
                    .join(format!("{}{}", name, ext));
                if let Some(parent) = path.parent() {
                    directories.insert(parent.to_path_buf());
                }

                let audio_streams = rng.gen_range(1..=3);
                let subtitle_streams = rng.gen_range(0..=5);

                share_files.push(SimFile {
                    path,
                    size,
                    mtime,
                    codec,
                    bitrate_mbps: bitrate,
                    duration_secs: duration,
                    audio_stream_count: audio_streams,
                    subtitle_stream_count: subtitle_streams,
                });
            }

            files.insert(share.name.clone(), share_files);
        }

        Self {
            files: Mutex::new(files),
            temp_files: Mutex::new(HashMap::new()),
            share_roots,
            directories: Mutex::new(directories),
        }
    }

    fn find_file(&self, path: &Path) -> Option<SimFile> {
        // Check VFS files first
        let files = self.files.lock().unwrap();
        for share_files in files.values() {
            for f in share_files {
                if f.path == path {
                    return Some(f.clone());
                }
            }
        }
        drop(files);

        // Check temp files
        let temp = self.temp_files.lock().unwrap();
        temp.get(path).cloned()
    }

    fn find_share_for_path(&self, path: &Path) -> Option<String> {
        self.share_roots.iter().find_map(|(name, root)| {
            if path.starts_with(root) {
                Some(name.clone())
            } else {
                None
            }
        })
    }

    fn track_directory(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            self.directories
                .lock()
                .unwrap()
                .insert(parent.to_path_buf());
        }
    }

    fn remove_from_shares(&self, path: &Path) -> Option<SimFile> {
        let mut files = self.files.lock().unwrap();
        for share_files in files.values_mut() {
            if let Some(idx) = share_files.iter().position(|f| f.path == path) {
                return Some(share_files.remove(idx));
            }
        }
        None
    }

    fn insert_path(&self, mut file: SimFile, path: &Path) {
        file.path = path.to_path_buf();
        self.track_directory(path);
        if let Some(share) = self.find_share_for_path(path) {
            self.files
                .lock()
                .unwrap()
                .entry(share)
                .or_default()
                .push(file);
        } else {
            self.temp_files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), file);
        }
    }
}

impl FileSystem for SimulatedFileSystem {
    fn walk_share(
        &self,
        share_name: &str,
        _root: &Path,
        _extensions: &[String],
    ) -> Result<Vec<FileEntry>> {
        let files = self.files.lock().unwrap();
        let share_files = files.get(share_name).cloned().unwrap_or_default();

        Ok(share_files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size,
                mtime: f.mtime,
                share_name: share_name.to_string(),
            })
            .collect())
    }

    fn file_size(&self, path: &Path) -> Result<u64> {
        self.find_file(path).map(|f| f.size).ok_or_else(|| {
            crate::error::WatchdogError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "simulated file not found",
            ))
        })
    }

    fn file_mtime(&self, path: &Path) -> Result<f64> {
        self.find_file(path).map(|f| f.mtime).ok_or_else(|| {
            crate::error::WatchdogError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "simulated file not found",
            ))
        })
    }

    fn exists(&self, path: &Path) -> bool {
        self.find_file(path).is_some()
    }

    fn is_dir(&self, path: &Path) -> bool {
        if self.directories.lock().unwrap().contains(path) {
            return true;
        }
        if self.share_roots.values().any(|root| root == path) {
            return true;
        }
        self.find_file(path).is_some()
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let moved = self
            .remove_from_shares(from)
            .or_else(|| self.temp_files.lock().unwrap().remove(from));

        match moved {
            Some(mut file) => {
                file.mtime += 1.0;
                self.insert_path(file, to);
                Ok(())
            }
            None => Err(crate::error::WatchdogError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "simulated file not found",
            ))),
        }
    }

    fn remove(&self, path: &Path) -> Result<()> {
        if self.remove_from_shares(path).is_none() {
            self.temp_files.lock().unwrap().remove(path);
        }
        Ok(())
    }

    fn free_space(&self, _path: &Path) -> Result<u64> {
        Ok(500_000_000_000) // 500 GB
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.directories.lock().unwrap().insert(path.to_path_buf());
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let files = self.files.lock().unwrap();
        for share_files in files.values() {
            for file in share_files {
                if file.path.parent() == Some(path) {
                    out.push(file.path.clone());
                }
            }
        }
        drop(files);
        let temp_files = self.temp_files.lock().unwrap();
        for temp_path in temp_files.keys() {
            if temp_path.parent() == Some(path) {
                out.push(temp_path.clone());
            }
        }
        Ok(out)
    }

    fn walk_files_with_suffix(&self, root: &Path, suffix: &str) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let files = self.files.lock().unwrap();
        for share_files in files.values() {
            for file in share_files {
                if file.path.starts_with(root)
                    && file
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|name| name.ends_with(suffix))
                {
                    out.push(file.path.clone());
                }
            }
        }
        drop(files);
        let temp_files = self.temp_files.lock().unwrap();
        for temp_path in temp_files.keys() {
            if temp_path.starts_with(root)
                && temp_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.ends_with(suffix))
            {
                out.push(temp_path.clone());
            }
        }
        Ok(out)
    }
}

/// Simulated prober returning pre-generated metadata.
pub struct SimulatedProber {
    fs: std::sync::Arc<SimulatedFileSystem>,
}

impl SimulatedProber {
    pub fn new(fs: std::sync::Arc<SimulatedFileSystem>) -> Self {
        Self { fs }
    }
}

impl Prober for SimulatedProber {
    fn probe(&self, path: &Path) -> Result<Option<ProbeResult>> {
        let mut rng = rand::thread_rng();

        let file = match self.fs.find_file(path) {
            Some(f) => f,
            None => return Ok(None),
        };

        // 5% chance of probe failure, but only for VFS files (not temp files)
        let is_temp = self.fs.temp_files.lock().unwrap().contains_key(path);
        if !is_temp && rng.gen_ratio(1, 20) {
            return Ok(None);
        }

        let bitrate_bps = (file.bitrate_mbps * 1_000_000.0) as u64;

        Ok(Some(ProbeResult {
            video_codec: Some(file.codec.clone()),
            stream_bitrate_bps: bitrate_bps,
            format_bitrate_bps: bitrate_bps,
            size_bytes: file.size,
            duration_seconds: file.duration_secs,
            video_stream_count: 1,
            audio_stream_count: file.audio_stream_count,
            subtitle_stream_count: file.subtitle_stream_count,
            raw_json: serde_json::json!({}),
        }))
    }

    fn health_check(&self, _path: &Path) -> Result<bool> {
        Ok(true)
    }
}

/// Simulated transcoder that emits fake progress over a short period.
pub struct SimulatedTranscoder {
    fs: std::sync::Arc<SimulatedFileSystem>,
}

impl SimulatedTranscoder {
    pub fn new(fs: std::sync::Arc<SimulatedFileSystem>) -> Self {
        Self { fs }
    }
}

impl Transcoder for SimulatedTranscoder {
    fn transcode(
        &self,
        input: &Path,
        output: &Path,
        _preset_file: &Path,
        _preset_name: &str,
        _timeout_secs: u64,
        progress_tx: mpsc::Sender<TranscodeProgress>,
        cancel: Arc<AtomicBool>,
    ) -> Result<TranscodeResult> {
        let mut rng = rand::thread_rng();

        // 10% failure rate
        let will_fail = rng.gen_ratio(1, 10);
        // 5% timeout rate
        let will_timeout = rng.gen_ratio(1, 20);

        let duration_ms = rng.gen_range(3000..=15000u64); // 3-15 seconds
        let steps = 50;
        let step_duration = std::time::Duration::from_millis(duration_ms / steps);

        for i in 1..=steps {
            if cancel.load(Ordering::Relaxed) {
                return Err(crate::error::WatchdogError::TranscodeCancelled {
                    path: input.to_path_buf(),
                    reason: "cancel signal received".to_string(),
                });
            }

            let percent = (i as f64 / steps as f64) * 100.0;
            let fps = rng.gen_range(5.0..=30.0);
            let avg_fps = rng.gen_range(8.0..=25.0);
            let remaining_secs = ((steps - i) as f64 * step_duration.as_secs_f64()) as u64;
            let eta = format!("{}m{:02}s", remaining_secs / 60, remaining_secs % 60);

            let _ = progress_tx.try_send(TranscodeProgress {
                percent,
                fps,
                avg_fps,
                eta,
            });

            std::thread::sleep(step_duration);

            if will_timeout && percent > 80.0 {
                return Err(crate::error::WatchdogError::TranscodeTimeout {
                    path: input.to_path_buf(),
                    timeout_secs: _timeout_secs,
                });
            }
        }

        if will_fail {
            return Ok(TranscodeResult {
                success: false,
                timed_out: false,
                output_exists: false,
            });
        }

        // Simulate output file: 30-70% of original size, preserving duration
        let original = self.fs.find_file(input);
        let original_size = original.as_ref().map(|f| f.size).unwrap_or(1_000_000_000);
        let original_duration = original.as_ref().map(|f| f.duration_secs).unwrap_or(7200.0);
        let ratio = rng.gen_range(0.3..=0.7);
        let output_size = (original_size as f64 * ratio) as u64;
        let output_bitrate = if original_duration > 0.0 {
            (output_size as f64 * 8.0) / (original_duration * 1_000_000.0)
        } else {
            6.0
        };
        let audio_streams = original.as_ref().map(|f| f.audio_stream_count).unwrap_or(1);
        let subtitle_streams = original
            .as_ref()
            .map(|f| f.subtitle_stream_count)
            .unwrap_or(0);
        self.fs.temp_files.lock().unwrap().insert(
            output.to_path_buf(),
            SimFile {
                path: output.to_path_buf(),
                size: output_size,
                mtime: 0.0,
                codec: "av1".to_string(),
                bitrate_mbps: output_bitrate,
                duration_secs: original_duration,
                audio_stream_count: audio_streams,
                subtitle_stream_count: subtitle_streams,
            },
        );

        Ok(TranscodeResult {
            success: true,
            timed_out: false,
            output_exists: true,
        })
    }
}

/// Simulated file transfer (instant success).
pub struct SimulatedTransfer {
    fs: std::sync::Arc<SimulatedFileSystem>,
}

impl SimulatedTransfer {
    pub fn new(fs: std::sync::Arc<SimulatedFileSystem>) -> Self {
        Self { fs }
    }
}

impl FileTransfer for SimulatedTransfer {
    fn transfer(
        &self,
        source: &Path,
        dest: &Path,
        _timeout_secs: u64,
        stage: TransferStage,
        progress_tx: Option<mpsc::Sender<TransferProgress>>,
    ) -> Result<TransferResult> {
        // Copy the source's metadata to dest in temp_files
        let source_file = self.fs.find_file(source);
        let sim_file = match source_file {
            Some(f) => SimFile {
                path: dest.to_path_buf(),
                size: f.size,
                mtime: f.mtime,
                codec: f.codec,
                bitrate_mbps: f.bitrate_mbps,
                duration_secs: f.duration_secs,
                audio_stream_count: f.audio_stream_count,
                subtitle_stream_count: f.subtitle_stream_count,
            },
            None => SimFile {
                path: dest.to_path_buf(),
                size: 1_000_000_000,
                mtime: 0.0,
                codec: "h264".to_string(),
                bitrate_mbps: 10.0,
                duration_secs: 7200.0,
                audio_stream_count: 1,
                subtitle_stream_count: 0,
            },
        };
        self.fs
            .temp_files
            .lock()
            .unwrap()
            .insert(dest.to_path_buf(), sim_file);
        if let Some(tx) = &progress_tx {
            let _ = tx.try_send(TransferProgress {
                stage,
                percent: 100.0,
                rate_mib_per_sec: 0.0,
                eta: String::new(),
            });
        }
        // Brief delay to simulate transfer
        std::thread::sleep(std::time::Duration::from_millis(100));
        Ok(TransferResult { success: true })
    }
}

/// Simulated mount manager (always healthy).
pub struct SimulatedMountManager;

impl MountManager for SimulatedMountManager {
    fn is_healthy(&self, _mount_point: &Path) -> bool {
        true
    }

    fn remount(
        &self,
        _server: &str,
        _remote_path: &str,
        _local_mount: &Path,
        _share_name: &str,
    ) -> Result<bool> {
        Ok(true)
    }
}

/// Create all simulated dependencies for --simulate mode.
pub fn create_simulated_deps(config: &Config) -> PipelineDeps {
    let fs = std::sync::Arc::new(SimulatedFileSystem::new(config));

    PipelineDeps {
        fs: std::sync::Arc::new(SimulatedFileSystemWrapper(fs.clone())),
        prober: Box::new(SimulatedProber::new(fs.clone())),
        transcoder: Box::new(SimulatedTranscoder::new(fs.clone())),
        transfer: Box::new(SimulatedTransfer::new(fs.clone())),
        mount_manager: Box::new(SimulatedMountManager),
        in_use_detector: Box::new(SimulatedInUseDetector),
    }
}

/// Wrapper to make Arc<SimulatedFileSystem> implement FileSystem.
struct SimulatedFileSystemWrapper(std::sync::Arc<SimulatedFileSystem>);

impl FileSystem for SimulatedFileSystemWrapper {
    fn walk_share(
        &self,
        share_name: &str,
        root: &Path,
        extensions: &[String],
    ) -> Result<Vec<FileEntry>> {
        self.0.walk_share(share_name, root, extensions)
    }
    fn file_size(&self, path: &Path) -> Result<u64> {
        self.0.file_size(path)
    }
    fn file_mtime(&self, path: &Path) -> Result<f64> {
        self.0.file_mtime(path)
    }
    fn exists(&self, path: &Path) -> bool {
        self.0.exists(path)
    }
    fn is_dir(&self, path: &Path) -> bool {
        self.0.is_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.rename(from, to)
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.0.remove(path)
    }
    fn free_space(&self, path: &Path) -> Result<u64> {
        self.0.free_space(path)
    }
    fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.0.create_dir_all(path)
    }
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.0.list_dir(path)
    }
    fn walk_files_with_suffix(&self, root: &Path, suffix: &str) -> Result<Vec<PathBuf>> {
        self.0.walk_files_with_suffix(root, suffix)
    }
}
