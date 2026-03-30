use crate::config::{
    bundled_preset_dir, bundled_preset_search_roots, BUNDLED_PRESET_DIR_NAME,
    HIDDEN_BUNDLED_PRESET_DIR_PATH,
};
use crate::error::{Result, WatchdogError};
use crate::process::{
    configure_subprocess_group, describe_exit_status, format_command_for_log, infer_failure_hint,
    register_subprocess, run_command, summarize_output_tail, terminate_subprocess, RunOptions,
};
use crate::traits::{TranscodeProgress, TranscodeResult, Transcoder};
use regex::Regex;
use serde_json::Value;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

// Strip ANSI escape sequences
static ANSI_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]").unwrap());

// HandBrake text progress:
// "Encoding: task 1 of 1, 12.34 % (5.67 fps, avg 4.56 fps, ETA 01h23m45s)"
static HB_PROGRESS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"Encoding:.*?(\d+(?:\.\d+)?)\s*%(?:\s*\((\d+(?:\.\d+)?)\s*fps,\s*avg\s*(\d+(?:\.\d+)?)\s*fps,\s*ETA\s*(\S+)\))?",
    )
    .unwrap()
});

static HB_JSON_SUPPORTED: LazyLock<bool> = LazyLock::new(|| {
    let mut cmd = Command::new("HandBrakeCLI");
    cmd.arg("--help");
    let out = match run_command(
        cmd,
        RunOptions {
            timeout: Some(Duration::from_secs(3)),
            stdout_limit: 256 * 1024,
            stderr_limit: 64 * 1024,
            ..RunOptions::default()
        },
    ) {
        Ok(out) => out,
        Err(_) => return false,
    };

    let text = if !out.stdout.is_empty() {
        String::from_utf8_lossy(&out.stdout).to_string()
    } else {
        String::from_utf8_lossy(&out.stderr_tail).to_string()
    };
    text.contains("--json")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresetSnapshot {
    pub preset_file: String,
    pub preset_name: String,
    pub target_codec: String,
}

impl PresetSnapshot {
    pub fn normalized(
        base_dir: &Path,
        preset_file: &str,
        preset_name: &str,
        target_codec: &str,
    ) -> Self {
        Self {
            preset_file: path_for_storage(base_dir, &resolve_preset_path(base_dir, preset_file)),
            preset_name: preset_name.to_string(),
            target_codec: target_codec.trim().to_ascii_lowercase(),
        }
    }

    pub fn resolve_path(&self, base_dir: &Path) -> PathBuf {
        resolve_preset_path(base_dir, &self.preset_file)
    }

    pub fn short_label(&self) -> String {
        let file_name = Path::new(&self.preset_file)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&self.preset_file);
        format!("{} :: {}", file_name, self.preset_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresetCatalogEntry {
    pub preset_file: String,
    pub preset_name: String,
    pub selectable: bool,
    pub target_codec: Option<String>,
    pub encoder: String,
    pub quality_summary: String,
    pub speed_preset: String,
    pub subtitle_summary: String,
    pub audio_summary: String,
    pub error_summary: Option<String>,
}

impl PresetCatalogEntry {
    pub fn label(&self) -> String {
        let file_name = Path::new(&self.preset_file)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&self.preset_file);
        format!("{} :: {}", file_name, self.preset_name)
    }

    pub fn snapshot(&self) -> Option<PresetSnapshot> {
        Some(PresetSnapshot {
            preset_file: self.preset_file.clone(),
            preset_name: self.preset_name.clone(),
            target_codec: self.target_codec.clone()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PresetMetadata {
    target_codec: String,
    container_extension: String,
    accepted_format_names: Vec<String>,
    encoder: String,
    quality_summary: String,
    speed_preset: String,
    subtitle_summary: String,
    audio_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresetContract {
    pub target_codec: String,
    pub container_extension: String,
    accepted_format_names: Vec<String>,
}

impl PresetContract {
    pub fn resolve(preset_file: &Path, preset_name: &str) -> Result<Self> {
        let metadata = load_preset_metadata(preset_file, preset_name)?;
        Ok(Self {
            target_codec: metadata.target_codec,
            container_extension: metadata.container_extension,
            accepted_format_names: metadata.accepted_format_names,
        })
    }

    pub fn from_payload_text(
        preset_payload_text: &str,
        preset_name: &str,
        source_label: &str,
    ) -> Result<Self> {
        let metadata =
            load_preset_metadata_from_text(preset_payload_text, preset_name, source_label)?;
        Ok(Self {
            target_codec: metadata.target_codec,
            container_extension: metadata.container_extension,
            accepted_format_names: metadata.accepted_format_names,
        })
    }

    pub fn ensure_target_codec(&self, configured_target_codec: &str) -> Result<()> {
        let configured_codec = configured_target_codec.trim().to_ascii_lowercase();
        if self.target_codec != configured_codec {
            return Err(WatchdogError::Config(format!(
                "preset outputs codec '{}' but transcode.target_codec is '{}'",
                self.target_codec, configured_codec
            )));
        }
        Ok(())
    }

    pub fn output_path_for(&self, source_path: &Path) -> PathBuf {
        if source_path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case(&self.container_extension))
        {
            return source_path.to_path_buf();
        }

        let parent = source_path.parent().unwrap_or(Path::new("."));
        let stem = source_path
            .file_stem()
            .or_else(|| source_path.file_name())
            .unwrap_or_default()
            .to_string_lossy();
        parent.join(format!("{}.{}", stem, self.container_extension))
    }

    pub fn container_matches(&self, format_names: &[String]) -> bool {
        format_names.iter().any(|name| {
            self.accepted_format_names
                .iter()
                .any(|expected| name.eq_ignore_ascii_case(expected))
        })
    }
}

pub fn resolve_preset_path(base_dir: &Path, preset_file: &str) -> PathBuf {
    resolve_preset_path_from_roots(
        base_dir,
        preset_file,
        &bundled_preset_search_roots(base_dir),
    )
}

fn resolve_preset_path_from_roots(
    base_dir: &Path,
    preset_file: &str,
    search_roots: &[PathBuf],
) -> PathBuf {
    let configured_path = Path::new(preset_file);
    let direct_path = if configured_path.is_absolute() {
        configured_path.to_path_buf()
    } else {
        base_dir.join(configured_path)
    };
    if direct_path.exists() {
        return direct_path;
    }

    if let Some(alternate) = resolve_preset_path_in_search_roots(configured_path, search_roots) {
        return alternate;
    }

    direct_path
}

fn resolve_preset_path_in_search_roots(
    configured_path: &Path,
    search_roots: &[PathBuf],
) -> Option<PathBuf> {
    if configured_path.is_absolute() {
        return None;
    }

    if let Ok(suffix) = configured_path.strip_prefix(BUNDLED_PRESET_DIR_NAME) {
        return find_existing_preset_path(search_roots, suffix);
    }
    if let Ok(suffix) = configured_path.strip_prefix(HIDDEN_BUNDLED_PRESET_DIR_PATH) {
        return find_existing_preset_path(search_roots, suffix);
    }

    let is_bare_filename = configured_path
        .parent()
        .is_none_or(|parent| parent.as_os_str().is_empty() || parent == Path::new("."));
    if is_bare_filename {
        let suffix = configured_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new(""));
        return find_existing_preset_path(search_roots, Path::new(suffix));
    }

    None
}

fn find_existing_preset_path(search_roots: &[PathBuf], suffix: &Path) -> Option<PathBuf> {
    search_roots
        .iter()
        .map(|root| root.join(suffix))
        .find(|candidate| candidate.exists())
}

pub fn load_preset_catalog(base_dir: &Path) -> Vec<PresetCatalogEntry> {
    let presets_dir = bundled_preset_dir(base_dir);
    let mut files = match fs::read_dir(&presets_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            })
            .collect::<Vec<_>>(),
        Err(_) => return Vec::new(),
    };
    files.sort();

    let mut catalog = Vec::new();
    for file_path in files {
        let stored_path = path_for_storage(base_dir, &file_path);
        match load_preset_payload(&file_path) {
            Ok(payload) => {
                let Some(presets) = payload.get("PresetList").and_then(Value::as_array) else {
                    catalog.push(invalid_catalog_entry(
                        &stored_path,
                        "(invalid file)",
                        "PresetList is missing or invalid".to_string(),
                    ));
                    continue;
                };

                if presets.is_empty() {
                    catalog.push(invalid_catalog_entry(
                        &stored_path,
                        "(empty file)",
                        "PresetList is empty".to_string(),
                    ));
                    continue;
                }

                for (idx, preset) in presets.iter().enumerate() {
                    let preset_name = preset
                        .get("PresetName")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .unwrap_or_else(|| format!("(unnamed preset #{})", idx + 1));
                    match parse_preset_metadata(preset, &file_path, &preset_name) {
                        Ok(metadata) => catalog.push(PresetCatalogEntry {
                            preset_file: stored_path.clone(),
                            preset_name,
                            selectable: true,
                            target_codec: Some(metadata.target_codec),
                            encoder: metadata.encoder,
                            quality_summary: metadata.quality_summary,
                            speed_preset: metadata.speed_preset,
                            subtitle_summary: metadata.subtitle_summary,
                            audio_summary: metadata.audio_summary,
                            error_summary: None,
                        }),
                        Err(err) => catalog.push(invalid_catalog_entry(
                            &stored_path,
                            &preset_name,
                            config_error_message(&err),
                        )),
                    }
                }
            }
            Err(err) => catalog.push(invalid_catalog_entry(
                &stored_path,
                "(invalid file)",
                config_error_message(&err),
            )),
        }
    }

    catalog
}

fn invalid_catalog_entry(
    preset_file: &str,
    preset_name: &str,
    error_summary: String,
) -> PresetCatalogEntry {
    PresetCatalogEntry {
        preset_file: preset_file.to_string(),
        preset_name: preset_name.to_string(),
        selectable: false,
        target_codec: None,
        encoder: "-".to_string(),
        quality_summary: "-".to_string(),
        speed_preset: "-".to_string(),
        subtitle_summary: "-".to_string(),
        audio_summary: "-".to_string(),
        error_summary: Some(error_summary),
    }
}

fn path_for_storage(base_dir: &Path, path: &Path) -> String {
    path.strip_prefix(base_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

pub fn load_preset_payload_text(preset_file: &Path) -> Result<String> {
    fs::read_to_string(preset_file).map_err(|e| {
        WatchdogError::Config(format!(
            "failed to read preset file {}: {}",
            preset_file.display(),
            e
        ))
    })
}

pub fn write_preset_payload_snapshot(
    snapshot_path: &Path,
    preset_payload_text: &str,
) -> Result<()> {
    if let Some(parent) = snapshot_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            WatchdogError::Config(format!(
                "failed to create preset snapshot directory {}: {}",
                parent.display(),
                e
            ))
        })?;
    }
    fs::write(snapshot_path, preset_payload_text).map_err(|e| {
        WatchdogError::Config(format!(
            "failed to write preset snapshot {}: {}",
            snapshot_path.display(),
            e
        ))
    })
}

fn parse_preset_payload_text(preset_payload_text: &str, source_label: &str) -> Result<Value> {
    serde_json::from_str(preset_payload_text).map_err(|e| {
        WatchdogError::Config(format!(
            "failed to parse preset file {} as JSON: {}",
            source_label, e
        ))
    })
}

fn load_preset_payload(preset_file: &Path) -> Result<Value> {
    let payload_text = load_preset_payload_text(preset_file)?;
    parse_preset_payload_text(&payload_text, &preset_file.display().to_string())
}

fn load_preset_metadata(preset_file: &Path, preset_name: &str) -> Result<PresetMetadata> {
    let payload = load_preset_payload(preset_file)?;
    load_preset_metadata_from_payload(&payload, preset_name, &preset_file.display().to_string())
}

fn load_preset_metadata_from_text(
    preset_payload_text: &str,
    preset_name: &str,
    source_label: &str,
) -> Result<PresetMetadata> {
    let payload = parse_preset_payload_text(preset_payload_text, source_label)?;
    load_preset_metadata_from_payload(&payload, preset_name, source_label)
}

fn load_preset_metadata_from_payload(
    payload: &Value,
    preset_name: &str,
    source_label: &str,
) -> Result<PresetMetadata> {
    let preset = payload
        .get("PresetList")
        .and_then(Value::as_array)
        .and_then(|presets| {
            presets.iter().find(|preset| {
                preset.get("PresetName").and_then(Value::as_str) == Some(preset_name)
            })
        })
        .ok_or_else(|| {
            WatchdogError::Config(format!(
                "preset '{}' was not found in {}",
                preset_name, source_label
            ))
        })?;
    parse_preset_metadata(preset, Path::new(source_label), preset_name)
}

fn parse_preset_metadata(
    preset: &Value,
    preset_file: &Path,
    preset_name: &str,
) -> Result<PresetMetadata> {
    let video_encoder = preset
        .get("VideoEncoder")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            WatchdogError::Config(format!(
                "preset '{}' in {} is missing VideoEncoder",
                preset_name,
                preset_file.display()
            ))
        })?;
    let file_format = preset
        .get("FileFormat")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            WatchdogError::Config(format!(
                "preset '{}' in {} is missing FileFormat",
                preset_name,
                preset_file.display()
            ))
        })?;

    let resolved_codec = map_video_encoder_to_codec(video_encoder).ok_or_else(|| {
        WatchdogError::Config(format!(
            "preset '{}' in {} uses unsupported VideoEncoder '{}'",
            preset_name,
            preset_file.display(),
            video_encoder
        ))
    })?;
    let (container_extension, accepted_format_names) = map_file_format_to_container(file_format)
        .ok_or_else(|| {
            WatchdogError::Config(format!(
                "preset '{}' in {} uses unsupported FileFormat '{}'",
                preset_name,
                preset_file.display(),
                file_format
            ))
        })?;

    Ok(PresetMetadata {
        target_codec: resolved_codec.to_string(),
        container_extension: container_extension.to_string(),
        accepted_format_names: accepted_format_names
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        encoder: video_encoder.to_string(),
        quality_summary: build_quality_summary(preset),
        speed_preset: preset
            .get("VideoPreset")
            .and_then(Value::as_str)
            .unwrap_or("auto")
            .to_string(),
        subtitle_summary: build_subtitle_summary(preset),
        audio_summary: build_audio_summary(preset),
    })
}

fn build_quality_summary(preset: &Value) -> String {
    let quality_type = preset
        .get("VideoQualityType")
        .and_then(Value::as_i64)
        .unwrap_or(2);
    if quality_type == 2 {
        preset
            .get("VideoQualitySlider")
            .and_then(value_as_f64)
            .map(|value| format!("CQ {}", format_decimal(value)))
            .unwrap_or_else(|| "CQ auto".to_string())
    } else {
        preset
            .get("VideoAvgBitrate")
            .and_then(Value::as_i64)
            .map(|bitrate| format!("ABR {} kbps", bitrate))
            .unwrap_or_else(|| "ABR auto".to_string())
    }
}

fn build_subtitle_summary(preset: &Value) -> String {
    let behavior = preset
        .get("SubtitleTrackSelectionBehavior")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let languages = value_as_string_list(preset.get("SubtitleLanguageList"));
    let burn = preset
        .get("SubtitleBurnBehavior")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let foreign_search = yes_no(
        preset
            .get("SubtitleAddForeignAudioSearch")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    let foreign_subtitle = yes_no(
        preset
            .get("SubtitleAddForeignAudioSubtitle")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    format!(
        "{} [{}], burn={}, foreign-search={}, foreign-sub={}",
        behavior,
        join_or_any(&languages),
        burn,
        foreign_search,
        foreign_subtitle
    )
}

fn build_audio_summary(preset: &Value) -> String {
    let selection = preset
        .get("AudioTrackSelectionBehavior")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let fallback = preset
        .get("AudioEncoderFallback")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let copy_mask = value_as_string_list(preset.get("AudioCopyMask"));
    let primary = preset
        .get("AudioList")
        .and_then(Value::as_array)
        .and_then(|audio_list| audio_list.first())
        .map(|track| {
            let encoder = track
                .get("AudioEncoder")
                .and_then(Value::as_str)
                .unwrap_or("auto");
            let bitrate = track
                .get("AudioBitrate")
                .and_then(Value::as_i64)
                .map(|bitrate| format!("{} kbps", bitrate))
                .unwrap_or_else(|| "auto".to_string());
            let mixdown = track
                .get("AudioMixdown")
                .and_then(Value::as_str)
                .unwrap_or("auto");
            format!("{}/{}/{}", encoder, bitrate, mixdown)
        })
        .unwrap_or_else(|| "auto".to_string());
    format!(
        "{} [{}], primary={}, fallback={}",
        selection,
        join_or_any(&copy_mask),
        primary,
        fallback
    )
}

fn value_as_string_list(value: Option<&Value>) -> Vec<String> {
    value
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

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_u64().map(|value| value as f64))
}

fn format_decimal(value: f64) -> String {
    if (value.fract() - 0.0).abs() < f64::EPSILON {
        format!("{}", value as i64)
    } else {
        format!("{:.1}", value)
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn join_or_any(values: &[String]) -> String {
    if values.is_empty() {
        "any".to_string()
    } else {
        values.join(",")
    }
}

fn config_error_message(err: &WatchdogError) -> String {
    match err {
        WatchdogError::Config(message) => message.clone(),
        _ => err.to_string(),
    }
}

fn map_video_encoder_to_codec(encoder: &str) -> Option<&'static str> {
    let normalized = encoder.trim().to_ascii_lowercase();
    if normalized.contains("av1") {
        Some("av1")
    } else if normalized.contains("265") || normalized.contains("hevc") {
        Some("hevc")
    } else if normalized.contains("264") || normalized.contains("avc") {
        Some("h264")
    } else if normalized.contains("mpeg2") {
        Some("mpeg2video")
    } else if normalized.contains("vp9") {
        Some("vp9")
    } else {
        None
    }
}

fn map_file_format_to_container(
    file_format: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    match file_format.trim().to_ascii_lowercase().as_str() {
        "av_mkv" => Some(("mkv", &["matroska", "webm"])),
        "av_mp4" | "av_mov" => Some(("mp4", &["mov", "mp4", "m4a", "3gp", "3g2", "mj2"])),
        "av_webm" => Some(("webm", &["matroska", "webm"])),
        "av_avi" => Some(("avi", &["avi"])),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct CapturedOutput {
    bytes: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
    stream_error: Option<String>,
    last_progress: Option<TranscodeProgress>,
}

#[derive(Debug)]
struct ProgressActivity {
    last_percent: f64,
    last_forward_progress_at: Option<Instant>,
    last_stream_activity_at: Option<Instant>,
}

impl Default for ProgressActivity {
    fn default() -> Self {
        Self {
            last_percent: -1.0,
            last_forward_progress_at: None,
            last_stream_activity_at: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct LivenessSnapshot {
    last_percent: f64,
    progress_idle_secs: f64,
    output_idle_secs: f64,
    stream_idle_secs: f64,
}

fn push_tail(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    if limit == 0 {
        return false;
    }
    buf.extend_from_slice(chunk);
    if buf.len() > limit {
        let drop_len = buf.len() - limit;
        buf.drain(0..drop_len);
        return true;
    }
    false
}

fn parse_handbrake_progress_text(line: &str) -> Option<TranscodeProgress> {
    let clean = ANSI_RE.replace_all(line, "");
    let caps = HB_PROGRESS_RE.captures(&clean)?;

    let percent: f64 = caps.get(1)?.as_str().parse().ok()?;
    let fps: f64 = caps
        .get(2)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0.0);
    let avg_fps: f64 = caps
        .get(3)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0.0);
    let eta = caps
        .get(4)
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();

    Some(TranscodeProgress {
        percent,
        fps,
        avg_fps,
        eta,
    })
}

fn format_eta_hms(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{:02}h{:02}m{:02}s", hours, minutes, seconds)
}

fn parse_handbrake_progress_json_block(block: &str) -> Option<TranscodeProgress> {
    let payload: Value = serde_json::from_str(block).ok()?;
    let state = payload
        .get("State")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if state.eq_ignore_ascii_case("WORKDONE") {
        return Some(TranscodeProgress {
            percent: 100.0,
            fps: 0.0,
            avg_fps: 0.0,
            eta: String::new(),
        });
    }

    if !state.eq_ignore_ascii_case("WORKING") {
        return None;
    }

    let working = payload.get("Working")?;
    let percent = working
        .get("Progress")
        .and_then(Value::as_f64)
        .map(|v| (v * 100.0).clamp(0.0, 100.0))
        .unwrap_or(0.0);
    let fps = working.get("Rate").and_then(Value::as_f64).unwrap_or(0.0);
    let avg_fps = working
        .get("RateAvg")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let eta = working
        .get("ETASeconds")
        .and_then(Value::as_i64)
        .and_then(|secs| (secs > 0).then_some(format_eta_hms(secs as u64)))
        .or_else(|| {
            let h = working.get("Hours").and_then(Value::as_i64)?;
            let m = working.get("Minutes").and_then(Value::as_i64)?;
            let s = working.get("Seconds").and_then(Value::as_i64)?;
            if h >= 0 && m >= 0 && s >= 0 {
                Some(format!("{:02}h{:02}m{:02}s", h, m, s))
            } else {
                None
            }
        })
        .unwrap_or_default();

    Some(TranscodeProgress {
        percent,
        fps,
        avg_fps,
        eta,
    })
}

fn summarize_progress(progress: Option<&TranscodeProgress>) -> String {
    match progress {
        Some(p) => format!(
            "{:.2}% (fps={:.2}, avg_fps={:.2}, eta={})",
            p.percent,
            p.fps,
            p.avg_fps,
            if p.eta.is_empty() { "-" } else { &p.eta }
        ),
        None => "no progress parsed".to_string(),
    }
}

fn brace_delta(text: &str) -> i32 {
    let opens = text.bytes().filter(|b| *b == b'{').count() as i32;
    let closes = text.bytes().filter(|b| *b == b'}').count() as i32;
    opens - closes
}

fn idle_secs_since(start: Instant, last_activity: Option<Instant>, now: Instant) -> f64 {
    let anchor = last_activity.unwrap_or(start);
    now.duration_since(anchor).as_secs_f64()
}

fn capture_liveness_snapshot(
    start: Instant,
    now: Instant,
    last_forward_progress_at: Option<Instant>,
    last_output_growth_at: Instant,
    last_stream_activity_at: Option<Instant>,
    last_percent: f64,
) -> LivenessSnapshot {
    LivenessSnapshot {
        last_percent,
        progress_idle_secs: idle_secs_since(start, last_forward_progress_at, now),
        output_idle_secs: now.duration_since(last_output_growth_at).as_secs_f64(),
        stream_idle_secs: idle_secs_since(start, last_stream_activity_at, now),
    }
}

fn spawn_progress_thread<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
    stream_name: &'static str,
    progress_tx: mpsc::Sender<TranscodeProgress>,
    activity: Arc<Mutex<ProgressActivity>>,
) -> thread::JoinHandle<CapturedOutput> {
    thread::spawn(move || {
        let mut out = CapturedOutput::default();
        let mut tmp = [0u8; 4096];
        let mut line_buf = Vec::new();
        let mut json_buf: Option<String> = None;
        let mut json_depth = 0i32;
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        let mut last_percent = -1.0f64;

        let mut emit_progress = |progress: TranscodeProgress, out: &mut CapturedOutput| {
            let now = Instant::now();
            let should_emit = progress.percent >= 100.0
                || (progress.percent - last_percent).abs() >= 0.2
                || now.duration_since(last_emit) >= Duration::from_millis(250);
            out.last_progress = Some(progress.clone());
            if let Ok(mut shared) = activity.lock() {
                if progress.percent > shared.last_percent {
                    shared.last_percent = progress.percent;
                    shared.last_forward_progress_at = Some(now);
                }
            }
            if should_emit {
                let _ = progress_tx.try_send(progress.clone());
                last_emit = now;
                last_percent = progress.percent;
            }
        };

        let mut process_line = |raw: &[u8], out: &mut CapturedOutput| {
            if raw.is_empty() {
                return;
            }
            out.truncated |= push_tail(&mut out.bytes, raw, limit);
            out.truncated |= push_tail(&mut out.bytes, b"\n", limit);

            let raw_line = String::from_utf8_lossy(raw);
            let clean = ANSI_RE.replace_all(&raw_line, "").to_string();

            if let Some(ref mut json_text) = json_buf {
                json_text.push_str(&clean);
                json_text.push('\n');
                json_depth += brace_delta(&clean);
                if json_depth <= 0 {
                    if let Some(progress) = parse_handbrake_progress_json_block(json_text) {
                        emit_progress(progress, out);
                    }
                    json_buf = None;
                }
                return;
            }

            if let Some(idx) = clean.find("Progress:") {
                if let Some(brace_idx) = clean[idx..].find('{') {
                    let block_start = idx + brace_idx;
                    let first = &clean[block_start..];
                    json_depth = brace_delta(first);
                    json_buf = Some(format!("{}\n", first));
                    if json_depth <= 0 {
                        if let Some(block) = json_buf.take() {
                            if let Some(progress) = parse_handbrake_progress_json_block(&block) {
                                emit_progress(progress, out);
                            }
                        }
                    }
                    return;
                }
            }

            if let Some(progress) = parse_handbrake_progress_text(&clean) {
                emit_progress(progress, out);
            }
        };

        loop {
            match reader.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    if let Ok(mut shared) = activity.lock() {
                        shared.last_stream_activity_at = Some(Instant::now());
                    }
                    out.total_bytes = out.total_bytes.saturating_add(n);
                    for &byte in &tmp[..n] {
                        if byte == b'\r' || byte == b'\n' {
                            process_line(&line_buf, &mut out);
                            line_buf.clear();
                        } else {
                            line_buf.push(byte);
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    out.stream_error = Some(format!("{} read error: {}", stream_name, e));
                    break;
                }
            }
        }

        if !line_buf.is_empty() {
            process_line(&line_buf, &mut out);
        }

        out
    })
}

/// Real HandBrakeCLI transcoder with stdout progress parsing.
pub struct HandBrakeTranscoder;

impl Transcoder for HandBrakeTranscoder {
    fn transcode(
        &self,
        input: &Path,
        output: &Path,
        preset_file: &Path,
        preset_name: &str,
        timeout_secs: u64,
        stall_timeout_secs: u64,
        progress_tx: mpsc::Sender<TranscodeProgress>,
        cancel: Arc<AtomicBool>,
    ) -> Result<TranscodeResult> {
        let use_json = *HB_JSON_SUPPORTED;

        let mut cmd = Command::new("HandBrakeCLI");
        configure_subprocess_group(&mut cmd);
        if use_json {
            cmd.arg("--json");
        }
        cmd.arg("--preset-import-file")
            .arg(preset_file)
            .arg("-i")
            .arg(input)
            .arg("-o")
            .arg(output)
            .arg("--preset")
            .arg(preset_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let command_repr = format_command_for_log(&cmd);
        info!(
            "Starting HandBrake transcode: input={} output={} preset={} timeout={}s stall_timeout={}s json_progress={}",
            input.display(),
            output.display(),
            preset_name,
            timeout_secs,
            stall_timeout_secs.max(1),
            use_json
        );

        let mut child = cmd.spawn().map_err(|e| WatchdogError::Transcode {
            path: input.to_path_buf(),
            reason: format!(
                "Failed to spawn HandBrakeCLI command `{}`: {}",
                command_repr, e
            ),
        })?;
        let _child_registration = register_subprocess(child.id());

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| WatchdogError::Transcode {
                path: input.to_path_buf(),
                reason: "Failed to capture HandBrakeCLI stdout".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| WatchdogError::Transcode {
                path: input.to_path_buf(),
                reason: "Failed to capture HandBrakeCLI stderr".to_string(),
            })?;

        const STDOUT_LIMIT: usize = 128 * 1024;
        const STDERR_LIMIT: usize = 128 * 1024;

        let progress_activity = Arc::new(Mutex::new(ProgressActivity::default()));
        let stderr_progress_tx = progress_tx.clone();
        let stdout_handle = spawn_progress_thread(
            stdout,
            STDOUT_LIMIT,
            "stdout",
            progress_tx,
            Arc::clone(&progress_activity),
        );
        let stderr_handle = spawn_progress_thread(
            stderr,
            STDERR_LIMIT,
            "stderr",
            stderr_progress_tx,
            Arc::clone(&progress_activity),
        );

        let start = Instant::now();
        let stall_timeout = Duration::from_secs(stall_timeout_secs.max(1));
        let mut last_output_growth_at = start;
        let mut last_output_size = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
        let mut timed_out = false;
        let mut stalled = false;
        let mut cancelled = false;
        let mut timeout_liveness: Option<LivenessSnapshot> = None;
        let mut stalled_liveness: Option<LivenessSnapshot> = None;

        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(e) => {
                    return Err(WatchdogError::Transcode {
                        path: input.to_path_buf(),
                        reason: format!("Failed while waiting for HandBrakeCLI: {}", e),
                    });
                }
            }

            if cancel.load(Ordering::Relaxed) {
                cancelled = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for cancelled HandBrakeCLI process: {}", e),
                })?;
            }

            let now = Instant::now();
            let output_size = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
            if output_size > last_output_size {
                last_output_size = output_size;
                last_output_growth_at = now;
            }

            let (last_forward_progress_at, last_stream_activity_at, last_percent) =
                progress_activity
                    .lock()
                    .ok()
                    .map(|shared| {
                        (
                            shared.last_forward_progress_at,
                            shared.last_stream_activity_at,
                            shared.last_percent,
                        )
                    })
                    .unwrap_or((None, None, -1.0));
            let liveness = capture_liveness_snapshot(
                start,
                now,
                last_forward_progress_at,
                last_output_growth_at,
                last_stream_activity_at,
                last_percent,
            );
            if timeout_secs > 0 && start.elapsed() >= Duration::from_secs(timeout_secs) {
                timed_out = true;
                timeout_liveness = Some(liveness);
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for timed out HandBrakeCLI process: {}", e),
                })?;
            }

            if should_mark_transcode_stalled(
                start,
                last_forward_progress_at,
                last_output_growth_at,
                last_stream_activity_at,
                now,
                stall_timeout,
            ) {
                stalled = true;
                stalled_liveness = Some(liveness);
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for stalled HandBrakeCLI process: {}", e),
                })?;
            }

            thread::sleep(Duration::from_millis(100));
        };

        let stdout_capture = stdout_handle.join().unwrap_or_default();
        let stderr_capture = stderr_handle.join().unwrap_or_default();
        let elapsed = start.elapsed().as_secs_f64();
        let last_progress = stdout_capture
            .last_progress
            .as_ref()
            .or(stderr_capture.last_progress.as_ref());

        if cancelled {
            warn!(
                "HandBrake transcode cancelled: input={} elapsed={:.1}s progress={} command={}",
                input.display(),
                elapsed,
                summarize_progress(last_progress),
                command_repr
            );
            return Err(WatchdogError::TranscodeCancelled {
                path: input.to_path_buf(),
                reason: "cancel signal received".to_string(),
            });
        }

        if timed_out {
            let stderr_summary = summarize_output_tail(&stderr_capture.bytes, 420);
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            let liveness = timeout_liveness.unwrap_or_else(|| {
                let now = Instant::now();
                let (last_forward_progress_at, last_stream_activity_at, last_percent) =
                    progress_activity
                        .lock()
                        .ok()
                        .map(|shared| {
                            (
                                shared.last_forward_progress_at,
                                shared.last_stream_activity_at,
                                shared.last_percent,
                            )
                        })
                        .unwrap_or((None, None, -1.0));
                capture_liveness_snapshot(
                    start,
                    now,
                    last_forward_progress_at,
                    last_output_growth_at,
                    last_stream_activity_at,
                    last_percent,
                )
            });
            warn!(
                "HandBrake timed out: input={} timeout={}s elapsed={:.1}s progress={} last_percent={:.2}% progress_idle={:.1}s output_idle={:.1}s stream_idle={:.1}s stdout_bytes={} stderr_bytes={} json_progress={} stderr={}{}{} command={}",
                input.display(),
                timeout_secs,
                elapsed,
                summarize_progress(last_progress),
                liveness.last_percent,
                liveness.progress_idle_secs,
                liveness.output_idle_secs,
                liveness.stream_idle_secs,
                stdout_capture.total_bytes,
                stderr_capture.total_bytes,
                use_json,
                stderr_summary,
                if stderr_capture.truncated {
                    " [stderr tail truncated]"
                } else {
                    ""
                },
                hint_suffix,
                command_repr
            );
            return Err(WatchdogError::TranscodeTimeout {
                path: input.to_path_buf(),
                timeout_secs,
            });
        }

        if stalled {
            let stderr_summary = summarize_output_tail(&stderr_capture.bytes, 420);
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            let liveness = stalled_liveness.unwrap_or_else(|| {
                let now = Instant::now();
                let (last_forward_progress_at, last_stream_activity_at, last_percent) =
                    progress_activity
                        .lock()
                        .ok()
                        .map(|shared| {
                            (
                                shared.last_forward_progress_at,
                                shared.last_stream_activity_at,
                                shared.last_percent,
                            )
                        })
                        .unwrap_or((None, None, -1.0));
                capture_liveness_snapshot(
                    start,
                    now,
                    last_forward_progress_at,
                    last_output_growth_at,
                    last_stream_activity_at,
                    last_percent,
                )
            });
            warn!(
                "HandBrake stalled: input={} stall_timeout={}s elapsed={:.1}s output_bytes={} progress={} last_percent={:.2}% progress_idle={:.1}s output_idle={:.1}s stream_idle={:.1}s stdout_bytes={} stderr_bytes={} json_progress={} stderr={}{}{} command={}",
                input.display(),
                stall_timeout_secs.max(1),
                elapsed,
                last_output_size,
                summarize_progress(last_progress),
                liveness.last_percent,
                liveness.progress_idle_secs,
                liveness.output_idle_secs,
                liveness.stream_idle_secs,
                stdout_capture.total_bytes,
                stderr_capture.total_bytes,
                use_json,
                stderr_summary,
                if stderr_capture.truncated {
                    " [stderr tail truncated]"
                } else {
                    ""
                },
                hint_suffix,
                command_repr
            );
            return Err(WatchdogError::TranscodeStalled {
                path: input.to_path_buf(),
                stall_timeout_secs: stall_timeout_secs.max(1),
            });
        }

        if let Some(err) = stdout_capture.stream_error.as_deref() {
            warn!(
                "HandBrake stdout stream issue for {}: {} (elapsed={:.1}s, status={})",
                input.display(),
                err,
                elapsed,
                describe_exit_status(&status)
            );
        }
        if let Some(err) = stderr_capture.stream_error.as_deref() {
            warn!(
                "HandBrake stderr stream issue for {}: {} (elapsed={:.1}s, status={})",
                input.display(),
                err,
                elapsed,
                describe_exit_status(&status)
            );
        }

        if !status.success() {
            let stderr_summary = summarize_output_tail(&stderr_capture.bytes, 480);
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            warn!(
                "HandBrakeCLI failed: input={} output={} status={} elapsed={:.1}s progress={} stderr={}{}{}",
                input.display(),
                output.display(),
                describe_exit_status(&status),
                elapsed,
                summarize_progress(last_progress),
                stderr_summary,
                if stderr_capture.truncated {
                    " [stderr tail truncated]"
                } else {
                    ""
                },
                hint_suffix
            );
        }

        Ok(TranscodeResult {
            success: status.success(),
            timed_out: false,
            output_exists: output.exists(),
        })
    }
}

fn should_mark_transcode_stalled(
    start: Instant,
    last_forward_progress_at: Option<Instant>,
    last_output_growth_at: Instant,
    last_stream_activity_at: Option<Instant>,
    now: Instant,
    stall_timeout: Duration,
) -> bool {
    let mut last_activity = start;
    if let Some(progress_at) = last_forward_progress_at {
        last_activity = last_activity.max(progress_at);
    }
    last_activity = last_activity.max(last_output_growth_at);
    if let Some(stream_at) = last_stream_activity_at {
        last_activity = last_activity.max(stream_at);
    }
    now.duration_since(last_activity) > stall_timeout
}

#[cfg(test)]
mod tests {
    use super::{
        load_preset_catalog, parse_handbrake_progress_json_block, parse_handbrake_progress_text,
        resolve_preset_path, resolve_preset_path_from_roots, should_mark_transcode_stalled,
        PresetContract,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    fn bundled_preset_file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".watchdog")
            .join("presets")
            .join("AV1_MKV.json")
    }

    #[test]
    fn parse_progress_line_with_stats() {
        let line = "Encoding: task 1 of 1, 12.34 % (5.67 fps, avg 4.56 fps, ETA 01h23m45s)";
        let p = parse_handbrake_progress_text(line).unwrap();
        assert!((p.percent - 12.34).abs() < 0.001);
        assert!((p.fps - 5.67).abs() < 0.001);
        assert!((p.avg_fps - 4.56).abs() < 0.001);
        assert_eq!(p.eta, "01h23m45s");
    }

    #[test]
    fn parse_progress_line_without_stats() {
        let line = "Encoding: task 1 of 1, 98.0 %";
        let p = parse_handbrake_progress_text(line).unwrap();
        assert!((p.percent - 98.0).abs() < 0.001);
        assert_eq!(p.fps, 0.0);
        assert_eq!(p.avg_fps, 0.0);
        assert_eq!(p.eta, "");
    }

    #[test]
    fn parse_progress_json_working() {
        let block = r#"{
  "State": "WORKING",
  "Working": {
    "Progress": 0.5,
    "Rate": 7.5,
    "RateAvg": 6.0,
    "ETASeconds": 75
  }
}"#;
        let p = parse_handbrake_progress_json_block(block).unwrap();
        assert!((p.percent - 50.0).abs() < 0.001);
        assert!((p.fps - 7.5).abs() < 0.001);
        assert!((p.avg_fps - 6.0).abs() < 0.001);
        assert_eq!(p.eta, "00h01m15s");
    }

    #[test]
    fn parse_progress_json_workdone() {
        let block = r#"{
  "State": "WORKDONE",
  "WorkDone": {
    "Error": 0,
    "SequenceID": 1
  }
}"#;
        let p = parse_handbrake_progress_json_block(block).unwrap();
        assert!((p.percent - 100.0).abs() < 0.001);
    }

    #[test]
    fn resolve_bundled_preset_contract() {
        let preset_file = bundled_preset_file();
        let contract = PresetContract::resolve(&preset_file, "AV1_MKV").unwrap();
        assert_eq!(contract.target_codec, "av1");
        assert_eq!(contract.container_extension, "mkv");
        assert!(contract.container_matches(&["matroska".to_string()]));
    }

    #[test]
    fn bundled_preset_target_codec_validation_is_separate() {
        let contract = PresetContract::resolve(&bundled_preset_file(), "AV1_MKV").unwrap();
        assert!(contract.ensure_target_codec("av1").is_ok());
        assert!(contract.ensure_target_codec("h264").is_err());
    }

    #[test]
    fn preset_path_falls_back_to_presets_dir_for_legacy_bare_filename() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("presets")).unwrap();
        fs::copy(
            bundled_preset_file(),
            dir.path().join("presets/AV1_MKV.json"),
        )
        .unwrap();

        let resolved = resolve_preset_path(dir.path(), "AV1_MKV.json");
        assert_eq!(resolved, dir.path().join("presets/AV1_MKV.json"));
    }

    #[test]
    fn preset_path_falls_back_to_hidden_preset_dir_for_legacy_relative_path() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".watchdog/presets")).unwrap();
        fs::copy(
            bundled_preset_file(),
            dir.path().join(".watchdog/presets/AV1_MKV.json"),
        )
        .unwrap();

        let resolved = resolve_preset_path(dir.path(), "presets/AV1_MKV.json");
        assert_eq!(resolved, dir.path().join(".watchdog/presets/AV1_MKV.json"));
    }

    #[test]
    fn preset_path_falls_back_to_packaged_search_root_for_bare_filename() {
        let base_dir = tempfile::tempdir().unwrap();
        let packaged_dir = tempfile::tempdir().unwrap();
        let packaged_presets = packaged_dir.path().join("presets");
        fs::create_dir_all(&packaged_presets).unwrap();
        fs::copy(bundled_preset_file(), packaged_presets.join("AV1_MKV.json")).unwrap();

        let resolved = resolve_preset_path_from_roots(
            base_dir.path(),
            "AV1_MKV.json",
            &[packaged_presets.clone()],
        );
        assert_eq!(resolved, packaged_presets.join("AV1_MKV.json"));
    }

    #[test]
    fn preset_path_falls_back_to_packaged_search_root_for_presets_relative_path() {
        let base_dir = tempfile::tempdir().unwrap();
        let packaged_dir = tempfile::tempdir().unwrap();
        let packaged_presets = packaged_dir.path().join("presets");
        fs::create_dir_all(&packaged_presets).unwrap();
        fs::copy(bundled_preset_file(), packaged_presets.join("AV1_MKV.json")).unwrap();

        let resolved = resolve_preset_path_from_roots(
            base_dir.path(),
            "presets/AV1_MKV.json",
            &[packaged_presets.clone()],
        );
        assert_eq!(resolved, packaged_presets.join("AV1_MKV.json"));
    }

    #[test]
    fn preset_catalog_surfaces_invalid_json_without_hiding_valid_presets() {
        let dir = tempfile::tempdir().unwrap();
        let presets_dir = dir.path().join("presets");
        fs::create_dir_all(&presets_dir).unwrap();
        fs::copy(bundled_preset_file(), presets_dir.join("valid.json")).unwrap();
        fs::write(presets_dir.join("broken.json"), b"{not-json").unwrap();

        let catalog = load_preset_catalog(dir.path());
        assert!(catalog.iter().any(|entry| entry.selectable
            && entry.preset_file == "presets/valid.json"
            && entry.preset_name == "AV1_MKV"
            && entry.target_codec.as_deref() == Some("av1")));
        assert!(catalog.iter().any(|entry| !entry.selectable
            && entry.preset_file == "presets/broken.json"
            && entry.error_summary.is_some()));
    }

    #[test]
    fn output_path_uses_resolved_container_extension() {
        let contract = PresetContract {
            target_codec: "av1".to_string(),
            container_extension: "mkv".to_string(),
            accepted_format_names: vec!["matroska".to_string(), "webm".to_string()],
        };

        for input in [
            "/mnt/movies/Example.mkv",
            "/mnt/movies/Example.mp4",
            "/mnt/movies/Example.avi",
            "/mnt/movies/Example.mov",
            "/mnt/movies/Example.webm",
        ] {
            let output = contract.output_path_for(Path::new(input));
            assert_eq!(output, PathBuf::from("/mnt/movies/Example.mkv"));
        }
    }

    #[test]
    fn stall_detection_uses_latest_activity_signal() {
        let now = Instant::now();
        let start = now - Duration::from_secs(100);
        let progress_at = Some(now - Duration::from_secs(8));
        let output_growth_at = now - Duration::from_secs(15);
        assert!(!should_mark_transcode_stalled(
            start,
            progress_at,
            output_growth_at,
            None,
            now,
            Duration::from_secs(10)
        ));
    }

    #[test]
    fn stall_detection_triggers_when_all_signals_idle() {
        let now = Instant::now();
        let start = now - Duration::from_secs(200);
        let progress_at = Some(now - Duration::from_secs(30));
        let output_growth_at = now - Duration::from_secs(25);
        assert!(should_mark_transcode_stalled(
            start,
            progress_at,
            output_growth_at,
            None,
            now,
            Duration::from_secs(10)
        ));
    }

    #[test]
    fn stall_detection_uses_stream_activity_when_progress_parser_misses() {
        let now = Instant::now();
        let start = now - Duration::from_secs(120);
        let output_growth_at = now - Duration::from_secs(40);
        let stream_activity_at = Some(now - Duration::from_secs(4));
        assert!(!should_mark_transcode_stalled(
            start,
            None,
            output_growth_at,
            stream_activity_at,
            now,
            Duration::from_secs(10)
        ));
    }
}
