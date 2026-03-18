use crate::config::TranscodeConfig;
use crate::error::{Result, WatchdogError};
use crate::process::{
    describe_exit_status, format_command_for_log, infer_failure_hint, run_command,
    summarize_output_tail, RunOptions,
};
use crate::traits::{ProbeResult, Prober};
use crate::transcode::PresetContract;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tracing::{info, warn};

/// Real ffprobe implementation.
pub struct FfprobeProber;

impl Prober for FfprobeProber {
    fn probe(&self, path: &Path) -> Result<Option<ProbeResult>> {
        let mut cmd = Command::new("ffprobe");
        cmd.args([
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(path);
        let command_repr = format_command_for_log(&cmd);
        let output = run_command(
            cmd,
            RunOptions {
                timeout: Some(Duration::from_secs(30)),
                stdout_limit: 32 * 1024 * 1024,
                stderr_limit: 64 * 1024,
                ..RunOptions::default()
            },
        )
        .map_err(|e| WatchdogError::Probe {
            path: path.to_path_buf(),
            reason: format!("Failed to execute ffprobe: {}", e),
        })?;

        if output.timed_out {
            warn!(
                "ffprobe timed out while probing {}: timeout=30s elapsed={:.1}s command={}",
                path.display(),
                output.elapsed.as_secs_f64(),
                command_repr
            );
            return Err(WatchdogError::Probe {
                path: path.to_path_buf(),
                reason: "ffprobe timed out after 30s".to_string(),
            });
        }

        if !output.status.success() {
            let stderr_summary = summarize_output_tail(&output.stderr_tail, 320);
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            let trunc_suffix = if output.stderr_truncated {
                format!(
                    " [stderr tail truncated; captured {} of {} bytes]",
                    output.stderr_tail.len(),
                    output.stderr_total_bytes
                )
            } else {
                String::new()
            };
            warn!(
                "ffprobe failed for {}: status={} elapsed={:.1}s stderr={}{}{}",
                path.display(),
                describe_exit_status(&output.status),
                output.elapsed.as_secs_f64(),
                stderr_summary,
                trunc_suffix,
                hint_suffix
            );
            return Ok(None);
        }

        let json_str = String::from_utf8_lossy(&output.stdout);
        let data: serde_json::Value = match serde_json::from_str(&json_str) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "ffprobe JSON parse failed for {}: {} | stdout_excerpt={}{}",
                    path.display(),
                    e,
                    summarize_output_tail(&output.stdout, 320),
                    if output.stdout_truncated {
                        format!(
                            " [stdout tail truncated; captured {} of {} bytes]",
                            output.stdout.len(),
                            output.stdout_total_bytes
                        )
                    } else {
                        String::new()
                    }
                );
                return Ok(None);
            }
        };

        Ok(Some(parse_probe_result(data)))
    }

    fn health_check(&self, path: &Path) -> Result<bool> {
        let mut cmd = Command::new("ffprobe");
        cmd.args(["-v", "error", "-hide_banner"]).arg(path);
        let command_repr = format_command_for_log(&cmd);
        let output = run_command(
            cmd,
            RunOptions {
                timeout: Some(Duration::from_secs(15)),
                stderr_limit: 64 * 1024,
                ..RunOptions::default()
            },
        )
        .map_err(|e| WatchdogError::Probe {
            path: path.to_path_buf(),
            reason: format!("ffprobe health check failed: {}", e),
        })?;

        if output.timed_out {
            warn!(
                "ffprobe health-check timed out for {} after 15s (elapsed={:.1}s): command={}",
                path.display(),
                output.elapsed.as_secs_f64(),
                command_repr
            );
            return Ok(false);
        }

        if !output.status.success() {
            let stderr_summary = summarize_output_tail(&output.stderr_tail, 320);
            let hint_suffix = infer_failure_hint(&stderr_summary)
                .map(|hint| format!(" likely_cause={}", hint))
                .unwrap_or_default();
            warn!(
                "ffprobe health-check failed for {}: status={} stderr={}{}",
                path.display(),
                describe_exit_status(&output.status),
                stderr_summary,
                hint_suffix
            );
            return Ok(false);
        }

        Ok(true)
    }
}

/// Parse ffprobe JSON output into a ProbeResult.
fn parse_probe_result(data: serde_json::Value) -> ProbeResult {
    let streams = data.get("streams").and_then(|s| s.as_array());
    let format = data.get("format");

    let mut video_codec = None;
    let mut stream_bitrate_bps = 0u64;
    let mut video_count = 0u32;
    let mut audio_count = 0u32;
    let mut subtitle_count = 0u32;

    if let Some(streams) = streams {
        for stream in streams {
            let codec_type = stream.get("codec_type").and_then(|v| v.as_str());
            match codec_type {
                Some("video") => {
                    video_count += 1;
                    if video_codec.is_none() {
                        video_codec = stream
                            .get("codec_name")
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        stream_bitrate_bps = stream
                            .get("bit_rate")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                    }
                }
                Some("audio") => audio_count += 1,
                Some("subtitle") => subtitle_count += 1,
                _ => {}
            }
        }
    }

    let size_bytes: u64 = format
        .and_then(|f| f.get("size"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let duration_seconds: f64 = format
        .and_then(|f| f.get("duration"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    let format_bitrate_bps: u64 = format
        .and_then(|f| f.get("bit_rate"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    ProbeResult {
        video_codec,
        stream_bitrate_bps,
        format_bitrate_bps,
        size_bytes,
        duration_seconds,
        video_stream_count: video_count,
        audio_stream_count: audio_count,
        subtitle_stream_count: subtitle_count,
        raw_json: data,
    }
}

/// Determine effective bitrate in bps from probe result.
fn effective_bitrate_bps(probe: &ProbeResult) -> u64 {
    if probe.format_bitrate_bps > 0 {
        probe.format_bitrate_bps
    } else if probe.stream_bitrate_bps > 0 {
        probe.stream_bitrate_bps
    } else if probe.size_bytes > 0 && probe.duration_seconds > 0.0 {
        ((probe.size_bytes * 8) as f64 / probe.duration_seconds) as u64
    } else {
        0
    }
}

/// Evaluation result for whether a file needs transcoding.
#[derive(Debug, Clone)]
pub struct TranscodeEval {
    pub needs_transcode: bool,
    pub reasons: Vec<String>,
    pub bitrate_mbps: f64,
    pub video_codec: Option<String>,
    pub bitrate_bps: u64,
}

/// Evaluate whether a file needs transcoding based on its probe results and config.
pub fn evaluate_transcode_need(probe: &ProbeResult, config: &TranscodeConfig) -> TranscodeEval {
    let mut reasons = Vec::new();
    let bitrate_bps = effective_bitrate_bps(probe);
    let bitrate_mbps = bitrate_bps as f64 / 1_000_000.0;
    let is_target_codec = probe
        .video_codec
        .as_deref()
        .is_some_and(|codec| codec.eq_ignore_ascii_case(&config.target_codec));

    if !is_target_codec {
        reasons.push(format!(
            "codec is {}",
            probe.video_codec.as_deref().unwrap_or("unknown")
        ));
    }

    if bitrate_mbps == 0.0 {
        // If we can't determine bitrate but the file is already in the target codec,
        // don't re-transcode it -- there's nothing to gain.
        if !is_target_codec {
            reasons.push("unable to determine bitrate".to_string());
        }
    } else if bitrate_mbps > config.max_average_bitrate_mbps {
        reasons.push(format!(
            "average bitrate {:.2} Mbps exceeds limit {:.2} Mbps",
            bitrate_mbps, config.max_average_bitrate_mbps
        ));
    }

    TranscodeEval {
        needs_transcode: !reasons.is_empty(),
        reasons,
        bitrate_mbps,
        video_codec: probe.video_codec.clone(),
        bitrate_bps,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationFailure {
    HealthCheckFailed,
    OriginalProbeFailed,
    OutputProbeFailed,
    OriginalDurationMissing,
    OutputDurationMissing,
    DurationMismatch {
        original_ms: u64,
        output_ms: u64,
        delta_ms: u64,
        tolerance_ms: u64,
    },
    CodecMismatch {
        expected: String,
        actual: Option<String>,
    },
    ContainerMismatch {
        expected_extension: String,
        actual_formats: Vec<String>,
    },
    VideoMissing,
    AudioMissing {
        original_count: u32,
        output_count: u32,
    },
}

impl VerificationFailure {
    pub fn code(&self) -> &'static str {
        match self {
            Self::HealthCheckFailed => "verification_health_check_failed",
            Self::OriginalProbeFailed => "verification_original_probe_failed",
            Self::OutputProbeFailed => "verification_output_probe_failed",
            Self::OriginalDurationMissing => "verification_original_zero_duration",
            Self::OutputDurationMissing => "verification_output_zero_duration",
            Self::DurationMismatch { .. } => "verification_duration_mismatch",
            Self::CodecMismatch { .. } => "verification_codec_mismatch",
            Self::ContainerMismatch { .. } => "verification_container_mismatch",
            Self::VideoMissing => "verification_video_missing",
            Self::AudioMissing { .. } => "verification_audio_missing",
        }
    }

    pub fn summary(&self) -> String {
        match self {
            Self::HealthCheckFailed => "ffprobe health check failed".to_string(),
            Self::OriginalProbeFailed => "failed to read source metadata".to_string(),
            Self::OutputProbeFailed => "failed to read transcoded metadata".to_string(),
            Self::OriginalDurationMissing => "source duration is 0 seconds".to_string(),
            Self::OutputDurationMissing => "transcoded duration is 0 seconds".to_string(),
            Self::DurationMismatch {
                original_ms,
                output_ms,
                delta_ms,
                tolerance_ms,
            } => format!(
                "duration mismatch: source={:.3}s output={:.3}s delta={:.3}s tolerance={:.3}s",
                (*original_ms as f64) / 1000.0,
                (*output_ms as f64) / 1000.0,
                (*delta_ms as f64) / 1000.0,
                (*tolerance_ms as f64) / 1000.0
            ),
            Self::CodecMismatch { expected, actual } => format!(
                "codec mismatch: expected {} got {}",
                expected,
                actual.as_deref().unwrap_or("unknown")
            ),
            Self::ContainerMismatch {
                expected_extension,
                actual_formats,
            } => format!(
                "container mismatch: expected {} got {}",
                expected_extension,
                if actual_formats.is_empty() {
                    "unknown".to_string()
                } else {
                    actual_formats.join(",")
                }
            ),
            Self::VideoMissing => "transcoded output has no video stream".to_string(),
            Self::AudioMissing {
                original_count,
                output_count,
            } => format!(
                "audio streams missing: source had {} output has {}",
                original_count, output_count
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationOutcome {
    Passed,
    Failed(VerificationFailure),
}

fn probe_format_names(probe: &ProbeResult) -> Vec<String> {
    probe
        .raw_json
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(serde_json::Value::as_str)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|segment| !segment.is_empty())
                .map(|segment| segment.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

fn verification_duration_tolerance(duration_seconds: f64) -> f64 {
    (duration_seconds * 0.001).clamp(2.0, 5.0)
}

/// Verify a transcode by comparing original and transcoded file metadata.
/// Rejects the transcode if any safety check fails — we never replace on doubt.
///
/// IMPORTANT: Callers should verify the output file exists and has non-zero size
/// via the FileSystem trait BEFORE calling this (to support simulation mode).
pub fn verify_transcode(
    prober: &dyn Prober,
    original: &Path,
    transcoded: &Path,
    contract: &PresetContract,
) -> Result<VerificationOutcome> {
    // Health check on the transcoded file (ffprobe -v error)
    if !prober.health_check(transcoded)? {
        warn!("Health check failed for {}", transcoded.display());
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::HealthCheckFailed,
        ));
    }

    let orig = match prober.probe(original)? {
        Some(p) => p,
        None => {
            warn!(
                "Failed to read metadata for verification of {}",
                original.display()
            );
            return Ok(VerificationOutcome::Failed(
                VerificationFailure::OriginalProbeFailed,
            ));
        }
    };

    let new = match prober.probe(transcoded)? {
        Some(p) => p,
        None => {
            warn!(
                "Failed to read metadata for verification of {}",
                transcoded.display()
            );
            return Ok(VerificationOutcome::Failed(
                VerificationFailure::OutputProbeFailed,
            ));
        }
    };

    // Duration: original must have a valid duration
    if orig.duration_seconds == 0.0 {
        warn!(
            "MANUAL ENCODING REQUIRED: original duration is 0.0s for {} — rejecting automatic transcode",
            original.display()
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::OriginalDurationMissing,
        ));
    }

    // Duration: transcoded must have a valid duration
    if new.duration_seconds == 0.0 {
        warn!(
            "Transcoded file has 0.0s duration — rejecting: {}",
            transcoded.display()
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::OutputDurationMissing,
        ));
    }

    let output_codec = new
        .video_codec
        .as_deref()
        .map(|codec| codec.to_ascii_lowercase());
    if output_codec
        .as_deref()
        .is_none_or(|codec| codec != contract.target_codec)
    {
        warn!(
            "Codec mismatch for {}: expected {} got {}",
            transcoded.display(),
            contract.target_codec,
            output_codec.as_deref().unwrap_or("unknown")
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::CodecMismatch {
                expected: contract.target_codec.clone(),
                actual: output_codec,
            },
        ));
    }

    let output_format_names = probe_format_names(&new);
    if !contract.container_matches(&output_format_names) {
        warn!(
            "Container mismatch for {}: expected {} got {:?}",
            transcoded.display(),
            contract.container_extension,
            output_format_names
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::ContainerMismatch {
                expected_extension: contract.container_extension.clone(),
                actual_formats: output_format_names,
            },
        ));
    }

    let duration_delta = (orig.duration_seconds - new.duration_seconds).abs();
    let duration_tolerance = verification_duration_tolerance(orig.duration_seconds);
    if duration_delta > duration_tolerance {
        warn!(
            "Duration mismatch: original={:.3}s new={:.3}s (delta={:.3}s tolerance={:.3}s)",
            orig.duration_seconds, new.duration_seconds, duration_delta, duration_tolerance
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::DurationMismatch {
                original_ms: (orig.duration_seconds * 1000.0).round() as u64,
                output_ms: (new.duration_seconds * 1000.0).round() as u64,
                delta_ms: (duration_delta * 1000.0).round() as u64,
                tolerance_ms: (duration_tolerance * 1000.0).round() as u64,
            },
        ));
    }

    // Must have at least 1 video stream
    if new.video_stream_count == 0 {
        warn!(
            "Transcoded file has no video streams — rejecting: {}",
            transcoded.display()
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::VideoMissing,
        ));
    }

    if orig.audio_stream_count > 0 && new.audio_stream_count == 0 {
        warn!(
            "Audio stream missing after transcode: orig={} new={}",
            orig.audio_stream_count, new.audio_stream_count
        );
        return Ok(VerificationOutcome::Failed(
            VerificationFailure::AudioMissing {
                original_count: orig.audio_stream_count,
                output_count: new.audio_stream_count,
            },
        ));
    }

    if orig.video_stream_count != new.video_stream_count
        || orig.audio_stream_count != new.audio_stream_count
    {
        info!(
            "Primary stream counts changed but required tracks remain: orig(v{},a{}) -> new(v{},a{})",
            orig.video_stream_count,
            orig.audio_stream_count,
            new.video_stream_count,
            new.audio_stream_count
        );
    }

    if orig.subtitle_stream_count != new.subtitle_stream_count {
        info!(
            "Subtitle track count changed: orig s{} -> new s{} (allowed)",
            orig.subtitle_stream_count, new.subtitle_stream_count
        );
    }

    info!(
        "Verification passed: duration {:.1}s codec={} container={} v{}/a{}/s{}",
        new.duration_seconds,
        contract.target_codec,
        contract.container_extension,
        new.video_stream_count,
        new.audio_stream_count,
        new.subtitle_stream_count,
    );

    Ok(VerificationOutcome::Passed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn base_probe() -> ProbeResult {
        ProbeResult {
            video_codec: Some("av1".to_string()),
            stream_bitrate_bps: 0,
            format_bitrate_bps: 0,
            size_bytes: 1000,
            duration_seconds: 10.0,
            video_stream_count: 1,
            audio_stream_count: 1,
            subtitle_stream_count: 0,
            raw_json: serde_json::json!({
                "format": {
                    "format_name": "matroska,webm"
                }
            }),
        }
    }

    fn contract() -> PresetContract {
        PresetContract::resolve(
            &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(".watchdog")
                .join("presets")
                .join("AV1_MKV.json"),
            "AV1_MKV",
        )
        .unwrap()
    }

    fn probe_with(
        codec: &str,
        duration_seconds: f64,
        video_stream_count: u32,
        audio_stream_count: u32,
        format_name: &str,
    ) -> ProbeResult {
        ProbeResult {
            video_codec: Some(codec.to_string()),
            duration_seconds,
            video_stream_count,
            audio_stream_count,
            raw_json: serde_json::json!({
                "format": {
                    "format_name": format_name
                }
            }),
            ..base_probe()
        }
    }

    struct MockProber {
        original: Option<ProbeResult>,
        output: Option<ProbeResult>,
        health_ok: bool,
    }

    impl Prober for MockProber {
        fn probe(&self, path: &Path) -> Result<Option<ProbeResult>> {
            if path.to_string_lossy().contains("source") {
                Ok(self.original.clone())
            } else {
                Ok(self.output.clone())
            }
        }

        fn health_check(&self, _path: &Path) -> Result<bool> {
            Ok(self.health_ok)
        }
    }

    #[test]
    fn test_effective_bitrate_prefers_format_then_stream_then_computed() {
        let mut probe = base_probe();
        probe.format_bitrate_bps = 7;
        probe.stream_bitrate_bps = 5;
        assert_eq!(effective_bitrate_bps(&probe), 7);

        probe.format_bitrate_bps = 0;
        assert_eq!(effective_bitrate_bps(&probe), 5);

        probe.stream_bitrate_bps = 0;
        assert_eq!(effective_bitrate_bps(&probe), 800);
    }

    #[test]
    fn test_codec_compare_is_case_insensitive() {
        let probe = ProbeResult {
            video_codec: Some("AV1".to_string()),
            ..base_probe()
        };
        let cfg = TranscodeConfig {
            target_codec: "av1".to_string(),
            max_average_bitrate_mbps: 1000.0,
            ..TranscodeConfig::default()
        };

        let eval = evaluate_transcode_need(&probe, &cfg);
        assert!(!eval.needs_transcode);
    }

    #[test]
    fn verify_transcode_rejects_wrong_codec() {
        let prober = MockProber {
            original: Some(probe_with("h264", 120.0, 1, 1, "matroska,webm")),
            output: Some(probe_with("h264", 120.0, 1, 1, "matroska,webm")),
            health_ok: true,
        };

        let result = verify_transcode(
            &prober,
            Path::new("/tmp/source.mkv"),
            Path::new("/tmp/output.mkv"),
            &contract(),
        )
        .unwrap();
        assert_eq!(
            result,
            VerificationOutcome::Failed(VerificationFailure::CodecMismatch {
                expected: "av1".to_string(),
                actual: Some("h264".to_string()),
            })
        );
    }

    #[test]
    fn verify_transcode_rejects_wrong_container() {
        let prober = MockProber {
            original: Some(probe_with("h264", 120.0, 1, 1, "matroska,webm")),
            output: Some(probe_with("av1", 120.0, 1, 1, "mov,mp4,m4a,3gp,3g2,mj2")),
            health_ok: true,
        };

        let result = verify_transcode(
            &prober,
            Path::new("/tmp/source.mkv"),
            Path::new("/tmp/output.mp4"),
            &contract(),
        )
        .unwrap();
        assert!(matches!(
            result,
            VerificationOutcome::Failed(VerificationFailure::ContainerMismatch { .. })
        ));
    }

    #[test]
    fn verify_transcode_allows_small_duration_drift_and_track_normalization() {
        let prober = MockProber {
            original: Some(probe_with("h264", 7200.0, 1, 1, "matroska,webm")),
            output: Some(probe_with("av1", 7204.0, 1, 2, "matroska,webm")),
            health_ok: true,
        };

        let result = verify_transcode(
            &prober,
            Path::new("/tmp/source.mkv"),
            Path::new("/tmp/output.mkv"),
            &contract(),
        )
        .unwrap();
        assert_eq!(result, VerificationOutcome::Passed);
    }

    #[test]
    fn verify_transcode_rejects_missing_required_audio() {
        let prober = MockProber {
            original: Some(probe_with("h264", 120.0, 1, 2, "matroska,webm")),
            output: Some(probe_with("av1", 120.0, 1, 0, "matroska,webm")),
            health_ok: true,
        };

        let result = verify_transcode(
            &prober,
            Path::new("/tmp/source.mkv"),
            Path::new("/tmp/output.mkv"),
            &contract(),
        )
        .unwrap();
        assert_eq!(
            result,
            VerificationOutcome::Failed(VerificationFailure::AudioMissing {
                original_count: 2,
                output_count: 0,
            })
        );
    }
}
