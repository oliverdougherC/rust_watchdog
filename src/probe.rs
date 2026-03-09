use crate::config::TranscodeConfig;
use crate::error::{Result, WatchdogError};
use crate::process::{
    describe_exit_status, format_command_for_log, infer_failure_hint, run_command,
    summarize_output_tail, RunOptions,
};
use crate::traits::{ProbeResult, Prober};
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

/// Verify a transcode by comparing original and transcoded file metadata.
/// Rejects the transcode if any safety check fails — we never replace on doubt.
///
/// IMPORTANT: Callers should verify the output file exists and has non-zero size
/// via the FileSystem trait BEFORE calling this (to support simulation mode).
pub fn verify_transcode(prober: &dyn Prober, original: &Path, transcoded: &Path) -> Result<bool> {
    // Health check on the transcoded file (ffprobe -v error)
    if !prober.health_check(transcoded)? {
        warn!("Health check failed for {}", transcoded.display());
        return Ok(false);
    }

    let orig = match prober.probe(original)? {
        Some(p) => p,
        None => {
            warn!(
                "Failed to read metadata for verification of {}",
                original.display()
            );
            return Ok(false);
        }
    };

    let new = match prober.probe(transcoded)? {
        Some(p) => p,
        None => {
            warn!(
                "Failed to read metadata for verification of {}",
                transcoded.display()
            );
            return Ok(false);
        }
    };

    // Duration: original must have a valid duration
    if orig.duration_seconds == 0.0 {
        warn!(
            "MANUAL ENCODING REQUIRED: original duration is 0.0s for {} — rejecting automatic transcode",
            original.display()
        );
        return Ok(false);
    }

    // Duration: transcoded must have a valid duration
    if new.duration_seconds == 0.0 {
        warn!(
            "Transcoded file has 0.0s duration — rejecting: {}",
            transcoded.display()
        );
        return Ok(false);
    }

    // Duration: must match within 1 second
    if (orig.duration_seconds - new.duration_seconds).abs() > 1.0 {
        warn!(
            "Duration mismatch: original={:.3}s new={:.3}s (delta={:.3}s)",
            orig.duration_seconds,
            new.duration_seconds,
            (orig.duration_seconds - new.duration_seconds).abs()
        );
        return Ok(false);
    }

    // Stream count check: video + audio must match exactly (subtitles can differ)
    if orig.video_stream_count != new.video_stream_count
        || orig.audio_stream_count != new.audio_stream_count
    {
        warn!(
            "Stream count mismatch: orig(v{},a{}) vs new(v{},a{})",
            orig.video_stream_count,
            orig.audio_stream_count,
            new.video_stream_count,
            new.audio_stream_count
        );
        return Ok(false);
    }

    // Must have at least 1 video stream
    if new.video_stream_count == 0 {
        warn!(
            "Transcoded file has no video streams — rejecting: {}",
            transcoded.display()
        );
        return Ok(false);
    }

    if orig.subtitle_stream_count != new.subtitle_stream_count {
        info!(
            "Subtitle track count changed: orig s{} -> new s{} (allowed)",
            orig.subtitle_stream_count, new.subtitle_stream_count
        );
    }

    info!(
        "Verification passed: duration {:.1}s, v{}/a{}/s{}",
        new.duration_seconds,
        new.video_stream_count,
        new.audio_stream_count,
        new.subtitle_stream_count,
    );

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            raw_json: serde_json::json!({}),
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
}
