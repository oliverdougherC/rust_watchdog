use crate::config::TranscodeConfig;
use crate::error::{Result, WatchdogError};
use crate::traits::{ProbeResult, Prober};
use std::path::Path;
use std::process::Command;
use tracing::{info, warn};

/// Real ffprobe implementation.
pub struct FfprobeProber;

impl Prober for FfprobeProber {
    fn probe(&self, path: &Path) -> Result<Option<ProbeResult>> {
        let output = Command::new("ffprobe")
            .args([
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
            ])
            .arg(path)
            .output()
            .map_err(|e| WatchdogError::Probe {
                path: path.to_path_buf(),
                reason: format!("Failed to execute ffprobe: {}", e),
            })?;

        if !output.status.success() {
            return Ok(None);
        }

        let json_str = String::from_utf8_lossy(&output.stdout);
        let data: serde_json::Value = match serde_json::from_str(&json_str) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        Ok(Some(parse_probe_result(data)))
    }

    fn health_check(&self, path: &Path) -> Result<bool> {
        let output = Command::new("ffprobe")
            .args(["-v", "error", "-hide_banner"])
            .arg(path)
            .output()
            .map_err(|e| WatchdogError::Probe {
                path: path.to_path_buf(),
                reason: format!("ffprobe health check failed: {}", e),
            })?;

        Ok(output.status.success())
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
                        video_codec =
                            stream.get("codec_name").and_then(|v| v.as_str()).map(String::from);
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
    let bps = if probe.format_bitrate_bps > 0 {
        probe.format_bitrate_bps
    } else if probe.stream_bitrate_bps > 0 {
        probe.stream_bitrate_bps
    } else if probe.size_bytes > 0 && probe.duration_seconds > 0.0 {
        ((probe.size_bytes * 8) as f64 / probe.duration_seconds) as u64
    } else {
        0
    };
    bps
}

/// Evaluation result for whether a file needs transcoding.
#[derive(Debug)]
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
    let is_target_codec = probe.video_codec.as_deref() == Some(&config.target_codec);

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
pub fn verify_transcode(
    prober: &dyn Prober,
    original: &Path,
    transcoded: &Path,
) -> Result<bool> {
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
