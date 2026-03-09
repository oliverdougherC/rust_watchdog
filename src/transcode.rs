use crate::error::{Result, WatchdogError};
use crate::process::{
    configure_subprocess_group, describe_exit_status, format_command_for_log, infer_failure_hint,
    run_command, summarize_output_tail, terminate_subprocess, RunOptions,
};
use crate::traits::{TranscodeProgress, TranscodeResult, Transcoder};
use regex::Regex;
use serde_json::Value;
use std::io::{self, Read};
use std::path::Path;
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
}

impl Default for ProgressActivity {
    fn default() -> Self {
        Self {
            last_percent: -1.0,
            last_forward_progress_at: None,
        }
    }
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

        let status = loop {
            if cancel.load(Ordering::Relaxed) {
                cancelled = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for cancelled HandBrakeCLI process: {}", e),
                })?;
            }

            if timeout_secs > 0 && start.elapsed().as_secs() > timeout_secs {
                timed_out = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for timed out HandBrakeCLI process: {}", e),
                })?;
            }

            let now = Instant::now();
            let output_size = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
            if output_size > last_output_size {
                last_output_size = output_size;
                last_output_growth_at = now;
            }

            let last_forward_progress_at = progress_activity
                .lock()
                .ok()
                .and_then(|shared| shared.last_forward_progress_at);
            if should_mark_transcode_stalled(
                start,
                last_forward_progress_at,
                last_output_growth_at,
                now,
                stall_timeout,
            ) {
                stalled = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                break child.wait().map_err(|e| WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to wait for stalled HandBrakeCLI process: {}", e),
                })?;
            }

            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(e) => {
                    return Err(WatchdogError::Transcode {
                        path: input.to_path_buf(),
                        reason: format!("Failed while waiting for HandBrakeCLI: {}", e),
                    });
                }
            }
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
            warn!(
                "HandBrake timed out: input={} timeout={}s elapsed={:.1}s progress={} stderr={}{}{} command={}",
                input.display(),
                timeout_secs,
                elapsed,
                summarize_progress(last_progress),
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
            warn!(
                "HandBrake stalled: input={} stall_timeout={}s elapsed={:.1}s output_bytes={} progress={} stderr={}{}{} command={}",
                input.display(),
                stall_timeout_secs.max(1),
                elapsed,
                last_output_size,
                summarize_progress(last_progress),
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
    now: Instant,
    stall_timeout: Duration,
) -> bool {
    let mut last_activity = start;
    if let Some(progress_at) = last_forward_progress_at {
        last_activity = last_activity.max(progress_at);
    }
    last_activity = last_activity.max(last_output_growth_at);
    now.duration_since(last_activity) > stall_timeout
}

#[cfg(test)]
mod tests {
    use super::{
        parse_handbrake_progress_json_block, parse_handbrake_progress_text,
        should_mark_transcode_stalled,
    };
    use std::time::{Duration, Instant};

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
    fn stall_detection_uses_latest_activity_signal() {
        let now = Instant::now();
        let start = now - Duration::from_secs(100);
        let progress_at = Some(now - Duration::from_secs(8));
        let output_growth_at = now - Duration::from_secs(15);
        assert!(!should_mark_transcode_stalled(
            start,
            progress_at,
            output_growth_at,
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
            now,
            Duration::from_secs(10)
        ));
    }
}
