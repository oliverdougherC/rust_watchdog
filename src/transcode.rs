use crate::error::{Result, WatchdogError};
use crate::process::{configure_subprocess_group, terminate_subprocess};
use crate::traits::{TranscodeProgress, TranscodeResult, Transcoder};
use regex::Regex;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// Strip ANSI escape sequences
static ANSI_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]").unwrap());

// HandBrake progress: "Encoding: task 1 of 1, 12.34 % (5.67 fps, avg 4.56 fps, ETA 01h23m45s)"
static HB_PROGRESS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"Encoding:.*?(\d+(?:\.\d+)?)\s*%(?:\s*\((\d+(?:\.\d+)?)\s*fps,\s*avg\s*(\d+(?:\.\d+)?)\s*fps,\s*ETA\s*(\S+)\))?",
    )
    .unwrap()
});

fn push_tail(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) {
    if limit == 0 {
        return;
    }
    buf.extend_from_slice(chunk);
    if buf.len() > limit {
        let drop_len = buf.len() - limit;
        buf.drain(0..drop_len);
    }
}

/// Parse a line of HandBrake output for progress information.
fn parse_handbrake_progress(line: &str) -> Option<TranscodeProgress> {
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

/// Real HandBrakeCLI transcoder using PTY for progress parsing.
pub struct HandBrakeTranscoder;

impl Transcoder for HandBrakeTranscoder {
    fn transcode(
        &self,
        input: &Path,
        output: &Path,
        preset_file: &Path,
        preset_name: &str,
        timeout_secs: u64,
        progress_tx: mpsc::Sender<TranscodeProgress>,
        cancel: Arc<AtomicBool>,
    ) -> Result<TranscodeResult> {
        use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
        use nix::pty::openpty;
        use std::io::Read;
        use std::os::fd::AsFd;

        use std::process::{Command, Stdio};

        let cmd_str = format!(
            "HandBrakeCLI --preset-import-file {} -i {} -o {} --preset {}",
            preset_file.display(),
            input.display(),
            output.display(),
            preset_name
        );
        info!("Running: {}", cmd_str);

        // Open a PTY so HandBrake thinks stderr is a terminal
        let pty = openpty(None, None).map_err(|e| WatchdogError::Transcode {
            path: input.to_path_buf(),
            reason: format!("Failed to open PTY: {}", e),
        })?;

        // Convert slave OwnedFd to a File for Stdio
        let slave_file = std::fs::File::from(pty.slave);

        let mut cmd = Command::new("HandBrakeCLI");
        configure_subprocess_group(&mut cmd);
        let child_result = cmd
            .arg("--preset-import-file")
            .arg(preset_file)
            .arg("-i")
            .arg(input)
            .arg("-o")
            .arg(output)
            .arg("--preset")
            .arg(preset_name)
            .stdout(Stdio::null())
            .stderr(Stdio::from(slave_file))
            .spawn();

        let mut child = match child_result {
            Ok(c) => c,
            Err(e) => {
                return Err(WatchdogError::Transcode {
                    path: input.to_path_buf(),
                    reason: format!("Failed to spawn HandBrakeCLI: {}", e),
                });
            }
        };

        let start = Instant::now();
        let mut timed_out = false;
        let mut line_buf = Vec::new();
        let mut stderr_tail = Vec::new();
        const STDERR_LIMIT: usize = 128 * 1024;

        // Convert master to a File for reading
        let mut master_file = std::fs::File::from(pty.master);

        // Use a helper to process bytes into progress
        let mut process_byte = |byte: u8| {
            if byte == b'\r' || byte == b'\n' {
                if !line_buf.is_empty() {
                    let line = String::from_utf8_lossy(&line_buf).to_string();
                    push_tail(&mut stderr_tail, line.as_bytes(), STDERR_LIMIT);
                    push_tail(&mut stderr_tail, b"\n", STDERR_LIMIT);
                    if let Some(progress) = parse_handbrake_progress(&line) {
                        let _ = progress_tx.try_send(progress);
                    }
                    line_buf.clear();
                }
            } else {
                line_buf.push(byte);
            }
        };

        loop {
            if cancel.load(Ordering::Relaxed) {
                terminate_subprocess(&mut child, Duration::from_secs(2));
                let _ = child.wait();
                return Err(WatchdogError::TranscodeCancelled {
                    path: input.to_path_buf(),
                    reason: "cancel signal received".to_string(),
                });
            }

            if timeout_secs > 0 && start.elapsed().as_secs() > timeout_secs {
                timed_out = true;
                terminate_subprocess(&mut child, Duration::from_secs(2));
                let _ = child.wait();
                warn!(
                    "HandBrake timed out after {}s: {}",
                    timeout_secs,
                    input.display()
                );
                break;
            }

            // Poll for data with 1s timeout
            let poll_timeout = PollTimeout::try_from(1000i32).unwrap_or(PollTimeout::NONE);
            let mut poll_fds = [PollFd::new(master_file.as_fd(), PollFlags::POLLIN)];
            match poll(&mut poll_fds, poll_timeout) {
                Ok(0) => {
                    // Timeout on poll - check if child exited
                    if let Some(_status) = child.try_wait().ok().flatten() {
                        // Drain remaining data
                        let mut buf = [0u8; 4096];
                        loop {
                            match master_file.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => {
                                    for &byte in &buf[..n] {
                                        process_byte(byte);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        break;
                    }
                    continue;
                }
                Ok(_) => {
                    let mut buf = [0u8; 4096];
                    match master_file.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            for &byte in &buf[..n] {
                                process_byte(byte);
                            }
                        }
                        Err(ref e) if e.raw_os_error() == Some(libc::EIO) => {
                            // EIO means slave side closed (child exited)
                            break;
                        }
                        Err(e) => {
                            error!("Error reading from PTY: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Poll error: {}", e);
                    break;
                }
            }
        }

        // Parse any remaining buffer
        if !line_buf.is_empty() {
            let line = String::from_utf8_lossy(&line_buf).to_string();
            if let Some(progress) = parse_handbrake_progress(&line) {
                let _ = progress_tx.try_send(progress);
            }
        }

        let status = child.wait().map_err(|e| WatchdogError::Transcode {
            path: input.to_path_buf(),
            reason: format!("Failed to wait for HandBrakeCLI: {}", e),
        })?;

        if timed_out {
            return Err(WatchdogError::TranscodeTimeout {
                path: input.to_path_buf(),
                timeout_secs,
            });
        }

        let output_exists = output.exists();

        if !status.success() {
            warn!(
                "HandBrakeCLI failed (rc={:?}) for {}: {}",
                status.code(),
                input.display(),
                String::from_utf8_lossy(&stderr_tail).trim()
            );
        }

        Ok(TranscodeResult {
            success: status.success(),
            timed_out: false,
            output_exists,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_handbrake_progress;

    #[test]
    fn parse_progress_line_with_stats() {
        let line = "Encoding: task 1 of 1, 12.34 % (5.67 fps, avg 4.56 fps, ETA 01h23m45s)";
        let p = parse_handbrake_progress(line).unwrap();
        assert!((p.percent - 12.34).abs() < 0.001);
        assert!((p.fps - 5.67).abs() < 0.001);
        assert!((p.avg_fps - 4.56).abs() < 0.001);
        assert_eq!(p.eta, "01h23m45s");
    }

    #[test]
    fn parse_progress_line_without_stats() {
        let line = "Encoding: task 1 of 1, 98.0 %";
        let p = parse_handbrake_progress(line).unwrap();
        assert!((p.percent - 98.0).abs() < 0.001);
        assert_eq!(p.fps, 0.0);
        assert_eq!(p.avg_fps, 0.0);
        assert_eq!(p.eta, "");
    }
}
