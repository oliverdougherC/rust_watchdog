use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;

/// Controls subprocess execution behavior.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub timeout: Option<Duration>,
    pub kill_grace: Duration,
    pub poll_interval: Duration,
    pub stdout_limit: usize,
    pub stderr_limit: usize,
    pub cancel: Option<Arc<AtomicBool>>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            kill_grace: Duration::from_secs(2),
            poll_interval: Duration::from_millis(100),
            stdout_limit: 16 * 1024 * 1024,
            stderr_limit: 128 * 1024,
            cancel: None,
        }
    }
}

/// Result of a subprocess execution.
#[derive(Debug)]
pub struct RunOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr_tail: Vec<u8>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub elapsed: Duration,
    pub stdout_total_bytes: usize,
    pub stderr_total_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Default)]
struct CapturedOutput {
    bytes: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
}

fn push_with_limit(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
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

fn spawn_capture_thread<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
) -> thread::JoinHandle<CapturedOutput> {
    thread::spawn(move || {
        let mut out = CapturedOutput::default();
        let mut tmp = [0u8; 4096];
        loop {
            match reader.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    out.total_bytes = out.total_bytes.saturating_add(n);
                    out.truncated |= push_with_limit(&mut out.bytes, &tmp[..n], limit);
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        out
    })
}

#[cfg(unix)]
fn configure_process_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: pre_exec runs in the child immediately before exec.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_cmd: &mut Command) {}

/// Configure a command to start in its own process group.
pub fn configure_subprocess_group(cmd: &mut Command) {
    configure_process_group(cmd);
}

#[cfg(unix)]
fn terminate_child_tree(child: &mut Child, grace: Duration) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let pid = Pid::from_raw(child.id() as i32);
    let _ = killpg(pid, Signal::SIGTERM);
    let deadline = Instant::now() + grace;

    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }

    let _ = killpg(pid, Signal::SIGKILL);
}

#[cfg(not(unix))]
fn terminate_child_tree(child: &mut Child, _grace: Duration) {
    let _ = child.kill();
}

/// Best-effort termination for a subprocess and its process group.
pub fn terminate_subprocess(child: &mut Child, grace: Duration) {
    terminate_child_tree(child, grace);
}

fn shell_quote(raw: &str) -> String {
    let safe = !raw.is_empty()
        && raw.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'-' | b'_' | b':' | b'=' | b',')
        });
    if safe {
        raw.to_string()
    } else {
        format!("'{}'", raw.replace('\'', "'\\''"))
    }
}

/// Render a command as a shell-like string for diagnostics.
pub fn format_command_for_log(cmd: &Command) -> String {
    let mut pieces = Vec::new();
    pieces.push(shell_quote(&cmd.get_program().to_string_lossy()));
    for arg in cmd.get_args() {
        pieces.push(shell_quote(&arg.to_string_lossy()));
    }
    pieces.join(" ")
}

/// Return a concise, cross-platform description of process termination.
pub fn describe_exit_status(status: &ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {}", signal);
        }
    }

    match status.code() {
        Some(code) => format!("exit code {}", code),
        None => "terminated (no exit code)".to_string(),
    }
}

/// Compact arbitrary command output for one-line logs.
pub fn summarize_output_tail(bytes: &[u8], max_chars: usize) -> String {
    if bytes.is_empty() {
        return "(empty)".to_string();
    }
    let text = String::from_utf8_lossy(bytes);
    let collapsed = text
        .split_whitespace()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if collapsed.is_empty() {
        return "(whitespace only)".to_string();
    }
    if max_chars == 0 || collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let mut trimmed = String::with_capacity(max_chars.saturating_add(1));
    for (idx, ch) in collapsed.chars().enumerate() {
        if idx >= max_chars {
            break;
        }
        trimmed.push(ch);
    }
    trimmed.push('…');
    trimmed
}

/// Map common stderr text patterns to a likely root-cause hint.
pub fn infer_failure_hint(details: &str) -> Option<&'static str> {
    let lowered = details.to_ascii_lowercase();
    if lowered.contains("no space left on device") {
        Some("destination storage is full")
    } else if lowered.contains("permission denied") || lowered.contains("operation not permitted") {
        Some("permission or ownership prevents the operation")
    } else if lowered.contains("timed out")
        || lowered.contains("connection timeout")
        || lowered.contains("operation timed out")
    {
        Some("network/storage latency caused a timeout")
    } else if lowered.contains("no such file or directory") {
        Some("a referenced file/path does not exist")
    } else if lowered.contains("resource busy") || lowered.contains("device busy") {
        Some("file or mount appears to be busy/in use")
    } else if lowered.contains("i/o error") || lowered.contains("input/output error") {
        Some("storage I/O error")
    } else if lowered.contains("connection refused")
        || lowered.contains("connection reset")
        || lowered.contains("network is unreachable")
    {
        Some("network connectivity problem")
    } else {
        None
    }
}

/// Run a command with timeout/cancellation handling and bounded output capture.
pub fn run_command(mut cmd: Command, options: RunOptions) -> io::Result<RunOutput> {
    let command_repr = format_command_for_log(&cmd);
    debug!(
        command = %command_repr,
        timeout_secs = ?options.timeout.map(|d| d.as_secs()),
        poll_interval_ms = options.poll_interval.as_millis(),
        stdout_limit = options.stdout_limit,
        stderr_limit = options.stderr_limit,
        "Spawning subprocess"
    );
    configure_process_group(&mut cmd);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("failed to capture stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("failed to capture stderr"))?;

    let stdout_handle = spawn_capture_thread(stdout, options.stdout_limit);
    let stderr_handle = spawn_capture_thread(stderr, options.stderr_limit);

    let start = Instant::now();
    let mut timed_out = false;
    let mut cancelled = false;
    let status = loop {
        if options
            .cancel
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
        {
            cancelled = true;
            terminate_child_tree(&mut child, options.kill_grace);
            break child.wait()?;
        }

        if options.timeout.is_some_and(|t| start.elapsed() >= t) {
            timed_out = true;
            terminate_child_tree(&mut child, options.kill_grace);
            break child.wait()?;
        }

        match child.try_wait()? {
            Some(status) => break status,
            None => thread::sleep(options.poll_interval),
        }
    };

    let elapsed = start.elapsed();
    let stdout_capture = stdout_handle.join().unwrap_or_default();
    let stderr_capture = stderr_handle.join().unwrap_or_default();

    debug!(
        command = %command_repr,
        success = status.success(),
        status = %describe_exit_status(&status),
        timed_out,
        cancelled,
        elapsed_ms = elapsed.as_millis(),
        stdout_total_bytes = stdout_capture.total_bytes,
        stderr_total_bytes = stderr_capture.total_bytes,
        stdout_truncated = stdout_capture.truncated,
        stderr_truncated = stderr_capture.truncated,
        "Subprocess finished"
    );

    Ok(RunOutput {
        status,
        stdout: stdout_capture.bytes,
        stderr_tail: stderr_capture.bytes,
        timed_out,
        cancelled,
        elapsed,
        stdout_total_bytes: stdout_capture.total_bytes,
        stderr_total_bytes: stderr_capture.total_bytes,
        stdout_truncated: stdout_capture.truncated,
        stderr_truncated: stderr_capture.truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_marks_output() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 2; echo done"]);
        let out = run_command(
            cmd,
            RunOptions {
                timeout: Some(Duration::from_millis(150)),
                ..RunOptions::default()
            },
        )
        .unwrap();
        assert!(out.timed_out);
    }

    #[test]
    fn stderr_is_bounded() {
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "python3 - <<'PY'\nimport sys\nsys.stderr.write('x'*10000)\nPY",
        ]);
        let out = run_command(
            cmd,
            RunOptions {
                stderr_limit: 256,
                ..RunOptions::default()
            },
        )
        .unwrap();
        assert!(out.stderr_tail.len() <= 256);
        assert!(out.status.success());
    }
}
