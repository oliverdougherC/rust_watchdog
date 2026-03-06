use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
}

fn push_with_limit(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) {
    if limit == 0 {
        return;
    }
    buf.extend_from_slice(chunk);
    if buf.len() > limit {
        let drop_len = buf.len() - limit;
        buf.drain(0..drop_len);
    }
}

fn spawn_capture_thread<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut out = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            match reader.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => push_with_limit(&mut out, &tmp[..n], limit),
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

/// Run a command with timeout/cancellation handling and bounded output capture.
pub fn run_command(mut cmd: Command, options: RunOptions) -> io::Result<RunOutput> {
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

    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr_tail = stderr_handle.join().unwrap_or_default();

    Ok(RunOutput {
        status,
        stdout,
        stderr_tail,
        timed_out,
        cancelled,
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
