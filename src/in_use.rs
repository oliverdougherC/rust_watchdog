use crate::error::{Result, WatchdogError};
use crate::process::{run_command, RunOptions};
use crate::traits::{InUseDetector, InUseStatus};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// Command-based in-use detector (defaults to lsof).
pub struct CommandInUseDetector {
    command: String,
}

impl CommandInUseDetector {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

impl InUseDetector for CommandInUseDetector {
    fn check_in_use(&self, path: &Path) -> Result<InUseStatus> {
        let mut cmd = Command::new(&self.command);
        cmd.args(["-t", "--"]).arg(path);
        let output = run_command(
            cmd,
            RunOptions {
                timeout: Some(Duration::from_secs(5)),
                stderr_limit: 16 * 1024,
                ..RunOptions::default()
            },
        )
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                WatchdogError::MissingDependency(self.command.clone())
            } else {
                WatchdogError::InUse {
                    path: path.to_path_buf(),
                    reason: format!("{} execution failed: {}", self.command, e),
                }
            }
        })?;

        if output.timed_out {
            return Err(WatchdogError::InUse {
                path: path.to_path_buf(),
                reason: format!("{} timed out after 5s", self.command),
            });
        }

        // lsof exits 0 when it found open handles, 1 when none are found.
        match output.status.code() {
            Some(0) => Ok(InUseStatus::InUse),
            Some(1) => Ok(InUseStatus::NotInUse),
            _ => Err(WatchdogError::InUse {
                path: path.to_path_buf(),
                reason: format!(
                    "{} exited with status {:?}: {}",
                    self.command,
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr_tail).trim()
                ),
            }),
        }
    }
}

/// Simulated detector always reports not-in-use.
pub struct SimulatedInUseDetector;

impl InUseDetector for SimulatedInUseDetector {
    fn check_in_use(&self, _path: &Path) -> Result<InUseStatus> {
        Ok(InUseStatus::NotInUse)
    }
}
