use crate::config::{read_metrics_from_path, write_metrics_to_path, MetricsConfig};
use crate::state::StateManager;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CumulativeMetrics {
    pub space_saved_bytes: i64,
    pub transcoded_files: u64,
    pub inspected_files: u64,
    pub retries_scheduled: u64,
}

impl From<&MetricsConfig> for CumulativeMetrics {
    fn from(value: &MetricsConfig) -> Self {
        Self {
            space_saved_bytes: value.space_saved_bytes,
            transcoded_files: value.transcoded_files,
            inspected_files: value.inspected_files,
            retries_scheduled: value.retries_scheduled,
        }
    }
}

impl CumulativeMetrics {
    fn apply_delta(self, delta: CumulativeMetricsDelta) -> Self {
        Self {
            space_saved_bytes: self.space_saved_bytes + delta.space_saved_bytes,
            transcoded_files: self.transcoded_files + delta.transcoded_files,
            inspected_files: self.inspected_files + delta.inspected_files,
            retries_scheduled: self.retries_scheduled + delta.retries_scheduled,
        }
    }

    fn into_config(self, flush_interval_seconds: u64) -> MetricsConfig {
        MetricsConfig {
            space_saved_bytes: self.space_saved_bytes,
            transcoded_files: self.transcoded_files,
            inspected_files: self.inspected_files,
            retries_scheduled: self.retries_scheduled,
            flush_interval_seconds,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CumulativeMetricsDelta {
    pub space_saved_bytes: i64,
    pub transcoded_files: u64,
    pub inspected_files: u64,
    pub retries_scheduled: u64,
}

impl CumulativeMetricsDelta {
    fn is_zero(self) -> bool {
        self.space_saved_bytes == 0
            && self.transcoded_files == 0
            && self.inspected_files == 0
            && self.retries_scheduled == 0
    }
}

#[derive(Debug)]
struct MetricsPersistenceState {
    last_persisted: CumulativeMetrics,
    pending_delta: CumulativeMetricsDelta,
    last_flush_at: Instant,
    flush_interval: Duration,
}

#[derive(Debug)]
pub struct MetricsPersistence {
    config_path: PathBuf,
    state: Mutex<MetricsPersistenceState>,
}

impl MetricsPersistence {
    pub fn new(config_path: PathBuf, initial: CumulativeMetrics, flush_interval: Duration) -> Self {
        Self {
            config_path,
            state: Mutex::new(MetricsPersistenceState {
                last_persisted: initial,
                pending_delta: CumulativeMetricsDelta::default(),
                last_flush_at: Instant::now(),
                flush_interval,
            }),
        }
    }

    pub fn record_delta(&self, delta: CumulativeMetricsDelta) {
        if delta.is_zero() {
            return;
        }

        let mut state = self.state.lock().unwrap();
        state.pending_delta.space_saved_bytes += delta.space_saved_bytes;
        state.pending_delta.transcoded_files += delta.transcoded_files;
        state.pending_delta.inspected_files += delta.inspected_files;
        state.pending_delta.retries_scheduled += delta.retries_scheduled;
    }

    pub fn maybe_flush(&self) -> Option<CumulativeMetrics> {
        self.flush(false)
    }

    pub fn flush_now(&self) -> Option<CumulativeMetrics> {
        self.flush(true)
    }

    fn flush(&self, force: bool) -> Option<CumulativeMetrics> {
        let mut state = self.state.lock().unwrap();
        if state.pending_delta.is_zero() {
            return None;
        }
        if !force && state.last_flush_at.elapsed() < state.flush_interval {
            return None;
        }

        let on_disk = match read_metrics_from_path(&self.config_path) {
            Ok(metrics) => metrics,
            Err(err) => {
                warn!(
                    "Failed to read persisted metrics from {}: {}",
                    self.config_path.display(),
                    err
                );
                return None;
            }
        };

        let flush_interval_seconds = on_disk.flush_interval_seconds.max(5);
        let new_totals = CumulativeMetrics::from(&on_disk).apply_delta(state.pending_delta);

        if let Err(err) = write_metrics_to_path(
            &self.config_path,
            &new_totals.into_config(flush_interval_seconds),
        ) {
            warn!(
                "Failed to write persisted metrics to {}: {}",
                self.config_path.display(),
                err
            );
            return None;
        }

        state.last_persisted = new_totals;
        state.pending_delta = CumulativeMetricsDelta::default();
        state.last_flush_at = Instant::now();
        state.flush_interval = Duration::from_secs(flush_interval_seconds);
        Some(new_totals)
    }
}

static GLOBAL_METRICS_PERSISTENCE: OnceLock<Option<Arc<MetricsPersistence>>> = OnceLock::new();

pub fn set_global_metrics_persistence(persistence: Option<Arc<MetricsPersistence>>) {
    let _ = GLOBAL_METRICS_PERSISTENCE.set(persistence);
}

pub fn maybe_flush_global_metrics_delta(
    delta: CumulativeMetricsDelta,
) -> Option<CumulativeMetrics> {
    let persistence = GLOBAL_METRICS_PERSISTENCE
        .get()
        .and_then(|persistence| persistence.as_ref())?;
    persistence.record_delta(delta);
    persistence.maybe_flush()
}

pub fn flush_global_metrics_now() -> Option<CumulativeMetrics> {
    let persistence = GLOBAL_METRICS_PERSISTENCE
        .get()
        .and_then(|persistence| persistence.as_ref())?;
    persistence.flush_now()
}

pub fn sync_cumulative_metrics_state(state: &StateManager, metrics: CumulativeMetrics) {
    state.update(|app_state| {
        app_state.total_space_saved = metrics.space_saved_bytes;
        app_state.total_transcoded = metrics.transcoded_files;
        app_state.total_inspected = metrics.inspected_files;
        app_state.total_retries_scheduled = metrics.retries_scheduled;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_rebases_on_disk_metrics_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("watchdog.toml");
        std::fs::write(
            &config_path,
            r#"local_mode = true

[metrics]
space_saved_bytes = 10
transcoded_files = 2
inspected_files = 7
retries_scheduled = 1
flush_interval_seconds = 60
"#,
        )
        .unwrap();

        let persistence = MetricsPersistence::new(
            config_path.clone(),
            CumulativeMetrics {
                space_saved_bytes: 10,
                transcoded_files: 2,
                inspected_files: 7,
                retries_scheduled: 1,
            },
            Duration::ZERO,
        );

        persistence.record_delta(CumulativeMetricsDelta {
            space_saved_bytes: 5,
            transcoded_files: 1,
            inspected_files: 3,
            retries_scheduled: 1,
        });

        write_metrics_to_path(
            &config_path,
            &MetricsConfig {
                space_saved_bytes: 1,
                transcoded_files: 9,
                inspected_files: 20,
                retries_scheduled: 4,
                flush_interval_seconds: 30,
            },
        )
        .unwrap();

        let flushed = persistence.flush_now().unwrap();
        assert_eq!(
            flushed,
            CumulativeMetrics {
                space_saved_bytes: 6,
                transcoded_files: 10,
                inspected_files: 23,
                retries_scheduled: 5,
            }
        );

        let on_disk = read_metrics_from_path(&config_path).unwrap();
        assert_eq!(on_disk.space_saved_bytes, 6);
        assert_eq!(on_disk.transcoded_files, 10);
        assert_eq!(on_disk.inspected_files, 23);
        assert_eq!(on_disk.retries_scheduled, 5);
        assert_eq!(on_disk.flush_interval_seconds, 30);
    }
}
