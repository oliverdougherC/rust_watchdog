use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use tracing::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodeOutcome {
    Replaced,
    SkippedNoSavings,
    RetryScheduled,
    Failed,
}

impl TranscodeOutcome {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Replaced => "replaced",
            Self::SkippedNoSavings => "skipped_no_savings",
            Self::RetryScheduled => "retry_scheduled",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_value(value: &str) -> Self {
        match value {
            "replaced" => Self::Replaced,
            "skipped_no_savings" => Self::SkippedNoSavings,
            "retry_scheduled" => Self::RetryScheduled,
            _ => Self::Failed,
        }
    }

    pub fn is_successful(self) -> bool {
        matches!(self, Self::Replaced | Self::SkippedNoSavings)
    }
}

/// Row from the transcode_history table.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TranscodeRecord {
    pub id: i64,
    pub source_path: String,
    pub share_name: Option<String>,
    pub original_codec: Option<String>,
    pub original_bitrate_bps: Option<i64>,
    pub original_size: Option<i64>,
    pub output_size: Option<i64>,
    pub space_saved: Option<i64>,
    pub duration_seconds: Option<f64>,
    pub outcome: TranscodeOutcome,
    pub success: bool,
    pub failure_reason: Option<String>,
    pub failure_code: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FileFailureState {
    pub file_path: String,
    pub consecutive_failures: u32,
    pub last_failure_reason: Option<String>,
    pub last_failure_code: Option<String>,
    pub next_eligible_at: i64,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FileReadinessState {
    pub file_path: String,
    pub last_seen_size: u64,
    pub last_seen_mtime: f64,
    pub first_stable_at: i64,
    pub stable_observations: u32,
    pub last_seen_at: i64,
}

#[derive(Debug, Clone)]
pub struct CooldownRecord {
    pub file_path: String,
    pub consecutive_failures: u32,
    pub last_failure_reason: Option<String>,
    pub last_failure_code: Option<String>,
    pub next_eligible_at: i64,
}

#[derive(Debug, Clone)]
pub struct LatestFailureRecord {
    pub source_path: String,
    pub failure_reason: Option<String>,
    pub failure_code: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServiceState {
    pub consecutive_pass_failures: u32,
    pub last_pass_failure_code: Option<String>,
    pub auto_paused_at: Option<String>,
    pub auto_pause_reason: Option<String>,
    pub worker_pid: Option<u32>,
    pub worker_run_mode: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueueRecord {
    pub id: i64,
    pub source_path: String,
    pub share_name: String,
    pub enqueue_source: String,
    pub preset_file: Option<String>,
    pub preset_name: Option<String>,
    pub target_codec: Option<String>,
    pub preset_payload_json: Option<String>,
    pub order_key: i64,
    pub queued_at: String,
    pub started_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewQueueItem {
    pub source_path: String,
    pub share_name: String,
    pub enqueue_source: String,
    pub preset_file: String,
    pub preset_name: String,
    pub target_codec: String,
    pub preset_payload_json: String,
}

#[derive(Debug, Clone)]
pub struct QuarantineRecord {
    pub file_path: String,
    pub quarantined_at: String,
    pub last_failure_code: Option<String>,
    pub reason: Option<String>,
    pub manual_clear_required: bool,
}

/// Thread-safe SQLite database layer.
/// Schema is identical to the Python version for migration compatibility.
pub struct WatchdogDb {
    conn: Mutex<Connection>,
}

impl WatchdogDb {
    /// Acquire the database lock, recovering from poisoned mutex if a previous
    /// holder panicked. This prevents cascading panics across the application.
    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|poisoned| {
            error!("DB mutex was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// Open (or create) the database at the given path.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init()?;
        Ok(db)
    }

    /// Open an in-memory database (for simulation/testing).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init()?;
        Ok(db)
    }

    /// Open an existing database in read-only mode.
    /// This is used for strict dry-run execution paths that must not mutate state.
    pub fn open_read_only(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn default_service_state() -> ServiceState {
        ServiceState {
            consecutive_pass_failures: 0,
            last_pass_failure_code: None,
            auto_paused_at: None,
            auto_pause_reason: None,
            worker_pid: None,
            worker_run_mode: None,
        }
    }

    fn read_service_state(conn: &Connection) -> rusqlite::Result<ServiceState> {
        conn.query_row(
            "SELECT consecutive_pass_failures, last_pass_failure_code, auto_paused_at, auto_pause_reason,
                    worker_pid, worker_run_mode
             FROM service_state WHERE id = 1",
            [],
            |row| {
                Ok(ServiceState {
                    consecutive_pass_failures: row.get::<_, i64>(0)?.max(0) as u32,
                    last_pass_failure_code: row.get(1)?,
                    auto_paused_at: row.get(2)?,
                    auto_pause_reason: row.get(3)?,
                    worker_pid: row
                        .get::<_, Option<i64>>(4)?
                        .map(|value| value.max(0) as u32),
                    worker_run_mode: row.get(5)?,
                })
            },
        )
    }

    fn init(&self) -> rusqlite::Result<()> {
        let conn = self.lock();
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn.execute_batch("PRAGMA busy_timeout=5000;")?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS transcode_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL,
                share_name TEXT,
                original_codec TEXT,
                original_bitrate_bps INTEGER,
                original_size INTEGER,
                output_size INTEGER,
                space_saved INTEGER,
                duration_seconds REAL,
                outcome TEXT NOT NULL DEFAULT 'failed',
                success INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT,
                failure_code TEXT,
                started_at TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS inspected_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_size INTEGER,
                file_mtime REAL,
                inspected_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS space_saved_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cumulative_bytes_saved INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_failure_state (
                file_path TEXT PRIMARY KEY,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_failure_reason TEXT,
                last_failure_code TEXT,
                next_eligible_at INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS file_readiness_state (
                file_path TEXT PRIMARY KEY,
                last_seen_size INTEGER NOT NULL,
                last_seen_mtime REAL NOT NULL,
                first_stable_at INTEGER NOT NULL,
                stable_observations INTEGER NOT NULL DEFAULT 1,
                last_seen_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_quarantine (
                file_path TEXT PRIMARY KEY,
                quarantined_at TEXT NOT NULL,
                last_failure_code TEXT,
                reason TEXT,
                manual_clear_required INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS service_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                consecutive_pass_failures INTEGER NOT NULL DEFAULT 0,
                last_pass_failure_code TEXT,
                auto_paused_at TEXT,
                auto_pause_reason TEXT,
                worker_pid INTEGER,
                worker_run_mode TEXT
            );

            CREATE TABLE IF NOT EXISTS queue_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL UNIQUE,
                share_name TEXT NOT NULL,
                enqueue_source TEXT NOT NULL,
                preset_file TEXT,
                preset_name TEXT,
                target_codec TEXT,
                preset_payload_json TEXT,
                order_key INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                started_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_inspected_files_file_path
                ON inspected_files (file_path);

            CREATE INDEX IF NOT EXISTS idx_transcode_history_source_path
                ON transcode_history (source_path);

            CREATE INDEX IF NOT EXISTS idx_file_failure_state_next_eligible_at
                ON file_failure_state (next_eligible_at);

            CREATE INDEX IF NOT EXISTS idx_file_readiness_state_last_seen_at
                ON file_readiness_state (last_seen_at);

            CREATE INDEX IF NOT EXISTS idx_file_quarantine_quarantined_at
                ON file_quarantine (quarantined_at DESC);

            CREATE INDEX IF NOT EXISTS idx_queue_items_order_key
                ON queue_items (order_key);
        ",
        )?;

        // Non-breaking migration for existing databases.
        let outcome_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transcode_history') WHERE name = 'outcome'",
            [],
            |row| row.get(0),
        )?;

        if outcome_col_exists == 0 {
            conn.execute("ALTER TABLE transcode_history ADD COLUMN outcome TEXT", [])?;
        }

        let failure_code_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transcode_history') WHERE name = 'failure_code'",
            [],
            |row| row.get(0),
        )?;
        if failure_code_col_exists == 0 {
            conn.execute(
                "ALTER TABLE transcode_history ADD COLUMN failure_code TEXT",
                [],
            )?;
        }

        let failure_state_code_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('file_failure_state') WHERE name = 'last_failure_code'",
            [],
            |row| row.get(0),
        )?;
        if failure_state_code_col_exists == 0 {
            conn.execute(
                "ALTER TABLE file_failure_state ADD COLUMN last_failure_code TEXT",
                [],
            )?;
        }

        let worker_pid_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('service_state') WHERE name = 'worker_pid'",
            [],
            |row| row.get(0),
        )?;
        if worker_pid_col_exists == 0 {
            conn.execute(
                "ALTER TABLE service_state ADD COLUMN worker_pid INTEGER",
                [],
            )?;
        }

        let worker_run_mode_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('service_state') WHERE name = 'worker_run_mode'",
            [],
            |row| row.get(0),
        )?;
        if worker_run_mode_col_exists == 0 {
            conn.execute(
                "ALTER TABLE service_state ADD COLUMN worker_run_mode TEXT",
                [],
            )?;
        }

        let queue_preset_file_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('queue_items') WHERE name = 'preset_file'",
            [],
            |row| row.get(0),
        )?;
        if queue_preset_file_col_exists == 0 {
            conn.execute("ALTER TABLE queue_items ADD COLUMN preset_file TEXT", [])?;
        }

        let queue_preset_name_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('queue_items') WHERE name = 'preset_name'",
            [],
            |row| row.get(0),
        )?;
        if queue_preset_name_col_exists == 0 {
            conn.execute("ALTER TABLE queue_items ADD COLUMN preset_name TEXT", [])?;
        }

        let queue_target_codec_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('queue_items') WHERE name = 'target_codec'",
            [],
            |row| row.get(0),
        )?;
        if queue_target_codec_col_exists == 0 {
            conn.execute("ALTER TABLE queue_items ADD COLUMN target_codec TEXT", [])?;
        }

        let queue_preset_payload_col_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('queue_items') WHERE name = 'preset_payload_json'",
            [],
            |row| row.get(0),
        )?;
        if queue_preset_payload_col_exists == 0 {
            conn.execute(
                "ALTER TABLE queue_items ADD COLUMN preset_payload_json TEXT",
                [],
            )?;
        }

        conn.execute(
            "INSERT INTO service_state (id, consecutive_pass_failures)
             VALUES (1, 0)
             ON CONFLICT(id) DO NOTHING",
            [],
        )?;

        conn.execute(
            "UPDATE transcode_history
             SET outcome = CASE
                 WHEN success = 1 AND COALESCE(space_saved, 0) = 0 THEN 'skipped_no_savings'
                 WHEN success = 1 THEN 'replaced'
                 ELSE 'failed'
             END
             WHERE outcome IS NULL OR outcome = ''",
            [],
        )?;

        conn.execute(
            "UPDATE transcode_history
             SET failure_code = CASE
                 WHEN failure_reason IS NULL OR failure_reason = '' THEN NULL
                 WHEN failure_reason IN ('interrupted_by_shutdown') THEN 'interrupted_shutdown'
                 WHEN failure_reason IN ('interrupted_by_pause') THEN 'interrupted_pause'
                 WHEN failure_reason IN ('timeout (requeued)', 'timeout_requeued') THEN 'timeout_requeued'
                 WHEN failure_reason IN ('timeout_exhausted') OR failure_reason LIKE 'timeout%' THEN 'timeout_exhausted'
                 WHEN failure_reason IN ('stalled (requeued)', 'stalled_requeued') THEN 'stalled_requeued'
                 WHEN failure_reason IN ('stalled_exhausted') OR failure_reason LIKE 'stalled%' THEN 'stalled_exhausted'
                 WHEN failure_reason IN ('transcode_failed') THEN 'transcode_failed'
                 WHEN failure_reason IN ('transcode_error') THEN 'transcode_error'
                 WHEN failure_reason IN ('verification_failed') THEN 'verification_failed'
                 WHEN failure_reason IN ('verification_error') THEN 'verification_error'
                 WHEN failure_reason IN ('output_missing') THEN 'output_missing'
                 WHEN failure_reason IN ('output_zero_bytes') THEN 'output_zero_bytes'
                 WHEN failure_reason IN ('output_suspiciously_small') THEN 'output_suspiciously_small'
                 WHEN failure_reason IN ('transfer_failed') THEN 'transfer_failed'
                 WHEN failure_reason IN ('transfer_error') THEN 'transfer_error'
                 WHEN failure_reason IN ('rsync failed (requeued)', 'transfer_failed_requeued') THEN 'transfer_failed_requeued'
                 WHEN failure_reason IN ('rsync error (requeued)', 'transfer_error_requeued') THEN 'transfer_error_requeued'
                 WHEN failure_reason IN ('insufficient_temp_space') THEN 'insufficient_temp_space'
                 WHEN failure_reason IN ('temp_space_probe_error') THEN 'temp_space_probe_error'
                 WHEN failure_reason IN ('insufficient_nfs_space') THEN 'insufficient_nfs_space'
                 WHEN failure_reason IN ('nfs_space_probe_error') THEN 'nfs_space_probe_error'
                 WHEN failure_reason IN ('source_changed_during_transcode') THEN 'source_changed_during_transcode'
                 WHEN failure_reason IN ('safe_replace_failed') THEN 'safe_replace_failed'
                 WHEN failure_reason IN ('safe_replace_error') THEN 'safe_replace_error'
                 WHEN failure_reason IN ('file_in_use') THEN 'file_in_use'
                 WHEN failure_reason IN ('scan_timeout') THEN 'scan_timeout'
                 WHEN failure_reason IN ('auto_paused_safety_trip') THEN 'auto_paused_safety_trip'
                 WHEN failure_reason IN ('quarantined_file') THEN 'quarantined_file'
                 WHEN failure_reason IN ('nfs_mount_failed') THEN 'nfs_mount_failed'
                 WHEN failure_reason IN ('no_media_directories') THEN 'no_media_directories'
                 WHEN failure_reason IN ('pass_error') THEN 'pass_error'
                 ELSE 'other'
             END
             WHERE failure_code IS NULL AND failure_reason IS NOT NULL AND failure_reason <> ''",
            [],
        )?;

        conn.execute(
            "UPDATE file_failure_state
             SET last_failure_code = CASE
                 WHEN last_failure_reason IS NULL OR last_failure_reason = '' THEN NULL
                 WHEN last_failure_reason IN ('interrupted_by_shutdown') THEN 'interrupted_shutdown'
                 WHEN last_failure_reason IN ('interrupted_by_pause') THEN 'interrupted_pause'
                 WHEN last_failure_reason IN ('timeout (requeued)', 'timeout_requeued') THEN 'timeout_requeued'
                 WHEN last_failure_reason IN ('timeout_exhausted') OR last_failure_reason LIKE 'timeout%' THEN 'timeout_exhausted'
                 WHEN last_failure_reason IN ('stalled (requeued)', 'stalled_requeued') THEN 'stalled_requeued'
                 WHEN last_failure_reason IN ('stalled_exhausted') OR last_failure_reason LIKE 'stalled%' THEN 'stalled_exhausted'
                 WHEN last_failure_reason IN ('transcode_failed') THEN 'transcode_failed'
                 WHEN last_failure_reason IN ('transcode_error') THEN 'transcode_error'
                 WHEN last_failure_reason IN ('verification_failed') THEN 'verification_failed'
                 WHEN last_failure_reason IN ('verification_error') THEN 'verification_error'
                 WHEN last_failure_reason IN ('output_missing') THEN 'output_missing'
                 WHEN last_failure_reason IN ('output_zero_bytes') THEN 'output_zero_bytes'
                 WHEN last_failure_reason IN ('output_suspiciously_small') THEN 'output_suspiciously_small'
                 WHEN last_failure_reason IN ('transfer_failed') THEN 'transfer_failed'
                 WHEN last_failure_reason IN ('transfer_error') THEN 'transfer_error'
                 WHEN last_failure_reason IN ('rsync failed (requeued)', 'transfer_failed_requeued') THEN 'transfer_failed_requeued'
                 WHEN last_failure_reason IN ('rsync error (requeued)', 'transfer_error_requeued') THEN 'transfer_error_requeued'
                 WHEN last_failure_reason IN ('insufficient_temp_space') THEN 'insufficient_temp_space'
                 WHEN last_failure_reason IN ('temp_space_probe_error') THEN 'temp_space_probe_error'
                 WHEN last_failure_reason IN ('insufficient_nfs_space') THEN 'insufficient_nfs_space'
                 WHEN last_failure_reason IN ('nfs_space_probe_error') THEN 'nfs_space_probe_error'
                 WHEN last_failure_reason IN ('source_changed_during_transcode') THEN 'source_changed_during_transcode'
                 WHEN last_failure_reason IN ('safe_replace_failed') THEN 'safe_replace_failed'
                 WHEN last_failure_reason IN ('safe_replace_error') THEN 'safe_replace_error'
                 WHEN last_failure_reason IN ('file_in_use') THEN 'file_in_use'
                 WHEN last_failure_reason IN ('scan_timeout') THEN 'scan_timeout'
                 WHEN last_failure_reason IN ('auto_paused_safety_trip') THEN 'auto_paused_safety_trip'
                 WHEN last_failure_reason IN ('quarantined_file') THEN 'quarantined_file'
                 WHEN last_failure_reason IN ('nfs_mount_failed') THEN 'nfs_mount_failed'
                 WHEN last_failure_reason IN ('no_media_directories') THEN 'no_media_directories'
                 WHEN last_failure_reason IN ('pass_error') THEN 'pass_error'
                 ELSE 'other'
             END
             WHERE last_failure_code IS NULL AND last_failure_reason IS NOT NULL AND last_failure_reason <> ''",
            [],
        )?;
        Ok(())
    }

    /// Return file failure-state for cooldown logic.
    pub fn get_file_failure_state(&self, path: &str) -> Option<FileFailureState> {
        let conn = self.lock();
        conn.query_row(
            "SELECT file_path, consecutive_failures, last_failure_reason, last_failure_code, next_eligible_at, updated_at
             FROM file_failure_state WHERE file_path = ?1",
            params![path],
            |row| {
                Ok(FileFailureState {
                    file_path: row.get(0)?,
                    consecutive_failures: row.get::<_, i64>(1)?.max(0) as u32,
                    last_failure_reason: row.get(2)?,
                    last_failure_code: row.get(3)?,
                    next_eligible_at: row.get::<_, i64>(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )
        .optional()
        .unwrap_or(None)
    }

    pub fn get_file_readiness_state(&self, path: &str) -> Option<FileReadinessState> {
        let conn = self.lock();
        conn.query_row(
            "SELECT file_path, last_seen_size, last_seen_mtime, first_stable_at, stable_observations, last_seen_at
             FROM file_readiness_state WHERE file_path = ?1",
            params![path],
            |row| {
                Ok(FileReadinessState {
                    file_path: row.get(0)?,
                    last_seen_size: row.get::<_, i64>(1)?.max(0) as u64,
                    last_seen_mtime: row.get(2)?,
                    first_stable_at: row.get(3)?,
                    stable_observations: row.get::<_, i64>(4)?.max(0) as u32,
                    last_seen_at: row.get(5)?,
                })
            },
        )
        .optional()
        .unwrap_or(None)
    }

    pub fn record_file_readiness_observation(
        &self,
        path: &str,
        size: u64,
        mtime: f64,
        now_ts: i64,
    ) -> Option<FileReadinessState> {
        let conn = self.lock();
        let previous = conn
            .query_row(
                "SELECT file_path, last_seen_size, last_seen_mtime, first_stable_at, stable_observations, last_seen_at
                 FROM file_readiness_state WHERE file_path = ?1",
                params![path],
                |row| {
                    Ok(FileReadinessState {
                        file_path: row.get(0)?,
                        last_seen_size: row.get::<_, i64>(1)?.max(0) as u64,
                        last_seen_mtime: row.get(2)?,
                        first_stable_at: row.get(3)?,
                        stable_observations: row.get::<_, i64>(4)?.max(0) as u32,
                        last_seen_at: row.get(5)?,
                    })
                },
            )
            .optional()
            .unwrap_or(None);
        let (first_stable_at, stable_observations) = match previous.as_ref() {
            Some(previous)
                if previous.last_seen_size == size
                    && (previous.last_seen_mtime - mtime).abs() < 1.0 =>
            {
                (
                    previous.first_stable_at.max(0),
                    previous.stable_observations.saturating_add(1),
                )
            }
            _ => (now_ts, 1),
        };
        if let Err(e) = conn.execute(
            "INSERT INTO file_readiness_state
             (file_path, last_seen_size, last_seen_mtime, first_stable_at, stable_observations, last_seen_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(file_path) DO UPDATE SET
                 last_seen_size=excluded.last_seen_size,
                 last_seen_mtime=excluded.last_seen_mtime,
                 first_stable_at=excluded.first_stable_at,
                 stable_observations=excluded.stable_observations,
                 last_seen_at=excluded.last_seen_at",
            params![
                path,
                size as i64,
                mtime,
                first_stable_at,
                stable_observations as i64,
                now_ts,
            ],
        ) {
            error!("DB error in record_file_readiness_observation('{}'): {}", path, e);
            return None;
        }
        Some(FileReadinessState {
            file_path: path.to_string(),
            last_seen_size: size,
            last_seen_mtime: mtime,
            first_stable_at,
            stable_observations,
            last_seen_at: now_ts,
        })
    }

    pub fn clear_file_readiness_state(&self, path: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "DELETE FROM file_readiness_state WHERE file_path = ?1",
            params![path],
        ) {
            error!("DB error in clear_file_readiness_state('{}'): {}", path, e);
        }
    }

    /// Clear file failure-state after successful replacement.
    pub fn clear_file_failure_state(&self, path: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "DELETE FROM file_failure_state WHERE file_path = ?1",
            params![path],
        ) {
            error!("DB error in clear_file_failure_state: {}", e);
        }
    }

    /// Record a file failure and compute cooldown next-eligible timestamp.
    pub fn record_file_failure(
        &self,
        path: &str,
        failure_reason: &str,
        failure_code: &str,
        max_failures_before_cooldown: u32,
        cooldown_base_seconds: u64,
        cooldown_max_seconds: u64,
    ) -> Option<FileFailureState> {
        let conn = self.lock();
        let current: Option<(i64, i64)> = conn
            .query_row(
                "SELECT consecutive_failures, next_eligible_at FROM file_failure_state WHERE file_path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .unwrap_or(None);

        let now_ts = Utc::now().timestamp();
        let current_failures = current.map(|v| v.0.max(0) as u32).unwrap_or(0);
        let consecutive = current_failures.saturating_add(1);

        let next_eligible_at = if consecutive >= max_failures_before_cooldown.max(1) {
            let exponent = consecutive.saturating_sub(max_failures_before_cooldown.max(1));
            let mut cooldown = cooldown_base_seconds.max(1);
            for _ in 0..exponent {
                cooldown = cooldown.saturating_mul(2);
                if cooldown >= cooldown_max_seconds.max(cooldown_base_seconds.max(1)) {
                    cooldown = cooldown_max_seconds.max(cooldown_base_seconds.max(1));
                    break;
                }
            }
            now_ts.saturating_add(cooldown as i64)
        } else {
            now_ts
        };

        let updated_at = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO file_failure_state (file_path, consecutive_failures, last_failure_reason, last_failure_code, next_eligible_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(file_path) DO UPDATE SET
                 consecutive_failures=excluded.consecutive_failures,
                 last_failure_reason=excluded.last_failure_reason,
                 last_failure_code=excluded.last_failure_code,
                 next_eligible_at=excluded.next_eligible_at,
                 updated_at=excluded.updated_at",
            params![
                path,
                consecutive as i64,
                failure_reason,
                failure_code,
                next_eligible_at,
                updated_at
            ],
        ) {
            error!("DB error in record_file_failure: {}", e);
            return None;
        }

        Some(FileFailureState {
            file_path: path.to_string(),
            consecutive_failures: consecutive,
            last_failure_reason: Some(failure_reason.to_string()),
            last_failure_code: Some(failure_code.to_string()),
            next_eligible_at,
            updated_at: Some(updated_at),
        })
    }

    /// Number of files currently in cooldown.
    pub fn get_cooldown_active_count(&self, now_ts: i64) -> i64 {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM file_failure_state WHERE next_eligible_at > ?1",
            params![now_ts],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// Top recent failure reasons from file failure-state.
    pub fn get_top_failure_reasons(&self, limit: u32) -> Vec<(String, i64)> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT last_failure_reason, COUNT(*) AS cnt
             FROM file_failure_state
             WHERE last_failure_reason IS NOT NULL AND last_failure_reason <> ''
             GROUP BY last_failure_reason
             ORDER BY cnt DESC
             LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("DB error in get_top_failure_reasons: {}", e);
                return Vec::new();
            }
        };

        let rows = match stmt.query_map(params![limit], |row| {
            let reason: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((reason, count))
        }) {
            Ok(r) => r,
            Err(e) => {
                error!("DB error in get_top_failure_reasons query: {}", e);
                return Vec::new();
            }
        };

        rows.filter_map(|r| r.ok()).collect()
    }

    /// List files currently in cooldown.
    pub fn get_cooldown_files(&self, now_ts: i64, limit: u32) -> Vec<CooldownRecord> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT file_path, consecutive_failures, last_failure_reason, last_failure_code, next_eligible_at
             FROM file_failure_state
             WHERE next_eligible_at > ?1
             ORDER BY next_eligible_at ASC
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("DB error in get_cooldown_files: {}", e);
                return Vec::new();
            }
        };

        let rows = match stmt.query_map(params![now_ts, limit], |row| {
            Ok(CooldownRecord {
                file_path: row.get(0)?,
                consecutive_failures: row.get::<_, i64>(1)?.max(0) as u32,
                last_failure_reason: row.get(2)?,
                last_failure_code: row.get(3)?,
                next_eligible_at: row.get(4)?,
            })
        }) {
            Ok(r) => r,
            Err(e) => {
                error!("DB error in get_cooldown_files query: {}", e);
                return Vec::new();
            }
        };
        rows.filter_map(|r| r.ok()).collect()
    }

    /// Latest failed transcode record.
    pub fn get_latest_failure(&self) -> Option<LatestFailureRecord> {
        let conn = self.lock();
        conn.query_row(
            "SELECT source_path, failure_reason, failure_code, completed_at
             FROM transcode_history
             WHERE outcome = 'failed'
               AND completed_at IS NOT NULL
             ORDER BY id DESC
             LIMIT 1",
            [],
            |row| {
                Ok(LatestFailureRecord {
                    source_path: row.get(0)?,
                    failure_reason: row.get(1)?,
                    failure_code: row.get(2)?,
                    completed_at: row.get(3)?,
                })
            },
        )
        .optional()
        .unwrap_or(None)
    }

    /// Current service-level safety state for tripwire enforcement.
    pub fn get_service_state(&self) -> ServiceState {
        let conn = self.lock();
        Self::read_service_state(&conn).unwrap_or_else(|_| Self::default_service_state())
    }

    /// Increment pass-failure streak and capture failure code.
    pub fn note_pass_failure(&self, failure_code: &str) -> ServiceState {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "INSERT INTO service_state (id, consecutive_pass_failures, last_pass_failure_code)
             VALUES (1, 0, NULL)
             ON CONFLICT(id) DO NOTHING",
            [],
        ) {
            error!(
                "DB error ensuring service_state in note_pass_failure: {}",
                e
            );
            return Self::read_service_state(&conn)
                .unwrap_or_else(|_| Self::default_service_state());
        }
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET consecutive_pass_failures = consecutive_pass_failures + 1,
                 last_pass_failure_code = ?1
             WHERE id = 1",
            params![failure_code],
        ) {
            error!("DB error in note_pass_failure update: {}", e);
        }
        Self::read_service_state(&conn).unwrap_or_else(|_| Self::default_service_state())
    }

    /// Reset pass-failure streak after a successful pass.
    pub fn note_pass_success(&self) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET consecutive_pass_failures = 0,
                 last_pass_failure_code = NULL
             WHERE id = 1",
            [],
        ) {
            error!("DB error in note_pass_success: {}", e);
        }
    }

    /// Persist auto-pause metadata for operator diagnostics.
    pub fn mark_auto_paused(&self, reason: &str) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET auto_paused_at = ?1,
                 auto_pause_reason = ?2
             WHERE id = 1",
            params![now, reason],
        ) {
            error!("DB error in mark_auto_paused: {}", e);
        }
    }

    /// Clear persisted auto-pause metadata when operator resumes.
    pub fn clear_auto_paused(&self) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET auto_paused_at = NULL,
                 auto_pause_reason = NULL
             WHERE id = 1",
            [],
        ) {
            error!("DB error in clear_auto_paused: {}", e);
        }
    }

    pub fn set_worker_state(&self, pid: u32, run_mode: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET worker_pid = ?1,
                 worker_run_mode = ?2
             WHERE id = 1",
            params![pid as i64, run_mode],
        ) {
            error!("DB error in set_worker_state: {}", e);
        }
    }

    pub fn clear_worker_state(&self) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE service_state
             SET worker_pid = NULL,
                 worker_run_mode = NULL
             WHERE id = 1",
            [],
        ) {
            error!("DB error in clear_worker_state: {}", e);
        }
    }

    /// Quarantine a file after repeated hard failures.
    pub fn quarantine_file(&self, path: &str, last_failure_code: &str, reason: &str) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO file_quarantine (file_path, quarantined_at, last_failure_code, reason, manual_clear_required)
             VALUES (?1, ?2, ?3, ?4, 1)
             ON CONFLICT(file_path) DO UPDATE SET
                 quarantined_at=excluded.quarantined_at,
                 last_failure_code=excluded.last_failure_code,
                 reason=excluded.reason,
                 manual_clear_required=1",
            params![path, now, last_failure_code, reason],
        ) {
            error!("DB error in quarantine_file: {}", e);
        }
    }

    /// Return true when the file is currently quarantined.
    pub fn is_quarantined(&self, path: &str) -> bool {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM file_quarantine WHERE file_path = ?1",
            params![path],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .unwrap_or(false)
    }

    /// Number of currently quarantined files.
    pub fn quarantine_count(&self) -> i64 {
        let conn = self.lock();
        conn.query_row("SELECT COUNT(*) FROM file_quarantine", [], |row| row.get(0))
            .unwrap_or(0)
    }

    /// List quarantined files by most recent quarantine time.
    pub fn list_quarantined_files(&self, limit: u32) -> Vec<QuarantineRecord> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT file_path, quarantined_at, last_failure_code, reason, manual_clear_required
             FROM file_quarantine
             ORDER BY quarantined_at DESC
             LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("DB error in list_quarantined_files: {}", e);
                return Vec::new();
            }
        };
        let rows = match stmt.query_map(params![limit], |row| {
            Ok(QuarantineRecord {
                file_path: row.get(0)?,
                quarantined_at: row.get(1)?,
                last_failure_code: row.get(2)?,
                reason: row.get(3)?,
                manual_clear_required: row.get::<_, i64>(4)? != 0,
            })
        }) {
            Ok(rows) => rows,
            Err(e) => {
                error!("DB error in list_quarantined_files query: {}", e);
                return Vec::new();
            }
        };
        rows.filter_map(|row| row.ok()).collect()
    }

    /// Clear one quarantined file entry.
    pub fn clear_quarantine_file(&self, path: &str) -> bool {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM file_quarantine WHERE file_path = ?1",
            params![path],
        )
        .map(|rows| rows > 0)
        .unwrap_or(false)
    }

    /// Clear all quarantined file entries.
    pub fn clear_all_quarantine_files(&self) -> usize {
        let conn = self.lock();
        conn.execute("DELETE FROM file_quarantine", []).unwrap_or(0)
    }

    pub fn get_queue_count(&self) -> i64 {
        let conn = self.lock();
        conn.query_row("SELECT COUNT(*) FROM queue_items", [], |row| row.get(0))
            .unwrap_or(0)
    }

    pub fn next_queue_order_key(&self) -> i64 {
        let conn = self.lock();
        conn.query_row(
            "SELECT COALESCE(MAX(order_key), 0) + 1 FROM queue_items",
            [],
            |row| row.get(0),
        )
        .unwrap_or(1)
    }

    pub fn enqueue_queue_items(&self, items: &[NewQueueItem]) -> usize {
        if items.is_empty() {
            return 0;
        }

        let mut conn = self.lock();
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(e) => {
                error!("DB error opening queue transaction: {}", e);
                return 0;
            }
        };
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let mut next_order_key: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(order_key), 0) + 1 FROM queue_items",
                [],
                |row| row.get(0),
            )
            .unwrap_or(1);
        let mut inserted = 0usize;

        for item in items {
            match tx.execute(
                "INSERT OR IGNORE INTO queue_items
                 (source_path, share_name, enqueue_source, preset_file, preset_name, target_codec, preset_payload_json, order_key, queued_at, started_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
                params![
                    item.source_path,
                    item.share_name,
                    item.enqueue_source,
                    item.preset_file,
                    item.preset_name,
                    item.target_codec,
                    item.preset_payload_json,
                    next_order_key,
                    now,
                ],
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        inserted += 1;
                        next_order_key += 1;
                    }
                }
                Err(e) => error!("DB error enqueuing '{}': {}", item.source_path, e),
            }
        }

        if let Err(e) = tx.commit() {
            error!("DB error committing queue transaction: {}", e);
            return 0;
        }

        inserted
    }

    pub fn list_queue_items(&self, limit: u32) -> Vec<QueueRecord> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT id, source_path, share_name, enqueue_source, preset_file, preset_name, target_codec, preset_payload_json, order_key, queued_at, started_at
             FROM queue_items
             ORDER BY CASE WHEN started_at IS NOT NULL THEN 0 ELSE 1 END,
                      order_key ASC
             LIMIT ?1",
        ) {
            Ok(stmt) => stmt,
            Err(e) => {
                error!("DB error in list_queue_items: {}", e);
                return Vec::new();
            }
        };

        let rows = match stmt.query_map(params![limit], |row| {
            Ok(QueueRecord {
                id: row.get(0)?,
                source_path: row.get(1)?,
                share_name: row.get(2)?,
                enqueue_source: row.get(3)?,
                preset_file: row.get(4)?,
                preset_name: row.get(5)?,
                target_codec: row.get(6)?,
                preset_payload_json: row.get(7)?,
                order_key: row.get(8)?,
                queued_at: row.get(9)?,
                started_at: row.get(10)?,
            })
        }) {
            Ok(rows) => rows,
            Err(e) => {
                error!("DB error querying queue items: {}", e);
                return Vec::new();
            }
        };

        rows.filter_map(|row| row.ok()).collect()
    }

    pub fn claim_next_queue_item(&self) -> Option<QueueRecord> {
        let mut conn = self.lock();
        let tx = conn.transaction().ok()?;
        let next = tx
            .query_row(
                "SELECT id, source_path, share_name, enqueue_source, preset_file, preset_name, target_codec, preset_payload_json, order_key, queued_at, started_at
                 FROM queue_items
                 WHERE started_at IS NULL
                 ORDER BY order_key ASC
                 LIMIT 1",
                [],
                |row| {
                    Ok(QueueRecord {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        share_name: row.get(2)?,
                        enqueue_source: row.get(3)?,
                        preset_file: row.get(4)?,
                        preset_name: row.get(5)?,
                        target_codec: row.get(6)?,
                        preset_payload_json: row.get(7)?,
                        order_key: row.get(8)?,
                        queued_at: row.get(9)?,
                        started_at: row.get(10)?,
                    })
                },
            )
            .optional()
            .ok()??;

        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if tx
            .execute(
                "UPDATE queue_items SET started_at = ?1 WHERE id = ?2",
                params![now, next.id],
            )
            .is_err()
        {
            return None;
        }

        if tx.commit().is_err() {
            return None;
        }

        Some(QueueRecord {
            started_at: Some(now),
            ..next
        })
    }

    pub fn requeue_queue_item(&self, source_path: &str) {
        let conn = self.lock();
        let next_order_key = conn
            .query_row(
                "SELECT COALESCE(MAX(order_key), 0) + 1 FROM queue_items",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(1);
        if let Err(e) = conn.execute(
            "UPDATE queue_items
             SET order_key = ?1,
                 started_at = NULL
             WHERE source_path = ?2",
            params![next_order_key, source_path],
        ) {
            error!("DB error in requeue_queue_item('{}'): {}", source_path, e);
        }
    }

    pub fn defer_queue_item(&self, source_path: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE queue_items SET started_at = NULL WHERE source_path = ?1",
            params![source_path],
        ) {
            error!("DB error in defer_queue_item('{}'): {}", source_path, e);
        }
    }

    pub fn mark_queue_item_started(&self, source_path: &str) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "UPDATE queue_items SET started_at = ?1 WHERE source_path = ?2",
            params![now, source_path],
        ) {
            error!(
                "DB error in mark_queue_item_started('{}'): {}",
                source_path, e
            );
        }
    }

    pub fn remove_queue_item(&self, source_path: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "DELETE FROM queue_items WHERE source_path = ?1",
            params![source_path],
        ) {
            error!("DB error in remove_queue_item('{}'): {}", source_path, e);
        }
    }

    pub fn remove_pending_queue_item(&self, source_path: &str) -> bool {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM queue_items WHERE source_path = ?1 AND started_at IS NULL",
            params![source_path],
        )
        .map(|rows| rows > 0)
        .unwrap_or(false)
    }

    pub fn queue_item_exists(&self, source_path: &str) -> bool {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM queue_items WHERE source_path = ?1",
            params![source_path],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .unwrap_or(false)
    }

    pub fn update_queue_item_preset_payload(&self, source_path: &str, preset_payload_json: &str) {
        let conn = self.lock();
        if let Err(e) = conn.execute(
            "UPDATE queue_items
             SET preset_payload_json = ?1
             WHERE source_path = ?2",
            params![preset_payload_json, source_path],
        ) {
            error!(
                "DB error in update_queue_item_preset_payload('{}'): {}",
                source_path, e
            );
        }
    }

    pub fn clear_queue_items(&self) -> usize {
        let conn = self.lock();
        match conn.execute("DELETE FROM queue_items", []) {
            Ok(rows) => rows,
            Err(e) => {
                error!("DB error in clear_queue_items: {}", e);
                0
            }
        }
    }

    pub fn reset_stale_queue_items(&self) -> usize {
        let conn = self.lock();
        match conn.execute(
            "UPDATE queue_items SET started_at = NULL WHERE started_at IS NOT NULL",
            [],
        ) {
            Ok(rows) => rows,
            Err(e) => {
                error!("DB error in reset_stale_queue_items: {}", e);
                0
            }
        }
    }

    /// Check if a file was previously inspected with the same size and mtime.
    pub fn is_inspected(&self, path: &str, size: u64, mtime: f64) -> bool {
        let conn = self.lock();
        let result: Option<(i64, f64)> = conn
            .query_row(
                "SELECT file_size, file_mtime FROM inspected_files WHERE file_path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .unwrap_or(None);

        match result {
            Some((db_size, db_mtime)) => db_size as u64 == size && (db_mtime - mtime).abs() < 1.0,
            None => false,
        }
    }

    /// Mark a file as inspected (upsert).
    pub fn mark_inspected(&self, path: &str, size: u64, mtime: f64) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO inspected_files (file_path, file_size, file_mtime, inspected_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(file_path) DO UPDATE SET
                 file_size=excluded.file_size,
                 file_mtime=excluded.file_mtime,
                 inspected_at=excluded.inspected_at",
            params![path, size as i64, mtime, now],
        ) {
            error!("DB error in mark_inspected: {}", e);
        }
    }

    /// Batch upsert inspected files in a single transaction.
    pub fn mark_inspected_batch(&self, entries: &[(String, u64, f64)]) {
        if entries.is_empty() {
            return;
        }
        let mut conn = self.lock();
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(e) => {
                error!(
                    "DB error opening transaction in mark_inspected_batch: {}",
                    e
                );
                return;
            }
        };
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        for (path, size, mtime) in entries {
            if let Err(e) = tx.execute(
                "INSERT INTO inspected_files (file_path, file_size, file_mtime, inspected_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(file_path) DO UPDATE SET
                     file_size=excluded.file_size,
                     file_mtime=excluded.file_mtime,
                     inspected_at=excluded.inspected_at",
                params![path, *size as i64, *mtime, now],
            ) {
                error!("DB error in mark_inspected_batch row '{}': {}", path, e);
            }
        }
        if let Err(e) = tx.commit() {
            error!("DB error committing mark_inspected_batch: {}", e);
        }
    }

    /// Record the start of a transcode job. Returns the row id.
    pub fn record_transcode_start(
        &self,
        source_path: &str,
        share_name: &str,
        original_codec: Option<&str>,
        original_bitrate_bps: i64,
        original_size: i64,
    ) -> rusqlite::Result<i64> {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        conn.execute(
            "INSERT INTO transcode_history
             (source_path, share_name, original_codec, original_bitrate_bps, original_size, outcome, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                source_path,
                share_name,
                original_codec,
                original_bitrate_bps,
                original_size,
                TranscodeOutcome::Failed.as_db_value(),
                now
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Record the completion of a transcode job.
    pub fn record_transcode_end(
        &self,
        row_id: i64,
        outcome: TranscodeOutcome,
        output_size: i64,
        space_saved: i64,
        duration_seconds: f64,
        failure_reason: Option<&str>,
    ) {
        self.record_transcode_end_with_code(
            row_id,
            outcome,
            output_size,
            space_saved,
            duration_seconds,
            failure_reason,
            failure_reason,
        );
    }

    /// Record the completion of a transcode job with machine-readable failure code.
    #[allow(clippy::too_many_arguments)]
    pub fn record_transcode_end_with_code(
        &self,
        row_id: i64,
        outcome: TranscodeOutcome,
        output_size: i64,
        space_saved: i64,
        duration_seconds: f64,
        failure_reason: Option<&str>,
        failure_code: Option<&str>,
    ) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "UPDATE transcode_history SET
             output_size=?1, space_saved=?2, duration_seconds=?3,
             outcome=?4, success=?5, failure_reason=?6, failure_code=?7, completed_at=?8
             WHERE id=?9",
            params![
                output_size,
                space_saved,
                duration_seconds,
                outcome.as_db_value(),
                outcome.is_successful() as i32,
                failure_reason,
                failure_code,
                now,
                row_id
            ],
        ) {
            error!("DB error in record_transcode_end: {}", e);
        }
    }

    /// Record a non-file, pipeline-level failure event in transcode_history.
    pub fn record_pipeline_failure(&self, failure_reason: &str, failure_code: &str) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO transcode_history
             (source_path, outcome, success, failure_reason, failure_code, started_at, completed_at)
             VALUES (?1, ?2, 0, ?3, ?4, ?5, ?5)",
            params![
                "[pipeline]",
                TranscodeOutcome::Failed.as_db_value(),
                failure_reason,
                failure_code,
                now
            ],
        ) {
            error!("DB error in record_pipeline_failure: {}", e);
        }
    }

    /// Record a quarantined file skip event in history.
    pub fn record_quarantine_skip(&self, source_path: &str, reason: &str) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO transcode_history
             (source_path, outcome, success, failure_reason, failure_code, started_at, completed_at)
             VALUES (?1, ?2, 0, ?3, 'quarantined_file', ?4, ?4)",
            params![
                source_path,
                TranscodeOutcome::Failed.as_db_value(),
                reason,
                now
            ],
        ) {
            error!("DB error in record_quarantine_skip: {}", e);
        }
    }

    /// Mark any incomplete transcode rows as failed (interrupted).
    pub fn close_stale_transcodes(&self) -> usize {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        match conn.execute(
            "UPDATE transcode_history SET
             outcome='failed', success=0, failure_reason='interrupted_by_shutdown',
             failure_code='interrupted_shutdown', completed_at=?1
             WHERE completed_at IS NULL",
            params![now],
        ) {
            Ok(count) => count,
            Err(e) => {
                error!("DB error in close_stale_transcodes: {}", e);
                0
            }
        }
    }

    /// Append a point to the space-saved timeseries.
    pub fn log_space_saved(&self, cumulative_bytes_saved: i64) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO space_saved_log (timestamp, cumulative_bytes_saved) VALUES (?1, ?2)",
            params![now, cumulative_bytes_saved],
        ) {
            error!("DB error in log_space_saved: {}", e);
        }
    }

    /// Return recent completed transcode history records.
    pub fn get_recent_transcodes(&self, limit: u32) -> Vec<TranscodeRecord> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT id, source_path, share_name, original_codec, original_bitrate_bps, \
             original_size, output_size, space_saved, duration_seconds, outcome, success, \
             failure_reason, failure_code, started_at, completed_at \
             FROM transcode_history
             WHERE completed_at IS NOT NULL
             ORDER BY id DESC
             LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("DB error in get_recent_transcodes: {}", e);
                return Vec::new();
            }
        };

        let mut records = Vec::new();
        let result = stmt.query_map(params![limit], |row| {
            Ok(TranscodeRecord {
                id: row.get(0)?,
                source_path: row.get(1)?,
                share_name: row.get(2)?,
                original_codec: row.get(3)?,
                original_bitrate_bps: row.get(4)?,
                original_size: row.get(5)?,
                output_size: row.get(6)?,
                space_saved: row.get(7)?,
                duration_seconds: row.get(8)?,
                outcome: TranscodeOutcome::from_db_value(&row.get::<_, String>(9)?),
                success: row.get::<_, i32>(10)? != 0,
                failure_reason: row.get(11)?,
                failure_code: row.get(12)?,
                started_at: row.get(13)?,
                completed_at: row.get(14)?,
            })
        });

        if let Ok(rows) = result {
            for row in rows {
                match row {
                    Ok(r) => records.push(r),
                    Err(e) => error!("DB row error: {}", e),
                }
            }
        }
        records
    }

    /// Get total cumulative space saved (from the timeseries log).
    pub fn get_total_space_saved(&self) -> i64 {
        let conn = self.lock();
        conn.query_row(
            "SELECT cumulative_bytes_saved FROM space_saved_log ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// Get count of successful transcodes.
    #[allow(dead_code)]
    pub fn get_transcode_count(&self) -> i64 {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM transcode_history WHERE outcome = 'replaced'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// Get count of records by outcome.
    #[allow(dead_code)]
    pub fn get_outcome_count(&self, outcome: TranscodeOutcome) -> i64 {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM transcode_history WHERE outcome = ?1",
            params![outcome.as_db_value()],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// Get count of inspected files.
    pub fn get_inspected_count(&self) -> i64 {
        let conn = self.lock();
        conn.query_row("SELECT COUNT(*) FROM inspected_files", [], |row| row.get(0))
            .unwrap_or(0)
    }

    /// Clear all inspected-file cache entries and return number of rows removed.
    pub fn clear_inspected_files(&self) -> usize {
        let conn = self.lock();
        match conn.execute("DELETE FROM inspected_files", []) {
            Ok(rows) => rows,
            Err(e) => {
                error!("DB error in clear_inspected_files: {}", e);
                0
            }
        }
    }

    /// Lightweight DB sanity check for health/doctor flows.
    pub fn healthcheck_query_ok(&self) -> bool {
        let conn = self.lock();
        let basic_ok = conn
            .query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            .map(|v| v == 1)
            .unwrap_or(false);
        if !basic_ok {
            return false;
        }

        let required_tables = [
            "transcode_history",
            "inspected_files",
            "space_saved_log",
            "file_failure_state",
            "file_readiness_state",
            "file_quarantine",
            "service_state",
            "queue_items",
        ];
        for table in required_tables {
            let exists = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?1",
                    params![table],
                    |row| row.get::<_, i64>(0),
                )
                .map(|v| v > 0)
                .unwrap_or(false);
            if !exists {
                return false;
            }
        }

        let required_columns = [
            ("transcode_history", "source_path"),
            ("transcode_history", "outcome"),
            ("transcode_history", "failure_code"),
            ("inspected_files", "file_path"),
            ("inspected_files", "file_mtime"),
            ("space_saved_log", "cumulative_bytes_saved"),
            ("file_failure_state", "file_path"),
            ("file_failure_state", "next_eligible_at"),
            ("file_failure_state", "last_failure_code"),
            ("file_readiness_state", "file_path"),
            ("file_readiness_state", "stable_observations"),
            ("file_readiness_state", "last_seen_at"),
            ("file_quarantine", "file_path"),
            ("file_quarantine", "manual_clear_required"),
            ("service_state", "consecutive_pass_failures"),
            ("service_state", "last_pass_failure_code"),
            ("service_state", "worker_pid"),
            ("service_state", "worker_run_mode"),
            ("queue_items", "source_path"),
            ("queue_items", "order_key"),
            ("queue_items", "preset_payload_json"),
        ];
        for (table, column) in required_columns {
            let sql = format!(
                "SELECT COUNT(*) FROM pragma_table_info('{}') WHERE name = ?1",
                table
            );
            let exists = conn
                .query_row(&sql, params![column], |row| row.get::<_, i64>(0))
                .map(|v| v > 0)
                .unwrap_or(false);
            if !exists {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    const TEST_PRESET_PAYLOAD_JSON: &str = r#"{"PresetList":[]}"#;

    #[test]
    fn test_schema_creation() {
        let db = WatchdogDb::open_in_memory().unwrap();
        assert_eq!(db.get_transcode_count(), 0);
        assert_eq!(db.get_inspected_count(), 0);
    }

    #[test]
    fn test_inspected_files() {
        let db = WatchdogDb::open_in_memory().unwrap();
        assert!(!db.is_inspected("/test/file.mkv", 1000, 1234567890.0));

        db.mark_inspected("/test/file.mkv", 1000, 1234567890.0);
        assert!(db.is_inspected("/test/file.mkv", 1000, 1234567890.0));
        // Different size should not match
        assert!(!db.is_inspected("/test/file.mkv", 2000, 1234567890.0));
    }

    #[test]
    fn test_clear_inspected_files() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.mark_inspected("/test/a.mkv", 1, 1.0);
        db.mark_inspected("/test/b.mkv", 2, 2.0);
        assert_eq!(db.get_inspected_count(), 2);
        assert_eq!(db.clear_inspected_files(), 2);
        assert_eq!(db.get_inspected_count(), 0);
    }

    #[test]
    fn test_transcode_lifecycle() {
        let db = WatchdogDb::open_in_memory().unwrap();

        let row_id = db
            .record_transcode_start(
                "/test/movie.mkv",
                "movies",
                Some("h264"),
                30_000_000,
                5_000_000_000,
            )
            .unwrap();
        assert!(row_id > 0);

        db.record_transcode_end(
            row_id,
            TranscodeOutcome::Replaced,
            2_000_000_000,
            3_000_000_000,
            3600.0,
            None,
        );
        assert_eq!(db.get_transcode_count(), 1);

        let records = db.get_recent_transcodes(10);
        assert_eq!(records.len(), 1);
        assert!(records[0].success);
        assert_eq!(records[0].outcome, TranscodeOutcome::Replaced);
    }

    #[test]
    fn test_incomplete_transcodes_are_hidden_from_recent_history() {
        let db = WatchdogDb::open_in_memory().unwrap();

        db.record_transcode_start("/test/active.mkv", "movies", Some("h264"), 0, 1000)
            .unwrap();

        assert!(db.get_recent_transcodes(10).is_empty());
    }

    #[test]
    fn test_stale_cleanup() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.record_transcode_start("/test/stale.mkv", "movies", None, 0, 0)
            .unwrap();

        let cleaned = db.close_stale_transcodes();
        assert_eq!(cleaned, 1);

        let records = db.get_recent_transcodes(10);
        assert!(!records[0].success);
        assert_eq!(records[0].outcome, TranscodeOutcome::Failed);
        assert_eq!(
            records[0].failure_reason.as_deref(),
            Some("interrupted_by_shutdown")
        );
        assert_eq!(
            records[0].failure_code.as_deref(),
            Some("interrupted_shutdown")
        );
    }

    #[test]
    fn test_outcome_counts_skips_not_transcodes() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let row_id = db
            .record_transcode_start("/test/skip.mkv", "movies", Some("h264"), 0, 1000)
            .unwrap();
        db.record_transcode_end(
            row_id,
            TranscodeOutcome::SkippedNoSavings,
            1000,
            0,
            1.0,
            None,
        );

        assert_eq!(db.get_outcome_count(TranscodeOutcome::SkippedNoSavings), 1);
        assert_eq!(db.get_transcode_count(), 0);
    }

    #[test]
    fn test_retry_scheduled_round_trip_marks_unsuccessful() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let row_id = db
            .record_transcode_start("/test/retry.mkv", "movies", Some("h264"), 0, 1000)
            .unwrap();
        db.record_transcode_end_with_code(
            row_id,
            TranscodeOutcome::RetryScheduled,
            0,
            0,
            2.5,
            Some("timeout_exhausted"),
            Some("timeout_exhausted"),
        );

        let records = db.get_recent_transcodes(1);
        assert_eq!(records[0].outcome, TranscodeOutcome::RetryScheduled);
        assert!(!records[0].success);
        assert_eq!(db.get_outcome_count(TranscodeOutcome::RetryScheduled), 1);
    }

    #[test]
    fn test_migration_adds_outcome_column_and_backfills() {
        let temp = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(temp.path()).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE transcode_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    share_name TEXT,
                    original_codec TEXT,
                    original_bitrate_bps INTEGER,
                    original_size INTEGER,
                    output_size INTEGER,
                    space_saved INTEGER,
                    duration_seconds REAL,
                    success INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT,
                    started_at TEXT,
                    completed_at TEXT
                );
                INSERT INTO transcode_history (source_path, success, space_saved)
                VALUES ('/a.mkv', 1, 100), ('/b.mkv', 1, 0), ('/c.mkv', 0, 0);
                ",
            )
            .unwrap();
        }

        let db = WatchdogDb::open(temp.path()).unwrap();
        assert_eq!(db.get_outcome_count(TranscodeOutcome::Replaced), 1);
        assert_eq!(db.get_outcome_count(TranscodeOutcome::SkippedNoSavings), 1);
        assert_eq!(db.get_outcome_count(TranscodeOutcome::Failed), 1);
    }

    #[test]
    fn test_migration_backfills_stalled_failure_codes() {
        let temp = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(temp.path()).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE transcode_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    share_name TEXT,
                    original_codec TEXT,
                    original_bitrate_bps INTEGER,
                    original_size INTEGER,
                    output_size INTEGER,
                    space_saved INTEGER,
                    duration_seconds REAL,
                    outcome TEXT NOT NULL DEFAULT 'failed',
                    success INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT,
                    failure_code TEXT,
                    started_at TEXT,
                    completed_at TEXT
                );
                INSERT INTO transcode_history
                    (source_path, outcome, success, failure_reason, failure_code, started_at, completed_at)
                VALUES
                    ('/stalled.mkv', 'failed', 0, 'stalled_exhausted', NULL, '2026-01-01T00:00:00', '2026-01-01T00:00:05');

                CREATE TABLE file_failure_state (
                    file_path TEXT PRIMARY KEY,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    last_failure_reason TEXT,
                    last_failure_code TEXT,
                    next_eligible_at INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT
                );
                INSERT INTO file_failure_state (file_path, consecutive_failures, last_failure_reason, last_failure_code, next_eligible_at)
                VALUES ('/stalled.mkv', 3, 'stalled_exhausted', NULL, 0);
                ",
            )
            .unwrap();
        }

        let db = WatchdogDb::open(temp.path()).unwrap();
        let recent = db.get_recent_transcodes(1);
        assert_eq!(recent[0].failure_code.as_deref(), Some("stalled_exhausted"));
        let failure_state = db
            .get_file_failure_state("/stalled.mkv")
            .expect("missing file failure state");
        assert_eq!(
            failure_state.last_failure_code.as_deref(),
            Some("stalled_exhausted")
        );
    }

    #[test]
    fn test_file_failure_state_cooldown_and_clear() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let state = db
            .record_file_failure(
                "/test/a.mkv",
                "verification_failed",
                "verification_failed",
                3,
                300,
                3600,
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(
            state.last_failure_code.as_deref(),
            Some("verification_failed")
        );
        assert!(state.next_eligible_at <= chrono::Utc::now().timestamp());

        let state = db
            .record_file_failure(
                "/test/a.mkv",
                "verification_failed",
                "verification_failed",
                2,
                10,
                60,
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 2);
        assert!(state.next_eligible_at > chrono::Utc::now().timestamp());

        assert!(db.get_file_failure_state("/test/a.mkv").is_some());
        db.clear_file_failure_state("/test/a.mkv");
        assert!(db.get_file_failure_state("/test/a.mkv").is_none());
    }

    #[test]
    fn test_healthcheck_query_ok() {
        let db = WatchdogDb::open_in_memory().unwrap();
        assert!(db.healthcheck_query_ok());
    }

    #[test]
    fn test_healthcheck_query_fails_when_required_table_missing() {
        let db = WatchdogDb::open_in_memory().unwrap();
        {
            let conn = db.lock();
            conn.execute("DROP TABLE space_saved_log", []).unwrap();
        }
        assert!(!db.healthcheck_query_ok());
    }

    #[test]
    fn test_record_transcode_end_with_failure_code() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let row_id = db
            .record_transcode_start("/x.mkv", "movies", Some("h264"), 0, 10)
            .unwrap();
        db.record_transcode_end_with_code(
            row_id,
            TranscodeOutcome::Failed,
            0,
            0,
            1.0,
            Some("verification_failed"),
            Some("verification_failed"),
        );
        let rec = db.get_recent_transcodes(1);
        assert_eq!(rec[0].failure_code.as_deref(), Some("verification_failed"));
    }

    #[test]
    fn test_record_pipeline_failure() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.record_pipeline_failure("scan_timeout", "scan_timeout");
        let rec = db.get_recent_transcodes(1);
        assert_eq!(rec[0].source_path, "[pipeline]");
        assert_eq!(rec[0].failure_reason.as_deref(), Some("scan_timeout"));
        assert_eq!(rec[0].failure_code.as_deref(), Some("scan_timeout"));
        assert_eq!(rec[0].outcome, TranscodeOutcome::Failed);
    }

    #[test]
    fn test_latest_failure_ignores_incomplete_transcode_rows() {
        let db = WatchdogDb::open_in_memory().unwrap();

        let completed_failure = db
            .record_transcode_start("/test/failed.mkv", "movies", Some("h264"), 0, 1000)
            .unwrap();
        db.record_transcode_end_with_code(
            completed_failure,
            TranscodeOutcome::Failed,
            0,
            0,
            1.0,
            Some("verification_failed"),
            Some("verification_failed"),
        );

        db.record_transcode_start("/test/active.mkv", "movies", Some("h264"), 0, 1000)
            .unwrap();

        let latest = db
            .get_latest_failure()
            .expect("expected completed failure record");
        assert_eq!(latest.source_path, "/test/failed.mkv");
        assert_eq!(latest.failure_code.as_deref(), Some("verification_failed"));
        assert!(latest.completed_at.is_some());
    }

    #[test]
    fn test_service_state_pass_failure_and_reset() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let initial = db.get_service_state();
        assert_eq!(initial.consecutive_pass_failures, 0);

        let after_failure = db.note_pass_failure("scan_timeout");
        assert_eq!(after_failure.consecutive_pass_failures, 1);
        assert_eq!(
            after_failure.last_pass_failure_code.as_deref(),
            Some("scan_timeout")
        );

        db.mark_auto_paused("tripwire");
        let paused = db.get_service_state();
        assert!(paused.auto_paused_at.is_some());
        assert_eq!(paused.auto_pause_reason.as_deref(), Some("tripwire"));

        db.clear_auto_paused();
        db.note_pass_success();
        let reset = db.get_service_state();
        assert_eq!(reset.consecutive_pass_failures, 0);
        assert!(reset.last_pass_failure_code.is_none());
        assert!(reset.auto_paused_at.is_none());
        assert!(reset.auto_pause_reason.is_none());
    }

    #[test]
    fn test_clear_worker_state_clears_run_mode() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.set_worker_state(4242, "precision");

        db.clear_worker_state();

        let state = db.get_service_state();
        assert!(state.worker_pid.is_none());
        assert!(state.worker_run_mode.is_none());
    }

    #[test]
    fn test_note_pass_failure_returns_when_service_state_missing() {
        let db = Arc::new(WatchdogDb::open_in_memory().unwrap());
        {
            let conn = db.lock();
            conn.execute("DROP TABLE service_state", []).unwrap();
        }

        let (tx, rx) = mpsc::channel();
        let db_thread = Arc::clone(&db);
        std::thread::spawn(move || {
            let state = db_thread.note_pass_failure("scan_timeout");
            let _ = tx.send(state);
        });

        let state = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("note_pass_failure did not return in time");
        assert_eq!(state.consecutive_pass_failures, 0);
        assert!(state.last_pass_failure_code.is_none());
    }

    #[test]
    fn test_file_quarantine_lifecycle() {
        let db = WatchdogDb::open_in_memory().unwrap();
        assert_eq!(db.quarantine_count(), 0);
        assert!(!db.is_quarantined("/a.mkv"));

        db.quarantine_file(
            "/a.mkv",
            "verification_failed",
            "repeated verification failures",
        );
        assert!(db.is_quarantined("/a.mkv"));
        assert_eq!(db.quarantine_count(), 1);

        let list = db.list_quarantined_files(10);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].file_path, "/a.mkv");
        assert_eq!(
            list[0].last_failure_code.as_deref(),
            Some("verification_failed")
        );
        assert!(list[0].manual_clear_required);

        assert!(db.clear_quarantine_file("/a.mkv"));
        assert_eq!(db.quarantine_count(), 0);
        assert!(!db.clear_quarantine_file("/a.mkv"));

        db.quarantine_file("/b.mkv", "safe_replace_failed", "replace failed");
        db.quarantine_file("/c.mkv", "safe_replace_error", "replace error");
        assert_eq!(db.clear_all_quarantine_files(), 2);
        assert_eq!(db.quarantine_count(), 0);
    }

    #[test]
    fn test_record_transcode_start_returns_error_when_table_missing() {
        let db = WatchdogDb::open_in_memory().unwrap();
        {
            let conn = db.lock();
            conn.execute("DROP TABLE transcode_history", []).unwrap();
        }

        let err = db
            .record_transcode_start("/broken.mkv", "movies", Some("h264"), 0, 10)
            .unwrap_err();
        assert!(
            matches!(err, rusqlite::Error::SqliteFailure(_, _)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_queue_items_deduplicate_and_preserve_order() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let inserted = db.enqueue_queue_items(&[
            NewQueueItem {
                source_path: "/mnt/movies/A.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "scan".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/B.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/A.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/H264.json".to_string(),
                preset_name: "H264".to_string(),
                target_codec: "h264".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
        ]);
        assert_eq!(inserted, 2);

        let queue = db.list_queue_items(10);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0].source_path, "/mnt/movies/A.mkv");
        assert_eq!(queue[0].order_key, 1);
        assert_eq!(
            queue[0].preset_file.as_deref(),
            Some("presets/AV1_MKV.json")
        );
        assert_eq!(queue[0].preset_name.as_deref(), Some("AV1_MKV"));
        assert_eq!(queue[0].target_codec.as_deref(), Some("av1"));
        assert_eq!(
            queue[0].preset_payload_json.as_deref(),
            Some(TEST_PRESET_PAYLOAD_JSON)
        );
        assert_eq!(queue[1].source_path, "/mnt/movies/B.mkv");
        assert_eq!(queue[1].order_key, 2);
        assert_eq!(queue[1].enqueue_source, "manual");
    }

    #[test]
    fn test_queue_items_can_reset_stale_started_rows() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.enqueue_queue_items(&[NewQueueItem {
            source_path: "/mnt/movies/A.mkv".to_string(),
            share_name: "movies".to_string(),
            enqueue_source: "scan".to_string(),
            preset_file: "presets/AV1_MKV.json".to_string(),
            preset_name: "AV1_MKV".to_string(),
            target_codec: "av1".to_string(),
            preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
        }]);
        db.mark_queue_item_started("/mnt/movies/A.mkv");
        assert!(db.list_queue_items(1)[0].started_at.is_some());

        assert_eq!(db.reset_stale_queue_items(), 1);
        assert!(db.list_queue_items(1)[0].started_at.is_none());
    }

    #[test]
    fn test_clear_queue_items_removes_remaining_rows() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.enqueue_queue_items(&[
            NewQueueItem {
                source_path: "/mnt/movies/A.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "scan".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/B.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
        ]);

        assert_eq!(db.get_queue_count(), 2);
        assert_eq!(db.clear_queue_items(), 2);
        assert_eq!(db.get_queue_count(), 0);
    }

    #[test]
    fn test_remove_pending_queue_item_leaves_active_rows() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.enqueue_queue_items(&[
            NewQueueItem {
                source_path: "/mnt/movies/A.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
            NewQueueItem {
                source_path: "/mnt/movies/B.mkv".to_string(),
                share_name: "movies".to_string(),
                enqueue_source: "manual".to_string(),
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
                preset_payload_json: TEST_PRESET_PAYLOAD_JSON.to_string(),
            },
        ]);
        db.mark_queue_item_started("/mnt/movies/B.mkv");

        assert!(db.remove_pending_queue_item("/mnt/movies/A.mkv"));
        assert!(!db.remove_pending_queue_item("/mnt/movies/B.mkv"));
        assert!(db.queue_item_exists("/mnt/movies/B.mkv"));
        assert!(!db.queue_item_exists("/mnt/movies/A.mkv"));
    }
}
