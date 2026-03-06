use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use tracing::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodeOutcome {
    Replaced,
    SkippedNoSavings,
    Failed,
}

impl TranscodeOutcome {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Replaced => "replaced",
            Self::SkippedNoSavings => "skipped_no_savings",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_value(value: &str) -> Self {
        match value {
            "replaced" => Self::Replaced,
            "skipped_no_savings" => Self::SkippedNoSavings,
            _ => Self::Failed,
        }
    }

    pub fn is_successful(self) -> bool {
        !matches!(self, Self::Failed)
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
    pub next_eligible_at: i64,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CooldownRecord {
    pub file_path: String,
    pub consecutive_failures: u32,
    pub last_failure_reason: Option<String>,
    pub next_eligible_at: i64,
}

#[derive(Debug, Clone)]
pub struct LatestFailureRecord {
    pub source_path: String,
    pub failure_reason: Option<String>,
    pub failure_code: Option<String>,
    pub completed_at: Option<String>,
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
                next_eligible_at INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_inspected_files_file_path
                ON inspected_files (file_path);

            CREATE INDEX IF NOT EXISTS idx_transcode_history_source_path
                ON transcode_history (source_path);

            CREATE INDEX IF NOT EXISTS idx_file_failure_state_next_eligible_at
                ON file_failure_state (next_eligible_at);
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
                 WHEN failure_reason IN ('timeout_exhausted') OR failure_reason LIKE 'timeout%' THEN 'timeout'
                 WHEN failure_reason IN ('transcode_failed', 'transcode_error') THEN 'transcode_error'
                 WHEN failure_reason IN ('verification_failed', 'verification_error') THEN 'verification_failed'
                 WHEN failure_reason IN ('output_missing', 'output_zero_bytes', 'output_suspiciously_small') THEN 'output_invalid'
                 WHEN failure_reason IN ('transfer_failed', 'transfer_error', 'rsync failed (requeued)', 'rsync error (requeued)') THEN 'transfer_error'
                 WHEN failure_reason IN ('insufficient_temp_space', 'insufficient_nfs_space') THEN 'insufficient_space'
                 WHEN failure_reason IN ('source_changed_during_transcode') THEN 'source_changed'
                 WHEN failure_reason IN ('safe_replace_failed', 'safe_replace_error') THEN 'safe_replace_failed'
                 WHEN failure_reason IN ('file_in_use') THEN 'file_in_use'
                 ELSE 'other'
             END
             WHERE failure_code IS NULL AND failure_reason IS NOT NULL AND failure_reason <> ''",
            [],
        )?;
        Ok(())
    }

    /// Return file failure-state for cooldown logic.
    pub fn get_file_failure_state(&self, path: &str) -> Option<FileFailureState> {
        let conn = self.lock();
        conn.query_row(
            "SELECT file_path, consecutive_failures, last_failure_reason, next_eligible_at, updated_at
             FROM file_failure_state WHERE file_path = ?1",
            params![path],
            |row| {
                Ok(FileFailureState {
                    file_path: row.get(0)?,
                    consecutive_failures: row.get::<_, i64>(1)?.max(0) as u32,
                    last_failure_reason: row.get(2)?,
                    next_eligible_at: row.get::<_, i64>(3)?,
                    updated_at: row.get(4)?,
                })
            },
        )
        .optional()
        .unwrap_or(None)
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
            "INSERT INTO file_failure_state (file_path, consecutive_failures, last_failure_reason, next_eligible_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(file_path) DO UPDATE SET
                 consecutive_failures=excluded.consecutive_failures,
                 last_failure_reason=excluded.last_failure_reason,
                 next_eligible_at=excluded.next_eligible_at,
                 updated_at=excluded.updated_at",
            params![
                path,
                consecutive as i64,
                failure_reason,
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
            "SELECT file_path, consecutive_failures, last_failure_reason, next_eligible_at
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
                next_eligible_at: row.get(3)?,
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
    ) -> i64 {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        match conn.execute(
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
        ) {
            Ok(_) => conn.last_insert_rowid(),
            Err(e) => {
                error!("DB error in record_transcode_start: {}", e);
                0
            }
        }
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

    /// Return recent transcode history records.
    pub fn get_recent_transcodes(&self, limit: u32) -> Vec<TranscodeRecord> {
        let conn = self.lock();
        let mut stmt = match conn.prepare(
            "SELECT id, source_path, share_name, original_codec, original_bitrate_bps, \
             original_size, output_size, space_saved, duration_seconds, outcome, success, \
             failure_reason, failure_code, started_at, completed_at \
             FROM transcode_history ORDER BY id DESC LIMIT ?1",
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

    /// Lightweight DB sanity check for health/doctor flows.
    pub fn healthcheck_query_ok(&self) -> bool {
        let conn = self.lock();
        conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            .map(|v| v == 1)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::NamedTempFile;

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
    fn test_transcode_lifecycle() {
        let db = WatchdogDb::open_in_memory().unwrap();

        let row_id = db.record_transcode_start(
            "/test/movie.mkv",
            "movies",
            Some("h264"),
            30_000_000,
            5_000_000_000,
        );
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
    fn test_stale_cleanup() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.record_transcode_start("/test/stale.mkv", "movies", None, 0, 0);

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
        let row_id = db.record_transcode_start("/test/skip.mkv", "movies", Some("h264"), 0, 1000);
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
    fn test_file_failure_state_cooldown_and_clear() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let state = db
            .record_file_failure("/test/a.mkv", "verification failed", 3, 300, 3600)
            .unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert!(state.next_eligible_at <= chrono::Utc::now().timestamp());

        let state = db
            .record_file_failure("/test/a.mkv", "verification failed", 2, 10, 60)
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
    fn test_record_transcode_end_with_failure_code() {
        let db = WatchdogDb::open_in_memory().unwrap();
        let row_id = db.record_transcode_start("/x.mkv", "movies", Some("h264"), 0, 10);
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
}
