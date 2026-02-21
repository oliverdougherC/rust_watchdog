use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use tracing::error;

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
    pub success: bool,
    pub failure_reason: Option<String>,
    pub started_at: Option<String>,
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
                success INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT,
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

            CREATE INDEX IF NOT EXISTS idx_inspected_files_file_path
                ON inspected_files (file_path);

            CREATE INDEX IF NOT EXISTS idx_transcode_history_source_path
                ON transcode_history (source_path);
        ",
        )?;
        Ok(())
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
             (source_path, share_name, original_codec, original_bitrate_bps, original_size, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                source_path,
                share_name,
                original_codec,
                original_bitrate_bps,
                original_size,
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
        success: bool,
        output_size: i64,
        space_saved: i64,
        duration_seconds: f64,
        failure_reason: Option<&str>,
    ) {
        let conn = self.lock();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        if let Err(e) = conn.execute(
            "UPDATE transcode_history SET
             output_size=?1, space_saved=?2, duration_seconds=?3,
             success=?4, failure_reason=?5, completed_at=?6
             WHERE id=?7",
            params![
                output_size,
                space_saved,
                duration_seconds,
                success as i32,
                failure_reason,
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
             success=0, failure_reason='interrupted', completed_at=?1
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
             original_size, output_size, space_saved, duration_seconds, success, \
             failure_reason, started_at, completed_at \
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
                success: row.get::<_, i32>(9)? != 0,
                failure_reason: row.get(10)?,
                started_at: row.get(11)?,
                completed_at: row.get(12)?,
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
            "SELECT COUNT(*) FROM transcode_history WHERE success = 1",
            [],
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

        db.record_transcode_end(row_id, true, 2_000_000_000, 3_000_000_000, 3600.0, None);
        assert_eq!(db.get_transcode_count(), 1);

        let records = db.get_recent_transcodes(10);
        assert_eq!(records.len(), 1);
        assert!(records[0].success);
    }

    #[test]
    fn test_stale_cleanup() {
        let db = WatchdogDb::open_in_memory().unwrap();
        db.record_transcode_start("/test/stale.mkv", "movies", None, 0, 0);

        let cleaned = db.close_stale_transcodes();
        assert_eq!(cleaned, 1);

        let records = db.get_recent_transcodes(10);
        assert!(!records[0].success);
        assert_eq!(records[0].failure_reason.as_deref(), Some("interrupted"));
    }
}
