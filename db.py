"""SQLite database layer for transcode history, inspected files, and space-saved timeseries."""

import logging
import os
import sqlite3
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)


class WatchdogDB:
    """Thread-safe SQLite database with WAL mode and thread-local connections."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def close_connection(self) -> None:
        """Close the thread-local connection if it exists."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def _init_schema(self) -> None:
        conn = self._get_conn()
        conn.executescript("""
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
        """)
        conn.commit()

    def is_inspected(self, path: str, size: int, mtime: float) -> bool:
        """Return True if file was previously inspected with same size and mtime."""
        try:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT file_size, file_mtime FROM inspected_files WHERE file_path = ?",
                (path,),
            ).fetchone()
        except sqlite3.Error as e:
            _log.error("DB error in is_inspected: %s", e)
            return False
        if row is None:
            return False
        return row["file_size"] == size and abs(row["file_mtime"] - mtime) < 1.0

    def mark_inspected(self, path: str, size: int, mtime: float) -> None:
        """Insert or update an inspected file record."""
        try:
            conn = self._get_conn()
            now = datetime.utcnow().isoformat()
            conn.execute(
                """INSERT INTO inspected_files (file_path, file_size, file_mtime, inspected_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(file_path) DO UPDATE SET
                       file_size=excluded.file_size,
                       file_mtime=excluded.file_mtime,
                       inspected_at=excluded.inspected_at""",
                (path, size, mtime, now),
            )
            conn.commit()
        except sqlite3.Error as e:
            _log.error("DB error in mark_inspected: %s", e)

    def record_transcode_start(
        self,
        source_path: str,
        share_name: str,
        original_codec: Optional[str] = None,
        original_bitrate_bps: int = 0,
        original_size: int = 0,
    ) -> int:
        """Record the start of a transcode job. Returns the row id (0 on failure)."""
        try:
            conn = self._get_conn()
            now = datetime.utcnow().isoformat()
            cursor = conn.execute(
                """INSERT INTO transcode_history
                   (source_path, share_name, original_codec, original_bitrate_bps,
                    original_size, started_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (source_path, share_name, original_codec, original_bitrate_bps,
                 original_size, now),
            )
            conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            _log.error("DB error in record_transcode_start: %s", e)
            return 0

    def record_transcode_end(
        self,
        row_id: int,
        success: bool,
        output_size: int = 0,
        space_saved: int = 0,
        duration_seconds: float = 0.0,
        failure_reason: Optional[str] = None,
    ) -> None:
        """Record the completion of a transcode job."""
        try:
            conn = self._get_conn()
            now = datetime.utcnow().isoformat()
            conn.execute(
                """UPDATE transcode_history SET
                   output_size=?, space_saved=?, duration_seconds=?,
                   success=?, failure_reason=?, completed_at=?
                   WHERE id=?""",
                (output_size, space_saved, duration_seconds,
                 1 if success else 0, failure_reason, now, row_id),
            )
            conn.commit()
        except sqlite3.Error as e:
            _log.error("DB error in record_transcode_end: %s", e)

    def close_stale_transcodes(self) -> int:
        """Mark any rows with completed_at IS NULL as failed (interrupted).

        Returns the number of rows cleaned up.
        """
        try:
            conn = self._get_conn()
            now = datetime.utcnow().isoformat()
            cursor = conn.execute(
                """UPDATE transcode_history SET
                   success=0, failure_reason='interrupted', completed_at=?
                   WHERE completed_at IS NULL""",
                (now,),
            )
            conn.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            _log.error("DB error in close_stale_transcodes: %s", e)
            return 0

    def log_space_saved(self, cumulative_bytes_saved: int) -> None:
        """Append a point to the space-saved timeseries."""
        try:
            conn = self._get_conn()
            now = datetime.utcnow().isoformat()
            conn.execute(
                "INSERT INTO space_saved_log (timestamp, cumulative_bytes_saved) VALUES (?, ?)",
                (now, cumulative_bytes_saved),
            )
            conn.commit()
        except sqlite3.Error as e:
            _log.error("DB error in log_space_saved: %s", e)

    def get_recent_transcodes(self, limit: int = 50) -> List[Dict]:
        """Return recent transcode history records."""
        try:
            conn = self._get_conn()
            rows = conn.execute(
                "SELECT * FROM transcode_history ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            _log.error("DB error in get_recent_transcodes: %s", e)
            return []

    def get_space_saved_timeseries(self, limit: int = 500) -> List[Dict]:
        """Return the space-saved timeseries for charting."""
        try:
            conn = self._get_conn()
            rows = conn.execute(
                """SELECT timestamp, cumulative_bytes_saved
                   FROM (SELECT id, timestamp, cumulative_bytes_saved
                         FROM space_saved_log ORDER BY id DESC LIMIT ?)
                   ORDER BY id ASC""",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            _log.error("DB error in get_space_saved_timeseries: %s", e)
            return []

    def get_total_space_saved(self) -> int:
        """Return the most recent cumulative space saved value."""
        try:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT cumulative_bytes_saved FROM space_saved_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return row["cumulative_bytes_saved"] if row else 0
        except sqlite3.Error as e:
            _log.error("DB error in get_total_space_saved: %s", e)
            return 0

    def get_transcode_count(self) -> int:
        """Return total number of successful transcodes."""
        try:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM transcode_history WHERE success = 1"
            ).fetchone()
            return row["cnt"] if row else 0
        except sqlite3.Error as e:
            _log.error("DB error in get_transcode_count: %s", e)
            return 0

    def get_inspected_count(self) -> int:
        """Return total number of inspected files in the DB."""
        try:
            conn = self._get_conn()
            row = conn.execute("SELECT COUNT(*) as cnt FROM inspected_files").fetchone()
            return row["cnt"] if row else 0
        except sqlite3.Error as e:
            _log.error("DB error in get_inspected_count: %s", e)
            return 0

    def migrate_inspected_files(self, log_path: str, logger=None) -> int:
        """Migrate inspected_files.log entries into the database.

        Stats existing files for real size/mtime. Renames the log to .log.bak
        after migration. Returns the number of entries migrated.
        """
        if not os.path.exists(log_path):
            return 0

        migrated = 0
        conn = self._get_conn()
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                for line in f:
                    path = line.strip()
                    if not path:
                        continue
                    try:
                        st = os.stat(path)
                        now = datetime.utcnow().isoformat()
                        conn.execute(
                            """INSERT INTO inspected_files
                               (file_path, file_size, file_mtime, inspected_at)
                               VALUES (?, ?, ?, ?)
                               ON CONFLICT(file_path) DO UPDATE SET
                                   file_size=excluded.file_size,
                                   file_mtime=excluded.file_mtime,
                                   inspected_at=excluded.inspected_at""",
                            (path, st.st_size, st.st_mtime, now),
                        )
                        migrated += 1
                    except OSError:
                        if logger:
                            logger.info(f"Migration skip (file gone): {path}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        bak_path = log_path + ".bak"
        os.rename(log_path, bak_path)
        if logger:
            logger.info(f"Migrated {migrated} inspected files to DB; log renamed to {bak_path}")
        return migrated
