"""Thread-safe shared state between the watchdog loop and the Flask dashboard."""

import collections
import threading
from datetime import datetime
from typing import Dict, List, Optional


class WatchdogState:
    """Holds live watchdog status, protected by a threading lock."""

    def __init__(self):
        self._lock = threading.Lock()
        self._state = "idle"  # idle | scanning | transcoding
        self._current_file: Optional[str] = None
        self._queue_position = 0
        self._queue_total = 0
        self._transcode_percent = 0.0
        self._transcode_fps = 0.0
        self._transcode_avg_fps = 0.0
        self._transcode_eta = ""
        self._last_pass_time: Optional[str] = None
        self._nfs_healthy = True
        self._log_lines: collections.deque = collections.deque(maxlen=80)

    def set_state(self, state: str) -> None:
        with self._lock:
            self._state = state
            if state == "idle":
                self._current_file = None
                self._transcode_percent = 0.0
                self._transcode_fps = 0.0
                self._transcode_avg_fps = 0.0
                self._transcode_eta = ""

    def set_current_file(self, path: Optional[str]) -> None:
        with self._lock:
            self._current_file = path

    def set_queue_info(self, position: int, total: int) -> None:
        with self._lock:
            self._queue_position = position
            self._queue_total = total

    def set_transcode_progress(
        self,
        percent: float,
        fps: float = 0.0,
        avg_fps: float = 0.0,
        eta: str = "",
    ) -> None:
        with self._lock:
            self._transcode_percent = percent
            self._transcode_fps = fps
            self._transcode_avg_fps = avg_fps
            self._transcode_eta = eta

    def set_last_pass_time(self) -> None:
        with self._lock:
            self._last_pass_time = datetime.utcnow().isoformat()

    def set_nfs_healthy(self, healthy: bool) -> None:
        with self._lock:
            self._nfs_healthy = healthy

    def append_log_line(self, line: str) -> None:
        with self._lock:
            self._log_lines.append(line)

    def get_log_lines(self) -> List[str]:
        with self._lock:
            return list(self._log_lines)

    def snapshot(self) -> Dict:
        """Return a dict copy of all state for the dashboard."""
        with self._lock:
            return {
                "state": self._state,
                "current_file": self._current_file,
                "queue_position": self._queue_position,
                "queue_total": self._queue_total,
                "transcode_percent": self._transcode_percent,
                "transcode_fps": self._transcode_fps,
                "transcode_avg_fps": self._transcode_avg_fps,
                "transcode_eta": self._transcode_eta,
                "last_pass_time": self._last_pass_time,
                "nfs_healthy": self._nfs_healthy,
            }
