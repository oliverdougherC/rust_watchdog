"""
Lightweight reporting script for the Jellyfin AV1 transcoding watchdog.

Usage:
    python watchdog_report.py

Outputs:
    - Total files processed (replaced)
    - Total space saved (human-readable)
    - Queue progress of current run (e.g., 129/12081)
    - Elapsed time of current run
    - Total elapsed time since logs began (approximate "time watching")
"""

import glob
import json
import os
import re
import sys
from datetime import datetime, timedelta

try:
    # Prefer config values if available
    from config import STATISTICS_STATE_FILE, MEDIA_DIRECTORIES
except Exception:
    STATISTICS_STATE_FILE = "statistics_state.json"
    MEDIA_DIRECTORIES = []

LOGS_DIR = "logs"
DEFAULT_ACTIVITY_LOG = os.path.join(LOGS_DIR, "activity.log")
TIME_FMT = "%Y-%m-%d %H:%M:%S"


def format_bytes(num_bytes: int) -> str:
    """Return human-friendly binary units for a byte count."""
    num = float(num_bytes)
    for unit in ("bytes", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(num) < 1024.0 or unit == "PiB":
            if unit == "bytes":
                return f"{int(num)} {unit}"
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num_bytes} bytes"


def format_timedelta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if days or hours:
        parts.append(f"{hours}h")
    if days or hours or minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def read_statistics_state(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def parse_activity_log(path: str):
    """
    Extract:
      - first and last timestamps seen
      - last queue length (Queue length: N)
      - last progress marker (------ Beginning analysis on item i/N ...)
      - timestamp of last queue length line (approx run start marker)
    """
    if not os.path.exists(path):
        return None

    first_ts = last_ts = None
    last_queue_len = None
    last_queue_ts = None
    last_progress = None  # (current_idx, total)

    queue_re = re.compile(r"Queue length:\s*(\d+)")
    progress_re = re.compile(r"analysis on item\s+(\d+)\s*/\s*(\d+)")

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if len(line) < 20:
                continue
            ts_text = line[:19]
            try:
                ts = datetime.strptime(ts_text, TIME_FMT)
            except ValueError:
                continue
            if first_ts is None:
                first_ts = ts
            last_ts = ts

            if "Queue length:" in line:
                m = queue_re.search(line)
                if m:
                    last_queue_len = int(m.group(1))
                    last_queue_ts = ts

            if "Beginning analysis on item" in line:
                m = progress_re.search(line)
                if m:
                    last_progress = (int(m.group(1)), int(m.group(2)))

    if first_ts is None:
        return None

    return {
        "first_ts": first_ts,
        "last_ts": last_ts,
        "last_queue_len": last_queue_len,
        "last_queue_ts": last_queue_ts,
        "last_progress": last_progress,
    }


def collect_log_span(paths):
    """Return earliest first_ts and latest last_ts across multiple activity logs."""
    span_first = span_last = None
    for p in paths:
        info = parse_activity_log(p)
        if not info:
            continue
        if span_first is None or info["first_ts"] < span_first:
            span_first = info["first_ts"]
        if span_last is None or info["last_ts"] > span_last:
            span_last = info["last_ts"]
    return span_first, span_last


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, LOGS_DIR)
    activity_log_path = os.path.join(base_dir, DEFAULT_ACTIVITY_LOG)
    stats_state_path = os.path.join(base_dir, STATISTICS_STATE_FILE)

    stats_state = read_statistics_state(stats_state_path)
    files_transcoded = int(stats_state.get("files_transcoded", 0))
    saved_bytes = int(stats_state.get("space_saved_bytes", 0))

    log_info = parse_activity_log(activity_log_path)

    log_paths = sorted(glob.glob(os.path.join(logs_dir, "activity*.log")))
    if not log_paths and os.path.exists(activity_log_path):
        log_paths = [activity_log_path]
    span_first, span_last = collect_log_span(log_paths)

    now = datetime.now()
    current_run_elapsed = None
    queue_display = "unknown"
    if log_info:
        # Approximate current run start as the last "Queue length" timestamp.
        if log_info["last_queue_ts"]:
            current_run_elapsed = now - log_info["last_queue_ts"]
        if log_info["last_progress"]:
            queue_display = f"{log_info['last_progress'][0]}/{log_info['last_progress'][1]}"
        elif log_info["last_queue_len"] is not None:
            queue_display = f"0/{log_info['last_queue_len']}"

    total_watch_elapsed = None
    if span_first and span_last:
        total_watch_elapsed = span_last - span_first

    print("Watchdog Report")
    print("-" * 40)
    print(f"Total files processed (replaced): {files_transcoded}")
    print(f"Total space saved: {format_bytes(saved_bytes)}")
    print(f"Queue progress (current run): {queue_display}")
    if current_run_elapsed is not None:
        print(f"Elapsed time (current run): {format_timedelta(current_run_elapsed)}")
    else:
        print("Elapsed time (current run): unknown")
    if total_watch_elapsed is not None:
        print(
            "Total elapsed time (since logs began): "
            f"{format_timedelta(total_watch_elapsed)}"
        )
    else:
        print("Total elapsed time (since logs began): unknown")


if __name__ == "__main__":
    sys.exit(main())

