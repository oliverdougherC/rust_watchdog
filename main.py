import argparse
import glob
import os
import sys
import json
import shlex
import logging
import subprocess
import shutil
import threading
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    # When executed as a package: python -m transcode_watchdog.main
    from .config import (
        MEDIA_DIRECTORIES,
        TRANSCODE_TEMP_PATH,
        HANDBRAKE_PRESET_FILE,
        HANDBRAKE_PRESET_NAME,
        INSPECTED_FILES_LOG,
        MAX_AVERAGE_BITRATE_MBPS,
        TARGET_CODEC,
        VIDEO_EXTENSIONS,
        SCAN_INTERVAL_SECONDS,
        NFS_SERVER,
        NFS_SHARES,
        STATISTICS_LOG,
        STATISTICS_STATE_FILE,
        TRANSCODE_TIMEOUT_SECONDS,
        TRANSCODE_MAX_RETRIES,
        DASHBOARD_PORT,
        DATABASE_PATH,
        MIN_FREE_SPACE_MULTIPLIER,
    )
except Exception:
    # When executed directly from the folder: python main.py
    from config import (
        MEDIA_DIRECTORIES,
        TRANSCODE_TEMP_PATH,
        HANDBRAKE_PRESET_FILE,
        HANDBRAKE_PRESET_NAME,
        INSPECTED_FILES_LOG,
        MAX_AVERAGE_BITRATE_MBPS,
        TARGET_CODEC,
        VIDEO_EXTENSIONS,
        SCAN_INTERVAL_SECONDS,
        NFS_SERVER,
        NFS_SHARES,
        STATISTICS_LOG,
        STATISTICS_STATE_FILE,
        TRANSCODE_TIMEOUT_SECONDS,
        TRANSCODE_MAX_RETRIES,
        DASHBOARD_PORT,
        DATABASE_PATH,
        MIN_FREE_SPACE_MULTIPLIER,
    )


def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


LOG_FORMATTER = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _rotate_activity_log(base_dir: str, logger: logging.Logger) -> str:
    """
    Rotate the current activity.log to the next sequential activityN.log and
    attach a fresh file handler for subsequent writes.
    """
    logs_dir = os.path.join(base_dir, "logs")
    ensure_dir(logs_dir)

    current_log = os.path.join(logs_dir, "activity.log")

    # Remove existing file handlers to avoid writing to a rotated file descriptor
    for handler in list(logger.handlers):
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    rotated_path = ""
    if os.path.exists(current_log):
        # Find the next available suffix
        max_idx = 0
        for path in glob.glob(os.path.join(logs_dir, "activity*.log")):
            name = os.path.basename(path)
            match = re.match(r"activity(\d+)\.log$", name)
            if match:
                max_idx = max(max_idx, int(match.group(1)))
        rotated_path = os.path.join(logs_dir, f"activity{max_idx + 1}.log")
        os.rename(current_log, rotated_path)

    file_handler = logging.FileHandler(current_log)
    file_handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(file_handler)
    return rotated_path


def setup_logging(base_dir: str) -> logging.Logger:
    logger = logging.getLogger("transcode_watchdog")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Ensure a single console handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(LOG_FORMATTER)
        logger.addHandler(console_handler)

    _rotate_activity_log(base_dir, logger)
    return logger


def resolve_path(base_dir: str, path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(base_dir, path)


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


class StatisticsTracker:
    """Handles run-level + cumulative statistics and logging."""

    METRIC_KEYS = (
        "files_inspected",
        "files_queued",
        "files_transcoded",
        "transcode_failures",
        "space_saved_bytes",
    )

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self._write_lock = threading.Lock()
        self.log_path = resolve_path(base_dir, STATISTICS_LOG)
        self.state_path = resolve_path(base_dir, STATISTICS_STATE_FILE)

        ensure_dir(os.path.dirname(self.log_path))
        ensure_dir(os.path.dirname(self.state_path))

        self.logger = logging.getLogger("transcode_watchdog.statistics")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        if not self.logger.handlers:
            file_handler = logging.FileHandler(self.log_path)
            fmt = logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(fmt)
            self.logger.addHandler(file_handler)

        self.cumulative = self._load_state()
        self.start_pass()

    def start_pass(self) -> None:
        self.current_metrics = {key: 0 for key in self.METRIC_KEYS}
        self._persist_realtime_state()

    def increment(self, key: str, amount: int = 1) -> None:
        if key not in self.current_metrics:
            raise KeyError(f"Unknown metric '{key}'")
        self.current_metrics[key] += amount
        self._persist_realtime_state()

    def add_space_saved(self, bytes_saved: int) -> None:
        if bytes_saved > 0:
            self.current_metrics["space_saved_bytes"] += int(bytes_saved)
            self._persist_realtime_state()

    def log_event(self, message: str, level: int = logging.INFO) -> None:
        self.logger.log(level, message)

    def record_run_failure(self, message: str) -> None:
        self.log_event(f"Run aborted: {message}", level=logging.ERROR)
        self.commit(status="aborted")

    def commit(self, status: str = "complete") -> None:
        run_metrics = self.current_metrics.copy()
        saved_gib = run_metrics["space_saved_bytes"] / float(1024 ** 3)
        status_label = status.upper()
        self.logger.info(
            "%s run metrics | inspected=%d queued=%d transcoded=%d failures=%d "
            "saved=%d bytes (%.2f GiB)",
            status_label,
            run_metrics["files_inspected"],
            run_metrics["files_queued"],
            run_metrics["files_transcoded"],
            run_metrics["transcode_failures"],
            run_metrics["space_saved_bytes"],
            saved_gib,
        )

        for key, value in run_metrics.items():
            self.cumulative[key] = int(self.cumulative.get(key, 0)) + int(value)

        total_saved_gib = self.cumulative["space_saved_bytes"] / float(1024 ** 3)
        self.logger.info(
            "Cumulative totals | inspected=%d queued=%d transcoded=%d failures=%d "
            "saved=%d bytes (%.2f GiB)",
            self.cumulative["files_inspected"],
            self.cumulative["files_queued"],
            self.cumulative["files_transcoded"],
            self.cumulative["transcode_failures"],
            self.cumulative["space_saved_bytes"],
            total_saved_gib,
        )
        self._write_state(self.cumulative)
        self.start_pass()

    def _load_state(self) -> Dict[str, int]:
        if not os.path.exists(self.state_path):
            return {key: 0 for key in self.METRIC_KEYS}
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {key: 0 for key in self.METRIC_KEYS}
        return {key: int(data.get(key, 0)) for key in self.METRIC_KEYS}

    def _snapshot_totals(self) -> Dict[str, int]:
        snapshot = {}
        for key in self.METRIC_KEYS:
            snapshot[key] = int(self.cumulative.get(key, 0)) + int(
                self.current_metrics.get(key, 0)
            )
        return snapshot

    def _persist_realtime_state(self) -> None:
        self._write_state(self._snapshot_totals())

    def _write_state(self, payload: Dict[str, int]) -> None:
        with self._write_lock:
            try:
                # Atomic write: write to temp file then rename
                temp_path = self.state_path + ".tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, sort_keys=True)
                os.replace(temp_path, self.state_path)
            except OSError:
                self.logger.exception("Unable to persist statistics state file")


class NFSManager:
    """Verifies and remounts NFS shares before each watchdog pass."""

    def __init__(self, logger: logging.Logger, stats: StatisticsTracker):
        self.logger = logger
        self.stats = stats

    def ensure_mounts(self) -> None:
        for share in NFS_SHARES:
            mount_point = self._resolved_mount(share["local_mount"])
            if self._share_is_healthy(mount_point):
                continue

            msg = (
                f"Mount '{share['name']}' at {mount_point} is unavailable; "
                "attempting remount"
            )
            self.logger.warning(msg)
            self.stats.log_event(msg, level=logging.WARNING)

            if not self._remount_share(share, mount_point):
                failure_msg = (
                    f"Failed to remount share '{share['name']}' "
                    f"({mount_point}) after multiple attempts"
                )
                self.logger.error(failure_msg)
                self.stats.log_event(failure_msg, level=logging.ERROR)
                raise RuntimeError(failure_msg)

    @staticmethod
    def _resolved_mount(path: str) -> str:
        return os.path.abspath(os.path.expanduser(path))

    def _share_is_healthy(self, mount_point: str) -> bool:
        if not os.path.ismount(mount_point):
            return False
        try:
            with os.scandir(mount_point) as it:
                next(it, None)
        except FileNotFoundError:
            return False
        except PermissionError:
            return False
        except OSError:
            return False
        return True

    def _remount_share(self, share: Dict[str, str], mount_point: str) -> bool:
        ensure_dir(mount_point)
        remote = f"{NFS_SERVER}:{share['remote_path']}"
        delay = 2
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            attempt_msg = (
                f"[{share['name']}] Remount attempt {attempt}/{max_attempts} "
                f"using mount_nfs {remote} {mount_point}"
            )
            self.logger.warning(attempt_msg)
            self.stats.log_event(attempt_msg, level=logging.WARNING)

            result = run_cmd(
                self.logger,
                ["mount_nfs", remote, mount_point],
            )
            if result.returncode == 0 and self._share_is_healthy(mount_point):
                success_msg = (
                    f"[{share['name']}] Remount succeeded at {mount_point}"
                )
                self.logger.info(success_msg)
                self.stats.log_event(success_msg)
                return True

            time.sleep(delay)
            delay = min(delay * 2, 30)

        exhaustion_msg = (
            f"[{share['name']}] Remount attempts exhausted for {mount_point}"
        )
        self.logger.error(exhaustion_msg)
        self.stats.log_event(exhaustion_msg, level=logging.ERROR)
        return False


def load_inspected_files(log_path: str) -> set:
    if not os.path.exists(log_path):
        return set()
    inspected = set()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            p = line.strip()
            if p:
                inspected.add(p)
    return inspected


def append_inspected_file(log_path: str, file_path: str) -> None:
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_path + "\n")


def verify_dependencies(logger: logging.Logger) -> bool:
    required = ["ffprobe", "HandBrakeCLI", "rsync"]
    missing = [cmd for cmd in required if shutil.which(cmd) is None]
    if missing:
        logger.critical(f"Missing required tools: {', '.join(missing)}")
        logger.critical("Ensure they are installed and available in PATH.")
        return False
    logger.info("All required CLI tools found: ffprobe, HandBrakeCLI, rsync")
    return True


def scan_media_shares(
    logger: logging.Logger,
    stats: StatisticsTracker,
    shares: List[Dict[str, str]],
    extensions: tuple,
):
    for share in shares:
        message = f"Scanning share '{share['name']}' at {share['path']}"
        logger.info(message)
        stats.log_event(message)
        try:
            for root, _dirs, files in os.walk(share["path"]):
                for name in files:
                    # Skip AppleDouble/hidden sidecar files that cause ffprobe failures
                    if name.startswith("."):
                        hidden_path = os.path.join(root, name)
                        logger.info(f"[{share['name']}] SKIP hidden/sidecar file: {hidden_path}")
                        stats.log_event(f"Skipped hidden/sidecar file: {hidden_path}")
                        continue
                    if name.lower().endswith(extensions):
                        yield share["name"], os.path.join(root, name)
        except OSError as e:
            err_msg = f"Failed to scan share '{share['name']}': {e}"
            logger.error(err_msg)
            stats.log_event(err_msg, level=logging.ERROR)
            continue


def run_cmd(
    logger: logging.Logger,
    cmd: list,
    check: bool = False,
    timeout: Optional[int] = None,
) -> subprocess.CompletedProcess:
    logger.info(f"Running: {' '.join(shlex.quote(c) for c in cmd)}")
    timed_out = False
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        stdout, stderr = proc.communicate()
        logger.error(
            "Command timed out after %ss: %s",
            timeout,
            " ".join(shlex.quote(c) for c in cmd),
        )

    result = subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
    result.timed_out = timed_out

    if result.returncode != 0 and not timed_out:
        logger.warning(
            "Command failed (rc=%s): %s\nSTDOUT: %s\nSTDERR: %s",
            result.returncode,
            " ".join(shlex.quote(c) for c in cmd),
            result.stdout.decode(errors="replace"),
            result.stderr.decode(errors="replace"),
        )
        if check:
            result.check_returncode()
    elif timed_out and check:
        raise subprocess.TimeoutExpired(cmd, timeout, output=stdout, stderr=stderr)

    return result


# Strip ANSI escape sequences (cursor positioning, color codes, line clearing)
_ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

# HandBrake progress regex: matches lines like
# "Encoding: task 1 of 1, 12.34 % (5.67 fps, avg 4.56 fps, ETA 01h23m45s)"
# Relaxed to allow integer percentages and varying spaces
_HB_PROGRESS_RE = re.compile(
    r"Encoding:.*?(\d+(?:\.\d+)?)\s*%"
    r"(?:\s*\((\d+(?:\.\d+)?)\s*fps,\s*avg\s*(\d+(?:\.\d+)?)\s*fps,\s*ETA\s*(\S+)\))?"
)


class StateLoggingHandler(logging.Handler):
    """Custom logging handler that sends log records to WatchdogState."""

    def __init__(self, state):
        super().__init__()
        self.state = state
        self.setFormatter(LOG_FORMATTER)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.state.append_log_line(msg)
        except Exception:
            self.handleError(record)


def run_handbrake(
    logger: logging.Logger,
    cmd: list,
    timeout: Optional[int] = None,
    state=None,
) -> subprocess.CompletedProcess:
    """Run HandBrakeCLI, streaming stderr to parse progress updates.

    Uses a pseudo-TTY for stderr so HandBrake outputs real-time progress
    (it suppresses \\r-based progress when stderr is a pipe).

    Returns a CompletedProcess compatible with run_cmd() so downstream code
    is unchanged. Updates `state` (WatchdogState) with live progress if provided.
    """
    import errno
    import select

    logger.info(f"Running: {' '.join(shlex.quote(c) for c in cmd)}")
    timed_out = False

    # Create a pseudo-TTY so HandBrake thinks stderr is a terminal
    master_fd, slave_fd = os.openpty()

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,  # Prevent deadlock from unread stdout
            stderr=slave_fd,
        )
    except Exception:
        os.close(slave_fd)
        os.close(master_fd)
        raise
    # Close the slave end in the parent — the child owns it now
    os.close(slave_fd)

    stderr_chunks = []
    line_buf = b""
    start = time.monotonic()

    def _parse_progress(line_bytes: bytes) -> None:
        """Parse a completed line for HandBrake progress and update state."""
        line_text = line_bytes.decode("utf-8", errors="replace")
        clean_text = _ANSI_ESCAPE_RE.sub('', line_text)
        # We generally don't want to flood the main log with every progress update,
        # but the dashboard "console" view (deque) can handle it.
        # However, since we now have StateLoggingHandler capturing all application logs,
        # we might duplicate output if we log everything.
        # But 'run_handbrake' is capturing stderr which ISN'T going through python logging.
        # So we explicitly add it here.
        if state:
            state.append_log_line(clean_text)
        m = _HB_PROGRESS_RE.search(clean_text)
        if m and state:
            pct = float(m.group(1))
            fps = float(m.group(2)) if m.group(2) else 0.0
            avg_fps = float(m.group(3)) if m.group(3) else 0.0
            eta = m.group(4) if m.group(4) else ""
            state.set_transcode_progress(pct, fps, avg_fps, eta)

    try:
        while True:
            if timeout and (time.monotonic() - start) > timeout:
                raise subprocess.TimeoutExpired(cmd, timeout)

            ready, _, _ = select.select([master_fd], [], [], 1.0)
            if not ready:
                if proc.poll() is not None:
                    # Process exited — drain remaining data from pty master
                    while True:
                        try:
                            chunk = os.read(master_fd, 4096)
                            if not chunk:
                                break
                        except OSError:
                            break
                        for ch in chunk:
                            byte = bytes([ch])
                            if byte in (b"\r", b"\n"):
                                if line_buf:
                                    _parse_progress(line_buf)
                                    stderr_chunks.append(line_buf)
                                    stderr_chunks.append(byte)
                                    line_buf = b""
                            else:
                                line_buf += byte
                    break
                continue

            try:
                chunk = os.read(master_fd, 4096)
            except OSError as e:
                if e.errno == errno.EIO:
                    # EIO means the slave side closed — process exited
                    break
                raise
            if not chunk:
                break

            for ch in chunk:
                byte = bytes([ch])
                if byte in (b"\r", b"\n"):
                    if line_buf:
                        _parse_progress(line_buf)
                        stderr_chunks.append(line_buf)
                        stderr_chunks.append(byte)
                        line_buf = b""
                else:
                    line_buf += byte

        proc.wait()
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        proc.wait()
        logger.error(
            "Command timed out after %ss: %s",
            timeout,
            " ".join(shlex.quote(c) for c in cmd),
        )
    finally:
        os.close(master_fd)

    # Parse any unterminated final line for progress
    if line_buf:
        _parse_progress(line_buf)

    stdout = proc.stdout.read() if proc.stdout else b""
    if proc.stdout:
        proc.stdout.close()
    stderr_bytes = b"".join(stderr_chunks)
    if line_buf:
        stderr_bytes += line_buf

    result = subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr_bytes)
    result.timed_out = timed_out

    if result.returncode != 0 and not timed_out:
        logger.warning(
            "Command failed (rc=%s): %s\nSTDERR (last 500 chars): %s",
            result.returncode,
            " ".join(shlex.quote(c) for c in cmd),
            result.stderr.decode(errors="replace")[-500:],
        )

    return result


def check_free_space(logger: logging.Logger, path: str, required_bytes: int) -> bool:
    """Return True if `path` has at least `required_bytes` free."""
    try:
        usage = shutil.disk_usage(path)
        if usage.free < required_bytes:
            logger.warning(
                "Insufficient disk space on %s: need %s, have %s",
                path,
                format_bytes(required_bytes),
                format_bytes(usage.free),
            )
            return False
        return True
    except OSError as e:
        logger.warning("Failed to check disk space for %s: %s", path, e)
        return False


def ffprobe_json(logger: logging.Logger, path: str) -> Optional[dict]:
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
    ]
    res = run_cmd(logger, cmd)
    if res.returncode != 0:
        return None
    try:
        return json.loads(res.stdout.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None


def inspect_file(
    logger: logging.Logger,
    file_path: str,
    inspected_log_path: str,
    db=None,
) -> bool:
    info = ffprobe_json(logger, file_path)
    if not info:
        logger.info(f"Inspection failed to read metadata; queueing for transcode: {file_path}")
        return True

    needs_transcode, reasons, bitrate_mbps, video_codec = evaluate_transcode_need(info)

    if not needs_transcode:
        bitrate_text = f"{bitrate_mbps:.2f} Mbps" if bitrate_mbps else "unknown bitrate"
        logger.info(
            f"PASS: {file_path} (codec={video_codec}, bitrate={bitrate_text} <= {MAX_AVERAGE_BITRATE_MBPS:.2f} Mbps)"
        )
        if db:
            try:
                st = os.stat(file_path)
                db.mark_inspected(file_path, st.st_size, st.st_mtime)
            except OSError:
                pass
        else:
            append_inspected_file(inspected_log_path, file_path)
        return False

    logger.info(f"QUEUE: {file_path} (reasons: {', '.join(reasons) or 'unknown'})")
    return True


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_transcode_need(info: dict) -> Tuple[bool, List[str], float, Optional[str]]:
    streams = info.get("streams", [])
    fmt = info.get("format", {})

    video_codec = None
    stream_bitrate_bps = 0
    for stream in streams:
        if stream.get("codec_type") == "video":
            video_codec = stream.get("codec_name")
            stream_bitrate_bps = _safe_int(stream.get("bit_rate"))
            break

    size_bytes = _safe_int(fmt.get("size", 0))
    duration_seconds = _safe_float(fmt.get("duration", 0.0))
    format_bitrate_bps = _safe_int(fmt.get("bit_rate", 0))

    bitrate_bps = format_bitrate_bps or stream_bitrate_bps
    if bitrate_bps == 0 and size_bytes > 0 and duration_seconds > 0:
        bitrate_bps = int((size_bytes * 8) / duration_seconds)

    bitrate_mbps = bitrate_bps / 1_000_000 if bitrate_bps else 0.0
    bitrate_limit_mbps = float(MAX_AVERAGE_BITRATE_MBPS)

    reasons: List[str] = []
    if video_codec != TARGET_CODEC:
        reasons.append(f"codec is {video_codec}")

    if bitrate_mbps == 0:
        reasons.append("unable to determine bitrate")
    elif bitrate_mbps > bitrate_limit_mbps:
        reasons.append(
            f"average bitrate {bitrate_mbps:.2f} Mbps exceeds limit {bitrate_limit_mbps:.2f} Mbps"
        )

    needs_transcode = bool(reasons)
    return needs_transcode, reasons, bitrate_mbps, video_codec


def verify_transcode(logger: logging.Logger, original_path: str, new_path: str) -> bool:
    # Health check
    health = run_cmd(logger, ["ffprobe", "-v", "error", "-hide_banner", new_path])
    if health.returncode != 0:
        logger.error(f"Health check failed for {new_path}")
        return False

    orig = ffprobe_json(logger, original_path)
    new = ffprobe_json(logger, new_path)
    if not orig or not new:
        logger.error("Failed to read metadata for verification")
        return False

    def extract_meta(meta: dict):
        fmt = meta.get("format", {})
        streams = meta.get("streams", [])
        try:
            duration = float(fmt.get("duration", 0))
        except (TypeError, ValueError):
            duration = 0.0
        v = sum(1 for s in streams if s.get("codec_type") == "video")
        a = sum(1 for s in streams if s.get("codec_type") == "audio")
        sub = sum(1 for s in streams if s.get("codec_type") == "subtitle")
        return duration, v, a, sub

    d1, v1, a1, s1 = extract_meta(orig)
    d2, v2, a2, s2 = extract_meta(new)

    if d1 == 0.0:
        logger.error(
            "MANUAL ENCODING REQUIRED: original duration is 0.0s (ffprobe couldn't "
            "extract it) for %s — rejecting automatic transcode to protect the file",
            original_path,
        )
        return False
    if abs(d1 - d2) > 1.0:
        logger.error(f"Duration mismatch: original={d1:.3f}s new={d2:.3f}s")
        return False
    # Enforce only video/audio stream count parity; subtitle differences are allowed
    if (v1, a1) != (v2, a2):
        logger.error(
            f"Stream count mismatch (video/audio): orig(v{v1},a{a1}) vs new(v{v2},a{a2})"
        )
        return False
    if s1 != s2:
        logger.info(
            f"Subtitle track count changed: orig s{s1} -> new s{s2} (allowed)"
        )
    return True


def safe_replace(logger: logging.Logger, source_path: str, new_local_path: str) -> bool:
    source_dir = os.path.dirname(source_path)
    source_filename = os.path.basename(source_path)
    temp_remote_path = os.path.join(source_dir, f"{source_filename}.tmp")
    old_remote_path = os.path.join(source_dir, f"{source_filename}.old")

    try:
        # Step 1: rsync new file to temp path on remote
        rsync_cmd = [
            "rsync",
            "-avh",
            new_local_path,
            temp_remote_path,
        ]
        # Timeout: allow at least 1 MB/s, minimum 5 minutes
        try:
            rsync_size = os.path.getsize(new_local_path)
        except OSError:
            rsync_size = 0
        rsync_timeout = max(300, rsync_size // (1024 * 1024)) if rsync_size > 0 else 3600
        res = run_cmd(logger, rsync_cmd, timeout=rsync_timeout)
        if res.returncode != 0:
            logger.error("rsync to temp failed")
            return False

        # Verify the temp file actually landed
        if not os.path.exists(temp_remote_path):
            logger.error("rsync reported success but temp file missing on remote")
            return False

        # Step 2: move original aside
        os.rename(source_path, old_remote_path)

        # Step 3: move new file into place
        try:
            os.rename(temp_remote_path, source_path)
        except OSError as e:
            # Swap failed — rollback: restore original from .old
            logger.critical(f"Rename temp->source failed ({e}); rolling back from .old")
            try:
                os.rename(old_remote_path, source_path)
                logger.info("Rollback succeeded — original file restored")
            except OSError as rb_err:
                logger.critical(
                    f"ROLLBACK FAILED — original at {old_remote_path}, "
                    f"source path missing: {rb_err}"
                )
            return False

        # Step 4: remove old file (non-fatal if this fails)
        try:
            os.remove(old_remote_path)
        except OSError as e:
            logger.warning(f"Failed to remove .old backup ({old_remote_path}): {e}")

        return True
    except Exception as e:
        logger.critical(f"Safe replace failed: {e}")
        # Best-effort rollback: cover both possible dangling states
        try:
            if not os.path.exists(source_path) and os.path.exists(old_remote_path):
                os.rename(old_remote_path, source_path)
                logger.info("Rollback succeeded — original file restored from .old")
            elif not os.path.exists(source_path) and os.path.exists(temp_remote_path):
                os.rename(temp_remote_path, source_path)
                logger.info("Rollback succeeded — placed temp file as source")
        except Exception as rb_err:
            logger.critical(f"Rollback also failed: {rb_err}")
        return False


def run_watchdog_pass(
    base_dir: str,
    logger: logging.Logger,
    stats: StatisticsTracker,
    nfs_manager: NFSManager,
    state=None,
    db=None,
    dry_run: bool = False,
) -> None:
    # Start a fresh activity log for this run
    _rotate_activity_log(base_dir, logger)
    stats.start_pass()
    stats.log_event("Starting watchdog scan pass")

    # Clean up any incomplete rows left from a previous crash/restart
    if db:
        stale = db.close_stale_transcodes()
        if stale:
            logger.info(f"Cleaned up {stale} stale transcode row(s) from previous run")

    if state:
        state.set_state("scanning")

    nfs_manager.ensure_mounts()

    if state:
        state.set_nfs_healthy(True)

    preset_path = resolve_path(base_dir, HANDBRAKE_PRESET_FILE)
    inspected_log_path = resolve_path(base_dir, INSPECTED_FILES_LOG)
    ensure_dir(os.path.dirname(inspected_log_path))
    temp_dir = os.path.abspath(os.path.expanduser(TRANSCODE_TEMP_PATH))
    ensure_dir(temp_dir)

    # Use DB-backed inspected files if available, fall back to flat file
    using_db = db is not None
    if not using_db:
        inspected_files = load_inspected_files(inspected_log_path)
        logger.info(f"Loaded {len(inspected_files)} previously inspected files")
    else:
        logger.info(f"Using database for inspected files ({db.get_inspected_count()} entries)")

    scannable_shares: List[Dict[str, str]] = []
    for share in NFS_SHARES:
        resolved = os.path.abspath(os.path.expanduser(share["local_mount"]))
        if not os.path.isdir(resolved):
            warning = (
                f"Media directory unavailable for share '{share['name']}': {resolved}"
            )
            logger.warning(warning)
            stats.log_event(warning, level=logging.WARNING)
            continue
        scannable_shares.append({"name": share["name"], "path": resolved})

    if not scannable_shares:
        raise RuntimeError("No media directories available for scanning")

    transcode_queue: List[Tuple[str, str, int]] = []
    discovered_count = 0
    for share_name, media_path in scan_media_shares(
        logger, stats, scannable_shares, VIDEO_EXTENSIONS
    ):
        discovered_count += 1

        # Check if already inspected
        if using_db:
            try:
                st = os.stat(media_path)
                if db.is_inspected(media_path, st.st_size, st.st_mtime):
                    logger.info(f"[{share_name}] SKIP inspected (db): {media_path}")
                    continue
            except OSError:
                pass
        else:
            if media_path in inspected_files:
                logger.info(f"[{share_name}] SKIP inspected: {media_path}")
                continue

        try:
            needs_transcode = inspect_file(logger, media_path, inspected_log_path, db=db)
            stats.increment("files_inspected")
        except Exception as e:
            logger.error(f"[{share_name}] Inspection error for {media_path}: {e}")
            needs_transcode = True
        if needs_transcode:
            try:
                file_size = os.path.getsize(media_path)
            except OSError:
                file_size = 0
            transcode_queue.append((share_name, media_path, file_size))
            stats.increment("files_queued")
        elif not using_db:
            inspected_files.add(media_path)

    # Sort queue largest files first
    transcode_queue.sort(key=lambda x: x[2], reverse=True)

    logger.info(f"Discovered {discovered_count} candidate files before filtering")
    logger.info(f"Queue length: {len(transcode_queue)}")

    if state:
        state.set_queue_info(0, len(transcode_queue))

    # --- Dry-run mode: report queue and exit ---
    if dry_run:
        logger.info("=== DRY RUN MODE ===")
        target_bitrate_bps = 6_000_000  # 6 Mbps target for estimation
        for idx, (share_name, source_path, _size) in enumerate(transcode_queue, 1):
            info = ffprobe_json(logger, source_path)
            codec = "unknown"
            bitrate_mbps = 0.0
            size_bytes = 0
            est_output = 0
            if info:
                _, _, bitrate_mbps, codec = evaluate_transcode_need(info)
                fmt = info.get("format", {})
                size_bytes = _safe_int(fmt.get("size", 0))
                duration_s = _safe_float(fmt.get("duration", 0))
                if duration_s > 0:
                    est_output = int((target_bitrate_bps * duration_s) / 8)
            logger.info(
                "[DRY-RUN %d/%d] %s | share=%s codec=%s bitrate=%.2f Mbps "
                "size=%s est_output=%s",
                idx,
                len(transcode_queue),
                source_path,
                share_name,
                codec,
                bitrate_mbps,
                format_bytes(size_bytes),
                format_bytes(est_output),
            )
        stats.commit()
        return

    # --- Normal transcode loop ---
    if state:
        state.set_state("transcoding")

    attempt_counts: Dict[str, int] = {}
    initial_queue_length = len(transcode_queue)
    processed_attempts = 0

    while transcode_queue:
        share_name, source_path, _queued_size = transcode_queue.pop(0)
        attempt_counts[source_path] = attempt_counts.get(source_path, 0) + 1
        attempt_num = attempt_counts[source_path]
        processed_attempts += 1

        # Keep denominator stable-ish for logging while still reflecting retries
        progress_total = max(initial_queue_length, processed_attempts + len(transcode_queue))
        progress_msg = (
            f"------ Beginning analysis on item {processed_attempts}/{progress_total}: "
            f"{source_path} (attempt {attempt_num}/{TRANSCODE_MAX_RETRIES + 1}) ------"
        )
        logger.info(progress_msg)
        stats.log_event(progress_msg)

        if state:
            state.set_current_file(source_path)
            state.set_queue_info(processed_attempts, progress_total)
            state.set_transcode_progress(0.0)

        failure_recorded = False

        def record_failure():
            nonlocal failure_recorded
            if not failure_recorded:
                stats.increment("transcode_failures")
                failure_recorded = True

        # Get file metadata for DB recording
        file_info = ffprobe_json(logger, source_path)
        original_codec = None
        original_bitrate_bps = 0
        if file_info:
            _, _, bm, vc = evaluate_transcode_need(file_info)
            original_codec = vc
            original_bitrate_bps = int(bm * 1_000_000) if bm else 0

        source_name = os.path.basename(source_path)
        local_source_path = os.path.join(temp_dir, source_name)
        name_no_ext, _ext = os.path.splitext(source_name)
        local_output_path = os.path.join(temp_dir, f"{name_no_ext}.av1.mkv")

        # Record transcode start in DB
        db_row_id = None
        transcode_start_time = time.monotonic()
        if db:
            try:
                original_size = os.path.getsize(source_path)
            except OSError:
                original_size = 0
            db_row_id = db.record_transcode_start(
                source_path, share_name, original_codec,
                original_bitrate_bps, original_size,
            )

        try:
            # Disk space check before rsync
            try:
                source_size = os.path.getsize(source_path)
            except OSError:
                source_size = 0
            if source_size > 0:
                required = int(source_size * MIN_FREE_SPACE_MULTIPLIER)
                if not check_free_space(logger, temp_dir, required):
                    logger.warning(f"[{share_name}] Skipping {source_path}: insufficient temp space")
                    record_failure()
                    if db and db_row_id:
                        duration = time.monotonic() - transcode_start_time
                        db.record_transcode_end(db_row_id, False, failure_reason="insufficient temp space", duration_seconds=duration)
                    continue
                source_dir_path = os.path.dirname(source_path)
                if not check_free_space(logger, source_dir_path, required):
                    logger.warning(f"[{share_name}] Skipping {source_path}: insufficient destination space")
                    record_failure()
                    if db and db_row_id:
                        duration = time.monotonic() - transcode_start_time
                        db.record_transcode_end(db_row_id, False, failure_reason="insufficient destination space", duration_seconds=duration)
                    continue

            rsync_cmd = [
                "rsync",
                "-avh",
                "--progress",
                source_path,
                local_source_path,
            ]
            # Timeout: allow at least 1 MB/s transfer, minimum 5 minutes
            rsync_timeout = max(300, source_size // (1024 * 1024)) if source_size > 0 else 3600
            res = run_cmd(logger, rsync_cmd, timeout=rsync_timeout)
            if res.returncode != 0:
                record_failure()
                logger.error(f"[{share_name}] Failed to rsync source: {source_path}")
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="rsync failed", duration_seconds=duration)
                continue

            hb_cmd = [
                "HandBrakeCLI",
                "--preset-import-file",
                preset_path,
                "-i",
                local_source_path,
                "-o",
                local_output_path,
                "--preset",
                HANDBRAKE_PRESET_NAME,
            ]
            hb_res = run_handbrake(
                logger,
                hb_cmd,
                timeout=TRANSCODE_TIMEOUT_SECONDS,
                state=state,
            )

            if getattr(hb_res, "timed_out", False):
                timeout_msg = (
                    f"[{share_name}] Transcode timed out after {TRANSCODE_TIMEOUT_SECONDS}s: {source_path}"
                )
                logger.warning(timeout_msg)
                stats.log_event(timeout_msg, level=logging.WARNING)

                if attempt_num <= TRANSCODE_MAX_RETRIES:
                    retry_msg = (
                        f"[{share_name}] Requeueing after timeout "
                        f"(attempt {attempt_num}/{TRANSCODE_MAX_RETRIES + 1}): {source_path}"
                    )
                    logger.info(retry_msg)
                    stats.log_event(retry_msg)
                    transcode_queue.append((share_name, source_path, 0))
                    if db and db_row_id:
                        duration = time.monotonic() - transcode_start_time
                        db.record_transcode_end(db_row_id, False, failure_reason="timeout (requeued)", duration_seconds=duration)
                    continue

                exhaustion_msg = (
                    f"[{share_name}] Timeout retries exhausted for {source_path}"
                )
                logger.error(exhaustion_msg)
                stats.log_event(exhaustion_msg, level=logging.ERROR)
                record_failure()
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="timeout exhausted", duration_seconds=duration)
                continue

            if hb_res.returncode != 0 or not os.path.exists(local_output_path):
                record_failure()
                logger.error(f"[{share_name}] Transcode failed for {source_path}")
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="handbrake failed", duration_seconds=duration)
                continue

            if not verify_transcode(logger, local_source_path, local_output_path):
                record_failure()
                logger.error(f"[{share_name}] Verification failed for {source_path}")
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="verification failed", duration_seconds=duration)
                continue

            try:
                original_size = os.path.getsize(local_source_path)
                new_size = os.path.getsize(local_output_path)
            except OSError as e:
                record_failure()
                logger.error(f"[{share_name}] Failed to stat files: {e}")
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="stat failed", duration_seconds=duration)
                continue

            if new_size >= original_size:
                logger.info(
                    f"[{share_name}] Not space-efficient (new {new_size} >= orig {original_size}); "
                    "skipping replace"
                )
                if db:
                    try:
                        st = os.stat(source_path)
                        db.mark_inspected(source_path, st.st_size, st.st_mtime)
                    except OSError:
                        pass
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(
                        db_row_id, True, output_size=new_size, space_saved=0,
                        duration_seconds=duration,
                    )
                else:
                    append_inspected_file(inspected_log_path, source_path)
                    inspected_files.add(source_path)
                continue

            if not safe_replace(logger, source_path, local_output_path):
                record_failure()
                logger.error(f"[{share_name}] Safe replace failed for {source_path}")
                if db and db_row_id:
                    duration = time.monotonic() - transcode_start_time
                    db.record_transcode_end(db_row_id, False, failure_reason="safe replace failed", duration_seconds=duration)
                continue

            stats.increment("files_transcoded")
            saved_bytes = original_size - new_size
            stats.add_space_saved(saved_bytes)

            if db:
                try:
                    st = os.stat(source_path)
                    db.mark_inspected(source_path, st.st_size, st.st_mtime)
                except OSError:
                    pass
                duration = time.monotonic() - transcode_start_time
                db.record_transcode_end(
                    db_row_id, True, output_size=new_size,
                    space_saved=saved_bytes, duration_seconds=duration,
                )
                cumulative = stats._snapshot_totals().get("space_saved_bytes", 0)
                db.log_space_saved(cumulative)
            else:
                append_inspected_file(inspected_log_path, source_path)
                inspected_files.add(source_path)

            logger.info(
                f"[{share_name}] SUCCESS: Replaced {source_path} | Saved {format_bytes(saved_bytes)} "
                f"({saved_bytes} bytes)"
            )

        except Exception as e:
            record_failure()
            logger.exception(f"Unhandled error processing {source_path}: {e}")
            if db and db_row_id:
                duration = time.monotonic() - transcode_start_time
                db.record_transcode_end(db_row_id, False, failure_reason=str(e), duration_seconds=duration)
        finally:
            for temp_file in (local_source_path, local_output_path):
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except Exception as cleanup_err:
                        logger.warning(f"Failed to clean up temp file {temp_file}: {cleanup_err}")

    if state:
        state.set_state("idle")
        state.set_last_pass_time()

    stats.commit()


def main():
    parser = argparse.ArgumentParser(description="Jellyfin AV1 Transcoding Watchdog")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and report what would be transcoded, then exit",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Dashboard port override (default: {DASHBOARD_PORT})",
    )
    args = parser.parse_args()
    port = args.port or DASHBOARD_PORT

    base_dir = os.path.dirname(os.path.abspath(__file__))
    logger = setup_logging(base_dir)
    stats = StatisticsTracker(base_dir)
    nfs_manager = NFSManager(logger, stats)

    logger.info("Starting Jellyfin AV1 Transcoding Watchdog")

    # Config validation
    from validation import validate_config
    errors = validate_config(logger)
    if errors:
        logger.critical("Configuration validation failed with %d error(s); exiting.", len(errors))
        sys.exit(1)

    if not verify_dependencies(logger):
        logger.critical("Dependency check failed; exiting.")
        sys.exit(1)

    # Initialize database
    from db import WatchdogDB
    db_path = resolve_path(base_dir, DATABASE_PATH)
    db = WatchdogDB(db_path)
    logger.info(f"Database initialized at {db_path}")

    # Migrate inspected files from flat file to DB
    inspected_log_path = resolve_path(base_dir, INSPECTED_FILES_LOG)
    if os.path.exists(inspected_log_path):
        count = db.migrate_inspected_files(inspected_log_path, logger)
        logger.info(f"Migrated {count} inspected files from log to database")

    # Initialize shared state
    from shared_state import WatchdogState
    state = WatchdogState()

    # Attach state logger to capture all application logs
    state_handler = StateLoggingHandler(state)
    logger.addHandler(state_handler)

    # Start dashboard (unless dry-run)
    if not args.dry_run:
        from dashboard import start_dashboard_thread
        start_dashboard_thread(state, db, stats, port=port)
        logger.info(f"Dashboard started on http://0.0.0.0:{port}/")

    if args.dry_run:
        logger.info("Running in dry-run mode")
        try:
            run_watchdog_pass(base_dir, logger, stats, nfs_manager,
                              state=state, db=db, dry_run=True)
        except Exception as exc:
            logger.exception(f"Dry-run failed: {exc}")
            sys.exit(1)
        return

    retry_delay = 30
    try:
        while True:
            try:
                run_watchdog_pass(base_dir, logger, stats, nfs_manager,
                                  state=state, db=db)
                retry_delay = 30
                time.sleep(SCAN_INTERVAL_SECONDS)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                message = f"Watchdog pass failed: {exc}"
                logger.exception(message)
                stats.record_run_failure(message)
                if state:
                    state.set_nfs_healthy(False)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
    except KeyboardInterrupt:
        logger.info("Shutting down on user request")
    finally:
        db.close_connection()


if __name__ == "__main__":
    main()
