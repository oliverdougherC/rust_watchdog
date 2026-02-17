#!/usr/bin/env python3
"""
scan_test.py

Run the watchdog discovery logic without performing any transcoding.
All candidate files and their reasons for transcoding are written to scan_test.log.
"""

import logging
import os
import sys
from typing import List, Dict

from config import (
    INSPECTED_FILES_LOG,
    NFS_SHARES,
    VIDEO_EXTENSIONS,
)

from main import (
    NFSManager,
    evaluate_transcode_need,
    ffprobe_json,
    load_inspected_files,
    scan_media_shares,
)

SCAN_TEST_LOG = "scan_test.log"


class ScanTestStats:
    """Minimal stats logger compatible with NFSManager + scan_media_shares."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_event(self, message: str, level: int = logging.INFO) -> None:
        self.logger.log(level, message)


def setup_scan_logger(base_dir: str) -> logging.Logger:
    logger = logging.getLogger("transcode_watchdog.scan_test")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    log_path = os.path.join(base_dir, SCAN_TEST_LOG)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


def resolve_shares(logger: logging.Logger) -> List[Dict[str, str]]:
    scannable = []
    for share in NFS_SHARES:
        resolved = os.path.abspath(os.path.expanduser(share["local_mount"]))
        if not os.path.isdir(resolved):
            logger.warning("Media directory unavailable for share '%s': %s", share["name"], resolved)
            continue
        scannable.append({"name": share["name"], "path": resolved})
    return scannable


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logger = setup_scan_logger(base_dir)
    logger.info("Starting scan-only watchdog test run")

    stats = ScanTestStats(logger)
    nfs_manager = NFSManager(logger, stats)

    try:
        nfs_manager.ensure_mounts()
    except RuntimeError as exc:
        logger.error("Mount verification failed: %s", exc)
        return 1

    inspected_log_path = os.path.join(base_dir, INSPECTED_FILES_LOG)
    inspected_files = load_inspected_files(inspected_log_path)
    logger.info("Loaded %d previously inspected files", len(inspected_files))

    scannable_shares = resolve_shares(logger)
    if not scannable_shares:
        logger.error("No media directories available for scanning; aborting.")
        return 1

    candidates = 0
    placeholder_log = os.devnull

    for share_name, media_path in scan_media_shares(
        logger, stats, scannable_shares, VIDEO_EXTENSIONS
    ):
        if media_path in inspected_files:
            continue

        info = ffprobe_json(logger, media_path)
        if not info:
            logger.info(
                "[SCAN-TEST] QUEUE: %s (share=%s, reason=metadata unavailable)",
                media_path,
                share_name,
            )
            candidates += 1
            continue

        needs_transcode, reasons, bitrate_mbps, video_codec = evaluate_transcode_need(info)
        if not needs_transcode:
            continue

        reason_text = ", ".join(reasons) or "unknown"
        bitrate_text = f"{bitrate_mbps:.2f} Mbps" if bitrate_mbps else "unknown"
        logger.info(
            "[SCAN-TEST] QUEUE: %s (share=%s, codec=%s, bitrate=%s, reasons=%s)",
            media_path,
            share_name,
            video_codec or "unknown",
            bitrate_text,
            reason_text,
        )
        candidates += 1

    logger.info("Scan-only test complete: %d candidates identified", candidates)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

