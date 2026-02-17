"""Startup configuration validation."""

import json
import os
import subprocess
import logging
from typing import List


def validate_config(logger: logging.Logger) -> List[str]:
    """Validate configuration at startup. Returns a list of error messages (empty = OK)."""
    errors: List[str] = []

    # Import config values (after dotenv has loaded)
    try:
        from config import (
            NFS_SERVER,
            HANDBRAKE_PRESET_FILE,
            TRANSCODE_TEMP_PATH,
        )
    except Exception as e:
        errors.append(f"Failed to import config: {e}")
        return errors

    # Check required env vars
    for var in ("NFS_SERVER", "NFS_USERNAME", "NFS_PASSWORD"):
        val = os.environ.get(var, "")
        if not val:
            errors.append(f"Required environment variable {var} is not set. Add it to .env")

    # Check preset file exists and is valid JSON
    base_dir = os.path.dirname(os.path.abspath(__file__))
    preset_path = HANDBRAKE_PRESET_FILE
    if not os.path.isabs(preset_path):
        preset_path = os.path.join(base_dir, preset_path)
    if not os.path.isfile(preset_path):
        errors.append(f"HandBrake preset file not found: {preset_path}")
    else:
        try:
            with open(preset_path, "r", encoding="utf-8") as f:
                json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            errors.append(f"HandBrake preset file is not valid JSON: {e}")

    # Check temp directory exists and is writable
    temp_dir = os.path.abspath(os.path.expanduser(TRANSCODE_TEMP_PATH))
    if not os.path.isdir(temp_dir):
        errors.append(f"Transcode temp directory does not exist: {temp_dir}")
    elif not os.access(temp_dir, os.W_OK):
        errors.append(f"Transcode temp directory is not writable: {temp_dir}")

    # Check NFS server is pingable
    # macOS ping uses -W (milliseconds), Linux uses -W (seconds)
    import platform
    wait_flag = "3000" if platform.system() == "Darwin" else "3"
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", wait_flag, NFS_SERVER],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        if result.returncode != 0:
            errors.append(f"NFS server {NFS_SERVER} is not reachable (ping failed)")
    except (subprocess.TimeoutExpired, OSError):
        errors.append(f"NFS server {NFS_SERVER} is not reachable (ping timed out)")

    for err in errors:
        logger.error(f"Config validation: {err}")

    return errors
