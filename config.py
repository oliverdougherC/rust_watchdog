"""Configuration for the Jellyfin AV1 transcoding watchdog.

All paths can be absolute. If relative, they are resolved relative to the
directory containing this config file at runtime.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root
load_dotenv(Path(__file__).resolve().parent / ".env")

# Absolute directory for resolving relative config entries
BASE_DIR = Path(__file__).resolve().parent

# --- NFS + Media Paths ---
NFS_SERVER = os.environ.get("NFS_SERVER", "")
NFS_USERNAME = os.environ.get("NFS_USERNAME", "")
NFS_PASSWORD = os.environ.get("NFS_PASSWORD", "")

def _mount_path(suffix: str) -> str:
    """Build a macOS-friendly /Volumes mount path for a share."""
    return os.path.join("/Volumes", suffix)

# Define each share we need to mount and scan.
# local_mount paths are derived via `_mount_path` for clarity.
_SHARE_DEFINITIONS = [
    ("movies", "/mnt/DataStore/share/jellyfin/jfmedia/Movies", "JellyfinMovies"),
    ("tv", "/mnt/DataStore/share/jellyfin/jfmedia/TV", "JellyfinTV"),
]

NFS_SHARES = [
    {
        "name": name,
        "remote_path": remote_path,
        "local_mount": _mount_path(local_suffix),
    }
    for name, remote_path, local_suffix in _SHARE_DEFINITIONS
]

# Derived list used by legacy code paths; includes both movie + TV roots.
MEDIA_DIRECTORIES = [share["local_mount"] for share in NFS_SHARES]

# Local directory on the VM for temporary work
TRANSCODE_TEMP_PATH = "/Volumes/External/tmp/"

# Path to the HandBrake preset file
HANDBRAKE_PRESET_FILE = "AV1_MKV.json"
HANDBRAKE_PRESET_NAME = "AV1_MKV"

# Path to the state file for tracking inspected files
INSPECTED_FILES_LOG = "logs/inspected_files.log"

# Path to the statistics log + state file
STATISTICS_LOG = "logs/statistics.log"
STATISTICS_STATE_FILE = "statistics_state.json"

# --- Transcoding Rules ---
# Average bitrate in megabits per second. Files above this limit are transcoded.
MAX_AVERAGE_BITRATE_MBPS = 25.0

# Video codec to check for. Files with this codec are considered "passed".
TARGET_CODEC = "av1"

# --- File Extensions ---
# Video file extensions to scan for
VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".mov", ".webm")


## --- Watchdog Behavior ---
# Seconds to wait between full scan passes
SCAN_INTERVAL_SECONDS = 300

# Maximum allowed duration for a single HandBrakeCLI transcode before it is
# considered hung and retried later. Default is 5 hours.
TRANSCODE_TIMEOUT_SECONDS = 5 * 60 * 60

# Number of additional attempts allowed after the initial transcode try when a
# timeout occurs. A value of 1 means we will try at most twice total.
TRANSCODE_MAX_RETRIES = 1

# --- Dashboard / Database ---
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8095"))
DATABASE_PATH = os.environ.get("DATABASE_PATH", "watchdog.db")

# Minimum free space multiplier: require this many times the source file size
# available on both temp and destination before starting a transcode.
MIN_FREE_SPACE_MULTIPLIER = float(os.environ.get("MIN_FREE_SPACE_MULTIPLIER", "2.0"))
