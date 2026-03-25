#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$ROOT_DIR/.test_videos"
WORK_DIR="$ROOT_DIR/.local_tui_run"
KEEP_WORK_DIR=0
PREPARE_ONLY=0
HEADLESS_ONCE=0
PRESET_FILE="${WATCHDOG_REAL_PRESET_FILE:-"$ROOT_DIR/.watchdog/presets/AV1_MKV.json"}"
PRESET_NAME="${WATCHDOG_REAL_PRESET_NAME:-AV1_MKV}"
SCAN_INTERVAL="${WATCHDOG_LOCAL_TUI_SCAN_INTERVAL:-120}"
HEADLESS_LOG_LEVEL="${WATCHDOG_LOCAL_TUI_HEADLESS_LOG_LEVEL:-info}"

print_usage() {
  cat <<'USAGE'
Usage: ./.dev/local_tui.sh [video_dir]

Options:
  --video-dir <path>   Source videos to clone into the local TUI sandbox
  --keep-work-dir      Keep existing .local_tui_run contents
  --prepare-only       Prepare sandbox/config, then exit (do not run watchdog)
  --headless-once      Run one pass in headless mode instead of interactive TUI
  -h, --help           Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --video-dir)
      if [[ $# -lt 2 ]]; then
        echo "--video-dir requires a value" >&2
        exit 1
      fi
      SOURCE_DIR="$2"
      shift 2
      ;;
    --keep-work-dir)
      KEEP_WORK_DIR=1
      shift
      ;;
    --prepare-only)
      PREPARE_ONLY=1
      shift
      ;;
    --headless-once)
      HEADLESS_ONCE=1
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      SOURCE_DIR="$1"
      shift
      ;;
  esac
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

toml_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

require_cmd cargo
require_cmd rsync
require_cmd ffprobe
require_cmd HandBrakeCLI
if [[ "$HEADLESS_ONCE" -eq 1 ]]; then
  require_cmd sqlite3
fi

if [[ "$PREPARE_ONLY" -eq 1 && "$HEADLESS_ONCE" -eq 1 ]]; then
  echo "Use either --prepare-only or --headless-once, not both" >&2
  exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "Source video directory not found: $SOURCE_DIR" >&2
  exit 1
fi

if [[ ! -f "$PRESET_FILE" ]]; then
  echo "Preset file not found: $PRESET_FILE" >&2
  exit 1
fi

INGEST_DIR="$WORK_DIR/ingest_tmp"
MEDIA_DIR="$WORK_DIR/media_share"
TMP_DIR="$WORK_DIR/transcode_tmp"
LOG_DIR="$WORK_DIR/logs"
DB_PATH="$WORK_DIR/watchdog.local_tui.db"
PAUSE_FILE="$WORK_DIR/watchdog.pause"
STATUS_PATH="$WORK_DIR/status.json"
EVENT_PATH="$WORK_DIR/events.ndjson"
CONFIG_PATH="$WORK_DIR/watchdog.local_tui.toml"

if [[ "$KEEP_WORK_DIR" -eq 0 ]]; then
  rm -rf "$WORK_DIR"
fi
mkdir -p "$INGEST_DIR" "$MEDIA_DIR" "$TMP_DIR" "$LOG_DIR"

echo "==> Preparing local TUI sandbox"
echo "    source: $SOURCE_DIR"
echo "    sandbox: $WORK_DIR"

echo "==> Staging videos (rsync hop 1: source -> ingest_tmp)"
rsync -a --delete --checksum --itemize-changes --stats --exclude ".DS_Store" "$SOURCE_DIR"/ "$INGEST_DIR"/

echo "==> Staging videos (rsync hop 2: ingest_tmp -> media_share)"
rsync -a --delete --checksum --itemize-changes --stats --exclude ".DS_Store" "$INGEST_DIR"/ "$MEDIA_DIR"/

VIDEO_COUNT="$(find "$MEDIA_DIR" -type f ! -name ".*" | wc -l | tr -d ' ')"
if [[ "$VIDEO_COUNT" -eq 0 ]]; then
  echo "No input files found in: $SOURCE_DIR" >&2
  exit 1
fi
MEDIA_SIZE="$(du -sh "$MEDIA_DIR" | awk '{print $1}')"
echo "    staged files in media_share: $VIDEO_COUNT"
echo "    staged media size: $MEDIA_SIZE"

ESC_SOURCE_DIR="$(toml_escape "$SOURCE_DIR")"
ESC_MEDIA_DIR="$(toml_escape "$MEDIA_DIR")"
ESC_PRESET_FILE="$(toml_escape "$PRESET_FILE")"
ESC_TMP_DIR="$(toml_escape "$TMP_DIR")"
ESC_DB_PATH="$(toml_escape "$DB_PATH")"
ESC_LOG_DIR="$(toml_escape "$LOG_DIR")"
ESC_PAUSE_FILE="$(toml_escape "$PAUSE_FILE")"
ESC_STATUS_PATH="$(toml_escape "$STATUS_PATH")"
ESC_EVENT_PATH="$(toml_escape "$EVENT_PATH")"
ESC_PRESET_NAME="$(toml_escape "$PRESET_NAME")"

cat > "$CONFIG_PATH" <<EOF
local_mode = true

[nfs]
server = ""

[[shares]]
name = "local_test_videos"
remote_path = ""
local_mount = "$ESC_MEDIA_DIR"

[transcode]
max_average_bitrate_mbps = 25.0
target_codec = "av1"
preset_file = "$ESC_PRESET_FILE"
preset_name = "$ESC_PRESET_NAME"
timeout_seconds = 18000
max_retries = 1
min_free_space_multiplier = 2.0

[scan]
video_extensions = [".mkv", ".mp4", ".avi", ".mov", ".webm"]
interval_seconds = $SCAN_INTERVAL
include_globs = []
exclude_globs = []
max_files_per_pass = 0
probe_workers = 0

[safety]
min_file_age_seconds = 0
pause_file = "$ESC_PAUSE_FILE"
max_failures_before_cooldown = 3
cooldown_base_seconds = 300
cooldown_max_seconds = 86400
in_use_guard_enabled = false
in_use_guard_command = "lsof"
recovery_scan_interval_seconds = 43200
share_scan_timeout_seconds = 180
max_consecutive_pass_failures = 3
auto_pause_on_pass_failures = true
quarantine_after_failures = 8
quarantine_failure_codes = [
  "transcode_failed",
  "transcode_error",
  "verification_failed",
  "verification_error",
  "safe_replace_failed",
  "safe_replace_error",
  "source_changed_during_transcode",
]
status_snapshot_stale_seconds = 30

[notify]
webhook_url = ""
events = []
timeout_seconds = 5

[paths]
transcode_temp = "$ESC_TMP_DIR"
database = "$ESC_DB_PATH"
log_dir = "$ESC_LOG_DIR"
status_snapshot = "$ESC_STATUS_PATH"
event_journal = "$ESC_EVENT_PATH"
EOF

echo "==> Building release binary"
cd "$ROOT_DIR"
cargo build --release

if [[ "$PREPARE_ONLY" -eq 1 ]]; then
  echo "==> Sandbox prepared (prepare-only mode)"
  echo "    config: $CONFIG_PATH"
  exit 0
fi

if [[ "$HEADLESS_ONCE" -eq 1 ]]; then
  echo "==> Launching headless once pass"
  echo "    config: $CONFIG_PATH"
  RUST_LOG="$HEADLESS_LOG_LEVEL" cargo run --release -- --config "$CONFIG_PATH" --once --headless
  echo
  echo "==> Headless pass summary (transcode_history by outcome)"
  sqlite3 "$DB_PATH" "select outcome, count(*) from transcode_history group by outcome order by count(*) desc;"
  exit 0
fi

echo "==> Launching TUI"
echo "    config: $CONFIG_PATH"
echo "    videos staged: $VIDEO_COUNT"
echo "    press q to quit"

cargo run --release -- --config "$CONFIG_PATH"
