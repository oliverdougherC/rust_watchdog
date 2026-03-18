#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VIDEO_DIR="$ROOT_DIR/.test_videos"
PRESET_FILE="${WATCHDOG_REAL_PRESET_FILE:-"$ROOT_DIR/.watchdog/presets/AV1_MKV.json"}"
PRESET_NAME="${WATCHDOG_REAL_PRESET_NAME:-AV1_MKV}"
RUN_SIM_ONCE="${WATCHDOG_RUN_SIM_ONCE:-0}"
VIDEO_DIR_SET=0
EXPECTS_TEST_ARG_VALUE=0
TEST_ARGS=()

print_usage() {
  cat <<'USAGE'
Usage: ./.dev/local_test.sh [video_dir] [cargo-test-args...]

Examples:
  ./.dev/local_test.sh
  ./.dev/local_test.sh .test_videos
  ./.dev/local_test.sh --test healthcheck_cli
  ./.dev/local_test.sh --video-dir .test_videos --test healthcheck_cli
  ./.dev/local_test.sh --full-sim-once

Options:
  --video-dir <path>   Override input video directory
  --test-videos        Alias for --video-dir .test_videos
  --test_videos        Alias for --video-dir .test_videos
  --full-sim-once      Run the slower full simulated once pass
  -h, --help           Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  arg="$1"
  shift

  if [[ "$EXPECTS_TEST_ARG_VALUE" -eq 1 ]]; then
    TEST_ARGS+=("$arg")
    EXPECTS_TEST_ARG_VALUE=0
    continue
  fi

  case "$arg" in
    -h|--help)
      print_usage
      exit 0
      ;;
    --video-dir)
      if [[ $# -eq 0 ]]; then
        echo "--video-dir requires a value" >&2
        exit 1
      fi
      VIDEO_DIR="$1"
      VIDEO_DIR_SET=1
      shift
      ;;
    --test-videos|--test_videos)
      VIDEO_DIR="$ROOT_DIR/.test_videos"
      VIDEO_DIR_SET=1
      ;;
    --full-sim-once)
      RUN_SIM_ONCE=1
      ;;
    --)
      TEST_ARGS+=("$@")
      break
      ;;
    -*)
      TEST_ARGS+=("$arg")
      case "$arg" in
        --test|--package|-p|--bin|--example|--features|--manifest-path|--target|--profile|--color|-j|--jobs)
          EXPECTS_TEST_ARG_VALUE=1
          ;;
      esac
      ;;
    *)
      if [[ "$VIDEO_DIR_SET" -eq 0 && "${#TEST_ARGS[@]}" -eq 0 ]]; then
        VIDEO_DIR="$arg"
        VIDEO_DIR_SET=1
      else
        TEST_ARGS+=("$arg")
      fi
      ;;
  esac
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

echo "==> Local watchdog test starting"
echo "    project: $ROOT_DIR"
echo "    videos:  $VIDEO_DIR"
echo "    preset:  $PRESET_FILE ($PRESET_NAME)"
if [[ "${#TEST_ARGS[@]}" -gt 0 ]]; then
  echo "    cargo test args: ${TEST_ARGS[*]}"
fi

require_cmd cargo
require_cmd ffprobe
require_cmd HandBrakeCLI

if [[ ! -d "$VIDEO_DIR" ]]; then
  echo "Video directory not found: $VIDEO_DIR" >&2
  exit 1
fi

if [[ ! -f "$PRESET_FILE" ]]; then
  echo "Preset file not found: $PRESET_FILE" >&2
  exit 1
fi

cd "$ROOT_DIR"

echo
echo "==> Building release artifacts"
cargo build --workspace --all-targets --release

echo
echo "==> Running project tests"
if [[ "${#TEST_ARGS[@]}" -gt 0 ]]; then
  cargo test --workspace --all-targets "${TEST_ARGS[@]}"
else
  cargo test --workspace --all-targets
fi

echo
if [[ "$RUN_SIM_ONCE" == "1" ]]; then
  echo "==> Running full simulated once pass (WATCHDOG_RUN_SIM_ONCE=1 or --full-sim-once)"
  cargo run --release -- --simulate --once --headless
else
  echo "==> Running quick simulated healthcheck"
  cargo run --release -- --simulate --healthcheck
fi

echo
echo "==> Real-tools transcoding smoke tests (one file at a time)"
video_count=0
while IFS= read -r -d '' video; do
  video_count=$((video_count + 1))
  echo
  echo "[$video_count] Testing transcode: $video"
  WATCHDOG_REAL_TOOLS=1 \
    WATCHDOG_REAL_INPUT="$video" \
    WATCHDOG_REAL_PRESET_FILE="$PRESET_FILE" \
    WATCHDOG_REAL_PRESET_NAME="$PRESET_NAME" \
    cargo test --release --test real_tools real_tools_transcode_smoke -- --exact --nocapture < /dev/null
done < <(find "$VIDEO_DIR" -type f ! -name ".*" -print0)

if [[ "$video_count" -eq 0 ]]; then
  echo "No input files found in: $VIDEO_DIR" >&2
  exit 1
fi

echo
echo "==> Local test completed successfully"
echo "    processed files: $video_count"
