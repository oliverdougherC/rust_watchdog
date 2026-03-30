#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="${WATCHDOG_APP_ROOT:-/mnt/NVME/docker/appdata/watchdog}"
CONFIG_PATH="${WATCHDOG_CONFIG_PATH:-$APP_ROOT/watchdog.toml}"
INSTALL_BIN_DIR="$APP_ROOT/bin"
INSTALL_BIN_PATH="$INSTALL_BIN_DIR/watchdog"
PRESET_DIR="$APP_ROOT/presets"
STATE_DIR="$APP_ROOT/state"
TRANSCODE_DIR="$APP_ROOT/transcode"
LOG_DIR="$APP_ROOT/logs"
SERVICE_NAME="${WATCHDOG_SERVICE_NAME:-watchdog-personal-server}"
SYSTEMD_UNIT_PATH="/etc/systemd/system/$SERVICE_NAME.service"
TARGET_UID="${WATCHDOG_TARGET_UID:-10000}"
TARGET_GID="${WATCHDOG_TARGET_GID:-10000}"
TARGET_USER="${WATCHDOG_TARGET_USER:-apps}"
TARGET_GROUP="${WATCHDOG_TARGET_GROUP:-apps}"
RUST_LOG_LEVEL="${RUST_LOG:-info}"
DEFAULT_TEMPLATE="$ROOT_DIR/deploy/watchdog.personal-server.host.toml"

print_usage() {
  cat <<EOF
Usage: ./deploy/run-personal-server.sh <command> [watchdog args...]

Commands:
  bootstrap         Build/update the installed binary, presets, and persistent config
  doctor            Run --doctor against the persistent install
  healthcheck       Run --healthcheck against the persistent install
  status            Run --status against the persistent install
  status-json       Run --status-json against the persistent install
  once              Run one headless pass
  run               Run the persistent install in headless mode
  pause             Create the persistent pause file
  resume            Remove the persistent pause file
  install-service   Install/update a systemd unit and enable it
  restart-service   Restart the systemd unit
  service-status    Show the systemd unit status
  logs              Tail service logs with journalctl
  paths             Print the persistent install paths
  help              Show this help

Examples:
  ./deploy/run-personal-server.sh bootstrap
  ./deploy/run-personal-server.sh doctor
  ./deploy/run-personal-server.sh once
  ./deploy/run-personal-server.sh run
  sudo ./deploy/run-personal-server.sh install-service
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

ensure_template_exists() {
  if [[ ! -f "$DEFAULT_TEMPLATE" ]]; then
    echo "Missing host template: $DEFAULT_TEMPLATE" >&2
    exit 1
  fi
}

ensure_runtime_dirs() {
  mkdir -p "$APP_ROOT" "$INSTALL_BIN_DIR" "$PRESET_DIR" "$STATE_DIR" "$TRANSCODE_DIR" "$LOG_DIR"
}

set_directory_permissions_if_possible() {
  local dir
  for dir in "$APP_ROOT" "$INSTALL_BIN_DIR" "$PRESET_DIR" "$STATE_DIR" "$TRANSCODE_DIR" "$LOG_DIR"; do
    chmod 2775 "$dir" 2>/dev/null || true
  done

  if [[ "$(id -u)" -eq 0 ]]; then
    chown -R "$TARGET_UID:$TARGET_GID" "$APP_ROOT"
  fi
}

ensure_config() {
  if [[ ! -f "$CONFIG_PATH" ]]; then
    mkdir -p "$(dirname "$CONFIG_PATH")"
    install -m 664 "$DEFAULT_TEMPLATE" "$CONFIG_PATH"
  fi
}

warn_if_using_container_template() {
  if [[ -f "$CONFIG_PATH" ]] && grep -q 'transcode_temp = "/transcode"' "$CONFIG_PATH"; then
    echo "Config at $CONFIG_PATH still looks like the Docker template." >&2
    echo "Replace it with $DEFAULT_TEMPLATE or rerun after removing the old config." >&2
    exit 1
  fi
}

build_release_binary() {
  require_cmd cargo
  echo "==> Building release binary"
  (cd "$ROOT_DIR" && cargo build --release)
}

install_binary_and_presets() {
  echo "==> Installing runtime artifacts into $APP_ROOT"
  install -m 755 "$ROOT_DIR/target/release/watchdog" "$INSTALL_BIN_PATH"
  rsync -a --delete "$ROOT_DIR/.watchdog/presets/" "$PRESET_DIR/"
}

bootstrap() {
  ensure_template_exists
  require_cmd rsync
  require_cmd ffprobe
  require_cmd HandBrakeCLI
  require_cmd lsof
  ensure_runtime_dirs
  ensure_config
  warn_if_using_container_template
  build_release_binary
  install_binary_and_presets
  set_directory_permissions_if_possible
}

run_installed_watchdog() {
  local mode_args=("$@")
  bootstrap
  echo "==> Using config: $CONFIG_PATH"
  echo "==> Binary:       $INSTALL_BIN_PATH"
  RUST_LOG="$RUST_LOG_LEVEL" "$INSTALL_BIN_PATH" --config "$CONFIG_PATH" "${mode_args[@]}"
}

write_systemd_unit() {
  cat > "$SYSTEMD_UNIT_PATH" <<EOF
[Unit]
Description=Jellyfin Transcoding Watchdog
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$TARGET_USER
Group=$TARGET_GROUP
WorkingDirectory=$APP_ROOT
Environment=RUST_LOG=$RUST_LOG_LEVEL
ExecStart=$INSTALL_BIN_PATH --config $CONFIG_PATH --headless
Restart=always
RestartSec=15
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
EOF
}

require_root() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "This command must be run as root." >&2
    exit 1
  fi
}

show_paths() {
  cat <<EOF
repo_root=$ROOT_DIR
app_root=$APP_ROOT
config_path=$CONFIG_PATH
binary_path=$INSTALL_BIN_PATH
preset_dir=$PRESET_DIR
state_dir=$STATE_DIR
transcode_dir=$TRANSCODE_DIR
log_dir=$LOG_DIR
service_name=$SERVICE_NAME
systemd_unit=$SYSTEMD_UNIT_PATH
host_template=$DEFAULT_TEMPLATE
EOF
}

command="${1:-help}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$command" in
  bootstrap)
    bootstrap
    show_paths
    ;;
  doctor)
    run_installed_watchdog --doctor "$@"
    ;;
  healthcheck)
    run_installed_watchdog --healthcheck "$@"
    ;;
  status)
    run_installed_watchdog --status "$@"
    ;;
  status-json)
    run_installed_watchdog --status-json "$@"
    ;;
  once)
    run_installed_watchdog --once --headless "$@"
    ;;
  run)
    run_installed_watchdog --headless "$@"
    ;;
  pause)
    run_installed_watchdog --pause "$@"
    ;;
  resume)
    run_installed_watchdog --resume "$@"
    ;;
  install-service)
    require_root
    require_cmd systemctl
    bootstrap
    write_systemd_unit
    systemctl daemon-reload
    systemctl enable --now "$SERVICE_NAME"
    systemctl status "$SERVICE_NAME" --no-pager
    ;;
  restart-service)
    require_root
    require_cmd systemctl
    bootstrap
    systemctl restart "$SERVICE_NAME"
    systemctl status "$SERVICE_NAME" --no-pager
    ;;
  service-status)
    require_cmd systemctl
    systemctl status "$SERVICE_NAME" --no-pager
    ;;
  logs)
    require_cmd journalctl
    journalctl -u "$SERVICE_NAME" -f
    ;;
  paths)
    show_paths
    ;;
  help|-h|--help)
    print_usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    echo >&2
    print_usage >&2
    exit 1
    ;;
esac
