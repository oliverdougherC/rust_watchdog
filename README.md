# Jellyfin Transcoding Watchdog

Automated media transcoding pipeline for Jellyfin libraries. The project is codec-configurable and cross-platform for Linux and macOS. It scans media directories, identifies files that need transcoding, runs HandBrakeCLI with a selected preset, verifies the output, and atomically replaces the original only after the replacement passes validation.

Bundled presets now ship with `HEVC_MKV` as the default and `AV1_MKV` as the alternate AV1 option.
Preset discovery works from the config directory, the current working directory, and packaged preset directories next to the executable, which makes Docker deployments easier without copying presets into `/config`.

## Features

- Automatic scanning of local media directories or NFS-backed library mounts
- Configurable codec target, bitrate threshold, and HandBrake preset selection
- Verification of duration, stream counts, container, and file health before replace
- Atomic safe-replace with rollback on failure
- SQLite tracking for inspected files, transcode history, queue state, quarantine, and service state
- TUI dashboard for queue, history, logs, cooldown, and service visibility
- Headless mode for `systemd`, `launchd`, or other supervisors
- Simulation mode for testing the full pipeline without touching real media
- Safety gates for file age, stability windows, hardlinks, temp artifacts, in-use checks, retries, cooldowns, and auto-pause
- Healthcheck, status, and doctor modes for operators

## Requirements

- Rust 1.75+ (2021 edition)
- `HandBrakeCLI`
- `ffprobe` from FFmpeg
- `rsync`
- Linux or macOS

Ubuntu package expectations:

- `ffmpeg` provides `ffprobe`
- `rsync`
- `nfs-common` for NFS mode
- A HandBrake package/install method that provides the `HandBrakeCLI` binary on `PATH`

Platform notes:

- Local mode works on both Linux and macOS.
- NFS remounts are platform-aware:
  - Linux uses `mount -t nfs`
  - macOS uses `mount_nfs`
- The optional in-use guard defaults to `lsof`, which is available on macOS and commonly installed on Linux.

## Quick Start

```bash
# Build
cargo build --release

# Run in simulation mode
./target/release/watchdog --simulate

# Copy and edit the sample config
cp .watchdog/watchdog.toml.example .watchdog/watchdog.toml

# Dry run
./target/release/watchdog --dry-run

# Interactive TUI
./target/release/watchdog

# Headless mode for service managers
./target/release/watchdog --headless
```

## Ubuntu Deployment Notes

Recommended deployment shape:

- Watch final Jellyfin library roots, not downloader or import staging directories.
- Keep `transcode_temp` outside any scanned library root.
- Run local mode unless you specifically need watchdog-managed NFS remounts.
- Use `--doctor`, `--healthcheck`, and `--status-json` as part of deployment validation.
- Keep `in_use_guard_enabled = false` in container deployments unless you deliberately share a PID namespace. `lsof` inside one container cannot see file users in other containers by default.
- The bundled `HEVC_MKV` preset uses HandBrake's `x265_10bit` encoder. It is CPU-based, so an NVIDIA GPU is available for future presets but is not used by this preset as shipped.
- Hardlinks are controlled by `safety.hardlink_policy`: `defer` waits for a single-link file, `ignore` skips while hardlinked, and `transcode` processes the file normally. The personal-server templates now default to `transcode`.

Example local-mode config for Ubuntu:

```toml
local_mode = true

[nfs]
server = ""

[[shares]]
name = "movies"
remote_path = ""
local_mount = "/srv/media/Movies"

[[shares]]
name = "tv"
remote_path = ""
local_mount = "/srv/media/TV"

[transcode]
target_codec = "hevc"
preset_file = "presets/HEVC_MKV.json"
preset_name = "HEVC_MKV"
```

## Personal Server Run Script

If you are running this directly on the host, use [deploy/run-personal-server.sh](/Users/ofhd/Developer/rust_watchdog/deploy/run-personal-server.sh). It keeps the runtime install outside the repo under `/mnt/NVME/docker/appdata/watchdog`, so redeploying the repo does not wipe your config, presets, database, status snapshot, or transcode scratch directory.

The host-native template it installs by default is [deploy/watchdog.personal-server.host.toml](/Users/ofhd/Developer/rust_watchdog/deploy/watchdog.personal-server.host.toml). That template already matches your two media roots and uses:

- `presets/HEVC_MKV.json`
- `transcode/` under `/mnt/NVME/docker/appdata/watchdog`
- `state/watchdog.db`
- `state/watchdog.status.json`
- `state/watchdog.events.ndjson`

Typical host-native flow:

```bash
# Build and install/update the persistent runtime
./deploy/run-personal-server.sh bootstrap

# Validate deps, paths, and mounts
./deploy/run-personal-server.sh doctor

# Run one pass
./deploy/run-personal-server.sh once

# Run continuously in the foreground
./deploy/run-personal-server.sh run

# Install as a systemd service so it survives reboots and redeploys
sudo ./deploy/run-personal-server.sh install-service
```

Useful follow-ups:

- `./deploy/run-personal-server.sh status`
- `./deploy/run-personal-server.sh healthcheck`
- `sudo ./deploy/run-personal-server.sh restart-service`
- `./deploy/run-personal-server.sh logs`

## Dockge / Docker Compose

This repo now includes a ready-to-adapt container deployment for your server layout:

- [Dockerfile](/Users/ofhd/Developer/rust_watchdog/Dockerfile)
- [deploy/docker-compose.personal-server.yml](/Users/ofhd/Developer/rust_watchdog/deploy/docker-compose.personal-server.yml)
- [deploy/watchdog.personal-server.toml](/Users/ofhd/Developer/rust_watchdog/deploy/watchdog.personal-server.toml)

Personal-server defaults in that config:

- local mode with `/mnt/DataStore/data/media/movies`
- local mode with `/mnt/DataStore/data/media/tv`
- `HEVC_MKV` preset
- `/transcode` mapped to NVMe-backed appdata storage
- state files under `/config/state`
- slower 15-minute scans and a longer share scan timeout for larger libraries

Suggested Dockge bootstrap:

```bash
mkdir -p /mnt/NVME/docker/appdata/watchdog/state
cp deploy/watchdog.personal-server.toml /mnt/NVME/docker/appdata/watchdog/watchdog.toml
cp deploy/docker-compose.personal-server.yml /mnt/NVME/docker/appdata/watchdog/docker-compose.yml
```

Then point Dockge at the copied compose file or run it directly from the repo checkout.

Example advanced NFS config for Ubuntu:

```toml
local_mode = false

[nfs]
server = "192.168.1.244"

[[shares]]
name = "movies"
remote_path = "/mnt/DataStore/share/jellyfin/jfmedia/Movies"
local_mount = "/mnt/jellyfin/movies"
```

macOS path example for the same share:

```toml
local_mount = "/Volumes/JellyfinMovies"
```

## Local Validation Scripts

`./.dev/local_test.sh`

- Builds release artifacts
- Runs the Rust test suite
- Runs a quick simulated healthcheck
- Runs real transcoding smoke tests for files in `.test_videos/`

`./.dev/local_tui.sh`

- Creates an isolated sandbox under `.local_tui_run/`
- Stages videos into a local test share
- Generates a one-share config
- Launches the TUI or a headless once-pass

Both scripts default to the bundled preset at `.watchdog/presets/HEVC_MKV.json`.

## CLI Flags

| Flag | Description |
|------|-------------|
| `--simulate` | Use fake data and in-memory DB |
| `--dry-run` | Scan and report the transcode queue, then exit |
| `--once` | Run one live pass, then exit |
| `--headless` | No TUI, log to stdout |
| `--log-level <level>` | Override default log level |
| `--healthcheck` | Read-only dependency, mount, and pause-state summary |
| `--healthcheck-json` | JSON healthcheck output |
| `--status` | SSH-friendly operational status |
| `--status-json` | JSON operational status |
| `--doctor` | Guided diagnostics for config, deps, mounts, and DB |
| `--pause` | Create pause file and exit |
| `--resume` | Remove pause file and exit |
| `--quarantine-list` | List quarantined files |
| `--quarantine-clear <path>` | Clear one quarantined file |
| `--quarantine-clear-all` | Clear all quarantined files |
| `--clear-scan-cache` | Clear `inspected_files` cache on startup |
| `--config <path>` | Config path, default `.watchdog/watchdog.toml` |

## Configuration Notes

See `.watchdog/watchdog.toml.example` for the full sample. Important sections:

- `local_mode`: scan host-visible paths directly and skip NFS remount logic
- `[nfs]`: NFS server address for advanced NFS deployments
- `[[shares]]`: share definitions with `name`, `remote_path`, and `local_mount`
- `[transcode]`: target codec, bitrate threshold, preset file/name, timeouts, retries, and disk-space multiplier
- `[scan]`: extensions, include/exclude globs, interval, and optional worker count
- `[safety]`: readiness gates, cooldowns, quarantine, in-use guard, and status freshness thresholds
- `safety.hardlink_policy`: `defer`, `ignore`, or `transcode` (`protect_hardlinked_files` is still accepted as a legacy alias)
- `[metrics]`: persistent counters flushed back into config on an interval
- `[paths]`: temp dir, DB path, log dir, status snapshot path, and event journal path

Bundled presets are resolved from `./.watchdog/presets/`.

## Operations

Useful commands:

```bash
# Validate dependencies, config, mounts, and runtime paths
./target/release/watchdog --doctor --config .watchdog/watchdog.toml

# Supervisor-friendly health output
./target/release/watchdog --healthcheck-json --config .watchdog/watchdog.toml

# Remote status inspection
./target/release/watchdog --status-json --config .watchdog/watchdog.toml
```

## Architecture

Pipeline loop:

`scan -> filter -> transcode -> verify -> replace -> wait`

External tools are abstracted behind traits so the pipeline can be simulated without touching real media. Real deployments use:

- `ffprobe`
- `HandBrakeCLI`
- `rsync`
- platform-specific NFS mount commands when `local_mode = false`

## TUI Controls

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `1` `2` `3` `4` `5` | Switch tab |
| `Tab` | Next tab |
| `j` / `k` | Scroll |
| `f` | Cycle log filter |
| `b` | Open precision-mode file browser |
| `c` | Open precision-mode codec selector |
| `d` | Delete selected pending queue row in precision mode |
| `Home` / `End` | Jump to top or bottom |
