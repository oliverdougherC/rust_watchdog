# Jellyfin Transcoding Watchdog

Automated media transcoding pipeline for Jellyfin libraries. The project is codec-configurable and cross-platform for Linux and macOS. It scans media directories, identifies files that need transcoding, runs HandBrakeCLI with a selected preset, verifies the output, and atomically replaces the original only after the replacement passes validation.

Bundled presets now ship with `HEVC_MKV` as the default and `AV1_MKV` as the alternate AV1 option.

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
