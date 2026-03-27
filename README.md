# Jellyfin AV1 Transcoding Watchdog

Automated media transcoding pipeline for Jellyfin libraries. It is now tuned primarily for local library roots, with conservative safety defaults for deployments that live next to an *arr stack. It scans media directories, identifies files that need transcoding (wrong codec or excessive bitrate), transcodes them to AV1 using HandBrakeCLI, verifies the output, and atomically replaces the originals.

## Features

- **Automatic scanning** of multiple local media directories or NFS media shares
- **Smart filtering** — skips files already in the target codec and under the bitrate threshold
- **AV1 transcoding** via HandBrakeCLI with configurable presets
- **Verification** — checks duration, stream counts, and file health before replacing
- **Atomic safe-replace** — renames original to `.watchdog.old`, swaps in new file, cleans up (with rollback on failure)
- **SQLite tracking** — remembers inspected files across runs, records full transcode history
- **TUI dashboard** — real-time progress, stats, logs, and history via Ratatui
- **Headless mode** — log-to-stdout for running as a system service
- **Simulation mode** — test the full pipeline with fake data, no real files needed
- **Graceful shutdown** — responds to SIGTERM/SIGINT, cleans up in-progress work
- **Safety controls** — min file age, persistent readiness tracking, temporary-file deferral, hardlink protection, pause file, cooldown/backoff for repeatedly failing files
- **Safety tripwire** — auto-pauses the pipeline after consecutive pass-level failures
- **Failure quarantine** — isolates repeatedly failing files until manually cleared
- **Healthcheck mode** — fast read-only status for service supervisors (`--healthcheck`, `--healthcheck-json`)
- **SSH status mode** — one-command status output for remote operators (`--status`, `--status-json`)
- **Pause/Resume controls** — create/remove pause marker safely from CLI (`--pause`, `--resume`)
- **Active-file guard (optional)** — best-effort in-use checks to avoid files currently opened by other processes
- **Status snapshot (optional)** — atomic JSON state file for service monitoring integrations
- **Event journal (optional)** — NDJSON operational event stream for audits and diagnostics
- **Structured fatal diagnostics** — error chain, machine code/category, context fields, and remediation hints

## Requirements

- **Rust** 1.75+ (2021 edition)
- **HandBrakeCLI** — for AV1 transcoding
- **ffprobe** — for media metadata inspection (part of FFmpeg)
- **rsync** — for file transfers
- **macOS** — local mode is fully supported; NFS mode uses `mount_nfs` (Linux support would need a small adapter)

## Quick Start

```bash
# Build
cargo build --release

# Run in simulation mode (no config or real files needed)
./target/release/watchdog --simulate

# Copy and edit the example config
cp .watchdog/watchdog.toml.example .watchdog/watchdog.toml
# Edit .watchdog/watchdog.toml with your local library roots and preferences
# Keep transcode_temp outside any scanned library root
# Point watchdog at final library roots, not download or incomplete directories
# Bundled presets and default runtime state also live under ./.watchdog/

# Dry run — scan and report what would be transcoded
./target/release/watchdog --dry-run

# Run with TUI dashboard
./target/release/watchdog

# Run headless (for launchd/systemd)
./target/release/watchdog --headless
```

## Local Real-Video Test

Use `./.dev/local_test.sh` to run a full local validation pass before deploying:

```bash
./.dev/local_test.sh
```

What it does:

- Builds release artifacts
- Runs the Rust test suite
- Runs a quick simulated healthcheck
- Runs real transcoding smoke tests for every file in `.test_videos/`

Optional:

```bash
# Use a different input directory
./.dev/local_test.sh /path/to/videos

# Forward args to `cargo test` (for focused runs)
./.dev/local_test.sh --test healthcheck_cli

# Override preset values used by the real-tools smoke test
WATCHDOG_REAL_PRESET_FILE=/abs/path/preset.json WATCHDOG_REAL_PRESET_NAME=MyPreset ./.dev/local_test.sh

# Optional: run a full (slower) simulated once pass
WATCHDOG_RUN_SIM_ONCE=1 ./.dev/local_test.sh
```

## Local TUI Test (.test_videos as the only share)

Use `./.dev/local_tui.sh` to run the real TUI pipeline with `.test_videos` as the only scanned share:

```bash
./.dev/local_tui.sh
```

What it does:

- Creates an isolated sandbox at `.local_tui_run/`
- Rsync-stages videos in two hops: source -> `ingest_tmp` -> sandbox share path
- Generates a single-share config with `local_mode = true` pointing only to that sandboxed media dir
- Launches the TUI against the local-mode config
- Exercises watchdog's rsync transfer path while transcoding

Optional:

```bash
# Use another source video directory
./.dev/local_tui.sh /path/to/videos

# Run one full pass in headless mode with detailed logs + outcome summary
./.dev/local_tui.sh --headless-once

# Optional: adjust headless log level (default: info)
WATCHDOG_LOCAL_TUI_HEADLESS_LOG_LEVEL=debug ./.dev/local_tui.sh --headless-once

# Prepare sandbox/config only (no watchdog run)
./.dev/local_tui.sh --prepare-only
```

## Debugging and Logging

When troubleshooting, run with explicit verbosity and backtraces:

```bash
RUST_LOG=watchdog=debug RUST_BACKTRACE=1 ./target/release/watchdog --headless --log-level debug
```

What you now get on fatal failures:

- Full error chain (`[0]` root through nested causes)
- Structured classification (`error_code`, `error_category`)
- Variant-specific context fields (for example `path`, `share`, timeout values)
- Actionable remediation hints for common failure classes

Useful operator commands:

```bash
# Fast environment + dependency + mount diagnostics
./target/release/watchdog --doctor --config .watchdog/watchdog.toml

# Snapshot-driven service status for remote debugging
./target/release/watchdog --status-json --config .watchdog/watchdog.toml

# Health-oriented checks suitable for supervisors
./target/release/watchdog --healthcheck-json --config .watchdog/watchdog.toml
```

## CLI Flags

| Flag | Description |
|------|-------------|
| `--simulate` | Use fake data and in-memory DB (no config needed) |
| `--dry-run` | Scan and report the transcode queue, then exit |
| `--once` | Run one live pass, then exit |
| `--headless` | No TUI, log to stdout (for services) |
| `--log-level <level>` | Override default log level (`error`, `warn`, `info`, `debug`, `trace`) |
| `--healthcheck` | Read-only dependency/NFS/paused/cooldown status summary |
| `--healthcheck-json` | JSON healthcheck output (implies healthcheck behavior) |
| `--status` | SSH-friendly operational status summary |
| `--status-json` | JSON operational status output (implies `--status`) |
| `--doctor` | Guided diagnostics (config/deps/mounts/DB) |
| `--pause` | Create pause file and exit |
| `--resume` | Remove pause file and exit |
| `--quarantine-list` | List quarantined files |
| `--quarantine-clear <path>` | Clear one quarantined file |
| `--quarantine-clear-all` | Clear all quarantined files |
| `--clear-scan-cache` | Clear `inspected_files` cache on startup (full rescan) |
| `--config <path>` | Config file path (default: `.watchdog/watchdog.toml`) |
| `--version` | Print version and exit |

## Configuration

See [`.watchdog/watchdog.toml.example`](.watchdog/watchdog.toml.example) for a complete example. Key settings:

The CLI defaults to `.watchdog/watchdog.toml`. If that hidden config is absent, the binary still falls back to a legacy root-level `watchdog.toml`.

- **`local_mode`** — local-first default; scan `shares[].local_mount` directly on the host and ignore NFS remount logic
- **`[nfs]`** — NFS server IP (required only for advanced NFS deployments when `local_mode = false`)
- **`[[shares]]`** — media share definitions (name plus local scan path in `local_mount`; `remote_path` is required only in NFS mode)
- **`[transcode]`** — codec target, bitrate threshold, HandBrake preset, timeout, stall timeout, retries, and retry-time timeout scaling/caps
- HandBrake preset JSON files are loaded from the hidden `./.watchdog/presets/` directory
- **`[scan]`** — video extensions, optional include/exclude globs (path-aware or basename-only patterns), scan interval, per-pass queue cap, optional `probe_workers`
- **`[safety]`** — min file age, stable observation count/window, temporary suffix deferrals, hardlink protection, pause file path, failure cooldown policy, pass-failure tripwire thresholds, quarantine thresholds/codes, optional in-use guard command, periodic recovery scan interval, bounded share scan timeout, status snapshot freshness threshold
- **`[paths]`** — temp directory for transcoding, database location, optional status snapshot output path, optional NDJSON event journal path
- The default `transcode_temp = "tmp"` resolves next to the config/app directory and is rejected if it points inside a scanned share root

## Deploying Beside an *Arr Stack

- Watch final library roots only. Do not watch qBittorrent, sabnzbd, incomplete, or staging directories.
- The default readiness gate waits for files to age, remain unchanged across multiple observations, and stay stable for a full window before they are eligible.
- Hardlinked files are deferred until their link count drops back to `1`, which protects common *arr seeding and post-import layouts.
- Temporary/import artifacts matching configured suffixes are deferred automatically.
- Local mode is the recommended deployment model. NFS is still supported, but it is treated as an advanced setup rather than the primary path.
- **`[notify]`** — optional webhook notifications (`pass_failure_summary`, `replacement_summary`, `cooldown_alert`)

## TUI Controls

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `1` `2` `3` `4` `5` | Switch tab (Dashboard, Queue, Logs, History, Cooldown) |
| `Tab` | Next tab |
| `j` / `k` | Scroll down / up |
| `f` | Cycle Logs tab filter (all / warn+error / error) |
| `b` | Open precision-mode file browser |
| `c` | Open precision-mode codec selector |
| `d` | Delete selected pending queue row in precision mode |
| `Home` / `End` | Jump to top / bottom |

## Architecture

The pipeline runs in a loop: **scan** → **filter** → **transcode** → **verify** → **replace** → **wait**.

All external tools (ffprobe, HandBrakeCLI, rsync, mount_nfs) and filesystem operations are abstracted behind traits, enabling full simulation without touching real files.

### Local Mode Example

```toml
local_mode = true

[nfs]
server = ""

[[shares]]
name = "movies"
remote_path = ""
local_mount = "/srv/media/Movies"
```

### Advanced NFS Example

```toml
local_mode = false

[nfs]
server = "192.168.1.244"

[[shares]]
name = "movies"
remote_path = "/mnt/DataStore/share/jellyfin/jfmedia/Movies"
local_mount = "/Volumes/JellyfinMovies"
```

## Database

Uses SQLite (WAL mode) with eight primary tables:

- `inspected_files` — tracks which files have been checked (by path + size + mtime)
- `transcode_history` — full record of every transcode attempt (human `failure_reason` + machine `failure_code`)
- `space_saved_log` — timeseries of cumulative space savings
- `file_failure_state` — per-file retry/cooldown and last failure code tracking
- `file_readiness_state` — persistent file stability observations used to defer settling files safely
- `queue_items` — durable pending/active queue rows with preset snapshots
- `file_quarantine` / `service_state` — quarantine set and pass-level safety tripwire state

The schema is compatible with the legacy Python version's database for seamless migration.
