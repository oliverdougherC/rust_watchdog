# Jellyfin AV1 Transcoding Watchdog

Automated media transcoding pipeline for Jellyfin libraries. Scans NFS-mounted media shares, identifies files that need transcoding (wrong codec or excessive bitrate), transcodes them to AV1 using HandBrakeCLI, verifies the output, and atomically replaces the originals.

## Features

- **Automatic scanning** of multiple NFS media shares (movies, TV, etc.)
- **Smart filtering** — skips files already in the target codec and under the bitrate threshold
- **AV1 transcoding** via HandBrakeCLI with configurable presets
- **Verification** — checks duration, stream counts, and file health before replacing
- **Atomic safe-replace** — renames original to `.old`, swaps in new file, cleans up (with rollback on failure)
- **SQLite tracking** — remembers inspected files across runs, records full transcode history
- **TUI dashboard** — real-time progress, stats, logs, and history via Ratatui
- **Headless mode** — log-to-stdout for running as a system service
- **Simulation mode** — test the full pipeline with fake data, no real files needed
- **Graceful shutdown** — responds to SIGTERM/SIGINT, cleans up in-progress work

## Requirements

- **Rust** 1.75+ (2021 edition)
- **HandBrakeCLI** — for AV1 transcoding
- **ffprobe** — for media metadata inspection (part of FFmpeg)
- **rsync** — for file transfers
- **macOS** — uses `mount_nfs` for NFS management (Linux support would need a small adapter)

## Quick Start

```bash
# Build
cargo build --release

# Run in simulation mode (no config or real files needed)
./target/release/watchdog --simulate

# Copy and edit the example config
cp watchdog.toml.example watchdog.toml
# Edit watchdog.toml with your NFS server, share paths, and preferences

# Dry run — scan and report what would be transcoded
./target/release/watchdog --dry-run

# Run with TUI dashboard
./target/release/watchdog

# Run headless (for launchd/systemd)
./target/release/watchdog --headless
```

## CLI Flags

| Flag | Description |
|------|-------------|
| `--simulate` | Use fake data and in-memory DB (no config needed) |
| `--dry-run` | Scan and report the transcode queue, then exit |
| `--headless` | No TUI, log to stdout (for services) |
| `--config <path>` | Config file path (default: `watchdog.toml`) |
| `--version` | Print version and exit |

## Configuration

See [`watchdog.toml.example`](watchdog.toml.example) for a complete example. Key settings:

- **`[nfs]`** — NFS server IP
- **`[[shares]]`** — media share definitions (name, remote path, local mount point)
- **`[transcode]`** — codec target, bitrate threshold, HandBrake preset, timeout, retries
- **`[scan]`** — video extensions to scan, interval between passes
- **`[paths]`** — temp directory for transcoding, database location

## TUI Controls

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `1` `2` `3` | Switch tab (Dashboard, Logs, History) |
| `Tab` | Next tab |
| `j` / `k` | Scroll down / up |
| `Home` / `End` | Jump to top / bottom |

## Architecture

The pipeline runs in a loop: **scan** → **filter** → **transcode** → **verify** → **replace** → **wait**.

All external tools (ffprobe, HandBrakeCLI, rsync, mount_nfs) and filesystem operations are abstracted behind traits, enabling full simulation without touching real files.

## Database

Uses SQLite (WAL mode) with three tables:

- `inspected_files` — tracks which files have been checked (by path + size + mtime)
- `transcode_history` — full record of every transcode attempt
- `space_saved_log` — timeseries of cumulative space savings

The schema is compatible with the legacy Python version's database for seamless migration.
