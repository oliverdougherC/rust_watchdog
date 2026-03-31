# Watchdog for Jellyfin Libraries

`watchdog` is a terminal app for automatically transcoding media libraries with a safer, more end-user-friendly workflow.

It watches your movie/TV folders, decides what should be transcoded, runs `HandBrakeCLI`, verifies the result, and only replaces the original when the output passes validation.

## What Changed

The app now has:

- A guided first-run setup flow with `watchdog --setup`
- In-app management for library folders, storage mode, and default presets
- Managed preset importing with `watchdog --import-preset <file>`
- A friendlier control-center style TUI
- One primary source-install script: [`watchdogctl`](/Users/ofhd/Developer/rust_watchdog/watchdogctl)

## 5-Minute Quick Start

### 1. Install dependencies

You need:

- Rust 1.75+
- `HandBrakeCLI`
- `ffprobe`
- `rsync`

Linux NFS mode also typically needs `nfs-common`. macOS NFS mode uses `mount_nfs`.

### 2. Build and open setup

```bash
cargo build --release
./watchdogctl setup
```

The setup wizard walks you through:

1. Checking tools and config health
2. Choosing `Local` vs `NFS`
3. Adding library folders
4. Choosing or importing a preset
5. Saving a usable config

### 3. Run it

Interactive control center:

```bash
cargo run -- --config .watchdog/watchdog.toml
```

Headless service-style run:

```bash
./watchdogctl run
```

One pass:

```bash
./watchdogctl once
```

## Common Tasks

- First run: [`docs/getting-started.md`](/Users/ofhd/Developer/rust_watchdog/docs/getting-started.md)
- Decide between local and NFS mode: [`docs/local-vs-nfs.md`](/Users/ofhd/Developer/rust_watchdog/docs/local-vs-nfs.md)
- Import or choose presets: [`docs/presets.md`](/Users/ofhd/Developer/rust_watchdog/docs/presets.md)
- Install as a persistent service: [`docs/deployment.md`](/Users/ofhd/Developer/rust_watchdog/docs/deployment.md)

## Main Commands

### Interactive app

```bash
watchdog --setup
watchdog --import-preset /path/to/Preset.json
watchdog --config .watchdog/watchdog.toml
watchdog --config .watchdog/watchdog.toml --precision
```

`--precision` is still supported for manual-selection mode, but the TUI now refers to it as `Manual mode`.

### `watchdogctl`

```bash
./watchdogctl setup
./watchdogctl import-preset /path/to/Preset.json
./watchdogctl bootstrap
./watchdogctl run
./watchdogctl once
./watchdogctl healthcheck
./watchdogctl status
./watchdogctl pause
./watchdogctl resume
```

## Example Terminal Capture

```text
Watchdog Control Center
Home | Queue | Activity | History | Cooling

[ Setup Wizard ] [ Health Check ] [ Browse Files ] [ Preset Selector ] [ Pause ] [ Import Preset ]

Setup & Health
- Configuration: ready
- Dependencies: ffprobe, HandBrakeCLI, and rsync found
- Default preset: HEVC_MKV.json :: HEVC_MKV
- Storage mode: Local
- Library folders: 2 configured
```

## Existing Deploy Files

The existing deploy assets are still here:

- [`deploy/watchdog.personal-server.host.toml`](/Users/ofhd/Developer/rust_watchdog/deploy/watchdog.personal-server.host.toml)
- [`deploy/watchdog.personal-server.toml`](/Users/ofhd/Developer/rust_watchdog/deploy/watchdog.personal-server.toml)
- [`deploy/docker-compose.personal-server.yml`](/Users/ofhd/Developer/rust_watchdog/deploy/docker-compose.personal-server.yml)

The old [`deploy/run-personal-server.sh`](/Users/ofhd/Developer/rust_watchdog/deploy/run-personal-server.sh) path still works, but it now forwards to [`watchdogctl`](/Users/ofhd/Developer/rust_watchdog/watchdogctl).
