# Getting Started

## Recommended path

For most home server users, use the interactive setup first:

```bash
cargo build --release
./watchdogctl setup
```

That opens the setup wizard in the TUI and lets you configure:

- Storage mode
- Library folders
- Default preset
- Transcode temp folder
- Scan interval
- Optional notification webhook

## After setup

Run the app interactively:

```bash
cargo run -- --config .watchdog/watchdog.toml
```

Run it as a persistent headless process:

```bash
./watchdogctl run
```

Run one pass:

```bash
./watchdogctl once
```

## If setup opens automatically

The app now opens setup automatically when:

- `.watchdog/watchdog.toml` does not exist
- the config exists but fails validation
- you pass `--setup`

Headless or automation-style runs still fail fast when config is missing or invalid. In that case, fix it with:

```bash
watchdog --setup --config .watchdog/watchdog.toml
```

## Example first-run flow

```text
1. Welcome / Check Environment
2. Choose Storage Mode
3. Add Library Folders
4. Choose or Import Preset
5. Review, Save, and Check
```

## Good defaults for most users

- Use `Local` mode unless you specifically need watchdog-managed NFS mounts
- Start with `HEVC_MKV`
- Keep the transcode temp folder outside watched library roots
- Add your final Jellyfin library folders, not downloader staging folders
