# Presets

## Bundled presets

The repo ships with bundled presets in [`.watchdog/presets`](/Users/ofhd/Developer/rust_watchdog/.watchdog/presets):

- `HEVC_MKV`
- `AV1_MKV`

These are available in the TUI preset selector without any extra setup.

## Import a HandBrake preset JSON

Interactive path:

1. Open `Setup Wizard` or `Import Preset` from the control center
2. Enter the path to your HandBrake preset JSON
3. Watchdog validates the file and copies it into its managed `presets/` directory
4. Choose one of the imported preset names as the default

Non-interactive path:

```bash
watchdog --import-preset /path/to/MyPresets.json --config .watchdog/watchdog.toml
```

Or:

```bash
./watchdogctl import-preset /path/to/MyPresets.json --config .watchdog/watchdog.toml
```

## What import does

- Copies the file into the managed `presets/` directory for the selected config base directory
- Validates that the JSON contains at least one selectable HandBrake preset
- Lists selectable preset names on stdout in non-interactive mode

## What it does not do

- It does not create new HandBrake presets from scratch
- It does not rewrite the internal preset content
- It does not automatically change advanced transcode safety settings

## Example terminal capture

```text
$ watchdog --import-preset ~/Downloads/MyPresets.json --config .watchdog/watchdog.toml
imported_preset_file: presets/MyPresets.json
- AppleTV 4K HEVC [hevc]
- AV1 Archive [av1]
```
