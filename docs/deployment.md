# Deployment

## Canonical source-install path

Use [`watchdogctl`](/Users/ofhd/Developer/rust_watchdog/watchdogctl) as the primary deployment entrypoint.

### Bootstrap a persistent runtime

```bash
./watchdogctl bootstrap
```

### Validate it

```bash
./watchdogctl doctor
./watchdogctl healthcheck
./watchdogctl status
```

### Run it

```bash
./watchdogctl run
./watchdogctl once
```

### Service install

```bash
sudo ./watchdogctl install-service
sudo ./watchdogctl restart-service
./watchdogctl service-status
./watchdogctl logs
```

## Canonical container path

Use the existing personal-server Docker assets:

- [`Dockerfile`](/Users/ofhd/Developer/rust_watchdog/Dockerfile)
- [`deploy/docker-compose.personal-server.yml`](/Users/ofhd/Developer/rust_watchdog/deploy/docker-compose.personal-server.yml)
- [`deploy/watchdog.personal-server.toml`](/Users/ofhd/Developer/rust_watchdog/deploy/watchdog.personal-server.toml)

Typical bootstrap:

```bash
mkdir -p /mnt/NVME/docker/appdata/watchdog/state
cp deploy/watchdog.personal-server.toml /mnt/NVME/docker/appdata/watchdog/watchdog.toml
cp deploy/docker-compose.personal-server.yml /mnt/NVME/docker/appdata/watchdog/docker-compose.yml
```

## Troubleshooting checklist

- Run `watchdog --setup` if the config is missing or invalid
- Run `watchdog --doctor --config <path>` for dependency and mount diagnostics
- Keep the transcode temp directory off the watched library roots
- Verify `HandBrakeCLI`, `ffprobe`, and `rsync` are on `PATH`
- Use `Local` mode unless you truly need NFS-specific behavior
