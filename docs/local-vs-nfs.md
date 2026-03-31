# Local vs NFS Mode

## Use Local mode when

- the media folders are already mounted and stable on the host
- you are running Watchdog on the same machine that sees the final library paths
- you want the simplest setup

Example:

```toml
local_mode = true

[[shares]]
name = "movies"
remote_path = ""
local_mount = "/srv/media/Movies"
```

## Use NFS mode when

- Watchdog itself is responsible for working with NFS-backed library folders
- you need the app to reason about the NFS server and mount health
- your deployment expects remote and local mount pairs

Example:

```toml
local_mode = false

[nfs]
server = "192.168.1.244"

[[shares]]
name = "movies"
remote_path = "/mnt/DataStore/share/jellyfin/jfmedia/Movies"
local_mount = "/mnt/media/Movies"
```

## Recommendation

If you are unsure, choose `Local`.

That is the default in setup, and it is usually the right answer for a home server where the operating system or container stack already handles mounts.

## Important rule

Keep `paths.transcode_temp` outside every watched library folder. The app validates this now, but it is still the most common deployment mistake.
