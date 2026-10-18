# Delivery Setup Guide

This guide explains how to let Smewlet move finished downloads into your media library (the **Local directory** provider) and how to have Plex or Jellyfin rescan afterwards. Settings live under *Settings → Providers → Local directory*; every field there also has a short help line.

## 1. Mount your library folders

Smewlet only sees folders that are mounted into its container. `compose.yaml` contains a commented example; uncomment and adjust the left-hand (host) side:

```yaml
volumes:
  - ./config:/app/config
  - /srv/media/movies:/media/movies
  - /srv/media/tv:/media/tv
  - /srv/media/anime:/media/anime
  - /srv/media/anime-movies:/media/anime-movies
```

In the dialog, enter the **container-side** paths (`/media/movies`, `/media/series`, `/media/anime`, `/media/anime-movies`). Leave a path empty to skip that media type.

### Write access (UID 1000)

The container runs as UID 1000. That user needs write access to every library folder (and to the free-space check). If a delivery fails with *Permission denied: … (check folder permissions for the Smewlet user)*, fix ownership or permissions on the host, for example `chown -R 1000:1000 /srv/media/anime`.

### Network shares (NFS/SMB)

- Mount the share on the host first, then bind-mount the mounted folder into the container.
- Make sure the export allows writes for UID 1000 (check `all_squash`/`anonuid` or matching UID mapping).
- If the share goes offline or is remounted, deliveries fail with *Storage error … (the share may be offline or remounted)*. Use *Retry Delivery* once it is back.

### Read-only mounts

A library mounted `:ro` cannot receive files. The failure reads *Read-only file system … (the mount is read-only)*. Remove `:ro` from the mount.

## 2. SABnzbd: same filesystem and path map

SABnzbd downloads are filed with a **hardlink** (or a move across filesystems), not copied. For hardlinks, SABnzbd's completed-download folder and the library must be on the same filesystem and both mounted into Smewlet from that filesystem.

If SABnzbd reports paths differently from how Smewlet sees them, set the **SABnzbd path map**:

| Field | Example | Meaning |
|---|---|---|
| remote prefix | `/downloads` | path as SABnzbd sees it |
| local prefix | `/mnt/downloads` | path as Smewlet sees it |

Set both or leave both empty. Smewlet swaps the remote prefix for the local one.

## 3. Naming templates

The four templates (movie, show, anime, anime movie) are prefilled with the defaults. Available placeholders:

`{title} {entry_title} {year} {season:02d} {episode:02d} {episode_title} {resolution} {source} {ext}`

- Regular shows get their episode title from TMDB (`Show - S01E01 - Chapter One.mkv`). If TMDB has no real title, the ` - {episode_title}` part is dropped.
- Anime never uses episode titles.

### Pinned anime folders

After the first delivery of an anime entry, its folder name is remembered. Later episodes and upgrades land in the same folder even if you change the anime title language afterwards, so Jellyfin/Plex never sees a duplicate series. Existing delivered entries are pinned automatically on startup.

## 4. Media server refresh (optional)

Pick one server in the **Media server** dropdown: *None*, *Plex* or *Jellyfin*. Only the selected server is refreshed and tested; saving clears the other server's settings.

- **Jellyfin:** server URL (for example `http://jellyfin:8096`) and an API key from the Jellyfin dashboard.
- **Plex (Experimental):** server URL (for example `http://plex:32400`) and your Plex token. Plex support has not yet been verified against a live server; please report problems.

Click **Test Connection** to check the connection and the path map. Refreshing is best-effort: a failure only logs a warning and sends one notification; delivery is never affected.

### Media server path map

If the server sees your library under a different path than Smewlet does, set the map (server prefix → Smewlet prefix). Set both or leave both empty.

| Situation | Server prefix | Smewlet prefix |
|---|---|---|
| Both containers mount the share the same way | `/media` | `/media` |
| Jellyfin mounts the library at `/data/tv`, Smewlet at `/media/tv` | `/data/tv` | `/media/tv` |

## 5. Aborting a delivery

Queued or running **cloud** deliveries (TorBox, Premiumize) show an **Abort** button in Active Pushes and the push history. Aborting stops the stream, removes the partial `.part` file and returns the row to its idle state, so *Deliver to Library* is offered again. Files that were already delivered stay in the library.

SABnzbd deliveries are near-instant hardlinks and cannot be aborted; they have no Abort button.

## 6. Troubleshooting

| Message | What to check |
|---|---|
| Permission denied | folder ownership/permissions for UID 1000 |
| Read-only file system | remove `:ro` from the mount |
| Path not found | is the folder mounted into the container? |
| Cannot hardlink across filesystems | SABnzbd downloads and library must share a filesystem (otherwise Smewlet moves the file) |
| No space left on device | free space on the library volume; adjust the free-space buffer |
