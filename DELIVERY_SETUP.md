# Delivery Setup Guide

This guide explains how to let Smewlet move finished downloads into your media library (the **Local directory** provider) and how to have Plex or Jellyfin rescan afterwards. Settings live under *Settings → Providers → Local directory*; every field there also has a short help line.

## 1. Which path goes where

Delivery involves up to four programs, and each one may see the same folder under a different path. Most setup mistakes come from mixing these up, so keep them apart:

| Where | What it is | Example |
|---|---|---|
| **Host path** | the real folder on your server | `/srv/media/series` |
| **Smewlet path** | the same folder *inside the Smewlet container* | `/media/series` |
| **Media server path** | the same folder as Jellyfin/Plex sees it | `/data/tv` |
| **SABnzbd path** | where SABnzbd reports its finished downloads | `/downloads` |

Rules of thumb:

- The **Smewlet path** is the only one you type into the four path fields (Movies, Shows, Anime, Anime movies). Never enter a host path there.
- You create the link between host path and Smewlet path yourself with a Docker volume (`host:container`, see below).
- The **path maps** (sections 3 and 5) only translate between the Smewlet path and the media server / SABnzbd path. If both programs see the same path as Smewlet, you need no path map.

### Worked example

Your series live in `/srv/media/series` on the host. Jellyfin mounts that folder at `/data/tv`.

1. Volume in `compose.yaml`: `/srv/media/series:/media/series` (host path : Smewlet path)
2. *Shows path* in the dialog: `/media/series` (Smewlet path)
3. Media server path map: server prefix `/data/tv`, Smewlet prefix `/media/series`

Smewlet writes `/media/series/Show/Season 01/...`, then tells Jellyfin to refresh `/data/tv/Show/Season 01/...`.

## 2. Mount your library folders

Smewlet only sees folders that are mounted into its container. `compose.yaml` contains a commented example; uncomment and adjust the left-hand (host) side:

```yaml
volumes:
  - ./config:/app/config
  - /srv/media/movies:/media/movies
  - /srv/media/series:/media/series
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

## 3. SABnzbd: same filesystem and path map

SABnzbd downloads are filed with a **hardlink** (or a move across filesystems), not copied. For hardlinks, SABnzbd's completed-download folder and the library must be on the same filesystem and both mounted into Smewlet from that filesystem.

If SABnzbd reports paths differently from how Smewlet sees them, set the **SABnzbd path map** (SABnzbd path → Smewlet path):

| Field | Example | Meaning |
|---|---|---|
| remote prefix | `/downloads` | path as SABnzbd sees it |
| local prefix | `/mnt/downloads` | path as Smewlet sees it |

Set both or leave both empty. Smewlet swaps the remote prefix for the local one.

## 4. Naming templates

The four templates (movie, show, anime, anime movie) are prefilled with the defaults. Available placeholders:

`{title} {entry_title} {year} {season:02d} {episode:02d} {episode_title} {resolution} {source} {ext}`

- Regular shows get their episode title from TMDB (`Show - S01E01 - Chapter One.mkv`). If TMDB has no real title, the ` - {episode_title}` part is dropped.
- Anime never uses episode titles.

### Pinned anime folders

After the first delivery of an anime entry, its folder name is remembered. Later episodes and upgrades land in the same folder even if you change the anime title language afterwards, so Jellyfin/Plex never sees a duplicate series. Existing delivered entries are pinned automatically on startup.

## 5. Media server refresh (optional)

Pick one server in the **Media server** dropdown: *None*, *Plex* or *Jellyfin*. Only the selected server is refreshed and tested; saving clears the other server's settings.

- **Jellyfin:** server URL (for example `http://jellyfin:8096`) and an API key from the Jellyfin dashboard.
- **Plex (Experimental):** server URL (for example `http://plex:32400`) and your Plex token. Plex support has not yet been verified against a live server; please report problems.

Click **Test Connection** to check the connection and the path map. Refreshing is best-effort: a failure only logs a warning and sends one notification; delivery is never affected.

### Media server path map

If the server sees your library under a different path than Smewlet does, set the map (server prefix → Smewlet prefix). Set both or leave both empty.

| Situation | Server prefix | Smewlet prefix |
|---|---|---|
| Both containers mount the share the same way | `/media` | `/media` |
| Jellyfin mounts the library at `/data/tv`, Smewlet at `/media/series` | `/data/tv` | `/media/series` |

## 6. Aborting a delivery

Queued or running **cloud** deliveries (TorBox, Premiumize) show an **Abort** button in Active Pushes and the push history. Aborting stops the stream, removes the partial `.part` file and returns the row to its idle state, so *Deliver to Library* is offered again. Files that were already delivered stay in the library.

SABnzbd deliveries are near-instant hardlinks and cannot be aborted; they have no Abort button.

## 7. When the NAS is off

If the library storage becomes unreachable (NAS powered down, NFS/SMB export gone, stale mount), Smewlet does not fail every queued file. The delivery in progress is **paused** with the status *Delivery paused: storage offline*, its half-written `.part` file is removed, and the rest of the queue is **held**: new deliveries are accepted and wait behind it. A banner on the dashboard shows the affected folders and the next check time.

Smewlet checks the storage again after 1, 2, 4, 8 and 16 minutes, then every 30 minutes. As soon as it is reachable, the paused delivery resumes first and the queue continues on its own. No action is needed; *Retry Delivery* on a paused row just moves it to the end of the queue.

A folder that is reachable but unwritable (wrong permissions, read-only mount) is not treated as offline: that delivery fails with a readable message and the next one proceeds. Queue state is kept in memory; after a restart Smewlet probes the storage once and resumes or waits again.

## 8. Troubleshooting

| Message | What to check |
|---|---|
| Permission denied | folder ownership/permissions for UID 1000 |
| Read-only file system | remove `:ro` from the mount |
| Unexpected new folder in the library | Smewlet creates missing library folders on delivery, so a typo in a path field creates a new folder instead of failing; check the four path fields |
| Path not found | is the folder mounted into the container? |
| Cannot hardlink across filesystems | SABnzbd downloads and library must share a filesystem (otherwise Smewlet moves the file) |
| No space left on device | free space on the library volume; adjust the free-space buffer |
