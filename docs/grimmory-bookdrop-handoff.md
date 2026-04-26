# Grimmory BookDrop Handoff

This note documents the live unRAID handoff from `MAM-QBTorrent` into Grimmory BookDrop. It is intentionally separate from MouseSearch's built-in auto-organization feature.

## Current Flow

- qBittorrent container: `MAM-QBTorrent`
- qBittorrent seed root: `/mnt/m2cache/MAM-QBTorrent`
- Grimmory BookDrop: `/mnt/m2cache/grimmory-test/bookdrop`
- qBittorrent completion hook:

```bash
/config/scripts/request_bookdrop_link.sh "%L" "%F" "%I" "%N"
```

The hook writes request JSON into `/config/scripts/bookdrop-requests.d/`, which is backed by `/mnt/user/appdata/MAM-QBTorrent/scripts/bookdrop-requests.d/` on the host.

Host-side scripts then do the real work:

- Immediate watcher: `/mnt/user/appdata/MAM-QBTorrent/scripts/watch_bookdrop_requests.sh`
- Catchall reconciler: `/mnt/user/appdata/MAM-QBTorrent/scripts/reconcile_bookdrop_from_qbit.sh`
- Host hardlinker: `/mnt/user/appdata/MAM-QBTorrent/scripts/link_completed_to_bookdrop_host.sh`
- Persistent cron: `/boot/config/plugins/dynamix/grimmory-bookdrop-qbit.cron`

The watcher runs at boot and polls request files every two seconds. The reconciler runs every minute and checks completed qBittorrent torrents in the `books` and `audiobooks` categories.

## MAM Tracker Identity

The live MouseSearch app runs on OCI, but `MAM-QBTorrent` announces from the unRAID network. Do not enable MouseSearch's OCI-side `ENABLE_DYNAMIC_IP_UPDATE` for this client; it would update MAM to the OCI public IP, not the qBittorrent egress IP.

The unRAID host has a separate MAM Dynamic Seedbox updater for this purpose:

- env file: `/mnt/user/appdata/MAM-QBTorrent/scripts/mam-dynamic-seedbox.env`
- updater: `/mnt/user/appdata/MAM-QBTorrent/scripts/update_mam_dynamic_seedbox.sh`
- log: `/mnt/user/appdata/MAM-QBTorrent/scripts/mam-dynamic-seedbox.log`
- cron: `/boot/config/plugins/dynamix/grimmory-bookdrop-qbit.cron`

The env file contains a `mam_id` session created from the local network and ASN-locked for the provider. Treat it as a secret. If MAM reports `Unrecognized host/PassKey`, run the updater from unRAID and reannounce the affected qBittorrent torrent before changing MouseSearch:

```bash
/mnt/user/appdata/MAM-QBTorrent/scripts/update_mam_dynamic_seedbox.sh
docker exec MAM-QBTorrent curl -fsS -X POST \
  --data-urlencode 'hashes=<torrent hash>' \
  http://127.0.0.1:18080/api/v2/torrents/reannounce
```

## Why The Hardlink Runs On The Host

Do not hardlink from `/downloads` to `/bookdrop` inside the qBittorrent container. Those paths are separate Docker bind mounts. In the live setup, container-side hardlink attempts failed with `Cross-device link`.

Hardlinks work from the host paths:

```bash
ln \
  '/mnt/m2cache/MAM-QBTorrent/media/books/Example.epub' \
  '/mnt/m2cache/grimmory-test/bookdrop/Example.epub'
```

Keep `/mnt/m2cache/MAM-QBTorrent` and `/mnt/m2cache/grimmory-test/bookdrop` on the same filesystem/cache device. unRAID mover, share cache policy changes, or moving either path to `/mnt/user` can break this assumption.

## BookDrop Placement

Linked files should land directly in the BookDrop root:

```text
/mnt/m2cache/grimmory-test/bookdrop/<file-or-new-folder>
```

Avoid a long-lived category folder such as `/bookdrop/books`. Grimmory watches the BookDrop root and scans a new top-level directory recursively, but files added later inside an already-existing subdirectory may wait for a periodic rescan.

## Markers And Logs

State files live under `/mnt/user/appdata/MAM-QBTorrent/scripts/`:

- `bookdrop-linker.log`: watcher, reconciler, and linker log output
- `bookdrop-requests.d/`: immediate hook request queue
- `bookdrop-linked.d/`: marker files for torrents linked into BookDrop
- `bookdrop-pending.d/`: retryable pending records
- `bookdrop-conflicts.d/`: conflicts needing human attention

A marker means only "linked into Grimmory BookDrop". It does not mean Grimmory imported the file into the library. Check Grimmory's BookDrop UI or logs for import state.

## Troubleshooting

Check qBittorrent's hook:

```bash
docker exec MAM-QBTorrent wget -qO- http://127.0.0.1:18080/api/v2/app/preferences \
  | jq -r '.autorun_enabled, .autorun_program'
```

Check the watcher and cron:

```bash
ps -ef | grep -E 'watch_bookdrop_requests|reconcile_bookdrop_from_qbit' | grep -v grep
cat /boot/config/plugins/dynamix/grimmory-bookdrop-qbit.cron
grep -n 'bookdrop\|reconcile\|dynamic_seedbox' /etc/cron.d/root
```

Check handoff state:

```bash
tail -n 100 /mnt/user/appdata/MAM-QBTorrent/scripts/bookdrop-linker.log
find /mnt/user/appdata/MAM-QBTorrent/scripts/bookdrop-requests.d -type f
find /mnt/user/appdata/MAM-QBTorrent/scripts/bookdrop-pending.d -type f
find /mnt/user/appdata/MAM-QBTorrent/scripts/bookdrop-conflicts.d -type f
```

Check that a linked file is still seeding and really is a hardlink:

```bash
stat -c '%d:%i links=%h owner=%u:%g %n' \
  '/mnt/m2cache/MAM-QBTorrent/media/books/<file>' \
  '/mnt/m2cache/grimmory-test/bookdrop/<file>'

docker exec MAM-QBTorrent wget -qO- 'http://127.0.0.1:18080/api/v2/torrents/info?filter=completed' \
  | jq -r '.[] | select(.name=="<torrent name>") | {name,category,state,progress,content_path,hash}'
```

Check Grimmory detection:

```bash
docker logs --since '10 minutes ago' grimmory 2>&1 | grep -Ei 'bookdrop|<file>|error|warn'
```

Check MAM tracker identity:

```bash
tail -n 50 /mnt/user/appdata/MAM-QBTorrent/scripts/mam-dynamic-seedbox.log
docker exec MAM-QBTorrent curl -fsS https://api.ipify.org
docker exec MAM-QBTorrent curl -fsS 'http://127.0.0.1:18080/api/v2/torrents/trackers?hash=<hash>' \
  | jq -r '.[] | select(.url|startswith("https://t.myanonamouse.net")) | {status,msg,num_seeds,num_peers}'
```

Common causes:

- qBittorrent hook disabled or changed
- watcher not running after reboot
- cron not loaded with `update_cron`
- MAM Dynamic Seedbox was updated from MouseSearch on OCI instead of from unRAID/qBittorrent's egress path
- files placed under a persistent BookDrop subdirectory instead of the root
- source and BookDrop paths no longer hardlink-compatible
- marker exists but Grimmory import was never finalized
