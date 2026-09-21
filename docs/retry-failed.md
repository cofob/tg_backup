# Recover failed downloads and work

`retry-failed` requeues failed records; it does not download or convert files itself.
Successful, skipped, paused, queued, and running work is unchanged. Completed,
deferred, unavailable, pending, and downloading attachments are unchanged.

```sh
tg-backup --dataset /data/archive retry-failed --target all
tg-backup --dataset /data/archive retry-failed --target all --apply
```

`--target` accepts `all` (default), `work`, or `attachments`. Without `--apply`, the
command opens a read-only snapshot and returns JSON counts without modifying data.
`found` and `changed` contain separate `work` and `attachments` counts. Apply requires
the normal exclusive writer lock; stop the coordinator and other writers first.

The update is atomic across both categories. Work keeps its ID, deduplication key,
configuration and scheduling policy, but receives a fresh three-attempt budget.
Download offsets and partial files are preserved. No completed representations,
original files, history, or scan checkpoints are removed. A second apply before
processing starts changes zero rows. `work --resume ID` also resets its attempt
budget and old progress.

## tg-backup2 on odin

Run these commands from the deployment directory containing the production Compose
file and its environment. First publish/build the corrected core and FFmpeg images;
ensure both image tags refer to the corrected version before proceeding. The commands
below are an operator runbook, not an automatic deployment or startup migration.

1. Stop the writers cleanly and wait for both containers to stop:

   ```sh
   sudo docker compose stop coordinator worker
   ```

2. Save a consistent catalog backup with the SQLite backup API on the host. This
   includes committed WAL contents; copying only `catalog.sqlite3` is insufficient.
   This backup is for the queue/catalog operation, not a replacement for a full
   archive backup (epochs, attachments, configuration and session).

   ```sh
   sudo python3 - <<'PY'
   from pathlib import Path
   import sqlite3
   from datetime import datetime, timezone
   root = Path('/data/secure/hd2/services/tg-backup2/tg-backup-archive/_data/archive')
   stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
   backup = root.parent / ('catalog-before-retry-' + stamp + '.sqlite3')
   backup.touch(mode=0o600, exist_ok=False)
   source = sqlite3.connect('file:' + str(root / 'catalog.sqlite3') + '?mode=ro', uri=True)
   destination = sqlite3.connect(backup)
   source.backup(destination)
   assert destination.execute('PRAGMA integrity_check').fetchone() == ('ok',)
   destination.close()
   source.close()
   print(backup)
   PY
   ```

3. Pull the corrected images, then preview and apply using a **new one-off container**
   with the coordinator's existing mounts and user. Do not use `docker exec` on the
   stopped container or start a second coordinator alongside the operation.

   ```sh
   sudo docker compose pull coordinator worker
   sudo docker compose run --rm --no-deps coordinator --dataset /data/archive retry-failed --target all
   sudo docker compose run --rm --no-deps coordinator --dataset /data/archive retry-failed --target all --apply
   ```

4. Restart the services and inspect progress:

   ```sh
   sudo docker compose up -d worker coordinator
   sudo docker compose logs --tail=100 coordinator worker
   sudo docker compose exec coordinator tg-backup --dataset /data/archive status --json
   sudo docker compose exec coordinator tg-backup --dataset /data/archive work --limit 200
   ```

Work respects its existing automatic/manual setting and configured time windows.
Failed downloads become eligible immediately and are picked up in the next
`download_pending` phase; metadata/history collection can precede that phase.
The command's changed counts are **not** completion counts.

After recovery, supported work can complete or fail with detailed diagnostics;
old sticker, custom emoji, document, archive and unknown-format tasks become
`skipped`, preserving originals. New unsupported files never enter the transcode
queue. Files in other Telegram DCs can download after authorization transfer.
Network failures, expired references or inaccessible media may still need retries;
zero failed rows immediately after applying is not proof that processing succeeded.

## Conversion and diagnostics

Only recognized JPEG, PNG, WebP, BMP, TIFF, MP3, FLAC, Ogg, WAV, AAC/ADTS,
MP4/MOV and Matroska/WebM inputs are admitted to probing. File extensions and MIME
claims do not bypass signature checks. The probe must also identify an allowed
container and image/audio/video codec. Animated images, all stickers/custom emoji,
unsupported layouts and odd-sized video are skipped. Originals are retained.

Worker errors include task ID, original hash, processing stage, exit status and
the last 8 KiB of stderr (with an explicit truncation marker). The error travels
through the worker socket into the existing work API/CLI `error` field and logs.
Historical short errors cannot be reconstructed; retry produces new diagnostics.
