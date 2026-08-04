# tg_backup

`tg_backup` exports Telegram chat history with [Kurigram](https://docs.kurigram.icu/), the maintained drop-in successor to Pyrogram. It writes persistent JSON and TXT exports, keeps per-chat progress in `state.json`, and can continue listening for new messages after the initial sync.

## Features

- Enumerates chats available to the authenticated Telegram account.
- Stores per-chat sync progress in `state/state.json`.
- Writes stable JSON and TXT export trees instead of per-run folders.
- Buckets chat history into weekly files.
- Downloads attachments from JSON media manifests when enabled.
- Tracks attachment discovery, retries, and verification in a durable SQLite media index.
- Retains every observed message version and records edits/deletions as append-only events.
- Writes state and replacement-style metadata atomically.
- Continues with other chats and media files when one item fails.
- Uses Pyrogram speedups with `TgCrypto` and `uvloop` when available.
- Can keep running with `--continuous` and append new messages immediately.
- Can use a Telegram takeout session with `--takeout`.

## Requirements

- Python 3.13+
- [`uv`](https://docs.astral.sh/uv/)
- Telegram API credentials from [my.telegram.org](https://my.telegram.org/)

Dependencies and the `tg-backup` console script are declared in [pyproject.toml](/Users/cofob/Development/tg_backup/pyproject.toml).

Kurigram speedups applied in this repo:

- `TgCrypto` is installed as a project dependency and is picked up automatically by Kurigram.
- `uvloop` is installed on Linux and activated before any `pyrogram.Client` instance is created.

## Installation

```bash
uv sync
```

## Configuration

The application reads configuration from environment variables.

Required:

```bash
export TG_BACKUP_APP_NAME="tg_backup"
export TG_BACKUP_API_ID="123456"
export TG_BACKUP_API_HASH="your_api_hash"
export TG_BACKUP_PHONE="+1234567890"
```

Optional:

```bash
export TG_BACKUP_EXPORT_JSON="true"
export TG_BACKUP_EXPORT_TEXT="true"
export TG_BACKUP_JSON_EXPORT_ROOT="./json"
export TG_BACKUP_TEXT_EXPORT_ROOT="./txt"
export TG_BACKUP_STATE_ROOT="./state"
export TG_BACKUP_DOWNLOAD_ATTACHMENTS="true"
```

Notes:

- `TG_BACKUP_EXPORT_JSON` defaults to `true`.
- `TG_BACKUP_EXPORT_TEXT` defaults to `true`.
- `TG_BACKUP_DOWNLOAD_ATTACHMENTS` defaults to `true`.
- `TG_BACKUP_STATE_ROOT` defaults to `./state`.
- `TG_BACKUP_JSON_EXPORT_ROOT` defaults to `./json`.
- `TG_BACKUP_TEXT_EXPORT_ROOT` defaults to `./txt`.
- `TG_BACKUP_DOWNLOAD_ATTACHMENTS=true` requires `TG_BACKUP_EXPORT_JSON=true`.
- The code reads environment variables directly. It does not load a `.env` file.

## Usage

Run as a module:

```bash
uv run python -m tg_backup
```

Or use the installed console script:

```bash
uv run tg-backup
```

Available flags:

- `--takeout`: build the Pyrogram client with `takeout=True`.
- `--continuous`: run the normal sync first, then stay connected and append new messages live.

Examples:

```bash
uv run python -m tg_backup --takeout
uv run python -m tg_backup --continuous
uv run python -m tg_backup --takeout --continuous
```

## Output Layout

By default the exporter writes into the current working directory:

```text
state/
json/
txt/
```

Typical layout:

```text
state/
  process.log
  state.json
  archive-index.sqlite3
  media-index.sqlite3
  <TG_BACKUP_APP_NAME>.session
  <TG_BACKUP_APP_NAME>.session-journal

json/
  chats.json
  chats/
    <chat_id>/
      info.json
      avatars.json
      topics.json
      YYYY-MM-wN.messages.json
      YYYY-MM-wN.medias.json
      YYYY-MM-wN.events.json
      <media type folders>/
    <chat_id>/<thread_id>/
      YYYY-MM-wN.messages.json
      YYYY-MM-wN.medias.json
      YYYY-MM-wN.events.json

txt/
  chats.txt
  chats/
    <chat_id>/
      topics.txt
      YYYY-MM-wN.txt
      YYYY-MM-wN.events.txt
    <chat_id>/<thread_id>/
      YYYY-MM-wN.txt
      YYYY-MM-wN.events.txt
```

Notes:

- `json/chats.json` maps chat id to chat name for the JSON tree.
- `txt/chats.txt` stores the same mapping as tab-separated lines.
- Forum chats also get per-chat topic mappings in `json/chats/<chat_id>/topics.json` and `txt/chats/<chat_id>/topics.txt`.
- Threaded messages are grouped under `<thread_id>/` when `reply_to_top_message_id` is present.
- Weekly buckets use the form `YYYY-MM-wN`.
- Attachment downloads are stored under the JSON chat directory because media metadata comes from `*.medias.json`.
- Media folder names are derived from Pyrogram file types plus `unknown_files`.
- Edit events contain both the previously retained version and the newly observed version. Deletion events contain the last retained version and a tombstone; original message and media exports are never removed.
- SQLite `-wal` and `-shm` sidecar files can exist beside the indexes while the process is running.

## Sync Behavior

On the first run the exporter:

1. Authenticates with Telegram.
2. Builds or refreshes `state/state.json` with the current chat list.
3. Backfills each chat until the oldest reachable message returned by Telegram.
4. Appends JSON/TXT output to the stable export tree.
5. Optionally downloads attachments from the JSON media manifests.

On later runs:

- chats with unfinished history resume from their last known `oldest_message_id`
- chats with completed history fetch only newer messages after `latest_message_id`
- mapping files and state are refreshed as chat metadata changes
- existing `*.messages.json` files are imported into the archive index once, so upgrades retain legacy text as the baseline for later edit events; older numeric Telegram IDs and legacy entity field names are normalized during import
- failed chats are recorded in state and do not prevent remaining chats from syncing
- failed or interrupted media downloads remain indexed for a later retry

Incoming Telegram updates are disabled for one-shot backups because history is fetched explicitly. This avoids unnecessary background update-recovery requests and makes reconnects much quieter. `--continuous` keeps incoming updates enabled.

With `--continuous`, update handlers are installed before the initial sync. Updates arriving during that sync are buffered, then applied in order. The process listens for new messages, edits, and authoritative Telegram deletion updates, writes append-only lifecycle events, updates `state.json`, and drains newly discovered attachments through a background media worker.

Media is downloaded to a unique staging path and moved into place only after size verification. A partial file from an interrupted transfer is retained with an `.incomplete*.bak` suffix. One failed file does not stop other workers, and failed files are retried on the next normal backup run. Existing zero-byte files are never treated as complete.

## State And Session Files

- `state/state.json` stores the chat list, progress fields (`history_complete`, `oldest_message_id`, and `latest_message_id`), and failure diagnostics (`failure_count`, `last_error`, and `last_error_at`). Successful syncs clear the current error without resetting the cumulative failure count.
- `state/archive-index.sqlite3` stores immutable first-observed/current message versions, deletion tombstones, lifecycle events, and a per-output-format delivery outbox. SQLite uses WAL mode with full synchronous commits.
- `state/media-index.sqlite3` stores stable media identity, the latest downloadable file id, target path, expected size, status, attempt count, and last error. SQLite uses WAL mode with full synchronous commits.
- `state.json`, chat/topic mappings, chat metadata, JSON export repairs, and lifecycle event files use same-directory temporary files plus atomic replacement. Existing file permissions are preserved.
- Pyrogram session files are stored in `TG_BACKUP_STATE_ROOT` because the client `workdir` is set to that directory.
- `state/process.log` contains exporter logs.

## Docker And CI

The repository includes [Dockerfile](/Users/cofob/Development/tg_backup/Dockerfile) and publishes `ghcr.io/cofob/tg-backup` from GitHub Actions on push via [ci.yml](/Users/cofob/Development/tg_backup/.github/workflows/ci.yml).

Run the local validation suite with:

```bash
uv run ruff check .
uv run mypy tg_backup
uv run python -m unittest discover -s tests -v
```

## Limitations

- Export completeness is bounded by what Telegram currently returns for a chat; the oldest available message id is not guaranteed to be `1`.
- “Original” means the first version observed by this exporter (including a version imported from an existing JSON backup). Telegram cannot reconstruct text that was edited before the first observation, nor intermediate edits missed while update recovery was unavailable.
- Authoritative deletion events come from Telegram updates. Run `--continuous` to receive and recover these updates; a one-shot history fetch cannot safely infer deletion from a missing id because access changes can also hide history.
- A later one-shot run audits edits in the newest overlap history page. Edits to older messages require `--continuous` update recovery; scanning every historical page on every run would be both rate-limit-heavy and still unable to reconstruct unobserved intermediate text.
- Existing text-only backups cannot be reconstructed into structured before/after versions; JSON exports are used for the one-time archive-index import.
