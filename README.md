# tg_backup

`tg_backup` exports Telegram chat history with [Pyrogram](https://docs.pyrogram.org/). It writes persistent JSON and TXT exports, keeps per-chat progress in `state.json`, and can continue listening for new messages after the initial sync.

## Features

- Enumerates chats available to the authenticated Telegram account.
- Stores per-chat sync progress in `state/state.json`.
- Writes stable JSON and TXT export trees instead of per-run folders.
- Buckets chat history into weekly files.
- Downloads attachments from JSON media manifests when enabled.
- Can keep running with `--continuous` and append new messages immediately.
- Can use a Telegram takeout session with `--takeout`.

## Requirements

- Python 3.13+
- [`uv`](https://docs.astral.sh/uv/)
- Telegram API credentials from [my.telegram.org](https://my.telegram.org/)

Dependencies and the `tg-backup` console script are declared in [pyproject.toml](/Users/cofob/Development/tg_backup/pyproject.toml).

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
      <media type folders>/
    <chat_id>/<thread_id>/
      YYYY-MM-wN.messages.json
      YYYY-MM-wN.medias.json

txt/
  chats.txt
  chats/
    <chat_id>/
      topics.txt
      YYYY-MM-wN.txt
    <chat_id>/<thread_id>/
      YYYY-MM-wN.txt
```

Notes:

- `json/chats.json` maps chat id to chat name for the JSON tree.
- `txt/chats.txt` stores the same mapping as tab-separated lines.
- Forum chats also get per-chat topic mappings in `json/chats/<chat_id>/topics.json` and `txt/chats/<chat_id>/topics.txt`.
- Threaded messages are grouped under `<thread_id>/` when `reply_to_top_message_id` is present.
- Weekly buckets use the form `YYYY-MM-wN`.
- Attachment downloads are stored under the JSON chat directory because media metadata comes from `*.medias.json`.
- Media folder names are derived from Pyrogram file types plus `unknown_files`.

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

With `--continuous`, after the sync phase the process stays connected, listens for new messages on all chats, appends them immediately, updates `state.json`, and downloads new attachments when enabled.

## State And Session Files

- `state/state.json` stores the chat list and per-chat progress fields such as `history_complete`, `oldest_message_id`, and `latest_message_id`.
- Pyrogram session files are stored in `TG_BACKUP_STATE_ROOT` because the client `workdir` is set to that directory.
- `state/process.log` contains exporter logs.

## Docker And CI

The repository includes [Dockerfile](/Users/cofob/Development/tg_backup/Dockerfile) and publishes `ghcr.io/cofob/tg-backup` from GitHub Actions on push via [ci.yml](/Users/cofob/Development/tg_backup/.github/workflows/ci.yml).

## Limitations

- Export completeness is bounded by what Telegram currently returns for a chat; the oldest available message id is not guaranteed to be `1`.
- Continuous mode depends on Pyrogram update delivery while the process is running.
- The project still has limited automated coverage.
