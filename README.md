# tg_backup

`tg_backup` is a small Python utility for exporting Telegram chats and downloading chat media through the [Pyrogram](https://docs.pyrogram.org/) client API.

It connects to a Telegram account, enumerates available dialogs, exports chat metadata and message history to JSON, and then downloads referenced media files into a local export directory.

## What It Does

For each run, the tool:

1. Starts a Pyrogram client session.
2. Fetches the list of chats available to the authenticated account.
3. Exports per-chat metadata and message history.
4. Extracts media references from messages.
5. Downloads media files into chat-specific folders.
6. Stores progress so interrupted runs can resume from the last processed chat.

## Repository Layout

```text
tg_backup/
├── README.md
├── requirements.txt
└── tg_backup/
    ├── __main__.py
    ├── __init__.py
    ├── types.py
    ├── backup/
    │   └── __init__.py
    └── utils/
        ├── __init__.py
        ├── json_streaming.py
        ├── loading.py
        └── progress_tracking.py
```

## Requirements

- Python 3.11+ is a practical baseline for the current code.
- A Telegram API ID and API hash from [my.telegram.org](https://my.telegram.org/).
- A phone number for the Telegram account you want to authenticate.

Runtime dependencies are listed in [requirements.txt](requirements.txt):

- `Pyrogram==2.0.106`
- `adaptix==3.0.0b8`
- `json-stream==2.3.2`
- `uvloop` on Linux only

## Installation

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configuration

The application is configured directly in [tg_backup/__main__.py](tg_backup/__main__.py).

Set these module-level constants before running:

```python
APP_NAME = "tg_backup"
API_ID = 123456
API_HASH = "your_api_hash"
PHONE = "+1234567890"
```

### Configuration Fields

- `APP_NAME`: Pyrogram session name. This determines the generated session file name.
- `API_ID`: Telegram API ID from `my.telegram.org`.
- `API_HASH`: Telegram API hash paired with the API ID.
- `PHONE`: Phone number for the Telegram account, in international format.

### Important Notes

- Session files are stored locally by Pyrogram and are ignored by git via `.gitignore`.
- The current code does not read environment variables, `.env` files, or CLI flags.
- Secrets should not be committed to the repository.

## Running

Run the package as a module from the repository root:

```bash
python -m tg_backup
```

The first run may prompt for Telegram authentication details depending on whether a Pyrogram session already exists.

## Output

The tool writes exports to a timestamped directory:

```text
C:/TelegramExports/YYYY-MM-DD_HH-MM-SS/
```

Inside that directory you should expect files similar to:

```text
process.log
progress
chats.json
chats/
  <chat_id>/
    info.json
    avatars.json
    messages.json
    medias.json
    audios/
    documents/
    photos/
    stickers/
    videos/
    voices/
    ...
```

### Exported Files

- `process.log`: Debug and progress logs for the run.
- `progress`: Last completed chat index for resume support.
- `chats.json`: Top-level list of discovered chats.
- `info.json`: Detailed metadata for a single chat.
- `avatars.json`: Exported chat avatars metadata.
- `messages.json`: Chat history written as a JSON array.
- `medias.json`: Metadata for media discovered in the chat.

## How Resume Works

The tool uses [tg_backup/utils/progress_tracking.py](tg_backup/utils/progress_tracking.py) to persist the last completed chat index in the `progress` file.

If a run is interrupted:

- already completed chats are skipped on the next run
- the export continues from the next chat index

This resume logic is chat-based, not message-based.

## Development Notes

This repository is currently a lightweight script-style project:

- no `pyproject.toml`
- no packaging metadata
- no automated tests
- no CLI argument parser
- no environment-based configuration layer

Most of the application logic lives in [tg_backup/backup/__init__.py](tg_backup/backup/__init__.py).

## Platform Notes

- The export path is currently hardcoded to `C:/TelegramExports/...`.
- That path is Windows-oriented. On Linux or macOS, you will likely want to change the output directory in [tg_backup/__main__.py](tg_backup/__main__.py) before running.
- On Linux, `uvloop` is installed and enabled when available.

## Limitations

Current limitations visible in the codebase:

- Configuration is hardcoded in source code.
- `load_medias()` is present but not implemented.
- Media download logging reports a total count of zero because the current counter is not updated.
- The project depends directly on Pyrogram behavior and some internal types.

## Deployment

There is no deployment packaging, service definition, or container setup in this repository today. The practical deployment model is "run it as a local script on a machine that has access to your Telegram credentials and enough disk space for exports."

### Recommended Local Deployment Workflow

1. Clone the repository onto the machine that will perform backups.
2. Create a virtual environment.
3. Install dependencies from `requirements.txt`.
4. Edit [tg_backup/__main__.py](tg_backup/__main__.py) with your Telegram credentials.
5. Adjust the export output path if needed.
6. Run `python -m tg_backup`.
7. Keep the generated Pyrogram session file private.

### Operational Considerations

- Large chat histories can produce large JSON files and significant media storage usage.
- Telegram rate limits may trigger `FloodWait` handling during some operations.
- Re-running the tool may skip some work based on existing output files and the progress tracker.
- The export directory should be placed on reliable storage with enough free capacity.

## Debugging

The repository includes a VS Code launch configuration in [.vscode/launch.json](.vscode/launch.json) with a module-based `tg_backup` debug target.

Primary troubleshooting sources:

- `process.log` in the export directory
- console output during execution
- Pyrogram authentication/session prompts

## Future Improvements

Useful next steps for the project would be:

- move credentials to environment variables or a config file
- add a proper CLI
- make the output directory configurable
- add tests for JSON streaming and progress tracking
- add packaging metadata and a lockfile
- improve media download accounting and reporting

## Safety

This project handles private Telegram data. Treat exported JSON, media files, and session files as sensitive.

- Do not commit credentials.
- Do not publish export artifacts.
- Protect the machine and disk location where backups are stored.
