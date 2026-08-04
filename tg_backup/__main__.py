from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
from contextlib import suppress
from pathlib import Path

from pyrogram import Client, idle, raw
from pyrogram.handlers import EditedMessageHandler, MessageHandler, RawUpdateHandler
from pyrogram.types import Message

from tg_backup.backup import (
    BackupSession,
    append_deleted_update,
    append_edited_message,
    append_live_message,
    backup,
    download_indexed_media,
    flush_archive_event_outbox,
    log,
)

ENV_PREFIX = "TG_BACKUP_"


class BackupClient(Client):
    async def handle_updates(self, updates: object) -> None:
        try:
            await super().handle_updates(updates)  # type: ignore[no-untyped-call]
        except (OSError, TimeoutError) as error:
            # Kurigram schedules this internal coroutine as a background task.
            # A reconnect can invalidate its gap-recovery RPC, but the session
            # itself will restart and recover subsequent updates.
            log.warning("Telegram update recovery was interrupted by a connection reset: %s", error)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--takeout", action="store_true", help="Use a Kurigram takeout session for export runs.")
    parser.add_argument("--continuous", action="store_true", help="Keep listening and append new messages after sync.")
    return parser.parse_args()


def configure_logging(state_output_dir: Path) -> None:
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    log_formatter = logging.Formatter("[%(asctime)s] %(levelname)s (%(name)s): %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)
    log.addHandler(console_handler)

    state_output_dir.mkdir(exist_ok=True, parents=True)
    log_path = state_output_dir / "process.log"
    log_path.touch(exist_ok=True)

    log_handler = logging.FileHandler(log_path, encoding="utf-8")
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)

    # Kurigram writes unknown RPC errors to ./unknown_errors.txt.
    # Use the writable state directory so containerized runs do not fail on /app.
    os.chdir(state_output_dir)


def get_env_name(name: str) -> str:
    return f"{ENV_PREFIX}{name}"


def get_str_env(name: str, *, default: str | None = None) -> str:
    value = os.getenv(get_env_name(name))
    if value is None:
        if default is None:
            raise ValueError(f"Missing required environment variable: {get_env_name(name)}")
        return default
    stripped = value.strip()
    if not stripped and default is None:
        raise ValueError(f"Environment variable {get_env_name(name)} must not be empty.")
    return stripped or (default if default is not None else stripped)


def get_int_env(name: str, *, default: int | None = None) -> int:
    value = os.getenv(get_env_name(name))
    if value is None:
        if default is None:
            raise ValueError(f"Missing required environment variable: {get_env_name(name)}")
        return default
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ValueError(f"Environment variable {get_env_name(name)} must be an integer.") from exc


def get_bool_env(name: str, *, default: bool) -> bool:
    value = os.getenv(get_env_name(name))
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Environment variable {get_env_name(name)} must be a boolean.")


def get_path_env(name: str, *, default: Path) -> Path:
    value = os.getenv(get_env_name(name))
    if value is None:
        return default
    return Path(value).expanduser()


def build_client(*, takeout: bool, continuous: bool, workdir: Path) -> Client:
    return BackupClient(
        name=get_str_env("APP_NAME"),
        api_id=get_int_env("API_ID"),
        api_hash=get_str_env("API_HASH"),
        phone_number=get_str_env("PHONE"),
        takeout=takeout,
        # A regular backup does not consume live updates. Disabling them keeps
        # Kurigram from spawning background gap-recovery requests while the
        # exporter is already fetching the same history explicitly.
        no_updates=not continuous,
        # Preserve queued edits/deletions across reconnects in continuous mode.
        skip_updates=not continuous,
        # Lifecycle updates must be applied in Telegram queue order. Multiple
        # dispatcher workers can let an edit overtake its original message.
        workers=1,
        # Sticker-set metadata is not needed by the export and otherwise adds
        # an RPC request while parsing every previously unseen sticker set.
        fetch_stickers=False,
        workdir=str(workdir),
    )


def get_default_root(directory_name: str) -> Path:
    return Path.cwd() / directory_name


async def run_app(args: argparse.Namespace) -> None:  # noqa: PLR0912, PLR0915
    try:
        uvloop = importlib.import_module("uvloop")
    except ImportError:
        pass
    else:
        uvloop.install()

    export_json = get_bool_env("EXPORT_JSON", default=True)
    export_text = get_bool_env("EXPORT_TEXT", default=True)
    download_attachments = get_bool_env("DOWNLOAD_ATTACHMENTS", default=True)

    state_output_dir = get_path_env("STATE_ROOT", default=get_default_root("state"))
    json_output_dir = get_path_env("JSON_EXPORT_ROOT", default=get_default_root("json")) if export_json else None
    text_output_dir = get_path_env("TEXT_EXPORT_ROOT", default=get_default_root("txt")) if export_text else None

    configure_logging(state_output_dir)
    log.info("Launching client")

    client = build_client(takeout=args.takeout, continuous=args.continuous, workdir=state_output_dir)
    session: BackupSession | None = None
    state_lock = asyncio.Lock()
    pending_updates: list[tuple[str, object]] = []
    media_wakeup = asyncio.Event()
    media_worker: asyncio.Task[None] | None = None

    async def run_media_worker() -> None:
        while True:
            await media_wakeup.wait()
            media_wakeup.clear()
            active_session = session
            if active_session is None or active_session.media_index is None:
                continue
            try:
                await download_indexed_media(
                    client,
                    active_session.media_index,
                    retry_failed=False,
                    allowed_root=active_session.json_output_dir,
                )
            except Exception:
                log.exception("Failed to drain the live media queue; pending items remain indexed.")

    async def apply_update(kind: str, payload: object, handler_client: Client, active_session: BackupSession) -> None:
        try:
            if kind == "new" and isinstance(payload, Message):
                await append_live_message(handler_client, payload, session=active_session)
            elif kind == "edited" and isinstance(payload, Message):
                append_edited_message(handler_client, payload, session=active_session)
            elif kind == "deleted":
                append_deleted_update(payload, session=active_session)
        except Exception:
            log.exception("Failed to preserve a live %s update; continuing.", kind)
        else:
            if active_session.download_attachments and active_session.media_index is not None:
                media_wakeup.set()

    async def dispatch_or_buffer(kind: str, payload: object, handler_client: Client) -> None:
        async with state_lock:
            if session is None:
                pending_updates.append((kind, payload))
                return
            await apply_update(kind, payload, handler_client, session)

    async def handle_message(handler_client: Client, message: Message) -> None:
        await dispatch_or_buffer("new", message, handler_client)

    async def handle_edited_message(handler_client: Client, message: Message) -> None:
        await dispatch_or_buffer("edited", message, handler_client)

    async def handle_raw_update(
        handler_client: Client,
        update: object,
        users: object,
        chats: object,
    ) -> None:
        del users, chats
        if isinstance(update, raw.types.UpdateDeleteMessages | raw.types.UpdateDeleteChannelMessages):
            await dispatch_or_buffer("deleted", update, handler_client)

    if args.continuous:
        client.add_handler(MessageHandler(handle_message))
        # Kurigram runs at most one matching handler per group. Keep lifecycle
        # handlers in separate groups so a generic MessageHandler cannot mask
        # an edited-message callback.
        client.add_handler(EditedMessageHandler(handle_edited_message), group=1)
        client.add_handler(RawUpdateHandler(handle_raw_update), group=2)

    try:
        completed_session = await backup(
            client,
            state_output_dir=state_output_dir,
            json_output_dir=json_output_dir,
            text_output_dir=text_output_dir,
            export_json=export_json,
            export_text=export_text,
            download_attachments=download_attachments,
        )
        if not args.continuous:
            session = completed_session
            return

        async with state_lock:
            session = completed_session
            media_worker = asyncio.create_task(run_media_worker(), name="tg-backup-media-worker")
            for kind, payload in pending_updates:
                await apply_update(kind, payload, client, session)
            pending_updates.clear()
        log.info("Continuous mode enabled. Listening for new messages.")
        await idle()
    finally:
        if media_worker is not None:
            media_worker.cancel()
            with suppress(asyncio.CancelledError):
                await media_worker
        try:
            if client.is_connected:
                # Stop and drain dispatcher callbacks while their indexes are
                # still open, then close persistence below.
                await client.stop()
        finally:
            if session is not None:
                try:
                    flush_archive_event_outbox(session)
                except Exception:
                    log.exception("Failed to flush the archive outbox during shutdown.")
            if session is not None and session.media_index is not None:
                try:
                    session.media_index.close()
                except Exception:
                    log.exception("Failed to close the media index cleanly.")
            if session is not None and session.archive_index is not None:
                try:
                    session.archive_index.close()
                except Exception:
                    log.exception("Failed to close the archive index cleanly.")


def main() -> None:
    args = parse_args()
    asyncio.run(run_app(args))


if __name__ == "__main__":
    main()
