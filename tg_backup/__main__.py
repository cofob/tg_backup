from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
from pathlib import Path

from pyrogram import Client, idle
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message

from tg_backup.backup import append_live_message, backup, log

ENV_PREFIX = "TG_BACKUP_"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--takeout", action="store_true", help="Use a Pyrogram takeout session for export runs.")
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

    # Pyrogram writes unknown RPC errors to ./unknown_errors.txt.
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


def build_client(*, takeout: bool, workdir: Path) -> Client:
    return Client(
        name=get_str_env("APP_NAME"),
        api_id=get_int_env("API_ID"),
        api_hash=get_str_env("API_HASH"),
        phone_number=get_str_env("PHONE"),
        takeout=takeout,
        workdir=str(workdir),
    )


def get_default_root(directory_name: str) -> Path:
    return Path.cwd() / directory_name


async def run_app(args: argparse.Namespace) -> None:
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

    client = build_client(takeout=args.takeout, workdir=state_output_dir)
    try:
        session = await backup(
            client,
            state_output_dir=state_output_dir,
            json_output_dir=json_output_dir,
            text_output_dir=text_output_dir,
            export_json=export_json,
            export_text=export_text,
            download_attachments=download_attachments,
        )
        if not args.continuous:
            return

        state_lock = asyncio.Lock()

        async def handle_message(handler_client: Client, message: Message) -> None:
            async with state_lock:
                await append_live_message(handler_client, message, session=session)

        client.add_handler(MessageHandler(handle_message))
        log.info("Continuous mode enabled. Listening for new messages.")
        await idle()
    finally:
        if client.is_connected:
            await client.stop()


def main() -> None:
    args = parse_args()
    asyncio.run(run_app(args))


if __name__ == "__main__":
    main()
