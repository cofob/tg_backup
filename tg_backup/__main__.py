import asyncio
import logging
from datetime import datetime as dt
from pathlib import Path

from pyrogram import Client

from tg_backup.backup import log, backup


APP_NAME = ""
API_ID = 0
API_HASH = ""
PHONE = ""


def main():
    try:
        import uvloop  # type: ignore
    except ImportError:
        pass
    else:
        uvloop.install()

    start_time = dt.now()

    log.debug("Setup logging")

    log.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s (%(name)s): %(message)s')

    # setup logging to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)
    log.addHandler(console_handler)

    backup_output_path = Path("C:") / "TelegramExports" / start_time.strftime('%Y-%m-%d_%H-%M-%S')
    backup_output_path.mkdir(exist_ok=True, parents=True)

    log_path = backup_output_path / 'process.log'
    log_path.touch(exist_ok=True)

    # setup logging to file
    log_handler = logging.FileHandler(log_path, encoding='utf-8')
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)

    log.info("Launching client")

    client = Client(name=APP_NAME, api_id=API_ID, api_hash=API_HASH, phone_number=PHONE)

    asyncio.run(backup(client, backup_output_path))


if __name__ == "__main__":
    main()
