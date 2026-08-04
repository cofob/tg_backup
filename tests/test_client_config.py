from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pyrogram import Client

from tg_backup.__main__ import BackupClient, build_client


class BackupClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_consumes_transient_update_recovery_failure(self) -> None:
        client = object.__new__(BackupClient)
        handle_updates = AsyncMock(side_effect=OSError("Connection lost"))

        with (
            patch.object(Client, "handle_updates", handle_updates),
            self.assertLogs("tg_backup.backup", level="WARNING") as captured,
        ):
            await client.handle_updates(object())

        handle_updates.assert_awaited_once()
        assert "connection reset" in captured.output[0]


class BuildClientTests(unittest.TestCase):
    def build(self, *, continuous: bool) -> dict[str, object]:
        environment = {
            "TG_BACKUP_APP_NAME": "backup",
            "TG_BACKUP_API_ID": "12345",
            "TG_BACKUP_API_HASH": "hash",
            "TG_BACKUP_PHONE": "+123456789",
        }
        with (
            patch.dict("os.environ", environment, clear=True),
            patch("tg_backup.__main__.BackupClient") as client_class,
        ):
            build_client(takeout=False, continuous=continuous, workdir=Path("state"))

        return client_class.call_args.kwargs

    def test_disables_updates_for_one_shot_backup(self) -> None:
        kwargs = self.build(continuous=False)

        assert kwargs["no_updates"] is True
        assert kwargs["skip_updates"] is True
        assert kwargs["fetch_stickers"] is False
        assert kwargs["workers"] == 1

    def test_enables_updates_for_continuous_backup(self) -> None:
        kwargs = self.build(continuous=True)

        assert kwargs["no_updates"] is False
        assert kwargs["skip_updates"] is False
        assert kwargs["workers"] == 1
