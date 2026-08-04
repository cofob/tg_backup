from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from tg_backup.backup import backup


class BackupResourceCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_indexes_are_closed_when_startup_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            media_index = Mock()
            archive_index = Mock()
            client = Mock()
            client.start = AsyncMock(side_effect=OSError("connection reset"))

            with (
                patch("tg_backup.backup.MediaIndex", return_value=media_index),
                patch("tg_backup.backup.ArchiveIndex", return_value=archive_index),
                self.assertRaisesRegex(OSError, "connection reset"),  # noqa: PT027 - unittest suite.
            ):
                await backup(
                    client,
                    state_output_dir=root / "state",
                    json_output_dir=root / "json",
                    text_output_dir=None,
                    export_json=True,
                    export_text=False,
                    download_attachments=True,
                )

            media_index.close.assert_called_once_with()
            archive_index.close.assert_called_once_with()

    async def test_media_index_is_closed_if_archive_index_creation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            media_index = Mock()

            with (
                patch("tg_backup.backup.MediaIndex", return_value=media_index),
                patch("tg_backup.backup.ArchiveIndex", side_effect=OSError("database unavailable")),
                self.assertRaisesRegex(OSError, "database unavailable"),  # noqa: PT027 - unittest suite.
            ):
                await backup(
                    Mock(),
                    state_output_dir=root / "state",
                    json_output_dir=root / "json",
                    text_output_dir=None,
                    export_json=True,
                    export_text=False,
                    download_attachments=True,
                )

            media_index.close.assert_called_once_with()
