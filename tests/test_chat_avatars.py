from __future__ import annotations

import unittest
from collections.abc import AsyncIterator
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock

from pyrogram.enums import ChatType
from pyrogram.errors.exceptions.bad_request_400 import BadRequest
from pyrogram.types import Chat, Photo

from tg_backup.backup import dump_chat_json_metadata, get_chat_avatars


async def unsupported_avatar_history() -> AsyncIterator[Photo]:
    raise BadRequest(value="MONOFORUM_NOT_SUPPORTED", rpc_name="messages.Search")
    yield Photo()  # pragma: no cover - makes this an async generator


async def unexpected_avatar_failure() -> AsyncIterator[Photo]:
    raise BadRequest(value="CHAT_ADMIN_REQUIRED", rpc_name="messages.Search")
    yield Photo()  # pragma: no cover - makes this an async generator


class GetChatAvatarsTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsupported_avatar_history_does_not_abort_chat_backup(self) -> None:
        client = Mock()
        client.get_chat_photos.return_value = unsupported_avatar_history()

        with self.assertLogs("tg_backup.backup", level="WARNING") as captured:
            avatars = await get_chat_avatars(client, -100123)

        assert avatars is None
        assert "MONOFORUM_NOT_SUPPORTED" in captured.output[0]

    async def test_unsupported_history_preserves_existing_avatar_metadata(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            chat_dir = Path(temporary_directory) / "chat"
            chat_dir.mkdir()
            avatars_path = chat_dir / "avatars.json"
            original = b'[{"file_unique_id": "retained"}]'
            avatars_path.write_bytes(original)
            chat = Chat(id=-100123, type=ChatType.SUPERGROUP, title="Forum")
            client = Mock()
            client.get_chat = AsyncMock(return_value=chat)
            client.get_chat_photos.return_value = unsupported_avatar_history()

            await dump_chat_json_metadata(client, chat, json_chat_dir=chat_dir)

            assert avatars_path.read_bytes() == original

    async def test_unexpected_rpc_failure_is_not_silently_masked(self) -> None:
        client = Mock()
        client.get_chat_photos.return_value = unexpected_avatar_failure()

        with self.assertRaises(BadRequest):  # noqa: PT027 - unittest suite.
            await get_chat_avatars(client, -100123)
