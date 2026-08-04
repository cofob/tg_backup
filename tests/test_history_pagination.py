from __future__ import annotations

import unittest
from collections.abc import AsyncIterator
from unittest.mock import Mock, patch

from pyrogram.enums import ChatType
from pyrogram.types import Chat, Message

from tg_backup.backup import ChatExportState, append_chat_history, get_chat_messages


async def empty_history() -> AsyncIterator[Message]:
    if False:
        yield Message()


class GetChatMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_max_id_without_deprecated_offset_id(self) -> None:
        client = Mock()
        client.get_chat_history.return_value = empty_history()

        batches = [batch async for batch in get_chat_messages(client, 123, max_id=41)]

        assert batches == [[]]
        client.get_chat_history.assert_called_once_with(chat_id=123, max_id=41)


class AppendChatHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_resumes_strictly_before_oldest_exported_message(self) -> None:
        captured_max_ids: list[int] = []

        async def capture_messages(
            client: object,
            chat_id: int,
            *,
            batch_size: int = 1000,
            max_id: int = 0,
        ) -> AsyncIterator[list[Message]]:
            del client, chat_id, batch_size
            captured_max_ids.append(max_id)
            if False:
                yield []

        chat_state = ChatExportState(
            id=123,
            chat_type=ChatType.PRIVATE.name,
            username=None,
            qualname="Test",
            oldest_message_id=42,
        )
        persist_state = Mock()

        with patch("tg_backup.backup.get_chat_messages", capture_messages):
            await append_chat_history(
                Mock(),
                Chat(id=123, type=ChatType.PRIVATE),
                chat_state,
                persist_state=persist_state,
                json_chat_dir=None,
                text_chat_dir=None,
                export_json=False,
                export_text=False,
            )

        assert captured_max_ids == [41]
        assert chat_state.history_complete is True
        persist_state.assert_called_once_with()

    async def test_message_one_completes_without_unbounded_request(self) -> None:
        chat_state = ChatExportState(
            id=123,
            chat_type=ChatType.PRIVATE.name,
            username=None,
            qualname="Test",
            oldest_message_id=1,
        )
        persist_state = Mock()

        with patch("tg_backup.backup.get_chat_messages") as get_messages:
            await append_chat_history(
                Mock(),
                Chat(id=123, type=ChatType.PRIVATE),
                chat_state,
                persist_state=persist_state,
                json_chat_dir=None,
                text_chat_dir=None,
                export_json=False,
                export_text=False,
            )

        get_messages.assert_not_called()
        assert chat_state.history_complete is True
        persist_state.assert_called_once_with()
