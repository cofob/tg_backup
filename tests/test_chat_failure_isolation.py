from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

from pyrogram.enums import ChatType
from pyrogram.types import Chat

from tg_backup.backup import (
    BackupState,
    ChatExportState,
    dump_backup_state,
    get_or_refresh_backup_state,
    sync_chats,
)

EXPECTED_PERSIST_CALLS = 2
OLD_CHAT_ID = 100
NEW_CHAT_ID = 200


def chat_state(chat_id: int) -> ChatExportState:
    return ChatExportState(
        id=chat_id,
        chat_type=ChatType.PRIVATE.name,
        username=None,
        qualname=f"Chat {chat_id}",
    )


class SyncChatsTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_chat_is_recorded_and_next_chat_is_still_synced(self) -> None:
        first = chat_state(1)
        second = chat_state(2)
        state = BackupState(chats=[first, second])
        synced_chat_ids: list[int] = []

        async def fake_sync_chat(client: object, *, chat: Chat, **kwargs: object) -> None:
            del client, kwargs
            assert chat.id is not None
            synced_chat_ids.append(chat.id)
            if chat.id == 1:
                raise OSError("connection reset")

        persist_state = Mock()
        with patch("tg_backup.backup.sync_chat", fake_sync_chat):
            await sync_chats(
                Mock(),
                state=state,
                chats_by_id={
                    1: Chat(id=1, type=ChatType.PRIVATE),
                    2: Chat(id=2, type=ChatType.PRIVATE),
                },
                persist_state=persist_state,
                json_output_dir=None,
                text_output_dir=None,
                export_json=False,
                export_text=False,
                media_index=None,
            )

        assert synced_chat_ids == [1, 2]
        assert first.failure_count == 1
        assert first.last_error == "OSError: connection reset"
        assert first.last_error_at is not None
        assert second.failure_count == 0
        assert second.last_error is None
        assert persist_state.call_count == EXPECTED_PERSIST_CALLS

    async def test_refresh_keeps_state_for_chats_no_longer_in_dialogs(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            state_file = Path(temporary_directory) / "state.json"
            dump_backup_state(state_file, BackupState(chats=[chat_state(OLD_CHAT_ID)]))
            current_chat = Chat(id=NEW_CHAT_ID, type=ChatType.PRIVATE, first_name="Current")

            with patch("tg_backup.backup.get_chats_info", AsyncMock(return_value=[current_chat])):
                state, chats_by_id = await get_or_refresh_backup_state(Mock(), state_file)

            assert [item.id for item in state.chats] == [NEW_CHAT_ID, OLD_CHAT_ID]
            assert set(chats_by_id) == {NEW_CHAT_ID}
