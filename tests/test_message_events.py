from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pyrogram import raw
from pyrogram.enums import ChatType
from pyrogram.types import Chat, Message, User

from tg_backup.archive_index import ArchiveEventSink, ArchiveEventSource, ArchiveIndex, MessageSnapshot
from tg_backup.backup import (
    BackupSession,
    BackupState,
    append_deleted_update,
    append_edited_message_event,
    append_export_batch,
    flush_archive_event_outbox,
    import_archive_message_manifests,
)

MESSAGE_ID = 7
CHANNEL_CHAT_ID = -1000000000123
EXPECTED_EDIT_EVENT_COUNT = 2
UNKNOWN_MESSAGE_ID = 404


class MessageEventTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        root = Path(self.temporary_directory.name)
        self.json_root = root / "json"
        self.text_root = root / "txt"
        self.json_chat_dir = self.json_root / "chats" / "123"
        self.text_chat_dir = self.text_root / "chats" / "123"
        self.sent_at = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
        self.chat = Chat(id=123, type=ChatType.PRIVATE, first_name="Chat")
        self.author = User(id=456, first_name="Alice")
        self.archive_index = ArchiveIndex(root / "state" / "archive.sqlite3")
        self.addCleanup(self.archive_index.close)

    def message(self, text: str, *, edited_after: int | None = None) -> Message:
        edit_date = self.sent_at + timedelta(seconds=edited_after) if edited_after is not None else None
        return Message(
            id=MESSAGE_ID,
            chat=self.chat,
            from_user=self.author,
            date=self.sent_at,
            edit_date=edit_date,
            text=text,
        )

    def test_edit_preserves_original_and_appends_each_distinct_version_once(self) -> None:
        original = self.message("original text")
        self.archive_index.index_original(MessageSnapshot.from_message(original))
        append_export_batch(
            messages=[original],
            json_chat_dir=self.json_chat_dir,
            text_chat_dir=self.text_chat_dir,
            export_json=True,
            export_text=True,
        )
        original_json_path = self.json_chat_dir / "2026-08-w1.messages.json"
        original_text_path = self.text_chat_dir / "2026-08-w1.txt"
        original_json = original_json_path.read_bytes()
        original_text = original_text_path.read_bytes()

        first_edit = self.message("first edited text", edited_after=10)
        second_edit = self.message("second edited text", edited_after=20)
        for edited in (first_edit, first_edit, second_edit):
            append_edited_message_event(
                edited,
                json_chat_dir=self.json_chat_dir,
                text_chat_dir=self.text_chat_dir,
                source="live_update",
                archive_index=self.archive_index,
            )

        assert original_json_path.read_bytes() == original_json
        assert original_text_path.read_bytes() == original_text
        events = json.loads((self.json_chat_dir / "2026-08-w1.events.json").read_text(encoding="utf-8"))
        assert [event["current"]["text"] for event in events] == ["first edited text", "second edited text"]
        rendered_events = (self.text_chat_dir / "2026-08-w1.events.txt").read_text(encoding="utf-8")
        assert rendered_events.count("[event-id:") == EXPECTED_EDIT_EVENT_COUNT
        assert rendered_events.count("first edited text") == EXPECTED_EDIT_EVENT_COUNT
        assert rendered_events.count("second edited text") == 1
        assert "BEFORE: original text" in rendered_events
        assert "BEFORE: first edited text" in rendered_events

    def test_delete_adds_tombstone_without_removing_original_files(self) -> None:
        session = BackupSession(
            state=BackupState(chats=[]),
            state_file=Path(self.temporary_directory.name) / "state.json",
            json_output_dir=self.json_root,
            text_output_dir=self.text_root,
            export_json=True,
            export_text=True,
            download_attachments=False,
            archive_index=self.archive_index,
        )
        original_path = self.json_root / "chats" / str(CHANNEL_CHAT_ID) / "original.bin"
        original_path.parent.mkdir(parents=True)
        original_path.write_bytes(b"retained")
        channel_message = Message(
            id=MESSAGE_ID,
            chat=Chat(id=CHANNEL_CHAT_ID, type=ChatType.SUPERGROUP, title="Channel"),
            date=self.sent_at,
            text="retained original text",
        )
        self.archive_index.index_original(MessageSnapshot.from_message(channel_message))
        update = raw.types.UpdateDeleteChannelMessages(channel_id=123, messages=[MESSAGE_ID], pts=10, pts_count=1)

        append_deleted_update(update, session=session)
        append_deleted_update(update, session=session)

        assert original_path.read_bytes() == b"retained"
        events = json.loads((original_path.parent / "2026-08-w1.events.json").read_text(encoding="utf-8"))
        assert len(events) == 1
        assert events[0]["event_type"] == "message_deleted"
        assert events[0]["message_id"] == MESSAGE_ID
        assert events[0]["previous"]["text"] == "retained original text"
        assert "no archived data was deleted" in events[0]["retention"]

    def test_existing_json_export_is_imported_as_the_before_version(self) -> None:
        original = self.message("legacy original text")
        append_export_batch(
            messages=[original],
            json_chat_dir=self.json_chat_dir,
            text_chat_dir=None,
            export_json=True,
            export_text=False,
        )

        import_archive_message_manifests(
            self.json_chat_dir,
            chat_id=self.chat.id,
            is_channel=False,
            archive_index=self.archive_index,
        )
        append_edited_message_event(
            self.message("new edited text", edited_after=10),
            json_chat_dir=self.json_chat_dir,
            text_chat_dir=None,
            source=ArchiveEventSource.LIVE_UPDATE,
            archive_index=self.archive_index,
        )

        events = json.loads((self.json_chat_dir / "2026-08-w1.events.json").read_text(encoding="utf-8"))
        assert events[0]["previous"]["text"] == "legacy original text"
        assert events[0]["current"]["text"] == "new edited text"

    def test_malformed_legacy_entry_does_not_block_valid_import(self) -> None:
        append_export_batch(
            messages=[self.message("valid")],
            json_chat_dir=self.json_chat_dir,
            text_chat_dir=None,
            export_json=True,
            export_text=False,
        )
        manifest_path = self.json_chat_dir / "2026-08-w1.messages.json"
        items = json.loads(manifest_path.read_text(encoding="utf-8"))
        items.append({"id": "not-an-integer"})
        manifest_path.write_text(json.dumps(items), encoding="utf-8")

        import_archive_message_manifests(
            self.json_chat_dir,
            chat_id=self.chat.id,
            is_channel=False,
            archive_index=self.archive_index,
        )

        assert self.archive_index.count_messages() == 1
        stat = manifest_path.stat()
        assert self.archive_index.source_needs_scan(
            manifest_path.absolute(),
            size=stat.st_size,
            modified_ns=stat.st_mtime_ns,
        )

    def test_legacy_numeric_entity_ids_import_once_without_retrying_manifest(self) -> None:
        self.json_chat_dir.mkdir(parents=True)
        manifest_path = self.json_chat_dir / "2026-08-w1.messages.json"
        manifest_path.write_text(
            json.dumps(
                [
                    {
                        "id": MESSAGE_ID,
                        "date": str(self.sent_at),
                        "text": "legacy custom emoji",
                        "entities": [
                            {
                                "type": "MessageEntityType.CUSTOM_EMOJI",
                                "offset": 7,
                                "length": 2,
                                "custom_emoji_id": 5373141891321699086,
                            }
                        ],
                    }
                ]
            ),
            encoding="utf-8",
        )

        import_archive_message_manifests(
            self.json_chat_dir,
            chat_id=self.chat.id,
            is_channel=False,
            archive_index=self.archive_index,
        )

        head = self.archive_index.get_head(self.chat.id, MESSAGE_ID)
        assert head is not None
        assert head.original.snapshot.entities[0].custom_emoji_id == "5373141891321699086"
        stat = manifest_path.stat()
        assert not self.archive_index.source_needs_scan(
            manifest_path.absolute(),
            size=stat.st_size,
            modified_ns=stat.st_mtime_ns,
        )

    def test_outbox_retry_deduplicates_file_written_before_acknowledgement(self) -> None:
        original = self.message("before")
        self.archive_index.index_original(MessageSnapshot.from_message(original))
        edited = self.message("after", edited_after=10)

        with (
            patch.object(self.archive_index, "mark_event_delivered", side_effect=OSError("database busy")),
            self.assertRaisesRegex(OSError, "database busy"),  # noqa: PT027 - unittest suite.
        ):
            append_edited_message_event(
                edited,
                json_chat_dir=self.json_chat_dir,
                text_chat_dir=None,
                source=ArchiveEventSource.LIVE_UPDATE,
                archive_index=self.archive_index,
            )

        session = BackupSession(
            state=BackupState(chats=[]),
            state_file=Path(self.temporary_directory.name) / "state.json",
            json_output_dir=self.json_root,
            text_output_dir=None,
            export_json=True,
            export_text=False,
            download_attachments=False,
            archive_index=self.archive_index,
        )
        flush_archive_event_outbox(session)

        events = json.loads((self.json_chat_dir / "2026-08-w1.events.json").read_text(encoding="utf-8"))
        assert len(events) == 1
        assert list(self.archive_index.iter_pending_events(ArchiveEventSink.JSON)) == []

    def test_peerless_private_delete_resolves_from_archive_index(self) -> None:
        original = self.message("private original")
        self.archive_index.index_original(MessageSnapshot.from_message(original))
        session = BackupSession(
            state=BackupState(chats=[]),
            state_file=Path(self.temporary_directory.name) / "state.json",
            json_output_dir=self.json_root,
            text_output_dir=None,
            export_json=True,
            export_text=False,
            download_attachments=False,
            archive_index=self.archive_index,
        )

        append_deleted_update(
            raw.types.UpdateDeleteMessages(messages=[MESSAGE_ID], pts=11, pts_count=1),
            session=session,
        )

        head = self.archive_index.get_head(self.chat.id, MESSAGE_ID)
        assert head is not None
        assert head.is_deleted is True
        events = json.loads((self.json_chat_dir / "2026-08-w1.events.json").read_text(encoding="utf-8"))
        assert events[0]["previous"]["text"] == "private original"

    def test_unknown_delete_is_durably_exported_without_guessing_a_chat(self) -> None:
        session = BackupSession(
            state=BackupState(chats=[]),
            state_file=Path(self.temporary_directory.name) / "state.json",
            json_output_dir=self.json_root,
            text_output_dir=None,
            export_json=True,
            export_text=False,
            download_attachments=False,
            archive_index=self.archive_index,
        )

        append_deleted_update(
            raw.types.UpdateDeleteMessages(messages=[UNKNOWN_MESSAGE_ID], pts=12, pts_count=1),
            session=session,
        )

        path = self.json_root / "unresolved.message-events.json"
        events = json.loads(path.read_text(encoding="utf-8"))
        assert len(events) == 1
        assert events[0]["chat_id"] is None
        assert events[0]["message_id"] == UNKNOWN_MESSAGE_ID
        assert list(self.archive_index.iter_pending_unresolved_events(ArchiveEventSink.JSON)) == []
