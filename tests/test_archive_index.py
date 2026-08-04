# ruff: noqa: PLR2004

from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyrogram.enums import ChatType, MessageEntityType, MessageMediaType
from pyrogram.types import Chat, Message, MessageEntity, Photo
from pyrogram.types.object import Object

from tg_backup.archive_index import (
    ArchiveEventKind,
    ArchiveEventSink,
    ArchiveEventSource,
    ArchiveIndex,
    EntitySnapshot,
    MessageSnapshot,
    normalized_content,
    stable_content_hash,
)

SENT_AT = datetime(2026, 8, 1, 12, tzinfo=UTC)
FIRST_EDIT = datetime(2026, 8, 2, 12, tzinfo=UTC)
SECOND_EDIT = datetime(2026, 8, 3, 12, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 8, 4, 12, tzinfo=UTC)


def snapshot(  # noqa: PLR0913
    text: str | None,
    *,
    chat_id: int = 100,
    message_id: int = 7,
    is_channel: bool = False,
    edit_date: datetime | None = None,
    caption: str | None = None,
    entities: tuple[EntitySnapshot, ...] = (),
    media_kind: str | None = None,
    media_id: str | None = None,
) -> MessageSnapshot:
    return MessageSnapshot(
        chat_id=chat_id,
        message_id=message_id,
        is_channel=is_channel,
        sent_at=SENT_AT,
        thread_id=42,
        edit_date=edit_date,
        text=text,
        caption=caption,
        entities=entities,
        media_kind=media_kind,
        media_id=media_id,
    )


class ArchiveIndexTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "archive.sqlite3"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()


class ContentHashTests(unittest.TestCase):
    def test_hash_is_stable_across_non_content_metadata(self) -> None:
        original = snapshot("hello")
        metadata_changed = replace(
            original,
            chat_id=999,
            message_id=800,
            is_channel=True,
            sent_at=SENT_AT + timedelta(days=10),
            thread_id=100,
            edit_date=SECOND_EDIT,
        )

        assert normalized_content(original) == normalized_content(metadata_changed)
        assert stable_content_hash(original) == stable_content_hash(metadata_changed)

    def test_hash_covers_text_caption_entities_and_media_identity(self) -> None:
        base = snapshot("hello")
        bold = EntitySnapshot(kind="BOLD", offset=0, length=5)
        variants = [
            replace(base, text="goodbye"),
            replace(base, caption="caption"),
            replace(base, entities=(bold,)),
            replace(base, media_kind="photo", media_id="photo-one"),
        ]

        hashes = {stable_content_hash(base), *(stable_content_hash(item) for item in variants)}
        assert len(hashes) == len(variants) + 1

    def test_hash_is_canonical_for_equivalent_entity_instances(self) -> None:
        first = EntitySnapshot(kind="TEXT_LINK", offset=0, length=4, url="https://example.com")
        second = EntitySnapshot(kind="TEXT_LINK", offset=0, length=4, url="https://example.com")

        assert stable_content_hash(snapshot("link", entities=(first,))) == stable_content_hash(
            snapshot("link", entities=(second,))
        )


class MessageSnapshotFactoryTests(unittest.TestCase):
    def test_extracts_kurigram_message_content_and_stable_media_identity(self) -> None:
        entity = MessageEntity(type=MessageEntityType.BOLD, offset=0, length=5)
        photo = Photo(
            file_id="downloadable-file-id",
            file_unique_id="stable-photo-id",
            width=100,
            height=100,
            file_size=123,
            date=SENT_AT,
        )
        message = Message(
            id=9,
            chat=Chat(id=-100123, type=ChatType.SUPERGROUP),
            date=SENT_AT,
            edit_date=FIRST_EDIT,
            text="hello",
            entities=[entity],
            media=MessageMediaType.PHOTO,
            photo=photo,
            reply_to_top_message_id=42,
        )

        result = MessageSnapshot.from_message(message)

        assert result.chat_id == -100123
        assert result.message_id == 9
        assert result.is_channel is True
        assert result.text == "hello"
        assert result.entities == (EntitySnapshot(kind="BOLD", offset=0, length=5),)
        assert result.media_kind == "photo"
        assert result.media_id == "stable-photo-id"
        assert result.thread_id == 42

    def test_requires_explicit_chat_metadata_for_skeletal_message(self) -> None:
        message = Message(id=9)

        with self.assertRaisesRegex(ValueError, "chat id"):  # noqa: PT027 - unittest suite.
            MessageSnapshot.from_message(message)

        result = MessageSnapshot.from_message(message, chat_id=123, is_channel=False)
        assert result.chat_id == 123
        assert result.is_channel is False

    def test_existing_json_export_normalizes_to_the_same_snapshot(self) -> None:
        entity = MessageEntity(type=MessageEntityType.BOLD, offset=0, length=5)
        photo = Photo(
            file_id="downloadable-file-id",
            file_unique_id="stable-photo-id",
            width=100,
            height=100,
            file_size=123,
            date=SENT_AT,
        )
        message = Message(
            id=9,
            chat=Chat(id=-100123, type=ChatType.SUPERGROUP),
            date=SENT_AT,
            edit_date=FIRST_EDIT,
            caption="hello",
            caption_entities=[entity],
            media=MessageMediaType.PHOTO,
            photo=photo,
            reply_to_top_message_id=42,
        )
        payload = json.loads(json.dumps(message, default=Object.default))

        restored = MessageSnapshot.from_export_payload(payload, chat_id=-100123, is_channel=True)

        assert restored == MessageSnapshot.from_message(message)

    def test_legacy_numeric_custom_emoji_id_is_normalized(self) -> None:
        payload = {
            "id": 9,
            "date": str(SENT_AT),
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

        restored = MessageSnapshot.from_export_payload(payload, chat_id=-100123, is_channel=True)

        assert restored.entities == (
            EntitySnapshot(
                kind="CUSTOM_EMOJI",
                offset=7,
                length=2,
                custom_emoji_id="5373141891321699086",
            ),
        )

    def test_legacy_entity_aliases_and_numeric_strings_are_supported(self) -> None:
        payload = {
            "id": 9,
            "date": str(SENT_AT),
            "message_thread_id": "42",
            "text": "legacy",
            "entities": [
                {
                    "kind": "DATE_TIME",
                    "offset": "0",
                    "length": "6",
                    "user_id": "123456",
                    "document_id": 987654321,
                    "collapsed": True,
                    "date": "1785844800",
                    "date_time_format": "r",
                }
            ],
        }

        restored = MessageSnapshot.from_export_payload(payload, chat_id=-100123, is_channel=True)

        assert restored.thread_id == 42
        assert restored.entities == (
            EntitySnapshot(
                kind="DATE_TIME",
                offset=0,
                length=6,
                user_id=123456,
                custom_emoji_id="987654321",
                expandable=True,
                unix_time=1785844800,
                date_time_format="r",
            ),
        )

    def test_invalid_legacy_field_names_the_exact_field_and_type(self) -> None:
        payload = {
            "id": 9,
            "entities": [
                {
                    "type": "MessageEntityType.TEXT_LINK",
                    "offset": 0,
                    "length": 4,
                    "url": {"unexpected": "object"},
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, r"entity\.url.*dict"):  # noqa: PT027 - unittest suite.
            MessageSnapshot.from_export_payload(payload, chat_id=-100123, is_channel=True)


class OriginalIndexTests(ArchiveIndexTestCase):
    def test_original_head_and_exact_version_persist_across_reopen(self) -> None:
        original = snapshot("original")
        with ArchiveIndex(self.database_path) as index:
            head = index.index_original(original, observed_at=OBSERVED_AT)

            assert head.original.snapshot == original
            assert head.current == head.original
            assert head.version_count == 1
            assert head.is_deleted is False
            assert index.get_exact(100, 7, original.content_hash) == head.original
            assert index.resolve_non_channel_deletion(7) == head

        with ArchiveIndex(self.database_path) as reopened:
            assert reopened.get_head(100, 7) == head
            assert reopened.count_messages() == 1
            assert reopened.count_versions() == 1

    def test_reindexing_does_not_replace_the_original(self) -> None:
        original = snapshot("original")
        different = snapshot("different", edit_date=FIRST_EDIT)
        with ArchiveIndex(self.database_path) as index:
            first = index.index_original(original, observed_at=OBSERVED_AT)
            second = index.index_original(different, observed_at=OBSERVED_AT + timedelta(hours=1))

            assert second == first
            assert index.count_versions() == 1

    def test_channel_ids_may_repeat_but_non_channel_ids_are_unambiguous(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            index.index_original(snapshot("one", chat_id=-1001, is_channel=True))
            index.index_original(snapshot("two", chat_id=-1002, is_channel=True))
            index.index_original(snapshot("private", chat_id=10))

            with self.assertRaisesRegex(ValueError, "already indexed"):  # noqa: PT027 - unittest suite.
                index.index_original(snapshot("collision", chat_id=11))

            assert index.count_messages() == 3
            assert index.resolve_non_channel_deletion(7) == index.get_head(10, 7)

    def test_invalid_snapshot_rolls_back_without_partial_rows(self) -> None:
        invalid = snapshot("invalid", media_kind="photo")
        with ArchiveIndex(self.database_path) as index:
            with self.assertRaisesRegex(ValueError, "media_kind"):  # noqa: PT027 - unittest suite.
                index.index_original(invalid)

            assert index.count_messages() == 0
            assert index.count_versions() == 0

    def test_manifest_fingerprint_is_durable(self) -> None:
        source_path = Path("json/chats/100/2026-08-w1.messages.json")
        with ArchiveIndex(self.database_path) as index:
            assert index.source_needs_scan(source_path, size=10, modified_ns=20)
            index.mark_source_scanned(source_path, size=10, modified_ns=20)
            assert not index.source_needs_scan(source_path, size=10, modified_ns=20)
            assert index.source_needs_scan(source_path, size=11, modified_ns=20)

        with ArchiveIndex(self.database_path) as reopened:
            assert not reopened.source_needs_scan(source_path, size=10, modified_ns=20)


class EditEventTests(ArchiveIndexTestCase):
    def test_edit_preserves_before_after_and_advances_only_the_head(self) -> None:
        original = snapshot("before")
        edited = snapshot("after", edit_date=FIRST_EDIT)
        with ArchiveIndex(self.database_path) as index:
            index.index_original(original, observed_at=OBSERVED_AT)

            event = index.reserve_edit(
                edited,
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            )

            assert event is not None
            assert event.kind is ArchiveEventKind.EDITED
            assert event.previous is not None
            assert event.current is not None
            assert event.previous.snapshot.text_payload == "before"
            assert event.current.snapshot.text_payload == "after"
            assert event.previous.content_hash == original.content_hash
            assert event.current.content_hash == edited.content_hash

            head = index.get_head(100, 7)
            assert head is not None
            assert head.original.snapshot.text == "before"
            assert head.current.snapshot.text == "after"
            assert head.version_count == 2
            assert index.get_exact(100, 7, original.content_hash) == head.original

    def test_duplicate_edit_is_deduplicated_and_remains_in_outbox(self) -> None:
        original = snapshot("before")
        edited = snapshot("after", edit_date=FIRST_EDIT)
        with ArchiveIndex(self.database_path) as index:
            index.index_original(original)
            event = index.reserve_edit(edited, source=ArchiveEventSource.LIVE_UPDATE, observed_at=OBSERVED_AT)
            duplicate = index.reserve_edit(
                edited,
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            )

            assert event is not None
            assert duplicate is None
            assert index.count_events() == 1
            assert [pending.event_id for pending in index.iter_pending_events(ArchiveEventSink.JSON)] == [
                event.event_id
            ]

    def test_successive_edits_form_an_append_only_version_chain(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            index.index_original(snapshot("v1"))
            first = index.reserve_edit(
                snapshot("v2", edit_date=FIRST_EDIT),
                source=ArchiveEventSource.BATCH_AUDIT,
                observed_at=OBSERVED_AT,
            )
            second = index.reserve_edit(
                snapshot("v3", edit_date=SECOND_EDIT),
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            )

            assert first is not None
            assert second is not None
            assert second.previous is not None
            assert second.previous.snapshot.text == "v2"
            assert second.current is not None
            assert second.current.snapshot.text == "v3"
            assert first.event_id != second.event_id
            assert index.count_versions() == 3
            assert index.count_events() == 2

    def test_observably_stale_edit_does_not_roll_the_head_back(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            index.index_original(snapshot("v1"))
            index.reserve_edit(
                snapshot("v3", edit_date=SECOND_EDIT),
                source=ArchiveEventSource.LIVE_UPDATE,
            )

            stale = index.reserve_edit(
                snapshot("v2", edit_date=FIRST_EDIT),
                source=ArchiveEventSource.LIVE_UPDATE,
            )

            assert stale is None
            head = index.get_head(100, 7)
            assert head is not None
            assert head.current.snapshot.text == "v3"
            assert index.count_events() == 1

    def test_unknown_edit_becomes_first_known_snapshot_without_fabricated_before(self) -> None:
        edited = snapshot("first known", edit_date=FIRST_EDIT)
        with ArchiveIndex(self.database_path) as index:
            event = index.reserve_edit(edited, source=ArchiveEventSource.LIVE_UPDATE)

            assert event is None
            head = index.get_head(100, 7)
            assert head is not None
            assert head.original.snapshot == edited
            assert index.count_events() == 0


class DeleteEventTests(ArchiveIndexTestCase):
    def test_delete_tombstones_without_removing_any_version(self) -> None:
        original = snapshot("retained")
        with ArchiveIndex(self.database_path) as index:
            index.index_original(original)

            event = index.reserve_delete(
                100,
                7,
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT,
                telegram_update_type="UpdateDeleteMessages",
                telegram_pts=51,
                telegram_pts_count=1,
            )

            assert event.kind is ArchiveEventKind.DELETED
            assert event.previous is not None
            assert event.previous.snapshot.text_payload == "retained"
            assert event.current is None
            assert event.telegram_update_type == "UpdateDeleteMessages"
            assert event.telegram_pts == 51
            assert event.telegram_pts_count == 1

            head = index.get_head(100, 7)
            assert head is not None
            assert head.is_deleted is True
            assert head.original == head.current
            assert index.count_messages() == 1
            assert index.count_versions() == 1

    def test_duplicate_delete_returns_same_event_and_preserves_first_observation(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            index.index_original(snapshot("retained"))
            first = index.reserve_delete(
                100,
                7,
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT,
                telegram_pts=51,
            )
            duplicate = index.reserve_delete(
                100,
                7,
                source=ArchiveEventSource.BATCH_AUDIT,
                observed_at=OBSERVED_AT + timedelta(days=1),
            )

            assert duplicate == first
            assert index.count_events() == 1
            head = index.get_head(100, 7)
            assert head is not None
            assert head.deleted_observed_at == OBSERVED_AT

    def test_unknown_delete_is_not_misattributed(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            with self.assertRaises(KeyError):  # noqa: PT027 - unittest suite.
                index.reserve_delete(100, 404, source=ArchiveEventSource.LIVE_UPDATE)

            assert index.count_events() == 0


class EventOutboxTests(ArchiveIndexTestCase):
    def test_each_sink_is_acknowledged_independently_and_durably(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            index.index_original(snapshot("before"))
            edit = index.reserve_edit(
                snapshot("after", edit_date=FIRST_EDIT),
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT,
            )
            assert edit is not None
            deletion = index.reserve_delete(
                100,
                7,
                source=ArchiveEventSource.LIVE_UPDATE,
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            )

            assert [event.event_id for event in index.iter_pending_events(ArchiveEventSink.JSON)] == [
                edit.event_id,
                deletion.event_id,
            ]
            index.mark_event_delivered(edit.event_id, ArchiveEventSink.JSON)

            assert [event.event_id for event in index.iter_pending_events(ArchiveEventSink.JSON)] == [deletion.event_id]
            assert [event.event_id for event in index.iter_pending_events(ArchiveEventSink.TEXT)] == [
                edit.event_id,
                deletion.event_id,
            ]

        with ArchiveIndex(self.database_path) as reopened:
            assert [event.event_id for event in reopened.iter_pending_events(ArchiveEventSink.JSON)] == [
                deletion.event_id
            ]

    def test_pending_limit_validation_and_unknown_delivery(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            with self.assertRaisesRegex(ValueError, "non-negative"):  # noqa: PT027 - unittest suite.
                list(index.iter_pending_events(ArchiveEventSink.JSON, limit=-1))
            with self.assertRaises(KeyError):  # noqa: PT027 - unittest suite.
                index.mark_event_delivered("missing", ArchiveEventSink.JSON)

    def test_unresolved_deletion_has_a_durable_per_sink_outbox(self) -> None:
        with ArchiveIndex(self.database_path) as index:
            first = index.reserve_unresolved_delete(
                99,
                chat_id=None,
                source=ArchiveEventSource.LIVE_UPDATE,
                telegram_update_type="UpdateDeleteMessages",
                telegram_pts=10,
                telegram_pts_count=1,
                observed_at=OBSERVED_AT,
            )
            duplicate = index.reserve_unresolved_delete(
                99,
                chat_id=None,
                source=ArchiveEventSource.LIVE_UPDATE,
                telegram_update_type="UpdateDeleteMessages",
                telegram_pts=11,
                telegram_pts_count=1,
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            )

            assert duplicate == first
            assert list(index.iter_pending_unresolved_events(ArchiveEventSink.JSON)) == [first]
            assert list(index.iter_pending_unresolved_events(ArchiveEventSink.TEXT)) == [first]
            index.mark_unresolved_event_delivered(first.event_id, ArchiveEventSink.JSON)

        with ArchiveIndex(self.database_path) as reopened:
            assert list(reopened.iter_pending_unresolved_events(ArchiveEventSink.JSON)) == []
            assert list(reopened.iter_pending_unresolved_events(ArchiveEventSink.TEXT)) == [first]


if __name__ == "__main__":
    unittest.main()
