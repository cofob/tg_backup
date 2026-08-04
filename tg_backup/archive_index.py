"""Append-only SQLite index for Telegram message versions and archive events."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum, StrEnum
from pathlib import Path
from types import TracebackType
from typing import Self

from pyrogram.enums import ChatType
from pyrogram.types import Message, MessageEntity

TELEGRAM_CHANNEL_CHAT_ID_CUTOFF = -1_000_000_000_000


def is_channel_chat_id(chat_id: int) -> bool:
    """Return whether a marked Telegram peer id belongs to a channel namespace.

    Telegram represents channels and supergroups as ``-100<channel_id>`` in
    high-level clients. Unlike private chats and basic groups, their message ids
    are scoped to the channel instead of the account-wide non-channel box.
    """

    return chat_id <= TELEGRAM_CHANNEL_CHAT_ID_CUTOFF


class ArchiveEventKind(StrEnum):
    """Kinds of immutable message lifecycle events retained by the archive."""

    EDITED = "edited"
    DELETED = "deleted"


class ArchiveEventSource(StrEnum):
    """How an archive event was discovered."""

    LIVE_UPDATE = "live_update"
    BATCH_AUDIT = "batch_audit"
    IMPORT = "import"


class ArchiveEventSink(StrEnum):
    """Built-in event export sinks tracked by the persistent outbox."""

    JSON = "json"
    TEXT = "text"


@dataclass(frozen=True, slots=True)
class EntitySnapshot:
    """Stable subset of a Telegram text entity that affects rendered content."""

    kind: str
    offset: int
    length: int
    url: str | None = None
    user_id: int | None = None
    language: str | None = None
    custom_emoji_id: str | None = None
    expandable: bool | None = None
    unix_time: int | None = None
    date_time_format: str | None = None

    @classmethod
    def from_entity(cls, entity: MessageEntity) -> EntitySnapshot:
        """Build a stable entity snapshot without retaining mutable user metadata."""

        entity_type = entity.type
        kind = entity_type.name if isinstance(entity_type, Enum) else str(entity_type)
        user_id = entity.user.id if entity.user is not None else None
        return cls(
            kind=kind,
            offset=entity.offset,
            length=entity.length,
            url=entity.url,
            user_id=user_id,
            language=entity.language,
            custom_emoji_id=entity.custom_emoji_id,
            expandable=entity.expandable,
            unix_time=entity.unix_time,
            date_time_format=entity.date_time_format,
        )


@dataclass(frozen=True, slots=True)
class MessageSnapshot:
    """Immutable metadata and content needed to identify one observed message version."""

    chat_id: int
    message_id: int
    is_channel: bool
    sent_at: datetime | None = None
    thread_id: int | None = None
    edit_date: datetime | None = None
    text: str | None = None
    caption: str | None = None
    entities: tuple[EntitySnapshot, ...] = ()
    caption_entities: tuple[EntitySnapshot, ...] = ()
    media_kind: str | None = None
    media_id: str | None = None

    @classmethod
    def from_message(
        cls,
        message: Message,
        *,
        chat_id: int | None = None,
        is_channel: bool | None = None,
    ) -> MessageSnapshot:
        """Extract a normalized snapshot from a Kurigram ``Message``.

        Explicit chat metadata is accepted for skeletal messages whose ``chat``
        field is absent. Content snapshots require a chat id from either source.
        """

        message_chat = message.chat
        resolved_chat_id = chat_id if chat_id is not None else (message_chat.id if message_chat is not None else None)
        if resolved_chat_id is None:
            raise ValueError("A message snapshot requires a chat id.")

        if is_channel is None:
            if message_chat is None or message_chat.type is None:
                raise ValueError("A message snapshot requires is_channel when chat type is unavailable.")
            is_channel = message_chat.type in {ChatType.CHANNEL, ChatType.SUPERGROUP}
        is_channel = is_channel or is_channel_chat_id(resolved_chat_id)

        thread_id = message.reply_to_top_message_id
        if thread_id is None:
            thread_id = message.message_thread_id

        media_kind, media_id = _message_media_identity(message)
        return cls(
            chat_id=resolved_chat_id,
            message_id=message.id,
            is_channel=is_channel,
            sent_at=message.date,
            thread_id=thread_id,
            edit_date=message.edit_date,
            text=None if message.text is None else str(message.text),
            caption=None if message.caption is None else str(message.caption),
            entities=_entity_snapshots(message.entities),
            caption_entities=_entity_snapshots(message.caption_entities),
            media_kind=media_kind,
            media_id=media_id,
        )

    @classmethod
    def from_export_payload(
        cls,
        payload: Mapping[str, object],
        *,
        chat_id: int,
        is_channel: bool,
    ) -> MessageSnapshot:
        """Restore the stable snapshot fields from an existing JSON export."""

        message_id = payload.get("id")
        if not isinstance(message_id, int) or isinstance(message_id, bool):
            raise TypeError("Exported message id must be an integer.")
        thread_id = _optional_integer(
            payload.get("reply_to_top_message_id"),
            field_name="reply_to_top_message_id",
            allow_numeric_string=True,
        )
        if thread_id is None:
            thread_id = _optional_integer(
                payload.get("message_thread_id"),
                field_name="message_thread_id",
                allow_numeric_string=True,
            )
        media_kind, media_id = _export_media_identity(payload)
        return cls(
            chat_id=chat_id,
            message_id=message_id,
            is_channel=is_channel or is_channel_chat_id(chat_id),
            sent_at=_export_datetime(payload.get("date")),
            thread_id=thread_id,
            edit_date=_export_datetime(payload.get("edit_date")),
            text=_optional_string(payload.get("text"), field_name="text"),
            caption=_optional_string(payload.get("caption"), field_name="caption"),
            entities=_export_entities(payload.get("entities")),
            caption_entities=_export_entities(payload.get("caption_entities")),
            media_kind=media_kind,
            media_id=media_id,
        )

    @property
    def text_payload(self) -> str | None:
        """Return message text, falling back to a media caption."""

        return self.text if self.text is not None else self.caption

    @property
    def content_hash(self) -> str:
        """Return a deterministic digest of user-visible content."""

        return stable_content_hash(self)


@dataclass(frozen=True, slots=True)
class MessageVersion:
    """A content snapshot stored by the index."""

    snapshot: MessageSnapshot
    content_hash: str
    first_observed_at: datetime


@dataclass(frozen=True, slots=True)
class MessageHead:
    """Original and most recently observed states of an indexed message."""

    original: MessageVersion
    current: MessageVersion
    deleted_observed_at: datetime | None
    version_count: int

    @property
    def is_deleted(self) -> bool:
        """Whether Telegram deletion has been observed without erasing content."""

        return self.deleted_observed_at is not None


@dataclass(frozen=True, slots=True)
class ArchiveEvent:
    """A deterministic append-only edit or deletion event."""

    event_id: str
    kind: ArchiveEventKind
    source: ArchiveEventSource
    observed_at: datetime
    chat_id: int
    message_id: int
    previous: MessageVersion | None
    current: MessageVersion | None
    telegram_update_type: str | None = None
    telegram_pts: int | None = None
    telegram_pts_count: int | None = None


@dataclass(frozen=True, slots=True)
class UnresolvedDeletionEvent:
    """A deletion update retained before its message can be mapped to a version head."""

    event_id: str
    source: ArchiveEventSource
    observed_at: datetime
    chat_id: int | None
    message_id: int
    telegram_update_type: str
    telegram_pts: int
    telegram_pts_count: int


def normalized_content(snapshot: MessageSnapshot) -> dict[str, object]:
    """Return the canonical content payload used for version hashing.

    Delivery metadata such as dates, chat ids, view counts, and reactions is
    deliberately absent. Formatting entities and stable media identity are part
    of the digest, so meaningful text, caption, formatting, and attachment
    changes form new versions.
    """

    return {
        "text": snapshot.text,
        "caption": snapshot.caption,
        "entities": [_entity_payload(entity) for entity in snapshot.entities],
        "caption_entities": [_entity_payload(entity) for entity in snapshot.caption_entities],
        "media": {
            "kind": snapshot.media_kind,
            "id": snapshot.media_id,
        },
    }


def stable_content_hash(snapshot: MessageSnapshot) -> str:
    """Hash normalized message content with canonical UTF-8 JSON."""

    encoded = json.dumps(
        normalized_content(snapshot),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ArchiveIndex:
    """Crash-safe message/version index and persistent event outbox.

    Original and version rows are immutable. Edits only advance a message head;
    deletions set a tombstone and retain every stored version. Event reservation
    and head mutation happen in one SQLite transaction.
    """

    _SCHEMA_VERSION = 2
    _CHANNEL_ID_SCHEMA_VERSION = 2

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        try:
            self._configure_connection()
            self._initialize_schema()
        except BaseException:
            self._connection.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()

    def index_original(
        self,
        snapshot: MessageSnapshot,
        *,
        observed_at: datetime | None = None,
    ) -> MessageHead:
        """Index the first observed state without replacing an existing original."""

        self.index_originals((snapshot,), observed_at=observed_at)

        return self._require_head(snapshot.chat_id, snapshot.message_id)

    def index_originals(
        self,
        snapshots: Iterable[MessageSnapshot],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        """Index first-observed states in one durable transaction."""

        observation_time = _utc_datetime(observed_at)
        with self._transaction():
            for snapshot in snapshots:
                normalized_snapshot = _normalize_channel_identity(snapshot)
                self._validate_snapshot(normalized_snapshot)
                existing = self._head_row(normalized_snapshot.chat_id, normalized_snapshot.message_id)
                if existing is not None:
                    if bool(existing["is_channel"]) != normalized_snapshot.is_channel:
                        raise ValueError("An indexed message cannot change between channel and non-channel identity.")
                    continue
                self._ensure_non_channel_id_available(normalized_snapshot)
                content_hash = stable_content_hash(normalized_snapshot)
                self._insert_version(normalized_snapshot, content_hash=content_hash, observed_at=observation_time)
                self._connection.execute(
                    """
                    INSERT INTO message_heads (
                        chat_id, message_id, is_channel, original_content_hash,
                        current_content_hash, current_edit_date, deleted_observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        normalized_snapshot.chat_id,
                        normalized_snapshot.message_id,
                        int(normalized_snapshot.is_channel),
                        content_hash,
                        content_hash,
                        _datetime_value(normalized_snapshot.edit_date),
                    ),
                )

    def get_head(self, chat_id: int, message_id: int) -> MessageHead | None:
        """Retrieve an indexed message's immutable original and current head."""

        head_row = self._head_row(chat_id, message_id)
        if head_row is None:
            return None
        return self._head_from_row(head_row)

    def get_exact(self, chat_id: int, message_id: int, content_hash: str) -> MessageVersion | None:
        """Retrieve one exact normalized content version by its digest."""

        row = self._version_row(chat_id, message_id, content_hash)
        return None if row is None else self._version_from_row(row)

    def resolve_non_channel_deletion(self, message_id: int) -> MessageHead | None:
        """Resolve a peer-less ``UpdateDeleteMessages`` id to its indexed message."""

        row = self._connection.execute(
            "SELECT chat_id FROM message_heads WHERE message_id = ? AND is_channel = 0",
            (message_id,),
        ).fetchone()
        return None if row is None else self.get_head(int(row["chat_id"]), message_id)

    def reserve_edit(
        self,
        snapshot: MessageSnapshot,
        *,
        source: ArchiveEventSource,
        observed_at: datetime | None = None,
    ) -> ArchiveEvent | None:
        """Atomically reserve a unique edit and advance the message head.

        An unknown message is indexed as its first-known snapshot and produces no
        edit event because Telegram did not provide the previous content. A
        duplicate/current or observably stale snapshot also produces no event.
        """

        snapshot = _normalize_channel_identity(snapshot)
        self._validate_snapshot(snapshot)
        observation_time = _utc_datetime(observed_at)
        event_id: str | None = None
        with self._transaction():
            head_row = self._head_row(snapshot.chat_id, snapshot.message_id)
            if head_row is None:
                self._ensure_non_channel_id_available(snapshot)
                current_hash = stable_content_hash(snapshot)
                self._insert_version(snapshot, content_hash=current_hash, observed_at=observation_time)
                self._connection.execute(
                    """
                    INSERT INTO message_heads (
                        chat_id, message_id, is_channel, original_content_hash,
                        current_content_hash, current_edit_date, deleted_observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        snapshot.chat_id,
                        snapshot.message_id,
                        int(snapshot.is_channel),
                        current_hash,
                        current_hash,
                        _datetime_value(snapshot.edit_date),
                    ),
                )
                return None

            if bool(head_row["is_channel"]) != snapshot.is_channel:
                raise ValueError("An indexed message cannot change between channel and non-channel identity.")

            previous_hash = str(head_row["current_content_hash"])
            current_hash = stable_content_hash(snapshot)
            if previous_hash == current_hash:
                return None

            previous_edit_date = _optional_datetime(str(head_row["current_edit_date"]))
            current_edit_date = _optional_utc_datetime(snapshot.edit_date)
            if _is_observably_stale(previous_edit_date, current_edit_date):
                return None

            self._insert_version(snapshot, content_hash=current_hash, observed_at=observation_time)
            event_id = _edit_event_id(
                chat_id=snapshot.chat_id,
                message_id=snapshot.message_id,
                previous_hash=previous_hash,
                current_hash=current_hash,
                current_edit_date=current_edit_date,
            )
            self._connection.execute(
                """
                INSERT OR IGNORE INTO archive_events (
                    event_id, kind, source, observed_at, chat_id, message_id,
                    previous_content_hash, current_content_hash,
                    previous_edit_date, current_edit_date,
                    telegram_update_type, telegram_pts, telegram_pts_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                """,
                (
                    event_id,
                    ArchiveEventKind.EDITED,
                    source,
                    _datetime_value(observation_time),
                    snapshot.chat_id,
                    snapshot.message_id,
                    previous_hash,
                    current_hash,
                    _datetime_value(previous_edit_date),
                    _datetime_value(current_edit_date),
                ),
            )
            self._connection.execute(
                """
                UPDATE message_heads
                SET current_content_hash = ?, current_edit_date = ?
                WHERE chat_id = ? AND message_id = ?
                """,
                (
                    current_hash,
                    _datetime_value(current_edit_date),
                    snapshot.chat_id,
                    snapshot.message_id,
                ),
            )

        return None if event_id is None else self._require_event(event_id)

    def reserve_delete(
        self,
        chat_id: int,
        message_id: int,
        *,
        source: ArchiveEventSource,
        observed_at: datetime | None = None,
        telegram_update_type: str | None = None,
        telegram_pts: int | None = None,
        telegram_pts_count: int | None = None,
    ) -> ArchiveEvent:
        """Atomically reserve a deterministic deletion and tombstone its head."""

        observation_time = _utc_datetime(observed_at)
        event_id = _delete_event_id(chat_id=chat_id, message_id=message_id)
        with self._transaction():
            head_row = self._head_row(chat_id, message_id)
            if head_row is None:
                raise KeyError((chat_id, message_id))

            self._connection.execute(
                """
                INSERT OR IGNORE INTO archive_events (
                    event_id, kind, source, observed_at, chat_id, message_id,
                    previous_content_hash, current_content_hash,
                    previous_edit_date, current_edit_date,
                    telegram_update_type, telegram_pts, telegram_pts_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?)
                """,
                (
                    event_id,
                    ArchiveEventKind.DELETED,
                    source,
                    _datetime_value(observation_time),
                    chat_id,
                    message_id,
                    str(head_row["current_content_hash"]),
                    head_row["current_edit_date"],
                    telegram_update_type,
                    telegram_pts,
                    telegram_pts_count,
                ),
            )
            self._connection.execute(
                """
                UPDATE message_heads
                SET deleted_observed_at = COALESCE(deleted_observed_at, ?)
                WHERE chat_id = ? AND message_id = ?
                """,
                (_datetime_value(observation_time), chat_id, message_id),
            )

        return self._require_event(event_id)

    def reserve_unresolved_delete(
        self,
        message_id: int,
        *,
        chat_id: int | None,
        source: ArchiveEventSource,
        telegram_update_type: str,
        telegram_pts: int,
        telegram_pts_count: int,
        observed_at: datetime | None = None,
    ) -> UnresolvedDeletionEvent:
        """Durably retain an authoritative deletion that cannot yet be mapped."""

        if message_id <= 0:
            raise ValueError("message_id must be positive")
        observation_time = _utc_datetime(observed_at)
        event_id = _unresolved_delete_event_id(chat_id=chat_id, message_id=message_id)
        with self._transaction():
            self._connection.execute(
                """
                INSERT OR IGNORE INTO unresolved_deletion_events (
                    event_id, source, observed_at, chat_id, message_id,
                    telegram_update_type, telegram_pts, telegram_pts_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    source,
                    _datetime_value(observation_time),
                    chat_id,
                    message_id,
                    telegram_update_type,
                    telegram_pts,
                    telegram_pts_count,
                ),
            )
        return self._require_unresolved_event(event_id)

    def get_event(self, event_id: str) -> ArchiveEvent | None:
        """Retrieve a reserved event and its retained before/after snapshots."""

        row = self._connection.execute("SELECT * FROM archive_events WHERE event_id = ?", (event_id,)).fetchone()
        return None if row is None else self._event_from_row(row)

    def iter_events(self) -> Iterator[ArchiveEvent]:
        """Iterate every reserved event in deterministic observation order."""

        cursor = self._connection.execute("SELECT * FROM archive_events ORDER BY observed_at, event_id")
        while rows := cursor.fetchmany(256):
            yield from (self._event_from_row(row) for row in rows)

    def iter_pending_events(
        self,
        sink: ArchiveEventSink,
        *,
        limit: int | None = None,
    ) -> Iterator[ArchiveEvent]:
        """Iterate outbox events not yet acknowledged by one export sink."""

        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative or None")
        sql = """
            SELECT event.*
            FROM archive_events AS event
            LEFT JOIN event_deliveries AS delivery
                ON delivery.event_id = event.event_id AND delivery.sink = ?
            WHERE delivery.event_id IS NULL
            ORDER BY event.observed_at, event.event_id
        """
        parameters: tuple[str | int, ...] = (sink,)
        if limit is not None:
            sql += " LIMIT ?"
            parameters = (*parameters, limit)
        cursor = self._connection.execute(sql, parameters)
        while rows := cursor.fetchmany(256):
            yield from (self._event_from_row(row) for row in rows)

    def mark_event_delivered(
        self,
        event_id: str,
        sink: ArchiveEventSink,
        *,
        delivered_at: datetime | None = None,
    ) -> None:
        """Acknowledge one event for one sink without modifying the event."""

        delivery_time = _utc_datetime(delivered_at)
        with self._transaction():
            if self.get_event(event_id) is None:
                raise KeyError(event_id)
            self._connection.execute(
                """
                INSERT OR IGNORE INTO event_deliveries (event_id, sink, delivered_at)
                VALUES (?, ?, ?)
                """,
                (event_id, sink, _datetime_value(delivery_time)),
            )

    def iter_pending_unresolved_events(
        self,
        sink: ArchiveEventSink,
        *,
        limit: int | None = None,
    ) -> Iterator[UnresolvedDeletionEvent]:
        """Iterate unresolved deletion events not acknowledged by one sink."""

        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative or None")
        sql = """
            SELECT event.*
            FROM unresolved_deletion_events AS event
            LEFT JOIN unresolved_event_deliveries AS delivery
                ON delivery.event_id = event.event_id AND delivery.sink = ?
            WHERE delivery.event_id IS NULL
            ORDER BY event.observed_at, event.event_id
        """
        parameters: tuple[str | int, ...] = (sink,)
        if limit is not None:
            sql += " LIMIT ?"
            parameters = (*parameters, limit)
        cursor = self._connection.execute(sql, parameters)
        while rows := cursor.fetchmany(256):
            yield from (self._unresolved_event_from_row(row) for row in rows)

    def mark_unresolved_event_delivered(
        self,
        event_id: str,
        sink: ArchiveEventSink,
        *,
        delivered_at: datetime | None = None,
    ) -> None:
        """Acknowledge one unresolved deletion for one output sink."""

        delivery_time = _utc_datetime(delivered_at)
        with self._transaction():
            if self.get_unresolved_event(event_id) is None:
                raise KeyError(event_id)
            self._connection.execute(
                """
                INSERT OR IGNORE INTO unresolved_event_deliveries (event_id, sink, delivered_at)
                VALUES (?, ?, ?)
                """,
                (event_id, sink, _datetime_value(delivery_time)),
            )

    def get_unresolved_event(self, event_id: str) -> UnresolvedDeletionEvent | None:
        """Retrieve one durably retained unresolved deletion event."""

        row = self._connection.execute(
            "SELECT * FROM unresolved_deletion_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return None if row is None else self._unresolved_event_from_row(row)

    def count_messages(self) -> int:
        """Return the number of retained message heads."""

        return self._count("message_heads")

    def count_versions(self) -> int:
        """Return the number of retained distinct content versions."""

        return self._count("message_versions")

    def count_events(self) -> int:
        """Return the number of immutable lifecycle events."""

        return self._count("archive_events")

    def source_needs_scan(self, source_path: Path, *, size: int, modified_ns: int) -> bool:
        """Return whether an exported message manifest changed since import."""

        row = self._connection.execute(
            "SELECT size, modified_ns FROM archive_sources WHERE source_path = ?",
            (str(source_path),),
        ).fetchone()
        return row is None or int(row["size"]) != size or int(row["modified_ns"]) != modified_ns

    def mark_source_scanned(self, source_path: Path, *, size: int, modified_ns: int) -> None:
        """Persist a successfully imported message-manifest fingerprint."""

        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO archive_sources (source_path, size, modified_ns, scanned_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    size = excluded.size,
                    modified_ns = excluded.modified_ns,
                    scanned_at = excluded.scanned_at
                """,
                (str(source_path), size, modified_ns, _datetime_value(_utc_datetime(None))),
            )

    def _configure_connection(self) -> None:
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")

    def _initialize_schema(self) -> None:
        version_row = self._connection.execute("PRAGMA user_version").fetchone()
        if version_row is None:  # pragma: no cover - SQLite always returns this row.
            raise RuntimeError("SQLite did not return an archive index schema version.")
        current_version = int(version_row[0])
        if current_version > self._SCHEMA_VERSION:
            raise RuntimeError(
                f"Archive index schema {current_version} is newer than supported version {self._SCHEMA_VERSION}."
            )
        needs_v2_migration = current_version < self._CHANNEL_ID_SCHEMA_VERSION

        with self._transaction():
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS message_versions (
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    sent_at TEXT,
                    thread_id INTEGER,
                    edit_date TEXT,
                    text TEXT,
                    caption TEXT,
                    entities_json TEXT NOT NULL,
                    caption_entities_json TEXT NOT NULL,
                    media_kind TEXT,
                    media_id TEXT,
                    first_observed_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, message_id, content_hash)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS message_heads (
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    is_channel INTEGER NOT NULL CHECK (is_channel IN (0, 1)),
                    original_content_hash TEXT NOT NULL,
                    current_content_hash TEXT NOT NULL,
                    current_edit_date TEXT,
                    deleted_observed_at TEXT,
                    PRIMARY KEY (chat_id, message_id),
                    FOREIGN KEY (chat_id, message_id, original_content_hash)
                        REFERENCES message_versions (chat_id, message_id, content_hash),
                    FOREIGN KEY (chat_id, message_id, current_content_hash)
                        REFERENCES message_versions (chat_id, message_id, content_hash)
                ) WITHOUT ROWID
                """
            )
            if needs_v2_migration:
                # Schema v1 trusted stale chat-type metadata during legacy
                # bootstrap. Correct every unambiguous marked channel id and
                # force one idempotent rescan so records skipped by the former
                # non-channel uniqueness conflict are recovered.
                self._connection.execute(
                    "UPDATE message_heads SET is_channel = 1 WHERE chat_id <= ? AND is_channel = 0",
                    (TELEGRAM_CHANNEL_CHAT_ID_CUTOFF,),
                )
            self._connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS message_heads_non_channel_id_idx
                ON message_heads (message_id) WHERE is_channel = 0
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_events (
                    event_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('edited', 'deleted')),
                    source TEXT NOT NULL CHECK (source IN ('live_update', 'batch_audit', 'import')),
                    observed_at TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    previous_content_hash TEXT,
                    current_content_hash TEXT,
                    previous_edit_date TEXT,
                    current_edit_date TEXT,
                    telegram_update_type TEXT,
                    telegram_pts INTEGER,
                    telegram_pts_count INTEGER,
                    FOREIGN KEY (chat_id, message_id)
                        REFERENCES message_heads (chat_id, message_id),
                    FOREIGN KEY (chat_id, message_id, previous_content_hash)
                        REFERENCES message_versions (chat_id, message_id, content_hash),
                    FOREIGN KEY (chat_id, message_id, current_content_hash)
                        REFERENCES message_versions (chat_id, message_id, content_hash)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS archive_events_message_idx
                ON archive_events (chat_id, message_id, observed_at)
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS event_deliveries (
                    event_id TEXT NOT NULL,
                    sink TEXT NOT NULL CHECK (sink IN ('json', 'text')),
                    delivered_at TEXT NOT NULL,
                    PRIMARY KEY (event_id, sink),
                    FOREIGN KEY (event_id) REFERENCES archive_events (event_id)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_sources (
                    source_path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    scanned_at TEXT NOT NULL
                ) WITHOUT ROWID
                """
            )
            if needs_v2_migration:
                self._connection.execute("DELETE FROM archive_sources")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS unresolved_deletion_events (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL CHECK (source IN ('live_update', 'batch_audit', 'import')),
                    observed_at TEXT NOT NULL,
                    chat_id INTEGER,
                    message_id INTEGER NOT NULL,
                    telegram_update_type TEXT NOT NULL,
                    telegram_pts INTEGER NOT NULL,
                    telegram_pts_count INTEGER NOT NULL
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS unresolved_event_deliveries (
                    event_id TEXT NOT NULL,
                    sink TEXT NOT NULL CHECK (sink IN ('json', 'text')),
                    delivered_at TEXT NOT NULL,
                    PRIMARY KEY (event_id, sink),
                    FOREIGN KEY (event_id) REFERENCES unresolved_deletion_events (event_id)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(f"PRAGMA user_version = {self._SCHEMA_VERSION}")

    def _head_row(self, chat_id: int, message_id: int) -> sqlite3.Row | None:
        return self._connection.execute(
            "SELECT * FROM message_heads WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()

    def _version_row(self, chat_id: int, message_id: int, content_hash: str) -> sqlite3.Row | None:
        return self._connection.execute(
            """
            SELECT version.*, head.is_channel
            FROM message_versions AS version
            JOIN message_heads AS head USING (chat_id, message_id)
            WHERE version.chat_id = ? AND version.message_id = ? AND version.content_hash = ?
            """,
            (chat_id, message_id, content_hash),
        ).fetchone()

    def _insert_version(self, snapshot: MessageSnapshot, *, content_hash: str, observed_at: datetime) -> None:
        self._connection.execute(
            """
            INSERT OR IGNORE INTO message_versions (
                chat_id, message_id, content_hash, sent_at, thread_id, edit_date,
                text, caption, entities_json, caption_entities_json,
                media_kind, media_id, first_observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.chat_id,
                snapshot.message_id,
                content_hash,
                _datetime_value(snapshot.sent_at),
                snapshot.thread_id,
                _datetime_value(snapshot.edit_date),
                snapshot.text,
                snapshot.caption,
                _entities_value(snapshot.entities),
                _entities_value(snapshot.caption_entities),
                snapshot.media_kind,
                snapshot.media_id,
                _datetime_value(observed_at),
            ),
        )

    def _head_from_row(self, row: sqlite3.Row) -> MessageHead:
        chat_id = int(row["chat_id"])
        message_id = int(row["message_id"])
        original_hash = str(row["original_content_hash"])
        current_hash = str(row["current_content_hash"])
        original = self._require_version(chat_id, message_id, original_hash)
        current = self._require_version(chat_id, message_id, current_hash)
        current_edit_date = _optional_datetime(str(row["current_edit_date"]))
        if current.snapshot.edit_date != current_edit_date:
            current = replace(current, snapshot=replace(current.snapshot, edit_date=current_edit_date))
        count_row = self._connection.execute(
            "SELECT COUNT(*) FROM message_versions WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()
        if count_row is None:  # pragma: no cover - COUNT always returns one row.
            raise RuntimeError("SQLite did not return a version count.")
        return MessageHead(
            original=original,
            current=current,
            deleted_observed_at=_optional_datetime(str(row["deleted_observed_at"])),
            version_count=int(count_row[0]),
        )

    def _version_from_row(self, row: sqlite3.Row) -> MessageVersion:
        snapshot = MessageSnapshot(
            chat_id=int(row["chat_id"]),
            message_id=int(row["message_id"]),
            is_channel=bool(row["is_channel"]),
            sent_at=_optional_datetime(str(row["sent_at"])),
            thread_id=None if row["thread_id"] is None else int(row["thread_id"]),
            edit_date=_optional_datetime(str(row["edit_date"])),
            text=None if row["text"] is None else str(row["text"]),
            caption=None if row["caption"] is None else str(row["caption"]),
            entities=_entities_from_value(str(row["entities_json"])),
            caption_entities=_entities_from_value(str(row["caption_entities_json"])),
            media_kind=None if row["media_kind"] is None else str(row["media_kind"]),
            media_id=None if row["media_id"] is None else str(row["media_id"]),
        )
        return MessageVersion(
            snapshot=snapshot,
            content_hash=str(row["content_hash"]),
            first_observed_at=_required_datetime(str(row["first_observed_at"])),
        )

    def _event_from_row(self, row: sqlite3.Row) -> ArchiveEvent:
        chat_id = int(row["chat_id"])
        message_id = int(row["message_id"])
        previous = self._event_version(
            chat_id,
            message_id,
            row["previous_content_hash"],
            row["previous_edit_date"],
        )
        current = self._event_version(
            chat_id,
            message_id,
            row["current_content_hash"],
            row["current_edit_date"],
        )
        return ArchiveEvent(
            event_id=str(row["event_id"]),
            kind=ArchiveEventKind(str(row["kind"])),
            source=ArchiveEventSource(str(row["source"])),
            observed_at=_required_datetime(str(row["observed_at"])),
            chat_id=chat_id,
            message_id=message_id,
            previous=previous,
            current=current,
            telegram_update_type=(None if row["telegram_update_type"] is None else str(row["telegram_update_type"])),
            telegram_pts=None if row["telegram_pts"] is None else int(row["telegram_pts"]),
            telegram_pts_count=None if row["telegram_pts_count"] is None else int(row["telegram_pts_count"]),
        )

    @staticmethod
    def _unresolved_event_from_row(row: sqlite3.Row) -> UnresolvedDeletionEvent:
        return UnresolvedDeletionEvent(
            event_id=str(row["event_id"]),
            source=ArchiveEventSource(str(row["source"])),
            observed_at=_required_datetime(str(row["observed_at"])),
            chat_id=None if row["chat_id"] is None else int(row["chat_id"]),
            message_id=int(row["message_id"]),
            telegram_update_type=str(row["telegram_update_type"]),
            telegram_pts=int(row["telegram_pts"]),
            telegram_pts_count=int(row["telegram_pts_count"]),
        )

    def _event_version(
        self,
        chat_id: int,
        message_id: int,
        content_hash_raw: object,
        edit_date_raw: object,
    ) -> MessageVersion | None:
        if content_hash_raw is None:
            return None
        version = self._require_version(chat_id, message_id, str(content_hash_raw))
        event_edit_date = _optional_datetime(str(edit_date_raw))
        if version.snapshot.edit_date == event_edit_date:
            return version
        return replace(version, snapshot=replace(version.snapshot, edit_date=event_edit_date))

    def _require_head(self, chat_id: int, message_id: int) -> MessageHead:
        head = self.get_head(chat_id, message_id)
        if head is None:  # pragma: no cover - protected by the preceding transaction.
            raise RuntimeError("Message head disappeared immediately after insertion.")
        return head

    def _require_version(self, chat_id: int, message_id: int, content_hash: str) -> MessageVersion:
        version = self.get_exact(chat_id, message_id, content_hash)
        if version is None:  # pragma: no cover - protected by foreign keys.
            raise RuntimeError("A referenced message version is missing from the archive index.")
        return version

    def _require_event(self, event_id: str) -> ArchiveEvent:
        event = self.get_event(event_id)
        if event is None:  # pragma: no cover - protected by the preceding transaction.
            raise RuntimeError("Archive event disappeared immediately after reservation.")
        return event

    def _require_unresolved_event(self, event_id: str) -> UnresolvedDeletionEvent:
        event = self.get_unresolved_event(event_id)
        if event is None:  # pragma: no cover - protected by the preceding transaction.
            raise RuntimeError("Unresolved deletion disappeared immediately after reservation.")
        return event

    def _ensure_non_channel_id_available(self, snapshot: MessageSnapshot) -> None:
        if snapshot.is_channel:
            return
        row = self._connection.execute(
            "SELECT chat_id FROM message_heads WHERE message_id = ? AND is_channel = 0",
            (snapshot.message_id,),
        ).fetchone()
        if row is not None and int(row["chat_id"]) != snapshot.chat_id:
            raise ValueError(
                f"Non-channel message id {snapshot.message_id} is already indexed for chat {int(row['chat_id'])}."
            )

    @staticmethod
    def _validate_snapshot(snapshot: MessageSnapshot) -> None:
        if snapshot.message_id <= 0:
            raise ValueError("message_id must be positive")
        if (snapshot.media_kind is None) != (snapshot.media_id is None):
            raise ValueError("media_kind and media_id must either both be set or both be None")

    def _count(self, table: str) -> int:
        allowed_tables = {"message_heads", "message_versions", "archive_events"}
        if table not in allowed_tables:  # pragma: no cover - private callers use constants.
            raise ValueError("Unsupported table")
        row = self._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
        if row is None:  # pragma: no cover - COUNT always returns one row.
            raise RuntimeError("SQLite did not return a count.")
        return int(row[0])

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        else:
            self._connection.execute("COMMIT")


def _entity_snapshots(entities: Sequence[MessageEntity] | None) -> tuple[EntitySnapshot, ...]:
    if not entities:
        return ()
    return tuple(EntitySnapshot.from_entity(entity) for entity in entities)


def _export_entities(value: object) -> tuple[EntitySnapshot, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TypeError("Exported message entities must be a JSON list.")
    snapshots: list[EntitySnapshot] = []
    for raw_entity in value:
        if not isinstance(raw_entity, Mapping):
            raise TypeError("Exported message entity must be a JSON object.")
        kind_raw = _first_non_null(raw_entity, "type", "kind", "_")
        if not isinstance(kind_raw, str):
            raise TypeError("Exported message entity has invalid required fields.")
        offset = _required_integer(
            raw_entity.get("offset"),
            field_name="entity.offset",
            allow_numeric_string=True,
        )
        length = _required_integer(
            raw_entity.get("length"),
            field_name="entity.length",
            allow_numeric_string=True,
        )
        user_raw = raw_entity.get("user")
        if isinstance(user_raw, Mapping):
            user_id_raw = user_raw.get("id")
        elif user_raw is not None:
            # Older Pyrogram/Kurigram exports could store the referenced user
            # directly instead of embedding the full User object.
            user_id_raw = user_raw
        else:
            user_id_raw = raw_entity.get("user_id")
        user_id = _optional_integer(
            user_id_raw,
            field_name="entity.user_id",
            allow_numeric_string=True,
        )
        custom_emoji_id = _optional_string(
            _first_non_null(raw_entity, "custom_emoji_id", "document_id"),
            field_name="entity.custom_emoji_id",
            allow_integer=True,
        )
        snapshots.append(
            EntitySnapshot(
                kind=kind_raw.rsplit(".", maxsplit=1)[-1],
                offset=offset,
                length=length,
                url=_optional_string(raw_entity.get("url"), field_name="entity.url"),
                user_id=user_id,
                language=_optional_string(raw_entity.get("language"), field_name="entity.language"),
                custom_emoji_id=custom_emoji_id,
                expandable=_optional_boolean(
                    _first_non_null(raw_entity, "expandable", "collapsed"),
                    field_name="entity.expandable",
                ),
                unix_time=_optional_integer(
                    _first_non_null(raw_entity, "unix_time", "date"),
                    field_name="entity.unix_time",
                    allow_numeric_string=True,
                ),
                date_time_format=_optional_string(
                    raw_entity.get("date_time_format"),
                    field_name="entity.date_time_format",
                ),
            )
        )
    return tuple(snapshots)


def _export_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        raise TypeError("Exported message date must be an ISO-8601 string.")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _export_media_identity(payload: Mapping[str, object]) -> tuple[str | None, str | None]:
    media_type = payload.get("media")
    if not isinstance(media_type, str):
        return None, None
    media_kind = media_type.rsplit(".", maxsplit=1)[-1].lower()
    media = payload.get(media_kind)
    if not isinstance(media, Mapping):
        return None, None

    stable_id = media.get("file_unique_id")
    if stable_id is None:
        stable_id = media.get("id")
    if stable_id is None:
        ignored = {"_", "file_id", "big_file_id", "file_reference", "date", "file_size"}
        stable_values = {
            str(name): value
            for name, value in media.items()
            if isinstance(name, str)
            and name not in ignored
            and (value is None or isinstance(value, str | int | float | bool))
        }
        if not stable_values:
            stable_values["type"] = str(media.get("_", media_kind))
        fallback = json.dumps(stable_values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        stable_id = hashlib.sha256(fallback.encode("utf-8")).hexdigest()
    return media_kind, str(stable_id)


def _entity_payload(entity: EntitySnapshot) -> dict[str, object]:
    return {
        "kind": entity.kind,
        "offset": entity.offset,
        "length": entity.length,
        "url": entity.url,
        "user_id": entity.user_id,
        "language": entity.language,
        "custom_emoji_id": entity.custom_emoji_id,
        "expandable": entity.expandable,
        "unix_time": entity.unix_time,
        "date_time_format": entity.date_time_format,
    }


def _entities_value(entities: Iterable[EntitySnapshot]) -> str:
    return json.dumps(
        [_entity_payload(entity) for entity in entities],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _entities_from_value(value: str) -> tuple[EntitySnapshot, ...]:
    raw: object = json.loads(value)
    if not isinstance(raw, list):
        raise TypeError("Stored message entities must be a JSON list.")
    entities: list[EntitySnapshot] = []
    for item in raw:
        if not isinstance(item, dict):
            raise TypeError("Stored message entity must be a JSON object.")
        entities.append(_entity_from_mapping(item))
    return tuple(entities)


def _entity_from_mapping(item: Mapping[object, object]) -> EntitySnapshot:
    kind = item.get("kind")
    offset = item.get("offset")
    length = item.get("length")
    if not isinstance(kind, str) or not isinstance(offset, int) or not isinstance(length, int):
        raise TypeError("Stored message entity has invalid required fields.")
    return EntitySnapshot(
        kind=kind,
        offset=offset,
        length=length,
        url=_optional_string(item.get("url"), field_name="stored entity.url"),
        user_id=_optional_integer(item.get("user_id"), field_name="stored entity.user_id"),
        language=_optional_string(item.get("language"), field_name="stored entity.language"),
        custom_emoji_id=_optional_string(
            item.get("custom_emoji_id"),
            field_name="stored entity.custom_emoji_id",
            allow_integer=True,
        ),
        expandable=_optional_boolean(item.get("expandable"), field_name="stored entity.expandable"),
        unix_time=_optional_integer(item.get("unix_time"), field_name="stored entity.unix_time"),
        date_time_format=_optional_string(
            item.get("date_time_format"),
            field_name="stored entity.date_time_format",
        ),
    )


def _message_media_identity(message: Message) -> tuple[str | None, str | None]:
    media_type = message.media
    media_kind: str | None
    media: object | None
    if media_type is not None:
        media_kind_raw = media_type.value if isinstance(media_type.value, str) else media_type.name.lower()
        media_kind = str(media_kind_raw)
        media = getattr(message, media_kind, None)
    else:
        media_kind, media = next(
            (
                (attribute, candidate)
                for attribute in (
                    "animation",
                    "audio",
                    "document",
                    "photo",
                    "sticker",
                    "video",
                    "video_note",
                    "voice",
                    "contact",
                    "location",
                    "venue",
                    "poll",
                    "dice",
                )
                if (candidate := getattr(message, attribute, None)) is not None
            ),
            (None, None),
        )

    if media_kind is None or media is None:
        return None, None

    stable_id = getattr(media, "file_unique_id", None)
    if stable_id is None:
        stable_id = getattr(media, "id", None)
    if stable_id is None:
        fallback = _media_fallback_identity(media)
        stable_id = hashlib.sha256(fallback.encode("utf-8")).hexdigest()
    return media_kind, str(stable_id)


def _media_fallback_identity(media: object) -> str:
    attributes = getattr(media, "__dict__", {})
    stable_values: dict[str, str | int | float | bool | None] = {}
    if isinstance(attributes, Mapping):
        ignored = {"_client", "file_id", "big_file_id", "file_reference", "date", "file_size"}
        for name, value in attributes.items():
            if not isinstance(name, str) or name.startswith("_") or name in ignored:
                continue
            if value is None or isinstance(value, str | int | float | bool):
                stable_values[name] = value
    if not stable_values:
        stable_values["type"] = type(media).__qualname__
    return json.dumps(stable_values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _edit_event_id(
    *,
    chat_id: int,
    message_id: int,
    previous_hash: str,
    current_hash: str,
    current_edit_date: datetime | None,
) -> str:
    return _event_digest(
        {
            "kind": ArchiveEventKind.EDITED,
            "chat_id": chat_id,
            "message_id": message_id,
            "previous": previous_hash,
            "current": current_hash,
            "edit_date": _datetime_value(current_edit_date),
        }
    )


def _delete_event_id(*, chat_id: int, message_id: int) -> str:
    return _event_digest(
        {
            "kind": ArchiveEventKind.DELETED,
            "chat_id": chat_id,
            "message_id": message_id,
        }
    )


def _unresolved_delete_event_id(*, chat_id: int | None, message_id: int) -> str:
    return _event_digest(
        {
            "kind": "unresolved_deleted",
            "chat_id": chat_id,
            "message_id": message_id,
        }
    )


def _event_digest(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _utc_datetime(value: datetime | None) -> datetime:
    return datetime.now(tz=UTC) if value is None else _as_utc(value)


def _optional_utc_datetime(value: datetime | None) -> datetime | None:
    return None if value is None else _as_utc(value)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _datetime_value(value: datetime | None) -> str | None:
    return None if value is None else _as_utc(value).isoformat()


def _optional_datetime(value: str) -> datetime | None:
    return None if value == "None" else _required_datetime(value)


def _required_datetime(value: str) -> datetime:
    return _as_utc(datetime.fromisoformat(value))


def _is_observably_stale(previous: datetime | None, current: datetime | None) -> bool:
    return previous is not None and current is not None and current < previous


def _normalize_channel_identity(snapshot: MessageSnapshot) -> MessageSnapshot:
    if snapshot.is_channel or not is_channel_chat_id(snapshot.chat_id):
        return snapshot
    return replace(snapshot, is_channel=True)


def _first_non_null(mapping: Mapping[object, object], *names: str) -> object:
    for name in names:
        value = mapping.get(name)
        if value is not None:
            return value
    return None


def _optional_string(value: object, *, field_name: str, allow_integer: bool = False) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if allow_integer and isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    expected = "a string, integer, or null" if allow_integer else "a string or null"
    raise ValueError(f"{field_name} must be {expected}; got {type(value).__name__}.")


def _required_integer(value: object, *, field_name: str, allow_numeric_string: bool = False) -> int:
    result = _optional_integer(value, field_name=field_name, allow_numeric_string=allow_numeric_string)
    if result is None:
        raise ValueError(f"{field_name} must not be null.")
    return result


def _optional_integer(value: object, *, field_name: str, allow_numeric_string: bool = False) -> int | None:
    if value is None:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if allow_numeric_string and isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    expected = "an integer, numeric string, or null" if allow_numeric_string else "an integer or null"
    raise ValueError(f"{field_name} must be {expected}; got {type(value).__name__}.")


def _optional_boolean(value: object, *, field_name: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} must be a boolean or null; got {type(value).__name__}.")
