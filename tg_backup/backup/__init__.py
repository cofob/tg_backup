from __future__ import annotations

import asyncio
import binascii
import bisect
import hashlib
import json
import logging
import struct
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC
from datetime import datetime as dt
from functools import cached_property
from pathlib import Path
from typing import NamedTuple, Protocol, TypeAlias, TypeVar
from uuid import uuid4

from pyrogram import Client, raw
from pyrogram import utils as pyrogram_utils
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait, RPCError, UserIdInvalid
from pyrogram.file_id import PHOTO_TYPES, FileId, FileType
from pyrogram.types import (
    Animation,
    Audio,
    Chat,
    Dialog,
    Document,
    Message,
    Photo,
    Sticker,
    User,
    Video,
    VideoNote,
    Voice,
)
from pyrogram.types.object import Object

from tg_backup.archive_index import (
    ArchiveEvent,
    ArchiveEventKind,
    ArchiveEventSink,
    ArchiveEventSource,
    ArchiveIndex,
    MessageSnapshot,
    MessageVersion,
    UnresolvedDeletionEvent,
)
from tg_backup.media_index import MediaDiscovery, MediaIndex, MediaRecord, MediaStatus
from tg_backup.utils.atomic import atomic_write_bytes, atomic_write_json, atomic_write_text
from tg_backup.utils.text_streaming import TextExportWriter, TextRecord

T = TypeVar("T")

type TGMedia = Audio | Document | Photo | Sticker | Animation | Video | Voice | VideoNote

DEFAULT_ENCODING = "utf-8"
DEFAULT_JSON_INDENT = 2
TELEGRAM_BACKOFF_TIME = 30
FORUM_TOPICS_PAGE_SIZE = 100
STATE_FILE_NAME = "state.json"
MEDIA_INDEX_FILE_NAME = "media-index.sqlite3"
ARCHIVE_INDEX_FILE_NAME = "archive-index.sqlite3"
UNKNOWN_WEEK_BUCKET = "unknown"
UNKNOWN_TEXT_TIMESTAMP = "unknown"
EVENT_SCHEMA_VERSION = 1

IMAGE_EXTS = dict.fromkeys(PHOTO_TYPES, ".jpg")
ANIMATED_EXTS = dict.fromkeys((FileType.VIDEO, FileType.ANIMATION, FileType.VIDEO_NOTE), ".mp4")
DEFAULT_EXTS = defaultdict(
    lambda: ".unknown",
    {
        **IMAGE_EXTS,
        **ANIMATED_EXTS,
        FileType.VOICE: ".ogg",
        FileType.STICKER: ".webp",
        FileType.AUDIO: ".mp3",
    },
)

log = logging.getLogger(__name__)

SIZES = sorted(
    [
        (10 ** (3 * n), prefix)
        for n, prefix in enumerate(
            ("q", "r", "y", "z", "a", "f", "p", "n", "µ", "m", "", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"),
            start=-10,
        )
    ]
)


def object_json_default(value: object) -> object:
    return Object.default(value)  # type: ignore[arg-type]


class HasId(Protocol):
    id: int


@dataclass
class ChatExportState:
    id: int
    chat_type: str
    username: str | None
    qualname: str
    history_complete: bool = False
    oldest_message_id: int | None = None
    latest_message_id: int | None = None
    failure_count: int = 0
    last_error: str | None = None
    last_error_at: str | None = None


@dataclass
class BackupState:
    chats: list[ChatExportState]


@dataclass
class BackupSession:
    state: BackupState
    state_file: Path
    json_output_dir: Path | None
    text_output_dir: Path | None
    export_json: bool
    export_text: bool
    download_attachments: bool
    media_index: MediaIndex | None = None
    archive_index: ArchiveIndex | None = None


@dataclass(frozen=True)
class ForumTopicEntry:
    id: int
    title: str


@dataclass(frozen=True)
class MediaFileInfo:
    raw_file_id: str
    file_name: str
    file_size: int | None
    media_id: str

    @cached_property
    def file_id(self) -> FileId:
        return FileId.decode(self.raw_file_id)

    @property
    def file_type(self) -> FileType:
        return self.file_id.file_type


class ChatBrief(NamedTuple):
    type: ChatType
    username: str | None
    qualname: str
    id: int


def human_readable(n: float, unit: str, *, precision: int = 2) -> str:
    idx = bisect.bisect(SIZES, (n, ""))
    idx = max(0, min(idx - 1, len(SIZES)))
    divisor, prefix = SIZES[idx]
    value = round(n / divisor, precision)
    return f"{value:.{precision}f} {prefix}{unit}"


def get_chat_brief(chat: Chat) -> ChatBrief:
    if chat.id is None or chat.type is None:
        raise ValueError("Telegram returned a chat without an id or type.")

    if chat.type is ChatType.BOT:
        qualname = chat.first_name or "unknown"
    elif chat.type is ChatType.PRIVATE:
        qualname = f"{chat.first_name} {chat.last_name}" if chat.last_name else (chat.first_name or "unknown")
    elif chat.type in {ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP}:
        qualname = chat.title or "unknown"
    else:
        qualname = "unknown"
    return ChatBrief(type=chat.type, username=chat.username, qualname=qualname, id=chat.id)


def get_chat_id(chat: Chat) -> int:
    if chat.id is None:
        raise ValueError("Telegram returned a chat without an id.")
    return chat.id


async def backup(
    client: Client,
    *,
    state_output_dir: Path,
    json_output_dir: Path | None = None,
    text_output_dir: Path | None = None,
    export_json: bool = True,
    export_text: bool = False,
    download_attachments: bool = True,
) -> BackupSession:
    validate_backup_config(
        json_output_dir=json_output_dir,
        text_output_dir=text_output_dir,
        export_json=export_json,
        export_text=export_text,
        download_attachments=download_attachments,
    )

    state_output_dir.mkdir(parents=True, exist_ok=True)
    if json_output_dir is not None:
        json_output_dir.mkdir(parents=True, exist_ok=True)
    if text_output_dir is not None:
        text_output_dir.mkdir(parents=True, exist_ok=True)

    media_index: MediaIndex | None = None
    archive_index: ArchiveIndex | None = None
    try:
        if download_attachments:
            media_index = MediaIndex(state_output_dir / MEDIA_INDEX_FILE_NAME)
        archive_index = ArchiveIndex(state_output_dir / ARCHIVE_INDEX_FILE_NAME)
        await client.start()

        state_file = state_output_dir / STATE_FILE_NAME
        state, chats_by_id = await get_or_refresh_backup_state(client=client, state_file=state_file)
        dump_export_chat_mappings(state, json_output_dir=json_output_dir, text_output_dir=text_output_dir)
        if json_output_dir is not None:
            for chat_state in state.chats:
                import_archive_message_manifests(
                    json_output_dir / "chats" / str(chat_state.id),
                    chat_id=chat_state.id,
                    is_channel=chat_state.chat_type in {ChatType.CHANNEL.name, ChatType.SUPERGROUP.name},
                    archive_index=archive_index,
                )
        session = BackupSession(
            state=state,
            state_file=state_file,
            json_output_dir=json_output_dir,
            text_output_dir=text_output_dir,
            export_json=export_json,
            export_text=export_text,
            download_attachments=download_attachments,
            media_index=media_index,
            archive_index=archive_index,
        )
        flush_archive_event_outbox(session)

        def persist_state() -> None:
            dump_backup_state(state_file, state)

        log.info("Start syncing chats.")
        await sync_chats(
            client,
            state=state,
            chats_by_id=chats_by_id,
            persist_state=persist_state,
            json_output_dir=json_output_dir,
            text_output_dir=text_output_dir,
            export_json=export_json,
            export_text=export_text,
            media_index=media_index,
            archive_index=archive_index,
        )
        log.info("Finished syncing chats.")

        if download_attachments and export_json and json_output_dir is not None and media_index is not None:
            log.info("Indexing and downloading chat media.")
            for chat_state in state.chats:
                chat_dir = json_output_dir / "chats" / str(chat_state.id)
                discover_media_manifests(client, chat_dir, media_index)
            await download_indexed_media(client, media_index, allowed_root=json_output_dir)
            log.info("Finished downloading chat media.")
        elif not download_attachments:
            log.info("Skipping media downloads because DOWNLOAD_ATTACHMENTS is disabled.")

        flush_archive_event_outbox(session)
    except BaseException:
        if archive_index is not None:
            with suppress(Exception):
                archive_index.close()
        if media_index is not None:
            with suppress(Exception):
                media_index.close()
        raise
    return session


async def sync_chats(  # noqa: PLR0913
    client: Client,
    *,
    state: BackupState,
    chats_by_id: dict[int, Chat],
    persist_state: Callable[[], None],
    json_output_dir: Path | None,
    text_output_dir: Path | None,
    export_json: bool,
    export_text: bool,
    media_index: MediaIndex | None,
    archive_index: ArchiveIndex | None = None,
) -> None:
    for chat_state in state.chats:
        chat = chats_by_id.get(chat_state.id)
        if chat is None:
            continue
        try:
            await sync_chat(
                client,
                chat=chat,
                chat_state=chat_state,
                persist_state=persist_state,
                json_output_dir=json_output_dir,
                text_output_dir=text_output_dir,
                export_json=export_json,
                export_text=export_text,
                media_index=media_index,
                archive_index=archive_index,
            )
        except Exception as error:
            record_chat_failure(chat_state, error)
            persist_state()
            log.exception(
                "Failed to sync chat %s (%s); continuing with the next chat.",
                chat_state.id,
                chat_state.qualname,
            )
        else:
            clear_chat_failure(chat_state)
            persist_state()


async def sync_chat(  # noqa: PLR0913
    client: Client,
    *,
    chat: Chat,
    chat_state: ChatExportState,
    persist_state: Callable[[], None],
    json_output_dir: Path | None,
    text_output_dir: Path | None,
    export_json: bool,
    export_text: bool,
    media_index: MediaIndex | None,
    archive_index: ArchiveIndex | None,
) -> None:
    chat_id = get_chat_id(chat)
    json_chat_dir = (json_output_dir / "chats" / str(chat_id)) if json_output_dir is not None else None
    text_chat_dir = (text_output_dir / "chats" / str(chat_id)) if text_output_dir is not None else None

    if export_json and json_chat_dir is not None:
        json_chat_dir.mkdir(parents=True, exist_ok=True)
        await dump_chat_json_metadata(client=client, chat=chat, json_chat_dir=json_chat_dir)
    if export_text and text_chat_dir is not None:
        text_chat_dir.mkdir(parents=True, exist_ok=True)

    await refresh_forum_topics(
        client,
        chat=chat,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
    )

    append_messages = append_recent_messages if chat_state.history_complete else append_chat_history
    await append_messages(
        client,
        chat,
        chat_state,
        persist_state=persist_state,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
        export_json=export_json,
        export_text=export_text,
        media_index=media_index,
        archive_index=archive_index,
    )


def record_chat_failure(chat_state: ChatExportState, error: Exception) -> None:
    chat_state.failure_count += 1
    chat_state.last_error = f"{type(error).__name__}: {error}"
    chat_state.last_error_at = dt.now(tz=UTC).isoformat()


def clear_chat_failure(chat_state: ChatExportState) -> None:
    chat_state.last_error = None
    chat_state.last_error_at = None


def validate_backup_config(
    *,
    json_output_dir: Path | None,
    text_output_dir: Path | None,
    export_json: bool,
    export_text: bool,
    download_attachments: bool,
) -> None:
    if not export_json and not export_text:
        raise ValueError("At least one export format must be enabled.")
    if export_json and json_output_dir is None:
        raise ValueError("JSON export requires json_output_dir.")
    if export_text and text_output_dir is None:
        raise ValueError("Text export requires text_output_dir.")
    if download_attachments and not export_json:
        raise ValueError("DOWNLOAD_ATTACHMENTS=True requires EXPORT_JSON=True.")


async def get_or_refresh_backup_state(client: Client, state_file: Path) -> tuple[BackupState, dict[int, Chat]]:
    chats = await get_chats_info(client=client)
    chats_by_id = {get_chat_id(chat): chat for chat in chats}
    existing_state = load_backup_state(state_file)
    existing_by_id = {chat.id: chat for chat in existing_state.chats}

    merged_states: list[ChatExportState] = []
    for chat in chats:
        brief = get_chat_brief(chat)
        current = existing_by_id.get(brief.id)
        if current is None:
            merged_states.append(
                ChatExportState(
                    id=brief.id,
                    chat_type=brief.type.name,
                    username=brief.username,
                    qualname=brief.qualname,
                )
            )
            continue

        merged_states.append(
            ChatExportState(
                id=brief.id,
                chat_type=brief.type.name,
                username=brief.username,
                qualname=brief.qualname,
                history_complete=current.history_complete,
                oldest_message_id=current.oldest_message_id,
                latest_message_id=current.latest_message_id,
                failure_count=current.failure_count,
                last_error=current.last_error,
                last_error_at=current.last_error_at,
            )
        )

    current_chat_ids = set(chats_by_id)
    merged_states.extend(chat_state for chat_state in existing_state.chats if chat_state.id not in current_chat_ids)

    state = BackupState(chats=merged_states)
    dump_backup_state(state_file, state)
    return state, chats_by_id


def load_backup_state(state_file: Path) -> BackupState:
    if not state_file.exists():
        return BackupState(chats=[])

    with state_file.open("r", encoding=DEFAULT_ENCODING) as fp:
        raw = json.load(fp)

    raw_chats = raw.get("chats")
    if not isinstance(raw_chats, list):
        return BackupState(chats=[])

    chats: list[ChatExportState] = []
    for item in raw_chats:
        if not isinstance(item, dict):
            continue
        chat_id = item.get("id")
        chat_type = item.get("chat_type")
        qualname = item.get("qualname")
        username = item.get("username")
        history_complete = item.get("history_complete", False)
        oldest_message_id = item.get("oldest_message_id")
        latest_message_id = item.get("latest_message_id")
        failure_count = item.get("failure_count", 0)
        last_error = item.get("last_error")
        last_error_at = item.get("last_error_at")

        if not isinstance(chat_id, int) or not isinstance(chat_type, str) or not isinstance(qualname, str):
            continue
        if username is not None and not isinstance(username, str):
            continue
        if not isinstance(history_complete, bool):
            continue
        if oldest_message_id is not None and not isinstance(oldest_message_id, int):
            continue
        if latest_message_id is not None and not isinstance(latest_message_id, int):
            continue
        if not isinstance(failure_count, int) or failure_count < 0:
            continue
        if last_error is not None and not isinstance(last_error, str):
            continue
        if last_error_at is not None and not isinstance(last_error_at, str):
            continue

        chats.append(
            ChatExportState(
                id=chat_id,
                chat_type=chat_type,
                username=username,
                qualname=qualname,
                history_complete=history_complete,
                oldest_message_id=oldest_message_id,
                latest_message_id=latest_message_id,
                failure_count=failure_count,
                last_error=last_error,
                last_error_at=last_error_at,
            )
        )

    return BackupState(chats=chats)


def dump_backup_state(state_file: Path, state: BackupState) -> None:
    payload = {"chats": [asdict(chat) for chat in state.chats]}
    atomic_write_json(
        state_file,
        payload,
        encoding=DEFAULT_ENCODING,
        indent=DEFAULT_JSON_INDENT,
        ensure_ascii=False,
    )


def dump_export_chat_mappings(
    state: BackupState, *, json_output_dir: Path | None, text_output_dir: Path | None
) -> None:
    chat_mapping = {str(chat.id): chat.qualname for chat in state.chats}

    if json_output_dir is not None:
        chats_json = json_output_dir / "chats.json"
        atomic_write_json(
            chats_json,
            chat_mapping,
            encoding=DEFAULT_ENCODING,
            indent=DEFAULT_JSON_INDENT,
            ensure_ascii=False,
        )

    if text_output_dir is not None:
        chats_txt = text_output_dir / "chats.txt"
        lines = [f"{chat.id}\t{chat.qualname}" for chat in state.chats]
        content = "\n".join(lines)
        atomic_write_text(chats_txt, f"{content}\n" if lines else "", encoding=DEFAULT_ENCODING)


def get_topics_json_path(json_chat_dir: Path) -> Path:
    return json_chat_dir / "topics.json"


def get_topics_txt_path(text_chat_dir: Path) -> Path:
    return text_chat_dir / "topics.txt"


def load_existing_forum_topics(*, json_chat_dir: Path | None, text_chat_dir: Path | None) -> list[ForumTopicEntry]:
    if json_chat_dir is not None:
        topics_json = get_topics_json_path(json_chat_dir)
        if topics_json.exists():
            with topics_json.open("r", encoding=DEFAULT_ENCODING) as fp:
                raw_topics = json.load(fp)
            if isinstance(raw_topics, dict):
                topics: list[ForumTopicEntry] = []
                for topic_id, topic_title in raw_topics.items():
                    if not isinstance(topic_id, str) or not isinstance(topic_title, str):
                        continue
                    try:
                        parsed_topic_id = int(topic_id)
                    except ValueError:
                        continue
                    topics.append(ForumTopicEntry(id=parsed_topic_id, title=topic_title))
                return sorted(topics, key=lambda topic: topic.id)

    if text_chat_dir is not None:
        topics_txt = get_topics_txt_path(text_chat_dir)
        if topics_txt.exists():
            topics = []
            for line in topics_txt.read_text(encoding=DEFAULT_ENCODING).splitlines():
                if not line.strip():
                    continue
                topic_id, separator, topic_title = line.partition("\t")
                if not separator:
                    continue
                try:
                    parsed_topic_id = int(topic_id)
                except ValueError:
                    continue
                topics.append(ForumTopicEntry(id=parsed_topic_id, title=topic_title))
            return sorted(topics, key=lambda topic: topic.id)

    return []


def dump_forum_topics(
    topics: list[ForumTopicEntry],
    *,
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
) -> None:
    sorted_topics = sorted(topics, key=lambda topic: topic.id)

    if json_chat_dir is not None:
        topics_json = get_topics_json_path(json_chat_dir)
        topics_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {str(topic.id): topic.title for topic in sorted_topics}
        atomic_write_json(
            topics_json,
            payload,
            encoding=DEFAULT_ENCODING,
            indent=DEFAULT_JSON_INDENT,
            ensure_ascii=False,
        )

    if text_chat_dir is not None:
        topics_txt = get_topics_txt_path(text_chat_dir)
        topics_txt.parent.mkdir(parents=True, exist_ok=True)
        lines = [f"{topic.id}\t{topic.title}" for topic in sorted_topics]
        content = "\n".join(lines)
        atomic_write_text(topics_txt, f"{content}\n" if lines else "", encoding=DEFAULT_ENCODING)


def forum_topic_exists(topic_id: int, *, json_chat_dir: Path | None, text_chat_dir: Path | None) -> bool:
    return any(
        topic.id == topic_id
        for topic in load_existing_forum_topics(
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
        )
    )


def forum_topic_entry(topic: raw.base.ForumTopic) -> ForumTopicEntry | None:
    if isinstance(topic, raw.types.ForumTopic):
        return ForumTopicEntry(id=topic.id, title=topic.title)
    return None


def forum_topic_entries(topics: Iterable[raw.base.ForumTopic]) -> list[ForumTopicEntry]:
    entries: list[ForumTopicEntry] = []
    for topic in topics:
        entry = forum_topic_entry(topic)
        if entry is not None:
            entries.append(entry)
    return entries


async def refresh_forum_topics(
    client: Client,
    *,
    chat: Chat,
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
    topic_ids: Sequence[int] | None = None,
) -> None:
    if json_chat_dir is None and text_chat_dir is None:
        return
    if chat.type is not ChatType.SUPERGROUP:
        return
    chat_id = get_chat_id(chat)
    if not await is_forum_chat(client, chat_id):
        return

    try:
        topics = await get_forum_topics(client, chat_id, topic_ids=topic_ids)
    except (RPCError, TypeError):
        return

    if topic_ids is None:
        dump_forum_topics(topics, json_chat_dir=json_chat_dir, text_chat_dir=text_chat_dir)
        return

    merged_topics = {
        topic.id: topic
        for topic in load_existing_forum_topics(
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
        )
    }
    for topic in topics:
        merged_topics[topic.id] = topic
    dump_forum_topics(list(merged_topics.values()), json_chat_dir=json_chat_dir, text_chat_dir=text_chat_dir)


async def get_forum_topics(
    client: Client,
    chat_id: int,
    *,
    topic_ids: Sequence[int] | None = None,
) -> list[ForumTopicEntry]:
    peer = await client.resolve_peer(chat_id)
    if peer is None:
        return []

    if topic_ids is not None:
        response = await client.invoke(
            raw.functions.messages.GetForumTopicsByID(
                peer=peer,
                topics=list(topic_ids),
            )
        )
        return forum_topic_entries(response.topics)

    topics: list[ForumTopicEntry] = []
    offset_date = 0
    offset_id = 0
    offset_topic = 0

    while True:
        response = await client.invoke(
            raw.functions.messages.GetForumTopics(
                peer=peer,
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=FORUM_TOPICS_PAGE_SIZE,
            )
        )
        if not response.topics:
            break

        topics.extend(forum_topic_entries(response.topics))
        last_topic = next(
            (topic for topic in reversed(response.topics) if isinstance(topic, raw.types.ForumTopic)),
            None,
        )
        if last_topic is None:
            break
        offset_date = last_topic.date
        offset_id = last_topic.top_message
        offset_topic = last_topic.id

        if len(response.topics) < FORUM_TOPICS_PAGE_SIZE:
            break

    return topics


async def is_forum_chat(client: Client, chat_id: int) -> bool:
    channel = get_input_channel(await client.resolve_peer(chat_id))
    if channel is None:
        return False
    if isinstance(channel, raw.types.InputChannelEmpty):
        return False

    try:
        full_chat = await client.invoke(raw.functions.channels.GetFullChannel(channel=channel))
    except RPCError:
        return False

    raw_channel = next(
        (
            candidate
            for candidate in full_chat.chats
            if isinstance(candidate, raw.types.Channel) and candidate.id == channel.channel_id
        ),
        None,
    )
    return bool(raw_channel is not None and raw_channel.forum)


def get_input_channel(peer: object) -> raw.base.InputChannel | None:
    if isinstance(peer, raw.types.InputChannel | raw.types.InputChannelEmpty | raw.types.InputChannelFromMessage):
        return peer
    if isinstance(peer, raw.types.InputPeerChannel):
        return raw.types.InputChannel(
            channel_id=peer.channel_id,
            access_hash=peer.access_hash,
        )
    if isinstance(peer, raw.types.InputPeerChannelFromMessage):
        return raw.types.InputChannelFromMessage(
            peer=peer.peer,
            msg_id=peer.msg_id,
            channel_id=peer.channel_id,
        )
    return None


async def append_chat_history(  # noqa: PLR0913
    client: Client,
    chat: Chat,
    chat_state: ChatExportState,
    *,
    persist_state: Callable[[], None],
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
    export_json: bool,
    export_text: bool,
    media_index: MediaIndex | None = None,
    archive_index: ArchiveIndex | None = None,
) -> None:
    if chat_state.oldest_message_id is not None and chat_state.oldest_message_id <= 1:
        chat_state.history_complete = True
        persist_state()
        return

    max_id = chat_state.oldest_message_id - 1 if chat_state.oldest_message_id is not None else 0
    async for messages_batch in get_chat_messages(client=client, chat_id=get_chat_id(chat), max_id=max_id):
        if not messages_batch:
            continue
        index_original_messages(messages_batch, archive_index=archive_index)
        index_message_media(client, messages_batch, media_index=media_index, json_chat_dir=json_chat_dir)
        append_export_batch(
            messages=messages_batch,
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
            export_json=export_json,
            export_text=export_text,
        )
        chat_state.oldest_message_id = min(message.id for message in messages_batch)
        batch_latest_id = max(message.id for message in messages_batch)
        if chat_state.latest_message_id is None:
            chat_state.latest_message_id = batch_latest_id
        persist_state()

    chat_state.history_complete = True
    persist_state()


async def append_recent_messages(  # noqa: PLR0913
    client: Client,
    chat: Chat,
    chat_state: ChatExportState,
    *,
    persist_state: Callable[[], None],
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
    export_json: bool,
    export_text: bool,
    media_index: MediaIndex | None = None,
    archive_index: ArchiveIndex | None = None,
) -> None:
    if chat_state.latest_message_id is None:
        return

    latest_known_id = chat_state.latest_message_id
    pending_messages: list[Message] = []
    async for messages_batch in get_chat_messages(client=client, chat_id=get_chat_id(chat)):
        if not messages_batch:
            continue

        new_messages = [message for message in messages_batch if message.id > latest_known_id]
        for known_message in messages_batch:
            if known_message.id <= latest_known_id and known_message.edit_date is not None:
                append_edited_message_event(
                    known_message,
                    json_chat_dir=json_chat_dir,
                    text_chat_dir=text_chat_dir,
                    source=ArchiveEventSource.BATCH_AUDIT,
                    archive_index=archive_index,
                )
        if new_messages:
            pending_messages.extend(new_messages)

        if len(new_messages) != len(messages_batch):
            break

    if not pending_messages:
        return

    pending_messages.sort(key=lambda message: message.id)
    index_original_messages(pending_messages, archive_index=archive_index)
    index_message_media(client, pending_messages, media_index=media_index, json_chat_dir=json_chat_dir)
    append_export_batch(
        messages=pending_messages,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
        export_json=export_json,
        export_text=export_text,
    )
    chat_state.latest_message_id = max(message.id for message in pending_messages)
    persist_state()


async def append_live_message(client: Client, message: Message, *, session: BackupSession) -> None:
    chat = message.chat
    if chat is None:
        log.warning("Skipping live message %s because Telegram did not include its chat.", message.id)
        return

    chat_state = ensure_chat_state(session.state, chat)
    if chat_state.latest_message_id is not None and message.id <= chat_state.latest_message_id:
        return

    json_chat_dir = None
    if session.export_json and session.json_output_dir is not None:
        json_chat_dir = session.json_output_dir / "chats" / str(chat.id)

    text_chat_dir = None
    if session.export_text and session.text_output_dir is not None:
        text_chat_dir = session.text_output_dir / "chats" / str(chat.id)

    if session.export_json and json_chat_dir is not None:
        json_chat_dir.mkdir(parents=True, exist_ok=True)
        info_json = json_chat_dir / "info.json"
        if not info_json.exists():
            try:
                await dump_chat_json_metadata(client=client, chat=chat, json_chat_dir=json_chat_dir)
            except Exception:
                log.exception("Failed to refresh metadata for live chat %s; preserving the message anyway.", chat.id)
    if session.export_text and text_chat_dir is not None:
        text_chat_dir.mkdir(parents=True, exist_ok=True)

    thread_id = get_thread_id(message)
    if thread_id is not None and not forum_topic_exists(
        thread_id,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
    ):
        try:
            await refresh_forum_topics(
                client,
                chat=chat,
                json_chat_dir=json_chat_dir,
                text_chat_dir=text_chat_dir,
                topic_ids=[thread_id],
            )
        except Exception:
            log.exception("Failed to refresh forum topic %s; preserving the live message anyway.", thread_id)

    index_original_messages([message], archive_index=session.archive_index)
    append_export_batch(
        messages=[message],
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
        export_json=session.export_json,
        export_text=session.export_text,
    )

    index_message_media(client, [message], media_index=session.media_index, json_chat_dir=json_chat_dir)

    if chat_state.oldest_message_id is None or message.id < chat_state.oldest_message_id:
        chat_state.oldest_message_id = message.id
    if chat_state.latest_message_id is None or message.id > chat_state.latest_message_id:
        chat_state.latest_message_id = message.id

    dump_backup_state(session.state_file, session.state)
    dump_export_chat_mappings(
        session.state,
        json_output_dir=session.json_output_dir,
        text_output_dir=session.text_output_dir,
    )


def ensure_chat_state(state: BackupState, chat: Chat) -> ChatExportState:
    for chat_state in state.chats:
        if chat_state.id == chat.id:
            return chat_state

    brief = get_chat_brief(chat)
    chat_state = ChatExportState(
        id=brief.id,
        chat_type=brief.type.name,
        username=brief.username,
        qualname=brief.qualname,
    )
    state.chats.append(chat_state)
    return chat_state


def append_export_batch(
    *,
    messages: list[Message],
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
    export_json: bool,
    export_text: bool,
) -> None:
    if export_json and json_chat_dir is not None:
        append_weekly_json_exports(json_chat_dir, messages=messages)

    if export_text and text_chat_dir is not None:
        with TextExportWriter() as text_writer:
            text_writer.write_records(build_text_records(messages, text_chat_dir))


def index_original_messages(messages: Iterable[Message], *, archive_index: ArchiveIndex | None) -> None:
    if archive_index is None:
        return
    snapshots: list[MessageSnapshot] = []
    for message in messages:
        try:
            snapshots.append(MessageSnapshot.from_message(message))
        except (TypeError, ValueError) as error:
            log.warning("Cannot index message %s: %s", message.id, error)
    index_archive_snapshots(snapshots, archive_index=archive_index)


def index_archive_snapshots(snapshots: list[MessageSnapshot], *, archive_index: ArchiveIndex) -> None:
    if not snapshots:
        return
    try:
        archive_index.index_originals(snapshots)
    except ValueError:
        # A malformed or conflicting legacy record must not block valid records
        # from the same export file.
        for snapshot in snapshots:
            try:
                archive_index.index_original(snapshot)
            except (TypeError, ValueError) as error:
                log.warning(
                    "Cannot import archive message %s/%s: %s",
                    snapshot.chat_id,
                    snapshot.message_id,
                    error,
                )


def import_archive_message_manifests(
    json_chat_dir: Path,
    *,
    chat_id: int,
    is_channel: bool,
    archive_index: ArchiveIndex | None,
) -> None:
    if archive_index is None or not json_chat_dir.exists():
        return
    try:
        manifest_paths = sorted(json_chat_dir.rglob("*.messages.json"))
    except OSError as error:
        log.warning("Cannot scan archived message manifests in %s: %s", json_chat_dir, error)
        return
    for manifest_path in manifest_paths:
        try:
            stat = manifest_path.stat()
            source_path = manifest_path.absolute()
            if not archive_index.source_needs_scan(source_path, size=stat.st_size, modified_ns=stat.st_mtime_ns):
                continue
            raw_items, _, repaired = read_json_list_for_append(manifest_path)
        except OSError as error:
            log.warning("Cannot read archived message manifest %s: %s", manifest_path, error)
            continue

        snapshots: list[MessageSnapshot] = []
        had_errors = repaired
        for item in raw_items:
            try:
                if not isinstance(item, dict):
                    raise TypeError("message entry must be a JSON object")
                nested_chat = item.get("chat")
                nested_chat_id = nested_chat.get("id") if isinstance(nested_chat, dict) else None
                if nested_chat_id is not None and nested_chat_id != chat_id:
                    raise ValueError(f"message chat id {nested_chat_id!r} does not match directory chat id {chat_id}")
                snapshots.append(MessageSnapshot.from_export_payload(item, chat_id=chat_id, is_channel=is_channel))
            except (TypeError, ValueError) as error:
                had_errors = True
                log.warning("Cannot import message entry from %s: %s", manifest_path, error)

        index_archive_snapshots(snapshots, archive_index=archive_index)
        if not had_errors:
            archive_index.mark_source_scanned(source_path, size=stat.st_size, modified_ns=stat.st_mtime_ns)


def append_edited_message(client: Client, message: Message, *, session: BackupSession) -> None:
    chat = message.chat
    if chat is None:
        log.warning("Cannot route edited message %s because Telegram omitted its chat.", message.id)
        return
    chat_id = get_chat_id(chat)
    json_chat_dir = (
        session.json_output_dir / "chats" / str(chat_id)
        if session.export_json and session.json_output_dir is not None
        else None
    )
    text_chat_dir = (
        session.text_output_dir / "chats" / str(chat_id)
        if session.export_text and session.text_output_dir is not None
        else None
    )
    append_edited_message_event(
        message,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
        source=ArchiveEventSource.LIVE_UPDATE,
        archive_index=session.archive_index,
    )
    index_message_media(client, [message], media_index=session.media_index, json_chat_dir=json_chat_dir)


def append_edited_message_event(
    message: Message,
    *,
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
    source: ArchiveEventSource | str,
    archive_index: ArchiveIndex | None,
) -> None:
    if message.chat is None:
        return
    if archive_index is None:
        log.warning("Cannot retain edit %s without an archive index.", message.id)
        return
    event = archive_index.reserve_edit(
        MessageSnapshot.from_message(message),
        source=ArchiveEventSource(source),
    )
    if event is not None:
        deliver_archive_event(
            event,
            archive_index=archive_index,
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
        )


def append_deleted_update(update: object, *, session: BackupSession) -> None:
    if isinstance(update, raw.types.UpdateDeleteChannelMessages):
        chat_id: int | None = pyrogram_utils.get_channel_id(update.channel_id)
        message_ids = update.messages
        update_type = type(update).__name__
        pts = update.pts
        pts_count = update.pts_count
    elif isinstance(update, raw.types.UpdateDeleteMessages):
        chat_id = None
        message_ids = update.messages
        update_type = type(update).__name__
        pts = update.pts
        pts_count = update.pts_count
    else:
        return

    for message_id in message_ids:
        resolved_chat_id = chat_id
        if session.archive_index is not None:
            head = (
                session.archive_index.get_head(chat_id, message_id)
                if chat_id is not None
                else session.archive_index.resolve_non_channel_deletion(message_id)
            )
            if head is not None:
                resolved_chat_id = head.current.snapshot.chat_id
                event = session.archive_index.reserve_delete(
                    resolved_chat_id,
                    message_id,
                    source=ArchiveEventSource.LIVE_UPDATE,
                    telegram_update_type=update_type,
                    telegram_pts=pts,
                    telegram_pts_count=pts_count,
                )
                deliver_archive_event(
                    event,
                    archive_index=session.archive_index,
                    json_chat_dir=get_session_chat_dir(session, resolved_chat_id, sink=ArchiveEventSink.JSON),
                    text_chat_dir=get_session_chat_dir(session, resolved_chat_id, sink=ArchiveEventSink.TEXT),
                )
                continue

            unresolved_event = session.archive_index.reserve_unresolved_delete(
                message_id,
                chat_id=resolved_chat_id,
                source=ArchiveEventSource.LIVE_UPDATE,
                telegram_update_type=update_type,
                telegram_pts=pts,
                telegram_pts_count=pts_count,
            )
            deliver_unresolved_deletion_event(unresolved_event, session=session)
            continue

        observed_at = dt.now(tz=UTC).isoformat()
        routing_key = str(resolved_chat_id) if resolved_chat_id is not None else f"unresolved:{pts}:{pts_count}"
        event_id = stable_event_id("deleted", routing_key, str(message_id))
        payload: dict[str, object] = {
            "schema_version": EVENT_SCHEMA_VERSION,
            "event_id": event_id,
            "event_type": "message_deleted",
            "source": "live_update",
            "observed_at": observed_at,
            "chat_id": resolved_chat_id,
            "message_id": message_id,
            "telegram_update": {
                "type": update_type,
                "pts": pts,
                "pts_count": pts_count,
            },
            "retention": "Original message and media remain unchanged in the backup.",
        }
        text = (
            f"[event-id:{event_id}]\n"
            f"[{observed_at}] MESSAGE DELETED: chat={resolved_chat_id or 'unresolved'} message={message_id}\n"
            "Original message text and media were retained unchanged.\n\n"
        )
        json_path, text_path = get_deletion_event_paths(session, resolved_chat_id)
        if json_path is not None:
            append_unique_json_event(json_path, payload)
        if text_path is not None:
            append_unique_text_event(text_path, event_id, text)


def deliver_archive_event(
    event: ArchiveEvent,
    *,
    archive_index: ArchiveIndex,
    json_chat_dir: Path | None,
    text_chat_dir: Path | None,
) -> None:
    payload = archive_event_payload(event)
    rendered = render_archive_event(event)
    if json_chat_dir is not None:
        json_path = get_snapshot_event_export_path(json_chat_dir, event, suffix="events.json")
        append_unique_json_event(json_path, payload)
        archive_index.mark_event_delivered(event.event_id, ArchiveEventSink.JSON)
    if text_chat_dir is not None:
        text_path = get_snapshot_event_export_path(text_chat_dir, event, suffix="events.txt")
        append_unique_text_event(text_path, event.event_id, rendered)
        archive_index.mark_event_delivered(event.event_id, ArchiveEventSink.TEXT)


def deliver_unresolved_deletion_event(event: UnresolvedDeletionEvent, *, session: BackupSession) -> None:
    archive_index = session.archive_index
    if archive_index is None:
        raise ValueError("Cannot deliver an unresolved deletion without an archive index.")
    for sink in (ArchiveEventSink.JSON, ArchiveEventSink.TEXT):
        path = get_unresolved_event_path(session, event, sink=sink)
        if path is None:
            continue
        if sink is ArchiveEventSink.JSON:
            append_unique_json_event(path, unresolved_deletion_payload(event))
        else:
            append_unique_text_event(path, event.event_id, render_unresolved_deletion(event))
        archive_index.mark_unresolved_event_delivered(event.event_id, sink)


def flush_archive_event_outbox(session: BackupSession) -> None:  # noqa: PLR0912
    archive_index = session.archive_index
    if archive_index is None:
        return
    enabled_sinks = []
    if session.export_json and session.json_output_dir is not None:
        enabled_sinks.append(ArchiveEventSink.JSON)
    if session.export_text and session.text_output_dir is not None:
        enabled_sinks.append(ArchiveEventSink.TEXT)

    for sink in enabled_sinks:
        for event in list(archive_index.iter_pending_events(sink)):
            try:
                chat_dir = get_session_chat_dir(session, event.chat_id, sink=sink)
                if chat_dir is None:  # pragma: no cover - guarded by enabled_sinks.
                    continue
                if sink is ArchiveEventSink.JSON:
                    append_unique_json_event(
                        get_snapshot_event_export_path(chat_dir, event, suffix="events.json"),
                        archive_event_payload(event),
                    )
                else:
                    append_unique_text_event(
                        get_snapshot_event_export_path(chat_dir, event, suffix="events.txt"),
                        event.event_id,
                        render_archive_event(event),
                    )
            except Exception:
                log.exception("Failed to deliver archive event %s to %s; it remains pending.", event.event_id, sink)
            else:
                archive_index.mark_event_delivered(event.event_id, sink)
        for event in list(archive_index.iter_pending_unresolved_events(sink)):
            try:
                path = get_unresolved_event_path(session, event, sink=sink)
                if path is None:  # pragma: no cover - guarded by enabled_sinks.
                    continue
                if sink is ArchiveEventSink.JSON:
                    append_unique_json_event(path, unresolved_deletion_payload(event))
                else:
                    append_unique_text_event(path, event.event_id, render_unresolved_deletion(event))
            except Exception:
                log.exception(
                    "Failed to deliver unresolved deletion %s to %s; it remains pending.",
                    event.event_id,
                    sink,
                )
            else:
                archive_index.mark_unresolved_event_delivered(event.event_id, sink)


def archive_event_payload(event: ArchiveEvent) -> dict[str, object]:
    previous = archive_version_payload(event.previous)
    current = archive_version_payload(event.current)
    snapshot = (
        event.current.snapshot if event.current is not None else event.previous.snapshot if event.previous else None
    )
    payload: dict[str, object] = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": event.event_id,
        "event_type": f"message_{event.kind.value}",
        "source": event.source.value,
        "observed_at": event.observed_at,
        "chat_id": event.chat_id,
        "message_id": event.message_id,
        "message_date": snapshot.sent_at if snapshot is not None else None,
        "edit_date": snapshot.edit_date if snapshot is not None else None,
        "thread_id": snapshot.thread_id if snapshot is not None else None,
        "previous": previous,
        "current": current,
        "retention": "All observed message versions and media remain retained; no archived data was deleted.",
    }
    if event.telegram_update_type is not None:
        payload["telegram_update"] = {
            "type": event.telegram_update_type,
            "pts": event.telegram_pts,
            "pts_count": event.telegram_pts_count,
        }
    return payload


def archive_version_payload(version: MessageVersion | None) -> dict[str, object] | None:
    if version is None:
        return None
    payload: dict[str, object] = asdict(version.snapshot)
    payload["content_hash"] = version.content_hash
    payload["first_observed_at"] = version.first_observed_at
    return payload


def render_archive_event(event: ArchiveEvent) -> str:
    previous_text = archive_version_text(event.previous)
    current_text = archive_version_text(event.current)
    if event.kind is ArchiveEventKind.EDITED:
        detail = f"BEFORE: {previous_text}\nAFTER: {current_text}"
        label = "MESSAGE EDITED"
    else:
        detail = f"LAST RETAINED CONTENT: {previous_text}\nArchived text and media were not removed."
        label = "MESSAGE DELETED"
    return (
        f"[event-id:{event.event_id}]\n"
        f"[{event.observed_at.isoformat()}] {label}: chat={event.chat_id} message={event.message_id}\n"
        f"{detail}\n\n"
    )


def archive_version_text(version: MessageVersion | None) -> str:
    if version is None:
        return "(none)"
    return version.snapshot.text_payload or "(no textual content)"


def unresolved_deletion_payload(event: UnresolvedDeletionEvent) -> dict[str, object]:
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": event.event_id,
        "event_type": "message_deleted",
        "source": event.source.value,
        "observed_at": event.observed_at,
        "chat_id": event.chat_id,
        "message_id": event.message_id,
        "previous": None,
        "current": None,
        "telegram_update": {
            "type": event.telegram_update_type,
            "pts": event.telegram_pts,
            "pts_count": event.telegram_pts_count,
        },
        "retention": "Deletion was retained without guessing a message mapping; no archived data was removed.",
    }


def render_unresolved_deletion(event: UnresolvedDeletionEvent) -> str:
    chat_label = event.chat_id if event.chat_id is not None else "unresolved"
    return (
        f"[event-id:{event.event_id}]\n"
        f"[{event.observed_at.isoformat()}] MESSAGE DELETED: chat={chat_label} message={event.message_id}\n"
        "The message was not present in the archive index, so no chat/content mapping was guessed.\n\n"
    )


def stable_event_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode(DEFAULT_ENCODING)).hexdigest()


def append_unique_json_event(path: Path, payload: dict[str, object]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_items: list[object] = []
    existing_prefix = b"["
    repaired = False
    if path.exists() and path.stat().st_size > 0:
        existing_items, existing_prefix, repaired = read_json_list_for_append(path)
    event_id = payload["event_id"]
    if any(isinstance(item, dict) and item.get("event_id") == event_id for item in existing_items):
        return False
    encoded = json.dumps(
        payload,
        indent=DEFAULT_JSON_INDENT,
        default=object_json_default,
        ensure_ascii=False,
    ).encode(DEFAULT_ENCODING)
    write_json_list_atomic(
        path,
        existing_prefix=existing_prefix,
        encoded_items=[encoded],
        need_separator=bool(existing_items),
        backup_existing=repaired,
    )
    return True


def append_unique_text_event(path: Path, event_id: str, rendered_event: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding=DEFAULT_ENCODING) if path.exists() else ""
    if f"[event-id:{event_id}]" in existing:
        return False
    atomic_write_text(path, f"{existing}{rendered_event}", encoding=DEFAULT_ENCODING)
    return True


def append_weekly_json_exports(json_chat_dir: Path, *, messages: list[Message]) -> None:
    grouped_messages: dict[Path, list[Message]] = defaultdict(list)
    grouped_medias: dict[Path, list[TGMedia]] = defaultdict(list)

    for message in messages:
        grouped_messages[get_json_export_path(json_chat_dir, message, kind="messages")].append(message)
        media = get_media(message)
        if media is not None:
            grouped_medias[get_json_export_path(json_chat_dir, message, kind="medias")].append(media)

    for path, bucket_messages in grouped_messages.items():
        append_json_objects(path, bucket_messages, default=Object.default)  # type: ignore[arg-type]
    for path, bucket_medias in grouped_medias.items():
        append_json_objects(path, bucket_medias, default=Object.default)  # type: ignore[arg-type]


def append_json_objects(path: Path, items: Iterable[object], *, default: Callable[[object], object]) -> None:
    dumped_items = [json.dumps(item, indent=DEFAULT_JSON_INDENT, default=default, ensure_ascii=False) for item in items]
    if not dumped_items:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    existing_items: list[object] = []
    existing_prefix = b"["
    repaired = False

    if path.exists() and path.stat().st_size > 0:
        existing_items, existing_prefix, repaired = read_json_list_for_append(path)

    dumped_values = [json.loads(item) for item in dumped_items]
    overlap = json_list_tail_overlap(existing_items, dumped_values)
    if overlap:
        log.info("Skipping %s already exported item(s) in %s.", overlap, path)
        dumped_items = dumped_items[overlap:]

    if not dumped_items and not repaired:
        return

    encoded_items = [item.encode(DEFAULT_ENCODING) for item in dumped_items]
    write_json_list_atomic(
        path,
        existing_prefix=existing_prefix,
        encoded_items=encoded_items,
        need_separator=bool(existing_items),
        backup_existing=repaired,
    )


def read_json_list_for_append(path: Path) -> tuple[list[object], bytes, bool]:
    content = path.read_bytes()
    try:
        parsed = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        parsed = None

    if isinstance(parsed, list):
        content_without_trailing_space = content.rstrip()
        return parsed, content_without_trailing_space[:-1], False

    recovered_items = recover_json_list_prefix(content)
    log.warning(
        "Auto-healed corrupted JSON export %s: recovered %s complete item(s).",
        path,
        len(recovered_items),
    )
    return recovered_items, encode_json_list_prefix(recovered_items), True


def recover_json_list_prefix(content: bytes) -> list[object]:
    try:
        text = content.decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as error:
        text = content[: error.start].decode(DEFAULT_ENCODING)

    position = skip_json_whitespace(text, 0)
    if position >= len(text) or text[position] != "[":
        return []

    decoder = json.JSONDecoder()
    position += 1
    recovered: list[object] = []
    while True:
        position = skip_json_whitespace(text, position)
        if position >= len(text) or text[position] == "]":
            return recovered

        try:
            item, position = decoder.raw_decode(text, position)
        except json.JSONDecodeError:
            return recovered
        recovered.append(item)

        position = skip_json_whitespace(text, position)
        if position >= len(text):
            return recovered
        if text[position] == "]":
            return recovered
        if text[position] != ",":
            return recovered
        position += 1


def skip_json_whitespace(text: str, position: int) -> int:
    while position < len(text) and text[position] in " \t\r\n":
        position += 1
    return position


def encode_json_list_prefix(items: list[object]) -> bytes:
    if not items:
        return b"["
    dumped_items = [
        json.dumps(item, indent=DEFAULT_JSON_INDENT, ensure_ascii=False).encode(DEFAULT_ENCODING) for item in items
    ]
    return b"[" + b",\n".join(dumped_items)


def json_list_tail_overlap(existing_items: list[object], new_items: list[object]) -> int:
    max_overlap = min(len(existing_items), len(new_items))
    for overlap in range(max_overlap, 0, -1):
        if existing_items[-overlap:] == new_items[:overlap]:
            return overlap
    return 0


def write_json_list_atomic(
    path: Path,
    *,
    existing_prefix: bytes,
    encoded_items: list[bytes],
    need_separator: bool,
    backup_existing: bool,
) -> None:
    pieces = [existing_prefix]
    if encoded_items:
        if need_separator:
            pieces.append(b",\n")
        pieces.append(b",\n".join(encoded_items))
    pieces.append(b"]")

    if backup_existing and path.exists():
        backup_path = get_corrupt_export_backup_path(path)
        atomic_write_bytes(backup_path, path.read_bytes())
        log.warning("Preserved corrupted JSON export as %s.", backup_path)
    atomic_write_bytes(path, b"".join(pieces))


def get_corrupt_export_backup_path(path: Path) -> Path:
    backup_path = path.with_name(f"{path.name}.corrupt.bak")
    counter = 1
    while backup_path.exists():
        backup_path = path.with_name(f"{path.name}.corrupt.{counter}.bak")
        counter += 1
    return backup_path


async def dump_chat_json_metadata(client: Client, chat: Chat, *, json_chat_dir: Path) -> None:
    chat_id = get_chat_id(chat)
    chat_info = await client.get_chat(chat_id=chat_id)
    chat_info_json = json_chat_dir / "info.json"
    log.info("Dump chat info (%s).", chat_info_json)
    atomic_write_json(
        chat_info_json,
        chat_info,
        encoding=DEFAULT_ENCODING,
        indent=DEFAULT_JSON_INDENT,
        default=object_json_default,
        ensure_ascii=False,
    )

    log.info("Get chat avatars info.")
    avatars = await get_chat_avatars(client, chat_id)
    avatars_json = json_chat_dir / "avatars.json"
    if avatars is None and avatars_json.exists():
        log.info("Keeping existing chat avatars info because Telegram did not return a replacement (%s).", avatars_json)
    else:
        log.info("Dump chat avatars info (%s).", avatars_json)
        atomic_write_json(
            avatars_json,
            avatars,
            encoding=DEFAULT_ENCODING,
            indent=DEFAULT_JSON_INDENT,
            default=object_json_default,
            ensure_ascii=False,
        )


def build_text_records(messages: list[Message], text_chat_dir: Path) -> list[TextRecord]:
    grouped: dict[Path, list[str]] = defaultdict(list)
    for message in messages:
        record = message_to_text_record(text_chat_dir, message)
        if record is None:
            continue
        grouped[record.path].append(record.text)
    return [TextRecord(path=path, text="".join(texts)) for path, texts in grouped.items()]


def message_to_text_record(text_chat_dir: Path, message: Message) -> TextRecord | None:
    body = get_message_text_payload(message)
    if body is None:
        body = get_message_event_text(message)
    if body is None:
        return None
    return TextRecord(path=get_text_export_path(text_chat_dir, message), text=render_text_record(message, body))


def get_message_text_payload(message: Message) -> str | None:
    if message.text:
        return message.text
    if message.caption:
        return message.caption
    return None


def get_message_event_text(message: Message) -> str | None:
    event_text: str | None = None
    if message.new_chat_members:
        names = ", ".join(get_display_name(user) for user in message.new_chat_members)
        event_text = f"added to chat: {names}"
    elif message.left_chat_member is not None:
        event_text = f"left chat: {get_display_name(message.left_chat_member)}"
    elif message.new_chat_title:
        event_text = f"changed chat title to: {message.new_chat_title}"
    elif message.delete_chat_photo:
        event_text = "removed the chat photo"
    elif message.new_chat_photo:
        event_text = "changed the chat photo"
    elif message.group_chat_created or message.supergroup_chat_created or message.channel_chat_created:
        event_text = "created the chat"
    elif message.pinned_message is not None:
        event_text = "pinned a message"
    return event_text


def get_display_name(user: User) -> str:
    full_name = " ".join(part for part in (user.first_name, user.last_name) if part)
    if full_name:
        return full_name
    if user.username:
        return user.username
    return str(user.id)


def get_message_author_label(message: Message) -> str:
    if message.from_user is not None:
        return get_display_name(message.from_user)
    if message.sender_chat is not None:
        if message.sender_chat.title:
            return message.sender_chat.title
        if message.sender_chat.username:
            return message.sender_chat.username
        return str(message.sender_chat.id)
    return "Unknown"


def render_text_record(message: Message, body: str) -> str:
    timestamp = format_message_timestamp(message.date)
    sender = get_message_author_label(message)
    return f"[{timestamp}] {sender}: {body}\n\n"


def get_thread_id(message: Message) -> int | None:
    return message.reply_to_top_message_id


def format_message_timestamp(date: dt | None) -> str:
    if date is None:
        return UNKNOWN_TEXT_TIMESTAMP
    return date.strftime("%Y-%m-%d %H:%M:%S")


def get_week_bucket(date: dt | None) -> str:
    if date is None:
        return UNKNOWN_WEEK_BUCKET
    week_of_month = ((date.day - 1) // 7) + 1
    return f"{date:%Y-%m}-w{week_of_month}"


def get_text_export_path(text_chat_dir: Path, message: Message) -> Path:
    path = text_chat_dir
    thread_id = get_thread_id(message)
    if thread_id is not None:
        path /= str(thread_id)
    return path / f"{get_week_bucket(message.date)}.txt"


def get_event_text_export_path(text_chat_dir: Path, message: Message) -> Path:
    path = text_chat_dir
    thread_id = get_thread_id(message)
    if thread_id is not None:
        path /= str(thread_id)
    return path / f"{get_week_bucket(message.date)}.events.txt"


def get_snapshot_event_export_path(chat_dir: Path, event: ArchiveEvent, *, suffix: str) -> Path:
    version = event.current if event.current is not None else event.previous
    if version is None:
        raise ValueError(f"Archive event {event.event_id} has no routable message snapshot.")
    path = chat_dir
    if version.snapshot.thread_id is not None:
        path /= str(version.snapshot.thread_id)
    return path / f"{get_week_bucket(version.snapshot.sent_at)}.{suffix}"


def get_session_chat_dir(session: BackupSession, chat_id: int, *, sink: ArchiveEventSink) -> Path | None:
    if sink is ArchiveEventSink.JSON:
        if not session.export_json or session.json_output_dir is None:
            return None
        return session.json_output_dir / "chats" / str(chat_id)
    if not session.export_text or session.text_output_dir is None:
        return None
    return session.text_output_dir / "chats" / str(chat_id)


def get_json_export_path(json_chat_dir: Path, message: Message, *, kind: str) -> Path:
    path = json_chat_dir
    thread_id = get_thread_id(message)
    if thread_id is not None:
        path /= str(thread_id)
    return path / f"{get_week_bucket(message.date)}.{kind}.json"


def get_deletion_event_paths(session: BackupSession, chat_id: int | None) -> tuple[Path | None, Path | None]:
    if chat_id is None:
        json_path = (
            session.json_output_dir / "unresolved.message-events.json"
            if session.export_json and session.json_output_dir is not None
            else None
        )
        text_path = (
            session.text_output_dir / "unresolved.message-events.txt"
            if session.export_text and session.text_output_dir is not None
            else None
        )
        return json_path, text_path

    json_path = (
        session.json_output_dir / "chats" / str(chat_id) / "unknown.events.json"
        if session.export_json and session.json_output_dir is not None
        else None
    )
    text_path = (
        session.text_output_dir / "chats" / str(chat_id) / "unknown.events.txt"
        if session.export_text and session.text_output_dir is not None
        else None
    )
    return json_path, text_path


def get_unresolved_event_path(
    session: BackupSession,
    event: UnresolvedDeletionEvent,
    *,
    sink: ArchiveEventSink,
) -> Path | None:
    json_path, text_path = get_deletion_event_paths(session, event.chat_id)
    return json_path if sink is ArchiveEventSink.JSON else text_path


def get_media_target_path(info: MediaFileInfo, output_dir: Path) -> Path:
    default_media_directory = output_dir / "unknown_files"
    media_types_directories: dict[FileType, Path] = {
        file_type: output_dir / f"{file_type.name}s".lower().replace("_", " ") for file_type in FileType
    }
    file_name = Path(info.file_name.replace(":", "-")).name
    directory = media_types_directories.get(info.file_type, default_media_directory)
    return directory / file_name


def index_message_media(
    client: Client,
    messages: Iterable[Message],
    *,
    media_index: MediaIndex | None,
    json_chat_dir: Path | None,
) -> None:
    if media_index is None or json_chat_dir is None:
        return

    discoveries: list[MediaDiscovery] = []
    for message in messages:
        try:
            media = get_media(message)
            if media is None:
                continue
            info = get_media_file_info(client, media)
            if info is None:
                continue
            target_path = get_media_target_path(info, json_chat_dir).absolute()
            discoveries.append(
                MediaDiscovery(
                    media_id=info.media_id,
                    target_path=target_path,
                    file_id=info.raw_file_id,
                    expected_size=info.file_size,
                )
            )
        except (binascii.Error, IndexError, OSError, struct.error, TypeError, ValueError) as error:
            log.warning("Cannot index media from message %s: %s", message.id, error)

    if discoveries:
        media_index.upsert_many(discoveries)
    records = [
        record
        for discovery in discoveries
        if (record := media_index.get(discovery.media_id, discovery.target_path)) is not None
    ]
    reconcile_media_records(media_index, records)


def discover_media_manifests(client: Client, output_dir: Path, media_index: MediaIndex) -> None:
    if not output_dir.exists():
        return

    try:
        manifest_paths = sorted(output_dir.rglob("*.medias.json"))
    except OSError as error:
        log.warning("Cannot scan media manifests in %s: %s", output_dir, error)
        return
    for manifest_path in manifest_paths:
        try:
            stat = manifest_path.stat()
            source_path = manifest_path.absolute()
            if not media_index.source_needs_scan(source_path, size=stat.st_size, modified_ns=stat.st_mtime_ns):
                continue
            with manifest_path.open("r", encoding=DEFAULT_ENCODING) as fp:
                raw_items = json.load(fp)
            if not isinstance(raw_items, list):
                raise TypeError("media manifest must contain a JSON list")
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as error:
            log.warning("Cannot read media manifest %s: %s", manifest_path, error)
            continue

        had_errors = False
        discoveries: list[MediaDiscovery] = []
        for item in raw_items:
            try:
                if not isinstance(item, dict):
                    raise TypeError("media entry must be a JSON object")
                info = get_media_file_info_from_payload(client, item)
                target_path = get_media_target_path(info, output_dir).absolute()
                discoveries.append(MediaDiscovery(info.media_id, target_path, info.raw_file_id, info.file_size))
            except (binascii.Error, IndexError, KeyError, struct.error, TypeError, ValueError) as error:
                had_errors = True
                log.warning("Cannot index media entry from %s: %s", manifest_path, error)

        try:
            if discoveries:
                media_index.upsert_many(discoveries)
            records = [
                record
                for discovery in discoveries
                if (record := media_index.get(discovery.media_id, discovery.target_path)) is not None
            ]
            reconcile_media_records(media_index, records)
        except (OSError, ValueError) as error:
            log.warning("Cannot persist media manifest %s: %s", manifest_path, error)
            continue
        if not had_errors:
            media_index.mark_source_scanned(source_path, size=stat.st_size, modified_ns=stat.st_mtime_ns)


def reconcile_media_record(media_index: MediaIndex, record: MediaRecord) -> None:
    reconcile_media_records(media_index, [record])


def reconcile_media_records(media_index: MediaIndex, records: Iterable[MediaRecord]) -> None:
    completed: list[tuple[str, Path]] = []
    pending: list[tuple[str, Path]] = []
    for record in records:
        try:
            complete = media_file_is_complete(record.target_path, record.expected_size)
        except OSError as error:
            log.warning("Cannot inspect indexed media %s: %s", record.target_path, error)
            continue
        key = (record.media_id, record.target_path)
        if complete and record.status is not MediaStatus.COMPLETED:
            completed.append(key)
        elif not complete and record.status is MediaStatus.COMPLETED:
            pending.append(key)
    if completed:
        media_index.mark_completed_many(completed)
    if pending:
        media_index.mark_pending_many(pending, reason="downloaded file is missing or incomplete")


def media_file_is_complete(path: Path, expected_size: int | None) -> bool:
    if not path.is_file():
        return False
    actual_size = path.stat().st_size
    return actual_size > 0 if expected_size in {None, 0} else actual_size == expected_size


async def download_indexed_media(
    client: Client,
    media_index: MediaIndex,
    *,
    retry_failed: bool = True,
    allowed_root: Path | None = None,
) -> None:
    reconcile_media_records(media_index, media_index.iter_status(MediaStatus.COMPLETED))

    records = list(media_index.iter_pending(include_failed=retry_failed))
    if allowed_root is not None:
        absolute_root = allowed_root.absolute()
        records = [record for record in records if record.target_path.is_relative_to(absolute_root)]
    if not records:
        return

    records_by_target: dict[Path, list[MediaRecord]] = defaultdict(list)
    for record in records:
        records_by_target[record.target_path].append(record)
    conflicting_targets = {
        target_path: target_records
        for target_path, target_records in records_by_target.items()
        if len({record.media_id for record in target_records}) > 1
    }
    for target_path, target_records in conflicting_targets.items():
        error = f"multiple media identities resolve to the same target path: {target_path}"
        for record in target_records:
            media_index.mark_failed(record.media_id, record.target_path, error)
        log.error("Cannot safely download %s", error)
    records = [record for record in records if record.target_path not in conflicting_targets]
    if not records:
        return

    queue: asyncio.Queue[MediaRecord] = asyncio.Queue()
    for record in records:
        queue.put_nowait(record)

    async def worker() -> None:
        while True:
            try:
                record = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                media_index.begin_attempt(record.media_id, record.target_path)
                await download_media_record(client, record)
            except Exception as error:
                media_index.mark_failed(record.media_id, record.target_path, f"{type(error).__name__}: {error}")
                log.exception("Failed to download media %s; continuing.", record.target_path)
            else:
                media_index.mark_completed(record.media_id, record.target_path)
            finally:
                queue.task_done()

    log.info("Downloading %s indexed media files.", len(records))
    workers = [asyncio.create_task(worker()) for _ in range(min(4, len(records)))]
    await asyncio.gather(*workers)


async def download_media_record(client: Client, record: MediaRecord) -> None:
    target_path = record.target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if media_file_is_complete(target_path, record.expected_size):
        log.info("Skip already complete %s", target_path)
        return
    if target_path.exists():
        preserved_path = get_incomplete_media_backup_path(target_path)
        target_path.replace(preserved_path)
        log.warning("Preserved incomplete media file as %s", preserved_path)

    temporary_path = target_path.with_name(f".{target_path.name}.{uuid4().hex}.part")
    log.info("Start downloading %s", target_path)
    file_id = FileId.decode(record.file_id)
    try:
        await client.handle_download(  # type: ignore[no-untyped-call]
            (file_id, target_path.parent, temporary_path.name, False, record.expected_size or 0, None, ())
        )
        if not media_file_is_complete(temporary_path, record.expected_size):
            raise OSError(f"download did not produce the expected file: {target_path}")
        temporary_path.replace(target_path)
    except BaseException:
        if temporary_path.exists():
            preserved_path = get_incomplete_media_backup_path(target_path)
            temporary_path.replace(preserved_path)
            log.warning("Preserved interrupted media download as %s", preserved_path)
        raise
    log.info("Complete %s", target_path)


def get_incomplete_media_backup_path(target_path: Path) -> Path:
    backup_path = target_path.with_name(f"{target_path.name}.incomplete.bak")
    counter = 1
    while backup_path.exists():
        backup_path = target_path.with_name(f"{target_path.name}.incomplete.{counter}.bak")
        counter += 1
    return backup_path


def get_media(source: Message) -> TGMedia | None:
    return (
        source.animation
        or source.audio
        or source.document
        or source.new_chat_photo
        or source.photo
        or source.sticker
        or source.video
        or source.video_note
        or source.voice
    )


def get_media_file_info(client: Client, media: TGMedia) -> MediaFileInfo | None:
    for attrname in ("big_file_id", "file_id"):
        file_id = getattr(media, attrname, None)
        if file_id is not None:
            break

    if not isinstance(file_id, str):
        log.warning("Non-string file_id or big_file_id in %s", type(media).__name__)
        return None

    try:
        file_info = FileId.decode(file_id)
    except (binascii.Error, IndexError, struct.error, TypeError, ValueError):
        log.warning("Can't decode file id %s (%s) ", file_id, type(media).__name__)
        return None

    file_name = getattr(media, "file_name", "")
    file_size = normalize_file_size(getattr(media, "file_size", None))
    mime_type = getattr(media, "mime_type", "")
    date = getattr(media, "date", None)
    unique_id = getattr(media, "file_unique_id", None)

    return build_media_file_info(
        client,
        raw_file_id=file_id,
        media_id=unique_id if isinstance(unique_id, str) and unique_id else file_id,
        file_type=file_info.file_type,
        file_name=file_name if isinstance(file_name, str) else "",
        file_size=file_size,
        mime_type=mime_type if isinstance(mime_type, str) else "",
        date=date,
        is_sticker=isinstance(media, Sticker),
        sticker_set_name=media.set_name if isinstance(media, Sticker) else None,
        sticker_emoji=media.emoji if isinstance(media, Sticker) else None,
        unique_id=unique_id if isinstance(unique_id, str) else None,
    )


def get_media_file_info_from_payload(client: Client, payload: dict[str, object]) -> MediaFileInfo:
    raw_file_id = payload.get("big_file_id") or payload.get("file_id")
    if not isinstance(raw_file_id, str) or not raw_file_id:
        raise ValueError("media entry has no downloadable file id")
    file_info = FileId.decode(raw_file_id)
    unique_id_raw = payload.get("file_unique_id")
    unique_id = unique_id_raw if isinstance(unique_id_raw, str) and unique_id_raw else None
    file_name_raw = payload.get("file_name")
    mime_type_raw = payload.get("mime_type")
    sticker_set_name_raw = payload.get("set_name")
    sticker_emoji_raw = payload.get("emoji")

    return build_media_file_info(
        client,
        raw_file_id=raw_file_id,
        media_id=unique_id or raw_file_id,
        file_type=file_info.file_type,
        file_name=file_name_raw if isinstance(file_name_raw, str) else "",
        file_size=normalize_file_size(payload.get("file_size")),
        mime_type=mime_type_raw if isinstance(mime_type_raw, str) else "",
        date=payload.get("date"),
        is_sticker=payload.get("_") == "Sticker",
        sticker_set_name=sticker_set_name_raw if isinstance(sticker_set_name_raw, str) else None,
        sticker_emoji=sticker_emoji_raw if isinstance(sticker_emoji_raw, str) else None,
        unique_id=unique_id,
    )


def build_media_file_info(  # noqa: PLR0913
    client: Client,
    *,
    raw_file_id: str,
    media_id: str,
    file_type: FileType,
    file_name: str,
    file_size: int | None,
    mime_type: str,
    date: object,
    is_sticker: bool,
    sticker_set_name: str | None,
    sticker_emoji: str | None,
    unique_id: str | None,
) -> MediaFileInfo:
    if not file_name:
        guessed_extension = client.guess_extension(mime_type)
        default_extension = DEFAULT_EXTS[file_type]
        extension = (guessed_extension or default_extension).lstrip(".")
        file_name = f"{file_type.name.lower()}_{date or dt.now(tz=UTC).strftime('%Y-%m-%d_%H-%M-%S')}.{extension}"
    if is_sticker:
        file_name = f"sticker_{sticker_set_name}_{sticker_emoji}_{unique_id}.webp"

    name, ext = file_name.rsplit(".", maxsplit=1) if "." in file_name else (file_name, "unknown")
    file_name = f"{name}_{unique_id or media_id}.{ext}"

    return MediaFileInfo(raw_file_id, file_name, file_size, media_id)


def normalize_file_size(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


async def get_chats_info(client: Client) -> list[Chat]:
    log.info("Start grabbbing chats info.")
    dialogs_iter: AsyncIterator[Dialog] = client.get_dialogs()
    dialogs: list[Dialog | None] = []
    async for counter, dialogs_batch in batch_asynciter(dialogs_iter):
        dialogs.extend(dialogs_batch)
        log.info("Grabbed %s.", counter)
    chats = [dialog.chat for dialog in dialogs if dialog is not None]

    log.info("Finished grabbbing chats info, got %s items.", len(chats))

    chats_by_type: dict[ChatType, list[Chat]] = {chat_type: [] for chat_type in ChatType}
    for chat in chats:
        if chat.type is not None:
            chats_by_type[chat.type].append(chat)

    log.info(
        "Collected chats info stats:\n%s",
        "\n".join(f"{chat_type.name.lower()}: {len(items)}" for chat_type, items in chats_by_type.items()),
    )
    return chats


async def get_chat_avatars(client: Client, chat_id: int) -> list[Photo | Animation] | None:
    retry = True
    while retry:
        retry = False
        try:
            chat_photos = client.get_chat_photos(chat_id)
            avatars = None if chat_photos is None else [avatar async for avatar in chat_photos]
        except UserIdInvalid:
            log.warning("Failed to get chat avatars.")
            avatars = None
        except FloodWait as flood:
            log.warning("Got floodwait from Telegram", exc_info=flood)
            retry = True
            await asyncio.sleep(TELEGRAM_BACKOFF_TIME)
        except RPCError as error:
            if "MONOFORUM_NOT_SUPPORTED" not in str(error):
                raise
            log.warning("Telegram does not support avatar history for chat %s: %s", chat_id, error)
            avatars = None
    return avatars


async def get_chat_messages(
    client: Client,
    chat_id: int,
    *,
    batch_size: int = 1000,
    max_id: int = 0,
) -> AsyncIterator[list[Message]]:
    log.info("Start grabbbing messages of chat %s.", chat_id)
    messages_iter: AsyncIterator[Message] = client.get_chat_history(chat_id=chat_id, max_id=max_id)
    count = 0
    async for counter, messages_batch in batch_asynciter(messages_iter, batch_size=batch_size):
        clean_batch = [message for message in messages_batch if message is not None]
        count += len(clean_batch)
        yield clean_batch
        log.info("Grabbed %s...", counter)
    log.info("Finishsed grabbbing messages of chat %s, got %s items.", chat_id, count)


async def batch_asynciter[T](
    async_iterator: AsyncIterator[T], batch_size: int = 100
) -> AsyncIterator[tuple[int, list[T]]]:
    finished = False
    counter = 0
    while not finished:
        batch: list[T] = []
        for _ in range(batch_size):
            try:
                batch.append(await anext(async_iterator))
            except StopAsyncIteration:
                finished = True
                break
        counter += len(batch)
        with suppress(asyncio.CancelledError):
            yield counter, batch
