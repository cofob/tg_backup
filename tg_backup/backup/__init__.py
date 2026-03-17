from __future__ import annotations

import asyncio
import bisect
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC
from datetime import datetime as dt
from functools import cached_property
from pathlib import Path
from typing import BinaryIO, NamedTuple, Protocol, TypeAlias, TypeVar

from adaptix import Retort
from json_stream import load, to_standard_types
from pyrogram import Client, raw
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

from tg_backup.utils.text_streaming import TextExportWriter, TextRecord

T = TypeVar("T")

type TGMedia = Audio | Document | Photo | Sticker | Animation | Video | Voice | VideoNote

DEFAULT_ENCODING = "utf-8"
DEFAULT_JSON_INDENT = 2
TELEGRAM_BACKOFF_TIME = 30
FORUM_TOPICS_PAGE_SIZE = 100
STATE_FILE_NAME = "state.json"

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

RETORT = Retort()
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


@dataclass(frozen=True)
class ForumTopicEntry:
    id: int
    title: str


@dataclass(frozen=True)
class MediaFileInfo:
    raw_file_id: str | FileId
    file_name: str
    file_size: int | None

    @cached_property
    def file_id(self) -> FileId:
        if isinstance(self.raw_file_id, FileId):
            return self.raw_file_id
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
    if chat.type is ChatType.BOT:
        qualname = chat.first_name
    elif chat.type is ChatType.PRIVATE:
        qualname = f"{chat.first_name} {chat.last_name}" if chat.last_name else chat.first_name
    elif chat.type in {ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP}:
        qualname = chat.title
    else:
        qualname = "unknown"
    return ChatBrief(type=chat.type, username=chat.username, qualname=qualname, id=chat.id)


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

    await client.start()

    state_file = state_output_dir / STATE_FILE_NAME
    state, chats_by_id = await get_or_refresh_backup_state(client=client, state_file=state_file)
    dump_export_chat_mappings(state, json_output_dir=json_output_dir, text_output_dir=text_output_dir)

    def persist_state() -> None:
        dump_backup_state(state_file, state)

    log.info("Start syncing chats.")
    for chat_state in state.chats:
        chat = chats_by_id[chat_state.id]
        json_chat_dir = (json_output_dir / "chats" / str(chat.id)) if json_output_dir is not None else None
        text_chat_dir = (text_output_dir / "chats" / str(chat.id)) if text_output_dir is not None else None

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

        if chat_state.history_complete:
            await append_recent_messages(
                client,
                chat,
                chat_state,
                persist_state=persist_state,
                json_chat_dir=json_chat_dir,
                text_chat_dir=text_chat_dir,
                export_json=export_json,
                export_text=export_text,
            )
        else:
            await append_chat_history(
                client,
                chat,
                chat_state,
                persist_state=persist_state,
                json_chat_dir=json_chat_dir,
                text_chat_dir=text_chat_dir,
                export_json=export_json,
                export_text=export_text,
            )

    log.info("Finished syncing chats.")

    if download_attachments and export_json and json_output_dir is not None:
        log.info("Start downloading chats medias.")
        for chat_state in state.chats:
            chat_dir = json_output_dir / "chats" / str(chat_state.id)
            await load_files(client, chat_dir)
        log.info("Finished downloading chats medias.")
    elif not download_attachments:
        log.info("Skipping media downloads because DOWNLOAD_ATTACHMENTS is disabled.")

    return BackupSession(
        state=state,
        state_file=state_file,
        json_output_dir=json_output_dir,
        text_output_dir=text_output_dir,
        export_json=export_json,
        export_text=export_text,
        download_attachments=download_attachments,
    )


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
    chats_by_id = {chat.id: chat for chat in chats}
    existing_state = load_backup_state(state_file)
    existing_by_id = {chat.id: chat for chat in existing_state.chats}

    merged_states: list[ChatExportState] = []
    for chat in chats:
        brief = get_chat_brief(chat)
        current = existing_by_id.get(chat.id)
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
            )
        )

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

        chats.append(
            ChatExportState(
                id=chat_id,
                chat_type=chat_type,
                username=username,
                qualname=qualname,
                history_complete=history_complete,
                oldest_message_id=oldest_message_id,
                latest_message_id=latest_message_id,
            )
        )

    return BackupState(chats=chats)


def dump_backup_state(state_file: Path, state: BackupState) -> None:
    payload = {"chats": [asdict(chat) for chat in state.chats]}
    with state_file.open("w", encoding=DEFAULT_ENCODING) as fp:
        json.dump(payload, fp, indent=DEFAULT_JSON_INDENT, ensure_ascii=False)


def dump_export_chat_mappings(
    state: BackupState, *, json_output_dir: Path | None, text_output_dir: Path | None
) -> None:
    chat_mapping = {str(chat.id): chat.qualname for chat in state.chats}

    if json_output_dir is not None:
        chats_json = json_output_dir / "chats.json"
        with chats_json.open("w", encoding=DEFAULT_ENCODING) as fp:
            json.dump(chat_mapping, fp, indent=DEFAULT_JSON_INDENT, ensure_ascii=False)

    if text_output_dir is not None:
        chats_txt = text_output_dir / "chats.txt"
        lines = [f"{chat.id}\t{chat.qualname}" for chat in state.chats]
        with chats_txt.open("w", encoding=DEFAULT_ENCODING) as fp:
            fp.write("\n".join(lines))
            if lines:
                fp.write("\n")


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
        with topics_json.open("w", encoding=DEFAULT_ENCODING) as fp:
            json.dump(payload, fp, indent=DEFAULT_JSON_INDENT, ensure_ascii=False)

    if text_chat_dir is not None:
        topics_txt = get_topics_txt_path(text_chat_dir)
        topics_txt.parent.mkdir(parents=True, exist_ok=True)
        lines = [f"{topic.id}\t{topic.title}" for topic in sorted_topics]
        with topics_txt.open("w", encoding=DEFAULT_ENCODING) as fp:
            fp.write("\n".join(lines))
            if lines:
                fp.write("\n")


def forum_topic_exists(topic_id: int, *, json_chat_dir: Path | None, text_chat_dir: Path | None) -> bool:
    return any(
        topic.id == topic_id
        for topic in load_existing_forum_topics(
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
        )
    )


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
    if not await is_forum_chat(client, chat.id):
        return

    try:
        topics = await get_forum_topics(client, chat.id, topic_ids=topic_ids)
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
    channel = get_input_channel(peer)
    if channel is None:
        return []

    if topic_ids is not None:
        response = await client.invoke(
            raw.functions.channels.GetForumTopicsByID(
                channel=channel,
                topics=list(topic_ids),
            )
        )
        return [ForumTopicEntry(id=topic.id, title=topic.title) for topic in response.topics]

    topics: list[ForumTopicEntry] = []
    offset_date = 0
    offset_id = 0
    offset_topic = 0

    while True:
        response = await client.invoke(
            raw.functions.channels.GetForumTopics(
                channel=channel,
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=FORUM_TOPICS_PAGE_SIZE,
            )
        )
        if not response.topics:
            break

        topics.extend(ForumTopicEntry(id=topic.id, title=topic.title) for topic in response.topics)
        last_topic = response.topics[-1]
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
) -> None:
    offset_id = chat_state.oldest_message_id or 0
    async for messages_batch in get_chat_messages(client=client, chat_id=chat.id, offset_id=offset_id):
        if not messages_batch:
            continue
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
) -> None:
    if chat_state.latest_message_id is None:
        return

    latest_known_id = chat_state.latest_message_id
    pending_messages: list[Message] = []
    async for messages_batch in get_chat_messages(client=client, chat_id=chat.id):
        if not messages_batch:
            continue

        new_messages = [message for message in messages_batch if message.id > latest_known_id]
        if new_messages:
            pending_messages.extend(new_messages)

        if len(new_messages) != len(messages_batch):
            break

    if not pending_messages:
        return

    pending_messages.sort(key=lambda message: message.id)
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
            await dump_chat_json_metadata(client=client, chat=chat, json_chat_dir=json_chat_dir)
    if session.export_text and text_chat_dir is not None:
        text_chat_dir.mkdir(parents=True, exist_ok=True)

    thread_id = get_thread_id(message)
    if thread_id is not None and not forum_topic_exists(
        thread_id,
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
    ):
        await refresh_forum_topics(
            client,
            chat=chat,
            json_chat_dir=json_chat_dir,
            text_chat_dir=text_chat_dir,
            topic_ids=[thread_id],
        )

    append_export_batch(
        messages=[message],
        json_chat_dir=json_chat_dir,
        text_chat_dir=text_chat_dir,
        export_json=session.export_json,
        export_text=session.export_text,
    )

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

    if session.download_attachments and session.export_json and json_chat_dir is not None:
        media = get_media(message)
        if media is not None:
            info = get_media_file_info(client, media)
            if info is not None:
                await download_media_file(client, info, json_chat_dir)


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
    encoded_items = [item.encode(DEFAULT_ENCODING) for item in dumped_items]

    if not path.exists() or path.stat().st_size == 0:
        with path.open("wb") as fp:
            fp.write(b"[")
            fp.write(b",\n".join(encoded_items))
            fp.write(b"]")
        return

    with path.open("rb+") as fp:
        list_end_position = find_json_list_end(fp)
        need_separator = json_list_has_items(fp, list_end_position)
        fp.seek(list_end_position)
        fp.truncate()
        if need_separator:
            fp.write(b",\n")
        fp.write(b",\n".join(encoded_items))
        fp.write(b"]")


def find_json_list_end(fp: BinaryIO) -> int:
    fp.seek(0, 2)
    position = fp.tell() - 1
    while position >= 0:
        fp.seek(position)
        chunk = fp.read(1)
        if chunk not in b" \t\r\n":
            if chunk != b"]":
                raise ValueError("Expected JSON list file to end with ']'.")
            return position
        position -= 1
    raise ValueError("Expected a non-empty JSON list file.")


def json_list_has_items(fp: BinaryIO, list_end_position: int) -> bool:
    position = list_end_position - 1
    while position >= 0:
        fp.seek(position)
        chunk = fp.read(1)
        if chunk in b" \t\r\n":
            position -= 1
            continue
        return chunk != b"["
    return False


async def dump_chat_json_metadata(client: Client, chat: Chat, *, json_chat_dir: Path) -> None:
    chat_info = await client.get_chat(chat_id=chat.id)
    chat_info_json = json_chat_dir / "info.json"
    log.info("Dump chat info (%s).", chat_info_json)
    with chat_info_json.open("w", encoding=DEFAULT_ENCODING) as fp:
        json.dump(chat_info, fp, indent=DEFAULT_JSON_INDENT, default=Object.default, ensure_ascii=False)

    log.info("Get chat avatars info.")
    avatars = await get_chat_avatars(client, chat.id)
    avatars_json = json_chat_dir / "avatars.json"
    log.info("Dump chat avatars info (%s).", avatars_json)
    with avatars_json.open("w", encoding=DEFAULT_ENCODING) as fp:
        json.dump(avatars, fp, indent=DEFAULT_JSON_INDENT, default=Object.default, ensure_ascii=False)


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
    elif message.new_chat_title:  # type: ignore[unreachable]
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
    if message.sender_chat is not None:  # type: ignore[unreachable]
        if message.sender_chat.title:
            return message.sender_chat.title
        if message.sender_chat.username:
            return message.sender_chat.username
        return str(message.sender_chat.id)
    return "Unknown"


def render_text_record(message: Message, body: str) -> str:
    timestamp = message.date.strftime("%Y-%m-%d %H:%M:%S")
    sender = get_message_author_label(message)
    return f"[{timestamp}] {sender}: {body}\n\n"


def get_thread_id(message: Message) -> int | None:
    return message.reply_to_top_message_id


def get_week_bucket(date: dt) -> str:
    week_of_month = ((date.day - 1) // 7) + 1
    return f"{date:%Y-%m}-w{week_of_month}"


def get_text_export_path(text_chat_dir: Path, message: Message) -> Path:
    path = text_chat_dir
    thread_id = get_thread_id(message)
    if thread_id is not None:
        path /= str(thread_id)
    return path / f"{get_week_bucket(message.date)}.txt"


def get_json_export_path(json_chat_dir: Path, message: Message, *, kind: str) -> Path:
    path = json_chat_dir
    thread_id = get_thread_id(message)
    if thread_id is not None:
        path /= str(thread_id)
    return path / f"{get_week_bucket(message.date)}.{kind}.json"


async def load_files(client: Client, output_dir: Path) -> None:
    default_media_directory = output_dir / "unknown_files"
    default_media_directory.mkdir(parents=True, exist_ok=True)
    media_types_directories: dict[FileType, Path] = {
        file_type: output_dir / f"{file_type.name}s".lower().replace("_", " ")
        for file_type in FileType
    }
    for directory in media_types_directories.values():
        directory.mkdir(exist_ok=True)

    queue: asyncio.Queue[MediaFileInfo] = asyncio.Queue()

    infos_files = sorted(output_dir.rglob("*.medias.json"))
    if not infos_files:
        return

    for infos_json in infos_files:
        try:
            with infos_json.open("r", encoding=DEFAULT_ENCODING) as fp:
                for info_view in load(fp):
                    info_raw = to_standard_types(info_view)
                    info = RETORT.load(info_raw, MediaFileInfo)
                    queue.put_nowait(info)
        except Exception:
            log.warning("Files infos file (%s) looks corrupted, skipping...", infos_json)

    total_files = queue.qsize()

    async def worker() -> None:
        while True:
            try:
                info = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            try:
                await download_media_file(client, info, output_dir)
            finally:
                queue.task_done()

    log.info("Downloading %s medias", total_files)
    workers = [asyncio.create_task(worker()) for _ in range(8)]
    await queue.join()
    await asyncio.gather(*workers)


async def download_media_file(client: Client, info: MediaFileInfo, output_dir: Path) -> None:
    default_media_directory = output_dir / "unknown_files"
    default_media_directory.mkdir(parents=True, exist_ok=True)
    media_types_directories: dict[FileType, Path] = {
        file_type: output_dir / f"{file_type.name}s".lower().replace("_", " ")
        for file_type in FileType
    }
    for directory in media_types_directories.values():
        directory.mkdir(exist_ok=True)

    file_name = info.file_name.replace(":", "-")
    directory = media_types_directories.get(info.file_type, default_media_directory)
    target_path = directory / file_name
    if target_path.exists():
        log.info("Skip already existing %s", target_path.as_posix())
        return

    log.info("Start downloading %s", target_path.as_posix())
    await client.handle_download((info.file_id, directory, file_name, False, info.file_size, None, ()))  # type: ignore[no-untyped-call]
    log.info("Complete %s", target_path.as_posix())


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

    file_info = FileId.decode(file_id)
    if file_info is None:
        log.warning("Can't decode file id %s (%s) ", file_id, type(media).__name__)
        return None

    file_type = file_info.file_type
    file_name = getattr(media, "file_name", "")
    file_size = getattr(media, "file_size", 0)
    mime_type = getattr(media, "mime_type", "")
    date = getattr(media, "date", None)

    if not file_name:
        guessed_extension = client.guess_extension(mime_type)
        default_extension = DEFAULT_EXTS[file_type]
        extension = guessed_extension or default_extension
        file_name = f"{file_type.name.lower()}_{date or dt.now(tz=UTC).strftime('%Y-%m-%d_%H-%M-%S')}.{extension}"
    if isinstance(media, Sticker):
        file_name = f"sticker_{media.set_name}_{media.emoji}_{media.file_unique_id}.webp"

    name, ext = file_name.rsplit(".", maxsplit=1) if "." in file_name else (file_name, "unknown")
    uniq_id = getattr(media, "file_unique_id", None)
    file_name = f"{name}_{uniq_id}.{ext}"

    return MediaFileInfo(file_info, file_name, file_size)


async def get_chats_info(client: Client) -> list[Chat]:
    log.info("Start grabbbing chats info.")
    dialogs_iter: AsyncIterator[Dialog] = client.get_dialogs()  # type: ignore[assignment]
    dialogs: list[Dialog | None] = []
    async for counter, dialogs_batch in batch_asynciter(dialogs_iter):
        dialogs.extend(dialogs_batch)
        log.info("Grabbed %s.", counter)
    chats = [dialog.chat for dialog in dialogs if dialog is not None]

    log.info("Finished grabbbing chats info, got %s items.", len(chats))

    chats_by_type: dict[ChatType, list[Chat]] = {chat_type: [] for chat_type in ChatType}
    for chat in chats:
        chats_by_type[chat.type].append(chat)

    log.info(
        "Collected chats info stats:\n%s",
        "\n".join(f"{chat_type.name.lower()}: {len(items)}" for chat_type, items in chats_by_type.items()),
    )
    return chats


async def get_chat_avatars(client: Client, chat_id: int) -> list[Photo] | None:
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
    return avatars


async def get_chat_messages(
    client: Client,
    chat_id: int,
    *,
    batch_size: int = 1000,
    offset_id: int = 0,
) -> AsyncIterator[list[Message]]:
    log.info("Start grabbbing messages of chat %s.", chat_id)
    messages_iter: AsyncIterator[Message] = client.get_chat_history(chat_id=chat_id, offset_id=offset_id)  # type: ignore[assignment]
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
