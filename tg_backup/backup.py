from __future__ import annotations

import asyncio
import bisect
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime as dt
from functools import cached_property
from operator import attrgetter
from pathlib import Path
from typing import ContextManager, NamedTuple, ParamSpec, Protocol, TypeAlias, TypeVar, TypeVarTuple

from adaptix import Retort
from json_stream import load, to_standard_types
from pyrogram import Client
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES
from pyrogram.types.object import Object
from pyrogram.types import Chat, Dialog
from pyrogram.types import Audio, Document, Photo, Sticker, Animation, Video, Voice, VideoNote
from pyrogram.enums import ChatType
from pyrogram.types import Message
from pyrogram.errors import UserIdInvalid, FloodWait

from tg_backup.utils.json_streaming import JSONListWriter, list_writer
from tg_backup.utils.progress_tracking import ProgressTracker


T = TypeVar("T")
Ts = TypeVarTuple("Ts")
Params = ParamSpec("Params")
Return = TypeVar("Return")

TGMedia: TypeAlias = Audio | Document | Photo | Sticker | Animation | Video | Voice | VideoNote
TGMediaTypes = Audio, Document, Photo, Sticker, Animation, Video, Voice, VideoNote


IMAGE_EXTS = {type: ".jpg" for type in PHOTO_TYPES}
ANIMATED_EXTS = {
    type: ".mp4" for type in (FileType.VIDEO, FileType.ANIMATION, FileType.VIDEO_NOTE)
}
DEFAULT_EXTS = defaultdict(lambda: ".unknown", {
    **IMAGE_EXTS,
    **ANIMATED_EXTS,
    FileType.VOICE: ".ogg",
    FileType.STICKER: ".webp",
    FileType.AUDIO: ".mp3",
})


RETORT = Retort()


log = logging.getLogger('tg_backup')


SIZES = sorted([
    (10**(3*n), prefix)
    for n, prefix in
    enumerate((
        'q', 'r', 'y', 'z', 'a', 'f', 'p', 'n', 'µ', 'm',
        '',
        'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y', 'R', 'Q'
    ), start=-10)
])


class HasId(Protocol):
    id: int


@dataclass
class LoadedChat:
    id: int


def human_readable(n: float, unit: str, *, precision: int = 2) -> str:
    idx = bisect.bisect(SIZES, (n, ''))
    idx = max(0, min(idx-1, len(SIZES)))
    divisor, prefix = SIZES[idx]
    value = round(n/divisor, precision)
    return f"{value:.{precision}f} {prefix}{unit}"


def tgobject_list_writer(
    fp,
    indent: int | str | None = 2,
    executor: Executor | None = None
) -> ContextManager[JSONListWriter]:
    return list_writer(fp=fp, indent=indent, default=Object.default, ensure_ascii=False, executor=executor)


class ChatBrief(NamedTuple):
    type: ChatType
    username: str | None
    qualname: str
    id: int


def get_chat_brief(chat: Chat) -> ChatBrief:
    qualname_getters: dict[ChatType, Callable[[Chat], str]] = {
        ChatType.BOT: attrgetter('first_name'),
        ChatType.PRIVATE: lambda chat: f"{chat.first_name} {chat.last_name}" if chat.last_name else chat.first_name,
        ChatType.CHANNEL: attrgetter('title'),
        ChatType.GROUP: attrgetter('title'),
        ChatType.SUPERGROUP: attrgetter('title'),
    }
    qualname = qualname_getters.get(chat.type, lambda _: 'unknown')(chat)
    info = ChatBrief(type=chat.type, username=chat.username, qualname=qualname, id=chat.id)
    return info


async def backup(client: Client, output_dir: Path) -> None:
    await client.start()

    chats_json = output_dir / "chats.json"
    chats: Sequence[Chat]
    if chats_json.exists():
        log.info("Read chats info (%s)", chats_json)
        with chats_json.open("r", encoding="utf-8") as f:
            chats_raw = json.load(f)
            ids = [info["id"] for info in chats_raw]
            chats_by_id = {chat.id: chat for chat in await get_chats_info(client=client)}
            chats = []
            for index, chat_id in enumerate(ids):
                chat_by_id = chats_by_id.pop(chat_id)
                if chat_by_id is None:
                    log.info("Skip chat %s (%s) - can't resolve", index, chat_id)
                    continue
                chats.append(chat_by_id)
            chats.extend(chats_by_id.values())
    else:
        chats = await get_chats_info(client=client)
        log.info("Dump chats info (%s).", chats_json)
        with open(chats_json, "w", encoding="utf-8") as f:
            json.dump(chats, f, indent=2, default=Object.default, ensure_ascii=False)

    chats_dir = output_dir / "chats"
    chats_dir.mkdir(parents=True, exist_ok=True)

    log.info("Start collecting chats messages.")

    progress_track_file = output_dir / "progress"
    tracking_wrapper = ProgressTracker(chats, tracking_file=progress_track_file, item_name="chat", logger=log)
    for chat in tracking_wrapper:
        chat_dir = chats_dir / str(chat.id)
        chat_dir.mkdir(parents=True, exist_ok=True)

        await backup_chat(client, chat, chat_dir)

    log.info("Finished collecting chats messages.")

    log.info("Start downloading chats medias.")

    for chat in chats:
        chat_dir = chats_dir / str(chat.id)
        await load_files(client, chat.id, chat_dir)

    log.info("Finished downloading chats medias.")


async def backup_chat(client: Client, chat: Chat, output_dir: Path):
    chat_info = await client.get_chat(chat_id=chat.id)
    chat_info_json = output_dir / "info.json"
    log.info("Dump chat info (%s).", chat_info_json)
    with open(chat_info_json, "w", encoding="utf-8") as f:
        json.dump(chat_info, f, indent=2, default=Object.default, ensure_ascii=False)

    log.info("Get chat avatars info.")
    avatars = await get_chat_avatars(client, chat.id)
    avatars_json = output_dir / "avatars.json"
    log.info("Dump chat avatars info (%s).", avatars_json)
    with avatars_json.open("w", encoding="utf-8") as f:
        json.dump(avatars, f, indent=2, default=Object.default, ensure_ascii=False)

    messages_json = output_dir / "messages.json"
    medias_json = output_dir / "medias.json"
    log.info("Dump chat messages (%s).", messages_json)
    with (
        messages_json.open("w", encoding="utf-8") as messages_file,
        medias_json.open("w", encoding="utf-8") as medias_file,
        ThreadPoolExecutor() as executor,
        tgobject_list_writer(messages_file, executor=executor) as messages_writer,
        tgobject_list_writer(medias_file, executor=executor) as medias_writer
    ):
        async for messages_batch in get_chat_messages(client=client, chat_id=chat.id):
            messages_writer.write_items(messages_batch)
            medias = [media for media in executor.map(get_media, messages_batch) if media is not None]
            medias_writer.write_items(medias)


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


async def load_files(client: Client, infos_json: Path, output_dir: Path) -> None:
    DEFAULT_MEDIA_DIRECTORY = output_dir / "unknown_files"
    DEFAULT_MEDIA_DIRECTORY.mkdir(parents=True, exist_ok=True)
    media_types_directories: dict[FileType, Path] = {
        file_type: output_dir / f"{file_type.name}s".lower().replace('_', ' ')
        for file_type in FileType
    }
    for directory in media_types_directories.values():
        directory.mkdir(exist_ok=True)

    total_files: int = 0
    queue: asyncio.Queue[MediaFileInfo] = asyncio.Queue()

    try:
        with infos_json.open("r", encoding="utf-8") as f:
            for index, info_view in enumerate(load(f)):
                info_raw = to_standard_types(info_view)
                info = RETORT.load(info_raw, MediaFileInfo)
                queue.put_nowait(info)
    except Exception:
        log.warning("Files infos file (%s) looks corrupted, skipping...", infos_json)
        return

    async def worker():
        while not queue.empty():
            info = await queue.get()
            file_name = info.file_name.replace(":", "-")
            directory = media_types_directories.get(info.file_type, DEFAULT_MEDIA_DIRECTORY)
            if (directory / file_name).exists():
                log.info(f"Skip alrady existed {directory.as_posix()}/{file_name}")
            log.info(f"Start downloading {directory.as_posix()}/{file_name}")
            await client.handle_download(
                (info.file_id, directory, file_name, False, info.file_size, None, ())
            )
            log.info(f"Complete {directory.as_posix()}/{file_name}")

    log.info(f"Downloading {total_files} medias")
    loop = asyncio.get_running_loop()
    workers = [loop.create_task(worker()) for _ in range(8)]
    await asyncio.wait(workers)


def load_medias(client: Client, medias_json: Path) -> list[TGMedia]:

    return []


def get_media(source: Message) -> TGMedia | None:
    media: TGMedia | None = (
        source.animation or source.audio or source.document or source.new_chat_photo or source.photo or
        source.sticker or source.video or source.video_note or source.voice
    )
    if media is None:
        return None
    return media


def get_media_file_info(client: Client, media: TGMedia) -> MediaFileInfo | None:
    for attrname in ('big_file_id', 'file_id'):
        file_id = getattr(media, attrname, None)
        if file_id is not None:
            break

    if not isinstance(file_id, str):
        log.warning("Non-string file_id or big_file_id in %s", type(media).__name__)
        return None

    file_info = FileId.decode(file_id)
    if file_info is None:
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
        file_name = (
            f"{file_type.name.lower()}"
            f"_{date or dt.now().strftime('%Y-%m-%d_%H-%M-%S')}"
            f".{extension}"
        )
    if isinstance(media, Sticker):
        file_name = f"sticker_{media.set_name}_{media.emoji}_{media.file_unique_id}.webp"

    name, ext = file_name.rsplit(".", maxsplit=1) if "." in file_name else (file_name, "unknown")
    uniq_id = getattr(media, "file_unique_id", None)
    file_name = f"{name}_{uniq_id}.{ext}"

    return MediaFileInfo(file_info, file_name, file_size)


async def process(iterator: AsyncIterator[T | None], descriprion: str) -> AsyncIterator[list[T]]:
    log.info("Start %s", descriprion)
    count = 0
    async for counter, batch in batch_asynciter(iterator):
        clean_batch = [item for item in batch if item is not None]
        count += len(clean_batch)
        yield clean_batch
        log.info("Grabbed %s...", counter)
    log.info("Finishsed %s (%s items).", descriprion, count)


async def get_chats_info(client: Client) -> list[Chat]:
    log.info("Start grabbbing chats info.")

    dialogs_iter: AsyncIterator[Dialog] = client.get_dialogs()  # type: ignore  # FUCK TYPEHINTS IN PYROGRAM'S METHODS!
    dialogs: list[Dialog | None] = []
    async for counter, dialogs_batch in batch_asynciter(dialogs_iter):
        dialogs.extend(dialogs_batch)
        log.info("Grabbed %s.", counter)
    chats = [dialog.chat for dialog in dialogs if dialog]

    log.info("Finished grabbbing chats info, got %s items.", len(chats))

    chats_by_type: dict[ChatType, list[Chat]] = {type: [] for type in ChatType}
    for chat in chats:
        chats_by_type[chat.type].append(chat)

    log.info("Collected chats info stats:\n%s",
             "\n".join(f"{type.name.lower()}: {len(items)}" for type, items in chats_by_type.items()),
             )
    return chats


async def get_chat_avatars(client: Client, chat_id: int) -> list[Photo] | None:
    retry = True
    while retry:
        retry = False
        try:
            avatars: list[Photo] | None = [
                avatar async for avatar in
                client.get_chat_photos(chat_id)  # type: ignore  # FUCK TYPEHINTS IN PYROGRAM'S METHODS!
            ]
        except UserIdInvalid:
            log.warning("Failed to get chat avatars.")
            avatars = None
        except FloodWait as flood:
            log.warning("Got floodwait from Telegram", exc_info=flood)
            retry = True
            await asyncio.sleep(30)
    return avatars


async def get_chat_messages(client: Client, chat_id: int, batch_size: int = 1000) -> AsyncIterator[list[Message]]:
    log.info("Start grabbbing messages of chat %s.", chat_id)
    messages_iter: AsyncIterator[Message] = client.get_chat_history(
        chat_id=chat_id
    )  # type: ignore  # FUCK TYPEHINTS IN PYROGRAM'S METHODS!
    count = 0
    async for counter, messages_batch in batch_asynciter(messages_iter, batch_size=batch_size):
        clean_batch = [message for message in messages_batch if message is not None]
        count += len(clean_batch)
        yield clean_batch
        log.info("Grabbed %s...", counter)
    log.info("Finishsed grabbbing messages of chat %s, got %s items.", chat_id, count)


async def batch_asynciter(
    async_iterator: AsyncIterator[T],
    batch_size: int = 100
) -> AsyncIterator[tuple[int, list[T]]]:
    finished = False
    counter = 0
    while not finished:
        batch = []
        for _ in range(batch_size):
            try:
                batch.append(await anext(async_iterator))
            except StopAsyncIteration:
                finished = True
        counter += len(batch)
        try:
            yield counter, batch
        except asyncio.CancelledError:
            pass
