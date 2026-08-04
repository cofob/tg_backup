from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

type StrPath = str | os.PathLike[str]


def _fsync_directory(directory: Path) -> None:
    """Best-effort synchronization of a directory entry."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        directory_fd = os.open(directory, flags)
    except OSError:
        return

    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        with suppress(OSError):
            os.close(directory_fd)


def atomic_write_bytes(path: StrPath, data: bytes) -> None:
    """Durably replace *path* with *data* without exposing a partial file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing_mode = target.stat().st_mode & 0o7777
    except FileNotFoundError:
        existing_mode = None
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(file_descriptor, "wb") as file:
            if existing_mode is not None:
                os.fchmod(file.fileno(), existing_mode)
            bytes_written = file.write(data)
            if bytes_written != len(data):
                raise OSError(f"short write: wrote {bytes_written} of {len(data)} bytes")
            file.flush()
            os.fsync(file.fileno())

        temporary_path.replace(target)
        _fsync_directory(target.parent)
    finally:
        temporary_path.unlink(missing_ok=True)


def atomic_write_text(
    path: StrPath,
    data: str,
    *,
    encoding: str = "utf-8",
    errors: str = "strict",
) -> None:
    """Encode and atomically replace *path* with text."""
    atomic_write_bytes(path, data.encode(encoding, errors))


def atomic_write_json(
    path: StrPath,
    value: object,
    *,
    encoding: str = "utf-8",
    ensure_ascii: bool = True,
    indent: int | str | None = None,
    default: Callable[[object], object] | None = None,
    sort_keys: bool = False,
) -> None:
    """Serialize JSON and atomically replace *path* with the result."""
    serialized = json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        indent=indent,
        default=default,
        sort_keys=sort_keys,
    )
    atomic_write_text(path, serialized, encoding=encoding)
