from __future__ import annotations

from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TextIO

DEFAULT_ENCODING = "utf-8"


@dataclass(frozen=True)
class TextRecord:
    path: Path
    text: str


class TextExportWriter(AbstractContextManager["TextExportWriter"]):
    def __init__(self) -> None:
        self._files: dict[Path, TextIO] = {}

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        for fp in self._files.values():
            fp.close()
        self._files.clear()

    def write_records(self, records: Iterable[TextRecord]) -> None:
        for record in records:
            record.path.parent.mkdir(parents=True, exist_ok=True)
            fp = self._files.get(record.path)
            if fp is None:
                fp = record.path.open("a", encoding=DEFAULT_ENCODING)
                self._files[record.path] = fp
            fp.write(record.text)
            fp.flush()

    def replace_records(self, records: Iterable[TextRecord]) -> None:
        for record in records:
            self._close_path(record.path)
            record.path.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(
                mode="w",
                encoding=DEFAULT_ENCODING,
                dir=record.path.parent,
                delete=False,
                prefix=f".{record.path.stem}-",
                suffix=record.path.suffix,
            ) as tmp_file:
                tmp_file.write(record.text)
                tmp_path = Path(tmp_file.name)
            tmp_path.replace(record.path)

    def _close_path(self, path: Path) -> None:
        fp = self._files.pop(path, None)
        if fp is None:
            return
        fp.close()
