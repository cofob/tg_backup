from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from pyrogram.file_id import FileId, FileType

from tg_backup.backup import discover_media_manifests, download_indexed_media, media_file_is_complete
from tg_backup.media_index import MediaDiscovery, MediaIndex, MediaStatus


def encoded_document_file_id(media_id: int) -> str:
    return FileId(
        file_type=FileType.DOCUMENT,
        dc_id=2,
        media_id=media_id,
        access_hash=media_id + 100,
    ).encode()


class DownloadIndexedMediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_failed_file_does_not_stop_other_downloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            failed_path = root / "a-failed.bin"
            completed_path = root / "b-completed.bin"
            index = MediaIndex(root / "media.sqlite3")
            self.addCleanup(index.close)
            index.upsert_many(
                [
                    MediaDiscovery("failed", failed_path, encoded_document_file_id(1), 2),
                    MediaDiscovery("completed", completed_path, encoded_document_file_id(2), 2),
                ]
            )

            async def handle_download(parameters: tuple[object, Path, str, bool, int, object, tuple[()]]) -> None:
                _, directory, file_name, *_ = parameters
                if "failed" in file_name:
                    raise OSError("connection reset")
                (directory / file_name).write_bytes(b"ok")

            client = Mock()
            client.handle_download = handle_download

            await download_indexed_media(client, index)

            failed = index.get("failed", failed_path)
            completed = index.get("completed", completed_path)
            assert failed is not None
            assert failed.status is MediaStatus.FAILED
            assert failed.attempt_count == 1
            assert failed.last_error is not None
            assert "connection reset" in failed.last_error
            assert completed is not None
            assert completed.status is MediaStatus.COMPLETED
            assert completed_path.read_bytes() == b"ok"

    async def test_interrupted_partial_download_is_preserved_and_not_completed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            target_path = root / "partial.bin"
            index = MediaIndex(root / "media.sqlite3")
            self.addCleanup(index.close)
            index.upsert(MediaDiscovery("partial", target_path, encoded_document_file_id(3), 2))

            async def handle_download(parameters: tuple[object, Path, str, bool, int, object, tuple[()]]) -> None:
                _, directory, file_name, *_ = parameters
                (directory / file_name).write_bytes(b"x")
                raise OSError("connection reset")

            client = Mock()
            client.handle_download = handle_download

            await download_indexed_media(client, index)

            record = index.get("partial", target_path)
            assert record is not None
            assert record.status is MediaStatus.FAILED
            assert not target_path.exists()
            assert (root / "partial.bin.incomplete.bak").read_bytes() == b"x"

    def test_zero_byte_file_is_never_treated_as_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "empty.bin"
            path.touch()

            assert media_file_is_complete(path, expected_size=None) is False

    def test_malformed_manifest_entry_does_not_block_valid_media(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest_path = root / "2026-08-w1.medias.json"
            manifest_path.write_text(
                json.dumps(
                    [
                        {"_": "Document", "file_id": "!!!!", "file_unique_id": "bad"},
                        {
                            "_": "Document",
                            "file_id": encoded_document_file_id(4),
                            "file_unique_id": "valid",
                            "file_name": "valid.bin",
                            "file_size": 2,
                            "mime_type": "application/octet-stream",
                        },
                    ]
                ),
                encoding="utf-8",
            )
            index = MediaIndex(root / "media.sqlite3")
            self.addCleanup(index.close)
            client = Mock()
            client.guess_extension.return_value = ".bin"

            discover_media_manifests(client, root, index)

            assert index.count() == 1
            stat = manifest_path.stat()
            assert index.source_needs_scan(
                manifest_path.absolute(),
                size=stat.st_size,
                modified_ns=stat.st_mtime_ns,
            )
