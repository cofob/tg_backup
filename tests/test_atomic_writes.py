from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tg_backup.backup import BackupState, ChatExportState, dump_backup_state
from tg_backup.utils.atomic import atomic_write_bytes, atomic_write_json, atomic_write_text

EXPECTED_FSYNC_CALLS = 2
EXISTING_FILE_MODE = 0o640


class AtomicWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.path = self.directory / "state.json"

    def temporary_paths(self) -> list[Path]:
        return list(self.directory.glob(f".{self.path.name}.*.tmp"))

    def test_writes_bytes_and_removes_temporary_file(self) -> None:
        atomic_write_bytes(self.path, b"new content")

        assert self.path.read_bytes() == b"new content"
        assert self.temporary_paths() == []

    def test_writes_unicode_text(self) -> None:
        atomic_write_text(self.path, "Za\u017c\u00f3\u0142\u0107 \U0001f4be")

        assert self.path.read_text(encoding="utf-8") == "Za\u017c\u00f3\u0142\u0107 \U0001f4be"

    def test_writes_json_with_serialization_options(self) -> None:
        atomic_write_json(
            self.path,
            {"label": "\u017c\u00f3\u0142\u0107", "value": complex(2, 3)},
            ensure_ascii=False,
            indent=2,
            default=str,
            sort_keys=True,
        )

        assert json.loads(self.path.read_text(encoding="utf-8")) == {
            "label": "\u017c\u00f3\u0142\u0107",
            "value": "(2+3j)",
        }

    def test_serialization_failure_preserves_original(self) -> None:
        original = b'{"status": "original"}'
        self.path.write_bytes(original)

        with self.assertRaises(TypeError):  # noqa: PT027 -- pytest is not a project dependency.
            atomic_write_json(self.path, {"unsupported": object()})

        assert self.path.read_bytes() == original
        assert self.temporary_paths() == []

    def test_file_sync_failure_preserves_original_and_cleans_temporary_file(self) -> None:
        original = b'{"status": "original"}'
        self.path.write_bytes(original)

        with (
            patch("tg_backup.utils.atomic.os.fsync", side_effect=OSError("disk failure")),
            self.assertRaisesRegex(OSError, "disk failure"),  # noqa: PT027 -- unittest suite.
        ):
            atomic_write_bytes(self.path, b"replacement")

        assert self.path.read_bytes() == original
        assert self.temporary_paths() == []

    def test_replace_failure_preserves_original_and_cleans_temporary_file(self) -> None:
        original = b'{"status": "original"}'
        self.path.write_bytes(original)

        with (
            patch("tg_backup.utils.atomic.Path.replace", side_effect=OSError("replace failure")),
            self.assertRaisesRegex(OSError, "replace failure"),  # noqa: PT027 -- unittest suite.
        ):
            atomic_write_bytes(self.path, b"replacement")

        assert self.path.read_bytes() == original
        assert self.temporary_paths() == []

    def test_parent_directory_sync_is_best_effort(self) -> None:
        with patch("tg_backup.utils.atomic.os.fsync", side_effect=[None, OSError("unsupported")]) as fsync:
            atomic_write_bytes(self.path, b"replacement")

        assert self.path.read_bytes() == b"replacement"
        assert fsync.call_count == EXPECTED_FSYNC_CALLS
        assert self.temporary_paths() == []

    def test_replacement_preserves_existing_file_permissions(self) -> None:
        self.path.write_bytes(b"old")
        self.path.chmod(EXISTING_FILE_MODE)

        atomic_write_bytes(self.path, b"new")

        assert self.path.stat().st_mode & 0o777 == EXISTING_FILE_MODE

    def test_backup_state_keeps_previous_commit_when_replace_fails(self) -> None:
        original = b'{"chats": [{"id": 1}]}'
        self.path.write_bytes(original)
        state = BackupState(
            chats=[
                ChatExportState(
                    id=2,
                    chat_type="PRIVATE",
                    username=None,
                    qualname="New state",
                )
            ]
        )

        with (
            patch("tg_backup.utils.atomic.Path.replace", side_effect=OSError("replace failure")),
            self.assertRaisesRegex(OSError, "replace failure"),  # noqa: PT027 -- unittest suite.
        ):
            dump_backup_state(self.path, state)

        assert self.path.read_bytes() == original
