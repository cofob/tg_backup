from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tg_backup.backup import append_json_objects


def identity(value: object) -> object:
    return value


class AppendJsonObjectsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "export.json"

    def read_export(self) -> list[object]:
        with self.path.open(encoding="utf-8") as fp:
            result = json.load(fp)
        assert isinstance(result, list)
        return result

    def backup_paths(self) -> list[Path]:
        return sorted(self.path.parent.glob(f"{self.path.name}.corrupt*.bak"))

    def test_creates_new_json_list(self) -> None:
        append_json_objects(self.path, [{"id": 1}, {"id": 2}], default=identity)

        assert self.read_export() == [{"id": 1}, {"id": 2}]

    def test_appends_to_valid_json_list(self) -> None:
        append_json_objects(self.path, [{"id": 1}], default=identity)
        append_json_objects(self.path, [{"id": 2}], default=identity)

        assert self.read_export() == [{"id": 1}, {"id": 2}]
        assert not self.backup_paths()

    def test_repeated_batch_is_idempotent(self) -> None:
        append_json_objects(self.path, [{"id": 1}, {"id": 2}], default=identity)
        append_json_objects(self.path, [{"id": 1}, {"id": 2}], default=identity)

        assert self.read_export() == [{"id": 1}, {"id": 2}]

    def test_recovers_complete_prefix_and_discards_partial_item(self) -> None:
        corrupted_content = b'[{"id": 1},\n{"id": 2},\n{"id": '
        self.path.write_bytes(corrupted_content)

        append_json_objects(self.path, [{"id": 2}, {"id": 3}], default=identity)

        assert self.read_export() == [{"id": 1}, {"id": 2}, {"id": 3}]
        backup_paths = self.backup_paths()
        assert len(backup_paths) == 1
        assert backup_paths[0].read_bytes() == corrupted_content

    def test_recovers_after_invalid_utf8_tail(self) -> None:
        self.path.write_bytes(b'[{"id": 1},\n{"id": 2},\n' + b"\xf0\x9f")

        append_json_objects(self.path, [{"id": 2}, {"id": 3}], default=identity)

        assert self.read_export() == [{"id": 1}, {"id": 2}, {"id": 3}]

    def test_replaces_unrecoverable_content_with_current_batch(self) -> None:
        corrupted_content = '{"not": "a list"}'
        self.path.write_text(corrupted_content, encoding="utf-8")

        append_json_objects(self.path, [{"id": 1}], default=identity)

        assert self.read_export() == [{"id": 1}]
        backup_paths = self.backup_paths()
        assert len(backup_paths) == 1
        assert backup_paths[0].read_text(encoding="utf-8") == corrupted_content

    def test_uses_unique_backup_names_for_repeated_corruption(self) -> None:
        first_corrupted_content = b'[{"id": 1}'
        self.path.write_bytes(first_corrupted_content)
        append_json_objects(self.path, [{"id": 1}], default=identity)

        second_corrupted_content = b'[{"id": 1},\n{"id": 2}'
        self.path.write_bytes(second_corrupted_content)
        append_json_objects(self.path, [{"id": 2}], default=identity)

        backup_paths = self.backup_paths()
        expected_backup_contents = {
            first_corrupted_content,
            second_corrupted_content,
        }
        assert len(backup_paths) == len(expected_backup_contents)
        assert {backup_path.read_bytes() for backup_path in backup_paths} == expected_backup_contents
