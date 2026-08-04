from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tg_backup.media_index import MediaDiscovery, MediaIndex, MediaStatus


class MediaIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "media.sqlite3"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_discovery_is_persisted_across_reopen(self) -> None:
        target_path = Path("photos/photo_abc.jpg")
        with MediaIndex(self.database_path) as index:
            record = index.upsert(
                MediaDiscovery(
                    media_id="stable-abc",
                    target_path=target_path,
                    file_id="downloadable-v1",
                    expected_size=123,
                )
            )

            assert record.status is MediaStatus.PENDING
            assert record.attempt_count == 0
            assert record.last_error is None

        with MediaIndex(self.database_path) as reopened_index:
            assert reopened_index.get("stable-abc", target_path) == record

    def test_attempt_and_result_transitions_are_durable(self) -> None:
        target_path = Path("videos/video_xyz.mp4")
        expected_attempt_count = 2
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("stable-xyz", target_path, "downloadable", 456))

            attempted = index.begin_attempt("stable-xyz", target_path)
            assert attempted.status is MediaStatus.PENDING
            assert attempted.attempt_count == 1

            failed = index.mark_failed("stable-xyz", target_path, "connection reset")
            assert failed.status is MediaStatus.FAILED
            assert failed.attempt_count == 1
            assert failed.last_error == "connection reset"

        with MediaIndex(self.database_path) as reopened_index:
            retried = reopened_index.begin_attempt("stable-xyz", target_path)
            assert retried.status is MediaStatus.PENDING
            assert retried.attempt_count == expected_attempt_count
            assert retried.last_error is None

            completed = reopened_index.mark_completed("stable-xyz", target_path)
            assert completed.status is MediaStatus.COMPLETED
            assert completed.attempt_count == expected_attempt_count
            assert completed.last_error is None

    def test_interrupted_attempt_remains_pending_for_next_run(self) -> None:
        target_path = Path("documents/report.pdf")
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("stable-report", target_path, "downloadable"))
            index.begin_attempt("stable-report", target_path)

        with MediaIndex(self.database_path) as reopened_index:
            assert list(reopened_index.iter_pending()) == [
                reopened_index.get("stable-report", target_path),
            ]

    def test_rediscovery_refreshes_metadata_without_losing_history(self) -> None:
        target_path = Path("audio/song.mp3")
        refreshed_size = 101
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("stable-song", target_path, "file-id-v1", 100))
            index.begin_attempt("stable-song", target_path)
            index.mark_completed("stable-song", target_path)

            refreshed = index.upsert(MediaDiscovery("stable-song", target_path, "file-id-v2", refreshed_size))
            assert refreshed.file_id == "file-id-v2"
            assert refreshed.expected_size == refreshed_size
            assert refreshed.status is MediaStatus.COMPLETED
            assert refreshed.attempt_count == 1

            without_size = index.upsert(MediaDiscovery("stable-song", target_path, "file-id-v3"))
            assert without_size.file_id == "file-id-v3"
            assert without_size.expected_size == refreshed_size
            assert without_size.status is MediaStatus.COMPLETED

    def test_refreshed_file_id_requeues_a_failed_download(self) -> None:
        target_path = Path("documents/retry.bin")
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("retry", target_path, "expired-file-id"))
            index.begin_attempt("retry", target_path)
            index.mark_failed("retry", target_path, "expired reference")

            refreshed = index.upsert(MediaDiscovery("retry", target_path, "fresh-file-id"))

            assert refreshed.status is MediaStatus.PENDING
            assert refreshed.file_id == "fresh-file-id"
            assert refreshed.last_error is None

    def test_pending_iteration_can_include_failures_and_apply_a_limit(self) -> None:
        with MediaIndex(self.database_path) as index:
            index.upsert_many(
                [
                    MediaDiscovery("c", Path("3.bin"), "file-c"),
                    MediaDiscovery("a", Path("1.bin"), "file-a"),
                    MediaDiscovery("b", Path("2.bin"), "file-b"),
                ]
            )
            index.begin_attempt("a", Path("1.bin"))
            index.mark_failed("a", Path("1.bin"), "temporary failure")
            index.begin_attempt("b", Path("2.bin"))
            index.mark_completed("b", Path("2.bin"))

            assert [record.media_id for record in index.iter_pending()] == ["c"]
            assert [record.media_id for record in index.iter_pending(include_failed=True)] == ["a", "c"]
            assert [record.media_id for record in index.iter_pending(include_failed=True, limit=1)] == ["a"]

    def test_bulk_discovery_rolls_back_as_one_transaction(self) -> None:
        valid = MediaDiscovery("valid", Path("valid.bin"), "file-valid")
        invalid = MediaDiscovery("invalid", Path("invalid.bin"), "file-invalid", expected_size=-1)

        with MediaIndex(self.database_path) as index:
            with self.assertRaises(ValueError):  # noqa: PT027 - pytest is not a project dependency
                index.upsert_many([valid, invalid])

            assert index.count() == 0

    def test_identity_and_target_path_form_a_composite_key(self) -> None:
        expected_count = 3
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("same-media", Path("one/file.bin"), "file-1"))
            index.upsert(MediaDiscovery("same-media", Path("two/file.bin"), "file-2"))
            index.upsert(MediaDiscovery("different-media", Path("one/file.bin"), "file-3"))

            assert index.count() == expected_count

    def test_completed_item_can_be_requeued_when_the_file_is_missing(self) -> None:
        target_path = Path("photos/missing.jpg")
        with MediaIndex(self.database_path) as index:
            index.upsert(MediaDiscovery("stable-missing", target_path, "file-id"))
            index.begin_attempt("stable-missing", target_path)
            index.mark_completed("stable-missing", target_path)

            pending = index.mark_pending("stable-missing", target_path, reason="downloaded file is missing")
            assert pending.status is MediaStatus.PENDING
            assert pending.last_error == "downloaded file is missing"
            assert list(index.iter_pending()) == [pending]

    def test_unknown_and_completed_items_cannot_begin_an_attempt(self) -> None:
        target_path = Path("photos/completed.jpg")
        with MediaIndex(self.database_path) as index:
            with self.assertRaises(KeyError):  # noqa: PT027 - pytest is not a project dependency
                index.begin_attempt("unknown", target_path)

            index.upsert(MediaDiscovery("completed", target_path, "file-id"))
            index.begin_attempt("completed", target_path)
            index.mark_completed("completed", target_path)
            with self.assertRaises(KeyError):  # noqa: PT027 - pytest is not a project dependency
                index.begin_attempt("completed", target_path)

    def test_manifest_fingerprint_skips_only_unchanged_sources(self) -> None:
        source_path = Path("chats/1/2026-08-w1.medias.json")
        with MediaIndex(self.database_path) as index:
            assert index.source_needs_scan(source_path, size=100, modified_ns=200)

            index.mark_source_scanned(source_path, size=100, modified_ns=200)

            assert not index.source_needs_scan(source_path, size=100, modified_ns=200)
            assert index.source_needs_scan(source_path, size=101, modified_ns=200)
            assert index.source_needs_scan(source_path, size=100, modified_ns=201)

        with MediaIndex(self.database_path) as reopened_index:
            assert not reopened_index.source_needs_scan(source_path, size=100, modified_ns=200)


if __name__ == "__main__":
    unittest.main()
