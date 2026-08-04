"""Crash-safe persistent index for Telegram media downloads."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Self


class MediaStatus(StrEnum):
    """Persistent state of a media download."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MediaDiscovery:
    """Download metadata learned while exporting Telegram messages.

    ``media_id`` should be Telegram's stable unique media identifier when one is
    available. ``file_id`` is the current downloadable file identifier and may
    change when the same media is encountered again.
    """

    media_id: str
    target_path: Path
    file_id: str
    expected_size: int | None = None


@dataclass(frozen=True, slots=True)
class MediaRecord:
    """A media item and its durable download state."""

    media_id: str
    target_path: Path
    file_id: str
    expected_size: int | None
    status: MediaStatus
    attempt_count: int
    last_error: str | None


class MediaIndex:
    """SQLite-backed index of media files that need to be downloaded.

    Every mutating operation commits as a separate SQLite transaction. WAL mode
    with full synchronous writes makes committed state durable, while an attempt
    interrupted between :meth:`begin_attempt` and its result remains pending and
    is returned on the next run.
    """

    _SCHEMA_VERSION = 1
    _SELECT_COLUMNS = "media_id, target_path, file_id, expected_size, status, attempt_count, last_error"

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
        """Close the underlying SQLite connection."""

        self._connection.close()

    def upsert(self, discovery: MediaDiscovery) -> MediaRecord:
        """Insert newly discovered media or refresh its download metadata.

        Existing status, attempt count, and error information are preserved.
        A missing new size does not discard a previously known size.
        """

        self._validate_discovery(discovery)
        with self._transaction():
            self._upsert(discovery)
        record = self.get(discovery.media_id, discovery.target_path)
        if record is None:  # pragma: no cover - protected by the transaction above
            raise RuntimeError("Media record disappeared immediately after insertion.")
        return record

    def upsert_many(self, discoveries: Iterable[MediaDiscovery]) -> int:
        """Upsert all discoveries atomically and return the processed count."""

        count = 0
        with self._transaction():
            for discovery in discoveries:
                self._validate_discovery(discovery)
                self._upsert(discovery)
                count += 1
        return count

    def get(self, media_id: str, target_path: Path) -> MediaRecord | None:
        """Return one indexed item, or ``None`` when it has not been discovered."""

        row = self._connection.execute(
            f"SELECT {self._SELECT_COLUMNS} FROM media_files WHERE media_id = ? AND target_path = ?",  # noqa: S608
            (media_id, self._path_value(target_path)),
        ).fetchone()
        return None if row is None else self._record_from_row(row)

    def iter_pending(
        self,
        *,
        include_failed: bool = False,
        limit: int | None = None,
    ) -> Iterator[MediaRecord]:
        """Iterate pending downloads using the status index.

        Set ``include_failed`` to retry failures from earlier runs. Records are
        ordered by target path for deterministic processing.
        """

        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative or None")

        statuses = (MediaStatus.PENDING, MediaStatus.FAILED) if include_failed else (MediaStatus.PENDING,)
        placeholders = ", ".join("?" for _ in statuses)
        sql = (
            f"SELECT {self._SELECT_COLUMNS} FROM media_files "  # noqa: S608
            f"WHERE status IN ({placeholders}) ORDER BY target_path, media_id"
        )
        parameters: tuple[str | int, ...] = tuple(statuses)
        if limit is not None:
            sql += " LIMIT ?"
            parameters = (*parameters, limit)

        cursor = self._connection.execute(sql, parameters)
        while rows := cursor.fetchmany(256):
            yield from (self._record_from_row(row) for row in rows)

    def iter_status(self, status: MediaStatus) -> Iterator[MediaRecord]:
        """Iterate all records in one status without scanning unrelated rows."""

        cursor = self._connection.execute(
            f"SELECT {self._SELECT_COLUMNS} FROM media_files "  # noqa: S608
            "WHERE status = ? ORDER BY target_path, media_id",
            (status,),
        )
        while rows := cursor.fetchmany(256):
            yield from (self._record_from_row(row) for row in rows)

    def begin_attempt(self, media_id: str, target_path: Path) -> MediaRecord:
        """Durably record the start of a download attempt.

        The item stays pending until the caller marks it completed or failed, so
        process termination during a download naturally causes a retry.
        """

        with self._transaction():
            cursor = self._connection.execute(
                """
                UPDATE media_files
                SET status = ?, attempt_count = attempt_count + 1, last_error = NULL
                WHERE media_id = ? AND target_path = ? AND status != ?
                """,
                (
                    MediaStatus.PENDING,
                    media_id,
                    self._path_value(target_path),
                    MediaStatus.COMPLETED,
                ),
            )
            self._require_updated(cursor, media_id, target_path, operation="begin an attempt for")
        return self._require_record(media_id, target_path)

    def mark_completed(self, media_id: str, target_path: Path) -> MediaRecord:
        """Mark a successfully downloaded item as complete."""

        with self._transaction():
            cursor = self._connection.execute(
                """
                UPDATE media_files
                SET status = ?, last_error = NULL
                WHERE media_id = ? AND target_path = ?
                """,
                (MediaStatus.COMPLETED, media_id, self._path_value(target_path)),
            )
            self._require_updated(cursor, media_id, target_path, operation="complete")
        return self._require_record(media_id, target_path)

    def mark_completed_many(self, keys: Iterable[tuple[str, Path]]) -> int:
        """Mark several verified files complete in one durable transaction."""

        count = 0
        with self._transaction():
            for media_id, target_path in keys:
                cursor = self._connection.execute(
                    """
                    UPDATE media_files
                    SET status = ?, last_error = NULL
                    WHERE media_id = ? AND target_path = ?
                    """,
                    (MediaStatus.COMPLETED, media_id, self._path_value(target_path)),
                )
                self._require_updated(cursor, media_id, target_path, operation="complete")
                count += 1
        return count

    def mark_failed(self, media_id: str, target_path: Path, error: str) -> MediaRecord:
        """Preserve an error and mark an unsuccessful item as failed."""

        if not error:
            raise ValueError("error must not be empty")
        with self._transaction():
            cursor = self._connection.execute(
                """
                UPDATE media_files
                SET status = ?, last_error = ?
                WHERE media_id = ? AND target_path = ?
                """,
                (MediaStatus.FAILED, error, media_id, self._path_value(target_path)),
            )
            self._require_updated(cursor, media_id, target_path, operation="fail")
        return self._require_record(media_id, target_path)

    def mark_pending(self, media_id: str, target_path: Path, *, reason: str | None = None) -> MediaRecord:
        """Queue an indexed item again, for example when its file is missing."""

        with self._transaction():
            cursor = self._connection.execute(
                """
                UPDATE media_files
                SET status = ?, last_error = ?
                WHERE media_id = ? AND target_path = ?
                """,
                (MediaStatus.PENDING, reason, media_id, self._path_value(target_path)),
            )
            self._require_updated(cursor, media_id, target_path, operation="queue")
        return self._require_record(media_id, target_path)

    def mark_pending_many(self, keys: Iterable[tuple[str, Path]], *, reason: str | None = None) -> int:
        """Queue several missing/incomplete files in one durable transaction."""

        count = 0
        with self._transaction():
            for media_id, target_path in keys:
                cursor = self._connection.execute(
                    """
                    UPDATE media_files
                    SET status = ?, last_error = ?
                    WHERE media_id = ? AND target_path = ?
                    """,
                    (MediaStatus.PENDING, reason, media_id, self._path_value(target_path)),
                )
                self._require_updated(cursor, media_id, target_path, operation="queue")
                count += 1
        return count

    def count(self, status: MediaStatus | None = None) -> int:
        """Return the number of all records or records in one state."""

        if status is None:
            row = self._connection.execute("SELECT COUNT(*) FROM media_files").fetchone()
        else:
            row = self._connection.execute(
                "SELECT COUNT(*) FROM media_files WHERE status = ?",
                (status,),
            ).fetchone()
        if row is None:  # pragma: no cover - COUNT always returns one row
            raise RuntimeError("SQLite did not return a count.")
        return int(row[0])

    def source_needs_scan(self, source_path: Path, *, size: int, modified_ns: int) -> bool:
        """Return whether a media manifest is new or changed since its last successful scan."""

        row = self._connection.execute(
            "SELECT size, modified_ns FROM media_sources WHERE source_path = ?",
            (self._path_value(source_path),),
        ).fetchone()
        return row is None or int(row["size"]) != size or int(row["modified_ns"]) != modified_ns

    def mark_source_scanned(self, source_path: Path, *, size: int, modified_ns: int) -> None:
        """Persist a manifest fingerprint after all its entries were indexed successfully."""

        if size < 0 or modified_ns < 0:
            raise ValueError("source size and modified_ns must be non-negative")
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO media_sources (source_path, size, modified_ns)
                VALUES (?, ?, ?)
                ON CONFLICT (source_path) DO UPDATE SET
                    size = excluded.size,
                    modified_ns = excluded.modified_ns
                """,
                (self._path_value(source_path), size, modified_ns),
            )

    def _configure_connection(self) -> None:
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA busy_timeout = 5000")

    def _initialize_schema(self) -> None:
        current_version_row = self._connection.execute("PRAGMA user_version").fetchone()
        if current_version_row is None:  # pragma: no cover - PRAGMA always returns a row
            raise RuntimeError("SQLite did not return a schema version.")
        current_version = int(current_version_row[0])
        if current_version > self._SCHEMA_VERSION:
            raise RuntimeError(
                f"Media index schema {current_version} is newer than supported version {self._SCHEMA_VERSION}."
            )

        with self._transaction():
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS media_files (
                    media_id TEXT NOT NULL,
                    target_path TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    expected_size INTEGER CHECK (expected_size IS NULL OR expected_size >= 0),
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'completed', 'failed')),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    last_error TEXT,
                    PRIMARY KEY (media_id, target_path)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS media_files_status_idx ON media_files (status, target_path, media_id)"
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS media_sources (
                    source_path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL CHECK (size >= 0),
                    modified_ns INTEGER NOT NULL CHECK (modified_ns >= 0)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(f"PRAGMA user_version = {self._SCHEMA_VERSION}")

    def _upsert(self, discovery: MediaDiscovery) -> None:
        self._connection.execute(
            """
            INSERT INTO media_files (media_id, target_path, file_id, expected_size)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (media_id, target_path) DO UPDATE SET
                file_id = excluded.file_id,
                expected_size = COALESCE(excluded.expected_size, media_files.expected_size),
                status = CASE
                    WHEN media_files.status = 'failed' AND media_files.file_id != excluded.file_id THEN 'pending'
                    ELSE media_files.status
                END,
                last_error = CASE
                    WHEN media_files.status = 'failed' AND media_files.file_id != excluded.file_id THEN NULL
                    ELSE media_files.last_error
                END
            """,
            (
                discovery.media_id,
                self._path_value(discovery.target_path),
                discovery.file_id,
                discovery.expected_size,
            ),
        )

    def _require_record(self, media_id: str, target_path: Path) -> MediaRecord:
        record = self.get(media_id, target_path)
        if record is None:
            raise KeyError(self._key_description(media_id, target_path))
        return record

    @staticmethod
    def _require_updated(
        cursor: sqlite3.Cursor,
        media_id: str,
        target_path: Path,
        *,
        operation: str,
    ) -> None:
        if cursor.rowcount != 1:
            key = MediaIndex._key_description(media_id, target_path)
            raise KeyError(f"Cannot {operation} unknown or completed media {key}")

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> MediaRecord:
        expected_size_raw = row["expected_size"]
        last_error_raw = row["last_error"]
        return MediaRecord(
            media_id=str(row["media_id"]),
            target_path=Path(str(row["target_path"])),
            file_id=str(row["file_id"]),
            expected_size=None if expected_size_raw is None else int(expected_size_raw),
            status=MediaStatus(str(row["status"])),
            attempt_count=int(row["attempt_count"]),
            last_error=None if last_error_raw is None else str(last_error_raw),
        )

    @staticmethod
    def _validate_discovery(discovery: MediaDiscovery) -> None:
        if not discovery.media_id:
            raise ValueError("media_id must not be empty")
        if not discovery.file_id:
            raise ValueError("file_id must not be empty")
        if discovery.expected_size is not None and discovery.expected_size < 0:
            raise ValueError("expected_size must be non-negative or None")

    @staticmethod
    def _path_value(target_path: Path) -> str:
        return str(target_path)

    @staticmethod
    def _key_description(media_id: str, target_path: Path) -> str:
        return f"({media_id!r}, {str(target_path)!r})"

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
