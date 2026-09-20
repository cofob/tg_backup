#!/usr/bin/env python3
"""Migrate a tg_backup v1 Kurigram/Pyrogram session into a v2 dataset.

This program intentionally uses only the Python standard library. It reads the
source session without modifying it and atomically creates the v2 session file.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import sys
import tomllib
from typing import Any
from urllib.parse import quote

try:
    import fcntl
except ImportError:  # Windows has no fcntl; no-clobber publication remains safe.
    fcntl = None


DC_ADDRESSES = {
    1: ("149.154.175.53:443", "[2001:b28:f23d:f001::a]:443"),
    2: ("149.154.167.41:443", "[2001:67c:4e8:f002::a]:443"),
    3: ("149.154.175.100:443", "[2001:b28:f23d:f003::a]:443"),
    4: ("149.154.167.92:443", "[2001:67c:4e8:f004::a]:443"),
    5: ("91.108.56.104:443", "[2001:b28:f23f:f005::a]:443"),
}
CHANNEL_MARK = 1_000_000_000_000
MAX_USER_ID = 0xFFFFFFFFFF
MAX_CHAT_ID = 999_999_999_999
MAX_CHANNEL_ID = 997_852_516_352
MIN_MONOFORUM_ID = 1_002_147_483_649
MAX_MONOFORUM_ID = 3_000_000_000_000
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class MigrationError(Exception):
    """A safe, user-facing migration failure."""


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def readonly_connection(path: Path) -> sqlite3.Connection:
    uri = "file:" + quote(str(path.resolve()), safe="/") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def require_columns(
    connection: sqlite3.Connection, table: str, required: set[str]
) -> None:
    found = columns(connection, table)
    missing = sorted(required - found)
    if missing:
        raise MigrationError(
            f"source is not a compatible v1 session: {table} lacks "
            + ", ".join(missing)
        )


def require_i64(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MigrationError(f"{label} must be an integer")
    if not I64_MIN <= value <= I64_MAX:
        raise MigrationError(f"{label} is outside Telegram's signed 64-bit range")
    return value


def peer_json(
    marked_id: Any,
    access_hash: Any,
    peer_type: Any,
    self_user_id: int,
    self_is_bot: bool,
) -> tuple[int, dict[str, Any]]:
    marked_id = require_i64(marked_id, "peer id")
    if access_hash is not None:
        access_hash = require_i64(access_hash, f"access hash for peer {marked_id}")
    if not isinstance(peer_type, str):
        raise MigrationError(f"peer {marked_id} has a non-text type")

    if peer_type in {"user", "bot"}:
        if not 1 <= marked_id <= MAX_USER_ID:
            raise MigrationError(f"{peer_type} peer {marked_id} must have a positive id")
        is_self = marked_id == self_user_id
        data = {
            "User": {
                "id": marked_id,
                "auth": access_hash,
                "bot": self_is_bot if is_self else peer_type == "bot",
                "is_self": True if is_self else None,
            }
        }
    elif peer_type == "group":
        if not -MAX_CHAT_ID <= marked_id <= -1:
            raise MigrationError(f"group peer {marked_id} has an invalid marked id")
        data = {"Chat": {"id": -marked_id}}
    elif peer_type in {"channel", "supergroup"}:
        bare_id = -marked_id - CHANNEL_MARK
        valid_channel = 1 <= bare_id <= MAX_CHANNEL_ID or (
            MIN_MONOFORUM_ID <= bare_id <= MAX_MONOFORUM_ID
        )
        if not valid_channel:
            raise MigrationError(f"{peer_type} peer {marked_id} has an invalid marked id")
        data = {
            "Channel": {
                "id": bare_id,
                "auth": access_hash,
                "kind": "Broadcast" if peer_type == "channel" else "Megagroup",
            }
        }
    else:
        raise MigrationError(f"peer {marked_id} has unsupported type {peer_type!r}")
    return marked_id, data


def read_source(path: Path) -> dict[str, Any]:
    try:
        connection = readonly_connection(path)
    except sqlite3.Error as error:
        raise MigrationError(f"cannot open source session: {error}") from error
    try:
        require_columns(
            connection,
            "sessions",
            {"dc_id", "api_id", "test_mode", "auth_key", "user_id", "is_bot"},
        )
        require_columns(connection, "peers", {"id", "access_hash", "type"})
        rows = connection.execute(
            "SELECT dc_id,api_id,test_mode,auth_key,user_id,is_bot FROM sessions"
        ).fetchall()
        if len(rows) != 1:
            raise MigrationError(
                f"source session must contain exactly one account, found {len(rows)}"
            )
        dc_id, api_id, test_mode, auth_key, user_id, is_bot = rows[0]
        dc_id = require_i64(dc_id, "dc_id")
        api_id = require_i64(api_id, "api_id")
        user_id = require_i64(user_id, "user_id")
        if dc_id not in DC_ADDRESSES:
            raise MigrationError(f"unsupported Telegram production dc_id {dc_id}")
        if not 1 <= api_id <= (1 << 31) - 1:
            raise MigrationError("source session has no valid api_id")
        if test_mode not in (0, 1):
            raise MigrationError("source session has an invalid test_mode flag")
        if test_mode == 1:
            raise MigrationError(
                "Telegram test-datacenter sessions cannot be migrated to this v2 build"
            )
        if not 1 <= user_id <= MAX_USER_ID or is_bot not in (0, 1):
            raise MigrationError("source session is not logged in")
        if not isinstance(auth_key, bytes) or len(auth_key) != 256:
            raise MigrationError("source session has no valid 256-byte authorization key")

        peers: dict[int, dict[str, Any]] = {}
        for row in connection.execute("SELECT id,access_hash,type FROM peers ORDER BY id"):
            peer_id, data = peer_json(*row, user_id, bool(is_bot))
            peers[peer_id] = data

        existing_self = peers.get(user_id, {}).get("User", {})
        peers[user_id] = {
            "User": {
                "id": user_id,
                "auth": existing_self.get("auth"),
                "bot": bool(is_bot),
                "is_self": True,
            }
        }
        return {
            "dc_id": int(dc_id),
            "api_id": int(api_id),
            "auth_key": auth_key,
            "user_id": user_id,
            "is_bot": bool(is_bot),
            "peers": peers,
        }
    except sqlite3.Error as error:
        raise MigrationError(f"cannot read source session: {error}") from error
    finally:
        connection.close()


def validate_dataset(dataset: Path, user_id: int) -> None:
    catalog = dataset / "catalog.sqlite3"
    if not catalog.is_file():
        raise MigrationError(
            f"{dataset} is not an initialized v2 dataset (catalog.sqlite3 is missing); "
            "run `tg-backup --dataset PATH setup --non-interactive --skip-login` first"
        )
    try:
        connection = readonly_connection(catalog)
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version != 2:
            raise MigrationError(f"target dataset format is {version}, expected 2")
        if "checkpoints" not in {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table'"
            )
        }:
            raise MigrationError("target catalog has no checkpoints table")
        row = connection.execute(
            "SELECT value FROM checkpoints WHERE key='account_id'"
        ).fetchone()
        if row is not None:
            try:
                archived_id = int(json.loads(row[0]))
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                raise MigrationError("target catalog has an invalid account_id") from error
            if archived_id != user_id:
                raise MigrationError(
                    f"target dataset belongs to Telegram account {archived_id}, "
                    f"but the v1 session belongs to {user_id}"
                )
    except sqlite3.Error as error:
        raise MigrationError(f"cannot validate target catalog: {error}") from error
    finally:
        if "connection" in locals():
            connection.close()


def validate_or_prepare_auth(
    dataset: Path, api_id: int, api_hash_env: str
) -> tuple[Path | None, str]:
    auth_path = dataset / "auth.toml"
    if auth_path.exists():
        try:
            with auth_path.open("rb") as stream:
                config = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise MigrationError(f"cannot read existing auth.toml: {error}") from error
        configured = config.get("api_id")
        if isinstance(configured, bool) or configured != api_id:
            raise MigrationError(
                f"existing auth.toml api_id {configured!r} does not match source {api_id}"
            )
        return None, "preserved"

    if not ENV_NAME.fullmatch(api_hash_env):
        raise MigrationError(f"invalid environment variable name {api_hash_env!r}")
    temporary = dataset / f".auth.toml.migrate-{secrets.token_hex(8)}"
    content = (
        f"api_id = {api_id}\n\n"
        "[api_hash]\n"
        'provider = "environment"\n'
        f'name = "{api_hash_env}"\n'
    )
    write_private_file(temporary, content.encode("utf-8"))
    return temporary, "created"


def write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        raise


def publish_no_replace(temporary: Path, destination: Path) -> None:
    """Atomically publish a same-filesystem file without replacing a peer."""
    try:
        os.link(temporary, destination)
    except FileExistsError as error:
        raise MigrationError(f"refusing to overwrite existing file: {destination}") from error
    temporary.unlink()


@contextmanager
def dataset_writer_lock(dataset: Path):
    lock_path = dataset / "writer.lock"
    try:
        stream = lock_path.open("r+b")
    except OSError as error:
        raise MigrationError(f"cannot open dataset writer lock: {error}") from error
    try:
        if fcntl is not None:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise MigrationError("target dataset already has a writer") from error
        yield
    finally:
        if fcntl is not None:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()


def create_session(temporary: Path, source: dict[str, Any]) -> None:
    connection = sqlite3.connect(temporary)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.executescript(
            "CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
            "CREATE TABLE peers(id INTEGER PRIMARY KEY,is_self INTEGER NOT NULL,data TEXT NOT NULL);"
        )
        dc_id = source["dc_id"]
        ipv4, ipv6 = DC_ADDRESSES[dc_id]
        dc = {
            "id": dc_id,
            "ipv4": ipv4,
            "ipv6": ipv6,
            "auth_key": source["auth_key"].hex(),
        }
        connection.executemany(
            "INSERT INTO settings(key,value) VALUES(?,?)",
            [("home_dc", compact_json(dc_id)), (f"dc:{dc_id}", compact_json(dc))],
        )
        connection.executemany(
            "INSERT INTO peers(id,is_self,data) VALUES(?,?,?)",
            [
                (peer_id, int(peer_id == source["user_id"]), compact_json(data))
                for peer_id, data in source["peers"].items()
            ],
        )
        connection.commit()
        result = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise MigrationError(f"generated v2 session failed integrity check: {result}")
    except sqlite3.Error as error:
        raise MigrationError(f"cannot create v2 session: {error}") from error
    finally:
        connection.close()
    os.chmod(temporary, 0o600)
    descriptor = os.open(temporary, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def migrate(source_path: Path, dataset: Path, api_hash_env: str) -> dict[str, Any]:
    source_path = source_path.expanduser().resolve()
    dataset = dataset.expanduser().resolve()
    if not source_path.is_file():
        raise MigrationError(f"source session does not exist: {source_path}")
    mode = stat.S_IMODE(source_path.stat().st_mode)
    if mode & 0o077:
        print(
            f"warning: source session permissions are {mode:04o}; 0600 is recommended",
            file=sys.stderr,
        )

    source = read_source(source_path)
    if not (dataset / "catalog.sqlite3").is_file():
        raise MigrationError(
            f"{dataset} is not an initialized v2 dataset (catalog.sqlite3 is missing); "
            "run `tg-backup --dataset PATH setup --non-interactive --skip-login` first"
        )
    destination = dataset / "session.sqlite3"
    with dataset_writer_lock(dataset):
        if destination.exists():
            raise MigrationError(
                f"refusing to overwrite existing v2 session: {destination}"
            )
        validate_dataset(dataset, source["user_id"])
        auth_temporary, auth_action = validate_or_prepare_auth(
            dataset, source["api_id"], api_hash_env
        )
        session_temporary = dataset / f".session.sqlite3.migrate-{secrets.token_hex(8)}"
        auth_published = False
        try:
            create_session(session_temporary, source)
            if auth_temporary is not None:
                publish_no_replace(auth_temporary, dataset / "auth.toml")
                auth_published = True
            publish_no_replace(session_temporary, destination)
            directory = os.open(dataset, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            for temporary in (session_temporary, auth_temporary):
                if temporary is not None:
                    try:
                        temporary.unlink()
                    except FileNotFoundError:
                        pass
            if auth_published and not destination.exists():
                try:
                    (dataset / "auth.toml").unlink()
                except FileNotFoundError:
                    pass
            raise

    return {
        "source": str(source_path),
        "source_preserved": True,
        "session": str(destination),
        "api_id": source["api_id"],
        "api_hash_environment": api_hash_env if auth_action == "created" else None,
        "auth_config": auth_action,
        "account_id": str(source["user_id"]),
        "is_bot": source["is_bot"],
        "dc_id": source["dc_id"],
        "peers_migrated": len(source["peers"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate a v1 Kurigram/Pyrogram authorization session to v2.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--source", required=True, type=Path, help="v1 .session file")
    parser.add_argument("--dataset", required=True, type=Path, help="v2 dataset directory")
    parser.add_argument(
        "--api-hash-env",
        default="TG_BACKUP_API_HASH",
        help="environment variable referenced by a newly-created auth.toml",
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    try:
        report = migrate(arguments.source, arguments.dataset, arguments.api_hash_env)
    except (MigrationError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(compact_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
