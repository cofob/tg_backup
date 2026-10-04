# tg_backup 2.4.1

Fix history synchronization slowdown on large catalogs.

- Add catalog indexes for pending observations and payloads. Writable archive startup adds them to existing v2 datasets.
- Cache allocated epoch size during each ingestion batch. Nested TL records no longer repeat the same SQLite size queries.
- Keep epoch size limits and rollover behavior unchanged.

The first writable startup can take time while SQLite builds the new indexes. Existing archive data does not change.
