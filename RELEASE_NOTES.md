# tg_backup 2.5.1

Add ClickHouse as an optional storage backend. SQLite remains the default.

- Use `--backend clickhouse` with `init` or `setup`. Both backends support sync, queries, exports, the TUI, and the work queue.
- Migrate in either direction with `migrate --to sqlite|clickhouse --output TARGET`. Use `--resume` after interruption.
- Show migration progress on terminal stderr and keep JSON on stdout. Verify the target before it becomes ready and leave source files unchanged.
- Publish ClickHouse data and checkpoints with durable batch intents and commit records. Recover pending batches before new writes.
- Use native ClickHouse text indexes with Unicode tokens, case normalization, and quoted phrases. SQLite keeps FTS5 search.
- Keep Telegram sessions in local `session.sqlite3` files. Copy sessions, credentials, media, and queue state during migration.
- Add a pinned ClickHouse Compose profile and update setup, backup, and restore instructions.

Requires Rust 1.89 or newer and ClickHouse 26.8.2.7 or newer for that backend. SQLite needs no ClickHouse server. Migration needs temporary disk space for local SQLite database copies.

This release keeps the dialog-scope fix from 2.4.8 and the history-before-media order from 2.4.7.
