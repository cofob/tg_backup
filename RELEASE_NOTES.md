# tg_backup 2.0.0

Rust rewrite with native Telegram TL storage, a durable ingestion journal, separate
SQLite epoch files, zstd blocks, retained object occurrences, and content-addressed
attachments. V2 creates a new dataset; v1 archive data is not migrated. The
standard-library-only `scripts/migrate_v1_session.py` tool can migrate a v1
Kurigram/Pyrogram authorization session and peer cache into an empty v2 dataset.

- Monthly/weekly/yearly epochs and a default 4 GiB size rollover target.
- Resumable and continuous collection, independent history/media selectors,
  takeout support, read-only search API, and JSON/NDJSON/TXT/HTML exports.
- `mine or personal` selects attachments from sent messages or private human chats.
- Guided setup, private credential references, Linux Secret Service and optional
  TPM2-backed client credentials.
- Optional WebP/Opus/AV1 media representations, historical age filters, durable
  work queues, local-time schedules, resource budgets, and progress/status views.
- Core and FFmpeg Docker images, a separate capped Compose worker, and optional
  authenticated Prometheus metrics.
- Standalone static Linux AMD64/ARM64 REST clients, SHA-256 checksums, Rust 1.98.1,
  pinned Actions, zizmor, and strict cargo-deny checks.

The archive preserves information that Telegram exposes and that the collector
observes. Deleted-before-backup data, inaccessible account/channel history, and
past edits not observed by the client cannot be recovered. Secret chats are
excluded. Collector coverage and failures are visible in status/coverage.

Transcoding keeps originals unless an explicit replacement policy is configured.
Physical TPM and authenticated Telegram smoke tests remain opt-in. Grammers'
live update queue requires monitoring under sustained ingestion lag; see README.
