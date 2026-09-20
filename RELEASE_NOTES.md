# tg_backup 2.1.0

Shared interactive archive explorer for the local CLI and standalone HTTP client.

- Run `tg-backup --dataset PATH tui` or `tg-backup-client --profile NAME tui`.
- Browse chats, forum topics, messages, archived records and object history, raw storage, and operational status.
- Search and filter records, inspect decoded metadata and original bytes, and preview retained images in compatible terminals.
- Export complete scopes across pagination as JSON, NDJSON, TXT, or HTML, with optional verified attachment downloads; download raw storage bytes and typed rows.
- Navigate with keyboard or mouse, manually refresh, and retain selection between abstraction levels.
- Authenticated read-only explorer APIs support the remote interface. Older servers retain record browsing and export with reduced capabilities.

Browsing does not migrate or modify datasets. Exports write to the machine running the TUI and require an output path. Whole-archive record export is not a filesystem backup.

Includes Linux and macOS terminal smoke tests, shared export formatting, bounded background requests and image decoding, and terminal restoration on exit.

Release assets contain static Linux clients for x86_64 and aarch64 with SHA-256 checksums. Container images are published to `ghcr.io/cofob/tg-backup` in core and FFmpeg variants.
