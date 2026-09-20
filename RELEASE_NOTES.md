# tg_backup 2.2.0

Expanded read-only Telegram exports with resumable collection and explicit coverage reporting.

- Collect call history, Stars balances and transactions, subscriptions, available payment receipts, saved payment information, gifts and collectibles, boosts, business settings and quick replies, channel statistics, shared locations, story viewers and reactions, and available bot/mini-app metadata.
- Export scheduled messages as a separate dataset. Ordinary messages, scheduled messages and quick replies use distinct identities; updates, deletions and complete schedule-queue snapshots preserve retained history without mixing these namespaces.
- Preserve every response page and atomically checkpoint pagination. Resume interrupted lists, detect repeated cursors, and retain exact native TL payloads and attachment references.
- Load channel statistics and asynchronous graphs from the appropriate datacenter. Refresh scheduled, quick-reply, story and gift attachment references through their original APIs.
- Use existing query/export commands, the HTTP client and the shared TUI for new record kinds. JSON/NDJSON retain all decoded fields; TXT/HTML include structured details for metadata-only records.
- Report unsupported local data, expired or inaccessible history, filtered results and unfinished collectors in coverage. Uploaded contacts require takeout mode.

Collectors run automatically during sync; continuous mode refreshes completed snapshots. Existing archives remain readable without migration. Completeness describes data available through the API at collection time, not all historical account activity. Device-local settings, cache, unsynchronized drafts and mini-app local storage are outside API coverage.

Validation: 70 workspace tests passed, including 14 new collector tests; formatting and Clippy passed. Live account validation remains opt-in and was not run for this change.

Release assets contain static Linux clients for x86_64 and aarch64 with SHA-256 checksums. Container images are published to `ghcr.io/cofob/tg-backup` in core and FFmpeg variants.
