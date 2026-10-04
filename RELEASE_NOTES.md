# tg_backup 2.4.5

Fix capture I/O on large catalogs.

- Link media from the decoded capture already in memory.
- Stop rereading each new observation from the catalog before materialization.
- Keep stored records and media references unchanged.

This release includes the indexed media queue and high-water reconciliation cursors from 2.4.2–2.4.4.
