# tg_backup 2.4.2

Fix media synchronization slowdown on large catalogs.

- Add an ordered partial index for media that needs download.
- Stop each media chunk from scanning and sorting the full media catalog.
- Keep retry order and download behavior unchanged.

The first writable startup can take time while SQLite builds the new index. Existing archive data does not change.
