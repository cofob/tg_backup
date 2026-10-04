# tg_backup 2.4.6

Fix the remaining sync I/O amplification.

- Keep small payloads in the durable catalog journal until one compression block is ready.
- Stop opening and syncing epoch files after every Telegram response.
- Index transformed-media hashes used by reconciliation.

This release includes all sync fixes from 2.4.1–2.4.5.
