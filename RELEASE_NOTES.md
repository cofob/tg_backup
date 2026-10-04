# tg_backup 2.4.8

Sync current dialogs, not every peer seen in Telegram responses.

- Keep normal and archived dialogs in the sync scope.
- Do not scan history or enrich unrelated cached channels.
- Defer queued files from unrelated chats, but keep forwarded files from current dialogs.
- Refresh the dialog scope in each continuous cycle and after resume.

This release keeps the history-before-media order from 2.4.7. Existing archive data is not removed.
