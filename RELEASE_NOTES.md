# tg_backup 2.4.7

Scan all selected chat history before downloading media.

- Run one shared media pass after history and enrichment in each sync cycle.
- Reconcile media and reset deferred files immediately before that pass.
- Continue other chats after a history error. Stop before media on cancellation or the message limit.
- Keep saved history checkpoints and partial media offsets.

This release includes all sync fixes from 2.4.1–2.4.6.
