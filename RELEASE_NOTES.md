# tg_backup 2.3.0

Recover cross-datacenter downloads, avoid unsupported media conversions, and retry failed work without discarding completed data.

- Recover `AUTH_KEY_UNREGISTERED` for downloads from other Telegram datacenters by exporting/importing authorization, then retrying the original request. Preserve takeout wrapping and bound recovery attempts.
- Exclude all stickers and custom emoji from transcoding. Admit only explicitly supported image, audio and video formats using file signatures and probe results; skip documents, archives, animated images, unknown codecs and odd-sized video. Preserve originals.
- Carry FFmpeg/ffprobe stderr, exit status and processing context through the worker socket into logs and work errors. Drain diagnostics continuously with bounded memory.
- Add `retry-failed --target all|work|attachments`, with a read-only preview by default and atomic changes with `--apply`. Retry only failed records, preserving successful results, partial downloads and deduplication. Both bulk retry and `work --resume` reset the attempt budget.
- Automatically resume compatible unfinished sync jobs. Report progress through all Telegram sync phases.
- Add offline social graph export with date filtering.

Existing archives remain compatible; no new archive schema migration is required. Failed work is not reset automatically. See [the recovery runbook](https://github.com/cofob/tg_backup/blob/v2.3.0/docs/retry-failed.md) for the coordinated shutdown, SQLite backup, preview/apply and restart procedure. New filtering also applies when retrying old tasks. Historical abbreviated errors cannot be reconstructed.

Local validation: 95 workspace tests passed; formatting and Clippy passed. Three opt-in tests were skipped locally (live Telegram, real FFmpeg, synthetic storage workload). Release CI additionally runs the real-codec and synthetic-storage checks on Linux, builds static Linux clients for x86_64/aarch64, and smoke-tests core/FFmpeg container images before publication.

Release assets contain static Linux clients with SHA-256 checksums. Images are published to `ghcr.io/cofob/tg-backup` in core and FFmpeg variants. This release does not automatically deploy to existing installations.
