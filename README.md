# tg_backup v2

A Rust Telegram cloud archive. Native Telegram TL payloads are stored in zstd-compressed blocks inside **separate SQLite databases for each epoch**. Attachments are stored as deduplicated files. The archive retains observed object versions, observations, presence/read-state changes, and authoritative deletion events.

V2 starts a new dataset. It does not import v1 backups or reuse Python/Kurigram sessions. V1 is available in Git history.

## Build and start

Requires Rust 1.88 or newer and a C/C++ toolchain. CI uses Rust 1.98.1 on Linux and macOS. SQLite and zstd are bundled dependencies.

```sh
cargo build --locked --release --workspace --bins
export PATH="$PWD/target/release:$PATH"

tg-backup --dataset ./dataset init
export TG_BACKUP_API_ID=123456
export TG_BACKUP_API_HASH=your_api_hash
export TG_BACKUP_PHONE=+1234567890

tg-backup --dataset ./dataset auth
tg-backup --dataset ./dataset sync --takeout
```

Obtain API credentials from [my.telegram.org](https://my.telegram.org/). Login prompts for the Telegram code and, if required, a 2FA password. Password entry is masked; `TG_BACKUP_PASSWORD` is also accepted. Authentication/session secrets are stored in `session.sqlite3`, separately from queryable archive data.

```sh
tg-backup sync --continuous
tg-backup sync --max-messages 10000 --max-seconds 3600
tg-backup sync --max-media-bytes 1073741824
tg-backup jobs
tg-backup sync --resume JOB_ID
tg-backup abort-takeout JOB_ID
```

Limits apply per invocation, including when resuming a job. Saved selectors, history cursors, media offsets, retry deadlines, and Telegram update checkpoints survive restarts. Completed backfills are reused by new jobs with the same date/ID boundaries. One-off runs stop; continuous runs receive live updates and periodically audit existing data.

`--since` and `--until` are Unix seconds; `--until` is exclusive. `--min-id` and `--max-id` are Telegram history RPC bounds. Job status distinguishes completion, completion with gaps, pausing, and failure. Details include the stop reason and resume command. A takeout initialization delay is recorded with its retry time; expired takeouts are replaced on explicit resume. Limited/interrupted takeouts remain resumable; `abort-takeout` explicitly closes one.

## Dataset

```text
dataset/
  config.toml
  catalog.sqlite3
  session.sqlite3
  writer.lock
  readers.lock
  epochs/
    2026-09.g0001.<generation>.sqlite3
    2026-10.g0001.<generation>.sqlite3
  attachments/
    ab/abcdef...<BLAKE3 hash>
  staging/
```

Epochs use **observation time in UTC**, including for backfills and edits to older messages. Each epoch embeds its exact TL schema definitions, schema hashes/layers, compression dictionaries, blocks, and observations. SQLite stores archive bookkeeping and query projections; TL remains the authoritative representation of Telegram objects.

The catalog contains a durable ingestion journal. Captured records and consumed checkpoints commit together. Epoch writes precede publication of their catalog locations, and replay is idempotent. Queries can read journaled data before materialization. Identical payloads share storage, while A→B→A version occurrences remain distinct.

Active databases use WAL and full synchronous commits. Sealed epochs are immutable; maintenance creates verified replacement generations. A reader lock prevents replacement/deletion while an export or API request is using the previous generation. Only one writer can use a dataset. Local readers and the HTTP server may run alongside sync.

Copy the **entire dataset** while its writer is stopped to make an independent backup. Copying only the catalog or omitting live SQLite WAL files does not produce a complete backup. Attachments referenced by any retained version remain live.

## Configuration and selection

`init` writes `config.toml` with these defaults:

```toml
epoch = "monthly"                 # weekly, monthly, yearly
compression_level = 9
block_bytes = 1048576
retrain = true
index_history = false
history_selector = "true"
attachment_selector = "true"
metadata_refresh_seconds = 3600
audit_interval_seconds = 86400
max_file_bytes = 4294967296
```

History and attachment selectors are independent. Attachment-only selection discovers media without retaining message text. Override them for a job:

```sh
tg-backup sync --history-selector 'personal or folder = 5' \
  --attachment-selector 'personal and not peer = "user:123"'
tg-backup sync --history-selector 'peer = "channel:456" and topic = 789'
tg-backup sync --attachment-selector false
```

Selectors support `and`, `or`, `not`, parentheses, `=`, `!=`, `<`, `<=`, `>`, and `>=`. Strings may be JSON-quoted. Array equality means membership. Available fields include `peer`, `category`, `folder`, `contact`, `archived`, `muted`, `unread`, `message_id`, `topic`, and `date`; query selectors additionally expose `key`, `kind`, `observed_at`, and `data.<field>`.

Peer IDs have namespaces: `user:123`, `chat:123`, `channel:123`. Message keys look like `channel:123/message:456`. `personal` selects human private conversations; bots and Saved Messages have categories `bot` and `saved`.

Custom folders honor Telegram's explicit includes/excludes and category, mute, archive, and unread rules. Continuous sync follows changing membership. Deselection pauses future work and never deletes stored data. Discovery responses can contain metadata and dialog previews for unselected chats; selectors govern history collection and attachment transfer.

Best-quality photo/document representations and distinct video content are downloaded without transcoding. Smaller variants remain in TL metadata. Downloads stage resumably, verify size and content hash, and publish atomically. Duplicate files across chats share one content-addressed file. Failed files retain diagnostics and retry deadlines.

## Coverage

Collectors capture dialogs, messages/service messages, users/full profiles, chats/full info, topics, drafts, folders, contacts, blocked users, privacy/settings, session descriptions, stickers, emoji, saved media, profile photos, stories, membership, and admin events. Native response envelopes preserve fields not yet exposed by a specialized projection. Message enrichment collects referenced custom emoji, poll results, visible reaction lists, and recent accessible read-participant information.

```sh
tg-backup coverage
tg-backup status
```

Every collector records success, progress, inaccessible data, or failure. Some endpoints depend on account permissions, subscription, chat type, or Telegram's current API support. Such failures are reported and do not prevent other collectors from progressing. Archived schema decoding is independent of the currently compiled Telegram layer.

“Full history” means versions actually observed or recovered through Telegram's update history. Telegram cannot reconstruct edits that happened before collection, missing intermediate edits after update recovery expires, inaccessible participant lists, expired unobserved stories, or device-local secret chats. Missing history results never imply deletion. A full payload takes precedence over a partial/minimal snapshot for the current-object view; partial snapshots remain available in history.

Live account validation is opt-in; automated tests use deterministic fixtures. This rewrite has not been validated against a production account merely by running its offline suite.

## Queries and exports

```sh
tg-backup query --kind message --text '"project launch"'
tg-backup query --regex '(?i)invoice|receipt' --selector 'category = personal'
tg-backup history 'channel:123/message:456'
tg-backup query --all-versions --as-of 1790000000000000

tg-backup export --all-versions --format ndjson --output archive.ndjson
tg-backup export --all-versions --format json --output archive.json
tg-backup export --kind message --format html --output messages.html \
  --attachments ./exported-attachments
tg-backup export --kind message --format txt --output messages.txt
```

Observation timestamps and `--as-of` use UTC microseconds. Results include source, TL root/schema identity, full decoded TL data, attachment content hashes, and flags for partial/deleted/compacted records. Telegram ID fields are strings at the public JSON boundary. TL byte fields appear as `{"$bytes":"hex..."}`.

Current text uses contentless SQLite FTS5 indexes, avoiding a second stored copy of message text; queries use FTS5 syntax. Historical text uses the same tokenizer, with compressed scans unless `index_history = true`; run `maintenance reindex` after enabling that option. Regex uses Rust's regex syntax. Query pages enforce record/scan limits, time and response-size budgets, and return `next_cursor`, `incomplete`, and `scanned`. Pass the exact same query with `--cursor` to continue. Cursors freeze an observation high-water mark and are invalidated by generation replacement. Exports follow cursors until complete.

JSON/NDJSON retain all fields in the selected records. TXT/HTML are readable projections. Use `--all-versions` for complete retained history; the default query/export view is current objects.

## Read-only HTTP API

```sh
export TG_BACKUP_HTTP_TOKEN=choose-a-long-random-token
tg-backup serve --bind 127.0.0.1:8080

tg-backup-client --url http://127.0.0.1:8080 query --kind message --text hello
tg-backup-client history 'user:123/message:456'
tg-backup-client export --all-versions --format ndjson --output archive.ndjson \
  --attachments ./exported-attachments
tg-backup-client attachments
```

`TG_BACKUP_HTTP_TOKEN` is used by both server and client. A token is mandatory for a non-loopback bind. Use a TLS reverse proxy for access across an untrusted network.

Endpoints:

- `GET/POST /v2/query`, `GET /v2/objects`: shared query model.
- `POST /v2/messages`, `/v2/versions`, `/v2/events`: message/history/event views.
- `GET /v2/history/{key}`: an object's observed history.
- `GET /v2/status`, `/v2/jobs`, `/v2/coverage`, `/v2/attachments`.
- `GET /v2/attachments/{content_hash}`: attachment bytes.

The API opens the archive read-only. It has no Telegram mutation, sync, maintenance, arbitrary SQL, or arbitrary-file endpoints. POST is used only for read-only query bodies.

## Maintenance and retention

```sh
tg-backup maintenance verify
tg-backup maintenance seal                 # dry run
tg-backup maintenance seal --apply
tg-backup maintenance compact              # lossless repack dry run
tg-backup maintenance compact --apply
tg-backup maintenance reorganize --apply   # consolidate into yearly files
tg-backup maintenance reindex
```

Sync seals closed epochs automatically after completing a collection cycle. Manual sealing may seal the current epoch; subsequent observations create another active generation for the same period.

Sealing/repacking trains bounded-sample zstd dictionaries when enabled. Dictionaries are kept only when they save space including their own cost. Records remain independently addressable inside bounded blocks. Verification checks SQLite integrity, references, schema/payload/block/dictionary checksums, TL decoding, and completed attachment hashes.

Lossy retention is **never automatic**. Supply a TOML policy and review the dry run:

```toml
# Explicit UTC observation cutoff in microseconds; choose the intended date.
before = 1695000000000000
coalesce_observations = true
intermediate_versions = false
drop_kinds = ["presence", "read_state"]
remove_fields = ["user.status"]
```

```sh
tg-backup maintenance compact --policy retention.toml
tg-backup maintenance compact --policy retention.toml --apply
```

Coalescing removes redundant consecutive observations while retaining first/latest state. Intermediate-version retention keeps first/latest object versions and tombstones; it also discards old envelopes containing message versions. Field removal rewrites native TL and derived data. Required/identity fields and incompatible shared-flag removals are rejected. Policies use fully qualified `constructor.field` names; no fields are guessed to be “useless.” Compacted records are marked, and the catalog records the policy/report. Unreferenced attachment files are garbage-collected after publication.

## Development

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
cargo build --locked --release --workspace --bins
```

Tests exercise TL compatibility, older schemas, journal/generation crash boundaries, replay, partial snapshots, corruption, selectors, history/as-of pagination, retention, media resumption/deduplication, FTS/scan parity, and local/HTTP parity. Authentication and live sync require a separate explicit smoke test:

```sh
# Use a disposable v2 dataset authenticated with tg-backup auth first.
TG_BACKUP_SMOKE_DATASET=./smoke-dataset cargo test --test live_telegram -- --ignored
cargo test --test archive_v2 synthetic_archive_storage_report -- --ignored --nocapture
```

`status --details` reports compressed payloads, dictionaries, deduplicated media bytes, and SQLite page usage by table/index. The 10,000-message synthetic fixture measured 6,876,000 raw TL bytes compressed to 51,319 bytes, with a 7,520,256-byte catalog and 4,481,024-byte epoch database. These are synthetic figures, not expected ratios for real accounts; metadata/index overhead is currently much larger than compressed payloads. Archive scans and compression use bounded batches. Grammers currently uses an unbounded live update queue to avoid dropping updates; sustained ingestion lag can grow memory, so that transport path still needs production load validation.

The Docker image contains both Rust binaries and runs as an unprivileged user. Mount a writable dataset directory at `/data`. GitHub Actions checks Linux/macOS and retains the repository's container publishing workflow.


## Guided setup and continuous operation

```sh
tg-backup --dataset ./dataset setup --no-secure-storage
tg-backup --dataset ./dataset run
tg-backup status --watch
tg-backup status --json
tg-backup work --limit 50
tg-backup work --resume 12
```

`setup` reviews configuration before saving, masks secrets, and offers Telegram login. `--non-interactive --skip-login` initializes defaults for automation. `--no-secure-storage` explicitly uses private files; on Linux the interactive default is Secret Service, with no silent downgrade if it is unavailable. Existing datasets and sessions are never overwritten. API credentials live in `auth.toml` with a secret reference; Telegram session keys remain in `session.sqlite3`. Neither is exported.

`run` starts continuous capture, the queue coordinator, and the read-only API, plus metrics when configured. `sync` also runs the work coordinator. For a stopped archive, `worker --once` drains currently eligible work; `worker` continues polling. Status shows recent sync jobs, scanned messages and media bytes, coverage/media states, queue waiting reasons, worker heartbeat age, resource enforcement, and local schedule eligibility. Unknown totals remain unknown. `--watch` defaults to two seconds; piped watch output is NDJSON. `/v2/status` and `/v2/work?after=0&limit=50` expose the same read-only views. `--details` opts into expensive storage accounting.

## Boolean attachment filters

`mine` means the message was sent by the account (`outgoing = true`). `personal` means a private human chat; bots and Saved Messages are separate categories. Ownership metadata is inherited by nested attachments, including when message history is deselected.

```sh
# All my sent attachments, plus every attachment in personal chats.
tg-backup export --selector 'mine or personal' --all-versions --attachments ./files --output archive.ndjson
tg-backup-client export --selector 'outgoing = true or category = personal' --all-versions --attachments ./files --output archive.ndjson
# The same union can select downloads independently of message history.
tg-backup sync --attachment-selector 'mine or personal'
```

Combine predicates with `and`, `or`, `not`, parentheses, and comparisons. For example `(mine or personal) and date >= 1704067200`. Selectors use message/attachment context, not the file's content hash: a shared file can be selected through any matching archived occurrence.

## Epoch size, scheduling, and transcoding

`max_epoch_bytes = 4294967296` is the default 4 GiB rollover target. Zero disables size rollover. Size includes logical SQLite allocation and pending record reservations, with conservative rollover before publication; WAL and a final bounded record/block may exceed the target. Catalog and attachment files are separate. Metadata-only growth also rolls over. Replacement generations and consolidation split oversized output into parts.

Example additions to `config.toml` (the setup wizard writes these tables):

```toml
[resources]
cpus = 2
memory_bytes = 2147483648
nice = 10
# cgroup = "/sys/fs/cgroup/tg-backup-worker"  # optional delegated cgroup v2

[[schedule.windows]]
days = [1, 2, 3, 4, 5, 6, 7]
start = "02:00"
end = "06:00"

[transcode]
enabled = true
selector = "mine or personal"
probe_selector = "true" # e.g. width <= 3840 and codec != av1
kinds = ["photo", "audio", "video"]
min_age_days = 365
max_video_bytes = 1073741824
max_video_seconds = 600.0
photo_quality = 82
video_crf = 32
video_preset = 6
replace_originals = false
```

No windows means all day. Weekdays are ISO Monday=1; overnight windows belong to their start day. The schedule follows machine-local time at runtime, including DST. Closing a window finishes the active item, then prevents new automatic work. Manual maintenance starts immediately under the same resource budget. One expensive worker runs at a time. Cgroup v2 or the Compose worker container provides hard CPU/memory limits; otherwise threads, nice priority and supported process limits provide best-effort enforcement, reported explicitly in status. Live update capture is outside this queue.

Transcoding is optional and requires FFmpeg/ffprobe. Defaults use WebP quality 82, Opus 128k stereo/48k mono, and SVT-AV1 CRF 32/preset 6 with Opus in Matroska. Outputs must decode, preserve supported stream layout/dimensions/channels/duration, and be smaller before publication. Unsupported layouts, HDR/high bit depth and rotated media are skipped with a reason. Source age is Telegram content time, not download time. Original files are kept unless `replace_originals = true`; replacement rechecks all shared references and requires reader leases to be clear. Busy readers conservatively keep the original. Native TL metadata is retained and every replacement is audited. Default jobs never transcode a derivative again.

```sh
tg-backup transcode                          # dry-run historical selection
tg-backup transcode --apply                  # enqueue historical work
tg-backup worker --once
tg-backup export --attachments ./files --media preferred
tg-backup-client export --attachments ./files --media all
tg-backup work --enqueue verify
```

`--media original|preferred|all` selects retained representations for attachment export. If an explicit lossy policy removed an original, `original` cannot export those removed bytes. Hash-based download URLs always return exactly the requested retained file, never a redirect to a derivative. The queue persists recipes, outcomes, retries, progress and stop reasons. Expensive lossless maintenance prepares a private snapshot; the coordinator merges its payload locations into the current catalog without replacing newly captured checkpoints. Explicit lossy compaction requires an exclusive writer.

## REST client configuration and credentials

The standalone `tg-backup-client` package has no Telegram, SQLite, zstd or HTTP-server dependencies. Linux AMD64/ARM64 musl builds are checked for dynamic dependencies and published with SHA-256 checksums as CI artifacts and tagged release assets.

```sh
tg-backup-client --url https://archive.example --profile home setup --no-secure-storage
tg-backup-client --profile home credential status
tg-backup-client --profile home status --watch
```

Configuration is `$XDG_CONFIG_HOME/tg-backup/client.toml`, falling back to `~/.config/tg-backup/client.toml`. `--config` overrides the location; `--profile` selects a profile. URL precedence is `--url`, `TG_BACKUP_URL`, then profile. `TG_BACKUP_HTTP_TOKEN` overrides the profile's credential reference. `setup --storage secret-service|tpm|file|environment` supports Linux Secret Service through Rust D-Bus, TPM2-sealed `systemd-creds --user` credentials (systemd 256+), mode-0600 files, or environment references. TPM support depends on installed tooling and hardware; failure never falls back to plaintext. `credential set|remove|status` manages the selected reference without printing its contents.

## Docker Compose and metrics

```sh
docker compose run --rm setup
docker compose up -d
```

Compose runs an unprivileged coordinator and a separate FFmpeg worker capped at two CPUs/2 GiB. Set `TG_BACKUP_WORKER_CPUS` and `TG_BACKUP_WORKER_MEMORY` to adjust container limits alongside the configuration budget. A mode-0600 Unix socket carries private worker requests; no Docker socket is mounted. Both services share persistent archive data, and `/etc/localtime` supplies the host timezone. Core and `-ffmpeg` images support Linux AMD64/ARM64. For local builds use `docker compose build` first.

API and metrics bind localhost by default. To access them through published container ports, configure `api_bind = "0.0.0.0:8080"` with `api_token`, and optionally `metrics_bind = "0.0.0.0:9090"`. Published host ports remain loopback-only in the example. A token reference has the form `api_token = { provider = "file", path = "/data/archive/api.token" }`. Set file permissions to 0600. `TG_BACKUP_HTTP_TOKEN_FILE` and `TG_BACKUP_METRICS_TOKEN_FILE` are also supported for mounted private secrets.

Metrics are disabled unless `metrics_bind` is set, or run `tg-backup metrics` for localhost:9090. `/metrics` uses a separate optional `metrics_token` reference or `TG_BACKUP_METRICS_TOKEN`. Scrapes read a five-second cached snapshot, with low-cardinality counts for ingestion, journal backlog, objects, media, coverage failures, queue states, representations, schedule and CPU budget. No peer IDs, message text or secrets are metric labels.

CI follows patterns from [codex-start](https://github.com/cofob/codex-start), [randd](https://github.com/cofob/randd), [fastside](https://github.com/cofob/fastside) and [incus-rpm-repo](https://github.com/cofob/incus-rpm-repo): pinned actions, minimal permissions, separate validation/artifact/publishing jobs, native architecture runners, deterministic archives and checksums. Zizmor, actionlint, ShellCheck, formatting, Clippy, tests and cargo-deny gate releases. Dependency duplicates are denied without per-package bypasses; update the checked-in lockfile deliberately and re-run `cargo deny --workspace check`.
