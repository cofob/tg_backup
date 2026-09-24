# tg_backup v2

A Rust Telegram cloud archive. Native Telegram TL payloads are stored in zstd-compressed blocks inside **separate SQLite databases for each epoch**. Attachments are stored as deduplicated files. The archive retains observed object versions, observations, presence/read-state changes, and authoritative deletion events.

V2 starts a new archive dataset and does not import v1 backup data. A standalone migration script can reuse a v1 Python/Kurigram authorization session. V1 is available in Git history.

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

### Migrate a v1 session

The dependency-free Python 3.11+ migrator transfers the Telegram authorization key and compatible peer cache from a v1 Kurigram/Pyrogram session. It does not migrate archived messages or files. Initialize an empty v2 dataset, run the migrator, then provide the API hash that belonged to the v1 application:

```sh
tg-backup --dataset ./dataset setup --non-interactive --skip-login
python3 scripts/migrate_v1_session.py \
  --source ./state/tg_backup.session \
  --dataset ./dataset
export TG_BACKUP_API_HASH=your_api_hash
tg-backup --dataset ./dataset sync
```

The API hash is not present in a v1 session, so a newly created `auth.toml` references `TG_BACKUP_API_HASH`. Use `--api-hash-env NAME` to choose another variable. Existing `auth.toml` files are preserved when their API ID matches. The script reads the source database in read-only mode, never deletes it, refuses to overwrite a v2 session, and publishes the new session atomically with mode 0600. Telegram test-datacenter sessions are rejected because this v2 build uses production datacenters. V1 did not persist update counters, so v2 obtains a fresh update state on its first connection.

```sh
tg-backup sync --continuous
tg-backup sync --max-messages 10000 --max-seconds 3600
tg-backup sync --max-media-bytes 1073741824
tg-backup jobs
tg-backup sync --resume JOB_ID
tg-backup sync --new-job
tg-backup abort-takeout JOB_ID
```

By default, `sync` (and continuous capture via `run`) resumes the most recently updated compatible unfinished job (`running`, `paused`, or `failed`). Compatibility requires matching continuous/takeout modes, date/ID boundaries, and effective history/attachment selectors, including defaults from the current configuration. If none matches, a new job is created; completed jobs are not automatically resumed. Use `--new-job` to force a new job, or `--resume JOB_ID` to select a specific job with its saved scope and mode. These two flags cannot be combined. Startup logs identify the job and whether it was resumed or created.

Limits apply per invocation, including when resuming a job: `--max-messages`, `--max-media-bytes`, and `--max-seconds` always come from the current command; omitted limits are unlimited. Saved selectors, history cursors, media offsets, retry deadlines, and Telegram update checkpoints survive restarts. Completed backfills are reused by new jobs with the same date/ID boundaries. One-off runs stop; continuous runs receive live updates and periodically audit existing data.

`--since` and `--until` are Unix seconds; `--until` is exclusive. `--min-id` and `--max-id` are Telegram history RPC bounds. Job status distinguishes completion, completion with gaps, pausing, and failure. Details include the stop reason and resume command. A takeout initialization delay is recorded with its retry time; expired takeouts are replaced on resume. Limited/interrupted takeouts remain resumable; `abort-takeout` explicitly closes one.

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

### Additional account data

These collectors run automatically during `sync` and refresh in `sync --continuous`.
They use the pinned Telegram schema and read-only user API methods. No installed
Telegram client profile or mini-app storage is read.

| Data | Export kinds | What is collected / limits |
| --- | --- | --- |
| Calls | `call` | Paginated global phone-call search, plus phone/group-call service events in collected messages. Deleted or unobserved calls cannot be recovered. |
| Stars and payments | `stars_balance`, `stars_transaction`, `stars_subscription`, `payment`, `payment_receipt`, `payment_info`, `stars_revenue` | Balance, all available transaction/subscription pages, payment/refund service events, available receipts and saved payment information. Channel finances require the relevant rights. Amounts retain their native units, currencies and fractional fields. |
| Gifts and collectibles | `gift`, `saved_gift`, `gift_collection`, `gift_event` | Saved gifts including unsaved/unique items exposed by Telegram, collections, unique-gift details, gift media and observed transfer/upgrade events. Catalog gift IDs and owned gift instances remain distinct. |
| Boosts | `boost`, `boost_status`, `boost_event` | Own boost slots, channel status, accessible ordinary/gift boost lists and observed service events. Current lists are snapshots, not an exhaustive past boost history. |
| Business | `business`, `quick_reply`, `quick_reply_message` | Business profile fields, connected bots/rights, chat links, greeting/away rules and every message returned for each quick-reply shortcut. External automation workflows are unavailable. |
| Channel statistics | `statistics` | Available channel/supergroup statistics and asynchronous graphs, requested from `stats_dc`. Returned reporting periods and graph failures are retained. |
| Locations | `location` | Geo points, venues and live locations in collected messages, plus up to 100 recent locations per selected peer. Only observed live-location revisions are retained; this is not a GPS timeline. |
| Story viewers | `story`, `story_views`, `story_viewer`, `story_reaction` | Counts and all accessible viewer pages for own stories; available reaction pages for administered channel/supergroup stories. Expired viewer lists, channel viewer identities and a complete log of stories you viewed are not available. |
| Bots and mini-apps | `bot_activity`, `bot_app` | Cloud bot/service-message activity, top peers including mini-app categories, attachment-menu bots, web authorizations, bot profiles and details of discovered apps. Local storage and internal app activity are unavailable. Bot conversations remain ordinary messages with `category = bot`. |
| Settings, contacts, drafts | existing native records / `rpc` | Existing cloud collectors remain active. `sync --takeout` also collects uploaded contacts with `contacts.getSaved`. Device-only settings/cache/contacts and unsynchronized drafts are unavailable. |
| Scheduled messages | `scheduled_message` | An uncached schedule-queue snapshot for every selected discovered peer, including Saved Messages and archived chats, plus observed scheduled updates/deletions. Completeness is per successful peer request, not a simultaneous account-wide snapshot. |

Native RPC envelopes and child objects are retained. List responses have separate
page keys, so the default current-object export does not overwrite earlier pages
of a list. Use `--all-versions` to include all retained observations; current views
show the latest observed state of each object, not a claim that every object still
exists on Telegram. An empty history search never deletes prior history. Complete
scheduled-queue snapshots additionally mark previously observed absent entries as
removed from the queue; this does not delete their retained versions. Updates
received while that snapshot request was in flight take precedence.

Scheduled keys use `user:123/scheduled_message:456` (also `chat:`/`channel:`).
Quick-reply messages use `account/quick_reply:7/message:456`. Their IDs cannot
replace ordinary `user:123/message:456` records. Explicit deletion updates retain
tombstones in their respective namespaces. File-reference refresh uses the
scheduled/quick-reply/story/gift API appropriate to the original attachment.

```sh
tg-backup sync --takeout
tg-backup export --kind scheduled_message --format json --output scheduled.json
tg-backup export --kind stars_transaction --all-versions --output stars.ndjson
tg-backup export --kind saved_gift --attachments ./gift-media --output gifts.ndjson
tg-backup export --kind call --format html --output calls.html
tg-backup export --kind quick_reply_message --format txt --output quick-replies.txt
tg-backup-client export --kind statistics --output statistics.ndjson
tg-backup coverage > coverage.json
tg-backup-client coverage
```

JSON/NDJSON include the complete decoded TL objects. TXT/HTML include structured
details for these types, including amounts, dates, coordinates and graph data.
The same kinds are queryable over HTTP and in the TUI record explorer.

Coverage entries under `extra/` include job, scope, method/progress where relevant
and completeness limits. `complete` means that the available API snapshot was
exhausted; it does not promise all historical data. `limited` describes filtered
or inherently restricted results; `inaccessible` describes permission errors;
`incomplete` includes failed requests, truncated results and repeated cursors.
`unsupported`, `requires_takeout`, `excluded`, `not_applicable`, `not_started` and
`in_progress` distinguish the remaining cases. A stopped/limited sync leaves
unvisited categories or peers visibly unfinished. These gaps are reflected in the
job status rather than silently being called a full backup.

Phone-call search shares the message budget with history. Selectors and date/ID
bounds apply to call-history messages; message selectors also apply to scheduled
snapshots and recent locations. Other account metadata is collected as before.
A new job retries unavailable data; resume continues durable list cursors, while
continuous mode starts fresh snapshots after completed scans. Enrichment work is
derived from retained objects, so a restart after a list response does not discard
pending gift, quick-reply, story-viewer or receipt requests.

Existing archives remain readable without a schema migration. New collection
cannot restore data never captured previously or already removed by Telegram.
API references: [calls and search](https://core.telegram.org/api/search),
[scheduled messages](https://core.telegram.org/api/scheduled-messages),
[stories and viewer limits](https://core.telegram.org/api/stories),
[channel statistics](https://core.telegram.org/api/stats),
[business settings](https://core.telegram.org/api/business).

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
- `GET /v2/dialog-status`: paginated per-dialog history status.
- `GET /v2/attachments/{content_hash}`: attachment bytes.

The API opens the archive read-only. It has no Telegram mutation, sync, maintenance, arbitrary SQL, or arbitrary-file endpoints. POST is used only for read-only query bodies.

`GET /v2/dialog-status?status=incomplete&type=group&limit=50` lists peers with an
archived, observed dialog record (`{peer_id}/dialog`) by `peer_id`; cache-only
contacts and message senders are excluded, even if their peer metadata exists.
This works with older archives without a metadata migration. Use `next_cursor`
as `after` for the next page. Each item has `peer_id`,
`display_name` (null when unknown), `dialog_type` (`user`, `group`, `channel`),
`history_backup_status` (`complete`, `limited`, `incomplete`, `not_started`),
`last_successful_sync_at` (Unix microseconds or null), `errors`, and `coverage_gaps`.
Filters accept those status/type values; `limit` is 1–200 (default 50). Use
`tg-backup-client dialog-status --status incomplete --type group` for client access.
Pages reflect live archive state; peers may change between requests.

`complete` requires explicitly recorded, unbounded, unfiltered `getHistory`
coverage of all applicable ranges. Legacy completion records without that evidence
are `incomplete`; bounded or selector-restricted scans are `limited`. A later failure
retains the last successful scan time. Message-specific `getMessages` lookups,
including media-reference refreshes, **never** establish complete history coverage.
Access limitations appear in `coverage_gaps`; completion only describes history
accessible through Telegram at scan time.

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

The Docker image contains both Rust binaries and defaults to the unprivileged numeric identity `10001:10001`. Mount a dataset directory writable by that identity at `/data`. GitHub Actions checks Linux/macOS and retains the repository's container publishing workflow.


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

`run` starts continuous capture, the queue coordinator, and the read-only API, plus metrics when configured. `sync` also runs the work coordinator. For a stopped archive, `worker --once` drains currently eligible work; `worker` continues polling. Status shows recent sync jobs, scanned messages and media bytes, coverage/media states, queue waiting reasons, worker heartbeat age, resource enforcement, and local schedule eligibility. Sync job details report each phase, the active Telegram RPC method, and per-phase `requests_finished` / `requests_failed` counters, including account collectors and paginated requests. Finished requests include failed requests; retries do not inflate the counters. Unknown totals remain unknown. `--watch` defaults to two seconds; piped watch output is NDJSON. `/v2/status` and `/v2/work?after=0&limit=50` expose the same read-only views. `--details` opts into expensive storage accounting.

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

Compose enforces UID/GID `10001:10001`, drops all Linux capabilities, enables `no-new-privileges`, and uses a read-only root filesystem with a private temporary filesystem. The coordinator and separate FFmpeg worker share writable archive/control volumes; the worker is capped at two CPUs/2 GiB. Set `TG_BACKUP_WORKER_CPUS` and `TG_BACKUP_WORKER_MEMORY` to adjust container limits alongside the configuration budget. A mode-0600 Unix socket carries private worker requests; no Docker socket is mounted. `/etc/localtime` supplies the host timezone. Core and `-ffmpeg` images support Linux AMD64/ARM64. For local builds use `docker compose build` first.

API and metrics bind localhost by default. To access them through published container ports, configure `api_bind = "0.0.0.0:8080"` with `api_token`, and optionally `metrics_bind = "0.0.0.0:9090"`. Published host ports remain loopback-only in the example. A token reference has the form `api_token = { provider = "file", path = "/data/archive/api.token" }`. Set file permissions to 0600. `TG_BACKUP_HTTP_TOKEN_FILE` and `TG_BACKUP_METRICS_TOKEN_FILE` are also supported for mounted private secrets.

Metrics are disabled unless `metrics_bind` is set, or run `tg-backup metrics` for localhost:9090. `/metrics` uses a separate optional `metrics_token` reference or `TG_BACKUP_METRICS_TOKEN`. Scrapes read a five-second cached snapshot, with low-cardinality counts for ingestion, journal backlog, objects, media, coverage failures, queue states, representations, schedule and CPU budget. No peer IDs, message text or secrets are metric labels.

CI follows patterns from [codex-start](https://github.com/cofob/codex-start), [randd](https://github.com/cofob/randd), [fastside](https://github.com/cofob/fastside) and [incus-rpm-repo](https://github.com/cofob/incus-rpm-repo): pinned actions, minimal permissions, separate validation/artifact/publishing jobs, native architecture runners, deterministic archives and checksums. Zizmor, actionlint, ShellCheck, formatting, Clippy, tests and cargo-deny gate releases. Dependency duplicates are denied without per-package bypasses; update the checked-in lockfile deliberately and re-run `cargo deny --workspace check`.

## Interactive archive explorer

```sh
tg-backup --dataset ./dataset tui
tg-backup-client --url http://127.0.0.1:8080 tui
tg-backup-client --profile home tui
```

Both commands use the same read-only terminal interface. The standalone client
uses its existing URL, profile and bearer-token settings; it still contains no
Telegram, SQLite, zstd or server dependencies. An interactive stdin/stdout is
required. Use the existing query/export commands in scripts and pipelines.

- **1 Chats:** browse folders, private chats, groups and channels; press `t` for
  forum topics. Message-only peers remain discoverable when dialog metadata is
  absent. Messages start with the newest Telegram message ID; `n` loads older
  pages. Topic views include the root message. The sidebar retains the current
  chat/topic list while a conversation is open.
- **2 Records:** browse every archived kind, including unknown kinds and response
  envelopes. `/` sets full-text search. `f` opens text, regex, selector, kind,
  observation-time, all-versions, peer and topic filters. `h` opens the selected
  object's history. Enter expands JSON fields; Enter on a scalar opens its full
  text, with scrolling and Esc to return.
- **3 Storage:** inspect catalog/epoch table definitions and typed rows. `s` links
  a selected record to its observation, journal/payload, archived TL schema,
  compressed block and dictionary. `d` returns to the database list. `a` lists
  attachment or text/BLOB fields; Enter opens paged hex/text inspection. Integers
  stay exact strings, SQL text stays text, and large text/BLOB values are fetched
  separately. `checkpoints` omits private takeout state. Authentication/session
  databases and configuration credentials are never exposed.
- **4 Operations:** inspect status, jobs, coverage, media, work and maintenance.
  These and physical table rows are live views; record pagination preserves its
  observation snapshot. Maintenance invalidation asks you to refresh.

Use Tab/Shift-Tab to select panes, arrows or `j`/`k` to move, Enter to open,
Esc to go back, `n`/`b` for next/previous pages, and `r` to refresh explicitly.
Mouse selection and scrolling are supported. Narrow terminals show one focused
pane. `g` opens folders, `c` opens all chats, and `?` shows help. `q` or Ctrl-C
exits and restores the terminal. No actions send Telegram messages, modify the
archive, run sync or execute maintenance.

Press `p` on an attachment to request an inline JPEG/PNG/WebP/GIF preview in a
terminal supporting Kitty, iTerm2 or Sixel graphics. Unsupported terminals,
missing files, unsupported formats and images exceeding 16 MiB/40 megapixels
show a diagnostic while keeping metadata/export available. Animated images show
the first frame. Previews decode off the rendering loop and are regenerated on
request after resize; no external viewer is launched.

### TUI exports

Press `e`, fill the form with Tab/Shift-Tab, and press Enter. Ctrl-U clears a
field. Choose `record`, `view`, `chat`, `topic`, `archive`, `row`, `table`, or
`binary`; supply a local output path. `chat` includes all topics, while
`topic`/`view` preserves the opened topic. Record/view/archive exports offer
NDJSON (default), JSON, TXT and HTML, all versions, and optional attachments
with `original`, `preferred`, or `all` representations. A selected record exports
that exact observation; enabling all versions exports its retained history.
Queries can also use `--peer channel:123 --topic 456` outside the TUI.

Exports follow every matching page, regardless of the pages loaded onscreen.
Raw row/table exports use typed JSON/NDJSON; binary exports save exact bytes
(including schema text, original TL, compressed blocks, dictionaries or media).
Raw tables are live diagnostic exports, not transactional dataset backups.
Use the documented whole-dataset backup procedure for a restorable backup.

Existing output requires typing `YES` in the overwrite field. Completed files
are published from staging files; Esc cancels and removes incomplete files.
Successfully copied attachments may remain after cancellation/failure. Attachment
hashes are verified, missing files are reported, and incomplete exports are never
reported as complete.

The additive read-only endpoints are `GET /v2/explorer/capabilities`,
`POST /v2/explorer/browse` and `POST /v2/explorer/binary`. Browse requests use the
shared typed target enum for conversations, folders, topics, messages, databases,
tables, rows, record locations and operations. Binary requests use server-resolved
references with bounded offsets/lengths. There are no arbitrary SQL or file-path
endpoints. Existing server authentication applies. Older servers retain record
browsing and existing-format exports; explorer-specific features require an
updated server. See [vendor/README.md](vendor/README.md) for the small upstream
compatibility patches needed by the dependency policy and Rust 1.88 baseline.

## Offline social graph

```sh
tg-backup --dataset ./dataset graph --focus user:123 \
  --from 2025-01-01 --to 2025-12-31 --output graph.html
```

Open the generated HTML in a browser. It is self-contained and works offline,
with no external scripts or network requests. The export contains the focal
person's entire connected component; the initial view shows immediate neighbors.
Search to recenter, expand selected nodes, filter relationship types, and use the
From/To controls to narrow the exported period. Reset restores the original
filters. The canvas displays at most 500 nodes; search and the paged neighbor
list provide access to the remaining connected nodes. Drag nodes or the background
to arrange or pan, and scroll or use the zoom buttons to zoom.

`--from` and `--to` are optional inclusive UTC calendar dates in `YYYY-MM-DD`
format. Omitted bounds are unbounded. Browser filters cannot recover evidence
outside the exported period. Messages use their original Telegram send date,
not their edit or collection date. Membership uses the participant snapshot's
observation date, **not a claim of membership throughout that day or today**.
Undated evidence is excluded whenever a date bound is active.

Edges distinguish directed private messages, replies, ID-based mentions,
message participation in groups/channels, and observed membership. Repeated
mentions within a message count once per target; membership is counted once per
person/community/observation day across overlapping snapshots. Counts use current
retained ordinary message versions, excluding deleted, scheduled and quick-reply
messages. Replies can resolve authors from messages outside the selected period.
Shared communities, forwarded authors and ambiguous username mentions do not
establish person-to-person interactions. Missing identities and unresolved reply
targets are reported. An existing person without matching edges remains an
isolated node. Incomplete participant lists and archive history limit coverage.

Graph exports contain names, exact namespaced IDs and daily relationship counts,
but no message bodies, phone numbers, attachments or credentials. Existing output
requires `--overwrite`; completed output is published atomically. Generation is
local and read-only and does not contact Telegram or change the dataset.

For atomic bulk retry of failed downloads and work, including the Docker recovery
procedure and supported conversion formats, see [Recover failed downloads and work](docs/retry-failed.md).
