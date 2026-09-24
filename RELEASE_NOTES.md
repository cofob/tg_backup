# tg_backup 2.4.0

Add per-dialog history coverage reporting and prevent expired media references from monopolizing a sync pass.

- Add `GET /v2/dialog-status` and `tg-backup-client dialog-status` for paginated, filterable per-dialog history status, last successful scan, errors and coverage gaps. Only observed dialogs appear; legacy completion records without proof of a full scan remain incomplete.
- Distinguish full, unbounded `getHistory` coverage from limited scans. Message-specific lookups and media-reference refreshes do not mark history complete.
- Preserve the normal retry backoff when a `FILE_REFERENCE_EXPIRED` refresh returns the same file location. Retry quickly only if the location changed, and select media eligible at the start of a download pass so renewed failures cannot starve other dialogs.

No archive schema migration is required. The 195 previously failed attachments are not automatically repaired; expired references may still need new Telegram data to become downloadable. Existing installations are not automatically upgraded by publishing this release.
