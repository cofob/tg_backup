#!/bin/sh
set -eu
# Client may share wire DTOs and credential helpers, never server/archive dependencies.
tree=$(cargo tree --locked -p tg-backup-client --edges normal)
if printf '%s\n' "$tree" | grep -E '(rusqlite|grammers-|axum |zstd |tg-backup v)' ; then
  echo 'Server dependency leaked into REST client' >&2
  exit 1
fi
