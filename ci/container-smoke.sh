#!/bin/sh
set -eu
volume="tg-backup-smoke-$$"
docker volume create "$volume" >/dev/null
trap 'docker volume rm "$volume" >/dev/null' EXIT
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive setup --non-interactive --skip-login --no-secure-storage
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive status --json
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive maintenance verify
docker run --rm --entrypoint ffmpeg tg-backup:smoke -hide_banner -version
