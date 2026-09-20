#!/bin/sh
set -eu
volume="tg-backup-smoke-$$"
docker volume create "$volume" >/dev/null
trap 'docker volume rm "$volume" >/dev/null' EXIT
test "$(docker image inspect --format '{{.Config.User}}' tg-backup:smoke)" = "10001:10001"
docker run --rm --entrypoint sh tg-backup:smoke -c 'test "$(id -u):$(id -g)" = 10001:10001 && test ! -w / && test -w /data && test -w /control'
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive setup --non-interactive --skip-login --no-secure-storage
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive status --json
docker run --rm -v "$volume:/data" tg-backup:smoke --dataset /data/archive maintenance verify
docker run --rm --entrypoint ffmpeg tg-backup:smoke -hide_banner -version
