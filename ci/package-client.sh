#!/bin/sh
set -eu
target=${1:?target required}
binary="target/$target/release/tg-backup-client"
if readelf -l "$binary" | grep -q INTERP; then
  echo 'Client has a dynamic interpreter' >&2
  exit 1
fi
if readelf -d "$binary" | grep -q NEEDED; then
  echo 'Client has shared library dependencies' >&2
  exit 1
fi
"$binary" --help
"$binary" --version
mkdir -p dist
name="tg-backup-client-$target"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -C "target/$target/release" -cf - tg-backup-client | gzip -n > "dist/$name.tar.gz"
(cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
