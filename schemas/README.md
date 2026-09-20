# Telegram TL schemas

`telegram-layer-227.tl` is the exact `tl/api.tl` distributed with
`grammers-tl-types` 0.10.0, from the Grammers project:
https://codeberg.org/Lonami/grammers

Grammers is distributed under MIT OR Apache-2.0. Telegram describes TL at:
https://core.telegram.org/mtproto/TL

When upgrading Grammers, add the new schema rather than modifying an existing
schema. Update the compiled `API_SCHEMA` reference and test it against the
generated codecs. Archives store schema text and BLAKE3 hashes inside their
SQLite files; the runtime decoder reads those definitions, so old epochs do
not depend on retaining old generated Rust types in the current binary.
