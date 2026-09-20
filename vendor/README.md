# TUI dependency compatibility patches

These source copies preserve upstream licenses and are excluded from workspace members.

- `ratatui-image` 10.0.1 (MIT): update dependency requirements: rand 0.8 → 0.10,
  rustix 0.38 → 1, thiserror 1 → 2. Replace the deprecated Ratatui cell skip setter with `CellDiffOption::Skip`.
  On Unix, bound capability probing with poll and finish it synchronously. This
  prevents a timed-out background stdin reader from swallowing the first TUI input.
  This release uses the dependency-free icy_sixel 0.1 encoder and supports Rust 1.88;
  newer releases pull a quantizer requiring Rust 1.90.
- `kasuari` 0.4.12 (MIT/Apache-2.0): update hashbrown 0.16 → 0.17, aligning with
  Ratatui and rusqlite. No source changes.

Only library sources, manifests, build script, README, and licenses are retained.
App/example/dev-test manifest entries are omitted. No dependency-policy exceptions
are added. Remove these patches when upstream dependency requirements converge.
