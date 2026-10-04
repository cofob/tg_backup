# tg_backup 2.4.3

Fix repeated media reconciliation on large catalogs.

- Save discovery and reference cursors after each reconciliation batch.
- Process each existing observation once instead of once per continuous sync pass.
- Keep new media discovery and reference linking unchanged.

The first pass still reconciles existing observations. Later passes only process new observations.
