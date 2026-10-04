# tg_backup 2.4.4

Fix media reconciliation startup on existing large catalogs.

- Initialize reconciliation cursors at the existing catalog high-water marks.
- Reconcile only observations added after the upgrade.
- Resume from the saved batch after an interrupted pass.

Existing media references stay unchanged. New observations keep the same discovery and linking checks.
