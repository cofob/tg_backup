use super::{CATALOG, rows::*};
use crate::archive::{Archive, CommitPoint, Maintenance};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

impl Archive {
    pub(crate) fn ch_maintain(
        &mut self,
        options: &Maintenance,
        hook: &mut impl FnMut(CommitPoint) -> Result<()>,
    ) -> Result<Value> {
        let root = self
            .root
            .join("staging")
            .join(format!("maintenance-{}", uuid::Uuid::new_v4()));
        let mut stage = crate::migration::snapshot(self, &root)?;
        let report = stage.maintain(options)?;
        if options.apply {
            stage.verify()?;
            hook(CommitPoint::GenerationReady)?;
            self.ch_publish_generation(&stage, false, options.retention.lossy())?;
            hook(CommitPoint::GenerationPublished)?;
        }
        drop(stage);
        fs::remove_dir_all(root)?;
        Ok(report)
    }
    pub(crate) fn ch_publish_generation(
        &mut self,
        stage: &Archive,
        reindex: bool,
        lossy: bool,
    ) -> Result<()> {
        stage.verify()?;
        let readers = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.root.join("readers.lock"))?;
        readers.lock_exclusive()?;
        let db = self.store.clickhouse().context("ClickHouse storage")?;
        if reindex {
            let mut after = 0;
            loop {
                let ids = stage.observation_ids(after, 128)?;
                if ids.is_empty() {
                    break;
                }
                let mut changes = vec![];
                for id in ids {
                    after = id;
                    changes.push(Change::put(
                        CATALOG,
                        &SearchRow {
                            id,
                            text: crate::tl::text(&stage.record(id)?.data),
                        },
                    )?);
                }
                db.commit(changes)?;
            }
            return Ok(());
        }
        let mut changes = vec![];
        let mut mapping = BTreeMap::new();
        for (next, mut epoch) in
            (self.store.next_id::<EpochRow>("id")?..).zip(stage.store.all::<EpochRow>()?)
        {
            let original = epoch.id;
            epoch.id = next;
            let path = epoch.path.clone();
            epoch.path = format!("epochs/generation-{}", uuid::Uuid::new_v4());
            for table in [
                "schemas",
                "dictionaries",
                "blocks",
                "payloads",
                "observations",
            ] {
                let mut after = 0;
                loop {
                    let mut rows = crate::migration::export(stage, &path, table, after, 128)?;
                    if rows.is_empty() {
                        break;
                    }
                    after = rows.last().context("generation rows")?.row;
                    for row in &mut rows {
                        row.scope = epoch.path.clone();
                        if row.data.get("epoch").is_some() {
                            row.data["epoch"] = json!(epoch.id);
                        }
                    }
                    db.commit(rows)?;
                }
            }
            mapping.insert(original, epoch.id);
            changes.push(Change::put(CATALOG, &epoch)?);
        }
        let high = stage.observation_high_water()?;
        let observations = stage.store.all::<ObservationRow>()?;
        let retained = observations.iter().map(|r| r.id).collect::<BTreeSet<_>>();
        let mut used = self
            .store
            .all::<ObservationRow>()?
            .into_iter()
            .filter(|r| r.id > high)
            .map(|r| r.payload)
            .collect::<BTreeSet<_>>();
        for mut row in observations {
            used.insert(row.payload.clone());
            row.epoch = mapping[&row.epoch];
            changes.push(Change::put(CATALOG, &row)?);
        }
        for mut row in stage.store.all::<PayloadRow>()? {
            ensure!(
                row.journal.is_empty(),
                "maintenance generation has pending payloads"
            );
            row.epoch = mapping[&row.epoch];
            changes.push(Change::put(CATALOG, &row)?);
        }
        for row in stage.store.all::<SchemaRow>()? {
            changes.push(Change::put(CATALOG, &row)?);
        }
        if lossy {
            if stage.store.all::<PeerRow>()?.is_empty() {
                let snapshot = stage
                    .store
                    .get::<Setting>("key", "maintenance_source_snapshot")?
                    .context("maintenance snapshot missing")?
                    .value
                    .parse::<i64>()?;
                for row in db.query::<PeerRow>(
                    format!(
                        "SELECT key,input,metadata,raw FROM {} WHERE _batch<=?",
                        db.source("peers", CATALOG)?
                    ),
                    vec![json!(snapshot)],
                )? {
                    changes.push(Change::delete(CATALOG, &row)?);
                }
            }
            for row in self
                .store
                .all::<ObservationRow>()?
                .into_iter()
                .filter(|r| r.id <= high && !retained.contains(&r.id))
            {
                changes.push(Change::delete(CATALOG, &row)?);
                changes.push(Change::delete(
                    CATALOG,
                    &SearchRow {
                        id: row.id,
                        text: String::new(),
                    },
                )?);
            }
            for row in self
                .store
                .all::<MediaRefRow>()?
                .into_iter()
                .filter(|r| r.observation <= high && !retained.contains(&r.observation))
            {
                changes.push(Change::delete(CATALOG, &row)?);
            }
            for row in stage.store.all::<HeadRow>()? {
                if self
                    .store
                    .get::<HeadRow>("key", row.key.clone())?
                    .is_none_or(|r| r.observation <= high)
                {
                    changes.push(Change::put(CATALOG, &row)?);
                }
            }
            let media = self
                .store
                .all::<MediaRefRow>()?
                .into_iter()
                .filter(|r| r.observation > high || retained.contains(&r.observation))
                .map(|r| r.media)
                .collect::<BTreeSet<_>>();
            for row in self
                .store
                .all::<MediaRow>()?
                .into_iter()
                .filter(|r| !media.contains(&r.id))
            {
                changes.push(Change::delete(CATALOG, &row)?);
            }
            for id in retained {
                changes.push(Change::put(
                    CATALOG,
                    &SearchRow {
                        id,
                        text: crate::tl::text(&stage.record(id)?.data),
                    },
                )?);
            }
        }
        for row in self
            .store
            .all::<PayloadRow>()?
            .into_iter()
            .filter(|r| !used.contains(&r.hash))
        {
            changes.push(Change::delete(CATALOG, &row)?);
        }
        let live_epochs = mapping
            .values()
            .copied()
            .chain(
                self.store
                    .all::<ObservationRow>()?
                    .into_iter()
                    .filter(|r| r.id > high)
                    .map(|r| r.epoch),
            )
            .chain(
                self.store
                    .all::<PayloadRow>()?
                    .into_iter()
                    .filter(|p| {
                        used.contains(&p.hash)
                            && stage
                                .store
                                .get::<PayloadRow>("hash", p.hash.clone())
                                .ok()
                                .flatten()
                                .is_none()
                    })
                    .map(|p| p.epoch),
            )
            .collect::<BTreeSet<_>>();
        for row in self
            .store
            .all::<EpochRow>()?
            .into_iter()
            .filter(|r| !live_epochs.contains(&r.id))
        {
            changes.push(Change::delete(CATALOG, &row)?);
        }
        changes.push(Change::put(
            CATALOG,
            &CheckpointRow {
                key: "maintenance_generation".into(),
                value: json!(uuid::Uuid::new_v4().to_string()).to_string(),
            },
        )?);
        changes.push(Change::put(
            CATALOG,
            &MaintenanceRow {
                id: uuid::Uuid::new_v4().to_string(),
                at: chrono::Utc::now().timestamp_micros(),
                policy: json!({"lossy":lossy}).to_string(),
                report: json!({"high_water":high}).to_string(),
            },
        )?);
        db.commit(changes)?;
        self.verify()?;
        if lossy {
            self.gc_attachments()?;
        }
        // Readers are excluded and all old cursors have been invalidated.
        db.prune(
            &self
                .store
                .all::<EpochRow>()?
                .into_iter()
                .map(|r| r.path)
                .collect::<Vec<_>>(),
        )?;
        Ok(())
    }
}
