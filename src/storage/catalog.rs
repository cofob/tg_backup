use super::{CATALOG, Select, Storage, rows::*};
use crate::archive::Archive;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn merge_patch(target: &mut Value, patch: &Value) {
    if let Value::Object(patch) = patch {
        if !target.is_object() {
            *target = json!({});
        }
        let object = target.as_object_mut().expect("JSON object");
        for (key, value) in patch {
            if value.is_null() {
                object.remove(key);
            } else {
                merge_patch(object.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
    } else {
        *target = patch.clone();
    }
}

impl Archive {
    pub(crate) fn catalog_status(&self) -> Result<Value> {
        let pending = if let Some(db) = self.store.clickhouse() {
            db.number(
                format!(
                    "SELECT toInt64(count()) AS value FROM {} WHERE length(journal)>0",
                    db.source("payloads", CATALOG)?
                ),
                vec![],
            )?
        } else {
            self.store.sqlite()?.query_row(
                "SELECT COUNT(*) FROM payloads WHERE journal IS NOT NULL",
                [],
                |r| r.get(0),
            )?
        };
        Ok(
            json!({"format_version":2,"backend":self.config.backend,"observations":self.store.count::<ObservationRow>(&Select::default())?,"objects":self.store.count::<HeadRow>(&Select::default())?,"payloads":self.store.count::<PayloadRow>(&Select::default())?,"media":self.store.count::<MediaRow>(&Select::default())?,"pending_payloads":pending,"catalog_bytes":std::fs::metadata(self.root.join("catalog.sqlite3")).map(|m| m.len()).unwrap_or(0),"storage_details_included":false}),
        )
    }
    pub(crate) fn head_ids(&self) -> Result<Vec<i64>> {
        let mut ids = self
            .store
            .all::<HeadRow>()?
            .into_iter()
            .map(|r| r.observation)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }
    pub(crate) fn current_observations(
        &self,
        kind: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObservationRow>> {
        let mut rows = vec![];
        for head in self
            .store
            .all::<HeadRow>()?
            .into_iter()
            .filter(|r| prefix.is_none_or(|p| r.key.starts_with(p)))
        {
            let row = self
                .store
                .get::<ObservationRow>("id", head.observation)?
                .context("head observation missing")?;
            if row.kind == kind {
                rows.push(row);
            }
        }
        Ok(rows)
    }
    pub(crate) fn clear_checkpoints(&self, key: &str, prefix: &str) -> Result<()> {
        for row in self
            .store
            .all::<CheckpointRow>()?
            .into_iter()
            .filter(|r| r.key == key || r.key.starts_with(prefix))
        {
            self.store.delete(&row)?;
        }
        Ok(())
    }
    pub fn dataset_id(&self) -> Result<String> {
        Ok(self
            .store
            .get::<Setting>("key", "dataset_id")?
            .context("dataset ID missing")?
            .value)
    }
    pub(crate) fn observation_high_water(&self) -> Result<i64> {
        Ok(self.store.next_id::<ObservationRow>("id")? - 1)
    }
    pub(crate) fn peer(&self, key: &str) -> Result<Option<PeerRow>> {
        self.store.get("key", key)
    }
    pub(crate) fn peers(&self) -> Result<Vec<PeerRow>> {
        self.store.select(&Select {
            order: Some("key"),
            ..Default::default()
        })
    }
    pub(crate) fn peer_metadata(&self, key: &str, metadata: &Value) -> Result<()> {
        if let Some(mut row) = self.peer(key)? {
            row.metadata = metadata.to_string();
            self.store.put(&row)?;
        }
        Ok(())
    }
    pub(crate) fn folders(&self) -> Result<Vec<FolderRow>> {
        self.store.all()
    }
    pub(crate) fn set_folders(&mut self, rows: &[FolderRow]) -> Result<()> {
        let mut changes = self
            .store
            .all::<FolderRow>()?
            .iter()
            .map(|r| Change::delete(CATALOG, r))
            .collect::<Result<Vec<_>>>()?;
        for row in rows {
            changes.push(Change::put(CATALOG, row)?);
        }
        self.store.batch(changes)
    }
    pub(crate) fn job(&self, id: &str) -> Result<JobRow> {
        self.store.get("id", id)?.context("unknown job")
    }
    pub(crate) fn unfinished_jobs(&self) -> Result<Vec<JobRow>> {
        let mut jobs = self
            .store
            .all::<JobRow>()?
            .into_iter()
            .filter(|r| ["running", "paused", "failed"].contains(&r.status.as_str()))
            .collect::<Vec<_>>();
        let order: BTreeMap<String, i64> = if let Some(db) = self.store.clickhouse() {
            #[derive(serde::Deserialize, clickhouse::Row)]
            struct Order {
                id: String,
                row: i64,
            }
            db.query::<Order>(
                format!("SELECT id,_row AS row FROM {}", db.source("jobs", CATALOG)?),
                vec![],
            )?
            .into_iter()
            .map(|r| (r.id, r.row))
            .collect()
        } else {
            self.store
                .sqlite()?
                .prepare("SELECT id,rowid FROM jobs")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        jobs.sort_by(|a, b| {
            (b.updated, b.created, order.get(&b.id)).cmp(&(a.updated, a.created, order.get(&a.id)))
        });
        Ok(jobs)
    }
    pub(crate) fn start_job(&self, id: &str, config: &Value, now: i64) -> Result<()> {
        let mut job = self.store.get::<JobRow>("id", id)?.unwrap_or(JobRow {
            id: id.into(),
            created: now,
            details: "{}".into(),
            ..Default::default()
        });
        job.config = config.to_string();
        job.status = "running".into();
        job.updated = now;
        self.store.put(&job)
    }
    pub(crate) fn patch_job(
        &self,
        id: &str,
        status: Option<&str>,
        patch: &Value,
        now: i64,
    ) -> Result<()> {
        let Some(mut row) = self.store.get::<JobRow>("id", id)? else {
            return Ok(());
        };
        let mut details: Value = serde_json::from_str(&row.details)?;
        merge_patch(&mut details, patch);
        if let Some(status) = status {
            row.status = status.into();
        }
        row.updated = now;
        row.details = details.to_string();
        self.store.put(&row)
    }
    pub(crate) fn job_progress(&self, id: &str, patch: &Value, now: i64) -> Result<()> {
        let Some(mut row) = self.store.get::<JobRow>("id", id)? else {
            return Ok(());
        };
        let mut details: Value = serde_json::from_str(&row.details)?;
        if details["phase"] != patch["phase"] {
            details["requests_finished"] = json!(0);
            details["requests_failed"] = json!(0);
            details["active_method"] = Value::Null;
        }
        merge_patch(&mut details, patch);
        details["peers_discovered"] = patch["peers_discovered"].clone();
        details["total_messages"] = Value::Null;
        row.updated = now;
        row.details = details.to_string();
        self.store.put(&row)
    }
    pub(crate) fn job_request(
        &self,
        id: &str,
        patch: &Value,
        finished: bool,
        failed: bool,
    ) -> Result<()> {
        let Some(mut row) = self.store.get::<JobRow>("id", id)? else {
            return Ok(());
        };
        let mut details: Value = serde_json::from_str(&row.details)?;
        merge_patch(&mut details, patch);
        for (key, increment) in [("requests_finished", finished), ("requests_failed", failed)] {
            if increment {
                details[key] = json!(details[key].as_i64().unwrap_or(0) + 1);
            }
        }
        row.updated = chrono::Utc::now().timestamp_micros();
        row.details = details.to_string();
        self.store.put(&row)
    }
    pub(crate) fn media(&self, id: &str) -> Result<MediaRow> {
        self.store.get("id", id)?.context("media not found")
    }
    pub(crate) fn update_media(&self, id: &str, update: impl FnOnce(&mut MediaRow)) -> Result<()> {
        let mut row = self.media(id)?;
        update(&mut row);
        self.store.put(&row)
    }
    pub(crate) fn update_media_where(
        &mut self,
        select: impl Fn(&MediaRow) -> bool,
        update: impl Fn(&mut MediaRow),
    ) -> Result<()> {
        let mut changes = vec![];
        for mut row in self.store.all::<MediaRow>()?.into_iter().filter(select) {
            update(&mut row);
            changes.push(Change::put(CATALOG, &row)?);
        }
        self.store.batch(changes)
    }
    pub(crate) fn next_media(&self, before: i64) -> Result<Option<MediaRow>> {
        match &self.store {
            Storage::Sqlite(db) => {
                let mut statement = db.prepare("SELECT * FROM media WHERE status!='complete' AND status!='unavailable' AND retry_at<=?1 ORDER BY attempts,id LIMIT 1")?;
                let mut rows = statement.query([before])?;
                let Some(row) = rows.next()? else {
                    return Ok(None);
                };
                Ok(Some(MediaRow {
                    id: row.get(0)?,
                    location: row.get(1)?,
                    dc: row.get(2)?,
                    size: row.get(3)?,
                    status: row.get(4)?,
                    offset: row.get(5)?,
                    hash: row.get(6)?,
                    error: row.get(7)?,
                    attempts: row.get(8)?,
                    retry_at: row.get(9)?,
                }))
            }
            Storage::ClickHouse(db) => {
                let columns = MediaRow::columns()
                    .iter()
                    .map(|(name, _)| format!("\"{name}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(db.query::<MediaRow>(format!("SELECT {columns} FROM {} WHERE status NOT IN ('complete','unavailable') AND retry_at<=? ORDER BY attempts,id LIMIT 1", db.source("media",CATALOG)?),vec![json!(before)])?.into_iter().next())
            }
        }
    }
    pub(crate) fn media_contexts(&self, id: &str) -> Result<Vec<String>> {
        let references = self.store.select::<MediaRefRow>(&Select::eq("media", id))?;
        let mut contexts = BTreeSet::new();
        for reference in references {
            if let Some(row) = self
                .store
                .get::<ObservationRow>("id", reference.observation)?
            {
                contexts.insert(row.metadata);
            }
        }
        Ok(contexts.into_iter().collect())
    }
    pub(crate) fn media_observations(&self, hash: &str) -> Result<Vec<(String, i64)>> {
        let mut references = BTreeSet::new();
        for media in self.store.select::<MediaRow>(&Select::eq("hash", hash))? {
            for r in self
                .store
                .select::<MediaRefRow>(&Select::eq("media", media.id.clone()))?
            {
                references.insert((media.id.clone(), r.observation));
            }
        }
        Ok(references.into_iter().collect())
    }
    pub(crate) fn complete_attachment_hashes(&self) -> Result<BTreeSet<String>> {
        let mut hashes = self
            .store
            .all::<MediaRow>()?
            .into_iter()
            .filter(|r| r.status == "complete")
            .filter_map(|r| r.hash)
            .collect::<BTreeSet<_>>();
        hashes.extend(
            self.store
                .all::<RepresentationRow>()?
                .into_iter()
                .map(|r| r.hash),
        );
        Ok(hashes)
    }
    pub(crate) fn has_attachment(&self, hash: &str) -> Result<bool> {
        Ok(self
            .store
            .select::<MediaRow>(&Select::eq("hash", hash))?
            .iter()
            .any(|r| r.status == "complete")
            || !self
                .store
                .select::<RepresentationRow>(&Select::eq("hash", hash))?
                .is_empty())
    }
    pub(crate) fn representations_for(&self, hash: &str) -> Result<Vec<RepresentationRow>> {
        let mut rows = BTreeMap::new();
        for row in self
            .store
            .select::<RepresentationRow>(&Select::eq("original", hash))?
            .into_iter()
            .chain(
                self.store
                    .select::<RepresentationRow>(&Select::eq("hash", hash))?,
            )
        {
            rows.insert(row.key(), row);
        }
        Ok(rows.into_values().collect())
    }
    pub(crate) fn transcode_candidates(&self, after: &str) -> Result<Vec<String>> {
        let representations = self
            .store
            .all::<RepresentationRow>()?
            .into_iter()
            .map(|r| r.hash)
            .collect::<BTreeSet<_>>();
        Ok(self
            .store
            .all::<MediaRow>()?
            .into_iter()
            .filter(|r| r.status == "complete")
            .filter_map(|r| r.hash)
            .filter(|hash| hash.as_str() > after && !representations.contains(hash))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(32)
            .collect())
    }
    pub(crate) fn work_row(&self, sequence: i64) -> Result<WorkRow> {
        self.store
            .get("sequence", sequence)?
            .context("unknown work")
    }
    pub(crate) fn update_work(
        &self,
        sequence: i64,
        update: impl FnOnce(&mut WorkRow),
    ) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        let mut row = self.work_row(sequence)?;
        update(&mut row);
        self.store.put(&row)
    }
    pub(crate) fn reset_running_work(&mut self) -> Result<()> {
        let mut changes = vec![];
        for mut row in self
            .store
            .select::<WorkRow>(&Select::eq("state", "running"))?
        {
            row.state = "queued".into();
            row.error = Some("interrupted; restarting".into());
            changes.push(Change::put(CATALOG, &row)?);
        }
        self.store.batch(changes)
    }
}
