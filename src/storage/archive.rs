use super::{CATALOG, Select, rows::*};
use crate::{
    archive::{Archive, Capture, CommitPoint, MAX_PAYLOAD, Record},
    tl,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

impl Archive {
    pub(crate) fn ch_ingest(
        &mut self,
        schema_hash: &str,
        items: &[Capture],
        checkpoint: Option<(&str, &Value)>,
    ) -> Result<Vec<i64>> {
        let schema = self.schema(schema_hash)?;
        // Validate all input before any durable intent or epoch change.
        let decoded = items
            .iter()
            .map(|c| {
                ensure!(c.bytes.len() <= MAX_PAYLOAD, "TL payload too large");
                DateTime::from_timestamp_micros(c.observed_at)
                    .context("invalid observation time")?;
                schema.decode(&c.root_type, &c.bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut changes = vec![];
        let mut ids = vec![];
        let mut sequence = self.store.next_id::<ObservationRow>("id")?;
        let mut epoch_id = self.store.next_id::<EpochRow>("id")?;
        let mut epochs = self.store.all::<EpochRow>()?;
        let mut sizes = HashMap::<i64, u64>::new();
        let mut heads = HashMap::<String, Option<HeadRow>>::new();
        let mut replays = HashMap::new();
        let mut payloads = std::collections::HashSet::new();
        for (c, value) in items.iter().zip(decoded) {
            if let Some(replay) = &c.replay_key {
                let existing = match replays.get(replay) {
                    Some(id) => Some(*id),
                    None => self
                        .store
                        .get::<ObservationRow>("replay_key", replay.clone())?
                        .map(|o| o.id),
                };
                if let Some(id) = existing {
                    ids.push(id);
                    continue;
                }
            }
            let name = self.config.epoch.key(
                DateTime::from_timestamp_micros(c.observed_at)
                    .context("invalid observation time")?,
            );
            let reservation = (c.bytes.len() * 2
                + c.metadata.to_string().len() * 2
                + c.key.len()
                + c.source.len()
                + 8192) as u64;
            let mut selected = None;
            for epoch in epochs
                .iter_mut()
                .rev()
                .filter(|e| e.name == name && e.sealed == 0 && e.accepting != 0)
            {
                let used = if let Some(used) = sizes.get(&epoch.id) {
                    *used
                } else {
                    let db = self.store.clickhouse().context("ClickHouse storage")?;
                    let bytes = db.number(
                        format!(
                            "SELECT toInt64(sum(length(data))) AS value FROM {}",
                            db.source("blocks", &epoch.path)?
                        ),
                        vec![],
                    )?;
                    let metadata = db.number(format!("SELECT toInt64(sum(length(metadata)*2+length(key)+length(source)+8192)) AS value FROM {} WHERE epoch=?", db.source("observations", CATALOG)?), vec![json!(epoch.id)])?;
                    let journal = db.number(
                        format!(
                            "SELECT toInt64(sum(length(journal)*2)) AS value FROM {} WHERE epoch=?",
                            db.source("payloads", CATALOG)?
                        ),
                        vec![json!(epoch.id)],
                    )?;
                    let used = u64::try_from(bytes + metadata + journal)?;
                    sizes.insert(epoch.id, used);
                    used
                };
                if self.config.max_epoch_bytes == 0 || used < self.config.max_epoch_bytes {
                    selected = Some(epoch.id);
                    break;
                }
                epoch.accepting = 0;
                changes.push(Change::put(CATALOG, epoch)?);
            }
            let epoch = if let Some(id) = selected {
                id
            } else {
                let generation = epochs
                    .iter()
                    .filter(|e| e.name == name)
                    .map(|e| e.generation)
                    .max()
                    .unwrap_or(0)
                    + 1;
                let part = epochs
                    .iter()
                    .filter(|e| e.name == name)
                    .map(|e| e.part)
                    .max()
                    .unwrap_or(0)
                    + 1;
                let epoch = EpochRow {
                    id: epoch_id,
                    name: name.clone(),
                    path: format!("epochs/{name}.g{generation}.{}", uuid::Uuid::new_v4()),
                    sealed: 0,
                    generation,
                    accepting: 1,
                    part,
                };
                changes.push(Change::put(CATALOG, &epoch)?);
                epochs.push(epoch);
                let id = epoch_id;
                epoch_id = epoch_id.checked_add(1).context("epoch ID overflow")?;
                id
            };
            *sizes.entry(epoch).or_default() += reservation;
            let mut hasher = blake3::Hasher::new();
            hasher.update(schema_hash.as_bytes());
            hasher.update(c.root_type.as_bytes());
            hasher.update(&c.bytes);
            let hash = hasher.finalize().to_hex().to_string();
            if payloads.insert(hash.clone())
                && self
                    .store
                    .get::<PayloadRow>("hash", hash.clone())?
                    .is_none()
            {
                changes.push(Change::put(
                    CATALOG,
                    &PayloadRow {
                        hash: hash.clone(),
                        schema_hash: schema_hash.into(),
                        root_type: c.root_type.clone(),
                        epoch,
                        block: None,
                        offset: None,
                        length: i64::try_from(c.bytes.len())?,
                        journal: c.bytes.clone(),
                    },
                )?);
            }
            let id = sequence;
            sequence = sequence.checked_add(1).context("observation ID overflow")?;
            changes.push(Change::put(
                CATALOG,
                &ObservationRow {
                    id,
                    key: c.key.clone(),
                    kind: c.kind.clone(),
                    observed: c.observed_at,
                    source: c.source.clone(),
                    payload: hash,
                    epoch,
                    metadata: c.metadata.to_string(),
                    partial: i64::from(c.partial),
                    deleted: i64::from(c.deleted),
                    transformed: 0,
                    replay_key: c.replay_key.clone(),
                },
            )?);
            changes.push(Change::put(
                CATALOG,
                &SearchRow {
                    id,
                    text: tl::text(&value),
                },
            )?);
            let revision = c
                .metadata
                .get("revision")
                .and_then(tl::integer)
                .unwrap_or(c.observed_at);
            if !heads.contains_key(&c.key) {
                heads.insert(
                    c.key.clone(),
                    self.store.get::<HeadRow>("key", c.key.clone())?,
                );
            }
            let head = heads.get_mut(&c.key).context("head cache")?;
            if head.as_ref().is_none_or(|h| {
                (h.partial != 0 && !c.partial)
                    || (h.partial == i64::from(c.partial) && revision >= h.revision)
            }) {
                let new = HeadRow {
                    key: c.key.clone(),
                    observation: id,
                    revision,
                    partial: i64::from(c.partial),
                };
                changes.push(Change::put(CATALOG, &new)?);
                *head = Some(new);
            }
            if let Some(replay) = &c.replay_key {
                replays.insert(replay.clone(), id);
            }
            ids.push(id);
        }
        if let Some((key, value)) = checkpoint {
            if let Some(counter) = value.get("counter_key").and_then(Value::as_str) {
                changes.push(Change::put(
                    CATALOG,
                    &CheckpointRow {
                        key: counter.into(),
                        value: value["messages"].to_string(),
                    },
                )?);
            }
            changes.push(Change::put(
                CATALOG,
                &CheckpointRow {
                    key: key.into(),
                    value: value.to_string(),
                },
            )?);
        }
        self.store
            .clickhouse()
            .context("ClickHouse storage")?
            .commit(changes)?;
        Ok(ids)
    }
    pub(crate) fn ch_materialize(
        &mut self,
        hook: &mut impl FnMut(CommitPoint) -> Result<()>,
    ) -> Result<()> {
        let db = self.store.clickhouse().context("ClickHouse storage")?;
        loop {
            let columns = PayloadRow::columns()
                .iter()
                .map(|(name, _)| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(",");
            let pending: Vec<PayloadRow> = db.query(format!("SELECT {columns} FROM {} WHERE length(journal)>0 ORDER BY epoch,hash LIMIT 128", db.source("payloads", CATALOG)?), vec![])?;
            let Some(first) = pending.first() else { break };
            let epoch = self
                .store
                .get::<EpochRow>("id", first.epoch)?
                .context("epoch not found")?;
            let block_id = db.next_id::<BlockRow>(&epoch.path, "id")?;
            let mut raw = vec![];
            let mut locations = vec![];
            let mut epoch_changes = vec![];
            let mut catalog_changes = vec![];
            for mut payload in pending.into_iter().filter(|p| p.epoch == epoch.id) {
                let existing = db
                    .select::<PayloadRow>(&epoch.path, &Select::eq("hash", payload.hash.clone()))?
                    .into_iter()
                    .next();
                if let Some(stored) = existing {
                    payload.block = stored.block;
                    payload.offset = stored.offset;
                    payload.journal.clear();
                    catalog_changes.push(Change::put(CATALOG, &payload)?);
                } else {
                    payload.block = Some(block_id);
                    payload.offset = Some(i64::try_from(raw.len())?);
                    raw.extend(&payload.journal);
                    let schema = self
                        .store
                        .get::<SchemaRow>("hash", payload.schema_hash.clone())?
                        .context("schema not found")?;
                    epoch_changes.push(Change::put(&epoch.path, &schema)?);
                    payload.journal.clear();
                    epoch_changes.push(Change::put(&epoch.path, &payload)?);
                    locations.push(Change::put(CATALOG, &payload)?);
                }
                if raw.len() >= self.config.block_bytes {
                    break;
                }
            }
            if !raw.is_empty() {
                epoch_changes.push(Change::put(
                    &epoch.path,
                    &BlockRow {
                        id: block_id,
                        codec: "zstd".into(),
                        dictionary: None,
                        raw_size: i64::try_from(raw.len())?,
                        checksum: blake3::hash(&raw).to_hex().to_string(),
                        data: zstd::bulk::compress(&raw, self.config.compression_level)?,
                    },
                )?);
                db.commit(epoch_changes)?;
                hook(CommitPoint::EpochCommitted)?;
            }
            catalog_changes.extend(locations);
            db.commit(catalog_changes)?;
            hook(CommitPoint::CatalogPublished)?;
        }
        for epoch in self.store.all::<EpochRow>()? {
            let mut after = db.number(
                format!(
                    "SELECT toInt64(ifNull(max(id),0)) AS value FROM {}",
                    db.source("observations", &epoch.path)?
                ),
                vec![],
            )?;
            loop {
                let mut query = Select::after("id", after, 128);
                query.filters.push(("epoch", "=", json!(epoch.id)));
                let observations = self.store.select::<ObservationRow>(&query)?;
                if observations.is_empty() {
                    break;
                }
                let mut changes = vec![];
                for row in observations {
                    after = row.id;
                    changes.push(Change::put(&epoch.path, &row)?);
                }
                db.commit(changes)?;
            }
            if epoch.accepting == 0 && epoch.sealed == 0 {
                self.store.put(&EpochRow { sealed: 1, ..epoch })?;
            }
        }
        Ok(())
    }
    pub(crate) fn ch_payload(&self, hash: &str) -> Result<Vec<u8>> {
        let p = self
            .store
            .get::<PayloadRow>("hash", hash)?
            .context("payload not found")?;
        let bytes = if p.has_journal() {
            p.journal
        } else {
            let epoch = self
                .store
                .get::<EpochRow>("id", p.epoch)?
                .context("epoch not found")?;
            let db = self.store.clickhouse().context("ClickHouse storage")?;
            let block = db
                .select::<BlockRow>(
                    &epoch.path,
                    &Select::eq("id", p.block.context("payload block missing")?),
                )?
                .into_iter()
                .next()
                .context("block not found")?;
            ensure!(block.codec == "zstd", "unsupported block codec");
            let dictionary = if let Some(id) = block.dictionary {
                let dictionary = db
                    .select::<DictionaryRow>(&epoch.path, &Select::eq("id", id))?
                    .into_iter()
                    .next()
                    .context("missing compression dictionary")?;
                ensure!(
                    blake3::hash(&dictionary.data).to_hex().as_str() == dictionary.hash,
                    "dictionary checksum mismatch"
                );
                dictionary.data
            } else {
                vec![]
            };
            let size = usize::try_from(block.raw_size)?;
            ensure!(size <= MAX_PAYLOAD * 2, "invalid compressed block size");
            let raw = zstd::bulk::Decompressor::with_dictionary(&dictionary)?
                .decompress(&block.data, size)?;
            ensure!(
                raw.len() == size && blake3::hash(&raw).to_hex().as_str() == block.checksum,
                "block checksum mismatch"
            );
            let offset = usize::try_from(p.offset.context("payload offset missing")?)?;
            let end = offset
                .checked_add(usize::try_from(p.length)?)
                .context("payload length overflow")?;
            raw.get(offset..end)
                .context("invalid payload bounds")?
                .to_vec()
        };
        let mut h = blake3::Hasher::new();
        h.update(p.schema_hash.as_bytes());
        h.update(p.root_type.as_bytes());
        h.update(&bytes);
        ensure!(
            h.finalize().to_hex().as_str() == hash,
            "payload checksum mismatch"
        );
        Ok(bytes)
    }
    pub(crate) fn ch_record(&self, id: i64) -> Result<Record> {
        let o = self
            .store
            .get::<ObservationRow>("id", id)?
            .context("observation not found")?;
        let p = self
            .store
            .get::<PayloadRow>("hash", o.payload.clone())?
            .context("payload not found")?;
        let data = self
            .schema(&p.schema_hash)?
            .decode(&p.root_type, &self.payload(&p.hash)?)?;
        let attachments = self.attachment_hashes(&data)?;
        let mut representations = vec![];
        for hash in &attachments {
            for row in self.representations_for(hash)? {
                representations.push(tg_backup_protocol::Representation {
                    original_retained: crate::media::attachment_path(&self.root, &row.original)?
                        .exists(),
                    original: row.original,
                    hash: row.hash,
                    recipe: row.recipe,
                    bytes: u64::try_from(row.bytes)?,
                });
            }
        }
        Ok(Record {
            sequence: o.id,
            key: o.key,
            kind: o.kind,
            observed_at: o.observed,
            source: o.source,
            payload_hash: p.hash,
            root_type: p.root_type,
            schema_hash: p.schema_hash,
            partial: o.partial != 0,
            deleted: o.deleted != 0,
            transformed: o.transformed != 0,
            metadata: serde_json::from_str(&o.metadata)?,
            data,
            attachments,
            representations,
        })
    }
    pub(crate) fn ch_reindex(&self) -> Result<()> {
        let db = self.store.clickhouse().context("ClickHouse storage")?;
        let mut after = 0;
        loop {
            let ids = self.observation_ids(after, 128)?;
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
                        text: tl::text(&self.record(id)?.data),
                    },
                )?);
            }
            db.commit(changes)?;
        }
        db.execute("ALTER TABLE text MATERIALIZE INDEX text_idx SETTINGS mutations_sync=2".into())
    }
    pub(crate) fn ch_storage_details(&self) -> Result<Value> {
        let db = self.store.clickhouse().context("ClickHouse storage")?;
        let count = |table| {
            db.number(
                format!(
                    "SELECT toInt64(count()) AS value FROM {}",
                    db.source(table, CATALOG)?
                ),
                vec![],
            )
        };
        let bytes = db.number("SELECT toInt64(sum(bytes_on_disk)) AS value FROM system.parts WHERE active AND database=currentDatabase()".into(), vec![])?;
        let mut epochs = BTreeMap::new();
        for epoch in self.store.all::<EpochRow>()? {
            let compressed = db.number(
                format!(
                    "SELECT toInt64(sum(length(data))) AS value FROM {}",
                    db.source("blocks", &epoch.path)?
                ),
                vec![],
            )?;
            let raw = db.number(
                format!(
                    "SELECT toInt64(sum(raw_size)) AS value FROM {}",
                    db.source("blocks", &epoch.path)?
                ),
                vec![],
            )?;
            let dictionaries = db.number(
                format!(
                    "SELECT toInt64(sum(length(data))) AS value FROM {}",
                    db.source("dictionaries", &epoch.path)?
                ),
                vec![],
            )?;
            epochs.insert(epoch.path, json!({"epoch":epoch.name,"compressed_payload_bytes":compressed,"uncompressed_payload_bytes":raw,"dictionary_bytes":dictionaries}));
        }
        let media = self.store.all::<MediaRow>()?;
        let mut media_sizes = BTreeMap::new();
        for m in media.into_iter().filter(|m| m.status == "complete") {
            if let Some(hash) = m.hash {
                media_sizes
                    .entry(hash)
                    .and_modify(|size: &mut i64| *size = (*size).max(m.offset))
                    .or_insert(m.offset);
            }
        }
        Ok(
            json!({"backend":"clickhouse","format_version":2,"storage_schema":1,"observations":count("observations")?,
            "objects":count("heads")?,"payloads":count("payloads")?,"media":count("media")?,
            "media_bytes":media_sizes.values().sum::<i64>(),"storage_bytes":bytes,"catalog_bytes":bytes,"epochs":epochs,
            "pending_payloads":db.number(format!("SELECT toInt64(count()) AS value FROM {} WHERE length(journal)>0",db.source("payloads",CATALOG)?),vec![])?}),
        )
    }
    pub(crate) fn ch_list_table(&self, table: &str) -> Result<Value> {
        fn values<T: CatalogRow>(a: &Archive) -> Result<Value> {
            let mut rows = serde_json::to_value(a.store.all::<T>()?)?;
            for row in rows.as_array_mut().context("catalog rows")? {
                for value in row.as_object_mut().context("catalog row")?.values_mut() {
                    if let Some(s) = value.as_str() {
                        *value = serde_json::from_str(s).unwrap_or(value.clone());
                    }
                }
            }
            Ok(rows)
        }
        match table {
            "jobs" => values::<JobRow>(self),
            "coverage" => values::<CoverageRow>(self),
            "media" => values::<MediaRow>(self),
            "maintenance" => values::<MaintenanceRow>(self),
            _ => anyhow::bail!("unsupported table"),
        }
    }
    pub(crate) fn ch_verify(&self) -> Result<Value> {
        let mut after = String::new();
        let mut checked = 0;
        loop {
            let query = Select {
                filters: vec![("hash", ">", json!(after))],
                order: Some("hash"),
                limit: Some(128),
            };
            let rows = self.store.select::<PayloadRow>(&query)?;
            if rows.is_empty() {
                break;
            }
            for p in rows {
                after = p.hash.clone();
                self.schema(&p.schema_hash)?
                    .decode(&p.root_type, &self.payload(&p.hash)?)?;
                checked += 1;
            }
        }
        let db = self.store.clickhouse().context("ClickHouse storage")?;
        for (table, column, target, key) in [
            ("observations", "payload", "payloads", "hash"),
            ("heads", "observation", "observations", "id"),
            ("media_refs", "media", "media", "id"),
            ("media_refs", "observation", "observations", "id"),
            ("payloads", "epoch", "epochs", "id"),
        ] {
            let bad = db.number(format!("SELECT toInt64(count()) AS value FROM {} WHERE {column} NOT IN(SELECT {key} FROM {})", db.source(table,CATALOG)?, db.source(target,CATALOG)?), vec![])?;
            ensure!(bad == 0, "dangling {table}.{column} reference");
        }
        for hash in self.complete_attachment_hashes()? {
            ensure!(
                crate::media::file_hash(&crate::media::attachment_path(&self.root, &hash)?)?
                    == hash,
                "attachment checksum mismatch"
            );
        }
        Ok(json!({"verified_payloads":checked,"integrity":"ok"}))
    }
    pub(crate) fn ch_seal_due(&mut self) -> Result<()> {
        self.materialize()?;
        let current = self.config.epoch.key(Utc::now());
        for mut epoch in self.store.all::<EpochRow>()? {
            if epoch.name != current && epoch.sealed == 0 {
                epoch.sealed = 1;
                epoch.accepting = 0;
                self.store.put(&epoch)?;
            }
        }
        Ok(())
    }
}
