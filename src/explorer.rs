//! Bounded read-only archive exploration shared by the HTTP and local adapters.
use crate::archive::Archive;
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::{Connection, OpenFlags, params, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};
use tg_backup_protocol::explorer::*;

const TABLES: &[&str] = &[
    "settings",
    "schemas",
    "epochs",
    "payloads",
    "observations",
    "heads",
    "checkpoints",
    "jobs",
    "coverage",
    "media",
    "media_refs",
    "maintenance",
    "retired",
    "peers",
    "folders",
    "work",
    "representations",
    "media_transformations",
    "runtime",
    "dictionaries",
    "blocks",
];
#[derive(Serialize, Deserialize)]
struct Cursor {
    dataset: String,
    generation: String,
    snapshot: i64,
    offset: i64,
    fingerprint: String,
}
fn quoted(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn scalar(v: &Value) -> Option<String> {
    v.as_str()
        .map(str::to_owned)
        .or_else(|| v.as_i64().map(|n| n.to_string()))
}
fn label(v: &Value) -> String {
    v.get("title")
        .and_then(|v| v.as_str().or_else(|| v["text"].as_str()))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            ["first_name", "last_name"]
                .iter()
                .filter_map(|k| v[*k].as_str())
                .collect::<Vec<_>>()
                .join(" ")
        })
}
fn peer_from_key(key: &str) -> Option<&str> {
    let peer = key.split('/').next()?;
    let (kind, id) = peer.split_once(':')?;
    (matches!(kind, "user" | "chat" | "channel") && id.parse::<i64>().is_ok()).then_some(peer)
}
pub(crate) fn topic_matches(record: &tg_backup_protocol::Record, topic: &str) -> bool {
    scalar(&record.metadata["topic"]).as_deref() == Some(topic)
        || scalar(&record.data["reply_to"]["reply_to_top_id"]).as_deref() == Some(topic)
        || (record.data["reply_to"]["forum_topic"] == true
            && scalar(&record.data["reply_to"]["reply_to_msg_id"]).as_deref() == Some(topic))
        || record
            .key
            .rsplit_once("/message:")
            .is_some_and(|(_, id)| id == topic)
}
impl Archive {
    pub fn explorer_capabilities(&self) -> Capabilities {
        Capabilities {
            version: 1,
            storage: true,
            conversations: true,
        }
    }
    fn explorer_generation(&self) -> Result<String> {
        Ok(self
            .checkpoint("maintenance_generation")?
            .unwrap_or(json!(0))
            .to_string())
    }
    fn explorer_database(&self, database: &str) -> Result<Connection> {
        let path = if database == "catalog" {
            self.root.join("catalog.sqlite3")
        } else {
            let id: i64 = database
                .strip_prefix("epoch:")
                .context("unknown database")?
                .parse()?;
            let path: String =
                self.db
                    .query_row("SELECT path FROM epochs WHERE id=?1", [id], |r| r.get(0))?;
            let candidate = self.root.join(path).canonicalize()?;
            ensure!(
                candidate.starts_with(self.root.join("epochs").canonicalize()?),
                "invalid epoch path"
            );
            candidate
        };
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; BEGIN;")?;
        let started = std::time::Instant::now();
        db.progress_handler(10000, Some(move || started.elapsed().as_secs() >= 2))?;
        Ok(db)
    }
    fn explorer_columns(db: &Connection, table: &str) -> Result<Vec<Column>> {
        ensure!(
            TABLES.contains(&table),
            "table is not exposed by the explorer"
        );
        let mut statement = db.prepare(&format!("PRAGMA table_info({})", quoted(table)))?;
        let columns = statement
            .query_map([], |r| {
                Ok(Column {
                    name: r.get(1)?,
                    declared_type: r.get(2)?,
                    primary_key: r.get::<_, i64>(5)? != 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(!columns.is_empty(), "table does not exist in this database");
        Ok(columns)
    }
    pub fn explore(&self, request: &BrowseRequest) -> Result<BrowsePage> {
        let _deadline = Deadline::new(&self.db)?;
        ensure!((1..=1000).contains(&request.limit), "limit must be 1–1000");
        ensure!(request.search.len() <= 4096, "search too long");
        let mut normalized = request.clone();
        normalized.cursor = None;
        let fingerprint = blake3::hash(&serde_json::to_vec(&normalized)?)
            .to_hex()
            .to_string();
        let dataset: String = self.db.query_row(
            "SELECT value FROM settings WHERE key='dataset_id'",
            [],
            |r| r.get(0),
        )?;
        let generation = self.explorer_generation()?;
        let mut cursor = if let Some(s) = &request.cursor {
            ensure!(s.len() < 8192, "cursor too large");
            serde_json::from_slice::<Cursor>(&URL_SAFE_NO_PAD.decode(s)?)?
        } else {
            Cursor {
                dataset: dataset.clone(),
                generation: generation.clone(),
                snapshot: self.db.query_row(
                    "SELECT COALESCE(MAX(id),0) FROM observations",
                    [],
                    |r| r.get(0),
                )?,
                offset: 0,
                fingerprint: fingerprint.clone(),
            }
        };
        ensure!(
            cursor.dataset == dataset
                && cursor.generation == generation
                && cursor.fingerprint == fingerprint
                && cursor.offset >= 0,
            "cursor invalidated or does not match this view; refresh"
        );
        let mut page = BrowsePage::default();
        let mut more = false;
        match &request.target {
            Browse::Attachment { hash } => {
                let path = crate::media::attachment_path(&self.root, hash)?;
                let mut st = self.db.prepare(
                    "SELECT id,size,status,error FROM media WHERE hash=?1 ORDER BY id LIMIT 100",
                )?;
                let media=st.query_map([hash],|r|Ok(json!({"id":r.get::<_,String>(0)?,"size":r.get::<_,Option<u64>>(1)?,"status":r.get::<_,String>(2)?,"error":r.get::<_,Option<String>>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
                let mut representations = vec![];
                let extended: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='representations')",
                    [],
                    |r| r.get(0),
                )?;
                if extended {
                    let mut st=self.db.prepare("SELECT original,hash,recipe,bytes FROM representations WHERE original=?1 OR hash=?1 LIMIT 100")?;
                    representations=st.query_map([hash],|r|Ok(json!({"original":r.get::<_,String>(0)?,"hash":r.get::<_,String>(1)?,"recipe":r.get::<_,String>(2)?,"bytes":r.get::<_,u64>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
                }
                ensure!(
                    !media.is_empty() || !representations.is_empty(),
                    "attachment not retained in archive metadata"
                );
                let available = path.is_file();
                let bytes = path.metadata().ok().map(|m| m.len());
                let mut e = Entry::new(
                    hash,
                    format!(
                        "{} · {} · {} bytes",
                        hash,
                        if available { "available" } else { "missing" },
                        bytes
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "unknown".into())
                    ),
                    json!({"hash":hash,"available":available,"bytes":bytes,"media":media,"representations":representations}),
                );
                e.binaries
                    .push(BinaryRef::Attachment { hash: hash.clone() });
                page.entries.push(e);
                page.live = true;
            }
            Browse::Databases => {
                page.entries.push(Entry::new(
                    "catalog",
                    "Catalog",
                    json!({"database":"catalog"}),
                ));
                for (id, name, _, sealed) in self.epochs()? {
                    page.entries.push(Entry::new(
                        format!("epoch:{id}"),
                        format!("{name} · {}", if sealed { "sealed" } else { "active" }),
                        json!({"epoch":id,"sealed":sealed}),
                    ));
                }
                for entry in &mut page.entries {
                    entry.open = Some(Browse::Tables {
                        database: entry.id.clone(),
                    });
                }
                paginate_entries(&mut page, &mut cursor, request, &mut more);
            }
            Browse::Tables { database } => {
                let db = self.explorer_database(database)?;
                let mut st = db.prepare(
                    "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name",
                )?;
                for row in st.query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
                })? {
                    let (name, sql) = row?;
                    if TABLES.contains(&name.as_str()) {
                        let columns = Self::explorer_columns(&db, &name)?;
                        let mut e = Entry::new(&name, &name, json!({"sql":sql,"columns":columns}));
                        e.open = Some(Browse::Rows {
                            database: database.clone(),
                            table: name,
                            row: None,
                        });
                        page.entries.push(e);
                    }
                }
                paginate_entries(&mut page, &mut cursor, request, &mut more);
            }
            Browse::Rows {
                database,
                table,
                row,
            } => {
                let db = self.explorer_database(database)?;
                page.columns = Self::explorer_columns(&db, table)?;
                page.live = true;
                // Large values are never fetched into the row response. typeof/length preserve SQLite storage classes.
                let expressions = page.columns.iter().map(|c| { let q = quoted(&c.name); format!("typeof({q}),length(CAST({q} AS BLOB)),CASE WHEN typeof({q})='blob' OR (typeof({q})='text' AND length(CAST({q} AS BLOB))>4096) THEN NULL ELSE {q} END") }).collect::<Vec<_>>().join(",");
                let filter = if table == "checkpoints" {
                    " AND key NOT LIKE 'takeout:%'"
                } else {
                    ""
                };
                let sql = format!(
                    "SELECT rowid,{expressions} FROM {} WHERE rowid>?1 AND (?2 IS NULL OR rowid=?2){filter} ORDER BY rowid LIMIT ?3",
                    quoted(table)
                );
                let mut st = db.prepare(&sql)?;
                let mut rows = st.query(params![cursor.offset, row, 10001])?;
                let mut bytes = 0usize;
                let mut scanned = 0;
                while let Some(r) = rows.next()? {
                    if page.entries.len() == request.limit
                        || bytes > 1024 * 1024
                        || scanned == 10000
                    {
                        more = true;
                        break;
                    }
                    scanned += 1;
                    let id: i64 = r.get(0)?;
                    cursor.offset = id;
                    let mut cells = serde_json::Map::new();
                    let mut binaries = vec![];
                    for (i, c) in page.columns.iter().enumerate() {
                        let ty: String = r.get(1 + i * 3)?;
                        let size: Option<u64> = r.get(2 + i * 3)?;
                        let reference = BinaryRef::Cell {
                            database: database.clone(),
                            table: table.clone(),
                            row: id,
                            column: c.name.clone(),
                            generation: generation.clone(),
                        };
                        let cell = match (ty.as_str(), r.get_ref(3 + i * 3)?) {
                            ("blob", _) => {
                                binaries.push(reference.clone());
                                Cell::Blob {
                                    bytes: size.unwrap_or(0),
                                    reference,
                                }
                            }
                            ("text", ValueRef::Null) => {
                                binaries.push(reference.clone());
                                Cell::LargeText {
                                    bytes: size.unwrap_or(0),
                                    reference,
                                }
                            }
                            (_, ValueRef::Null) => Cell::Null,
                            (_, ValueRef::Integer(n)) => Cell::Integer(n.to_string()),
                            (_, ValueRef::Real(n)) => Cell::Real(n.to_string()),
                            (_, ValueRef::Text(s)) => {
                                binaries.push(reference.clone());
                                match std::str::from_utf8(s) {
                                    Ok(text) => Cell::Text(text.into()),
                                    Err(_) => Cell::LargeText {
                                        bytes: s.len() as u64,
                                        reference,
                                    },
                                }
                            }
                            _ => bail!("unsupported SQLite cell"),
                        };
                        cells.insert(c.name.clone(), serde_json::to_value(cell)?);
                    }
                    let detail = Value::Object(cells);
                    if !request.search.is_empty()
                        && !detail
                            .to_string()
                            .to_lowercase()
                            .contains(&request.search.to_lowercase())
                    {
                        continue;
                    }
                    bytes += serde_json::to_vec(&detail)?.len();
                    let preview = page
                        .columns
                        .iter()
                        .take(3)
                        .filter_map(|c| detail[&c.name]["value"].as_str())
                        .collect::<Vec<_>>()
                        .join(" · ");
                    let mut e = Entry::new(id.to_string(), format!("#{id} {preview}"), detail);
                    e.binaries = binaries;
                    if table == "observations" {
                        e.open = Some(Browse::Location { sequence: id });
                    }
                    page.entries.push(e);
                    cursor.offset = id;
                }
            }
            Browse::Location { sequence } => {
                let (hash, epoch, block, offset, length, pending, schema): (String,i64,Option<i64>,Option<i64>,i64,bool,String) = self.db.query_row("SELECT p.hash,p.epoch,p.block,p.offset,p.length,p.journal IS NOT NULL,p.schema_hash FROM observations o JOIN payloads p ON p.hash=o.payload WHERE o.id=?1", [sequence], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
                let mut e = Entry::new(
                    "payload",
                    "Original TL bytes",
                    json!({"hash":hash,"epoch":epoch,"block":block,"offset":offset,"length":length,"journaled":pending,"schema":schema}),
                );
                e.binaries.push(BinaryRef::Payload { hash: hash.clone() });
                page.entries.push(e);
                let mut add_row = |database: String, table: &str, row: i64, title: &str| {
                    let mut e = Entry::new(table, title, json!({"row":row}));
                    e.open = Some(Browse::Rows {
                        database,
                        table: table.into(),
                        row: Some(row),
                    });
                    page.entries.push(e);
                };
                add_row(
                    "catalog".into(),
                    "observations",
                    *sequence,
                    "Catalog observation",
                );
                let payload_row = self.db.query_row(
                    "SELECT rowid FROM payloads WHERE hash=?1",
                    [&hash],
                    |r| r.get(0),
                )?;
                add_row("catalog".into(), "payloads", payload_row, "Catalog payload");
                let schema_row = self.db.query_row(
                    "SELECT rowid FROM schemas WHERE hash=?1",
                    [&schema],
                    |r| r.get(0),
                )?;
                add_row(
                    "catalog".into(),
                    "schemas",
                    schema_row,
                    "TL schema definition",
                );
                if let Some(block) = block {
                    let database = format!("epoch:{epoch}");
                    add_row(database.clone(), "blocks", block, "Compressed block");
                    let db = self.explorer_database(&database)?;
                    let dictionary: Option<i64> =
                        db.query_row("SELECT dictionary FROM blocks WHERE id=?1", [block], |r| {
                            r.get(0)
                        })?;
                    if let Some(id) = dictionary {
                        add_row(database, "dictionaries", id, "Compression dictionary");
                    }
                }
            }
            Browse::Operations { name } => {
                if name == "status" {
                    page.entries.push(Entry::new(
                        "status",
                        "Archive status (live)",
                        self.operational_status(false)?,
                    ));
                    page.live = true;
                } else {
                    ensure!(
                        ["jobs", "coverage", "media", "work", "maintenance"]
                            .contains(&name.as_str()),
                        "unknown operation view"
                    );
                    let mut req = request.clone();
                    req.target = Browse::Rows {
                        database: "catalog".into(),
                        table: name.clone(),
                        row: None,
                    };
                    req.cursor = None;
                    // Preserve the external cursor identity while reusing typed table reads.
                    let inner_fingerprint = blake3::hash(&serde_json::to_vec(&req)?)
                        .to_hex()
                        .to_string();
                    let inner = Cursor {
                        fingerprint: inner_fingerprint,
                        ..cursor
                    };
                    req.cursor = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&inner)?));
                    let mut result = self.explore(&req)?;
                    if let Some(c) = &result.next_cursor {
                        let mut c: Cursor = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(c)?)?;
                        c.fingerprint = fingerprint;
                        result.next_cursor = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&c)?));
                    }
                    return Ok(result);
                }
            }
            Browse::Messages { peer, topic } => {
                ensure!(peer_from_key(peer) == Some(peer.as_str()), "invalid peer");
                // Snapshot ranking matches the archive current-object query, including full-over-partial precedence.
                let sql = "WITH ranked AS (SELECT id,key,ROW_NUMBER() OVER(PARTITION BY key ORDER BY partial ASC,COALESCE(json_extract(metadata,'$.revision'),observed) DESC,id DESC) AS rank FROM observations WHERE id<=?1 AND kind='message' AND key>=?2 AND key<(?2 || char(127))) SELECT id,CAST(substr(key,length(?2)+1) AS INTEGER) AS message_id FROM ranked WHERE rank=1 AND (?3=0 OR CAST(substr(key,length(?2)+1) AS INTEGER)<?3) ORDER BY message_id DESC LIMIT 10001";
                let prefix = format!("{peer}/message:");
                let mut st = self.db.prepare(sql)?;
                let candidates = st
                    .query_map(params![cursor.snapshot, prefix, cursor.offset], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let start = std::time::Instant::now();
                let mut bytes = 0;
                for (index, (id, message_id)) in candidates.iter().enumerate() {
                    if page.entries.len() >= request.limit
                        || index == 10000
                        || start.elapsed().as_secs() >= 2
                        || bytes >= 1024 * 1024
                    {
                        more = true;
                        break;
                    }
                    cursor.offset = *message_id;
                    let record = self.record(*id)?;
                    if topic
                        .as_ref()
                        .is_some_and(|topic| !topic_matches(&record, topic))
                    {
                        continue;
                    }
                    let text = tg_backup_protocol::text(&record.data);
                    if !text.to_lowercase().contains(&request.search.to_lowercase()) {
                        continue;
                    }
                    let from = &record.data["from_id"];
                    let sender = [
                        ("user_id", "user"),
                        ("chat_id", "chat"),
                        ("channel_id", "channel"),
                    ]
                    .iter()
                    .find_map(|(field, kind)| {
                        scalar(&from[*field]).map(|id| format!("{kind}:{id}"))
                    });
                    let sender = if let Some(peer) = sender {
                        self.explorer_head(&peer, cursor.snapshot)?
                            .map(|r| label(&r.data))
                            .filter(|s| !s.is_empty())
                            .unwrap_or(peer)
                    } else {
                        "unknown sender".into()
                    };
                    let date = scalar(&record.data["date"])
                        .and_then(|s| s.parse::<i64>().ok())
                        .and_then(|t| chrono::DateTime::from_timestamp(t, 0))
                        .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
                        .unwrap_or_else(|| "date unknown".into());
                    let reply = &record.data["reply_to"]["reply_to_msg_id"];
                    let summary = format!(
                        "#{message_id} · {sender} · {date}{}{}\n{}{}",
                        if record.deleted { " [deleted]" } else { "" },
                        if record.partial { " [partial]" } else { "" },
                        if text.is_empty() {
                            record.data["_"]
                                .as_str()
                                .unwrap_or("service message")
                                .to_owned()
                        } else {
                            text.chars().take(240).collect::<String>()
                        },
                        scalar(reply)
                            .map(|r| format!(" ↩ #{r}"))
                            .unwrap_or_default()
                    );
                    let mut entry = record_entry(record);
                    entry.label = summary;
                    bytes += serde_json::to_vec(&entry)?.len();
                    page.entries.push(entry);
                }
                page.incomplete = more;
            }
            Browse::Conversations { folder } => {
                // All observed peer keys participate, even if dialog/profile collection was incomplete.
                let sql = "SELECT DISTINCT CASE WHEN instr(key,'/')>0 THEN substr(key,1,instr(key,'/')-1) ELSE key END AS peer FROM observations WHERE id<=?1 AND (key LIKE 'user:%' OR key LIKE 'chat:%' OR key LIKE 'channel:%') ORDER BY peer LIMIT ?2 OFFSET ?3";
                let mut st = self.db.prepare(sql)?;
                let peers = st
                    .query_map(params![cursor.snapshot, 1001, cursor.offset], |r| {
                        r.get::<_, String>(0)
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (index, peer) in peers.iter().enumerate() {
                    if page.entries.len() == request.limit || index == 1000 {
                        more = true;
                        break;
                    }
                    cursor.offset += 1;
                    if peer_from_key(peer) != Some(peer.as_str()) {
                        continue;
                    }
                    let profile = self.explorer_head(peer, cursor.snapshot)?;
                    let dialog = self.explorer_head(&format!("{peer}/dialog"), cursor.snapshot)?;
                    if let Some(folder) = folder {
                        let matches =
                            profile
                                .as_ref()
                                .into_iter()
                                .chain(dialog.as_ref())
                                .any(|r| {
                                    r.metadata["folder"].as_array().is_some_and(|a| {
                                        a.iter().any(|v| scalar(v).as_ref() == Some(folder))
                                    })
                                });
                        let fallback: bool = if matches {
                            false
                        } else {
                            self.db.query_row("SELECT EXISTS(SELECT 1 FROM observations o,json_each(o.metadata,'$.folder') f WHERE o.id<=?1 AND o.key>=?2 AND o.key<(?2 || char(127)) AND CAST(f.value AS TEXT)=?3)",params![cursor.snapshot,format!("{peer}/"),folder],|r|r.get(0))?
                        };
                        if !matches && !fallback {
                            continue;
                        }
                    }
                    let title = profile
                        .as_ref()
                        .map(|r| label(&r.data))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| peer.clone());
                    if !format!("{title} {peer}")
                        .to_lowercase()
                        .contains(&request.search.to_lowercase())
                    {
                        continue;
                    }
                    let mut e = Entry::new(
                        peer,
                        title,
                        json!({"peer":peer,"profile":profile,"dialog":dialog}),
                    );
                    e.open = Some(Browse::Messages {
                        peer: peer.clone(),
                        topic: None,
                    });
                    page.entries.push(e);
                }
                page.incomplete = more;
            }
            Browse::Topics { peer } => {
                ensure!(peer_from_key(peer) == Some(peer.as_str()), "invalid peer");
                let prefix = format!("{peer}/");
                let sql = "WITH candidates AS (SELECT substr(key,instr(key,'/topic:')+7) AS topic,id FROM observations WHERE id<=?1 AND kind='topic' AND key>=?2 AND key<(?2 || char(127)) AND instr(key,'/topic:')>0 UNION ALL SELECT CAST(json_extract(metadata,'$.topic') AS TEXT),NULL FROM observations WHERE id<=?1 AND kind='message' AND key>=?2 AND key<(?2 || char(127)) AND json_extract(metadata,'$.topic') IS NOT NULL) SELECT topic,MAX(id) FROM candidates GROUP BY topic ORDER BY CAST(topic AS INTEGER),topic LIMIT ?3 OFFSET ?4";
                let mut st = self.db.prepare(sql)?;
                let topics = st
                    .query_map(
                        params![cursor.snapshot, prefix, request.limit + 1, cursor.offset],
                        |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?)),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                more = topics.len() > request.limit;
                for (topic, id) in topics.into_iter().take(request.limit) {
                    cursor.offset += 1;
                    let mut e = if let Some(id) = id {
                        let r = self.record(id)?;
                        let r = self.explorer_head(&r.key, cursor.snapshot)?.unwrap_or(r);
                        let title = label(&r.data);
                        let mut e = record_entry(r);
                        e.label = if title.is_empty() {
                            format!("Topic {topic}")
                        } else {
                            title
                        };
                        e
                    } else {
                        Entry::new(
                            &topic,
                            format!("Topic {topic} (metadata unavailable)"),
                            json!({"peer":peer,"topic":topic}),
                        )
                    };
                    e.open = Some(Browse::Messages {
                        peer: peer.clone(),
                        topic: Some(topic),
                    });
                    if e.label
                        .to_lowercase()
                        .contains(&request.search.to_lowercase())
                    {
                        page.entries.push(e);
                    }
                }
            }
            Browse::Folders => {
                let kind = if matches!(request.target, Browse::Folders) {
                    "folder"
                } else {
                    "topic"
                };
                let prefix = match &request.target {
                    Browse::Topics { peer } => format!("{peer}/"),
                    _ => String::new(),
                };
                let sql = "WITH ranked AS (SELECT id,key,ROW_NUMBER() OVER(PARTITION BY key ORDER BY partial ASC,COALESCE(json_extract(metadata,'$.revision'),observed) DESC,id DESC) AS rank FROM observations WHERE id<=?1 AND kind=?2 AND substr(key,1,length(?3))=?3) SELECT id FROM ranked WHERE rank=1 ORDER BY key LIMIT ?4 OFFSET ?5";
                let mut st = self.db.prepare(sql)?;
                let ids = st
                    .query_map(
                        params![
                            cursor.snapshot,
                            kind,
                            prefix,
                            request.limit + 1,
                            cursor.offset
                        ],
                        |r| r.get::<_, i64>(0),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                more = ids.len() > request.limit;
                for id in ids.into_iter().take(request.limit) {
                    cursor.offset += 1;
                    let r = self.record(id)?;
                    let id = scalar(&r.data["id"])
                        .or_else(|| r.key.rsplit_once(':').map(|(_, s)| s.into()))
                        .unwrap_or_default();
                    let title = label(&r.data);
                    let mut e = record_entry(r);
                    e.label = if title.is_empty() { id.clone() } else { title };
                    e.open = Some(match &request.target {
                        Browse::Topics { peer } => Browse::Messages {
                            peer: peer.clone(),
                            topic: Some(id),
                        },
                        _ => Browse::Conversations { folder: Some(id) },
                    });
                    if e.label
                        .to_lowercase()
                        .contains(&request.search.to_lowercase())
                    {
                        page.entries.push(e);
                    }
                }
            }
        }
        if more {
            page.next_cursor = Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor)?));
        }
        Ok(page)
    }
    fn explorer_head(
        &self,
        key: &str,
        snapshot: i64,
    ) -> Result<Option<tg_backup_protocol::Record>> {
        use rusqlite::OptionalExtension;
        let id = self.db.query_row("SELECT id FROM observations WHERE key=?1 AND id<=?2 ORDER BY partial ASC,COALESCE(json_extract(metadata,'$.revision'),observed) DESC,id DESC LIMIT 1", params![key,snapshot], |r| r.get(0)).optional()?;
        id.map(|id| self.record(id)).transpose()
    }
    pub fn explorer_binary(&self, request: &BinaryRequest) -> Result<BinaryPage> {
        ensure!(
            (1..=BINARY_CHUNK).contains(&request.limit),
            "binary limit must be 1–65536"
        );
        ensure!(
            request.offset <= i64::MAX as u64 - BINARY_CHUNK as u64,
            "invalid offset"
        );
        let (bytes, total) = match &request.reference {
            BinaryRef::Payload { hash } => {
                let bytes = self.payload(hash)?;
                let total = bytes.len() as u64;
                ensure!(request.offset <= total, "offset beyond payload");
                (
                    bytes[request.offset as usize
                        ..(request.offset as usize + request.limit).min(bytes.len())]
                        .to_vec(),
                    total,
                )
            }
            BinaryRef::Attachment { hash } => {
                let exists: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM media WHERE hash=?1 AND status='complete')",
                    [hash],
                    |r| r.get(0),
                )?;
                let representations: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='representations')",
                    [],
                    |r| r.get(0),
                )?;
                let derivative = representations
                    && self.db.query_row(
                        "SELECT EXISTS(SELECT 1 FROM representations WHERE hash=?1)",
                        [hash],
                        |r| r.get::<_, bool>(0),
                    )?;
                ensure!(exists || derivative, "attachment unavailable");
                let path = crate::media::attachment_path(&self.root, hash)?;
                let mut file = std::fs::File::open(path)?;
                let total = file.metadata()?.len();
                ensure!(request.offset <= total, "offset beyond attachment");
                file.seek(SeekFrom::Start(request.offset))?;
                let mut bytes = vec![];
                file.take(request.limit as u64).read_to_end(&mut bytes)?;
                (bytes, total)
            }
            BinaryRef::Cell {
                database,
                table,
                row,
                column,
                generation,
            } => {
                ensure!(
                    *generation == self.explorer_generation()?,
                    "storage generation changed; refresh"
                );
                let db = self.explorer_database(database)?;
                let columns = Self::explorer_columns(&db, table)?;
                ensure!(columns.iter().any(|c| c.name == *column), "unknown column");
                let filter = if table == "checkpoints" {
                    " AND key NOT LIKE 'takeout:%'"
                } else {
                    ""
                };
                let sql = format!(
                    "SELECT length(CAST({0} AS BLOB)),substr(CAST({0} AS BLOB),?2,?3) FROM {1} WHERE rowid=?1{filter}",
                    quoted(column),
                    quoted(table)
                );
                let (total, bytes): (u64, Vec<u8>) =
                    db.query_row(&sql, params![row, request.offset + 1, request.limit], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
                ensure!(request.offset <= total, "offset beyond cell");
                (bytes, total)
            }
        };
        let end = request.offset + bytes.len() as u64;
        Ok(BinaryPage {
            hex: hex::encode(bytes),
            total,
            next_offset: (end < total).then_some(end),
        })
    }
}
fn paginate_entries(
    page: &mut BrowsePage,
    cursor: &mut Cursor,
    request: &BrowseRequest,
    more: &mut bool,
) {
    page.entries.retain(|e| {
        e.label
            .to_lowercase()
            .contains(&request.search.to_lowercase())
    });
    let skip = (cursor.offset as usize).min(page.entries.len());
    page.entries.drain(..skip);
    *more = page.entries.len() > request.limit;
    page.entries.truncate(request.limit);
    cursor.offset += page.entries.len() as i64;
}
pub fn record_entry(record: tg_backup_protocol::Record) -> Entry {
    let text = tg_backup_protocol::text(&record.data);
    let mut e = Entry::new(
        &record.key,
        format!(
            "{} {}{}",
            record.key,
            text.chars().take(160).collect::<String>(),
            if record.deleted {
                " [deleted]"
            } else if record.partial {
                " [partial]"
            } else {
                ""
            }
        ),
        serde_json::to_value(&record).unwrap_or(Value::Null),
    );
    e.binaries = record
        .media_hashes(tg_backup_protocol::MediaSelection::All)
        .into_iter()
        .map(|hash| BinaryRef::Attachment { hash })
        .collect();
    e.record = Some(record);
    e
}
pub struct LocalBackend {
    pub root: std::path::PathBuf,
}
impl tg_backup_tui::Backend for LocalBackend {
    fn request(&self, request: Request) -> tg_backup_tui::BackendFuture<'_> {
        let root = self.root.clone();
        Box::pin(async move { tokio::task::spawn_blocking(move || execute(&root, request)).await? })
    }
}
pub fn execute(root: &Path, request: Request) -> Result<Response> {
    let a = Archive::open(root, false)?;
    match request {
        Request::Capabilities => Ok(Response::Capabilities(a.explorer_capabilities())),
        Request::Query(q) => {
            let mut page = a.query(&q)?;
            for record in &mut page.records {
                tg_backup_protocol::public_json(&mut record.data);
                tg_backup_protocol::public_json(&mut record.metadata);
            }
            Ok(Response::Query(page))
        }
        Request::Browse(q) => {
            let mut value = serde_json::to_value(a.explore(&q)?)?;
            tg_backup_protocol::public_json(&mut value);
            Ok(Response::Browse(serde_json::from_value(value)?))
        }
        Request::Binary(q) => Ok(Response::Binary(a.explorer_binary(&q)?)),
    }
}

struct Deadline<'a>(&'a Connection);
impl<'a> Deadline<'a> {
    fn new(db: &'a Connection) -> Result<Self> {
        let started = std::time::Instant::now();
        db.progress_handler(10000, Some(move || started.elapsed().as_secs() >= 2))?;
        Ok(Self(db))
    }
}
impl Drop for Deadline<'_> {
    fn drop(&mut self) {
        let _ = self.0.progress_handler(0, None::<fn() -> bool>);
    }
}
