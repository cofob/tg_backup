use crate::{
    config::Config,
    tl::{self, Schema},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

pub const FORMAT_VERSION: i64 = 2;
pub const MAX_PAYLOAD: usize = 64 * 1024 * 1024;
/// Durable boundaries exposed for monitoring and failure-injection tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitPoint {
    EpochCommitted,
    CatalogPublished,
    GenerationReady,
    GenerationPublished,
}

type BlockCache = std::cell::RefCell<HashMap<(String, i64), std::sync::Arc<Vec<u8>>>>;
type PayloadLocation = (
    String,
    String,
    Option<Vec<u8>>,
    String,
    Option<i64>,
    Option<usize>,
    usize,
);
pub struct Archive {
    pub root: PathBuf,
    pub db: Connection,
    pub config: Config,
    pub writable: bool,
    schemas: std::cell::RefCell<HashMap<String, std::sync::Arc<Schema>>>,
    blocks: BlockCache,
    _lock: File,
}
#[derive(Clone, Debug)]
pub struct Capture {
    pub key: String,
    pub kind: String,
    pub root_type: String,
    pub bytes: Vec<u8>,
    pub observed_at: i64,
    pub source: String,
    pub metadata: Value,
    pub replay_key: Option<String>,
    pub partial: bool,
    pub deleted: bool,
}
pub use tg_backup_protocol::Record;
const CATALOG: &str = r#"
PRAGMA user_version=2;
CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE schemas(hash TEXT PRIMARY KEY,layer INTEGER NOT NULL,definition TEXT NOT NULL);
CREATE TABLE epochs(id INTEGER PRIMARY KEY,name TEXT NOT NULL,path TEXT UNIQUE NOT NULL,sealed INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL);
CREATE TABLE payloads(hash TEXT PRIMARY KEY,schema_hash TEXT NOT NULL REFERENCES schemas(hash),root_type TEXT NOT NULL,epoch INTEGER NOT NULL REFERENCES epochs(id),block INTEGER,offset INTEGER,length INTEGER NOT NULL,journal BLOB);
CREATE TABLE observations(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL,kind TEXT NOT NULL,observed INTEGER NOT NULL,source TEXT NOT NULL,payload TEXT NOT NULL REFERENCES payloads(hash),epoch INTEGER NOT NULL REFERENCES epochs(id),metadata TEXT NOT NULL,partial INTEGER NOT NULL,deleted INTEGER NOT NULL,transformed INTEGER NOT NULL DEFAULT 0,replay_key TEXT UNIQUE);
CREATE INDEX observations_key ON observations(key,id);
CREATE INDEX observations_time ON observations(observed,id);
CREATE INDEX observations_kind ON observations(kind,id);
CREATE INDEX observations_payload ON observations(payload);
CREATE TABLE heads(key TEXT PRIMARY KEY,observation INTEGER NOT NULL REFERENCES observations(id),revision INTEGER NOT NULL,partial INTEGER NOT NULL);
CREATE VIRTUAL TABLE search USING fts5(text,content='',contentless_delete=1,tokenize='unicode61');
CREATE VIRTUAL TABLE history_search USING fts5(text,content='',contentless_delete=1,tokenize='unicode61');
CREATE TABLE checkpoints(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE jobs(id TEXT PRIMARY KEY,config TEXT NOT NULL,status TEXT NOT NULL,created INTEGER NOT NULL,updated INTEGER NOT NULL,details TEXT NOT NULL);
CREATE TABLE coverage(name TEXT PRIMARY KEY,status TEXT NOT NULL,updated INTEGER NOT NULL,details TEXT NOT NULL);
CREATE TABLE media(id TEXT PRIMARY KEY,location TEXT NOT NULL,dc INTEGER NOT NULL,size INTEGER,status TEXT NOT NULL DEFAULT 'pending',offset INTEGER NOT NULL DEFAULT 0,hash TEXT,error TEXT,attempts INTEGER NOT NULL DEFAULT 0,retry_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE media_refs(media TEXT NOT NULL REFERENCES media(id),observation INTEGER NOT NULL REFERENCES observations(id),PRIMARY KEY(media,observation));
CREATE TABLE maintenance(id TEXT PRIMARY KEY,at INTEGER NOT NULL,policy TEXT NOT NULL,report TEXT NOT NULL);
CREATE TABLE retired(path TEXT PRIMARY KEY);
"#;
const EPOCH: &str = r#"
PRAGMA user_version=2;
CREATE TABLE schemas(hash TEXT PRIMARY KEY,layer INTEGER NOT NULL,definition TEXT NOT NULL);
CREATE TABLE dictionaries(id INTEGER PRIMARY KEY,data BLOB NOT NULL,hash TEXT NOT NULL);
CREATE TABLE blocks(id INTEGER PRIMARY KEY,codec TEXT NOT NULL,dictionary INTEGER,raw_size INTEGER NOT NULL,checksum TEXT NOT NULL,data BLOB NOT NULL);
CREATE TABLE payloads(hash TEXT PRIMARY KEY,schema_hash TEXT NOT NULL,root_type TEXT NOT NULL,block INTEGER NOT NULL,offset INTEGER NOT NULL,length INTEGER NOT NULL);
CREATE TABLE observations(id INTEGER PRIMARY KEY,key TEXT NOT NULL,kind TEXT NOT NULL,observed INTEGER NOT NULL,source TEXT NOT NULL,payload TEXT NOT NULL,metadata TEXT NOT NULL,partial INTEGER NOT NULL,deleted INTEGER NOT NULL,transformed INTEGER NOT NULL,replay_key TEXT);
CREATE INDEX observations_key ON observations(key,id);
"#;
fn connection(path: &Path, write: bool) -> Result<Connection> {
    let flags = if write {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let db = Connection::open_with_flags(path, flags)?;
    db.busy_timeout(Duration::from_secs(30))?;
    db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF;")?;
    if write {
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    } else {
        db.execute_batch("PRAGMA query_only=ON;")?;
    }
    Ok(db)
}
fn lock(root: &Path, writer: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(writer)
        .open(root.join(if writer {
            "writer.lock"
        } else {
            "readers.lock"
        }))?;
    if writer {
        file.try_lock_exclusive()
            .context("dataset already has a writer")?;
    } else {
        FileExt::lock_shared(&file)?;
    }
    Ok(file)
}
impl Archive {
    pub fn init(root: &Path, config: &Config) -> Result<Self> {
        fs::create_dir_all(root)?;
        ensure!(
            !root.join("catalog.sqlite3").exists(),
            "dataset already exists"
        );
        for dir in ["epochs", "attachments", "staging"] {
            fs::create_dir_all(root.join(dir))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        for name in ["writer.lock", "readers.lock"] {
            File::create(root.join(name))?.sync_all()?;
        }
        let _lock = lock(root, true)?;
        fs::write(root.join("config.toml"), toml::to_string_pretty(config)?)?;
        let db = connection(&root.join("catalog.sqlite3"), true)?;
        db.execute_batch(CATALOG)?;
        crate::work::migrate(&db)?;
        db.execute(
            "INSERT INTO settings VALUES('dataset_id',?1)",
            [uuid::Uuid::new_v4().to_string()],
        )?;
        let schema_hash = blake3::hash(tl::API_SCHEMA.as_bytes()).to_hex().to_string();
        db.execute(
            "INSERT INTO schemas VALUES(?1,?2,?3)",
            params![schema_hash, tl::LAYER, tl::API_SCHEMA],
        )?;
        Ok(Self {
            root: root.into(),
            db,
            config: config.clone(),
            writable: true,
            schemas: Default::default(),
            blocks: Default::default(),
            _lock,
        })
    }
    pub fn open(root: &Path, write: bool) -> Result<Self> {
        let _lock = lock(root, write)?;
        let db = connection(&root.join("catalog.sqlite3"), write)?;
        let version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        ensure!(
            version == FORMAT_VERSION,
            "unsupported archive version {version}"
        );
        let mut a = Self {
            root: root.into(),
            db,
            config: Config::load(root)?,
            writable: write,
            schemas: Default::default(),
            blocks: Default::default(),
            _lock,
        };
        if write {
            crate::work::migrate(&a.db)?;
            a.register_schema(tl::LAYER, tl::API_SCHEMA)?;
            a.materialize()?;
            a.recover_retired()?;
        } else {
            a.db.execute_batch("BEGIN;")?;
        }
        Ok(a)
    }
    pub fn register_schema(&self, layer: i32, text: &str) -> Result<String> {
        ensure!(self.writable, "read-only archive");
        Schema::parse(text)?;
        let hash = blake3::hash(text.as_bytes()).to_hex().to_string();
        self.db.execute(
            "INSERT OR IGNORE INTO schemas VALUES(?1,?2,?3)",
            params![hash, layer, text],
        )?;
        Ok(hash)
    }
    pub fn schema(&self, hash: &str) -> Result<std::sync::Arc<Schema>> {
        if let Some(schema) = self.schemas.borrow().get(hash) {
            return Ok(schema.clone());
        }
        let text: String = self.db.query_row(
            "SELECT definition FROM schemas WHERE hash=?1",
            [hash],
            |r| r.get(0),
        )?;
        ensure!(
            blake3::hash(text.as_bytes()).to_hex().as_str() == hash,
            "schema checksum mismatch"
        );
        let schema = std::sync::Arc::new(Schema::parse(&text)?);
        self.schemas
            .borrow_mut()
            .insert(hash.into(), schema.clone());
        Ok(schema)
    }
    fn epoch(&self, observed: i64, reserved: &HashMap<i64, u64>) -> Result<i64> {
        let dt = DateTime::from_timestamp_micros(observed).context("invalid observation time")?;
        let name = self.config.epoch.key(dt);
        if let Some(id) = self
            .db
            .query_row(
                "SELECT id FROM epochs WHERE name=?1 AND sealed=0 AND accepting=1 ORDER BY id DESC LIMIT 1",
                [&name],
                |r| r.get(0),
            )
            .optional()?
        {
            let path: String = self.db.query_row("SELECT path FROM epochs WHERE id=?1",[id],|r|r.get(0))?;
            let epoch = connection(&self.root.join(path), false)?;
            let pages: u64 = epoch.pragma_query_value(None,"page_count",|r|r.get(0))?;
            let size: u64 = epoch.pragma_query_value(None,"page_size",|r|r.get(0))?;
            let high: i64 = epoch.query_row("SELECT COALESCE(MAX(id),0) FROM observations",[],|r|r.get(0))?;
            let pending: u64 = self.db.query_row("SELECT COALESCE(SUM(length(metadata)*2+length(key)+length(source)+8192),0) FROM observations WHERE epoch=?1 AND id>?2",params![id,high],|r|r.get(0))?;
            let raw: u64 = self.db.query_row("SELECT COALESCE(SUM(length*2),0) FROM payloads WHERE epoch=?1 AND journal IS NOT NULL",[id],|r|r.get(0))?;
            if self.config.max_epoch_bytes == 0 || pages*size+pending+raw+reserved.get(&id).copied().unwrap_or(0) < self.config.max_epoch_bytes {
                return Ok(id);
            }
            self.db.execute("UPDATE epochs SET accepting=0 WHERE id=?1",[id])?;
        }
        let generation: i64 = self.db.query_row(
            "SELECT COALESCE(MAX(generation),0)+1 FROM epochs WHERE name=?1",
            [&name],
            |r| r.get(0),
        )?;
        let path = format!(
            "epochs/{name}.g{generation:04}.{}.sqlite3",
            uuid::Uuid::new_v4()
        );
        let epoch = connection(&self.root.join(&path), true)?;
        epoch.execute_batch(EPOCH)?;
        epoch.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(epoch);
        sync_dir(&self.root.join("epochs"))?;
        self.db.execute(
            "INSERT INTO epochs(name,path,generation,part) VALUES(?1,?2,?3,(SELECT COALESCE(MAX(part),0)+1 FROM epochs WHERE name=?1))",
            params![name, path, generation],
        )?;
        Ok(self.db.last_insert_rowid())
    }
    pub fn ingest(
        &mut self,
        schema_hash: &str,
        items: &[Capture],
        checkpoint: Option<(&str, &Value)>,
    ) -> Result<Vec<i64>> {
        ensure!(self.writable, "read-only archive");
        let schema = self.schema(schema_hash)?;
        let mut prepared = Vec::new();
        let mut reserved = HashMap::new();
        for c in items {
            ensure!(c.bytes.len() <= MAX_PAYLOAD, "TL payload too large");
            let value = schema.decode(&c.root_type, &c.bytes)?;
            let mut hash = blake3::Hasher::new();
            hash.update(schema_hash.as_bytes());
            hash.update(c.root_type.as_bytes());
            hash.update(&c.bytes);
            let epoch = self.epoch(c.observed_at, &reserved)?;
            *reserved.entry(epoch).or_insert(0u64) += (c.bytes.len() * 2
                + c.metadata.to_string().len() * 2
                + c.key.len()
                + c.source.len()
                + 8192) as u64;
            prepared.push((
                c,
                hash.finalize().to_hex().to_string(),
                epoch,
                tl::text(&value),
            ));
        }
        let tx = self.db.transaction()?;
        let mut ids = vec![];
        for (c, hash, epoch, text) in prepared {
            if let Some(replay) = &c.replay_key
                && let Some(id) = tx
                    .query_row(
                        "SELECT id FROM observations WHERE replay_key=?1",
                        [replay],
                        |r| r.get::<_, i64>(0),
                    )
                    .optional()?
            {
                ids.push(id);
                continue;
            }
            tx.execute("INSERT OR IGNORE INTO payloads(hash,schema_hash,root_type,epoch,length,journal) VALUES(?1,?2,?3,?4,?5,?6)",params![hash,schema_hash,c.root_type,epoch,c.bytes.len(),c.bytes])?;
            tx.execute("INSERT INTO observations(key,kind,observed,source,payload,epoch,metadata,partial,deleted,replay_key) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![c.key,c.kind,c.observed_at,c.source,hash,epoch,c.metadata.to_string(),c.partial,c.deleted,c.replay_key])?;
            let id = tx.last_insert_rowid();
            ids.push(id);
            if self.config.index_history {
                tx.execute(
                    "INSERT INTO history_search(rowid,text) VALUES(?1,?2)",
                    params![id, text],
                )?;
            }
            let revision = c
                .metadata
                .get("revision")
                .and_then(tl::integer)
                .unwrap_or(c.observed_at);
            let head = tx
                .query_row(
                    "SELECT revision,partial FROM heads WHERE key=?1",
                    [&c.key],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, bool>(1)?)),
                )
                .optional()?;
            if head.is_none_or(|(r, p)| (p && !c.partial) || (p == c.partial && revision >= r)) {
                tx.execute(
                    "DELETE FROM search WHERE rowid=(SELECT observation FROM heads WHERE key=?1)",
                    [&c.key],
                )?;
                tx.execute("INSERT INTO heads VALUES(?1,?2,?3,?4) ON CONFLICT(key) DO UPDATE SET observation=excluded.observation,revision=excluded.revision,partial=excluded.partial",params![c.key,id,revision,c.partial])?;
                if !c.deleted {
                    tx.execute(
                        "INSERT INTO search(rowid,text) VALUES(?1,?2)",
                        params![id, text],
                    )?;
                }
            }
        }
        if let Some((key, value)) = checkpoint {
            if let Some(counter) = value.get("counter_key").and_then(Value::as_str) {
                tx.execute("INSERT INTO checkpoints VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![counter,value["messages"].to_string()])?;
            }
            tx.execute("INSERT INTO checkpoints VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,value.to_string()])?;
        }
        tx.commit()?;
        Ok(ids)
    }
    pub fn materialize(&mut self) -> Result<()> {
        self.materialize_with_hook(|_| Ok(()))
    }
    pub fn materialize_with_hook(
        &mut self,
        mut hook: impl FnMut(CommitPoint) -> Result<()>,
    ) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        loop {
            let pending: Option<i64> = self
                .db
                .query_row(
                    "SELECT epoch FROM payloads WHERE journal IS NOT NULL LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(epoch_id) = pending else { break };
            let path: String =
                self.db
                    .query_row("SELECT path FROM epochs WHERE id=?1", [epoch_id], |r| {
                        r.get(0)
                    })?;
            let mut epoch = connection(&self.root.join(path), true)?;
            let mut rows=self.db.prepare("SELECT hash,schema_hash,root_type,journal FROM payloads WHERE epoch=?1 AND journal IS NOT NULL ORDER BY hash")?;
            let mut iter = rows.query([epoch_id])?;
            let mut payloads = vec![];
            let mut raw = vec![];
            while let Some(r) = iter.next()? {
                let bytes: Vec<u8> = r.get(3)?;
                let offset = raw.len();
                raw.extend(&bytes);
                payloads.push((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    offset,
                    bytes.len(),
                ));
                if raw.len() >= self.config.block_bytes {
                    break;
                }
            }
            drop(iter);
            drop(rows);
            let compressed = zstd::bulk::compress(&raw, self.config.compression_level)?;
            let tx = epoch.transaction()?;
            tx.execute(
                "INSERT INTO blocks(codec,raw_size,checksum,data) VALUES('zstd',?1,?2,?3)",
                params![
                    raw.len(),
                    blake3::hash(&raw).to_hex().to_string(),
                    compressed
                ],
            )?;
            let block = tx.last_insert_rowid();
            for (hash, schema, root, off, len) in &payloads {
                let (layer, definition): (i32, String) = self.db.query_row(
                    "SELECT layer,definition FROM schemas WHERE hash=?1",
                    [schema],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO schemas VALUES(?1,?2,?3)",
                    params![schema, layer, definition],
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO payloads VALUES(?1,?2,?3,?4,?5,?6)",
                    params![hash, schema, root, block, off, len],
                )?;
            }
            tx.commit()?;
            hook(CommitPoint::EpochCommitted)?;
            let tx = self.db.transaction()?;
            for (hash, _, _, _, _) in &payloads {
                let (block, offset, length): (i64, i64, i64) = epoch.query_row(
                    "SELECT block,offset,length FROM payloads WHERE hash=?1",
                    [hash],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                tx.execute(
                    "UPDATE payloads SET block=?2,offset=?3,length=?4,journal=NULL WHERE hash=?1",
                    params![hash, block, offset, length],
                )?;
            }
            tx.commit()?;
            hook(CommitPoint::CatalogPublished)?;
        }
        // Epoch observation copies are replayable projections of the committed journal/catalog.
        let epochs = self.epochs()?;
        for (id, _, path, sealed) in epochs {
            if sealed {
                continue;
            }
            let mut epoch = connection(&self.root.join(path), true)?;
            loop {
                let high: i64 =
                    epoch.query_row("SELECT COALESCE(MAX(id),0) FROM observations", [], |r| {
                        r.get(0)
                    })?;
                let mut statement=self.db.prepare("SELECT id,key,kind,observed,source,payload,metadata,partial,deleted,transformed,replay_key FROM observations WHERE epoch=?1 AND id>?2 ORDER BY id LIMIT 128")?;
                let mut rows = statement.query(params![id, high])?;
                let tx = epoch.transaction()?;
                let mut copied = 0;
                while let Some(r) = rows.next()? {
                    copied += 1;
                    let values = (0..11)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    tx.execute(
                    "INSERT OR IGNORE INTO observations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    rusqlite::params_from_iter(values),
                )?;
                }
                tx.commit()?;
                if copied == 0 {
                    break;
                }
            }
            self.db.execute(
                "UPDATE epochs SET sealed=1 WHERE id=?1 AND accepting=0",
                [id],
            )?;
        }
        Ok(())
    }
    pub fn epochs(&self) -> Result<Vec<(i64, String, String, bool)>> {
        Ok(self
            .db
            .prepare("SELECT id,name,path,sealed FROM epochs ORDER BY id")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn payload(&self, hash: &str) -> Result<Vec<u8>> {
        let (schema,root,journal,path,block,offset,length):PayloadLocation=self.db.query_row("SELECT p.schema_hash,p.root_type,p.journal,e.path,p.block,p.offset,p.length FROM payloads p JOIN epochs e ON e.id=p.epoch WHERE p.hash=?1",[hash],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
        let bytes = if let Some(bytes) = journal {
            bytes
        } else {
            let block = block.context("payload block missing")?;
            let cache_key = (path.clone(), block);
            let cached = self.blocks.borrow().get(&cache_key).cloned();
            let raw = if let Some(raw) = cached {
                raw
            } else {
                let db = connection(&self.root.join(&path), false)?;
                let raw = std::sync::Arc::new(read_block(&db, block)?);
                let mut cache = self.blocks.borrow_mut();
                if cache.values().map(|v| v.len()).sum::<usize>() + raw.len() > 16 * 1024 * 1024 {
                    cache.clear();
                }
                if raw.len() <= 16 * 1024 * 1024 {
                    cache.insert(cache_key, raw.clone());
                }
                raw
            };
            let offset = offset.context("payload offset missing")?;
            raw.get(
                offset
                    ..offset
                        .checked_add(length)
                        .context("payload length overflow")?,
            )
            .context("invalid payload bounds")?
            .to_vec()
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(schema.as_bytes());
        hasher.update(root.as_bytes());
        hasher.update(&bytes);
        ensure!(
            hasher.finalize().to_hex().as_str() == hash,
            "payload checksum mismatch"
        );
        Ok(bytes)
    }
    pub fn record(&self, id: i64) -> Result<Record> {
        let mut record=self.db.query_row("SELECT o.id,o.key,o.kind,o.observed,o.source,o.payload,p.root_type,p.schema_hash,o.partial,o.deleted,o.transformed,o.metadata FROM observations o JOIN payloads p ON o.payload=p.hash WHERE o.id=?1",[id],|r|Ok(Record{sequence:r.get(0)?,key:r.get(1)?,kind:r.get(2)?,observed_at:r.get(3)?,source:r.get(4)?,payload_hash:r.get(5)?,root_type:r.get(6)?,schema_hash:r.get(7)?,partial:r.get(8)?,deleted:r.get(9)?,transformed:r.get(10)?,metadata:Value::String(r.get(11)?),data:Value::Null,attachments:vec![],representations:vec![]}))?;
        record.metadata = serde_json::from_str(record.metadata.as_str().unwrap())?;
        record.data = self
            .schema(&record.schema_hash)?
            .decode(&record.root_type, &self.payload(&record.payload_hash)?)?;
        record.attachments = self.attachment_hashes(&record.data)?;
        let extended: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='representations')",
            [],
            |r| r.get(0),
        )?;
        if extended {
            for hash in &record.attachments {
                let mut statement=self.db.prepare("SELECT original,hash,recipe,bytes FROM representations WHERE original=?1 OR hash=?1")?;
                for row in statement.query_map([hash], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, u64>(3)?,
                    ))
                })? {
                    let (original, hash, recipe, bytes) = row?;
                    let original_retained =
                        crate::media::attachment_path(&self.root, &original)?.exists();
                    record
                        .representations
                        .push(tg_backup_protocol::Representation {
                            original,
                            hash,
                            recipe,
                            bytes,
                            original_retained,
                        });
                }
            }
        }
        Ok(record)
    }
    pub fn checkpoint(&self, key: &str) -> Result<Option<Value>> {
        self.db
            .query_row("SELECT value FROM checkpoints WHERE key=?1", [key], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .map(|s| Ok(serde_json::from_str(&s)?))
            .transpose()
    }
    pub fn set_checkpoint(&self, key: &str, value: &Value) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        self.db.execute("INSERT INTO checkpoints VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,value.to_string()])?;
        Ok(())
    }
    pub fn coverage(&self, name: &str, status: &str, details: &Value) -> Result<()> {
        self.db.execute("INSERT INTO coverage VALUES(?1,?2,?3,?4) ON CONFLICT(name) DO UPDATE SET status=excluded.status,updated=excluded.updated,details=excluded.details",params![name,status,Utc::now().timestamp_micros(),details.to_string()])?;
        Ok(())
    }
    pub fn storage_details(&self) -> Result<Value> {
        let count = |table: &str| -> Result<i64> {
            Ok(self
                .db
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?)
        };
        let pages = |db: &Connection| -> Result<BTreeMap<String, i64>> {
            Ok(db
                .prepare("SELECT name,SUM(pgsize) FROM dbstat GROUP BY name")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?)
        };
        let media_bytes: i64 = self.db.query_row("SELECT COALESCE(SUM(bytes),0) FROM (SELECT hash,MAX(offset) bytes FROM media WHERE status='complete' GROUP BY hash)", [], |r| r.get(0))?;
        let mut sizes = BTreeMap::new();
        for (_, name, path, _) in self.epochs()? {
            let epoch = connection(&self.root.join(&path), false)?;
            let (compressed, raw): (i64, i64) = epoch.query_row(
                "SELECT COALESCE(SUM(length(data)),0),COALESCE(SUM(raw_size),0) FROM blocks",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let dictionaries: i64 = epoch.query_row(
                "SELECT COALESCE(SUM(length(data)),0) FROM dictionaries",
                [],
                |r| r.get(0),
            )?;
            sizes.insert(path.clone(),json!({"epoch":name,"bytes":fs::metadata(self.root.join(&path))?.len(),"compressed_payload_bytes":compressed,"uncompressed_payload_bytes":raw,"dictionary_bytes":dictionaries,"sqlite_pages_by_table":pages(&epoch)?}));
        }
        Ok(
            json!({"format_version":FORMAT_VERSION,"observations":count("observations")?,"objects":count("heads")?,"payloads":count("payloads")?,"media":count("media")?,"media_bytes":media_bytes,"catalog_pages_by_table":pages(&self.db)?,"epochs":sizes,"catalog_bytes":fs::metadata(self.root.join("catalog.sqlite3"))?.len(),"pending_payloads":self.db.query_row("SELECT COUNT(*) FROM payloads WHERE journal IS NOT NULL",[],|r|r.get::<_,i64>(0))?}),
        )
    }
    pub fn list_table(&self, table: &str) -> Result<Value> {
        ensure!(
            ["jobs", "coverage", "media", "maintenance"].contains(&table),
            "unsupported table"
        );
        let mut st = self
            .db
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let names = st
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut rows = st.query([])?;
        let mut out = vec![];
        while let Some(r) = rows.next()? {
            let mut obj = serde_json::Map::new();
            for (i, name) in names.iter().enumerate() {
                let v = match r.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => json!(n),
                    rusqlite::types::ValueRef::Real(n) => json!(n),
                    rusqlite::types::ValueRef::Text(s) => {
                        let s = std::str::from_utf8(s)?;
                        serde_json::from_str(s).unwrap_or(json!(s))
                    }
                    rusqlite::types::ValueRef::Blob(_) => bail!("unexpected blob"),
                };
                obj.insert(name.clone(), v);
            }
            out.push(Value::Object(obj));
        }
        Ok(json!(out))
    }
    pub fn verify(&self) -> Result<Value> {
        self.blocks.borrow_mut().clear();
        let integrity: String = self
            .db
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        ensure!(integrity == "ok", "catalog: {integrity}");
        let mut checked = 0;
        let mut st = self
            .db
            .prepare("SELECT hash,schema_hash,root_type FROM payloads ORDER BY hash")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            let hash: String = r.get(0)?;
            self.schema(&r.get::<_, String>(1)?)?
                .decode(&r.get::<_, String>(2)?, &self.payload(&hash)?)?;
            checked += 1;
        }
        for (_, _, path, _) in self.epochs()? {
            let db = connection(&self.root.join(path), false)?;
            let integrity: String = db.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
            ensure!(integrity == "ok", "epoch: {integrity}");
        }
        let violations: i64 =
            self.db
                .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
                    r.get(0)
                })?;
        ensure!(violations == 0, "dangling references");
        let mut statement = self
            .db
            .prepare("SELECT DISTINCT hash FROM media WHERE status='complete' UNION SELECT hash FROM representations")?;
        for row in statement.query_map([], |r| r.get::<_, String>(0))? {
            let hash = row?;
            let path = crate::media::attachment_path(&self.root, &hash)?;
            ensure!(
                crate::media::file_hash(&path)? == hash,
                "attachment checksum mismatch"
            );
        }
        Ok(json!({"verified_payloads":checked,"integrity":"ok"}))
    }
    pub fn reindex(&mut self) -> Result<()> {
        self.db.execute_batch("BEGIN IMMEDIATE;")?;
        let result = self.reindex_inner();
        if result.is_ok() {
            self.db.execute_batch("COMMIT;")?;
        } else {
            self.db.execute_batch("ROLLBACK;")?;
        }
        result
    }
    fn reindex_inner(&mut self) -> Result<()> {
        self.db.execute("DELETE FROM history_search", [])?;
        if self.config.index_history {
            let mut after = 0;
            loop {
                let ids = self.observation_ids(after, 128)?;
                if ids.is_empty() {
                    break;
                }
                for id in ids {
                    after = id;
                    let r = self.record(id)?;
                    self.db.execute(
                        "INSERT INTO history_search(rowid,text) VALUES(?1,?2)",
                        params![id, tl::text(&r.data)],
                    )?;
                }
            }
        }
        self.db.execute("DELETE FROM search", [])?;
        let mut after = 0;
        loop {
            let ids:Vec<i64>=self.db.prepare("SELECT observation FROM heads WHERE observation>?1 ORDER BY observation LIMIT 128")?.query_map([after],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                after = id;
                let r = self.record(id)?;
                if !r.deleted {
                    self.db.execute(
                        "INSERT INTO search(rowid,text) VALUES(?1,?2)",
                        params![id, tl::text(&r.data)],
                    )?;
                }
            }
        }
        Ok(())
    }
}
pub fn read_block(db: &Connection, id: i64) -> Result<Vec<u8>> {
    let (raw_size,checksum,data,dict):(usize,String,Vec<u8>,Option<Vec<u8>>)=db.query_row("SELECT b.raw_size,b.checksum,b.data,d.data FROM blocks b LEFT JOIN dictionaries d ON d.id=b.dictionary WHERE b.id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let dictionary: Option<i64> =
        db.query_row("SELECT dictionary FROM blocks WHERE id=?1", [id], |r| {
            r.get(0)
        })?;
    if let Some(id) = dictionary {
        let dict = dict.as_ref().context("missing compression dictionary")?;
        let hash: String =
            db.query_row("SELECT hash FROM dictionaries WHERE id=?1", [id], |r| {
                r.get(0)
            })?;
        ensure!(
            blake3::hash(dict).to_hex().as_str() == hash,
            "dictionary checksum mismatch"
        );
    }
    ensure!(raw_size <= MAX_PAYLOAD * 2, "invalid compressed block size");
    let mut decoder = zstd::bulk::Decompressor::with_dictionary(dict.as_deref().unwrap_or(&[]))?;
    let bytes = decoder.decompress(&data, raw_size)?;
    ensure!(
        bytes.len() == raw_size && blake3::hash(&bytes).to_hex().as_str() == checksum,
        "block checksum mismatch"
    );
    Ok(bytes)
}
pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Retention {
    /// Observation age cutoff in UTC microseconds. No data loss without an explicit cutoff.
    pub before: Option<i64>,
    pub drop_kinds: Vec<String>,
    pub intermediate_versions: bool,
    pub coalesce_observations: bool,
    /// Fully qualified constructor.field names; only optional non-identity fields are accepted.
    pub remove_fields: Vec<String>,
}
impl Retention {
    pub fn lossy(&self) -> bool {
        !self.drop_kinds.is_empty()
            || self.intermediate_versions
            || self.coalesce_observations
            || !self.remove_fields.is_empty()
    }
}
#[derive(Debug, Clone, Default)]
pub struct Maintenance {
    pub apply: bool,
    pub consolidate_yearly: bool,
    pub seal: bool,
    pub closed_only: bool,
    pub retention: Retention,
}
impl Archive {
    pub fn maintain(&mut self, options: &Maintenance) -> Result<Value> {
        self.maintain_with_hook(options, |_| Ok(()))
    }
    pub fn maintain_with_hook(
        &mut self,
        options: &Maintenance,
        mut hook: impl FnMut(CommitPoint) -> Result<()>,
    ) -> Result<Value> {
        ensure!(self.writable, "maintenance needs writer access");
        ensure!(
            !options.retention.lossy() || options.retention.before.is_some(),
            "lossy retention requires an observation cutoff"
        );
        let cutoff = options.retention.before.unwrap_or(i64::MIN);
        // Validate fields before modifying any representation, even when the archive has no match.
        for field in &options.retention.remove_fields {
            let (constructor, name) = field
                .rsplit_once('.')
                .context("retention field must be constructor.field")?;
            let mut found = false;
            let mut st = self.db.prepare("SELECT definition FROM schemas")?;
            for definition in st.query_map([], |r| r.get::<_, String>(0))? {
                let s = Schema::parse(&definition?)?;
                let mut value = json!({"_":constructor,name:Value::Null});
                if s.redact(&mut value, std::slice::from_ref(field)).is_ok() {
                    found = true;
                }
            }
            ensure!(
                found,
                "unknown, required, or structural retention field: {field}"
            );
        }
        let mut remove = 0u64;
        let mut redact = 0u64;
        let mut scan_after = 0;
        loop {
            let ids = self.observation_ids(scan_after, 128)?;
            if ids.is_empty() {
                break;
            }
            for id in &ids {
                scan_after = *id;
                let record = self.record(*id)?;
                if self.should_remove(&record, &options.retention, cutoff)? {
                    remove += 1;
                } else if record.observed_at < cutoff && !options.retention.remove_fields.is_empty()
                {
                    let mut v = record.data;
                    redact += self
                        .schema(&record.schema_hash)?
                        .redact(&mut v, &options.retention.remove_fields)?
                        as u64;
                }
            }
        }
        let report = json!({"apply":options.apply,"remove_observations":remove,"remove_fields":redact,"consolidate_yearly":options.consolidate_yearly,"seal":options.seal,"lossy":options.retention.lossy(),"note":"Intermediate-version retention also removes old raw update/RPC envelopes that could retain discarded versions. Latest object states and tombstones remain."});
        if !options.apply {
            return Ok(report);
        }
        self.materialize()?;
        let readers = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.root.join("readers.lock"))?;
        readers.lock_exclusive()?;
        let token = uuid::Uuid::new_v4().to_string();
        let staged_path = self
            .root
            .join("staging")
            .join(format!("catalog-{token}.sqlite3"));
        self.db.execute(
            "VACUUM INTO ?1",
            [staged_path.to_str().context("dataset path must be UTF-8")?],
        )?;
        let stage = connection(&staged_path, true)?;
        stage.execute_batch("PRAGMA foreign_keys=OFF;")?;
        let mut scan_after = 0;
        loop {
            let ids = self.observation_ids(scan_after, 128)?;
            if ids.is_empty() {
                break;
            }
            for id in &ids {
                scan_after = *id;
                let record = self.record(*id)?;
                if self.should_remove(&record, &options.retention, cutoff)? {
                    stage.execute("DELETE FROM media_refs WHERE observation=?1", [id])?;
                    stage.execute("DELETE FROM observations WHERE id=?1", [id])?;
                } else if record.observed_at < cutoff && !options.retention.remove_fields.is_empty()
                {
                    let mut value = record.data;
                    let schema = self.schema(&record.schema_hash)?;
                    if schema.redact(&mut value, &options.retention.remove_fields)? > 0 {
                        let bytes = schema.encode(&record.root_type, &value)?;
                        schema.decode(&record.root_type, &bytes)?;
                        let mut h = blake3::Hasher::new();
                        h.update(record.schema_hash.as_bytes());
                        h.update(record.root_type.as_bytes());
                        h.update(&bytes);
                        let hash = h.finalize().to_hex().to_string();
                        let epoch: i64 = stage.query_row(
                            "SELECT epoch FROM observations WHERE id=?1",
                            [id],
                            |r| r.get(0),
                        )?;
                        stage.execute("INSERT OR IGNORE INTO payloads(hash,schema_hash,root_type,epoch,length,journal) VALUES(?1,?2,?3,?4,?5,?6)",params![hash,record.schema_hash,record.root_type,epoch,bytes.len(),bytes])?;
                        stage.execute(
                            "UPDATE observations SET payload=?2,transformed=1 WHERE id=?1",
                            params![id, hash],
                        )?;
                    }
                }
            }
        }
        if !options.retention.remove_fields.is_empty() {
            // Operational peer caches are disposable projections; rebuilding avoids retaining stripped fields.
            let has_peers: bool = stage.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='peers')",
                [],
                |r| r.get(0),
            )?;
            if has_peers {
                stage.execute("DELETE FROM peers", [])?;
            }
            stage.execute("UPDATE observations SET metadata=json_remove(metadata,'$.title') WHERE observed<?1 AND transformed=1",[cutoff])?;
        }
        stage.execute(
            "DELETE FROM payloads WHERE hash NOT IN(SELECT payload FROM observations)",
            [],
        )?;
        stage.execute(
            "DELETE FROM heads WHERE observation NOT IN(SELECT id FROM observations)",
            [],
        )?;
        stage.execute(
            "DELETE FROM media WHERE id NOT IN(SELECT media FROM media_refs)",
            [],
        )?;
        stage.execute("DELETE FROM search", [])?;
        // Repack from the staged graph. Cross-epoch references are rewritten, never duplicated.
        let mut groups: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        let current = self.config.epoch.key(Utc::now());
        let mut replaced = Vec::new();
        let mut sealed_ids = std::collections::HashSet::new();
        for (id, name, path, sealed) in self.epochs()? {
            if options.closed_only && (name == current || sealed) {
                continue;
            }
            if sealed {
                sealed_ids.insert(id);
            }
            replaced.push(path);
            groups
                .entry(if options.consolidate_yearly {
                    name[..4].into()
                } else {
                    name
                })
                .or_default()
                .push(id);
        }
        for (name, old_ids) in groups {
            let path = format!("epochs/{name}.g-{token}.sqlite3");
            let mut out = connection(&self.root.join(&path), true)?;
            out.execute_batch(EPOCH)?;
            let list = old_ids
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let query = format!(
                "SELECT hash,schema_hash,root_type,journal FROM payloads WHERE epoch IN ({list}) ORDER BY hash"
            );
            let mut samples = vec![];
            let mut sample_bytes = 0;
            if self.config.retrain {
                let mut st = stage.prepare(&query)?;
                let mut rows = st.query([])?;
                while let Some(r) = rows.next()? {
                    let bytes = match r.get::<_, Option<Vec<u8>>>(3)? {
                        Some(b) => b,
                        None => self.payload(&r.get::<_, String>(0)?)?,
                    };
                    if bytes.len() > 128 * 1024 {
                        continue;
                    }
                    sample_bytes += bytes.len();
                    samples.push(bytes);
                    if sample_bytes >= 16 * 1024 * 1024 || samples.len() >= 8192 {
                        break;
                    }
                }
            }
            let candidate = if samples.len() >= 64 && sample_bytes >= 64 * 1024 {
                zstd::dict::from_samples(&samples, 64 * 1024).unwrap_or_default()
            } else {
                vec![]
            };
            let dictionary = if candidate.is_empty() {
                vec![]
            } else {
                let mut with = zstd::bulk::Compressor::with_dictionary(
                    self.config.compression_level,
                    &candidate,
                )?;
                let mut saved = 0i64;
                for s in &samples {
                    saved += zstd::bulk::compress(s, self.config.compression_level)?.len() as i64
                        - with.compress(s)?.len() as i64;
                }
                if saved > candidate.len() as i64 {
                    candidate
                } else {
                    vec![]
                }
            };
            drop(samples);
            if !dictionary.is_empty() {
                out.execute(
                    "INSERT INTO dictionaries VALUES(1,?1,?2)",
                    params![dictionary, blake3::hash(&dictionary).to_hex().to_string()],
                )?;
            }
            let new_id: i64 =
                stage.query_row("SELECT COALESCE(MAX(id),0)+1 FROM epochs", [], |r| r.get(0))?;
            stage.execute(
                "INSERT INTO epochs(id,name,path,sealed,generation) VALUES(?1,?2,?3,?4,1)",
                params![
                    new_id,
                    name,
                    path,
                    options.seal || old_ids.iter().all(|id| sealed_ids.contains(id))
                ],
            )?;
            let mut hashes: Vec<(String, String, String, usize, usize)> = vec![];
            let mut raw = vec![];
            // Read a bounded page at a time so writing payload locations never invalidates an iterator.
            let mut last = String::new();
            loop {
                let batch:Vec<(String,String,String,Option<Vec<u8>>)>=stage.prepare(&format!("SELECT hash,schema_hash,root_type,journal FROM payloads WHERE epoch IN ({list}) AND hash>?1 ORDER BY hash LIMIT 32"))?.query_map([&last],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?.collect::<rusqlite::Result<_>>()?;
                if batch.is_empty() {
                    break;
                }
                for (hash, schema, root, journal) in batch {
                    last = hash.clone();
                    let bytes = journal.map(Ok).unwrap_or_else(|| self.payload(&hash))?;
                    hashes.push((hash, schema, root, raw.len(), bytes.len()));
                    raw.extend(bytes);
                    if raw.len() >= self.config.block_bytes {
                        write_repacked(
                            &mut out,
                            &stage,
                            new_id,
                            &mut hashes,
                            &mut raw,
                            &dictionary,
                            self.config.compression_level,
                        )?;
                    }
                }
            }
            if !hashes.is_empty() {
                write_repacked(
                    &mut out,
                    &stage,
                    new_id,
                    &mut hashes,
                    &mut raw,
                    &dictionary,
                    self.config.compression_level,
                )?;
            }
            if !dictionary.is_empty() {
                let ids: Vec<i64> = out
                    .prepare("SELECT id FROM blocks WHERE dictionary IS NOT NULL")?
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                let mut saved = 0i64;
                for id in &ids {
                    let raw = read_block(&out, *id)?;
                    let actual: i64 =
                        out.query_row("SELECT length(data) FROM blocks WHERE id=?1", [id], |r| {
                            r.get(0)
                        })?;
                    saved += zstd::bulk::compress(&raw, self.config.compression_level)?.len()
                        as i64
                        - actual;
                }
                if saved <= dictionary.len() as i64 {
                    for id in ids {
                        let data = zstd::bulk::compress(
                            &read_block(&out, id)?,
                            self.config.compression_level,
                        )?;
                        out.execute(
                            "UPDATE blocks SET dictionary=NULL,data=?2 WHERE id=?1",
                            params![id, data],
                        )?;
                    }
                    out.execute("DELETE FROM dictionaries", [])?;
                }
            }
            stage.execute(
                &format!("UPDATE observations SET epoch={new_id} WHERE epoch IN ({list})"),
                [],
            )?;
            let tx = out.transaction()?;
            let mut st=stage.prepare("SELECT id,key,kind,observed,source,payload,metadata,partial,deleted,transformed,replay_key FROM observations WHERE epoch=?1")?;
            let mut rows = st.query([new_id])?;
            while let Some(r) = rows.next()? {
                let values = (0..11)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                tx.execute(
                    "INSERT INTO observations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    rusqlite::params_from_iter(values),
                )?;
            }
            tx.commit()?;
            out.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
            drop(out);
            File::open(self.root.join(&path))?.sync_all()?;
            stage.execute(&format!("DELETE FROM epochs WHERE id IN ({list})"), [])?;
        }
        for path in replaced {
            stage.execute("INSERT OR IGNORE INTO retired VALUES(?1)", [path])?;
        }
        stage.execute(
            "INSERT INTO maintenance VALUES(?1,?2,?3,?4)",
            params![
                token,
                Utc::now().timestamp_micros(),
                serde_json::to_string(&options.retention)?,
                report.to_string()
            ],
        )?;
        stage.execute("INSERT INTO checkpoints VALUES('maintenance_generation',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[json!(token).to_string()])?;
        stage.execute_batch("PRAGMA foreign_keys=ON;")?;
        let bad: i64 =
            stage.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })?;
        ensure!(bad == 0, "maintenance created dangling references");
        // Rebuild search against staged payload locations before publishing.
        let staged_archive = Archive {
            root: self.root.clone(),
            db: stage,
            config: self.config.clone(),
            writable: true,
            schemas: Default::default(),
            blocks: Default::default(),
            _lock: self._lock.try_clone()?,
        };
        let mut staged_archive = staged_archive;
        staged_archive.split_oversized()?;
        staged_archive.reindex()?;
        staged_archive.verify()?;
        staged_archive.db.execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE; VACUUM;",
        )?;
        drop(staged_archive);
        File::open(&staged_path)?.sync_all()?;
        hook(CommitPoint::GenerationReady)?;
        sync_dir(&self.root.join("epochs"))?;
        self.db
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
        let old = std::mem::replace(&mut self.db, Connection::open_in_memory()?);
        drop(old);
        fs::rename(&staged_path, self.root.join("catalog.sqlite3"))?;
        sync_dir(&self.root)?;
        self.db = connection(&self.root.join("catalog.sqlite3"), true)?;
        self.blocks.borrow_mut().clear();
        hook(CommitPoint::GenerationPublished)?;
        // Readers were drained before replacement, so no process can still reference retired files.
        let retired: Vec<String> = self
            .db
            .prepare("SELECT path FROM retired")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for path in retired {
            for suffix in ["", "-wal", "-shm"] {
                let path = self.root.join(format!("{path}{suffix}"));
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
        self.db.execute("DELETE FROM retired", [])?;
        self.gc_attachments()?;
        drop(readers);
        Ok(report)
    }
    pub fn observation_ids(&self, after: i64, limit: usize) -> Result<Vec<i64>> {
        Ok(self
            .db
            .prepare("SELECT id FROM observations WHERE id>?1 ORDER BY id LIMIT ?2")?
            .query_map(params![after, limit], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn seal_due(&mut self) -> Result<()> {
        let current = self.config.epoch.key(Utc::now());
        if self
            .epochs()?
            .iter()
            .any(|(_, name, _, sealed)| !sealed && name != &current)
        {
            self.maintain(&Maintenance {
                apply: true,
                seal: true,
                closed_only: true,
                ..Default::default()
            })?;
        }
        Ok(())
    }
    fn recover_retired(&self) -> Result<()> {
        let paths: Vec<String> = self
            .db
            .prepare("SELECT path FROM retired")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let staged: Vec<_> = fs::read_dir(self.root.join("staging"))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let token = name.strip_prefix("catalog-")?.strip_suffix(".sqlite3")?;
                uuid::Uuid::parse_str(token).ok()?;
                Some((entry.path(), token.to_owned()))
            })
            .collect();
        if paths.is_empty() && staged.is_empty() {
            return Ok(());
        }
        let readers = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.root.join("readers.lock"))?;
        readers.lock_exclusive()?;
        for path in paths {
            ensure!(
                path.starts_with("epochs/") && !path.contains(".."),
                "invalid retired path"
            );
            for suffix in ["", "-wal", "-shm"] {
                let p = self.root.join(format!("{path}{suffix}"));
                if p.exists() {
                    fs::remove_file(p)?;
                }
            }
        }
        self.db.execute("DELETE FROM retired", [])?;
        for (catalog, token) in staged {
            for entry in fs::read_dir(self.root.join("epochs"))? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.ends_with(&format!(".g-{token}.sqlite3")) {
                    continue;
                }
                let path = format!("epochs/{name}");
                let live: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM epochs WHERE path=?1)",
                    [&path],
                    |r| r.get(0),
                )?;
                if !live {
                    for suffix in ["", "-wal", "-shm"] {
                        let p = self.root.join(format!("{path}{suffix}"));
                        if p.exists() {
                            fs::remove_file(p)?;
                        }
                    }
                }
            }
            for suffix in ["", "-wal", "-shm"] {
                let p = PathBuf::from(format!("{}{suffix}", catalog.display()));
                if p.exists() {
                    fs::remove_file(p)?;
                }
            }
        }
        self.gc_attachments()?;
        Ok(())
    }
    fn should_remove(&self, r: &Record, policy: &Retention, cutoff: i64) -> Result<bool> {
        if r.observed_at >= cutoff || r.deleted {
            return Ok(false);
        }
        let latest: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM heads WHERE observation=?1)",
            [r.sequence],
            |r| r.get(0),
        )?;
        if policy.drop_kinds.contains(&r.kind) {
            return Ok(!latest);
        }
        if policy.coalesce_observations && !latest {
            let previous:Option<String>=self.db.query_row("SELECT payload FROM observations WHERE key=?1 AND id<?2 ORDER BY id DESC LIMIT 1",params![r.key,r.sequence],|r|r.get(0)).optional()?;
            if previous.as_deref() == Some(&r.payload_hash) {
                return Ok(true);
            }
        }
        if !policy.intermediate_versions {
            return Ok(false);
        }
        if matches!(r.kind.as_str(), "update" | "rpc") {
            let bytes = self.payload(&r.payload_hash)?;
            let (_, slices) = self
                .schema(&r.schema_hash)?
                .decode_slices(&r.root_type, &bytes)?;
            if slices.iter().any(|s| s.root == "Message") {
                return Ok(true);
            }
        }
        if latest {
            return Ok(false);
        }
        let first: i64 = self.db.query_row(
            "SELECT MIN(id) FROM observations WHERE key=?1",
            [&r.key],
            |r| r.get(0),
        )?;
        Ok(first != r.sequence)
    }
    fn gc_attachments(&self) -> Result<()> {
        self.db.execute("DELETE FROM representations WHERE original NOT IN(SELECT hash FROM media WHERE hash IS NOT NULL) AND hash NOT IN(SELECT hash FROM media WHERE hash IS NOT NULL)",[])?;
        for dir in fs::read_dir(self.root.join("attachments"))? {
            let dir = dir?;
            if !dir.file_type()?.is_dir() {
                continue;
            }
            for file in fs::read_dir(dir.path())? {
                let file = file?;
                let hash = file.file_name().to_string_lossy().into_owned();
                if hash.len() != 64 {
                    continue;
                }
                let used: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM media WHERE hash=?1 UNION SELECT 1 FROM representations WHERE hash=?1)",
                    [&hash],
                    |r| r.get(0),
                )?;
                if !used {
                    fs::remove_file(file.path())?;
                }
            }
        }
        Ok(())
    }
}
fn write_repacked(
    out: &mut Connection,
    stage: &Connection,
    epoch: i64,
    payloads: &mut Vec<(String, String, String, usize, usize)>,
    raw: &mut Vec<u8>,
    dictionary: &[u8],
    level: i32,
) -> Result<()> {
    let plain = zstd::bulk::compress(raw, level)?;
    let trained = if dictionary.is_empty() {
        vec![]
    } else {
        zstd::bulk::Compressor::with_dictionary(level, dictionary)?.compress(raw)?
    };
    let use_dict = !dictionary.is_empty() && trained.len() < plain.len();
    let data = if use_dict { trained } else { plain };
    let checksum = blake3::hash(raw).to_hex().to_string();
    let tx = out.transaction()?;
    tx.execute(
        "INSERT INTO blocks(codec,dictionary,raw_size,checksum,data) VALUES('zstd',?1,?2,?3,?4)",
        params![
            if use_dict { Some(1) } else { None },
            raw.len(),
            checksum,
            data
        ],
    )?;
    let block = tx.last_insert_rowid();
    for (hash, schema, root, offset, len) in payloads.iter() {
        let (layer, definition): (i32, String) = stage.query_row(
            "SELECT layer,definition FROM schemas WHERE hash=?1",
            [schema],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO schemas VALUES(?1,?2,?3)",
            params![schema, layer, definition],
        )?;
        tx.execute(
            "INSERT INTO payloads VALUES(?1,?2,?3,?4,?5,?6)",
            params![hash, schema, root, block, offset, len],
        )?;
    }
    tx.commit()?;
    ensure!(
        read_block(out, block)? == *raw,
        "compression round-trip mismatch"
    );
    for (hash, _, _, offset, len) in payloads.drain(..) {
        stage.execute(
            "UPDATE payloads SET epoch=?2,block=?3,offset=?4,length=?5,journal=NULL WHERE hash=?1",
            params![hash, epoch, block, offset, len],
        )?;
    }
    raw.clear();
    Ok(())
}

impl Archive {
    /// Split replacement generations by allocated SQLite size, preserving complete
    /// independently decodable blocks. A final block or observation may overshoot.
    fn split_oversized(&mut self) -> Result<()> {
        let cap = self.config.max_epoch_bytes;
        if cap == 0 {
            return Ok(());
        }
        for (old_id, name, old_path, sealed) in self.epochs()? {
            let input = connection(&self.root.join(&old_path), false)?;
            if allocated(&input)? <= cap {
                continue;
            }
            let mut part = 0i64;
            let mut create = || -> Result<(Connection, i64, String)> {
                part += 1;
                let path = format!("epochs/{name}.p{part:04}.{}.sqlite3", uuid::Uuid::new_v4());
                let out = connection(&self.root.join(&path), true)?;
                out.execute_batch(EPOCH)?;
                self.db.execute("INSERT INTO epochs(name,path,sealed,generation,accepting,part) VALUES(?1,?2,?3,1,0,?4)",params![name,path,sealed,part])?;
                Ok((out, self.db.last_insert_rowid(), path))
            };
            let (mut out, mut id, mut path) = create()?;
            let mut wrote = false;
            let mut st = input.prepare(
                "SELECT id,codec,dictionary,raw_size,checksum,data FROM blocks ORDER BY id",
            )?;
            let mut rows = st.query([])?;
            while let Some(row) = rows.next()? {
                if wrote && allocated(&out)? >= cap {
                    out.execute_batch(
                        "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;",
                    )?;
                    (out, id, path) = create()?;
                }
                let block: i64 = row.get(0)?;
                let dictionary: Option<i64> = row.get(2)?;
                if let Some(dict) = dictionary {
                    let (data, hash): (Vec<u8>, String) = input.query_row(
                        "SELECT data,hash FROM dictionaries WHERE id=?1",
                        [dict],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    out.execute(
                        "INSERT OR IGNORE INTO dictionaries VALUES(?1,?2,?3)",
                        params![dict, data, hash],
                    )?;
                }
                out.execute(
                    "INSERT INTO blocks VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        block,
                        row.get::<_, String>(1)?,
                        dictionary,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Vec<u8>>(5)?
                    ],
                )?;
                let mut p=input.prepare("SELECT hash,schema_hash,root_type,block,offset,length FROM payloads WHERE block=?1")?;
                let mut ps = p.query([block])?;
                while let Some(r) = ps.next()? {
                    let hash: String = r.get(0)?;
                    let schema: String = r.get(1)?;
                    let (layer, definition): (i32, String) = input.query_row(
                        "SELECT layer,definition FROM schemas WHERE hash=?1",
                        [&schema],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    out.execute(
                        "INSERT OR IGNORE INTO schemas VALUES(?1,?2,?3)",
                        params![schema, layer, definition],
                    )?;
                    let values = (0..6)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    out.execute(
                        "INSERT INTO payloads VALUES(?1,?2,?3,?4,?5,?6)",
                        rusqlite::params_from_iter(values),
                    )?;
                    self.db.execute(
                        "UPDATE payloads SET epoch=?2 WHERE hash=?1 AND epoch=?3",
                        params![hash, id, old_id],
                    )?;
                }
                wrote = true;
            }
            let mut st=input.prepare("SELECT id,key,kind,observed,source,payload,metadata,partial,deleted,transformed,replay_key FROM observations ORDER BY id")?;
            let mut rows = st.query([])?;
            while let Some(row) = rows.next()? {
                if wrote && allocated(&out)? >= cap {
                    out.execute_batch(
                        "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;",
                    )?;
                    (out, id, path) = create()?;
                }
                let values = (0..11)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                out.execute(
                    "INSERT INTO observations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    rusqlite::params_from_iter(values),
                )?;
                self.db.execute(
                    "UPDATE observations SET epoch=?2 WHERE id=?1 AND epoch=?3",
                    params![row.get::<_, i64>(0)?, id, old_id],
                )?;
                wrote = true;
            }
            out.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
            File::open(self.root.join(path))?.sync_all()?;
            self.db
                .execute("DELETE FROM epochs WHERE id=?1", [old_id])?;
            self.db
                .execute("INSERT OR IGNORE INTO retired VALUES(?1)", [old_path])?;
        }
        sync_dir(&self.root.join("epochs"))?;
        self.blocks.borrow_mut().clear();
        Ok(())
    }
}
fn allocated(db: &Connection) -> Result<u64> {
    let count: u64 = db.pragma_query_value(None, "page_count", |r| r.get(0))?;
    let size: u64 = db.pragma_query_value(None, "page_size", |r| r.get(0))?;
    Ok(count * size)
}
