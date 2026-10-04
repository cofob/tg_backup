use super::{CATALOG, Select, rows::*};
use crate::config::ClickHouse;
use anyhow::{Context, Result, ensure};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{cell::Cell, future::Future, sync::mpsc, thread, time::Duration};

type Job = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send>;
struct Executor(mpsc::Sender<Job>);
impl Executor {
    fn new() -> Result<Self> {
        let (send, receive) = mpsc::channel::<Job>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        thread::Builder::new()
            .name("clickhouse".into())
            .spawn(move || {
                for job in receive {
                    job(&runtime);
                }
            })?;
        Ok(Self(send))
    }
    fn run<T: Send + 'static>(
        &self,
        future: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        let (send, receive) = mpsc::sync_channel(1);
        self.0
            .send(Box::new(move |runtime| {
                let _ = send.send(runtime.block_on(future));
            }))
            .map_err(|_| anyhow::anyhow!("ClickHouse executor stopped"))?;
        receive.recv().context("ClickHouse executor stopped")?
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct Intent {
    id: i64,
    body: String,
    checksum: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct Commit {
    id: i64,
}
#[derive(Deserialize, Row)]
struct Number {
    value: i64,
}
#[derive(Deserialize, Row)]
struct Identity {
    _scope: String,
    _key: String,
    _row: i64,
}

pub struct ClickHouseStore {
    client: Client,
    executor: Executor,
    snapshot: Cell<i64>,
    writable: bool,
    timeout: Duration,
    pending: Cell<bool>,
    frozen: Cell<bool>,
}
pub struct Snapshot<'a> {
    store: &'a ClickHouseStore,
    previous: i64,
    frozen: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchPoint {
    JournalCommitted,
    RowsInserted,
    Committed,
}
impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.store.snapshot.set(self.previous);
        self.store.frozen.set(self.frozen);
    }
}
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl ClickHouseStore {
    pub fn connect(config: &ClickHouse, writable: bool) -> Result<Self> {
        config.validate()?;
        let executor = Executor::new()?;
        let config = config.clone();
        let timeout = Duration::from_secs(config.timeout_seconds);
        let client = executor.run(async move {
            let mut client = Client::default()
                .with_url(config.url)
                .with_database(config.database)
                .with_user(config.user)
                .with_setting("async_insert", "0")
                .with_setting("max_execution_time", config.timeout_seconds.to_string());
            if let Some(password) = config.password {
                client = client.with_password(password.load().await?);
            }
            Ok(client)
        })?;
        Ok(Self {
            client,
            executor,
            snapshot: Cell::new(0),
            writable,
            timeout,
            pending: Cell::new(false),
            frozen: Cell::new(false),
        })
    }
    pub fn execute(&self, sql: String) -> Result<()> {
        let client = self.client.clone();
        let timeout = self.timeout;
        self.executor.run(async move {
            tokio::time::timeout(timeout, client.query_raw(&sql).execute())
                .await
                .context("ClickHouse timeout")??;
            Ok(())
        })
    }
    pub fn query<T>(&self, sql: String, values: Vec<Value>) -> Result<Vec<T>>
    where
        T: clickhouse::RowOwned + clickhouse::RowRead + Send + 'static,
    {
        let client = self.client.clone();
        let timeout = self.timeout;
        self.executor.run(async move {
            let mut query = client.query(&sql);
            for value in values {
                query = match value {
                    Value::String(s) => query.bind(s),
                    Value::Number(n) => {
                        query.bind(n.as_i64().context("invalid integer parameter")?)
                    }
                    Value::Bool(b) => query.bind(b),
                    Value::Null => query.bind(Option::<String>::None),
                    _ => anyhow::bail!("unsupported ClickHouse parameter"),
                };
            }
            Ok(tokio::time::timeout(timeout, query.fetch_all::<T>())
                .await
                .context("ClickHouse timeout")??)
        })
    }
    pub fn number(&self, sql: String, values: Vec<Value>) -> Result<i64> {
        Ok(self
            .query::<Number>(sql, values)?
            .first()
            .context("missing scalar")?
            .value)
    }
    fn create<T: CatalogRow>(&self) -> Result<()> {
        let columns = T::columns()
            .iter()
            .map(|(name, ty)| format!("{} {ty}", quoted(name)))
            .collect::<Vec<_>>()
            .join(",");
        let index = if T::TABLE == "text" {
            ", INDEX text_idx lowerUTF8(text) TYPE text(tokenizer = icu('en'))"
        } else {
            ""
        };
        self.execute(format!("CREATE TABLE IF NOT EXISTS {} ({columns},_scope String,_key String,_row Int64,_batch Int64,_deleted UInt8{index}) ENGINE=ReplacingMergeTree ORDER BY (_scope,_key,_batch) SETTINGS fsync_after_insert=1,fsync_part_directory=1", quoted(T::TABLE)))
    }
    pub fn init(&self, database: &str) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        let client = self.client.clone().with_database("default");
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {} ENGINE=Atomic",
            quoted(database)
        );
        self.executor.run(async move {
            client.query_raw(&sql).execute().await?;
            Ok(())
        })?;
        let count = self.number(
            "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase()"
                .into(),
            vec![],
        )?;
        ensure!(count == 0, "ClickHouse database is not empty");
        self.execute("CREATE TABLE intents(id Int64,body String,checksum String) ENGINE=ReplacingMergeTree ORDER BY id SETTINGS fsync_after_insert=1,fsync_part_directory=1".into())?;
        self.execute("CREATE TABLE commits(id Int64) ENGINE=ReplacingMergeTree ORDER BY id SETTINGS fsync_after_insert=1,fsync_part_directory=1".into())?;
        macro_rules! tables { ($($row:ty),+) => { $(self.create::<$row>()?;)+ }; }
        tables!(
            Setting,
            CounterRow,
            SchemaRow,
            EpochRow,
            PayloadRow,
            ObservationRow,
            HeadRow,
            CheckpointRow,
            JobRow,
            CoverageRow,
            MediaRow,
            MediaRefRow,
            MaintenanceRow,
            RetiredRow,
            WorkRow,
            RepresentationRow,
            TransformationRow,
            RuntimeRow,
            PeerRow,
            FolderRow,
            DictionaryRow,
            BlockRow,
            SearchRow
        );
        let mut version = Change::put(
            CATALOG,
            &Setting {
                key: "storage_schema".into(),
                value: "1".into(),
            },
        )?;
        version.row = -1;
        self.commit(vec![version])?;
        Ok(())
    }
    pub fn open(&self) -> Result<()> {
        let high = self.number(
            "SELECT toInt64(ifNull(max(id),0)) AS value FROM commits".into(),
            vec![],
        )?;
        self.snapshot.set(high);
        if self.writable {
            self.recover()?;
        }
        let version = self.select::<Setting>(CATALOG, &Select::eq("key", "storage_schema"))?;
        ensure!(
            version.first().is_some_and(|r| r.value == "1"),
            "unsupported ClickHouse schema"
        );
        Ok(())
    }
    pub fn snapshot(&self) -> i64 {
        self.snapshot.get()
    }
    pub fn export(
        &self,
        table: &str,
        scope: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<Change>> {
        #[derive(Deserialize, Row)]
        struct Export {
            row: i64,
            json_data: String,
        }
        let (columns, keys) = table_columns(table)?;
        let fields = columns
            .iter()
            .map(|(name, _)| quoted(name))
            .collect::<Vec<_>>()
            .join(",");
        self.query::<Export>(format!("SELECT _row AS row,toJSONString(tuple({fields})) AS json_data FROM {} WHERE _row>? ORDER BY _row LIMIT {limit} SETTINGS output_format_json_named_tuples_as_objects=1,output_format_json_quote_64bit_integers=0",self.source(table,scope)?), vec![json!(after)])?.into_iter().map(|row| {
            let values: Vec<Value> = serde_json::from_str(&row.json_data)?;
            ensure!(values.len() == columns.len(),"invalid exported row");
            let data = Value::Object(columns.iter().zip(values).map(|((name,_),value)| ((*name).into(),value)).collect());
            let key = serde_json::to_string(&keys.iter().map(|key| &data[*key]).collect::<Vec<_>>())?;
            Ok(Change { table: table.into(), scope: scope.into(), key, row: row.row, deleted: false, data })
        }).collect()
    }
    pub fn freeze(&self, snapshot: i64) -> Result<Snapshot<'_>> {
        ensure!(
            snapshot >= 0 && snapshot <= self.snapshot.get(),
            "invalid ClickHouse snapshot"
        );
        let previous = self.snapshot.replace(snapshot);
        let frozen = self.frozen.replace(true);
        Ok(Snapshot {
            store: self,
            previous,
            frozen,
        })
    }
    pub fn source(&self, table: &str, scope: &str) -> Result<String> {
        if self.writable && self.pending.get() && !self.frozen.get() {
            self.recover()?;
        }
        ensure!(
            Self::table_names().contains(&table),
            "unknown archive table"
        );
        // Keep versions from all committed batches. Filtering precedes latest-state selection.
        let escaped = scope.replace('\\', "\\\\").replace('\'', "\\'");
        Ok(format!(
            "(SELECT * EXCEPT (_rank) FROM (SELECT *,row_number() OVER (PARTITION BY _scope,_key ORDER BY _batch DESC) AS _rank FROM {} FINAL WHERE _scope='{escaped}' AND _batch IN (SELECT id FROM commits WHERE id<={})) WHERE _rank=1 AND _deleted=0)",
            quoted(table),
            self.snapshot.get()
        ))
    }
    pub fn select<T: CatalogRow>(&self, scope: &str, query: &Select) -> Result<Vec<T>> {
        let columns = T::columns()
            .iter()
            .map(|(name, _)| quoted(name))
            .collect::<Vec<_>>()
            .join(",");
        self.query(
            format!(
                "SELECT {columns} FROM {}{}",
                self.source(T::TABLE, scope)?,
                query.sql::<T>()?
            ),
            query.filters.iter().map(|(_, _, v)| v.clone()).collect(),
        )
    }
    pub fn next_id<T: CatalogRow>(&self, scope: &str, column: &str) -> Result<i64> {
        let next = self.number(
            format!(
                "SELECT toInt64(ifNull(max({}),0)+1) AS value FROM {}",
                quoted(column),
                self.source(T::TABLE, scope)?
            ),
            vec![],
        )?;
        if scope == CATALOG
            && ["observations", "work", "epochs", "media_transformations"].contains(&T::TABLE)
            && let Some(row) = self
                .select::<CounterRow>(CATALOG, &Select::eq("name", T::TABLE))?
                .first()
        {
            return Ok(next.max(row.value.checked_add(1).context("ID overflow")?));
        }
        Ok(next)
    }
    pub fn commit(&self, changes: Vec<Change>) -> Result<()> {
        self.commit_with_hook(changes, |_| Ok(()))
    }
    pub fn commit_with_hook(
        &self,
        mut changes: Vec<Change>,
        mut hook: impl FnMut(BatchPoint) -> Result<()>,
    ) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        ensure!(!self.frozen.get(), "cannot write a frozen snapshot");
        if changes.is_empty() {
            return Ok(());
        }
        self.recover()?;
        let mut last = std::collections::BTreeMap::new();
        for change in changes {
            last.insert(
                (
                    change.table.clone(),
                    change.scope.clone(),
                    change.key.clone(),
                ),
                change,
            );
        }
        changes = last.into_values().collect();
        for table in ["observations", "work", "epochs", "media_transformations"] {
            if let Some(high) = changes
                .iter()
                .filter(|r| r.scope == CATALOG && r.table == table)
                .map(|r| r.row)
                .max()
            {
                let old = self
                    .select::<CounterRow>(CATALOG, &Select::eq("name", table))?
                    .first()
                    .map(|r| r.value)
                    .unwrap_or(0);
                changes.push(Change::put(
                    CATALOG,
                    &CounterRow {
                        name: table.into(),
                        value: old.max(high),
                    },
                )?);
            }
        }
        let id = self.number(
            "SELECT toInt64(ifNull(max(id),0)+1) AS value FROM intents".into(),
            vec![],
        )?;
        let mut allocated = std::collections::HashMap::new();
        for change in &mut changes {
            ensure!(
                Self::table_names().contains(&change.table.as_str()),
                "unknown archive table"
            );
            let existing = self.query::<Identity>(
                format!(
                    "SELECT _scope,_key,_row FROM {} WHERE _key=?",
                    self.source(&change.table, &change.scope)?
                ),
                vec![json!(change.key)],
            )?;
            if let Some(existing) = existing.first().filter(|_| change.row == 0) {
                change.row = existing._row;
            } else if existing.is_empty() && change.row == 0 {
                let next = match allocated.get_mut(&change.table) {
                    Some(next) => next,
                    None => {
                        let next = self.number(
                            format!(
                            "SELECT toInt64(greatest(ifNull(max(_row),0),0)+1) AS value FROM {}",
                                quoted(&change.table)
                            ),
                            vec![],
                        )?;
                        allocated.entry(change.table.clone()).or_insert(next)
                    }
                };
                change.row = *next;
                *next = next.checked_add(1).context("row ID overflow")?;
            }
        }
        let body = serde_json::to_string(&changes)?;
        let intent = Intent {
            id,
            checksum: blake3::hash(body.as_bytes()).to_hex().to_string(),
            body,
        };
        self.pending.set(true);
        self.insert_json("intents", &[serde_json::to_value(&intent)?])?;
        hook(BatchPoint::JournalCommitted)?;
        self.apply_with_hook(&intent, &mut hook)?;
        Ok(())
    }
    fn insert_json(&self, table: &str, rows: &[Value]) -> Result<()> {
        // One request can contain several parts. A commit marker is written only after all requests succeed.
        for chunk in rows.chunks(128) {
            let body = chunk
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            self.execute(format!(
                "INSERT INTO {} FORMAT JSONEachRow\n{body}",
                quoted(table)
            ))?;
        }
        Ok(())
    }
    fn apply(&self, intent: &Intent) -> Result<()> {
        self.apply_with_hook(intent, &mut |_| Ok(()))
    }
    fn apply_with_hook(
        &self,
        intent: &Intent,
        hook: &mut impl FnMut(BatchPoint) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            blake3::hash(intent.body.as_bytes()).to_hex().as_str() == intent.checksum,
            "batch checksum mismatch"
        );
        let changes: Vec<Change> = serde_json::from_str(&intent.body)?;
        let mut tables = std::collections::BTreeMap::<String, Vec<Value>>::new();
        for change in changes {
            ensure!(
                Self::table_names().contains(&change.table.as_str()),
                "unknown batch table"
            );
            let mut row = change
                .data
                .as_object()
                .context("invalid batch row")?
                .clone();
            row.insert("_scope".into(), json!(change.scope));
            row.insert("_key".into(), json!(change.key));
            row.insert("_row".into(), json!(change.row));
            row.insert("_batch".into(), json!(intent.id));
            row.insert("_deleted".into(), json!(u8::from(change.deleted)));
            tables
                .entry(change.table)
                .or_default()
                .push(Value::Object(row));
        }
        for (table, rows) in tables {
            self.insert_json(&table, &rows)?;
        }
        hook(BatchPoint::RowsInserted)?;
        self.insert_json(
            "commits",
            &[serde_json::to_value(Commit { id: intent.id })?],
        )?;
        self.snapshot.set(intent.id);
        self.pending.set(false);
        hook(BatchPoint::Committed)?;
        Ok(())
    }
    fn recover(&self) -> Result<()> {
        let intents: Vec<Intent> = self.query("SELECT id,body,checksum FROM intents FINAL WHERE id NOT IN(SELECT id FROM commits) ORDER BY id".into(), vec![])?;
        for intent in intents {
            self.apply(&intent)?;
        }
        self.snapshot.set(self.number(
            "SELECT toInt64(ifNull(max(id),0)) AS value FROM commits".into(),
            vec![],
        )?);
        self.pending.set(false);
        Ok(())
    }
    pub fn prune(&self, scopes: &[String]) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        let mut scopes = scopes.to_vec();
        scopes.push(CATALOG.into());
        let scopes = scopes
            .iter()
            .map(|s| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        for table in Self::table_names() {
            self.execute(format!("ALTER TABLE \"{table}\" DELETE WHERE _deleted=1 OR _scope NOT IN ({scopes}) OR (_scope,_key,_batch) NOT IN (SELECT _scope,_key,max(_batch) FROM \"{table}\" WHERE _batch IN (SELECT id FROM commits) GROUP BY _scope,_key) SETTINGS mutations_sync=2"))?;
        }
        self.execute("ALTER TABLE intents UPDATE body='',checksum='' WHERE id IN (SELECT id FROM commits) SETTINGS mutations_sync=2".into())
    }
    pub fn table_names() -> &'static [&'static str] {
        &[
            "settings",
            "counters",
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
            "work",
            "representations",
            "media_transformations",
            "runtime",
            "peers",
            "folders",
            "dictionaries",
            "blocks",
            "text",
        ]
    }
}
