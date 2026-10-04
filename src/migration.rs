//! Copy a fixed logical snapshot into a separate dataset.
use crate::{
    archive::{Archive, connection, sync_dir},
    config::{Backend, ClickHouse, Config},
    storage::{CATALOG, rows::*},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, clap::Args)]
pub struct ConnectionOptions {
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,
    #[arg(long, default_value = "tg_backup")]
    pub clickhouse_database: String,
    #[arg(long, default_value = "default")]
    pub clickhouse_user: String,
    /// Read the password from a private file.
    #[arg(long)]
    pub clickhouse_password_file: Option<PathBuf>,
}
impl Default for ConnectionOptions {
    fn default() -> Self {
        let options = ClickHouse::default();
        Self {
            clickhouse_url: options.url,
            clickhouse_database: options.database,
            clickhouse_user: options.user,
            clickhouse_password_file: None,
        }
    }
}
impl ConnectionOptions {
    pub fn config(&self, backend: Backend, mut config: Config) -> Result<Config> {
        config.backend = backend;
        config.clickhouse = if backend == Backend::Clickhouse {
            let options = ClickHouse {
                url: self.clickhouse_url.clone(),
                database: self.clickhouse_database.clone(),
                user: self.clickhouse_user.clone(),
                password: self
                    .clickhouse_password_file
                    .as_ref()
                    .map(|p| tg_backup_credentials::Secret::File { path: p.clone() }),
                ..Default::default()
            };
            options.validate()?;
            Some(options)
        } else {
            None
        };
        Ok(config)
    }
}
#[derive(clap::Args)]
pub struct Options {
    #[arg(long, value_enum)]
    pub to: Backend,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub resume: bool,
    #[command(flatten)]
    pub connection: ConnectionOptions,
}
#[derive(Serialize, Deserialize)]
struct Progress {
    source: PathBuf,
    fingerprint: String,
    target_id: String,
    backend: Backend,
    section: usize,
    after: i64,
}
// Read private SQLite copies so WAL readers cannot create or change source files.
struct SourceCopy(PathBuf);
impl SourceCopy {
    fn new(root: &Path) -> Result<Self> {
        let copy =
            Self(std::env::temp_dir().join(format!("tg-backup-migrate-{}", uuid::Uuid::new_v4())));
        fs::create_dir(&copy.0)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&copy.0, fs::Permissions::from_mode(0o700))?;
        }
        for name in [
            "config.toml",
            "writer.lock",
            "readers.lock",
            "catalog.sqlite3",
            "catalog.sqlite3-wal",
            "session.sqlite3",
            "session.sqlite3-wal",
        ] {
            if root.join(name).exists() {
                fs::copy(root.join(name), copy.0.join(name))?;
            }
        }
        fn epochs(from: &Path, to: &Path) -> Result<()> {
            fs::create_dir(to)?;
            for entry in fs::read_dir(from)? {
                let entry = entry?;
                let target = to.join(entry.file_name());
                if entry.file_type()?.is_dir() {
                    epochs(&entry.path(), &target)?;
                } else if !entry.file_name().to_string_lossy().ends_with("-shm") {
                    fs::copy(entry.path(), target)?;
                }
            }
            Ok(())
        }
        epochs(&root.join("epochs"), &copy.0.join("epochs"))?;
        Ok(copy)
    }
}
impl Drop for SourceCopy {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn save(root: &Path, progress: &Progress) -> Result<()> {
    let path = root.join("migration.next");
    tg_backup_credentials::private_write(&path, &serde_json::to_vec(progress)?)?;
    fs::File::open(&path)?.sync_all()?;
    fs::rename(path, root.join("migration.json"))?;
    sync_dir(root)
}

pub(crate) fn export_sqlite(
    db: &Connection,
    table: &str,
    scope: &str,
    after: i64,
    limit: usize,
) -> Result<Vec<Change>> {
    let (columns, keys) = table_columns(table)?;
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(vec![]);
    }
    let mut statement = db.prepare(&format!(
        "SELECT rowid,* FROM \"{table}\" WHERE rowid>? ORDER BY rowid LIMIT {limit}"
    ))?;
    let names = statement
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut rows = statement.query([after])?;
    let mut output = vec![];
    while let Some(row) = rows.next()? {
        let mut data = serde_json::Map::new();
        for (name, ty) in &columns {
            data.insert(
                (*name).into(),
                match *ty {
                    "String" => json!(""),
                    "Array(UInt8)" => json!([]),
                    t if t.starts_with("Nullable") => Value::Null,
                    _ => json!(0),
                },
            );
        }
        if table == "epochs" {
            data.insert("accepting".into(), json!(1));
            data.insert("part".into(), json!(1));
        }
        for (i, name) in names.iter().enumerate().skip(1) {
            let mut value = crate::storage::json_value(row.get_ref(i)?)?;
            if name == "journal" && value.is_null() {
                value = json!([]);
            }
            data.insert(name.clone(), value);
        }
        let data = Value::Object(data);
        let key = serde_json::to_string(&keys.iter().map(|key| &data[*key]).collect::<Vec<_>>())?;
        output.push(Change {
            table: table.into(),
            scope: scope.into(),
            key,
            row: row.get(0)?,
            deleted: false,
            data,
        });
    }
    Ok(output)
}
fn sections(a: &Archive) -> Result<Vec<(String, String)>> {
    let mut sections = crate::storage::ClickHouseStore::table_names()
        .iter()
        .filter(|t| !["text", "blocks", "dictionaries", "counters"].contains(t))
        .map(|t| (CATALOG.into(), (*t).into()))
        .collect::<Vec<_>>();
    for row in a.store.all::<EpochRow>()? {
        for table in [
            "schemas",
            "dictionaries",
            "blocks",
            "payloads",
            "observations",
        ] {
            sections.push((row.path.clone(), table.into()));
        }
    }
    Ok(sections)
}
pub(crate) fn export(
    a: &Archive,
    scope: &str,
    table: &str,
    after: i64,
    limit: usize,
) -> Result<Vec<Change>> {
    let limit = if ["blocks", "dictionaries", "payloads"].contains(&table) {
        limit.min(1)
    } else {
        limit
    };
    if let Some(db) = a.store.clickhouse() {
        return db.export(table, scope, after, limit);
    }
    if scope == CATALOG {
        export_sqlite(a.store.sqlite()?, table, scope, after, limit)
    } else {
        let path = a.root.join(scope).canonicalize()?;
        ensure!(
            path.starts_with(a.root.join("epochs").canonicalize()?),
            "invalid epoch path"
        );
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut rows = export_sqlite(&db, table, scope, after, limit)?;
        let epoch = a
            .store
            .all::<EpochRow>()?
            .into_iter()
            .find(|e| e.path == scope)
            .context("epoch missing")?;
        for row in &mut rows {
            if row.data.get("epoch").is_some() {
                row.data["epoch"] = json!(epoch.id);
            }
        }
        Ok(rows)
    }
}
fn normalize(row: &mut Change, target_id: &str) {
    if row.table == "settings" && row.data["key"] == "dataset_id" {
        row.data["value"] = json!(target_id);
    }
    if row.table == "jobs" && row.data["status"] == "running" {
        row.data["status"] = json!("paused");
    }
    if row.table == "work" && row.data["state"] == "running" {
        row.data["state"] = json!("queued");
    }
}
fn excluded(row: &Change) -> bool {
    row.table == "retired"
        || (row.table == "settings"
            && ["storage_schema", "extensions_version"]
                .iter()
                .any(|key| row.data["key"] == *key))
}
pub(crate) fn import(a: &mut Archive, rows: Vec<Change>) -> Result<()> {
    if let Some(db) = a.store.clickhouse() {
        return db.commit(rows);
    }
    let mut grouped = std::collections::BTreeMap::<String, Vec<Change>>::new();
    for row in rows {
        grouped.entry(row.scope.clone()).or_default().push(row);
    }
    for (scope, rows) in grouped {
        if scope == CATALOG {
            for row in rows.iter().filter(|r| r.table == "epochs" && !r.deleted) {
                let path = row.data["path"].as_str().context("epoch path")?;
                ensure!(
                    Path::new(path)
                        .components()
                        .all(|c| matches!(c, std::path::Component::Normal(_))),
                    "invalid epoch path"
                );
                let db = connection(&a.root.join(path), true)?;
                if db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))? == 0 {
                    db.execute_batch(crate::archive::EPOCH)?;
                }
            }
            a.store.batch(rows)?;
        } else {
            ensure!(
                Path::new(&scope)
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
                "invalid epoch path"
            );
            let mut db = connection(&a.root.join(scope), true)?;
            if db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))? == 0 {
                db.execute_batch(crate::archive::EPOCH)?;
            }
            let tx = db.transaction()?;
            for row in rows {
                crate::storage::write_change(&tx, &row)?;
            }
            tx.commit()?;
        }
    }
    Ok(())
}
fn sequences(a: &Archive) -> Result<Vec<CounterRow>> {
    let mut rows = vec![];
    for (table, column) in [
        ("observations", "id"),
        ("work", "sequence"),
        ("epochs", "id"),
        ("media_transformations", "id"),
    ] {
        let value = if let Some(db) = a.store.clickhouse() {
            db.select::<CounterRow>(CATALOG, &crate::storage::Select::eq("name", table))?
                .first()
                .map(|r| r.value)
                .unwrap_or(0)
        } else {
            let db = a.store.sqlite()?;
            let max = db
                .query_row(
                    &format!("SELECT COALESCE(MAX({column}),0) FROM {table}"),
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0);
            let sequence = db
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name=?1",
                    [table],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0);
            max.max(sequence)
        };
        rows.push(CounterRow {
            name: table.into(),
            value,
        });
    }
    Ok(rows)
}
fn copy_sequences(source: &Archive, target: &Archive) -> Result<()> {
    for row in sequences(source)? {
        if target.store.clickhouse().is_some() {
            target.store.put(&row)?;
        } else if ["observations", "work"].contains(&row.name.as_str()) {
            let db = target.store.sqlite()?;
            if db.execute(
                "UPDATE sqlite_sequence SET seq=?2 WHERE name=?1",
                rusqlite::params![row.name, row.value],
            )? == 0
            {
                db.execute(
                    "INSERT INTO sqlite_sequence(name,seq) VALUES(?1,?2)",
                    rusqlite::params![row.name, row.value],
                )?;
            }
        }
    }
    Ok(())
}
fn prepare(target: &Archive) -> Result<()> {
    for row in target.store.all::<SchemaRow>()? {
        target.store.delete(&row)?;
    }
    if target.store.clickhouse().is_none()
        && let Some(row) = target.store.get::<Setting>("key", "extensions_version")?
    {
        target.store.delete(&row)?;
    }
    Ok(())
}
fn fingerprint(a: &Archive, file_root: &Path) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    hash.update(&fs::read(file_root.join("config.toml"))?);
    hash.update(&serde_json::to_vec(&sequences(a)?)?);
    for (scope, table) in sections(a)? {
        let mut after = 0;
        loop {
            let rows = export(a, &scope, &table, after, 128)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                after = row.row;
                hash.update(&serde_json::to_vec(&row)?);
            }
        }
    }
    for hash_name in a.complete_attachment_hashes()? {
        hash.update(
            crate::media::file_hash(&crate::media::attachment_path(file_root, &hash_name)?)?
                .as_bytes(),
        );
    }
    for name in ["auth.toml", "session.sqlite3", "session.sqlite3-wal"] {
        let path = file_root.join(name);
        if path.exists() {
            hash.update(crate::media::file_hash(&path)?.as_bytes());
        }
    }
    Ok(hash.finalize().to_hex().to_string())
}
fn copy_files(source: &Archive, target: &Archive, file_root: &Path) -> Result<()> {
    for hash in source.complete_attachment_hashes()? {
        let from = crate::media::attachment_path(file_root, &hash)?;
        ensure!(
            crate::media::file_hash(&from)? == hash,
            "corrupt attachment {hash}"
        );
        let to = crate::media::attachment_path(&target.root, &hash)?;
        fs::create_dir_all(to.parent().context("attachment directory")?)?;
        if !to.exists() {
            fs::copy(&from, &to)?;
            fs::File::open(&to)?.sync_all()?;
        }
        ensure!(
            crate::media::file_hash(&to)? == hash,
            "target attachment checksum mismatch"
        );
        sync_dir(to.parent().context("attachment directory")?)?;
    }
    // Copy private credential files without changing credential references.
    for file in fs::read_dir(file_root)? {
        let file = file?;
        let name = file.file_name();
        let name_str = name.to_string_lossy();
        if file.file_type()?.is_file()
            && ![
                "config.toml",
                "catalog.sqlite3",
                "writer.lock",
                "readers.lock",
            ]
            .contains(&name_str.as_ref())
            && !name_str.starts_with("catalog.sqlite3-")
            && !name_str.starts_with("session.sqlite3")
            && !name_str.starts_with("migration.")
        {
            tg_backup_credentials::private_write(&target.root.join(name), &fs::read(file.path())?)?;
        }
    }
    for file in fs::read_dir(file_root.join("staging"))? {
        let file = file?;
        if file.file_type()?.is_file() && file.file_name().to_string_lossy().ends_with(".part") {
            let path = target.root.join("staging").join(file.file_name());
            fs::copy(file.path(), &path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            }
            fs::File::open(path)?.sync_all()?;
        }
    }
    let session = source.root.join("session.sqlite3");
    if session.exists() {
        let db = Connection::open_with_flags(session, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.backup("main", target.root.join("session.sqlite3"), None)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                target.root.join("session.sqlite3"),
                fs::Permissions::from_mode(0o600),
            )?;
        }
    }
    for directory in ["attachments", "staging", "epochs"] {
        sync_dir(&target.root.join(directory))?;
    }
    sync_dir(&target.root)?;
    Ok(())
}
fn verify(source: &Archive, target: &Archive) -> Result<()> {
    let id = target.dataset_id()?;
    for (scope, table) in sections(source)? {
        let mut after = 0;
        loop {
            let rows = export(source, &scope, &table, after, 128)?;
            if rows.is_empty() {
                break;
            }
            let end = rows.last().context("rows missing")?.row;
            let expected = rows
                .into_iter()
                .filter(|r| !excluded(r))
                .map(|mut r| {
                    normalize(&mut r, &id);
                    r
                })
                .collect::<Vec<_>>();
            let actual = export(target, &scope, &table, after, 128)?
                .into_iter()
                .filter(|r| r.row <= end && !excluded(r))
                .collect::<Vec<_>>();
            ensure!(
                serde_json::to_value(&expected)? == serde_json::to_value(&actual)?,
                "migration count/hash mismatch in {scope}/{table}: expected {} rows {:?}, found {} rows {:?}",
                expected.len(),
                expected.iter().map(|r| (&r.key, r.row)).collect::<Vec<_>>(),
                actual.len(),
                actual.iter().map(|r| (&r.key, r.row)).collect::<Vec<_>>()
            );
            for row in &expected {
                if scope == CATALOG && table == "payloads" {
                    let hash = row.data["hash"].as_str().context("payload hash")?;
                    ensure!(
                        source.payload(hash)? == target.payload(hash)?,
                        "payload bytes changed"
                    );
                }
            }
            after = end;
        }
    }
    target.verify()?;
    Ok(())
}
/// Private maintenance copies do not pause jobs or replace the live dataset.
pub(crate) fn snapshot(source: &Archive, root: &Path) -> Result<Archive> {
    let config = Config {
        backend: Backend::Sqlite,
        clickhouse: None,
        ..source.config.clone()
    };
    let mut target = Archive::init(root, &config)?;
    prepare(&target)?;
    target.store.ensure_peers()?;
    for (scope, table) in sections(source)? {
        let mut after = 0;
        loop {
            let rows = export(source, &scope, &table, after, 128)?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().context("rows")?.row;
            import(
                &mut target,
                rows.into_iter().filter(|r| !excluded(r)).collect(),
            )?;
        }
    }
    copy_sequences(source, &target)?;
    copy_files(source, &target, &source.root)?;
    if let Some(db) = source.store.clickhouse() {
        target.store.put(&Setting {
            key: "maintenance_source_snapshot".into(),
            value: db.snapshot().to_string(),
        })?;
    }
    Ok(target)
}
pub fn run(source_root: &Path, options: &Options) -> Result<Value> {
    run_with_hook(source_root, options, || Ok(()))
}
/// The hook runs after a durable batch and before its progress record.
pub fn run_with_hook(
    source_root: &Path,
    options: &Options,
    hook: impl FnMut() -> Result<()>,
) -> Result<Value> {
    run_inner(source_root, options, hook, |_, _, _| {})
}
fn run_inner(
    source_root: &Path,
    options: &Options,
    mut hook: impl FnMut() -> Result<()>,
    mut update: impl FnMut(usize, usize, &str),
) -> Result<Value> {
    update(0, 1, "Read source snapshot");
    let source_root = source_root.canonicalize()?;
    ensure!(
        source_root
            != options
                .output
                .canonicalize()
                .unwrap_or_else(|_| options.output.clone()),
        "source and target must differ"
    );
    let writer = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(source_root.join("writer.lock"))?;
    writer
        .try_lock_exclusive()
        .context("source has a writer; stop the coordinator")?;
    let source_copy = SourceCopy::new(&source_root)?;
    let source = Archive::open(&source_copy.0, false)?;
    let fingerprint = fingerprint(&source, &source_root)?;
    let mut progress;
    let mut target = if options.resume {
        progress = serde_json::from_slice::<Progress>(
            &fs::read(options.output.join("migration.json"))
                .context("target has no migration to resume")?,
        )?;
        ensure!(
            progress.source == source_root
                && progress.fingerprint == fingerprint
                && progress.backend == options.to,
            "source snapshot changed or target is unrelated"
        );
        let target = Archive::open_migration(&options.output)?;
        ensure!(
            target.dataset_id()? == progress.target_id,
            "target dataset changed"
        );
        target
    } else {
        ensure!(
            !options.output.exists() || fs::read_dir(&options.output)?.next().is_none(),
            "target is not empty; use --resume for its migration"
        );
        let config = options
            .connection
            .config(options.to, source.config.clone())?;
        let target = Archive::init(&options.output, &config)?;
        progress = Progress {
            source: source_root.clone(),
            fingerprint,
            target_id: target.dataset_id()?,
            backend: options.to,
            section: 0,
            after: 0,
        };
        save(&options.output, &progress)?;
        prepare(&target)?;
        target
    };
    target.store.ensure_peers()?;
    let sections = sections(&source)?;
    let total = sections.len() + 3;
    update(progress.section, total, "Copy archive");
    while progress.section < sections.len() {
        let (scope, table) = &sections[progress.section];
        let mut rows = export(&source, scope, table, progress.after, 128)?;
        if rows.is_empty() {
            progress.section += 1;
            progress.after = 0;
        } else {
            let after = rows.last().context("rows missing")?.row;
            rows.retain(|r| !excluded(r));
            for row in &mut rows {
                normalize(row, &progress.target_id);
            }
            import(&mut target, rows)?;
            hook()?;
            progress.after = after;
        }
        save(&options.output, &progress)?;
        update(
            progress.section,
            total,
            &format!("Copy {scope}/{table} #{}", progress.after),
        );
    }
    update(sections.len(), total, "Copy attachments and session");
    copy_files(&source, &target, &source_root)?;
    copy_sequences(&source, &target)?;
    update(sections.len() + 1, total, "Build search index");
    target.reindex()?;
    update(sections.len() + 2, total, "Verify target");
    verify(&source, &target)?;
    fs::remove_file(options.output.join("migration.json"))?;
    sync_dir(&options.output)?;
    update(total, total, "Ready");
    Ok(
        json!({"ready":true,"backend":options.to,"dataset_id":progress.target_id,"output":options.output}),
    )
}

/// Terminal progress uses stderr; stdout remains a machine-readable result.
pub fn run_cli(source: &Path, options: &Options) -> Result<Value> {
    use std::io::{IsTerminal, Write};
    struct Bar(bool);
    impl Drop for Bar {
        fn drop(&mut self) {
            if self.0 {
                eprintln!();
            }
        }
    }
    let bar = Bar(std::io::stderr().is_terminal());
    run_inner(
        source,
        options,
        || Ok(()),
        |done, total, label| {
            if bar.0 {
                let filled = done * 32 / total;
                eprint!(
                    "\r\x1b[2KMigration [{}{}] {:3}% {label}",
                    "=".repeat(filled),
                    " ".repeat(32 - filled),
                    done * 100 / total
                );
                let _ = std::io::stderr().flush();
            }
        },
    )
}
