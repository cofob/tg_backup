//! Lossless maintenance prepares a private snapshot. Publication updates only its
//! payload/epoch graph, never replaces the live catalog or its newer checkpoints.
use crate::{
    archive::{Archive, Maintenance, sync_dir},
    work::Task,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::path::Path;
pub fn execute(root: &Path, task: &Task) -> Result<Value> {
    let source = Archive::open(root, false)?;
    let staging = root.join("staging").join(format!("work-{}", task.sequence));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    drop(Archive::init(&staging, &source.config)?);
    source
        .db
        .backup("main", staging.join("catalog.sqlite3"), None)?;
    for (_, _, path, _) in source.epochs()? {
        let input = Connection::open_with_flags(
            root.join(&path),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        input.backup("main", staging.join(path), None)?;
    }
    let mut st=source.db.prepare("SELECT DISTINCT hash FROM media WHERE status='complete' UNION SELECT hash FROM representations")?;
    for hash in st.query_map([], |r| r.get::<_, String>(0))? {
        let hash = hash?;
        let from = crate::media::attachment_path(root, &hash)?;
        let to = crate::media::attachment_path(&staging, &hash)?;
        std::fs::create_dir_all(to.parent().unwrap())?;
        std::fs::hard_link(from, to)?;
    }
    let generation = source.checkpoint("maintenance_generation")?;
    let high: i64 =
        source
            .db
            .query_row("SELECT COALESCE(MAX(id),0) FROM observations", [], |r| {
                r.get(0)
            })?;
    drop(st);
    drop(source);
    let mut snapshot = Archive::open(&staging, true)?;
    let result = if task.kind == "reindex" {
        // Keep ordinary text rows only inside this private, temporary worker result.
        snapshot.db.execute_batch(
            "CREATE TABLE IF NOT EXISTS rebuilt_text(id INTEGER PRIMARY KEY,text TEXT NOT NULL);",
        )?;
        let mut after = 0;
        loop {
            let ids = snapshot.observation_ids(after, 128)?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                after = id;
                let record = snapshot.record(id)?;
                snapshot.db.execute(
                    "INSERT OR REPLACE INTO rebuilt_text VALUES(?1,?2)",
                    params![id, crate::tl::text(&record.data)],
                )?;
            }
        }
        json!({"indexed_through":high})
    } else {
        snapshot.maintain(&Maintenance {
            apply: true,
            seal: true,
            consolidate_yearly: task.kind == "consolidate",
            ..Default::default()
        })?
    };
    snapshot
        .db
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(
        json!({"state":"complete","snapshot":format!("work-{}",task.sequence),"high_water":high,"generation":generation,"result":result}),
    )
}
pub fn publish(a: &mut Archive, task: &Task, report: &Value) -> Result<()> {
    let name = report["snapshot"]
        .as_str()
        .context("missing maintenance snapshot")?;
    ensure!(
        name == format!("work-{}", task.sequence),
        "invalid snapshot path"
    );
    ensure!(
        a.checkpoint("maintenance_generation")?
            .unwrap_or(Value::Null)
            == report["generation"],
        "archive generation changed; resume maintenance to prepare a fresh snapshot"
    );
    let root = a.root.join("staging").join(name);
    let snapshot = Archive::open(&root, false)?;
    if task.kind == "reindex" {
        let tx = a.db.transaction()?;
        let mut st = snapshot
            .db
            .prepare("SELECT id,text FROM rebuilt_text ORDER BY id")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let text: String = row.get(1)?;
            let current:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM heads JOIN observations ON observation=id WHERE observation=?1 AND deleted=0)",[id],|r|r.get(0))?;
            tx.execute("DELETE FROM search WHERE rowid=?1", [id])?;
            if current {
                tx.execute(
                    "INSERT INTO search(rowid,text) VALUES(?1,?2)",
                    params![id, text],
                )?;
            }
            if a.config.index_history {
                tx.execute("DELETE FROM history_search WHERE rowid=?1", [id])?;
                tx.execute("INSERT INTO history_search(rowid,text) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM observations WHERE id=?1)",params![id,text])?;
            }
        }
        tx.commit()?;
    } else {
        // Files are durable before a single catalog transaction makes them visible.
        let mut paths = Vec::new();
        for (id, name, path, _) in snapshot.epochs()? {
            let filename = Path::new(&path)
                .file_name()
                .context("epoch file")?
                .to_string_lossy();
            let dest = format!("epochs/work-{}-{filename}", task.sequence);
            std::fs::copy(root.join(&path), a.root.join(&dest))?;
            std::fs::File::open(a.root.join(&dest))?.sync_all()?;
            paths.push((id, name, dest));
        }
        sync_dir(&a.root.join("epochs"))?;
        let tx = a.db.transaction()?;
        let mut mapping = std::collections::BTreeMap::new();
        for (id, name, path) in paths {
            tx.execute("INSERT INTO epochs(name,path,sealed,generation,accepting,part) VALUES(?1,?2,1,1,0,1)",params![name,path])?;
            mapping.insert(id, tx.last_insert_rowid());
        }
        let mut st = snapshot
            .db
            .prepare("SELECT hash,epoch,block,offset,length FROM payloads")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let hash: String = row.get(0)?;
            let epoch: i64 = row.get(1)?;
            tx.execute("UPDATE payloads SET epoch=?2,block=?3,offset=?4,length=?5,journal=NULL WHERE hash=?1",params![hash,mapping[&epoch],row.get::<_,i64>(2)?,row.get::<_,i64>(3)?,row.get::<_,i64>(4)?])?;
        }
        let mut st = snapshot
            .db
            .prepare("SELECT id,epoch,payload FROM observations")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let epoch: i64 = row.get(1)?;
            tx.execute(
                "UPDATE observations SET epoch=?2 WHERE id=?1 AND payload=?3",
                params![
                    row.get::<_, i64>(0)?,
                    mapping[&epoch],
                    row.get::<_, String>(2)?
                ],
            )?;
        }
        tx.execute("INSERT OR IGNORE INTO retired SELECT path FROM epochs WHERE id NOT IN(SELECT epoch FROM observations UNION SELECT epoch FROM payloads)",[])?;
        tx.execute("DELETE FROM epochs WHERE id NOT IN(SELECT epoch FROM observations UNION SELECT epoch FROM payloads)",[])?;
        tx.execute("INSERT INTO checkpoints VALUES('maintenance_generation',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[json!(uuid::Uuid::new_v4().to_string()).to_string()])?;
        tx.execute(
            "INSERT INTO maintenance VALUES(?1,?2,'{}',?3)",
            params![
                uuid::Uuid::new_v4().to_string(),
                chrono::Utc::now().timestamp_micros(),
                report.to_string()
            ],
        )?;
        tx.commit()?;
    }
    drop(snapshot);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
