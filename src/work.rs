//! Durable expensive-work queue. Only the coordinator writes the archive catalog.
use crate::{
    archive::Archive,
    storage::{Select, rows::*},
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Datelike, Local, Timelike, Utc};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Resources {
    pub cpus: u32,
    pub memory_bytes: u64,
    pub nice: u8,
    pub cgroup: Option<PathBuf>,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            cpus: 2,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            nice: 10,
            cgroup: None,
        }
    }
}
impl Resources {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.cpus > 0
                && self.cpus <= 256
                && self.memory_bytes >= 64 * 1024 * 1024
                && self.nice <= 19,
            "invalid resource budget"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Schedule {
    pub windows: Vec<Window>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// ISO weekdays, Monday=1. Overnight windows belong to their start day.
    pub days: Vec<u32>,
    pub start: String,
    pub end: String,
}
fn minute(s: &str) -> Result<u32> {
    let (h, m) = s.split_once(':').context("schedule time must be HH:MM")?;
    let (h, m) = (h.parse::<u32>()?, m.parse::<u32>()?);
    ensure!(h < 24 && m < 60, "invalid schedule time");
    Ok(h * 60 + m)
}
impl Schedule {
    pub fn validate(&self) -> Result<()> {
        for w in &self.windows {
            minute(&w.start)?;
            minute(&w.end)?;
            ensure!(
                !w.days.is_empty() && w.days.iter().all(|d| (1..=7).contains(d)),
                "schedule days must be 1–7"
            );
        }
        Ok(())
    }
    pub fn allows_components(&self, day: u32, time: u32) -> bool {
        self.windows.is_empty()
            || self.windows.iter().any(|w| {
                let (Ok(start), Ok(end)) = (minute(&w.start), minute(&w.end)) else {
                    return false;
                };
                if start == end {
                    w.days.contains(&day)
                } else if start < end {
                    w.days.contains(&day) && time >= start && time < end
                } else {
                    (w.days.contains(&day) && time >= start)
                        || (w.days.contains(&(if day == 1 { 7 } else { day - 1 })) && time < end)
                }
            })
    }
    pub fn allows(&self, at: DateTime<Utc>) -> bool {
        let local = at.with_timezone(&Local);
        self.allows_components(
            local.weekday().number_from_monday(),
            local.hour() * 60 + local.minute(),
        )
    }
    pub fn next(&self, at: DateTime<Utc>) -> Option<String> {
        (0..=8 * 24 * 60)
            .map(|m| at + chrono::Duration::minutes(m))
            .find(|t| self.allows(*t))
            .map(|t| t.with_timezone(&Local).to_rfc3339())
    }
}
pub fn migrate(db: &Connection) -> Result<()> {
    // Idempotent transactional extension of the v2 format; native payloads are unchanged.
    let tx = db.unchecked_transaction()?;
    let columns: Vec<String> = tx
        .prepare("PRAGMA table_info(epochs)")?
        .query_map([], |r| r.get(1))?
        .collect::<rusqlite::Result<_>>()?;
    if !columns.iter().any(|c| c == "accepting") {
        tx.execute_batch("ALTER TABLE epochs ADD COLUMN accepting INTEGER NOT NULL DEFAULT 1; ALTER TABLE epochs ADD COLUMN part INTEGER NOT NULL DEFAULT 1;")?;
    }
    tx.execute_batch("CREATE TABLE IF NOT EXISTS work(sequence INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,dedupe TEXT UNIQUE NOT NULL,config TEXT NOT NULL,automatic INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'queued',created INTEGER NOT NULL,updated INTEGER NOT NULL,retry_at INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,progress TEXT NOT NULL DEFAULT '{}',error TEXT);
    CREATE INDEX IF NOT EXISTS work_state ON work(state,retry_at,sequence);
    CREATE INDEX IF NOT EXISTS observations_epoch_id ON observations(epoch,id);
    CREATE INDEX IF NOT EXISTS payloads_pending_epoch ON payloads(epoch) WHERE journal IS NOT NULL;
    CREATE INDEX IF NOT EXISTS media_pending ON media(attempts,id,retry_at) WHERE status!='complete' AND status!='unavailable';
    CREATE TABLE IF NOT EXISTS representations(original TEXT NOT NULL,hash TEXT NOT NULL,recipe TEXT NOT NULL,bytes INTEGER NOT NULL,created INTEGER NOT NULL,details TEXT NOT NULL,PRIMARY KEY(original,recipe));
    CREATE INDEX IF NOT EXISTS representations_hash ON representations(hash);
    CREATE TABLE IF NOT EXISTS media_transformations(id INTEGER PRIMARY KEY,at INTEGER NOT NULL,original TEXT NOT NULL,replacement TEXT NOT NULL,policy TEXT NOT NULL,details TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS runtime(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    INSERT OR REPLACE INTO settings VALUES('extensions_version','1');")?;
    tx.commit()?;
    Ok(())
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub sequence: i64,
    pub kind: String,
    pub config: Value,
}
impl Archive {
    pub fn enqueue_work(
        &self,
        kind: &str,
        dedupe: &str,
        config: &Value,
        automatic: bool,
    ) -> Result<i64> {
        ensure!(self.writable, "read-only archive");
        ensure!(
            [
                "transcode",
                "verify",
                "reindex",
                "seal",
                "repack",
                "consolidate"
            ]
            .contains(&kind),
            "unknown work kind"
        );
        if let Some(row) = self.store.get::<WorkRow>("dedupe", dedupe)? {
            return Ok(row.sequence);
        }
        let now = Utc::now().timestamp_micros();
        let row = WorkRow {
            sequence: self.store.next_id::<WorkRow>("sequence")?,
            kind: kind.into(),
            dedupe: dedupe.into(),
            config: config.to_string(),
            automatic: i64::from(automatic),
            state: "queued".into(),
            created: now,
            updated: now,
            progress: "{}".into(),
            ..Default::default()
        };
        self.store.put(&row)?;
        Ok(row.sequence)
    }
    pub fn work_page(&self, after: i64, limit: usize) -> Result<Value> {
        self.work_page_filtered(after, limit, false)
    }
    fn work_page_filtered(&self, after: i64, limit: usize, active: bool) -> Result<Value> {
        ensure!(
            after >= 0 && (1..=200).contains(&limit),
            "invalid queue page"
        );
        let counts = self.store.states::<WorkRow>("state")?;
        let mut select = Select::after("sequence", after, limit + 1);
        if active {
            select.limit = None;
        }
        let rows = self.store.select::<WorkRow>(&select)?;
        let items = rows.into_iter().filter(|r| !active || ["queued","running","failed","paused"].contains(&r.state.as_str())).take(limit + 1).map(|r| json!({"sequence":r.sequence,"kind":r.kind,"state":r.state,"automatic":r.automatic != 0,"created":r.created,"updated":r.updated,"retry_at":r.retry_at,"attempts":r.attempts,"progress":serde_json::from_str::<Value>(&r.progress).unwrap_or(Value::Null),"error":r.error})).collect::<Vec<_>>();
        let more = items.len() > limit;
        let now = Utc::now();
        let items: Vec<_> = items
            .into_iter()
            .take(limit)
            .map(|mut item| {
                let p = item["progress"].clone();
                let mut safe = serde_json::Map::new();
                for key in [
                    "state",
                    "reason",
                    "kind",
                    "bytes",
                    "original_bytes",
                    "saved_bytes",
                    "elapsed_seconds",
                    "encoded_seconds",
                    "duration_seconds",
                    "fraction",
                    "phase",
                ] {
                    if let Some(value) = p.get(key) {
                        safe.insert(key.into(), value.clone());
                    }
                }
                item["progress"] = Value::Object(safe);
                item["waiting_reason"] = if item["state"] == "queued" {
                    json!(
                        if item["retry_at"].as_i64().unwrap_or(0) > now.timestamp_micros() {
                            "retry_deadline"
                        } else if item["automatic"] == true && !self.config.schedule.allows(now) {
                            "schedule"
                        } else {
                            "worker_capacity"
                        }
                    )
                } else {
                    Value::Null
                };
                item
            })
            .collect();
        let next = if more {
            items.last().and_then(|v| v["sequence"].as_i64())
        } else {
            None
        };
        Ok(json!({"items":items,"counts":counts,"next_cursor":next}))
    }
    pub fn resume_work(&self, id: i64) -> Result<()> {
        ensure!(self.writable, "read-only archive");
        let row = self.work_row(id)?;
        ensure!(
            ["failed", "skipped", "paused"].contains(&row.state.as_str()),
            "work is unknown or not resumable"
        );
        self.update_work(id, |r| {
            r.state = "queued".into();
            r.retry_at = 0;
            r.attempts = 0;
            r.error = None;
            r.progress = "{}".into();
            r.updated = Utc::now().timestamp_micros();
        })
    }
    fn claim_work(&self) -> Result<Option<Task>> {
        let now = Utc::now();
        let allowed = self.config.schedule.allows(now);
        let mut rows = self
            .store
            .select::<WorkRow>(&Select::eq("state", "queued"))?;
        rows.retain(|r| r.retry_at <= now.timestamp_micros() && (r.automatic == 0 || allowed));
        rows.sort_by_key(|r| (r.automatic, r.sequence));
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.update_work(row.sequence, |r| {
            r.state = "running".into();
            r.updated = now.timestamp_micros();
            r.attempts += 1;
            r.error = None;
        })?;
        Ok(Some(Task {
            sequence: row.sequence,
            kind: row.kind,
            config: serde_json::from_str(&row.config)?,
        }))
    }
    pub fn status(&self) -> Result<Value> {
        self.operational_status(false)
    }
    pub fn operational_status(&self, details: bool) -> Result<Value> {
        let mut v = if details {
            self.storage_details()?
        } else {
            self.catalog_status()?
        };
        let now = Utc::now();
        v["observed_at"] = json!(now.to_rfc3339());
        v["work"] = self.work_page_filtered(0, 20, true)?;
        let mut jobs = self.store.all::<JobRow>()?;
        jobs.sort_by_key(|r| std::cmp::Reverse(r.updated));
        let jobs = jobs.into_iter().take(20).map(|r| json!({"id":r.id,"status":r.status,"updated":r.updated,"details":serde_json::from_str::<Value>(&r.details).unwrap_or(Value::Null),"created":r.created})).collect::<Vec<_>>();
        v["sync"] = json!(jobs);
        if let Some(jobs) = v["sync"].as_array_mut() {
            for job in jobs {
                let id = job["id"].as_str().unwrap_or("");
                let messages = self
                    .checkpoint(&format!("messages:{id}"))?
                    .unwrap_or(json!(0));
                let media = self
                    .checkpoint(&format!("media_bytes:{id}"))?
                    .unwrap_or(json!(0));
                job["messages_scanned"] = messages;
                job["media_bytes"] = media;
                job["heartbeat_age_seconds"] = json!(
                    job["updated"]
                        .as_i64()
                        .map(|t| (now.timestamp_micros() - t) / 1_000_000)
                );
            }
        }
        v["update_checkpoint_date"] = self
            .checkpoint("telegram_updates")?
            .and_then(|v| v.get("date").cloned())
            .unwrap_or(Value::Null);
        v["media_states"] = json!(self.store.states::<MediaRow>("status")?);
        v["coverage_states"] = json!(self.store.states::<CoverageRow>("status")?);
        v["resources"] = serde_json::to_value(&self.config.resources)?;
        v["resources"]["enforcement"] = json!("best_effort");
        if let Some(runtime) = self.checkpoint("worker_runtime")? {
            let age = runtime["heartbeat"]
                .as_i64()
                .map(|t| (now.timestamp_micros() - t) / 1_000_000);
            v["worker"] = runtime;
            v["worker"]["heartbeat_age_seconds"] = json!(age);
            v["worker"]["stale"] = json!(age.is_none_or(|s| s > 30));
            if let Some(mode) = v["worker"]["enforcement"].as_str() {
                v["resources"]["enforcement"] = json!(mode);
            }
        } else {
            v["worker"] = json!({"state":"offline","stale":true});
        }
        v["scheduler"] = json!({"timezone":"local","local_time":now.with_timezone(&Local).to_rfc3339(),"window_open":self.config.schedule.allows(now),"next_eligible":self.config.schedule.next(now),"boundary":"finish_current_item","manual_bypasses_schedule":true});
        v["disk_available_bytes"] = json!(fs2::available_space(&self.root).ok());
        v["epoch_limit_bytes"] = json!(self.config.max_epoch_bytes);
        Ok(v)
    }
}
fn heartbeat(a: &Archive, state: &str, enforcement: &str) -> Result<()> {
    a.set_checkpoint(
        "worker_runtime",
        &json!({"heartbeat":Utc::now().timestamp_micros(),"state":state,"enforcement":enforcement}),
    )
}
/// Launch a worker that cannot consume its task until the parent applies cgroup controls.
pub async fn execute(root: &Path, task: &Task, resources: &Resources) -> Result<(Value, String)> {
    if let Some(path) = std::env::var_os("TG_BACKUP_WORKER_SOCKET") {
        let mut stream = tokio::net::UnixStream::connect(path)
            .await
            .context("worker unavailable")?;
        stream
            .write_all(&serde_json::to_vec(
                &json!({"task":task,"resources":resources}),
            )?)
            .await?;
        stream.shutdown().await?;
        let mut bytes = Vec::new();
        stream.take(4 * 1024 * 1024).read_to_end(&mut bytes).await?;
        let result: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            result["error"].is_null(),
            "worker: {}",
            result["error"].as_str().unwrap_or("invalid worker error")
        );
        return Ok((
            result["report"].clone(),
            result["enforcement"]
                .as_str()
                .unwrap_or("best_effort")
                .into(),
        ));
    }
    execute_local(root, task, resources).await
}
pub(crate) async fn execute_local(
    root: &Path,
    task: &Task,
    resources: &Resources,
) -> Result<(Value, String)> {
    let mut cmd = tokio::process::Command::new("nice");
    cmd.arg("-n")
        .arg(resources.nice.to_string())
        .arg(std::env::current_exe()?)
        .arg("--dataset")
        .arg(root)
        .arg("worker-task")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.as_std_mut().process_group(0);
    }
    let mut child = cmd.spawn()?;
    let _group = ProcessGroup(child.id());
    let mut enforcement = "best_effort".to_owned();
    if let Some(path) = &resources.cgroup {
        let result = (|| -> Result<()> {
            std::fs::write(
                path.join("cpu.max"),
                format!("{} 100000", resources.cpus as u64 * 100000),
            )?;
            std::fs::write(path.join("memory.max"), resources.memory_bytes.to_string())?;
            std::fs::write(
                path.join("cgroup.procs"),
                child.id().context("worker PID missing")?.to_string(),
            )?;
            Ok(())
        })();
        if result.is_ok() {
            enforcement = "cgroup_v2".into();
        } else {
            tracing::warn!("cgroup unavailable; using thread/concurrency/nice limits");
        }
    }
    child
        .stdin
        .take()
        .context("worker stdin")?
        .write_all(&serde_json::to_vec(
            &json!({"task":task,"resources":resources}),
        )?)
        .await?;
    let stderr = child.stderr.take().context("worker stderr")?;
    let drain = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut tail = Vec::new();
        let mut buf = [0u8; 4096];
        let mut truncated = false;
        loop {
            let n = stderr.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            tail.extend_from_slice(&buf[..n]);
            // The worker adds an anyhow context chain around the tool's 8 KiB tail.
            // Reserve bounded room for that chain so a second truncation cannot erase it.
            const WORKER_DIAGNOSTIC_LIMIT: usize = 16 * 1024;
            if tail.len() > WORKER_DIAGNOSTIC_LIMIT {
                tail.drain(..tail.len() - WORKER_DIAGNOSTIC_LIMIT);
                truncated = true;
            }
        }
        if truncated {
            tail.splice(
                ..0,
                b"[truncated; last 16384 worker bytes] ".iter().copied(),
            );
        }
        Ok::<_, std::io::Error>(tail)
    });
    let output = child.wait_with_output().await?;
    let stderr = drain.await??;
    if !output.status.success() {
        let error = format!(
            "worker task {} failed (kind={}, original={}): {}; stderr: {}",
            task.sequence,
            task.kind,
            task.config["original"].as_str().unwrap_or("n/a"),
            output.status,
            crate::diagnostics::text(&stderr)
        );
        tracing::error!("{error}");
        anyhow::bail!(error);
    }
    Ok((
        serde_json::from_slice(&output.stdout).context("invalid worker report")?,
        enforcement,
    ))
}
pub async fn run(archive: Arc<Mutex<Archive>>, once: bool) -> Result<()> {
    {
        archive.lock().unwrap().reset_running_work()?;
    }
    let mut enforcement = "best_effort".to_owned();
    let mut last_scan = std::time::Instant::now() - Duration::from_secs(3600);
    loop {
        let task = {
            let a = archive.lock().unwrap();
            heartbeat(&a, "idle", &enforcement)?;
            if last_scan.elapsed() >= Duration::from_secs(60) {
                crate::transcode::enqueue(&a, &a.config.transcode, true, true)?;
                last_scan = std::time::Instant::now();
            }
            a.claim_work()?
        };
        if let Some(task) = task {
            let (root, resources) = {
                let a = archive.lock().unwrap();
                (a.root.clone(), a.config.resources.clone())
            };
            let work = execute(&root, &task, &resources);
            tokio::pin!(work);
            let result = loop {
                tokio::select! {
                    result=&mut work => break result,
                    _=tokio::time::sleep(Duration::from_secs(5)) => { let a=archive.lock().unwrap(); heartbeat(&a,"running",&enforcement)?;
                        if let Ok(progress)=crate::transcode::progress(&root,task.sequence) {a.update_work(task.sequence, |r| r.progress = progress.to_string())?;} }
                }
            };
            let mut a = archive.lock().unwrap();
            match result {
                Ok((report, mode)) => {
                    enforcement = mode.clone();
                    let publication = crate::transcode::publish(&mut a, &task, &report);
                    match publication {
                        Ok(()) => {
                            let state = report["state"].as_str().unwrap_or("complete");
                            a.update_work(task.sequence, |r| {
                                r.state = state.into();
                                r.progress = report.to_string();
                                r.updated = Utc::now().timestamp_micros();
                            })?;
                        }
                        Err(e) => {
                            a.update_work(task.sequence, |r| {
                                r.state = "failed".into();
                                r.error = Some(format!("{e:#}"));
                                r.updated = Utc::now().timestamp_micros();
                            })?;
                        }
                    }
                    heartbeat(&a, "idle", &mode)?;
                }
                Err(e) => {
                    let now = Utc::now().timestamp_micros();
                    a.update_work(task.sequence, |r| {
                        r.state = if r.attempts < 3 { "queued" } else { "failed" }.into();
                        r.error = Some(format!("{e:#}"));
                        r.updated = now;
                        r.retry_at = now + 60_000_000 * r.attempts;
                    })?;
                }
            }
        } else if once {
            break;
        } else {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    Ok(())
}

struct ProcessGroup(Option<u32>);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0 {
            // SAFETY: this is the private process group created for this worker. Negative PID
            // targets its descendants too, so cancellation cannot orphan an FFmpeg encoder.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}

/// Private control socket: file permissions, separate from the read-only REST API.
pub async fn service(root: std::path::PathBuf, socket: std::path::PathBuf) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let _lock_path = socket.with_extension("lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(_lock_path)?;
    fs2::FileExt::try_lock_exclusive(&lock).context("worker service already running")?;
    if socket.exists() {
        std::fs::remove_file(&socket)?;
    }
    let listener = tokio::net::UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    loop {
        let (mut stream, _) =
            tokio::select! { result=listener.accept()=>result?,_=crate::shutdown::signal()=>break };
        let mut bytes = Vec::new();
        (&mut stream)
            .take(1024 * 1024)
            .read_to_end(&mut bytes)
            .await?;
        let request: Value = serde_json::from_slice(&bytes)?;
        let task: Task = serde_json::from_value(request["task"].clone())?;
        ensure!(
            task.kind != "compact_exclusive",
            "exclusive catalog maintenance must run locally"
        );
        let resources: Resources = serde_json::from_value(request["resources"].clone())?;
        resources.validate()?;
        let result = match execute_local(&root, &task, &resources).await {
            Ok((report, mut enforcement)) => {
                if std::fs::read_to_string("/sys/fs/cgroup/cpu.max")
                    .is_ok_and(|s| !s.starts_with("max"))
                    && std::fs::read_to_string("/sys/fs/cgroup/memory.max")
                        .is_ok_and(|s| !s.starts_with("max"))
                {
                    enforcement = "container_cgroup".into();
                }
                json!({"report":report,"enforcement":enforcement})
            }
            Err(e) => json!({"error":format!("{e:#}")}),
        };
        let _ = stream.write_all(&serde_json::to_vec(&result)?).await;
    }
    std::fs::remove_file(socket)?;
    Ok(())
}

/// Manual invocations bypass time windows, but use exactly the same worker budget.
pub async fn manual(a: &mut Archive, kind: &str) -> Result<Value> {
    let id = a.enqueue_work(kind, &uuid::Uuid::new_v4().to_string(), &json!({}), false)?;
    a.update_work(id, |r| {
        r.state = "running".into();
        r.attempts += 1;
    })?;
    let task = Task {
        sequence: id,
        kind: kind.into(),
        config: json!({}),
    };
    let result = execute(&a.root, &task, &a.config.resources).await;
    match result {
        Ok((report, mode)) => {
            if let Err(e) = crate::transcode::publish(a, &task, &report) {
                a.update_work(id, |r| {
                    r.state = "failed".into();
                    r.error = Some(format!("{e:#}"));
                })?;
                return Err(e);
            }
            a.update_work(id, |r| {
                r.state = "complete".into();
                r.progress = report.to_string();
                r.updated = Utc::now().timestamp_micros();
            })?;
            heartbeat(a, "offline", &mode)?;
            Ok(report)
        }
        Err(e) => {
            a.update_work(id, |r| {
                r.state = "failed".into();
                r.error = Some(format!("{e:#}"));
            })?;
            Err(e)
        }
    }
}
/// Lossy maintenance is exclusive: the child is the sole archive writer. It uses
/// the same process/cgroup limits, and the existing generation recovery protocol.
pub async fn compact(root: &Path, retention: crate::archive::Retention) -> Result<Value> {
    let c = crate::config::Config::load(root)?;
    let task = Task {
        sequence: 0,
        kind: "compact_exclusive".into(),
        config: serde_json::to_value(retention)?,
    };
    Ok(execute_local(root, &task, &c.resources).await?.0)
}
