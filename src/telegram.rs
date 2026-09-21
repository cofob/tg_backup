//! Read-only Telegram capture. Every collector archives native TL before advancing its cursor.
mod extra;
use crate::session::SqliteSession;
use crate::{
    archive::{Archive, Capture},
    selector::Selector,
    tl::{self, RawRequest, Schema, integer},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use grammers_client::{Client, SignInError, client::UpdatesConfiguration, peer::Peer};
use grammers_mtsender::{InvocationError, SenderPool};
use grammers_session::{
    Session,
    types::{ChannelState, UpdateState, UpdatesState},
    updates::MessageBox,
};
use grammers_tl_types::{Deserializable, Serializable};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Shared = Arc<Mutex<Archive>>;
struct NetworkGuard(grammers_mtsender::SenderPoolFatHandle);
impl Drop for NetworkGuard {
    fn drop(&mut self) {
        self.0.quit();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, clap::Args, Default)]
pub struct SyncOptions {
    #[arg(long)]
    pub continuous: bool,
    #[arg(long)]
    pub takeout: bool,
    /// Resume a specific job using its saved scope and mode.
    #[arg(long, conflicts_with = "new_job")]
    pub resume: Option<String>,
    /// Create a new job instead of resuming a compatible unfinished job.
    #[arg(long)]
    #[serde(default)]
    pub new_job: bool,
    #[arg(long)]
    pub max_messages: Option<u64>,
    #[arg(long)]
    pub max_media_bytes: Option<u64>,
    #[arg(long)]
    pub max_seconds: Option<u64>,
    #[arg(long)]
    pub min_id: Option<i64>,
    #[arg(long)]
    pub max_id: Option<i64>,
    /// Inclusive Telegram message date, Unix seconds.
    #[arg(long)]
    pub since: Option<i64>,
    /// Exclusive Telegram message date, Unix seconds.
    #[arg(long)]
    pub until: Option<i64>,
    #[arg(long)]
    pub history_selector: Option<String>,
    #[arg(long)]
    pub attachment_selector: Option<String>,
}
#[cfg(test)]
struct MockReply {
    method: &'static str,
    args: Value,
    dc: Option<i32>,
    result: std::result::Result<Value, String>,
}
struct Engine {
    dc_auth_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    mock_rpc: Option<Mutex<std::collections::VecDeque<MockReply>>>,
    client: Client,
    session: Arc<SqliteSession>,
    archive: Shared,
    schema: Arc<Schema>,
    schema_hash: String,
    job: String,
    options: SyncOptions,
    takeout: Option<i64>,
    started: Instant,
    base_messages: u64,
    base_media: u64,
    stop: tokio_util::sync::CancellationToken,
}
fn now() -> i64 {
    Utc::now().timestamp_micros()
}
fn prompt(label: &str) -> Result<String> {
    eprint!("{label}");
    io::stderr().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(s.trim().into())
}
#[derive(Serialize, Deserialize)]
pub struct AuthConfig {
    pub api_id: i32,
    pub api_hash: tg_backup_credentials::Secret,
}
async fn credentials(root: &Path) -> Result<(i32, String)> {
    if let (Ok(id), Ok(hash)) = (
        std::env::var("TG_BACKUP_API_ID"),
        std::env::var("TG_BACKUP_API_HASH"),
    ) {
        return Ok((id.parse()?, hash));
    }
    let config: AuthConfig = toml::from_str(
        &std::fs::read_to_string(root.join("auth.toml"))
            .context("run tg-backup setup or set TG_BACKUP_API_ID and TG_BACKUP_API_HASH")?,
    )?;
    Ok((config.api_id, config.api_hash.load().await?))
}
pub async fn authenticate(root: &Path) -> Result<()> {
    let _a = Archive::open(root, true)?;
    let (id, hash) = credentials(root).await?;
    let session = Arc::new(SqliteSession::open(root.join("session.sqlite3")).await?);
    let SenderPool { runner, handle, .. } = SenderPool::new(session, id);
    let _network_guard = NetworkGuard(handle.clone());
    let client = Client::new(handle.clone());
    let task = tokio::spawn(runner.run());
    let result = async {
        if !client.is_authorized().await? {
            let phone = std::env::var("TG_BACKUP_PHONE")
                .map(Ok)
                .unwrap_or_else(|_| prompt("Phone number: "))?;
            let token = client.request_login_code(&phone, &hash).await?;
            match client
                .sign_in(&token, &prompt("Telegram login code: ")?)
                .await
            {
                Ok(_) => {}
                Err(SignInError::PasswordRequired(token)) => {
                    let password =
                        std::env::var("TG_BACKUP_PASSWORD")
                            .map(Ok)
                            .unwrap_or_else(|_| {
                                tg_backup_credentials::prompt_password("2FA password: ")
                                    .map_err(anyhow::Error::from)
                            })?;
                    client.check_password(token, password).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    handle.quit();
    let _ = task.await;
    result
}
fn prepare_sync_job(archive: &mut Archive, options: &mut SyncOptions) -> Result<String> {
    ensure!(
        !(options.new_job && options.resume.is_some()),
        "--new-job cannot be combined with --resume"
    );
    let limits = (
        options.max_messages,
        options.max_media_bytes,
        options.max_seconds,
    );
    if options.resume.is_none() && !options.new_job {
        let mut statement = archive.db.prepare(
            "SELECT id,config FROM jobs WHERE status IN ('running','paused','failed') ORDER BY updated DESC,created DESC,rowid DESC",
        )?;
        let jobs = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for job in jobs {
            let (id, config) = job?;
            let stored: SyncOptions = serde_json::from_str(&config)
                .with_context(|| format!("invalid config for sync job {id}"))?;
            if options.continuous == stored.continuous
                && options.takeout == stored.takeout
                && options.min_id == stored.min_id
                && options.max_id == stored.max_id
                && options.since == stored.since
                && options.until == stored.until
                && options
                    .history_selector
                    .as_ref()
                    .unwrap_or(&archive.config.history_selector)
                    == stored
                        .history_selector
                        .as_ref()
                        .unwrap_or(&archive.config.history_selector)
                && options
                    .attachment_selector
                    .as_ref()
                    .unwrap_or(&archive.config.attachment_selector)
                    == stored
                        .attachment_selector
                        .as_ref()
                        .unwrap_or(&archive.config.attachment_selector)
            {
                options.resume = Some(id);
                break;
            }
        }
    }
    let job = if let Some(job) = &options.resume {
        let stored: String = archive
            .db
            .query_row("SELECT config FROM jobs WHERE id=?1", [job], |r| r.get(0))
            .context("unknown job")?;
        let job = job.clone();
        *options = serde_json::from_str(&stored)?;
        options.resume = Some(job.clone());
        job
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    // Limits belong to this invocation, including an explicit resume.
    (
        options.max_messages,
        options.max_media_bytes,
        options.max_seconds,
    ) = limits;
    options.new_job = false;
    if let Some(selector) = &options.history_selector {
        archive.config.history_selector = selector.clone();
    }
    if let Some(selector) = &options.attachment_selector {
        archive.config.attachment_selector = selector.clone();
    }
    Selector::parse(&archive.config.history_selector)?;
    Selector::parse(&archive.config.attachment_selector)?;
    // Persist resolved selectors so a resume is independent of subsequent config edits.
    options.history_selector = Some(archive.config.history_selector.clone());
    options.attachment_selector = Some(archive.config.attachment_selector.clone());
    archive.db.execute("INSERT INTO jobs VALUES(?1,?2,'running',?3,?3,'{}') ON CONFLICT(id) DO UPDATE SET status='running',config=excluded.config,updated=excluded.updated",params![job,serde_json::to_string(&options)?,now()])?;
    archive.set_checkpoint(&format!("pause_reason:{job}"), &Value::Null)?;
    if options.resume.is_some() {
        tracing::info!(%job, "sync job resumed");
    } else {
        tracing::info!(%job, "new sync job started");
    }
    Ok(job)
}

pub async fn sync(root: &Path, mut options: SyncOptions) -> Result<()> {
    let mut archive = Archive::open(root, true)?;
    archive.db.execute_batch("CREATE TABLE IF NOT EXISTS peers(key TEXT PRIMARY KEY,input TEXT NOT NULL,metadata TEXT NOT NULL,raw TEXT NOT NULL); CREATE TABLE IF NOT EXISTS folders(id TEXT PRIMARY KEY,data TEXT NOT NULL);")?;
    let job = prepare_sync_job(&mut archive, &mut options)?;
    let (id, _) = credentials(root).await?;
    let session = Arc::new(SqliteSession::open(root.join("session.sqlite3")).await?);
    let SenderPool {
        runner,
        updates,
        handle,
    } = SenderPool::new(session.clone(), id);
    let _network_guard = NetworkGuard(handle.clone());
    let client = Client::new(handle.clone());
    let network = tokio::spawn(runner.run());
    if !client.is_authorized().await? {
        handle.quit();
        let _ = network.await;
        bail!("run tg-backup auth first");
    }
    let schema = Arc::new(Schema::current()?);
    let schema_hash = archive.register_schema(tl::LAYER, tl::API_SCHEMA)?;
    let account = client
        .invoke(&grammers_tl_types::functions::users::GetUsers {
            id: vec![grammers_tl_types::enums::InputUser::UserSelf],
        })
        .await?;
    let self_id = account
        .iter()
        .find_map(|u| match u {
            grammers_tl_types::enums::User::User(u) => Some(u.id),
            _ => None,
        })
        .context("account ID unavailable")?;
    if let Some(previous) = archive.checkpoint("account_id")? {
        ensure!(
            integer(&previous) == Some(self_id),
            "session belongs to a different account"
        );
    } else {
        archive.set_checkpoint("account_id", &json!(self_id.to_string()))?;
    }
    let state = if let Some(value) = archive.checkpoint("telegram_updates")? {
        decode_state(&value)?
    } else {
        let value = client
            .invoke(&grammers_tl_types::functions::updates::GetState {})
            .await?;
        let grammers_tl_types::enums::updates::State::State(s) = value;
        let state = UpdatesState {
            pts: s.pts,
            qts: s.qts,
            date: s.date,
            seq: s.seq,
            channels: vec![],
        };
        archive.set_checkpoint("telegram_updates", &encode_state(&state))?;
        state
    };
    {
        let mut statement = archive.db.prepare("SELECT raw FROM peers")?;
        let rows = statement
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for raw in rows {
            let value: Value = serde_json::from_str(&raw)?;
            let root = if value["_"] == "user" { "User" } else { "Chat" };
            let bytes = schema.encode(root, &value)?;
            let peer = if root == "User" {
                grammers_session::types::PeerInfo::from(grammers_tl_types::enums::User::from_bytes(
                    &bytes,
                )?)
            } else {
                grammers_session::types::PeerInfo::from(grammers_tl_types::enums::Chat::from_bytes(
                    &bytes,
                )?)
            };
            session.cache_peer(&peer).await?;
        }
    }
    session
        .set_update_state(UpdateState::All(state.clone()))
        .await?;
    let base_messages = archive
        .checkpoint(&format!("messages:{job}"))?
        .and_then(|v| integer(&v))
        .unwrap_or(0) as u64;
    let base_media = archive
        .checkpoint(&format!("media_bytes:{job}"))?
        .and_then(|v| integer(&v))
        .unwrap_or(0) as u64;
    let archive = Arc::new(Mutex::new(archive));
    let work_task = tokio::spawn(crate::work::run(archive.clone(), false));
    let stop = tokio_util::sync::CancellationToken::new();
    let mut engine = Engine {
        dc_auth_lock: Arc::new(tokio::sync::Mutex::new(())),
        #[cfg(test)]
        mock_rpc: None,
        client: client.clone(),
        session: session.clone(),
        archive: archive.clone(),
        schema: schema.clone(),
        schema_hash: schema_hash.clone(),
        job: job.clone(),
        options: options.clone(),
        takeout: None,
        started: Instant::now(),
        base_messages,
        base_media,
        stop: stop.clone(),
    };
    let deadline_task = options.max_seconds.map(|seconds| {
        let stop = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            stop.cancel();
        })
    });
    let signal = tokio::spawn({
        let stop = stop.clone();
        async move {
            let _ = crate::shutdown::signal().await;
            stop.cancel();
        }
    });
    // No lossy bounded update queue. The consumer commits each update before requesting another.
    let mut stream = client
        .stream_updates(
            updates,
            UpdatesConfiguration {
                catch_up: true,
                update_queue_limit: None,
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let update_engine = Engine {
        dc_auth_lock: engine.dc_auth_lock.clone(),
        #[cfg(test)]
        mock_rpc: None,
        client: client.clone(),
        session: session.clone(),
        archive: archive.clone(),
        schema,
        schema_hash,
        job: job.clone(),
        options: options.clone(),
        takeout: None,
        started: Instant::now(),
        base_messages,
        base_media,
        stop: stop.clone(),
    };
    let update_task = tokio::spawn(async move {
        let _cancel_on_failure = update_engine.stop.clone().drop_guard();
        let mut state = state;
        loop {
            let next = tokio::select! {_=update_engine.stop.cancelled()=>break,update=stream.next_raw()=>update};
            let (update, s, peers) = match next {
                Ok(v) => v,
                Err(e) => {
                    update_engine.archive.lock().unwrap().coverage(
                        "updates",
                        "recovery_error",
                        &json!({"error":e.to_string()}),
                    )?;
                    tokio::select! {_=update_engine.stop.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
                    continue;
                }
            };
            let update_value = update_engine.schema.decode("Update", &update.to_bytes())?;
            let expected = integer(&update_value["pts_count"]).unwrap_or(0) as i32;
            let gap = match s.message_box {
                Some(MessageBox::Common { pts }) if state.pts > 0 => {
                    pts - state.pts > expected.max(0)
                }
                Some(MessageBox::Channel { channel_id, pts }) => state
                    .channels
                    .iter()
                    .find(|c| c.id == channel_id)
                    .is_some_and(|c| pts - c.pts > expected.max(0)),
                _ => false,
            };
            if gap {
                update_engine.archive.lock().unwrap().coverage(&format!("update_gap:{}",now()),"potential_gap",&json!({"reason":"Telegram checkpoint advanced beyond the observed update sequence; audit cannot reconstruct missing edits", "state":format!("{s:?}")}))?;
            }
            state.date = s.date;
            state.seq = s.seq;
            match s.message_box {
                Some(MessageBox::Common { pts }) => state.pts = pts,
                Some(MessageBox::Secondary { qts }) => state.qts = qts,
                Some(MessageBox::Channel { channel_id, pts }) => {
                    if let Some(c) = state.channels.iter_mut().find(|c| c.id == channel_id) {
                        c.pts = pts;
                    } else {
                        state.channels.push(ChannelState {
                            id: channel_id,
                            pts,
                        });
                    }
                }
                None => {}
            }
            for peer in peers.iter_peers() {
                let (root, bytes) = match peer {
                    Peer::User(u) => ("User", u.raw.to_bytes()),
                    Peer::Group(g) => ("Chat", g.raw.to_bytes()),
                    Peer::Channel(c) => (
                        "Chat",
                        grammers_tl_types::enums::Chat::Channel(c.raw.clone()).to_bytes(),
                    ),
                };
                update_engine.capture(root, &bytes, "update_peer", None, None, None)?;
            }
            let bytes = update.to_bytes();
            let replay = if s.message_box.is_some() {
                Some(format!("update:{s:?}:{}", blake3::hash(&bytes).to_hex()))
            } else {
                None
            };
            update_engine.capture(
                "Update",
                &bytes,
                "live",
                Some("updates"),
                replay.as_deref(),
                Some(("telegram_updates", &encode_state(&state))),
            )?;
            // Never call stream.sync_update_state(): its queue may contain uncommitted updates.
        }
        Ok::<_, anyhow::Error>(())
    });
    let result = engine.run().await;
    let interrupted = stop.is_cancelled();
    stop.cancel();
    let updates_result = update_task.await;
    signal.abort();
    if let Some(task) = deadline_task {
        task.abort();
    }
    handle.quit();
    let _ = network.await;
    work_task.abort();
    let _ = work_task.await;
    let message_limit = engine.message_limit()?;
    let mut archive = archive.lock().unwrap();
    archive.materialize()?;
    let update_result = updates_result.map_err(anyhow::Error::from).and_then(|r| r);
    let update_failed = update_result.is_err();
    let result = update_result.and(result);
    let limit_reason = archive
        .checkpoint(&format!("pause_reason:{job}"))?
        .filter(|v| !v.is_null());
    let stop_reason = if options
        .max_seconds
        .is_some_and(|n| engine.started.elapsed().as_secs() >= n)
    {
        "duration_limit"
    } else if interrupted && !update_failed {
        "interrupted"
    } else if message_limit {
        "message_limit"
    } else if limit_reason.is_some() {
        "media_limit"
    } else if result.is_err() {
        "error"
    } else {
        "exhausted"
    };
    let failed_media: i64 = archive.db.query_row(
        "SELECT COUNT(*) FROM media WHERE status='failed'",
        [],
        |r| r.get(0),
    )?;
    archive.coverage(
        "media",
        if failed_media > 0 {
            "incomplete"
        } else {
            "complete"
        },
        &json!({"failed":failed_media}),
    )?;
    let incomplete:i64=archive.db.query_row("SELECT COUNT(*) FROM coverage WHERE status IN ('incomplete','failed_or_inaccessible','recovery_error','waiting_or_failed','inaccessible','limited','unsupported','requires_takeout','not_started','in_progress')",[],|r|r.get(0))?;
    let status = if update_failed || stop_reason == "error" {
        "failed"
    } else if stop_reason != "exhausted" {
        "paused"
    } else if incomplete > 0 {
        "complete_with_gaps"
    } else {
        "complete"
    };
    archive.db.execute("UPDATE jobs SET status=?2,updated=?3,details=json_patch(details,?4) WHERE id=?1",params![job,status,now(),json!({"stop_reason":stop_reason,"result":result.as_ref().err().map(ToString::to_string),"resume":format!("tg-backup sync --resume {job}")}).to_string()])?;
    result
}
fn encode_state(s: &UpdatesState) -> Value {
    json!({"pts":s.pts,"qts":s.qts,"date":s.date,"seq":s.seq,"channels":s.channels.iter().map(|c|json!({"id":c.id.to_string(),"pts":c.pts})).collect::<Vec<_>>()})
}
fn decode_state(v: &Value) -> Result<UpdatesState> {
    Ok(UpdatesState {
        pts: integer(&v["pts"]).context("pts")? as i32,
        qts: integer(&v["qts"]).context("qts")? as i32,
        date: integer(&v["date"]).context("date")? as i32,
        seq: integer(&v["seq"]).context("seq")? as i32,
        channels: v["channels"]
            .as_array()
            .context("channels")?
            .iter()
            .map(|c| {
                Ok(ChannelState {
                    id: integer(&c["id"]).context("channel id")?,
                    pts: integer(&c["pts"]).context("channel pts")? as i32,
                })
            })
            .collect::<Result<_>>()?,
    })
}
impl Engine {
    fn progress(&self, phase: &str, peers_done: u64, peers_total: Option<u64>) -> Result<()> {
        let a = self.archive.lock().unwrap();
        a.db.execute(
            "UPDATE jobs SET details=json_set(details,'$.requests_finished',0,'$.requests_failed',0,'$.active_method',NULL) WHERE id=?1 AND COALESCE(json_extract(details,'$.phase'),'') != ?2",
            params![self.job, phase],
        )?;
        let messages = a
            .checkpoint(&format!("messages:{}", self.job))?
            .unwrap_or(json!(0));
        let media = a
            .checkpoint(&format!("media_bytes:{}", self.job))?
            .unwrap_or(json!(0));
        a.db.execute("UPDATE jobs SET updated=?2,details=json_set(json_patch(details,?3),'$.peers_discovered',json_extract(?3,'$.peers_discovered'),'$.total_messages',NULL) WHERE id=?1",params![self.job,now(),json!({"phase":phase,"peers_visited":peers_done,"peers_discovered":peers_total,"messages_scanned":messages,"media_bytes":media,"elapsed_seconds":self.started.elapsed().as_secs(),"continuous":self.options.continuous,"total_messages":null}).to_string()])?;
        Ok(())
    }
    fn check_stop(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled(),
            "sync interrupted; resume job {}",
            self.job
        );
        ensure!(
            !self
                .options
                .max_seconds
                .is_some_and(|n| self.started.elapsed().as_secs() >= n),
            "duration limit reached; resume job {}",
            self.job
        );
        Ok(())
    }
    async fn rpc(
        &self,
        name: &str,
        args: Value,
        range: Option<&Value>,
        dc: Option<i32>,
    ) -> Result<(String, Vec<u8>, Value)> {
        self.check_stop()?;
        self.rpc_progress(Some(name), false, false)?;
        let result = self.rpc_inner(name, args, range, dc).await;
        self.rpc_progress(None, true, result.is_err())?;
        result
    }

    // Count logical requests (including each pagination request), not retry attempts.
    fn rpc_progress(&self, method: Option<&str>, finished: bool, failed: bool) -> Result<()> {
        let a = self.archive.lock().unwrap();
        let messages = a
            .checkpoint(&format!("messages:{}", self.job))?
            .unwrap_or(json!(0));
        let media = a
            .checkpoint(&format!("media_bytes:{}", self.job))?
            .unwrap_or(json!(0));
        a.db.execute(
            "UPDATE jobs SET updated=?2,details=json_set(details,
             '$.active_method',?3,
             '$.requests_finished',COALESCE(json_extract(details,'$.requests_finished'),0)+?4,
             '$.requests_failed',COALESCE(json_extract(details,'$.requests_failed'),0)+?5,
             '$.elapsed_seconds',?6,'$.messages_scanned',json(?7),'$.media_bytes',json(?8)) WHERE id=?1",
            params![self.job, now(), method, u64::from(finished), u64::from(failed), self.started.elapsed().as_secs(), messages.to_string(), media.to_string()],
        )?;
        Ok(())
    }

    async fn rpc_inner(
        &self,
        name: &str,
        args: Value,
        range: Option<&Value>,
        dc: Option<i32>,
    ) -> Result<(String, Vec<u8>, Value)> {
        self.check_stop()?;
        #[cfg(test)]
        if let Some(queue) = &self.mock_rpc {
            let expected = queue.lock().unwrap().pop_front().expect("unexpected RPC");
            assert_eq!(name, expected.method);
            assert_eq!(args, expected.args);
            assert_eq!(dc, expected.dc);
            let (_, root) = self.schema.request(name, args)?;
            let value = expected.result.map_err(anyhow::Error::msg)?;
            let bytes = self.schema.encode(&root, &value)?;
            return Ok((
                root.clone(),
                bytes.clone(),
                self.schema.decode(&root, &bytes)?,
            ));
        }
        let (mut bytes, root) = self.schema.request(name, args)?;
        let mut dc = dc;
        if let Some(range) = range {
            bytes = grammers_tl_types::functions::InvokeWithMessagesRange {
                range: grammers_tl_types::enums::MessageRange::from_bytes(
                    &self.schema.encode("MessageRange", range)?,
                )?,
                query: RawRequest(bytes),
            }
            .to_bytes();
        }
        if let Some(takeout) = self.takeout.filter(|_| takeout_method(name)) {
            bytes = grammers_tl_types::functions::InvokeWithTakeout {
                takeout_id: takeout,
                query: RawRequest(bytes),
            }
            .to_bytes();
        }
        let request = RawRequest(bytes);
        let mut auth_attempted = HashSet::new();
        for attempt in 0..8 {
            self.check_stop()?;
            let call = crate::dc_auth::invoke(
                &self.client,
                &request,
                dc,
                self.session.home_dc_id()?,
                &mut auth_attempted,
                &self.dc_auth_lock,
            );
            let result = tokio::select! {
                _ = self.stop.cancelled() => bail!("sync interrupted"),
                result = call => result.with_context(|| format!("authorizing DC {dc:?} for {name}"))?,
            };
            match result {
                Ok(response) => {
                    let (value, slices) = self.schema.decode_slices(&root, &response.0)?;
                    for slice in slices {
                        let bytes = &response.0[slice.offset..slice.offset + slice.length];
                        let peer = match slice.root.as_str() {
                            "User" => Some(grammers_session::types::PeerInfo::from(
                                grammers_tl_types::enums::User::from_bytes(bytes)?,
                            )),
                            "Chat" => Some(grammers_session::types::PeerInfo::from(
                                grammers_tl_types::enums::Chat::from_bytes(bytes)?,
                            )),
                            _ => None,
                        };
                        if let Some(peer) = peer {
                            self.session.cache_peer(&peer).await?;
                        }
                    }
                    return Ok((root, response.0, value));
                }
                Err(InvocationError::Rpc(e)) if e.name.starts_with("TAKEOUT_INIT_DELAY") => {
                    let retry_at = Utc::now().timestamp() + i64::from(e.value.unwrap_or(86400));
                    self.archive.lock().unwrap().set_checkpoint(
                        &format!("takeout:{}", self.job),
                        &json!({"retry_at":retry_at}),
                    )?;
                    bail!(
                        "takeout initialization delayed until Unix timestamp {retry_at}; resume job {}",
                        self.job
                    );
                }
                Err(InvocationError::Rpc(e))
                    if e.name == "TAKEOUT_INVALID" || e.name == "TAKEOUT_ID_INVALID" =>
                {
                    let a = self.archive.lock().unwrap();
                    a.set_checkpoint(&format!("takeout:{}", self.job), &json!({"expired":true}))?;
                    a.db.execute(
                        "DELETE FROM checkpoints WHERE key=?1 OR key LIKE ?2",
                        params![
                            format!("ranges:{}", self.job),
                            format!("peer_range:{}:%", self.job)
                        ],
                    )?;
                    self.stop.cancel();
                    bail!(
                        "takeout expired; resume job {} to initialize a replacement",
                        self.job
                    );
                }
                Err(InvocationError::Rpc(e))
                    if e.name.starts_with("FLOOD_WAIT")
                        || e.name.starts_with("FLOOD_PREMIUM_WAIT") =>
                {
                    let seconds = u64::from(e.value.unwrap_or(60));
                    self.archive.lock().unwrap().coverage(
                        name,
                        "rate_limited",
                        &json!({"retry_at":Utc::now().timestamp()+seconds as i64}),
                    )?;
                    tokio::select! {_=self.stop.cancelled()=>bail!("sync interrupted"),_=tokio::time::sleep(Duration::from_secs(seconds))=>{}}
                }
                Err(InvocationError::Rpc(e))
                    if e.code == 303 && dc.is_some() && e.value.is_some() =>
                {
                    dc = e.value.map(|d| d as i32);
                }
                Err(InvocationError::Rpc(e)) if e.code >= 500 && attempt < 7 => {
                    tokio::select! { _=self.stop.cancelled()=>bail!("sync interrupted"), _=tokio::time::sleep(Duration::from_secs(1<<attempt.min(5)))=>{} }
                }
                Err(e) if attempt < 7 && !matches!(e, InvocationError::Rpc(_)) => {
                    tokio::time::sleep(Duration::from_secs(1 << attempt.min(5))).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        bail!("RPC retry budget exhausted: {name}")
    }
    fn capture(
        &self,
        root: &str,
        bytes: &[u8],
        source: &str,
        scope: Option<&str>,
        replay: Option<&str>,
        checkpoint: Option<(&str, &Value)>,
    ) -> Result<Value> {
        let (value, slices) = self.schema.decode_slices(root, bytes)?;
        let at = now();
        let scope = scope.unwrap_or(source);
        let mut archive = self.archive.lock().unwrap();
        let self_id = archive
            .checkpoint("account_id")?
            .and_then(|v| integer(&v))
            .unwrap_or(0);
        for slice in &slices {
            if slice.root == "User" || slice.root == "Chat" {
                cache_peer(&archive, &slice.value, self_id)?;
            }
        }
        let selector = Selector::parse(&archive.config.history_selector)?;
        let attachment_selector = Selector::parse(&archive.config.attachment_selector)?;
        let excluded: Vec<(usize, usize, bool)> = if matches!(
            source,
            "live"
                | "history"
                | "messages.searchGlobal"
                | "messages.getRecentLocations"
                | "messages.getScheduledHistory"
                | "messages.getScheduledMessages"
        ) {
            slices
                .iter()
                .filter(|s| s.root == "Message")
                .filter_map(|s| {
                    let meta = peer_key(&s.value["peer_id"])
                        .and_then(|p| peer_metadata(&archive, &p).ok().flatten())
                        .unwrap_or(json!({}));
                    let meta = message_context(&meta, &s.value);
                    let outside_call_range = source == "messages.searchGlobal"
                        && extra::outside_call_bounds(&self.options, &s.value);
                    (!selector.matches(&meta) || outside_call_range).then(|| {
                        (
                            s.offset,
                            s.offset + s.length,
                            attachment_selector.matches(&meta),
                        )
                    })
                })
                .collect()
        } else {
            vec![]
        };
        let mut records = vec![];
        // Preserve response-level fields as well as independently identifiable child objects.
        if excluded.is_empty() {
            records.push(Capture {
                key: format!("{root}:{scope}"),
                kind: if root == "Update" {
                    "update"
                } else {
                    extra::envelope_kind(source)
                }
                .into(),
                root_type: root.into(),
                bytes: bytes.to_vec(),
                observed_at: at,
                source: source.into(),
                metadata: json!({"scope":scope}),
                replay_key: replay.map(|r| format!("{r}:envelope")),
                partial: false,
                deleted: false,
            });
        }
        let self_id = archive
            .checkpoint("account_id")?
            .and_then(|v| integer(&v))
            .unwrap_or(0);
        let mut media = vec![];
        let mut unique = HashSet::new();
        for slice in &slices {
            if excluded.iter().any(|(start, end, keep_media)| {
                slice.offset >= *start
                    && slice.offset + slice.length <= *end
                    && !(*keep_media && matches!(slice.root.as_str(), "Photo" | "Document"))
            }) {
                continue;
            }
            let v = &slice.value;
            let constructor = v["_"].as_str().unwrap_or("");
            let owner_scope = if slice.root == "UserStatus" {
                slices
                    .iter()
                    .find(|p| {
                        p.offset <= slice.offset
                            && p.offset + p.length >= slice.offset + slice.length
                            && (p.root == "User" || p.value["_"] == "updateUserStatus")
                    })
                    .and_then(|p| integer(&p.value["id"]).or_else(|| integer(&p.value["user_id"])))
                    .map(|id| format!("user:{id}"))
            } else {
                None
            };
            let containing_message = slices.iter().find(|parent| {
                parent.root == "Message"
                    && parent.offset <= slice.offset
                    && parent.offset + parent.length >= slice.offset + slice.length
            });
            let message_scope = containing_message.map(|parent| {
                let scheduled = slices.iter().any(|update| {
                    update.value["_"] == "updateNewScheduledMessage"
                        && update.offset <= parent.offset
                        && update.offset + update.length >= parent.offset + parent.length
                });
                let shortcut_scope = if source == "messages.getQuickReplies" {
                    value["quick_replies"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|reply| reply["top_message"] == parent.value["id"])
                        .and_then(|reply| integer(&reply["shortcut_id"]))
                        .map(|id| format!("account/quick_reply:{id}"))
                } else {
                    None
                };
                extra::message_scope(
                    &parent.value,
                    source,
                    shortcut_scope.as_deref().unwrap_or(scope),
                    scheduled,
                )
            });
            let parent_identity = containing_message.and_then(|parent| {
                identity(
                    "Message",
                    &parent.value,
                    message_scope.as_deref().unwrap_or(scope),
                )
            });
            let inferred = parent_identity
                .as_ref()
                .and_then(|(key, _)| extra::message_child_identity(&slice.root, v, key))
                .or_else(|| {
                    identity(
                        &slice.root,
                        v,
                        if slice.root == "Message" {
                            message_scope.as_deref().unwrap_or(scope)
                        } else {
                            owner_scope.as_deref().unwrap_or(scope)
                        },
                    )
                });
            let Some((mut key, kind)) = inferred else {
                continue;
            };

            let mut metadata = scope
                .split('/')
                .find(|part| {
                    part.starts_with("user:")
                        || part.starts_with("chat:")
                        || part.starts_with("channel:")
                })
                .map(|key| peer_metadata(&archive, key))
                .transpose()?
                .flatten()
                .unwrap_or(json!({"category":"account"}));
            if let Some(parent) = containing_message
                && let Some(peer) = peer_key(&parent.value["peer_id"])
            {
                if let Some(m) = peer_metadata(&archive, &peer)? {
                    merge(&mut metadata, &m);
                }
                metadata["peer"] = json!(peer);
                metadata["message_id"] = parent.value["id"].clone();
                metadata["topic"] = parent.value["reply_to"]["reply_to_top_id"].clone();
                metadata["date"] = parent.value["date"].clone();
                metadata["outgoing"] = json!(parent.value["out"] == true);
                metadata["sender"] =
                    peer_key(&parent.value["from_id"]).map_or(Value::Null, Value::String);
            }
            if let Some((message_key, message_kind)) = &parent_identity {
                metadata["message_key"] = json!(message_key);
                metadata["message_kind"] = json!(message_kind);
                if let Some(parent) = containing_message {
                    metadata["quick_reply_shortcut_id"] =
                        parent.value["quick_reply_shortcut_id"].clone();
                    if metadata["quick_reply_shortcut_id"].is_null()
                        && let Some(shortcut) = message_scope
                            .as_deref()
                            .and_then(|s| s.strip_prefix("account/quick_reply:"))
                    {
                        metadata["quick_reply_shortcut_id"] = json!(shortcut);
                    }
                }
            }
            if let Some(story) = slices
                .iter()
                .filter(|parent| {
                    parent.root == "StoryItem"
                        && parent.offset <= slice.offset
                        && parent.offset + parent.length >= slice.offset + slice.length
                })
                .min_by_key(|p| p.length)
            {
                let peer = slices
                    .iter()
                    .filter(|parent| {
                        parent.offset <= story.offset
                            && parent.offset + parent.length >= story.offset + story.length
                    })
                    .filter_map(|parent| match parent.root.as_str() {
                        "PeerStories" | "Update" => peer_key(&parent.value["peer"]),
                        "StoryView" | "StoryReaction" => peer_key(&parent.value["peer_id"]),
                        _ => None,
                    })
                    .next()
                    .or_else(|| {
                        scope
                            .split('/')
                            .find(|s| s.starts_with("user:") || s.starts_with("channel:"))
                            .map(str::to_owned)
                    })
                    .or_else(|| {
                        scope
                            .starts_with("own_stories")
                            .then(|| format!("user:{self_id}"))
                    });
                if let Some(peer) = &peer {
                    if slice.root == "StoryItem" {
                        key = format!("{peer}/story:{}", integer(&v["id"]).context("story ID")?);
                    }
                    metadata["peer"] = json!(peer);
                }
                metadata["story_peer"] = json!(peer);
                metadata["story_id"] = story.value["id"].clone();
            }
            for parent in slices.iter().filter(|parent| {
                parent.offset <= slice.offset
                    && parent.offset + parent.length >= slice.offset + slice.length
            }) {
                if parent.root == "StarGift" && parent.value["slug"].is_string() {
                    metadata["gift_slug"] = parent.value["slug"].clone();
                }
                if parent.root == "SavedStarGift" {
                    if let Some(id) = integer(&parent.value["saved_id"]) {
                        if let Some(peer) = scope.split('/').find(|s| s.starts_with("channel:")) {
                            let input: String = archive.db.query_row(
                                "SELECT input FROM peers WHERE key=?1",
                                [peer],
                                |r| r.get(0),
                            )?;
                            metadata["saved_gift_reference"] = json!({"_":"inputSavedStarGiftChat","peer":serde_json::from_str::<Value>(&input)?,"saved_id":id.to_string()});
                        }
                    } else if let Some(id) = integer(&parent.value["msg_id"]) {
                        metadata["saved_gift_reference"] =
                            json!({"_":"inputSavedStarGiftUser","msg_id":id});
                    }
                }
            }
            merge(
                &mut metadata,
                &json!({"id":v.get("id").map(|v|integer(v).map(|x|x.to_string()).unwrap_or_else(||v.to_string())),"scope":scope}),
            );
            if let Some(peer) = peer_key(&v["peer_id"]) {
                metadata["peer"] = json!(peer);
                if let Some(m) = peer_metadata(&archive, &peer)? {
                    merge(&mut metadata, &m);
                }
            }
            if slice.root == "StarsTransaction" {
                if let Some(peer) = peer_key(&v["peer"]["peer"]) {
                    metadata["peer"] = json!(peer);
                }
                metadata["message_id"] = v["msg_id"].clone();
                metadata["date"] = v["date"].clone();
            }
            if slice.root == "Message" {
                metadata["id"] = json!(integer(&v["id"]).map(|id| id.to_string()));
                metadata["message_id"] = metadata["id"].clone();
                metadata["date"] = v["date"].clone();
                metadata["outgoing"] = json!(v["out"] == true);
                metadata["sender"] = peer_key(&v["from_id"]).map_or(Value::Null, Value::String);
                metadata["topic"] = v["reply_to"]["reply_to_top_id"].clone();
                metadata["revision"] = json!(
                    integer(&v["edit_date"])
                        .or_else(|| integer(&v["date"]))
                        .unwrap_or(0)
                        * 1_000_000
                );
            }
            if matches!(kind.as_str(), "scheduled_message" | "quick_reply_message") {
                metadata["revision"] = json!(
                    checkpoint
                        .and_then(|(_, progress)| integer(&progress["snapshot_revision"]))
                        .unwrap_or(at)
                );
            }
            if slice.root == "User" || slice.root == "Chat" {
                cache_peer(&archive, v, self_id)?;
                if let Some(m) = peer_metadata(&archive, &key)? {
                    merge(&mut metadata, &m);
                }
            }
            if !unique.insert((
                key.clone(),
                blake3::hash(&bytes[slice.offset..slice.offset + slice.length]),
            )) {
                continue;
            }
            let index = records.len();
            records.push(Capture {
                key: key.clone(),
                kind,
                root_type: slice.root.clone(),
                bytes: bytes[slice.offset..slice.offset + slice.length].to_vec(),
                observed_at: at,
                source: source.into(),
                metadata,
                replay_key: replay.map(|r| format!("{r}:{key}:{}", slice.offset)),
                partial: v["min"] == true || constructor.ends_with("Empty"),
                deleted: false,
            });
            if matches!(constructor, "document" | "photo") {
                media.push((index, v.clone()));
            }
            if matches!(
                constructor,
                "dialogFilter" | "dialogFilterChatlist" | "dialogFilterDefault"
            ) {
                archive.db.execute("INSERT INTO folders VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET data=excluded.data",params![integer(&v["id"]).unwrap_or(0).to_string(),v.to_string()])?;
            }
        }
        if root == "Update" {
            if value["_"] == "updateDialogFilter" && value["filter"].is_null() {
                archive.db.execute(
                    "DELETE FROM folders WHERE id=?1",
                    [integer(&value["id"]).unwrap_or(0).to_string()],
                )?;
            }
            let key = peer_key(&value["peer"])
                .or_else(|| integer(&value["channel_id"]).map(|id| format!("channel:{id}")));
            if let Some(key) = key
                && let Some(mut meta) = peer_metadata(&archive, &key)?
            {
                if let Some(unread) = integer(&value["still_unread_count"]) {
                    meta["unread"] = json!(unread > 0);
                }
                archive.db.execute(
                    "UPDATE peers SET metadata=?2 WHERE key=?1",
                    params![key, meta.to_string()],
                )?;
            }
        }
        for update in slices.iter().filter(|s| s.root == "Update") {
            deletion_records(
                &archive,
                &update.value,
                &bytes[update.offset..update.offset + update.length],
                at,
                replay,
                &mut records,
            )?;
        }
        let ids = archive.ingest(&self.schema_hash, &records, checkpoint)?;
        for (index, v) in media {
            discover_media(&archive, &v, ids[index])?;
        }
        for id in ids {
            let record = archive.record(id)?;
            archive.link_media(id, &record.data)?;
        }
        archive.db.execute(
            "UPDATE jobs SET updated=?2 WHERE id=?1",
            params![self.job, now()],
        )?;
        archive.materialize()?;
        Ok(value)
    }
    async fn fetch(&self, name: &str, args: Value, scope: &str) -> Result<Value> {
        let (root, bytes, _) = self.rpc(name, args, None, None).await?;
        self.capture(&root, &bytes, name, Some(scope), None, None)
    }
    async fn collect(&self, name: &str, args: Value, scope: &str) -> Result<Option<Value>> {
        match self.fetch(name, args, scope).await {
            Ok(v) => {
                self.archive.lock().unwrap().coverage(
                    scope,
                    "complete",
                    &json!({"method":name}),
                )?;
                Ok(Some(v))
            }
            Err(e) => {
                self.check_stop()?;
                self.archive.lock().unwrap().coverage(
                    scope,
                    "failed_or_inaccessible",
                    &json!({"method":name,"error":e.to_string()}),
                )?;
                tracing::warn!(collector=scope,error=%e,"collector incomplete");
                Ok(None)
            }
        }
    }
    async fn run(&mut self) -> Result<()> {
        if self.options.takeout {
            self.progress("takeout_preparation", 0, None)?;
            self.collect(
                "messages.getDialogFilters",
                json!({}),
                "messages.getDialogFilters",
            )
            .await?;
            self.dialogs().await?;
            self.refresh_folders()?;
            self.progress("takeout_initialization", 0, None)?;
            self.start_takeout().await?;
        }
        loop {
            self.check_stop()?;
            self.progress("media_reconciliation", 0, None)?;
            self.reconcile_media()?;
            self.archive
                .lock()
                .unwrap()
                .db
                .execute("UPDATE media SET retry_at=0 WHERE status='deferred'", [])?;
            self.progress("coverage_preparation", 0, None)?;
            self.prepare_extra_coverage()?;
            self.progress("account_collectors", 0, None)?;
            self.account_collectors().await?;
            self.progress("dialogs", 0, None)?;
            self.dialogs().await?;
            self.progress("folders", 0, None)?;
            self.refresh_folders()?;
            self.progress("extra_account_collectors", 0, None)?;
            self.extra_account_collectors().await?;
            self.progress("peer_preparation", 0, None)?;
            let peers = self.peer_list()?;
            self.prepare_extra_peers(&peers)?;
            let total = peers.len() as u64;
            for (index, (key, input, metadata, raw)) in peers.into_iter().enumerate() {
                self.progress("peer_selection", index as u64, Some(total))?;
                self.check_stop()?;
                let selected = {
                    let a = self.archive.lock().unwrap();
                    peer_selected(&a.config, &metadata)?
                };
                if !selected {
                    continue;
                }
                self.progress("peer_collectors", index as u64, Some(total))?;
                self.peer_collectors(&key, &input, &raw).await?;
                self.progress("extra_peer_collectors", index as u64, Some(total))?;
                self.extra_peer_collectors(&key, &input, &raw).await?;
                self.progress("history", index as u64, Some(total))?;
                if let Err(e) = self.history(&key, &input).await {
                    self.archive.lock().unwrap().coverage(
                        &format!("history:{key}"),
                        "incomplete",
                        &json!({"error":e.to_string()}),
                    )?;
                    tracing::warn!(peer=key,error=%e,"history incomplete");
                }
                self.progress("media", index as u64 + 1, Some(total))?;
                self.download_pending().await?;
                self.progress("media", index as u64 + 1, Some(total))?;
                if self.message_limit()? {
                    return Ok(());
                }
            }
            self.progress("enrichment", total, Some(total))?;
            self.extra_enrichment().await?;
            self.progress("coverage_finalization", total, Some(total))?;
            self.finish_extra_coverage()?;
            self.progress("media", total, Some(total))?;
            self.download_pending().await?;
            self.progress("finalization", total, Some(total))?;
            {
                let a = self.archive.lock().unwrap();
                let current = a.config.epoch.key(Utc::now());
                if a.epochs()?
                    .iter()
                    .any(|(_, name, _, sealed)| name != &current && !sealed)
                {
                    a.enqueue_work("seal", &format!("seal:{current}"), &json!({}), true)?;
                }
            }
            if !self.options.continuous {
                let (pending, paused) = {
                    let a = self.archive.lock().unwrap();
                    let pending: i64 = a.db.query_row(
                        "SELECT COUNT(*) FROM media WHERE status NOT IN ('complete','deferred','unavailable')",
                        [],
                        |r| r.get(0),
                    )?;
                    let paused = a
                        .checkpoint(&format!("pause_reason:{}", self.job))?
                        .is_some_and(|v| !v.is_null());
                    (pending, paused)
                };
                if self.takeout.is_some() && pending == 0 && !paused {
                    self.progress("takeout_finalization", total, Some(total))?;
                    self.finish_takeout(true).await?;
                }
                break;
            }
            self.progress("waiting", total, Some(total))?;
            let wait = self.archive.lock().unwrap().config.metadata_refresh_seconds;
            tokio::select! {_=self.stop.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(wait))=>{}}
        }
        Ok(())
    }
    async fn start_takeout(&mut self) -> Result<()> {
        let key = format!("takeout:{}", self.job);
        if let Some(v) = self.archive.lock().unwrap().checkpoint(&key)? {
            if let Some(retry) = integer(&v["retry_at"]) {
                ensure!(
                    Utc::now().timestamp() >= retry,
                    "takeout is waiting until Unix timestamp {retry}; resume job {}",
                    self.job
                );
            }
            if let Some(id) = integer(&v["id"]) {
                self.takeout = Some(id);
                return Ok(());
            }
        }
        let max = self.archive.lock().unwrap().config.max_file_bytes;
        let (history_selector, media_selector) = {
            let a = self.archive.lock().unwrap();
            (
                Selector::parse(&a.config.history_selector)?,
                Selector::parse(&a.config.attachment_selector)?,
            )
        };
        let mut flags = json!({"contacts":true});
        let mut files = media_selector.matches(&json!({"category":"account"}));
        for (_, _, metadata, raw) in self.peer_list()? {
            files |= media_selector.may_match(&metadata);
            if !history_selector.may_match(&metadata) && !media_selector.may_match(&metadata) {
                continue;
            }
            match metadata["category"].as_str() {
                Some("personal" | "bot" | "saved") => flags["message_users"] = json!(true),
                Some("channel") => flags["message_channels"] = json!(true),
                Some("group") => {
                    flags["message_megagroups"] = json!(true);
                    if raw["_"] == "chat" {
                        flags["message_chats"] = json!(true);
                    }
                }
                _ => {}
            }
        }
        if files {
            flags["files"] = json!(true);
            flags["file_max_size"] = json!(max.to_string());
        }
        match self
            .rpc("account.initTakeoutSession", flags, None, None)
            .await
        {
            Ok((_, _, v)) => {
                let id = integer(&v["id"]).context("takeout ID")?;
                self.archive
                    .lock()
                    .unwrap()
                    .set_checkpoint(&key, &json!({"id":id.to_string()}))?;
                self.takeout = Some(id);
                Ok(())
            }
            Err(e) => {
                self.archive.lock().unwrap().coverage(
                    "takeout",
                    "waiting_or_failed",
                    &json!({"error":e.to_string(),"job":self.job}),
                )?;
                Err(e)
            }
        }
    }
    async fn finish_takeout(&mut self, success: bool) -> Result<()> {
        self.rpc(
            "account.finishTakeoutSession",
            if success {
                json!({"success":true})
            } else {
                json!({})
            },
            None,
            None,
        )
        .await?;
        self.archive.lock().unwrap().set_checkpoint(
            &format!("takeout:{}", self.job),
            &json!({"finished":success}),
        )?;
        self.takeout = None;
        Ok(())
    }
    async fn ranges(&self) -> Result<Vec<Option<Value>>> {
        if self.takeout.is_none() {
            return Ok(vec![None]);
        }
        let key = format!("ranges:{}", self.job);
        if let Some(v) = self.archive.lock().unwrap().checkpoint(&key)? {
            return Ok(v
                .as_array()
                .context("saved split ranges")?
                .iter()
                .cloned()
                .map(Some)
                .collect());
        }
        let v = self
            .fetch("messages.getSplitRanges", json!({}), "takeout_ranges")
            .await?;
        self.archive.lock().unwrap().set_checkpoint(&key, &v)?;
        Ok(v.as_array()
            .context("split ranges")?
            .iter()
            .cloned()
            .map(Some)
            .collect())
    }
    async fn dialogs(&self) -> Result<()> {
        for (range_index, range) in self.ranges().await?.iter().enumerate() {
            for folder_id in [0, 1] {
                let mut offset_date = 0;
                let mut offset_id = 0;
                let mut offset_peer = json!({"_":"inputPeerEmpty"});
                loop {
                    let (root,bytes,v)=self.rpc("messages.getDialogs",json!({"folder_id":folder_id,"offset_date":offset_date,"offset_id":offset_id,"offset_peer":offset_peer,"limit":100,"hash":"0"}),range.as_ref(),None).await?;
                    self.capture(
                        &root,
                        &bytes,
                        "dialogs",
                        Some(&format!("dialogs:{range_index}")),
                        None,
                        None,
                    )?;
                    let dialogs = v["dialogs"].as_array().cloned().unwrap_or_default();
                    if dialogs.is_empty() {
                        break;
                    }
                    for dialog in &dialogs {
                        if let Some(key) = peer_key(&dialog["peer"]) {
                            let a = self.archive.lock().unwrap();
                            if let Some(mut meta) = peer_metadata(&a, &key)? {
                                meta["archived"] = json!(integer(&dialog["folder_id"]) == Some(1));
                                meta["unread"] =
                                    json!(integer(&dialog["unread_count"]).unwrap_or(0) > 0);
                                meta["muted"] = json!(
                                    integer(&dialog["notify_settings"]["mute_until"]).unwrap_or(0)
                                        > Utc::now().timestamp()
                                );
                                a.db.execute(
                                    "UPDATE peers SET metadata=?2 WHERE key=?1",
                                    params![key, meta.to_string()],
                                )?;
                            }
                            a.set_checkpoint(
                                &format!("peer_range:{}:{key}:{range_index}", self.job),
                                &json!(true),
                            )?;
                        }
                    }
                    let last = dialogs.last().unwrap();
                    let key = peer_key(&last["peer"]).context("dialog peer missing")?;
                    offset_peer = self.input_peer(&key)?;
                    let next = integer(&last["top_message"]).unwrap_or(0);
                    ensure!(
                        next != offset_id || offset_id == 0,
                        "dialog pagination made no progress"
                    );
                    offset_id = next;
                    offset_date = v["messages"]
                        .as_array()
                        .and_then(|a| {
                            a.iter().find(|m| {
                                integer(&m["id"]) == Some(offset_id)
                                    && peer_key(&m["peer_id"]).as_deref() == Some(&key)
                            })
                        })
                        .and_then(|m| integer(&m["date"]))
                        .unwrap_or(0);
                    if dialogs.len() < 100 {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    fn input_peer(&self, key: &str) -> Result<Value> {
        let s: String = self.archive.lock().unwrap().db.query_row(
            "SELECT input FROM peers WHERE key=?1",
            [key],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&s)?)
    }
    fn peer_list(&self) -> Result<Vec<(String, Value, Value, Value)>> {
        let a = self.archive.lock().unwrap();
        let rows =
            a.db.prepare("SELECT key,input,metadata,raw FROM peers ORDER BY key")?
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(k, i, m, r)| {
                Ok((
                    k,
                    serde_json::from_str(&i)?,
                    serde_json::from_str(&m)?,
                    serde_json::from_str(&r)?,
                ))
            })
            .collect()
    }
    fn refresh_folders(&self) -> Result<()> {
        let a = self.archive.lock().unwrap();
        let filters: Vec<String> =
            a.db.prepare("SELECT data FROM folders")?
                .query_map([], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
        drop(a);
        for (key, _, mut metadata, _) in self.peer_list()? {
            let mut folders = vec![];
            for raw in &filters {
                let filter: Value = serde_json::from_str(raw)?;
                if folder_matches(&filter, &key, &metadata) {
                    folders.push(filter["id"].clone());
                }
            }
            metadata["folder"] = json!(folders);
            self.archive.lock().unwrap().db.execute(
                "UPDATE peers SET metadata=?2 WHERE key=?1",
                params![key, metadata.to_string()],
            )?;
        }
        Ok(())
    }
    fn message_limit(&self) -> Result<bool> {
        let total = self
            .archive
            .lock()
            .unwrap()
            .checkpoint(&format!("messages:{}", self.job))?
            .and_then(|v| integer(&v))
            .unwrap_or(0) as u64;
        let reached = self
            .options
            .max_messages
            .is_some_and(|n| total.saturating_sub(self.base_messages) >= n);
        if reached {
            self.archive.lock().unwrap().set_checkpoint(
                &format!("pause_reason:{}", self.job),
                &json!("message_limit"),
            )?;
        }
        Ok(reached)
    }
    async fn history(&self, key: &str, peer: &Value) -> Result<()> {
        for (range_index, range) in self.ranges().await?.iter().enumerate() {
            if self.takeout.is_some()
                && self
                    .archive
                    .lock()
                    .unwrap()
                    .checkpoint(&format!("peer_range:{}:{key}:{range_index}", self.job))?
                    .is_none()
            {
                continue;
            }
            let policy = serde_json::to_vec(
                &json!({"min_id":self.options.min_id,"max_id":self.options.max_id,"since":self.options.since,"until":self.options.until,"range":range,"history_selector":self.options.history_selector,"attachment_selector":self.options.attachment_selector}),
            )?;
            let cursor_key = format!("history:{}:{key}", blake3::hash(&policy).to_hex());
            let saved = self
                .archive
                .lock()
                .unwrap()
                .checkpoint(&cursor_key)?
                .unwrap_or(json!({}));
            let audit_secs = self.archive.lock().unwrap().config.audit_interval_seconds;
            let completed = integer(&saved["completed_at"]);
            if completed.is_some() && !self.options.continuous && saved["job"] == self.job {
                continue;
            }
            let audit = integer(&saved["audited_at"])
                .is_some_and(|t| now() - t > (audit_secs as i64) * 1_000_000);
            let incremental = completed.is_some() && !audit;
            let mut offset = if completed.is_some() {
                0
            } else {
                integer(&saved["offset"]).unwrap_or(0)
            };
            let mut newest = integer(&saved["newest"]).unwrap_or(0);
            let lower = if incremental {
                newest
                    .saturating_sub(100)
                    .max(self.options.min_id.unwrap_or(0))
            } else {
                self.options.min_id.unwrap_or(0)
            };
            loop {
                self.check_stop()?;
                if self.message_limit()? {
                    return Ok(());
                }
                self.refresh_folders()?;
                let selected = {
                    let a = self.archive.lock().unwrap();
                    peer_selected(&a.config, &peer_metadata(&a, key)?.unwrap_or(json!({})))?
                };
                if !selected {
                    return Ok(());
                }
                let total = self
                    .archive
                    .lock()
                    .unwrap()
                    .checkpoint(&format!("messages:{}", self.job))?
                    .and_then(|v| integer(&v))
                    .unwrap_or(0) as u64;
                let limit = self
                    .options
                    .max_messages
                    .map(|n| {
                        n.saturating_sub(total.saturating_sub(self.base_messages))
                            .min(100)
                    })
                    .unwrap_or(100);
                let args = json!({"peer":peer,"offset_id":offset,"offset_date":self.options.until.unwrap_or(0),"add_offset":0,"limit":limit,"max_id":self.options.max_id.unwrap_or(0),"min_id":lower,"hash":"0"});
                let response = self
                    .rpc("messages.getHistory", args, range.as_ref(), None)
                    .await;
                let (root, bytes, v) = match response {
                    Err(e)
                        if self.takeout.is_some() && e.to_string().contains("CHANNEL_PRIVATE") =>
                    {
                        self.archive.lock().unwrap().coverage(&format!("access:{key}"),"limited",&json!({"reason":"Only own messages accessible in left/private channel"}))?;
                        self.rpc("messages.search",json!({"peer":peer,"q":"","from_id":{"_":"inputPeerSelf"},"filter":{"_":"inputMessagesFilterEmpty"},"min_date":self.options.since.unwrap_or(0),"max_date":self.options.until.unwrap_or(0),"offset_id":offset,"add_offset":0,"limit":limit,"max_id":self.options.max_id.unwrap_or(0),"min_id":lower,"hash":"0"}),range.as_ref(),None).await?
                    }
                    result => result?,
                };
                let messages = v["messages"].as_array().cloned().unwrap_or_default();
                let next = messages
                    .iter()
                    .filter_map(|m| integer(&m["id"]))
                    .min()
                    .unwrap_or(offset);
                newest = newest.max(
                    messages
                        .iter()
                        .filter_map(|m| integer(&m["id"]))
                        .max()
                        .unwrap_or(0),
                );
                let old = messages
                    .iter()
                    .filter_map(|m| integer(&m["date"]))
                    .min()
                    .is_some_and(|d| self.options.since.is_some_and(|since| d < since));
                let done = messages.is_empty() || next <= lower + 1 || old;
                ensure!(
                    done || offset == 0 || next < offset,
                    "history pagination made no progress"
                );
                let progress = json!({"offset":next,"newest":newest,"completed_at":if done{Some(now())}else{None},"audited_at":if done && !incremental{Some(now())}else{integer(&saved["audited_at"])},"job":self.job,"messages":total+messages.len() as u64,"counter_key":format!("messages:{}",self.job)});
                let replay = format!(
                    "{}:{key}:{range_index}:{offset}:{}",
                    self.job,
                    blake3::hash(&bytes).to_hex()
                );
                let mut filtered = v.clone();
                let selected: Vec<Value> = {
                    let a = self.archive.lock().unwrap();
                    let selector = Selector::parse(&a.config.history_selector)?;
                    let media_selector = Selector::parse(&a.config.attachment_selector)?;
                    let meta = peer_metadata(&a, key)?.unwrap_or(json!({}));
                    messages
                        .iter()
                        .filter(|m| {
                            let date = integer(&m["date"]);
                            let inside = !self
                                .options
                                .since
                                .is_some_and(|min| date.is_some_and(|d| d < min))
                                && !self
                                    .options
                                    .until
                                    .is_some_and(|max| date.is_some_and(|d| d >= max));
                            let context = message_context(&meta, m);
                            inside
                                && (selector.matches(&context) || media_selector.matches(&context))
                        })
                        .cloned()
                        .collect()
                };
                filtered["messages"] = json!(selected);
                let selected_bytes = self.schema.encode(&root, &filtered)?;
                self.capture(
                    &root,
                    &selected_bytes,
                    "history",
                    Some(key),
                    Some(&replay),
                    Some((&cursor_key, &progress)),
                )?;
                self.archive
                    .lock()
                    .unwrap()
                    .set_checkpoint(&format!("messages:{}", self.job), &progress["messages"])?;
                let enriched: Vec<Value> = {
                    let a = self.archive.lock().unwrap();
                    let selector = Selector::parse(&a.config.history_selector)?;
                    let meta = peer_metadata(&a, key)?.unwrap_or(json!({}));
                    selected
                        .into_iter()
                        .filter(|m| selector.matches(&message_context(&meta, m)))
                        .collect()
                };
                self.message_enrichment(key, peer, &enriched).await?;
                if done {
                    self.archive.lock().unwrap().coverage(
                        &format!("history:{key}"),
                        "complete",
                        &progress,
                    )?;
                    break;
                }
                offset = next;
            }
        }
        Ok(())
    }
    async fn message_enrichment(&self, key: &str, peer: &Value, messages: &[Value]) -> Result<()> {
        let mut custom = HashSet::new();
        fn emojis(v: &Value, ids: &mut HashSet<i64>) {
            match v {
                Value::Object(o) => {
                    if matches!(
                        o.get("_").and_then(Value::as_str),
                        Some("messageEntityCustomEmoji" | "reactionCustomEmoji")
                    ) && let Some(id) = o.get("document_id").and_then(integer)
                    {
                        ids.insert(id);
                    }
                    for v in o.values() {
                        emojis(v, ids);
                    }
                }
                Value::Array(a) => {
                    for v in a {
                        emojis(v, ids);
                    }
                }
                _ => {}
            }
        }
        for m in messages {
            emojis(m, &mut custom);
        }
        let ids: Vec<String> = custom.into_iter().map(|id| id.to_string()).collect();
        for chunk in ids.chunks(100) {
            self.collect(
                "messages.getCustomEmojiDocuments",
                json!({"document_id":chunk}),
                &format!("{key}/custom_emoji"),
            )
            .await?;
        }
        // Recent read information is transient. Older message payloads still retain their supplied state.
        for m in messages {
            let Some(id) = integer(&m["id"]) else {
                continue;
            };
            if m["media"]["_"] == "messageMediaPoll" {
                self.collect(
                    "messages.getPollResults",
                    json!({"peer":peer,"msg_id":id,"poll_hash":"0"}),
                    &format!("{key}/message:{id}/poll"),
                )
                .await?;
            }
            if m["reactions"]["can_see_list"] == true {
                let mut args = json!({"peer":peer,"id":id,"limit":100});
                let mut seen = HashSet::new();
                loop {
                    let Some(v) = self
                        .collect(
                            "messages.getMessageReactionsList",
                            args.clone(),
                            &format!("{key}/message:{id}/reactions"),
                        )
                        .await?
                    else {
                        break;
                    };
                    let Some(offset) = v["next_offset"].as_str().filter(|s| !s.is_empty()) else {
                        break;
                    };
                    ensure!(
                        seen.insert(offset.to_string()),
                        "reaction cursor did not advance"
                    );
                    args["offset"] = json!(offset);
                }
            }
            if integer(&m["date"]).is_some_and(|date| date > Utc::now().timestamp() - 7 * 86400) {
                self.collect(
                    "messages.getMessageReadParticipants",
                    json!({"peer":peer,"msg_id":id}),
                    &format!("{key}/message:{id}/readers"),
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn account_collectors(&self) -> Result<()> {
        let singleton = [
            ("users.getFullUser", json!({"id":{"_":"inputUserSelf"}})),
            ("contacts.getContacts", json!({"hash":"0"})),
            ("contacts.getStatuses", json!({})),
            ("account.getAuthorizations", json!({})),
            ("account.getWebAuthorizations", json!({})),
            ("account.getGlobalPrivacySettings", json!({})),
            ("account.getAccountTTL", json!({})),
            ("account.getNotifyExceptions", json!({})),
            ("account.getAutoDownloadSettings", json!({})),
            ("account.getAutoSaveSettings", json!({})),
            ("account.getContentSettings", json!({})),
            ("account.getWallPapers", json!({"hash":"0"})),
            ("account.getThemes", json!({"format":"tdesktop","hash":"0"})),
            ("account.getSavedRingtones", json!({"hash":"0"})),
            ("account.getReactionsNotifySettings", json!({})),
            ("messages.getAllDrafts", json!({})),
            ("messages.getDialogFilters", json!({})),
            ("messages.getSavedGifs", json!({"hash":"0"})),
            ("messages.getFavedStickers", json!({"hash":"0"})),
            ("messages.getRecentStickers", json!({"hash":"0"})),
            ("messages.getEmojiStickers", json!({"hash":"0"})),
            ("messages.getMaskStickers", json!({"hash":"0"})),
            ("messages.getAllStickers", json!({"hash":"0"})),
            ("messages.getDefaultHistoryTTL", json!({})),
            ("messages.getQuickReplies", json!({"hash":"0"})),
        ];
        for (name, args) in singleton {
            self.check_stop()?;
            if let Some(v) = self.collect(name, args, name).await? {
                if name == "messages.getDialogFilters" {
                    let a = self.archive.lock().unwrap();
                    a.db.execute("DELETE FROM folders", [])?;
                    for filter in v["filters"].as_array().into_iter().flatten() {
                        a.db.execute(
                            "INSERT OR REPLACE INTO folders VALUES(?1,?2)",
                            params![
                                integer(&filter["id"]).unwrap_or(0).to_string(),
                                filter.to_string()
                            ],
                        )?;
                    }
                }
                if let Some(sets) = v["sets"].as_array() {
                    for set in sets {
                        let set = set.get("set").unwrap_or(set);
                        if !set["access_hash"].is_null() {
                            self.collect("messages.getStickerSet",json!({"stickerset":{"_":"inputStickerSetID","id":set["id"],"access_hash":set["access_hash"]},"hash":0}),&format!("stickerset:{}",set["id"])).await?;
                        }
                    }
                }
            }
        }
        for key in [
            "StatusTimestamp",
            "ChatInvite",
            "PhoneCall",
            "PhoneP2P",
            "Forwards",
            "ProfilePhoto",
            "PhoneNumber",
            "AddedByPhone",
            "VoiceMessages",
            "About",
            "Birthday",
            "SavedMusic",
        ] {
            self.collect(
                "account.getPrivacy",
                json!({"key":{"_":format!("inputPrivacyKey{key}")}}),
                &format!("privacy:{key}"),
            )
            .await?;
        }
        self.paged(
            "contacts.getBlocked",
            json!({"offset":0,"limit":100}),
            "blocked",
            "blocked",
            "offset",
            None,
        )
        .await?;
        self.paged("contacts.getTopPeers",json!({"correspondents":true,"bots_pm":true,"bots_inline":true,"phone_calls":true,"forward_users":true,"forward_chats":true,"groups":true,"channels":true,"bots_app":true,"bots_guestchat":true,"offset":0,"limit":100,"hash":"0"}),"top_peers","categories","offset",None).await?;
        self.paged(
            "photos.getUserPhotos",
            json!({"user_id":{"_":"inputUserSelf"},"offset":0,"max_id":"0","limit":100}),
            "self_photos",
            "photos",
            "max_id",
            Some("id"),
        )
        .await?;
        self.paged(
            "stories.getStoriesArchive",
            json!({"peer":{"_":"inputPeerSelf"},"offset_id":0,"limit":100}),
            "own_stories",
            "stories",
            "offset_id",
            Some("id"),
        )
        .await?;
        self.paged(
            "users.getSavedMusic",
            json!({"id":{"_":"inputUserSelf"},"offset":0,"limit":100,"hash":"0"}),
            "profile_music",
            "documents",
            "offset",
            None,
        )
        .await?;
        if self.takeout.is_some() {
            self.extra_saved_contacts().await?;
            self.paged(
                "channels.getLeftChannels",
                json!({"offset":0}),
                "left_channels",
                "chats",
                "offset",
                None,
            )
            .await?;
        }
        for hidden in [false, true] {
            let mut args = json!({});
            if hidden {
                args["hidden"] = json!(true);
            }
            let mut seen = HashSet::new();
            loop {
                let Some(v) = self
                    .collect(
                        "stories.getAllStories",
                        args.clone(),
                        if hidden { "hidden_stories" } else { "stories" },
                    )
                    .await?
                else {
                    break;
                };
                if v["has_more"] != true {
                    break;
                }
                let state = v["state"].clone();
                ensure!(
                    seen.insert(state.to_string()),
                    "stories cursor did not advance"
                );
                args["state"] = state;
                args["next"] = json!(true);
            }
        }
        Ok(())
    }
    async fn paged(
        &self,
        method: &str,
        mut args: Value,
        scope: &str,
        field: &str,
        offset: &str,
        id_field: Option<&str>,
    ) -> Result<()> {
        let key = format!("collector:{}:{scope}", self.job);
        if let Some(saved) = self.archive.lock().unwrap().checkpoint(&key)? {
            if saved["complete"] != true {
                args = saved["args"].clone();
            } else if !self.options.continuous {
                return Ok(());
            }
        }
        let mut previous = Value::Null;
        loop {
            self.check_stop()?;
            let response = self.rpc(method, args.clone(), None, None).await;
            let (root, bytes, v) = match response {
                Ok(r) => r,
                Err(e) => {
                    self.check_stop()?;
                    self.archive.lock().unwrap().coverage(
                        scope,
                        "failed_or_inaccessible",
                        &json!({"error":e.to_string(),"cursor":args}),
                    )?;
                    return Ok(());
                }
            };
            let items = v[field].as_array().cloned().unwrap_or_default();
            let count = if field == "categories" {
                items
                    .iter()
                    .map(|c| c["peers"].as_array().map(Vec::len).unwrap_or(0))
                    .max()
                    .unwrap_or(0)
            } else {
                items.len()
            };
            let mut next = args.clone();
            if let Some(id) = id_field {
                if let Some(last) = items.last() {
                    next[offset] = last[id].clone();
                }
            } else {
                next[offset] = json!(integer(&args[offset]).unwrap_or(0) + count as i64);
            }
            let done = count == 0;
            let progress = json!({"args":next,"complete":done});
            self.capture(
                &root,
                &bytes,
                method,
                Some(&extra::page_scope(scope, &args)),
                None,
                Some((&key, &progress)),
            )?;
            self.archive.lock().unwrap().coverage(
                scope,
                if done { "complete" } else { "in_progress" },
                &progress,
            )?;
            if done {
                break;
            }
            ensure!(
                next != previous && next != args,
                "collector cursor did not advance: {scope}"
            );
            previous = args.clone();
            args = next;
        }
        Ok(())
    }
    async fn peer_collectors(&self, key: &str, input: &Value, raw: &Value) -> Result<()> {
        let constructor = raw["_"].as_str().unwrap_or("");
        if key.starts_with("user:") {
            let user = if raw["self"] == true {
                json!({"_":"inputUserSelf"})
            } else {
                json!({"_":"inputUser","user_id":raw["id"],"access_hash":raw["access_hash"]})
            };
            self.collect(
                "users.getFullUser",
                json!({"id":user}),
                &format!("{key}/profile"),
            )
            .await?;
            self.paged(
                "photos.getUserPhotos",
                json!({"user_id":user,"offset":0,"max_id":"0","limit":100}),
                &format!("{key}/photos"),
                "photos",
                "max_id",
                Some("id"),
            )
            .await?;
        } else if constructor.starts_with("channel") {
            let channel =
                json!({"_":"inputChannel","channel_id":raw["id"],"access_hash":raw["access_hash"]});
            self.collect(
                "channels.getFullChannel",
                json!({"channel":channel}),
                &format!("{key}/profile"),
            )
            .await?;
            for filter in [
                "channelParticipantsRecent",
                "channelParticipantsAdmins",
                "channelParticipantsBots",
            ] {
                self.paged("channels.getParticipants",json!({"channel":channel,"filter":{"_":filter},"offset":0,"limit":200,"hash":"0"}),&format!("{key}/{filter}"),"participants","offset",None).await?;
            }
            self.paged(
                "channels.getAdminLog",
                json!({"channel":channel,"q":"","max_id":"0","min_id":"0","limit":100}),
                &format!("{key}/admin_log"),
                "events",
                "max_id",
                Some("id"),
            )
            .await?;
        } else {
            self.collect(
                "messages.getFullChat",
                json!({"chat_id":raw["id"]}),
                &format!("{key}/profile"),
            )
            .await?;
        }
        self.collect(
            "account.getNotifySettings",
            json!({"peer":{"_":"inputNotifyPeer","peer":input}}),
            &format!("{key}/notifications"),
        )
        .await?;
        self.collect(
            "messages.getPeerSettings",
            json!({"peer":input}),
            &format!("{key}/settings"),
        )
        .await?;
        self.collect(
            "stories.getPeerStories",
            json!({"peer":input}),
            &format!("{key}/stories"),
        )
        .await?;
        self.paged(
            "stories.getPinnedStories",
            json!({"peer":input,"offset_id":0,"limit":100}),
            &format!("{key}/pinned_stories"),
            "stories",
            "offset_id",
            Some("id"),
        )
        .await?;
        if raw["forum"] == true {
            let mut args =
                json!({"peer":input,"offset_date":0,"offset_id":0,"offset_topic":0,"limit":100});
            let mut seen = HashSet::new();
            loop {
                let Some(v) = self
                    .collect(
                        "messages.getForumTopics",
                        args.clone(),
                        &format!("{key}/topics"),
                    )
                    .await?
                else {
                    break;
                };
                let Some(last) = v["topics"].as_array().and_then(|a| a.last()) else {
                    break;
                };
                ensure!(
                    seen.insert(last["id"].to_string()),
                    "topic cursor did not advance"
                );
                args["offset_topic"] = last["id"].clone();
                args["offset_id"] = last["top_message"].clone();
                args["offset_date"] = last["date"].clone();
            }
        }
        Ok(())
    }
    fn reconcile_media(&self) -> Result<()> {
        let a = self.archive.lock().unwrap();
        let mut after = 0;
        loop {
            let ids:Vec<i64>=a.db.prepare("SELECT id FROM observations WHERE id>?1 AND kind='media' AND id NOT IN(SELECT observation FROM media_refs) ORDER BY id LIMIT 128")?.query_map([after],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                after = id;
                let record = a.record(id)?;
                discover_media(&a, &record.data, id)?;
            }
        }
        let mut after = 0;
        loop {
            let ids = a.observation_ids(after, 128)?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                after = id;
                let record = a.record(id)?;
                a.link_media(id, &record.data)?;
            }
        }
        Ok(())
    }
    async fn download_pending(&self) -> Result<()> {
        loop {
            self.check_stop()?;
            let used = self
                .archive
                .lock()
                .unwrap()
                .checkpoint(&format!("media_bytes:{}", self.job))?
                .and_then(|v| integer(&v))
                .unwrap_or(0) as u64;
            if self
                .options
                .max_media_bytes
                .is_some_and(|n| used.saturating_sub(self.base_media) >= n)
            {
                self.archive
                    .lock()
                    .unwrap()
                    .set_checkpoint(&format!("pause_reason:{}", self.job), &json!("media_limit"))?;
                break;
            }
            let row = {
                let a = self.archive.lock().unwrap();
                a.db.query_row("SELECT id,location,dc,size,offset FROM media WHERE status!='complete' AND status!='unavailable' AND retry_at<=?1 ORDER BY attempts,id LIMIT 1",[Utc::now().timestamp()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i32>(2)?,r.get::<_,Option<u64>>(3)?,r.get::<_,u64>(4)?))).optional()?
            };
            let Some((id, location, dc, size, offset)) = row else {
                break;
            };
            let location: Value = serde_json::from_str(&location)?;
            let max_file = self.archive.lock().unwrap().config.max_file_bytes;
            if size.is_some_and(|s| s > max_file) {
                self.archive.lock().unwrap().media_error(
                    &id,
                    "configured file size limit",
                    Utc::now().timestamp() + 3600,
                )?;
                self.archive
                    .lock()
                    .unwrap()
                    .db
                    .execute("UPDATE media SET status='deferred' WHERE id=?1", [&id])?;
                continue;
            }
            let metadata = {
                let a = self.archive.lock().unwrap();
                let mut st=a.db.prepare("SELECT o.metadata FROM observations o JOIN media_refs r ON r.observation=o.id WHERE r.media=?1")?;
                st.query_map([&id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            let selector =
                Selector::parse(&self.archive.lock().unwrap().config.attachment_selector)?;
            if !metadata.iter().any(|m| {
                serde_json::from_str::<Value>(m).is_ok_and(|mut m| {
                    if let Some(peer) = m["peer"].as_str()
                        && let Ok(Some(current)) =
                            peer_metadata(&self.archive.lock().unwrap(), peer)
                    {
                        merge(&mut m, &current);
                    }
                    selector.matches(&m)
                })
            }) {
                self.archive.lock().unwrap().media_error(
                    &id,
                    "attachment selector excludes source",
                    Utc::now().timestamp() + 3600,
                )?;
                self.archive
                    .lock()
                    .unwrap()
                    .db
                    .execute("UPDATE media SET status='deferred' WHERE id=?1", [&id])?;
                continue;
            }
            let stage = {
                let a = self.archive.lock().unwrap();
                crate::media::stage_path(&a.root, &id)
            };
            let offset = if stage.exists() {
                std::fs::metadata(&stage)?.len().min(offset)
            } else {
                0
            };
            if size == Some(offset) && stage.exists() {
                let a = self.archive.lock().unwrap();
                if let Err(e) = a.finish_media(&id) {
                    a.media_error(&id, &e.to_string(), Utc::now().timestamp() + 300)?;
                }
                continue;
            }
            let limit = 512 * 1024u64;
            if self.options.max_media_bytes.is_some_and(|n| {
                n.saturating_sub(used.saturating_sub(self.base_media))
                    < size
                        .map(|s| s.saturating_sub(offset).min(limit))
                        .unwrap_or(limit)
            }) {
                self.archive
                    .lock()
                    .unwrap()
                    .set_checkpoint(&format!("pause_reason:{}", self.job), &json!("media_limit"))?;
                break;
            }
            let response = self
                .rpc(
                    "upload.getFile",
                    json!({"location":location,"offset":offset.to_string(),"limit":limit}),
                    None,
                    Some(dc),
                )
                .await;
            match response {
                Ok((_, _, v)) => {
                    let Some(data) = v["bytes"]["$bytes"].as_str() else {
                        self.archive.lock().unwrap().media_error(
                            &id,
                            "unexpected file response",
                            Utc::now().timestamp() + 3600,
                        )?;
                        continue;
                    };
                    let bytes = hex::decode(data)?;
                    let a = self.archive.lock().unwrap();
                    let next = a.append_media(&id, offset, &bytes)?;
                    a.set_checkpoint(
                        &format!("media_bytes:{}", self.job),
                        &json!(used + bytes.len() as u64),
                    )?;
                    if (size.is_some_and(|s| next == s) || bytes.len() < (limit as usize))
                        && let Err(e) = a.finish_media(&id)
                    {
                        a.media_error(&id, &e.to_string(), Utc::now().timestamp() + 300)?;
                    }
                }
                Err(e) => {
                    self.archive.lock().unwrap().media_error(
                        &id,
                        &e.to_string(),
                        Utc::now().timestamp() + 300,
                    )?;
                    if e.to_string().contains("FILE_REFERENCE") {
                        self.refresh_media_reference(&id).await?;
                    }
                }
            }
        }
        Ok(())
    }
    async fn refresh_media_reference(&self, id: &str) -> Result<()> {
        let contexts = {
            let a = self.archive.lock().unwrap();
            a.db.prepare("SELECT DISTINCT o.metadata FROM observations o JOIN media_refs r ON r.observation=o.id WHERE r.media=?1")?.query_map([id],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for ctx in contexts {
            let ctx: Value = serde_json::from_str(&ctx)?;
            if self.refresh_extra_media(&ctx).await? {
                self.archive.lock().unwrap().db.execute(
                    "UPDATE media SET retry_at=strftime('%s','now')+5 WHERE id=?1",
                    [id],
                )?;
                return Ok(());
            }
            let Some(peer) = ctx["peer"].as_str() else {
                continue;
            };
            let Some(message) = integer(&ctx["message_id"]) else {
                continue;
            };
            let input = self.input_peer(peer)?;
            let (method, args) = if ctx["message_kind"] == "scheduled_message" {
                (
                    "messages.getScheduledMessages",
                    json!({"peer":input,"id":[message]}),
                )
            } else if ctx["message_kind"] == "quick_reply_message" {
                (
                    "messages.getQuickReplyMessages",
                    json!({"shortcut_id":ctx["quick_reply_shortcut_id"],"id":[message],"hash":"0"}),
                )
            } else if peer.starts_with("channel:") {
                (
                    "channels.getMessages",
                    json!({"channel":{"_":"inputChannel","channel_id":input["channel_id"],"access_hash":input["access_hash"]},"id":[{"_":"inputMessageID","id":message}]}),
                )
            } else {
                (
                    "messages.getMessages",
                    json!({"id":[{"_":"inputMessageID","id":message}]}),
                )
            };
            let scope = if ctx["message_kind"] == "quick_reply_message" {
                format!(
                    "account/quick_reply:{}",
                    integer(&ctx["quick_reply_shortcut_id"])
                        .context("missing quick reply shortcut")?
                )
            } else {
                peer.to_string()
            };
            if self.collect(method, args, &scope).await?.is_some() {
                self.archive.lock().unwrap().db.execute(
                    "UPDATE media SET retry_at=strftime('%s','now')+5 WHERE id=?1",
                    [id],
                )?;
                return Ok(());
            }
        }
        Ok(())
    }
}
fn peer_selected(config: &crate::config::Config, metadata: &Value) -> Result<bool> {
    Ok(
        Selector::parse(&config.history_selector)?.may_match(metadata)
            || Selector::parse(&config.attachment_selector)?.may_match(metadata),
    )
}
fn message_context(metadata: &Value, message: &Value) -> Value {
    let mut context = metadata.clone();
    context["message_id"] = message["id"].clone();
    context["date"] = message["date"].clone();
    context["outgoing"] = json!(message["out"] == true);
    context["sender"] = peer_key(&message["from_id"]).map_or(Value::Null, Value::String);
    context["topic"] = message["reply_to"]["reply_to_top_id"].clone();
    context["data"] = message.clone();
    if let Some(peer) = peer_key(&message["peer_id"]) {
        context["peer"] = json!(peer);
    }
    context
}
fn merge(dst: &mut Value, src: &Value) {
    if let (Some(dst), Some(src)) = (dst.as_object_mut(), src.as_object()) {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
}
pub fn peer_key(v: &Value) -> Option<String> {
    let kind = v["_"].as_str()?;
    let (prefix, field) = match kind {
        "peerUser" | "inputPeerUser" => ("user", "user_id"),
        "peerChat" | "inputPeerChat" => ("chat", "chat_id"),
        "peerChannel" | "inputPeerChannel" => ("channel", "channel_id"),
        _ => return None,
    };
    Some(format!("{prefix}:{}", integer(&v[field])?))
}
fn peer_metadata(a: &Archive, key: &str) -> Result<Option<Value>> {
    a.db.query_row("SELECT metadata FROM peers WHERE key=?1", [key], |r| {
        r.get::<_, String>(0)
    })
    .optional()?
    .map(|s| Ok(serde_json::from_str(&s)?))
    .transpose()
}
fn cache_peer(a: &Archive, v: &Value, self_id: i64) -> Result<()> {
    let Some(id) = integer(&v["id"]) else {
        return Ok(());
    };
    let constructor = v["_"].as_str().unwrap_or("");
    let (key, input, category) = if constructor == "user" {
        if id != self_id && v["access_hash"].is_null() {
            return Ok(());
        }
        (
            format!("user:{id}"),
            if id == self_id {
                json!({"_":"inputPeerSelf"})
            } else {
                json!({"_":"inputPeerUser","user_id":id.to_string(),"access_hash":v["access_hash"]})
            },
            if id == self_id {
                "saved"
            } else if v["bot"] == true {
                "bot"
            } else {
                "personal"
            },
        )
    } else if constructor.starts_with("channel") {
        if v["access_hash"].is_null() {
            return Ok(());
        }
        (
            format!("channel:{id}"),
            json!({"_":"inputPeerChannel","channel_id":id.to_string(),"access_hash":v["access_hash"]}),
            if v["broadcast"] == true {
                "channel"
            } else {
                "group"
            },
        )
    } else if constructor == "chat" {
        (
            format!("chat:{id}"),
            json!({"_":"inputPeerChat","chat_id":id.to_string()}),
            "group",
        )
    } else {
        return Ok(());
    };
    let mut metadata = peer_metadata(a, &key)?.unwrap_or(json!({}));
    merge(
        &mut metadata,
        &json!({"id":key,"peer":key,"category":category,"contact":v["contact"]==true,"title":v.get("title").or_else(||v.get("first_name"))}),
    );
    if v["min"] == true && peer_metadata(a, &key)?.is_some() {
        return Ok(());
    }
    a.db.execute("INSERT INTO peers VALUES(?1,?2,?3,?4) ON CONFLICT(key) DO UPDATE SET input=excluded.input,metadata=excluded.metadata,raw=excluded.raw",params![key,input.to_string(),metadata.to_string(),v.to_string()])?;
    Ok(())
}
fn identity(root: &str, v: &Value, scope: &str) -> Option<(String, String)> {
    let name = v["_"].as_str()?;
    if root == "Message"
        && let Some(identity) = extra::message_identity(v, scope)
    {
        return Some(identity);
    }
    if let Some(identity) = extra::object_identity(root, v, scope) {
        return Some(identity);
    }
    let scope = scope.split("/page:").next().unwrap_or(scope);
    let id = integer(&v["id"]);
    Some(match root {
        "User" => (format!("user:{}", id?), "user".into()),
        "Chat" => (
            format!(
                "{}:{}",
                if name.starts_with("channel") {
                    "channel"
                } else {
                    "chat"
                },
                id?
            ),
            "chat".into(),
        ),
        "Message" => (
            format!(
                "{}/message:{}",
                peer_key(&v["peer_id"]).unwrap_or_else(|| scope.into()),
                id?
            ),
            "message".into(),
        ),
        "Dialog" => (format!("{}/dialog", peer_key(&v["peer"])?), "dialog".into()),
        "Photo" | "Document" => (format!("{root}:{}", id?), "media".into()),
        "UserStatus" => (format!("{scope}/presence"), "presence".into()),
        "DialogFilter" => (format!("folder:{}", id?), "folder".into()),
        "ForumTopic" => (format!("{scope}/topic:{}", id?), "topic".into()),
        "StoryItem" => (format!("{scope}/story:{}", id?), "story".into()),
        "Update" if name.contains("Read") => (
            format!(
                "{}/{name}",
                peer_key(&v["peer"])
                    .or_else(|| integer(&v["channel_id"]).map(|id| format!("channel:{id}")))
                    .or_else(|| integer(&v["user_id"]).map(|id| format!("user:{id}")))
                    .unwrap_or_else(|| scope.into())
            ),
            "read_state".into(),
        ),
        "Update" if name == "updateUserStatus" => (
            format!("user:{}/presence", integer(&v["user_id"])?),
            "presence".into(),
        ),
        _ => {
            if matches!(root, "Peer" | "InputPeer" | "Bool") {
                return None;
            }
            let id = id?;
            (format!("{scope}/{root}:{id}"), root.into())
        }
    })
}
pub fn folder_matches(filter: &Value, key: &str, meta: &Value) -> bool {
    let contains = |field: &str| {
        filter[field]
            .as_array()
            .is_some_and(|a| a.iter().any(|p| peer_key(p).as_deref() == Some(key)))
    };
    if contains("exclude_peers") {
        return false;
    }
    if contains("include_peers") || contains("pinned_peers") {
        return true;
    }
    if filter["exclude_archived"] == true && meta["archived"] == true {
        return false;
    }
    if filter["exclude_muted"] == true && meta["muted"] == true {
        return false;
    }
    if filter["exclude_read"] == true && meta["unread"] != true {
        return false;
    }
    match meta["category"].as_str() {
        Some("personal") => {
            if meta["contact"] == true {
                filter["contacts"] == true
            } else {
                filter["non_contacts"] == true
            }
        }
        Some("bot") => filter["bots"] == true,
        Some("group") => filter["groups"] == true,
        Some("channel") => filter["broadcasts"] == true,
        _ => false,
    }
}
fn deletion_records(
    a: &Archive,
    v: &Value,
    bytes: &[u8],
    at: i64,
    replay: Option<&str>,
    records: &mut Vec<Capture>,
) -> Result<()> {
    let constructor = v["_"].as_str().unwrap_or("");
    if constructor == "updateDeleteQuickReply" {
        let shortcut = integer(&v["shortcut_id"]).context("deleted shortcut ID")?;
        let prefix = format!("account/quick_reply:{shortcut}/message:");
        let mut targets: Vec<(String, String)> = a.db.prepare("SELECT h.key,o.kind FROM heads h JOIN observations o ON o.id=h.observation WHERE o.kind='quick_reply_message' AND substr(h.key,1,length(?1))=?1")?
            .query_map([prefix], |r| Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        targets.push(
            extra::object_identity(
                "QuickReply",
                &json!({"_":"quickReply","shortcut_id":shortcut}),
                "account",
            )
            .context("shortcut identity")?,
        );
        for (key, kind) in targets {
            records.push(Capture {
                key: key.clone(),
                kind,
                root_type: "Update".into(),
                bytes: bytes.to_vec(),
                observed_at: at,
                source: "authoritative_deletion".into(),
                metadata: json!({"quick_reply_shortcut_id":shortcut,"revision":at}),
                replay_key: replay.map(|r| format!("{r}:delete:{key}")),
                partial: false,
                deleted: true,
            });
        }
        return Ok(());
    }
    if !matches!(
        constructor,
        "updateDeleteMessages"
            | "updateDeleteChannelMessages"
            | "updateDeleteScheduledMessages"
            | "updateDeleteQuickReplyMessages"
    ) {
        return Ok(());
    }
    for id in v["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(integer)
    {
        let key = if constructor == "updateDeleteScheduledMessages" {
            peer_key(&v["peer"]).map(|peer| format!("{peer}/scheduled_message:{id}"))
        } else if constructor == "updateDeleteQuickReplyMessages" {
            integer(&v["shortcut_id"])
                .map(|shortcut| format!("account/quick_reply:{shortcut}/message:{id}"))
        } else if let Some(channel) = integer(&v["channel_id"]) {
            Some(format!("channel:{channel}/message:{id}"))
        } else {
            let keys: Vec<String> = a
                .db
                .prepare("SELECT h.key FROM heads h JOIN observations o ON o.id=h.observation WHERE o.kind='message' AND h.key LIKE ?1 AND h.key NOT LIKE 'channel:%'")?
                .query_map([format!("%/message:{id}")], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            if keys.len() == 1 {
                Some(keys[0].clone())
            } else {
                None
            }
        };
        records.push(Capture {
            key: key.clone().unwrap_or_else(|| {
                format!("unresolved-delete:{id}:{}", blake3::hash(bytes).to_hex())
            }),
            kind: if constructor == "updateDeleteScheduledMessages" && key.is_some() {
                "scheduled_message"
            } else if constructor == "updateDeleteQuickReplyMessages" && key.is_some() {
                "quick_reply_message"
            } else if key.is_some() {
                "message"
            } else {
                "unresolved_deletion"
            }
            .into(),
            root_type: "Update".into(),
            bytes: bytes.to_vec(),
            observed_at: at,
            source: "authoritative_deletion".into(),
            metadata: json!({"id":id.to_string(),"revision":at}),
            replay_key: replay.map(|r| format!("{r}:delete:{id}")),
            partial: false,
            deleted: true,
        });
    }
    Ok(())
}
fn discover_media(a: &Archive, v: &Value, observation: i64) -> Result<()> {
    let Some(id) = integer(&v["id"]) else {
        return Ok(());
    };
    let dc = integer(&v["dc_id"]).unwrap_or(0) as i32;
    if v["_"] == "document" {
        a.queue_media(&format!("document:{id}"),&json!({"_":"inputDocumentFileLocation","id":v["id"],"access_hash":v["access_hash"],"file_reference":v["file_reference"],"thumb_size":""}),dc,integer(&v["size"]).map(|s|s as u64),observation)?;
    } else {
        let best = v["sizes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| s["_"] == "photoSize" || s["_"] == "photoSizeProgressive")
            .max_by_key(|s| integer(&s["w"]).unwrap_or(0) * integer(&s["h"]).unwrap_or(0));
        if let Some(size) = best {
            let size_bytes = integer(&size["size"])
                .or_else(|| size["sizes"].as_array()?.last().and_then(integer));
            let kind = size["type"].as_str().context("photo size type")?;
            a.queue_media(&format!("photo:{id}:{kind}"),&json!({"_":"inputPhotoFileLocation","id":v["id"],"access_hash":v["access_hash"],"file_reference":v["file_reference"],"thumb_size":kind}),dc,size_bytes.map(|s|s as u64),observation)?;
        }
        for video in v["video_sizes"].as_array().into_iter().flatten() {
            if video["_"] == "videoSize" {
                let kind = video["type"].as_str().context("video size type")?;
                a.queue_media(&format!("photo:{id}:{kind}"),&json!({"_":"inputPhotoFileLocation","id":v["id"],"access_hash":v["access_hash"],"file_reference":v["file_reference"],"thumb_size":kind}),dc,integer(&video["size"]).map(|s|s as u64),observation)?;
            }
        }
    }
    Ok(())
}

pub async fn abort_takeout(root: &Path, job: &str) -> Result<()> {
    let a = Archive::open(root, true)?;
    let saved = a
        .checkpoint(&format!("takeout:{job}"))?
        .context("no takeout session for job")?;
    let id = integer(&saved["id"]).context("takeout session already finished")?;
    let (api_id, _) = credentials(root).await?;
    let session = Arc::new(SqliteSession::open(root.join("session.sqlite3")).await?);
    let SenderPool { runner, handle, .. } = SenderPool::new(session, api_id);
    let _network_guard = NetworkGuard(handle.clone());
    let client = Client::new(handle.clone());
    let task = tokio::spawn(runner.run());
    let schema = Schema::current()?;
    let (bytes, _) = schema.request("account.finishTakeoutSession", json!({}))?;
    let request = grammers_tl_types::functions::InvokeWithTakeout {
        takeout_id: id,
        query: RawRequest(bytes),
    };
    let result = client.invoke(&request).await;
    handle.quit();
    let _ = task.await;
    result?;
    a.set_checkpoint(&format!("takeout:{job}"), &json!({"aborted":true}))?;
    Ok(())
}

fn takeout_method(name: &str) -> bool {
    matches!(
        name,
        "account.finishTakeoutSession"
            | "messages.getSplitRanges"
            | "messages.getDialogs"
            | "messages.getHistory"
            | "messages.getMessages"
            | "messages.search"
            | "channels.getMessages"
            | "channels.getLeftChannels"
            | "contacts.getSaved"
            | "contacts.getTopPeers"
            | "users.getUsers"
            | "users.getFullUser"
            | "users.getSavedMusic"
            | "photos.getUserPhotos"
            | "stories.getStoriesArchive"
            | "account.getAuthorizations"
            | "account.getWebAuthorizations"
            | "messages.getReplies"
            | "messages.getCustomEmojiDocuments"
            | "upload.getFile"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, query::Query};

    #[test]
    fn sync_job_selection_resumes_only_unfinished_jobs_and_honors_new_job() {
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::init(dir.path(), &Config::default()).unwrap();
        let first = prepare_sync_job(&mut archive, &mut SyncOptions::default()).unwrap();
        for status in ["running", "paused", "failed"] {
            archive
                .db
                .execute("UPDATE jobs SET status=?1", [status])
                .unwrap();
            let mut options = SyncOptions::default();
            assert_eq!(prepare_sync_job(&mut archive, &mut options).unwrap(), first);
            assert_eq!(options.resume.as_deref(), Some(first.as_str()));
        }
        let second = prepare_sync_job(
            &mut archive,
            &mut SyncOptions {
                new_job: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_ne!(first, second);
        // Force a tie in timestamps: the most recently inserted job wins.
        archive
            .db
            .execute("UPDATE jobs SET updated=1,created=1", [])
            .unwrap();
        assert_eq!(
            prepare_sync_job(&mut archive, &mut SyncOptions::default()).unwrap(),
            second
        );
        for status in ["complete", "complete_with_gaps", "aborted"] {
            archive
                .db
                .execute("UPDATE jobs SET status=?1", [status])
                .unwrap();
            let mut options = SyncOptions::default();
            let fresh = prepare_sync_job(&mut archive, &mut options).unwrap();
            assert_ne!(fresh, first);
            assert_ne!(fresh, second);
            assert!(options.resume.is_none());
        }
    }

    #[test]
    fn sync_job_selection_checks_scope_mode_and_config_selectors() {
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::init(dir.path(), &Config::default()).unwrap();
        let first = prepare_sync_job(&mut archive, &mut SyncOptions::default()).unwrap();
        for mut options in [
            SyncOptions {
                continuous: true,
                ..Default::default()
            },
            SyncOptions {
                takeout: true,
                ..Default::default()
            },
            SyncOptions {
                min_id: Some(1),
                ..Default::default()
            },
            SyncOptions {
                max_id: Some(100),
                ..Default::default()
            },
            SyncOptions {
                since: Some(1),
                ..Default::default()
            },
            SyncOptions {
                until: Some(100),
                ..Default::default()
            },
            SyncOptions {
                history_selector: Some("false".into()),
                ..Default::default()
            },
            SyncOptions {
                attachment_selector: Some("false".into()),
                ..Default::default()
            },
        ] {
            archive.config = Config::default();
            let fresh = prepare_sync_job(&mut archive, &mut options).unwrap();
            assert_ne!(fresh, first);
            assert!(options.resume.is_none());
            assert_eq!(prepare_sync_job(&mut archive, &mut options).unwrap(), fresh);
        }
        archive.config = Config::default();
        // More recent incompatible jobs must not hide an older matching job.
        assert_eq!(
            prepare_sync_job(&mut archive, &mut SyncOptions::default()).unwrap(),
            first
        );
        archive.config.history_selector = "false".into();
        let mut options = SyncOptions::default();
        assert_ne!(prepare_sync_job(&mut archive, &mut options).unwrap(), first);
    }

    #[test]
    fn sync_job_resume_uses_current_limits_and_preserves_saved_scope() {
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::init(dir.path(), &Config::default()).unwrap();
        let first = prepare_sync_job(
            &mut archive,
            &mut SyncOptions {
                max_messages: Some(1),
                max_media_bytes: Some(2),
                max_seconds: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
        archive
            .set_checkpoint(&format!("messages:{first}"), &json!(123))
            .unwrap();
        let mut options = SyncOptions {
            max_messages: Some(4),
            max_seconds: Some(5),
            ..Default::default()
        };
        assert_eq!(prepare_sync_job(&mut archive, &mut options).unwrap(), first);
        assert_eq!(
            (
                options.max_messages,
                options.max_media_bytes,
                options.max_seconds
            ),
            (Some(4), None, Some(5))
        );
        let saved: String = archive
            .db
            .query_row("SELECT config FROM jobs WHERE id=?1", [&first], |r| {
                r.get(0)
            })
            .unwrap();
        let saved: SyncOptions = serde_json::from_str(&saved).unwrap();
        assert_eq!(saved.max_messages, Some(4));
        archive.config.history_selector = "false".into();
        let mut explicit = SyncOptions {
            resume: Some(first.clone()),
            ..Default::default()
        };
        assert_eq!(
            prepare_sync_job(&mut archive, &mut explicit).unwrap(),
            first
        );
        assert_eq!(explicit.history_selector, saved.history_selector);
        assert_eq!(
            (
                explicit.max_messages,
                explicit.max_media_bytes,
                explicit.max_seconds
            ),
            (None, None, None)
        );
        assert_eq!(
            archive.checkpoint(&format!("messages:{first}")).unwrap(),
            Some(json!(123))
        );
        let mut legacy = serde_json::to_value(saved).unwrap();
        legacy.as_object_mut().unwrap().remove("new_job");
        assert!(
            !serde_json::from_value::<SyncOptions>(legacy)
                .unwrap()
                .new_job
        );
        assert!(
            prepare_sync_job(
                &mut archive,
                &mut SyncOptions {
                    resume: Some("missing".into()),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn sync_job_cli_rejects_new_job_with_explicit_resume() {
        use clap::Parser;
        #[derive(Parser)]
        struct Args {
            #[command(flatten)]
            options: SyncOptions,
        }
        assert!(
            Args::try_parse_from(["sync", "--new-job"])
                .unwrap()
                .options
                .new_job
        );
        assert!(Args::try_parse_from(["sync", "--new-job", "--resume", "id"]).is_err());
    }

    pub(super) async fn fixture(root: &Path, config: Config) -> Engine {
        let a = Archive::init(root, &config).unwrap();
        a.db.execute_batch("CREATE TABLE peers(key TEXT PRIMARY KEY,input TEXT,metadata TEXT,raw TEXT); CREATE TABLE folders(id TEXT PRIMARY KEY,data TEXT);").unwrap();
        a.set_checkpoint("account_id", &json!(1)).unwrap();
        cache_peer(
            &a,
            &json!({"_":"user","id":"42","access_hash":"123","first_name":"Test"}),
            1,
        )
        .unwrap();
        let schema_hash = a.register_schema(tl::LAYER, tl::API_SCHEMA).unwrap();
        let session = Arc::new(
            SqliteSession::open(root.join("test-session.sqlite3"))
                .await
                .unwrap(),
        );
        let SenderPool { handle, .. } = SenderPool::new(session.clone(), 1);
        Engine {
            dc_auth_lock: Arc::new(tokio::sync::Mutex::new(())),
            mock_rpc: None,
            client: Client::new(handle),
            session,
            archive: Arc::new(Mutex::new(a)),
            schema: Arc::new(Schema::current().unwrap()),
            schema_hash,
            job: "fixture".into(),
            options: SyncOptions::default(),
            takeout: None,
            started: Instant::now(),
            base_messages: 0,
            base_media: 0,
            stop: Default::default(),
        }
    }
    #[tokio::test]
    async fn progress_tracks_requests_and_resets_only_on_phase_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        e.mock_rpc = Some(Mutex::new(std::collections::VecDeque::from([
            MockReply {
                method: "account.getAccountTTL",
                args: json!({}),
                dc: None,
                result: Ok(json!({"_":"accountDaysTTL","days":365})),
            },
            MockReply {
                method: "account.getAccountTTL",
                args: json!({}),
                dc: None,
                result: Err("collector unavailable".into()),
            },
        ])));
        e.archive
            .lock()
            .unwrap()
            .db
            .execute(
                "INSERT INTO jobs VALUES('fixture','{}','running',0,0,'{}')",
                [],
            )
            .unwrap();
        let details = || -> Value {
            let a = e.archive.lock().unwrap();
            let text: String =
                a.db.query_row("SELECT details FROM jobs WHERE id='fixture'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            serde_json::from_str(&text).unwrap()
        };
        e.progress("account_collectors", 0, None).unwrap();
        e.rpc_progress(Some("contacts.getContacts"), false, false)
            .unwrap();
        assert_eq!(details()["active_method"], "contacts.getContacts");
        assert_eq!(details()["requests_finished"], 0);
        e.rpc("account.getAccountTTL", json!({}), None, None)
            .await
            .unwrap();
        assert!(
            e.rpc("account.getAccountTTL", json!({}), None, None)
                .await
                .is_err()
        );
        e.progress("account_collectors", 0, None).unwrap();
        assert_eq!(details()["requests_finished"], 2);
        assert_eq!(details()["requests_failed"], 1);
        assert!(details()["active_method"].is_null());
        e.archive
            .lock()
            .unwrap()
            .set_checkpoint("messages:fixture", &json!(42))
            .unwrap();
        e.progress("history", 1, Some(2)).unwrap();
        assert_eq!(details()["requests_finished"], 0);
        assert_eq!(details()["requests_failed"], 0);
        assert_eq!(details()["messages_scanned"], 42);
        assert_eq!(details()["peers_visited"], 1);
        assert_eq!(details()["peers_discovered"], 2);
    }

    pub(super) fn message() -> Value {
        json!({"_":"message","id":17,"peer_id":{"_":"peerUser","user_id":"42"},"date":1700000000,"message":"private text","media":{"_":"messageMediaPhoto","photo":{"_":"photo","id":"123","access_hash":"456","file_reference":{"$bytes":"abcd"},"date":1700000000,"sizes":[{"_":"photoSize","type":"x","w":100,"h":100,"size":4}],"dc_id":2}}})
    }
    #[tokio::test]
    async fn own_or_personal_attachment_selector_inherits_message_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(
            dir.path(),
            Config {
                history_selector: "false".into(),
                attachment_selector: "mine or personal".into(),
                ..Default::default()
            },
        )
        .await;
        {
            let a = e.archive.lock().unwrap();
            cache_peer(&a, &json!({"_":"chat","id":"7","title":"Group"}), 1).unwrap();
        }
        for (id, outgoing, personal) in [(1, true, false), (2, false, true), (3, false, false)] {
            let mut m = message();
            m["id"] = json!(id);
            m["out"] = json!(outgoing);
            m["media"]["photo"]["id"] = json!((100 + id).to_string());
            if !personal {
                m["peer_id"] = json!({"_":"peerChat","chat_id":"7"});
            }
            let update = json!({"_":"updateNewMessage","message":m,"pts":id,"pts_count":1});
            e.capture(
                "Update",
                &e.schema.encode("Update", &update).unwrap(),
                "live",
                None,
                None,
                None,
            )
            .unwrap();
        }
        let a = e.archive.lock().unwrap();
        let records = a
            .query(&Query {
                selector: "mine or personal".into(),
                all_versions: true,
                ..Default::default()
            })
            .unwrap()
            .records;
        assert_eq!(records.len(), 2);
        assert!(records.iter().any(|r| r.metadata["outgoing"] == true));
        assert!(records.iter().any(|r| r.metadata["category"] == "personal"));
        assert_eq!(
            a.db.query_row("SELECT COUNT(*) FROM media", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
    #[tokio::test]
    async fn attachment_only_capture_keeps_media_without_message_text() {
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(
            dir.path(),
            Config {
                history_selector: "false".into(),
                attachment_selector: "personal".into(),
                ..Default::default()
            },
        )
        .await;
        assert!(
            peer_selected(
                &e.archive.lock().unwrap().config,
                &json!({"category":"personal"})
            )
            .unwrap()
        );
        let update = json!({"_":"updateNewMessage","message":message(),"pts":1,"pts_count":1});
        let bytes = e.schema.encode("Update", &update).unwrap();
        e.capture(
            "Update",
            &bytes,
            "live",
            None,
            Some("event:1"),
            Some(("telegram_updates", &json!({"pts":1}))),
        )
        .unwrap();
        let a = e.archive.lock().unwrap();
        let records = a
            .query(&Query {
                all_versions: true,
                ..Default::default()
            })
            .unwrap()
            .records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, "media");
        assert!(
            !serde_json::to_string(&records)
                .unwrap()
                .contains("private text")
        );
        assert_eq!(records[0].metadata["message_id"], 17);
        assert_eq!(a.checkpoint("telegram_updates").unwrap().unwrap()["pts"], 1);
        assert_eq!(
            a.db.query_row("SELECT COUNT(*) FROM media", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[tokio::test]
    async fn update_replay_backfill_overlap_and_authoritative_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(dir.path(), Config::default()).await;
        let update = json!({"_":"updateNewMessage","message":message(),"pts":1,"pts_count":1});
        let bytes = e.schema.encode("Update", &update).unwrap();
        e.capture("Update", &bytes, "live", None, Some("event:1"), None)
            .unwrap();
        e.capture("Update", &bytes, "live", None, Some("event:1"), None)
            .unwrap();
        e.capture(
            "Message",
            &e.schema.encode("Message", &message()).unwrap(),
            "history",
            Some("user:42"),
            Some("backfill:1"),
            None,
        )
        .unwrap();
        let q = Query {
            key: Some("user:42/message:17".into()),
            all_versions: true,
            ..Default::default()
        };
        assert_eq!(
            e.archive.lock().unwrap().query(&q).unwrap().records.len(),
            2
        );
        for (ids, replay) in [(vec![17], "delete:17"), (vec![99], "delete:99")] {
            let v = json!({"_":"updateDeleteMessages","messages":ids,"pts":2,"pts_count":1});
            e.capture(
                "Update",
                &e.schema.encode("Update", &v).unwrap(),
                "live",
                None,
                Some(replay),
                None,
            )
            .unwrap();
        }
        let a = e.archive.lock().unwrap();
        assert!(
            a.query(&Query {
                all_versions: false,
                ..q
            })
            .unwrap()
            .records[0]
                .deleted
        );
        assert_eq!(
            a.query(&Query {
                kind: Some("unresolved_deletion".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        a.verify().unwrap();
    }
    #[tokio::test]
    async fn invocation_limits_resume_from_saved_totals_and_completed_media_stage() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        e.options.max_messages = Some(2);
        e.base_messages = 10;
        e.archive
            .lock()
            .unwrap()
            .set_checkpoint("messages:fixture", &json!(11))
            .unwrap();
        assert!(!e.message_limit().unwrap());
        e.archive
            .lock()
            .unwrap()
            .set_checkpoint("messages:fixture", &json!(12))
            .unwrap();
        assert!(e.message_limit().unwrap());
        e.base_messages = 12;
        assert!(!e.message_limit().unwrap());
        e.capture(
            "Message",
            &e.schema.encode("Message", &message()).unwrap(),
            "history",
            Some("user:42"),
            None,
            None,
        )
        .unwrap();
        e.archive
            .lock()
            .unwrap()
            .append_media("photo:123:x", 0, b"test")
            .unwrap();
        // The complete stage must publish without contacting Telegram.
        e.download_pending().await.unwrap();
        let a = e.archive.lock().unwrap();
        assert_eq!(
            a.db.query_row("SELECT status FROM media", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "complete"
        );
        a.verify().unwrap();
        let dest = dir.path().join("exported");
        let mut output = Vec::new();
        crate::export::export(
            &a,
            &Query {
                kind: Some("message".into()),
                ..Default::default()
            },
            crate::export::Format::Json,
            &mut output,
            Some(&dest),
        )
        .unwrap();
        let exported: Value = serde_json::from_slice(&output).unwrap();
        let hash = blake3::hash(b"test").to_hex().to_string();
        assert_eq!(exported[0]["attachments"][0], hash);
        assert_eq!(std::fs::read(dest.join(hash)).unwrap(), b"test");
    }
    #[test]
    fn update_state_round_trip_preserves_channel_checkpoints() {
        let state = UpdatesState {
            pts: 10,
            qts: 2,
            seq: 3,
            date: 1700000000,
            channels: vec![ChannelState {
                id: 1234567890123,
                pts: 20,
            }],
        };
        assert_eq!(
            encode_state(&decode_state(&encode_state(&state)).unwrap()),
            encode_state(&state)
        );
        assert!(takeout_method("upload.getFile"));
        assert!(takeout_method("account.finishTakeoutSession"));
        assert!(!takeout_method("updates.getDifference"));
    }
}
