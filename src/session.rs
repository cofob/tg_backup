//! Grammers session storage using the archive's SQLite runtime. Secrets never enter the catalog.
use anyhow::Context;
use grammers_session::{
    BoxFuture, Session, SessionData,
    types::{ChannelState, DcOption, PeerId, PeerInfo, UpdateState, UpdatesState},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

#[derive(Debug)]
pub struct SessionError(anyhow::Error);
impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SessionError {}
impl From<anyhow::Error> for SessionError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}
impl From<rusqlite::Error> for SessionError {
    fn from(e: rusqlite::Error) -> Self {
        Self(e.into())
    }
}
impl From<serde_json::Error> for SessionError {
    fn from(e: serde_json::Error) -> Self {
        Self(e.into())
    }
}
impl From<std::io::Error> for SessionError {
    fn from(e: std::io::Error) -> Self {
        Self(e.into())
    }
}
type Result<T> = std::result::Result<T, SessionError>;
pub struct SqliteSession(Mutex<(Connection, SessionData)>);
impl SqliteSession {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Connection::open(path.as_ref())?;
        db.busy_timeout(std::time::Duration::from_secs(30))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS peers(id INTEGER PRIMARY KEY,is_self INTEGER NOT NULL,data TEXT NOT NULL);")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let mut data = SessionData::default();
        let mut st = db.prepare("SELECT key,value FROM settings")?;
        for row in st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (key, value) = row?;
            match key.as_str() {
                "home_dc" => data.home_dc = serde_json::from_str(&value)?,
                "updates" => data.updates_state = serde_json::from_str(&value)?,
                k if k.starts_with("dc:") => {
                    let dc: DcOption = serde_json::from_str(&value)?;
                    data.dc_options.insert(dc.id, dc);
                }
                _ => {}
            }
        }
        drop(st);
        Ok(Self(Mutex::new((db, data))))
    }
    fn lock(&self) -> Result<MutexGuard<'_, (Connection, SessionData)>> {
        self.0
            .lock()
            .map_err(|_| SessionError(anyhow::anyhow!("session lock poisoned")))
    }
}
fn save(db: &Connection, key: &str, value: &impl serde::Serialize) -> Result<()> {
    db.execute(
        "INSERT INTO settings VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, serde_json::to_string(value)?],
    )?;
    Ok(())
}
fn get_peer(db: &Connection, id: PeerId) -> Result<Option<PeerInfo>> {
    let raw: Option<String> = if let Some(id) = id.bot_api_dialog_id() {
        db.query_row("SELECT data FROM peers WHERE id=?1", [id], |r| r.get(0))
            .optional()?
    } else {
        db.query_row("SELECT data FROM peers WHERE is_self=1 LIMIT 1", [], |r| {
            r.get(0)
        })
        .optional()?
    };
    Ok(raw
        .map(|s| serde_json::from_str(&s).context("decode session peer"))
        .transpose()?)
}
impl Session for SqliteSession {
    type Error = SessionError;
    fn home_dc_id(&self) -> Result<i32> {
        Ok(self.lock()?.1.home_dc)
    }
    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock()?;
            save(&s.0, "home_dc", &dc_id)?;
            s.1.home_dc = dc_id;
            Ok(())
        })
    }
    fn dc_option(&self, id: i32) -> Result<Option<DcOption>> {
        Ok(self.lock()?.1.dc_options.get(&id).cloned())
    }
    fn set_dc_option(&self, dc: &DcOption) -> BoxFuture<'_, Result<()>> {
        let dc = dc.clone();
        Box::pin(async move {
            let mut s = self.lock()?;
            save(&s.0, &format!("dc:{}", dc.id), &dc)?;
            s.1.dc_options.insert(dc.id, dc);
            Ok(())
        })
    }
    fn peer(&self, id: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>>> {
        Box::pin(async move { get_peer(&self.lock()?.0, id) })
    }
    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<()>> {
        let mut peer = peer.clone();
        Box::pin(async move {
            let s = self.lock()?;
            if let Some(previous) = get_peer(&s.0, peer.id())? {
                peer.extend_info(&previous);
            }
            let is_self = matches!(
                peer,
                PeerInfo::User {
                    is_self: Some(true),
                    ..
                }
            );
            s.0.execute("INSERT INTO peers VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET is_self=excluded.is_self,data=excluded.data",params![peer.id().bot_api_dialog_id().context("peer ID")?,is_self,serde_json::to_string(&peer)?])?;
            Ok(())
        })
    }
    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState>> {
        Box::pin(async move { Ok(self.lock()?.1.updates_state.clone()) })
    }
    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut s = self.lock()?;
            let mut state = s.1.updates_state.clone();
            match update {
                UpdateState::All(v) => state = v,
                UpdateState::Primary { pts, date, seq } => {
                    state.pts = pts;
                    state.date = date;
                    state.seq = seq;
                }
                UpdateState::Secondary { qts } => state.qts = qts,
                UpdateState::Channel { id, pts } => {
                    state.channels.retain(|c| c.id != id);
                    state.channels.push(ChannelState { id, pts });
                }
            }
            save(&s.0, "updates", &state)?;
            s.1.updates_state = state;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_session::types::PeerAuth;
    #[tokio::test]
    async fn secrets_and_channel_state_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.sqlite3");
        let s = SqliteSession::open(&path).await.unwrap();
        s.set_home_dc_id(4).await.unwrap();
        let mut dc = s.dc_option(4).unwrap().unwrap();
        dc.auth_key = Some([42; 256]);
        s.set_dc_option(&dc).await.unwrap();
        let peer = PeerInfo::User {
            id: 17,
            auth: Some(PeerAuth::from_hash(123)),
            bot: Some(false),
            is_self: Some(true),
        };
        s.cache_peer(&peer).await.unwrap();
        s.cache_peer(&PeerInfo::User {
            id: 17,
            auth: None,
            bot: None,
            is_self: None,
        })
        .await
        .unwrap();
        s.set_update_state(UpdateState::Channel { id: 25, pts: 20 })
            .await
            .unwrap();
        drop(s);
        let s = SqliteSession::open(&path).await.unwrap();
        assert_eq!(s.home_dc_id().unwrap(), 4);
        assert_eq!(s.dc_option(4).unwrap().unwrap().auth_key, Some([42; 256]));
        assert_eq!(s.peer(PeerId::self_user()).await.unwrap(), Some(peer));
        assert_eq!(
            s.updates_state().await.unwrap().channels,
            vec![ChannelState { id: 25, pts: 20 }]
        );
    }
}
