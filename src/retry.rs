//! Explicit operator-triggered retry; no completed data is removed.
use crate::archive::Archive;
use anyhow::{Result, ensure};
use serde::Serialize;
use serde_json::{Value, json};
#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    #[default]
    All,
    Work,
    Attachments,
}
impl Archive {
    pub fn retry_failed(&mut self, target: Target, apply: bool) -> Result<Value> {
        ensure!(!apply || self.writable, "read-only archive");
        // A read-only Archive already holds a snapshot transaction.
        let counts = |db: &rusqlite::Connection| -> Result<(i64, i64)> {
            Ok((
                if matches!(target, Target::All | Target::Work) {
                    db.query_row("SELECT COUNT(*) FROM work WHERE state='failed'", [], |r| {
                        r.get(0)
                    })?
                } else {
                    0
                },
                if matches!(target, Target::All | Target::Attachments) {
                    db.query_row(
                        "SELECT COUNT(*) FROM media WHERE status='failed'",
                        [],
                        |r| r.get(0),
                    )?
                } else {
                    0
                },
            ))
        };
        let (found, changed) = if apply {
            let tx = self.db.transaction()?;
            let found = counts(&tx)?;
            let work = if matches!(target, Target::All | Target::Work) {
                tx.execute("UPDATE work SET state='queued',attempts=0,retry_at=0,error=NULL,progress='{}',updated=?1 WHERE state='failed'",[chrono::Utc::now().timestamp_micros()])?
            } else {
                0
            };
            let attachments = if matches!(target, Target::All | Target::Attachments) {
                tx.execute("UPDATE media SET status='pending',attempts=0,retry_at=0,error=NULL WHERE status='failed'",[])?
            } else {
                0
            };
            tx.commit()?;
            (found, (work, attachments))
        } else {
            (counts(&self.db)?, (0, 0))
        };
        Ok(
            json!({"target":target,"apply":apply,"found":{"work":found.0,"attachments":found.1},"changed":{"work":changed.0,"attachments":changed.1}}),
        )
    }
}
