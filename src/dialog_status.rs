//! Read-only projection of cached peers and per-peer getHistory coverage.
use crate::archive::Archive;
use anyhow::{Result, ensure};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub status: Option<String>,
    #[serde(rename = "type")]
    pub dialog_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DialogStatus {
    pub peer_id: String,
    pub display_name: Option<String>,
    pub dialog_type: String,
    pub history_backup_status: String,
    pub last_successful_sync_at: Option<i64>,
    pub errors: Vec<String>,
    pub coverage_gaps: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Page {
    pub items: Vec<DialogStatus>,
    pub next_cursor: Option<String>,
}

fn display_name(raw: &Value) -> Option<String> {
    if let Some(title) = raw["title"].as_str().filter(|s| !s.is_empty()) {
        return Some(title.to_owned());
    }
    let name = ["first_name", "last_name"]
        .iter()
        .filter_map(|field| raw[*field].as_str())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (!name.is_empty()).then_some(name)
}

fn project(
    key: String,
    raw: &str,
    metadata: &str,
    coverage_status: Option<String>,
    details: Option<String>,
    successful: Option<String>,
    access: Option<String>,
) -> Result<DialogStatus> {
    let raw: Value = serde_json::from_str(raw)?;
    let metadata: Value = serde_json::from_str(metadata)?;
    let details: Value = details
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or(Value::Null);
    let access: Option<Value> = access.map(|text| serde_json::from_str(&text)).transpose()?;
    let dialog_type = if key.starts_with("user:") {
        "user"
    } else if key.starts_with("chat:")
        || raw["megagroup"] == true
        || raw["gigagroup"] == true
        || metadata["category"] == "group"
    {
        "group"
    } else {
        "channel"
    };
    let mut errors = Vec::new();
    if let Some(error) = details["error"].as_str() {
        errors.push(error.to_owned());
    }
    let mut coverage_gaps = Vec::new();
    let history_backup_status = match coverage_status.as_deref() {
        Some("complete") if details["full_history"] == true && access.is_none() => "complete",
        Some("complete") if details["full_history"] == true => "limited",
        Some("limited") => {
            coverage_gaps.push(
                details["reason"]
                    .as_str()
                    .unwrap_or("History scan has known limits")
                    .to_owned(),
            );
            "limited"
        }
        Some("complete") => {
            coverage_gaps.push("Legacy completion does not verify full getHistory coverage".into());
            "incomplete"
        }
        Some("incomplete" | "in_progress") => {
            coverage_gaps.push(
                details["reason"]
                    .as_str()
                    .unwrap_or("getHistory scan has not completed")
                    .to_owned(),
            );
            "incomplete"
        }
        Some(other) => {
            coverage_gaps.push(format!("getHistory coverage: {other}"));
            "incomplete"
        }
        None => {
            coverage_gaps.push("No completed getHistory scan recorded".into());
            "not_started"
        }
    };
    if let Some(reason) = access.as_ref().and_then(|v| v["reason"].as_str()) {
        coverage_gaps.push(reason.to_owned());
    }
    let last_successful_sync_at = successful
        .as_deref()
        .and_then(|value| serde_json::from_str::<i64>(value).ok())
        .or_else(|| details["completed_at"].as_i64());
    Ok(DialogStatus {
        peer_id: key,
        display_name: display_name(&raw),
        dialog_type: dialog_type.into(),
        history_backup_status: history_backup_status.into(),
        last_successful_sync_at,
        errors,
        coverage_gaps,
    })
}

impl Archive {
    pub fn dialog_status_page(&self, options: &Options) -> Result<Page> {
        let limit = options.limit.unwrap_or(50);
        ensure!(
            (1..=200).contains(&limit),
            "limit must be between 1 and 200"
        );
        ensure!(
            options.status.as_deref().is_none_or(|s| [
                "complete",
                "incomplete",
                "limited",
                "not_started"
            ]
            .contains(&s)),
            "invalid status filter"
        );
        ensure!(
            options
                .dialog_type
                .as_deref()
                .is_none_or(|t| ["user", "group", "channel"].contains(&t)),
            "invalid type filter"
        );
        ensure!(
            options.after.as_ref().is_none_or(|s| s.len() <= 256),
            "cursor too long"
        );
        let has_peers: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='peers')",
            [],
            |row| row.get(0),
        )?;
        if !has_peers {
            return Ok(Page {
                items: vec![],
                next_cursor: None,
            });
        }
        let mut items = Vec::new();
        let mut after = options.after.clone().unwrap_or_default();
        loop {
            let mut statement = self.db.prepare(
                "SELECT p.key,p.raw,p.metadata,h.status,h.details,s.value,a.details FROM peers p
                 LEFT JOIN coverage h ON h.name='history:'||p.key
                 LEFT JOIN checkpoints s ON s.key='history_success:'||p.key
                 LEFT JOIN coverage a ON a.name='access:'||p.key AND a.status='limited'
                 WHERE p.key>?1
                   AND EXISTS (SELECT 1 FROM observations d
                               WHERE d.key=p.key||'/dialog' AND d.kind='dialog' AND d.deleted=0)
                 ORDER BY p.key LIMIT 256",
            )?;
            let rows = statement
                .query_map(params![after], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let count = rows.len();
            for (key, raw, metadata, status, details, successful, access) in rows {
                after = key.clone();
                let item = project(key, &raw, &metadata, status, details, successful, access)?;
                if options
                    .status
                    .as_ref()
                    .is_some_and(|s| s != &item.history_backup_status)
                    || options
                        .dialog_type
                        .as_ref()
                        .is_some_and(|t| t != &item.dialog_type)
                {
                    continue;
                }
                if items.len() == limit {
                    return Ok(Page {
                        next_cursor: items.last().map(|item: &DialogStatus| item.peer_id.clone()),
                        items,
                    });
                }
                items.push(item);
            }
            if count < 256 {
                return Ok(Page {
                    items,
                    next_cursor: None,
                });
            }
        }
    }
}
