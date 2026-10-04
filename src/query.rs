use crate::{archive::Archive, config::Backend, selector::Selector, storage::CATALOG};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub use tg_backup_protocol::{Page, Query, public_json};
#[derive(Serialize, Deserialize)]
struct Cursor {
    after: i64,
    snapshot: i64,
    query: String,
    dataset: String,
    generation: String,
    #[serde(default)]
    backend: Backend,
    #[serde(default)]
    commit: Option<i64>,
}
impl Archive {
    fn ch_query_ids(
        &self,
        q: &Query,
        after: i64,
        snapshot: i64,
        current: bool,
    ) -> Result<Vec<i64>> {
        #[derive(serde::Deserialize, clickhouse::Row)]
        struct Id {
            id: i64,
        }
        let db = self
            .store
            .clickhouse()
            .context("ClickHouse backend required")?;
        let mut filters = vec!["id<=?".to_string()];
        let mut values = vec![json!(snapshot)];
        for (field, value) in [
            ("observed", q.as_of.map(|v| json!(v))),
            ("key", q.key.as_ref().map(|v| json!(v))),
            ("kind", q.kind.as_ref().map(|v| json!(v))),
        ] {
            if let Some(value) = value {
                filters.push(format!(
                    "{field}{}?",
                    if field == "observed" { "<=" } else { "=" }
                ));
                values.push(value);
            }
        }
        let source = db.source("observations", CATALOG)?;
        let revision = crate::storage::REVISION;
        let mut sql = if q.all_versions || current {
            if current {
                filters.push(format!(
                    "id IN (SELECT observation FROM {})",
                    db.source("heads", CATALOG)?
                ));
            }
            format!("SELECT id FROM {source} WHERE {}", filters.join(" AND "))
        } else {
            format!(
                "SELECT id FROM (SELECT id,row_number() OVER (PARTITION BY key ORDER BY partial,{revision} DESC,id DESC) AS rank FROM {source} WHERE {}) WHERE rank=1",
                filters.join(" AND ")
            )
        };
        sql.push_str(" AND id>?");
        values.push(json!(after));
        if let Some(text) = &q.text {
            if current {
                sql.push_str(" AND deleted=0");
            }
            let phrase = text.starts_with('"') && text.ends_with('"');
            let text = if phrase {
                serde_json::from_str::<String>(text)?
            } else {
                text.clone()
            };
            sql.push_str(&format!(" AND id IN (SELECT id FROM {} WHERE {}(lowerUTF8(text),lowerUTF8(?), 'icu(\\'en\\')'))", db.source("text", CATALOG)?, if phrase { "hasPhrase" } else { "hasAllTokens" }));
            values.push(json!(text));
        }
        sql.push_str(&format!(" ORDER BY id LIMIT {}", q.scan_limit + 1));
        if !current && !self.config.index_history {
            sql.push_str(" SETTINGS use_skip_indexes=0");
        }
        Ok(db
            .query::<Id>(sql, values)?
            .into_iter()
            .map(|r| r.id)
            .collect())
    }
    pub fn query(&self, q: &Query) -> Result<Page> {
        ensure!((1..=1000).contains(&q.limit), "limit must be 1–1000");
        ensure!(
            (1..=100000).contains(&q.scan_limit),
            "scan_limit must be 1–100000"
        );
        let selector = Selector::parse(&q.selector)?;
        let regex = q
            .regex
            .as_ref()
            .map(|r| {
                regex::RegexBuilder::new(r)
                    .size_limit(8 * 1024 * 1024)
                    .build()
            })
            .transpose()?;
        let mut normalized = q.clone();
        normalized.cursor = None;
        let fingerprint = blake3::hash(&serde_json::to_vec(&normalized)?)
            .to_hex()
            .to_string();
        let dataset = self.dataset_id()?;
        let generation = self
            .checkpoint("maintenance_generation")?
            .unwrap_or(json!(0))
            .to_string();
        let snapshot = self.observation_high_water()?;
        let cursor = if let Some(c) = &q.cursor {
            serde_json::from_slice::<Cursor>(&URL_SAFE_NO_PAD.decode(c)?)?
        } else {
            Cursor {
                after: 0,
                snapshot,
                query: fingerprint.clone(),
                dataset: dataset.clone(),
                generation: generation.clone(),
                backend: self.config.backend,
                commit: self.store.clickhouse().map(|db| db.snapshot()),
            }
        };
        ensure!(
            cursor.query == fingerprint
                && cursor.dataset == dataset
                && cursor.generation == generation
                && cursor.backend == self.config.backend,
            "cursor does not match query/dataset or was invalidated by maintenance"
        );
        // A frozen high-water mark makes pagination consistent even while new observations arrive.
        let current = !q.all_versions && q.as_of.is_none() && q.cursor.is_none();
        let _snapshot = self
            .store
            .clickhouse()
            .map(|db| db.freeze(cursor.commit.context("ClickHouse cursor has no commit")?))
            .transpose()?;
        let indexed = q.text.is_some()
            && (self.store.clickhouse().is_some() || current || self.config.index_history);
        let index_filter = if indexed {
            if current {
                " AND id IN(SELECT rowid FROM search WHERE search MATCH ?7)"
            } else {
                " AND id IN(SELECT rowid FROM history_search WHERE history_search MATCH ?7)"
            }
        } else {
            " AND ?7 IS NULL"
        };
        let sql = if q.all_versions {
            format!(
                "SELECT id FROM observations WHERE id>?1 AND id<=?2 AND (?3 IS NULL OR observed<=?3) AND (?4 IS NULL OR key=?4) AND (?5 IS NULL OR kind=?5){index_filter} ORDER BY id LIMIT ?6"
            )
        } else if current {
            format!(
                "SELECT id FROM observations WHERE id IN(SELECT observation FROM heads) AND id>?1 AND id<=?2 AND (?3 IS NULL OR observed<=?3) AND (?4 IS NULL OR key=?4) AND (?5 IS NULL OR kind=?5){index_filter} ORDER BY id LIMIT ?6"
            )
        } else {
            format!(
                "WITH ranked AS (SELECT id,key,kind,ROW_NUMBER() OVER(PARTITION BY key ORDER BY partial ASC,COALESCE(json_extract(metadata,'$.revision'),observed) DESC,id DESC) AS rank FROM observations WHERE id<=?2 AND (?3 IS NULL OR observed<=?3) AND (?4 IS NULL OR key=?4) AND (?5 IS NULL OR kind=?5)) SELECT id FROM ranked WHERE rank=1 AND id>?1{index_filter} ORDER BY id LIMIT ?6"
            )
        };
        let ids: Vec<i64> = if self.store.clickhouse().is_some() {
            self.ch_query_ids(q, cursor.after, cursor.snapshot, current)?
        } else {
            self.store
                .sqlite()?
                .prepare(&sql)?
                .query_map(
                    params![
                        cursor.after,
                        cursor.snapshot,
                        q.as_of,
                        q.key,
                        q.kind,
                        q.scan_limit + 1,
                        if indexed { q.text.as_deref() } else { None }
                    ],
                    |r| r.get(0),
                )?
                .collect::<rusqlite::Result<_>>()?
        };
        let mut records = vec![];
        let mut scanned = 0;
        let mut after = cursor.after;
        let matcher = if q.text.is_some() && self.config.backend == Backend::Sqlite {
            let db = rusqlite::Connection::open_in_memory()?;
            db.execute_batch(
                "CREATE VIRTUAL TABLE text_match USING fts5(text,tokenize='unicode61');",
            )?;
            Some(db)
        } else {
            None
        };
        let started = std::time::Instant::now();
        let mut response_bytes = 0usize;
        for id in ids.iter().take(q.scan_limit) {
            if started.elapsed().as_secs() >= 10 {
                break;
            }
            let previous = after;
            if records.len() == q.limit {
                break;
            }
            scanned += 1;
            after = *id;
            let record = self.record(*id)?;
            if q.peer
                .as_ref()
                .is_some_and(|peer| !record.key.starts_with(&format!("{peer}/message:")))
                || q.topic
                    .as_ref()
                    .is_some_and(|topic| !crate::explorer::topic_matches(&record, topic))
            {
                continue;
            }
            let mut context = record.metadata.clone();
            if !context.is_object() {
                context = json!({});
            }
            context["key"] = json!(record.key);
            context["kind"] = json!(record.kind);
            context["observed_at"] = json!(record.observed_at);
            context["data"] = record.data.clone();
            if record.kind == "message" {
                context["outgoing"] = json!(record.data["out"] == true);
            }
            context["has_attachments"] = json!(!record.attachments.is_empty());
            if !selector.matches(&context) {
                continue;
            }
            let text = crate::tl::text(&record.data);
            if regex.as_ref().is_some_and(|r| !r.is_match(&text)) {
                continue;
            }
            if !indexed && let (Some(db), Some(pattern)) = (&matcher, &q.text) {
                db.execute("DELETE FROM text_match", [])?;
                db.execute("INSERT INTO text_match VALUES(?1)", [&text])?;
                let matched: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM text_match WHERE text_match MATCH ?1)",
                    [pattern],
                    |r| r.get(0),
                )?;
                if !matched {
                    continue;
                }
            }
            let bytes = serde_json::to_vec(&record)?.len();
            if !records.is_empty() && response_bytes + bytes > 8 * 1024 * 1024 {
                after = previous;
                break;
            }
            response_bytes += bytes;
            records.push(record);
        }
        let more = ids.iter().any(|id| *id > after);
        let next_cursor = if more {
            Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor { after, ..cursor })?))
        } else {
            None
        };
        Ok(Page {
            records,
            incomplete: more,
            scanned,
            next_cursor,
            snapshot: cursor.snapshot,
        })
    }
}
