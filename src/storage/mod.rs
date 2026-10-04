mod adapter;
mod archive;
mod catalog;
mod maintenance;
pub mod rows;

use anyhow::{Context, Result, ensure};
use rows::{CatalogRow, Change};
use rusqlite::{Connection, OptionalExtension, params_from_iter, types::Value as SqlValue};
use serde_json::{Value, json};

pub use adapter::{BatchPoint, ClickHouseStore};
pub const CATALOG: &str = "catalog";
pub(crate) const REVISION: &str =
    "coalesce(toInt64OrNull(replaceAll(JSONExtractRaw(metadata,'revision'),'\"','')),observed)";

pub enum Storage {
    Sqlite(Connection),
    ClickHouse(Box<ClickHouseStore>),
}

#[derive(Default)]
pub struct Select {
    pub filters: Vec<(&'static str, &'static str, Value)>,
    pub order: Option<&'static str>,
    pub limit: Option<usize>,
}
impl Select {
    pub fn eq(column: &'static str, value: impl Into<Value>) -> Self {
        Self {
            filters: vec![(column, "=", value.into())],
            ..Self::default()
        }
    }
    pub fn after(column: &'static str, after: i64, limit: usize) -> Self {
        Self {
            filters: vec![(column, ">", json!(after))],
            order: Some(column),
            limit: Some(limit),
        }
    }
    pub fn with_operator(mut self, operator: &'static str) -> Self {
        if let Some((_, op, _)) = self.filters.first_mut() {
            *op = operator;
        }
        self
    }
    pub fn sql<T: CatalogRow>(&self) -> Result<String> {
        let columns = T::columns();
        let valid = |column: &str| columns.iter().any(|(name, _)| *name == column);
        let mut sql = String::new();
        for (i, (column, operator, _)) in self.filters.iter().enumerate() {
            ensure!(
                valid(column) && ["=", "!=", ">", ">=", "<", "<=", "LIKE"].contains(operator),
                "invalid catalog filter"
            );
            sql.push_str(if i == 0 { " WHERE " } else { " AND " });
            sql.push_str(&format!("\"{column}\" {operator} ?"));
        }
        if let Some(column) = self.order {
            ensure!(valid(column), "invalid catalog order");
            sql.push_str(&format!(" ORDER BY \"{column}\""));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        Ok(sql)
    }
}

impl Storage {
    pub fn sqlite(&self) -> Result<&Connection> {
        match self {
            Self::Sqlite(db) => Ok(db),
            _ => anyhow::bail!("operation requires SQLite"),
        }
    }
    pub fn sqlite_mut(&mut self) -> Result<&mut Connection> {
        match self {
            Self::Sqlite(db) => Ok(db),
            _ => anyhow::bail!("operation requires SQLite"),
        }
    }
    pub fn clickhouse(&self) -> Option<&ClickHouseStore> {
        match self {
            Self::ClickHouse(db) => Some(db),
            _ => None,
        }
    }
    pub fn select<T: CatalogRow>(&self, query: &Select) -> Result<Vec<T>> {
        match self {
            Self::Sqlite(db) => read_rows(db, query),
            Self::ClickHouse(db) => db.select(CATALOG, query),
        }
    }
    pub fn all<T: CatalogRow>(&self) -> Result<Vec<T>> {
        self.select(&Select::default())
    }
    pub fn count<T: CatalogRow>(&self, query: &Select) -> Result<i64> {
        let filter = query.sql::<T>()?;
        let values = query
            .filters
            .iter()
            .map(|(_, _, v)| v.clone())
            .collect::<Vec<_>>();
        match self {
            Self::Sqlite(db) => Ok(db.query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"{filter}", T::SQLITE_TABLE),
                params_from_iter(values.iter().map(sql_value)),
                |r| r.get(0),
            )?),
            Self::ClickHouse(db) => db.number(
                format!(
                    "SELECT toInt64(count()) AS value FROM {}{filter}",
                    db.source(T::TABLE, CATALOG)?
                ),
                values,
            ),
        }
    }
    pub fn states<T: CatalogRow>(
        &self,
        field: &str,
    ) -> Result<std::collections::BTreeMap<String, i64>> {
        ensure!(
            T::columns().iter().any(|(name, _)| *name == field),
            "invalid state column"
        );
        #[derive(serde::Deserialize, ::clickhouse::Row)]
        struct State {
            state: String,
            count: i64,
        }
        match self {
            Self::Sqlite(db) => Ok(db.prepare(&format!("SELECT \"{field}\",COUNT(*) FROM \"{}\" GROUP BY \"{field}\"", T::SQLITE_TABLE))?.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?),
            Self::ClickHouse(db) => Ok(db.query::<State>(format!("SELECT \"{field}\" AS state,toInt64(count()) AS count FROM {} GROUP BY \"{field}\"", db.source(T::TABLE, CATALOG)?), vec![])?.into_iter().map(|r| (r.state,r.count)).collect()),
        }
    }
    pub fn get<T: CatalogRow>(
        &self,
        column: &'static str,
        value: impl Into<Value>,
    ) -> Result<Option<T>> {
        Ok(self.select(&Select::eq(column, value))?.into_iter().next())
    }
    pub fn put<T: CatalogRow>(&self, row: &T) -> Result<()> {
        match self {
            Self::Sqlite(db) => write_row(db, row, None),
            Self::ClickHouse(db) => db.commit(vec![Change::put(CATALOG, row)?]),
        }
    }
    pub fn delete<T: CatalogRow>(&self, row: &T) -> Result<()> {
        match self {
            Self::Sqlite(db) => {
                let value = serde_json::to_value(row)?;
                let filter = T::KEYS
                    .iter()
                    .map(|name| format!("\"{name}\"=?"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                db.execute(
                    &format!("DELETE FROM \"{}\" WHERE {filter}", T::SQLITE_TABLE),
                    params_from_iter(T::KEYS.iter().map(|k| sql_value(&value[*k]))),
                )?;
                Ok(())
            }
            Self::ClickHouse(db) => db.commit(vec![Change::delete(CATALOG, row)?]),
        }
    }
    pub fn next_id<T: CatalogRow>(&self, column: &'static str) -> Result<i64> {
        ensure!(
            T::columns().iter().any(|(name, _)| *name == column),
            "invalid ID column"
        );
        match self {
            Self::Sqlite(db) => {
                let next: i64 = db.query_row(
                    &format!(
                        "SELECT COALESCE(MAX(\"{column}\"),0)+1 FROM \"{}\"",
                        T::SQLITE_TABLE
                    ),
                    [],
                    |r| r.get(0),
                )?;
                if ["observations", "work"].contains(&T::TABLE)
                    && let Some(high) = db
                        .query_row(
                            "SELECT seq FROM sqlite_sequence WHERE name=?1",
                            [T::SQLITE_TABLE],
                            |r| r.get::<_, i64>(0),
                        )
                        .optional()?
                {
                    return Ok(next.max(high.checked_add(1).context("ID overflow")?));
                }
                Ok(next)
            }
            Self::ClickHouse(db) => db.next_id::<T>(CATALOG, column),
        }
    }
    pub fn ensure_peers(&self) -> Result<()> {
        if let Self::Sqlite(db) = self {
            db.execute_batch("CREATE TABLE IF NOT EXISTS peers(key TEXT PRIMARY KEY,input TEXT NOT NULL,metadata TEXT NOT NULL,raw TEXT NOT NULL); CREATE TABLE IF NOT EXISTS folders(id TEXT PRIMARY KEY,data TEXT NOT NULL);")?;
        }
        Ok(())
    }
    pub fn batch(&mut self, changes: Vec<Change>) -> Result<()> {
        match self {
            Self::ClickHouse(db) => db.commit(changes),
            Self::Sqlite(db) => {
                let tx = db.transaction()?;
                for change in changes {
                    ensure!(change.scope == CATALOG, "catalog batch contains epoch data");
                    write_change(&tx, &change)?;
                }
                tx.commit()?;
                Ok(())
            }
        }
    }
}

pub(crate) fn write_change(db: &Connection, change: &Change) -> Result<()> {
    ensure!(
        ClickHouseStore::table_names().contains(&change.table.as_str()) && change.table != "text",
        "unknown catalog table"
    );
    let info = db
        .prepare(&format!("PRAGMA table_info(\"{}\")", change.table))?
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let keys = info
        .iter()
        .filter(|(_, key)| *key != 0)
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    ensure!(!keys.is_empty(), "table has no primary key");
    if change.deleted {
        let filter = keys
            .iter()
            .map(|key| format!("\"{key}\"=?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        db.execute(
            &format!("DELETE FROM \"{}\" WHERE {filter}", change.table),
            params_from_iter(keys.iter().map(|key| sql_value(&change.data[*key]))),
        )?;
    } else {
        let mut fields = info
            .iter()
            .filter(|(name, _)| change.data.get(name).is_some())
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        if change.row > 0 {
            fields.insert(0, "rowid");
        }
        let names = fields
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(",");
        let slots = vec!["?"; fields.len()].join(",");
        let updates = fields
            .iter()
            .filter(|name| !keys.contains(name))
            .map(|name| format!("\"{name}\"=excluded.\"{name}\""))
            .collect::<Vec<_>>()
            .join(",");
        let action = if updates.is_empty() {
            "DO NOTHING".into()
        } else {
            format!("DO UPDATE SET {updates}")
        };
        let values = fields.iter().map(|name| {
            if *name == "rowid" {
                return SqlValue::Integer(change.row);
            }
            let v = &change.data[*name];
            if *name == "journal" && v.as_array().is_some_and(Vec::is_empty) {
                SqlValue::Null
            } else {
                sql_value(v)
            }
        });
        db.execute(
            &format!(
                "INSERT INTO \"{}\"({names}) VALUES({slots}) ON CONFLICT({}) {action}",
                change.table,
                keys.join(",")
            ),
            params_from_iter(values),
        )?;
    }
    Ok(())
}

pub(crate) fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(i64::from(*b)),
        Value::Number(n) => n
            .as_i64()
            .map(SqlValue::Integer)
            .unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => SqlValue::Text(s.clone()),
        Value::Array(a) => {
            SqlValue::Blob(a.iter().map(|v| v.as_u64().unwrap_or(0) as u8).collect())
        }
        Value::Object(_) => SqlValue::Text(value.to_string()),
    }
}
pub(crate) fn json_value(value: rusqlite::types::ValueRef<'_>) -> Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(n) => json!(n),
        ValueRef::Real(n) => json!(n),
        ValueRef::Text(s) => json!(std::str::from_utf8(s)?),
        ValueRef::Blob(b) => json!(b),
    })
}
pub(crate) fn read_rows<T: CatalogRow>(db: &Connection, query: &Select) -> Result<Vec<T>> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [T::SQLITE_TABLE],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(vec![]);
    }
    let mut st = db.prepare(&format!(
        "SELECT * FROM \"{}\"{}",
        T::SQLITE_TABLE,
        query.sql::<T>()?
    ))?;
    let names = st
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut rows = st.query(params_from_iter(
        query.filters.iter().map(|(_, _, v)| sql_value(v)),
    ))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut object = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let mut value = json_value(row.get_ref(i)?)?;
            if value.is_null() {
                match T::columns()
                    .iter()
                    .find(|(column, _)| *column == name)
                    .map(|(_, ty)| *ty)
                {
                    Some("Array(UInt8)") => value = json!([]),
                    Some("String") => value = json!(""),
                    _ => {}
                }
            }
            object.insert(name.clone(), value);
        }
        if T::TABLE == "epochs" && !object.contains_key("accepting") {
            object.insert("accepting".into(), json!(1));
        }
        out.push(
            serde_json::from_value(Value::Object(object))
                .with_context(|| format!("decode {} row", T::TABLE))?,
        );
    }
    Ok(out)
}
pub(crate) fn write_row<T: CatalogRow>(
    db: &Connection,
    row: &T,
    row_id: Option<i64>,
) -> Result<()> {
    let value = serde_json::to_value(row)?;
    let actual = db
        .prepare(&format!("PRAGMA table_info(\"{}\")", T::SQLITE_TABLE))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut columns = T::columns()
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| actual.iter().any(|s| s == name))
        .collect::<Vec<_>>();
    let mut values = columns
        .iter()
        .map(|k| {
            if *k == "journal" && value[*k].as_array().is_some_and(Vec::is_empty) {
                SqlValue::Null
            } else {
                sql_value(&value[*k])
            }
        })
        .collect::<Vec<_>>();
    if let Some(row_id) = row_id {
        columns.insert(0, "rowid");
        values.insert(0, SqlValue::Integer(row_id));
    }
    let names = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let placeholders = vec!["?"; columns.len()].join(",");
    let keys = T::KEYS.join(",");
    let updates = columns
        .iter()
        .filter(|name| **name != "rowid" && !T::KEYS.contains(name))
        .map(|name| format!("\"{name}\"=excluded.\"{name}\""))
        .collect::<Vec<_>>()
        .join(",");
    let conflict = if updates.is_empty() {
        "DO NOTHING".into()
    } else {
        format!("DO UPDATE SET {updates}")
    };
    db.execute(
        &format!(
            "INSERT INTO \"{}\"({names}) VALUES({placeholders}) ON CONFLICT({keys}) {conflict}",
            T::SQLITE_TABLE
        ),
        params_from_iter(values),
    )?;
    Ok(())
}
