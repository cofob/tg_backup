use serde::{Deserialize, Serialize};
use serde_json::Value;
pub type TableColumns = (Vec<(&'static str, &'static str)>, &'static [&'static str]);

pub trait CatalogRow:
    clickhouse::RowOwned + clickhouse::RowRead + Serialize + Clone + Send + 'static
{
    const TABLE: &'static str;
    const SQLITE_TABLE: &'static str = Self::TABLE;
    const KEYS: &'static [&'static str];
    fn columns() -> Vec<(&'static str, &'static str)>;
    fn key(&self) -> String {
        let value = serde_json::to_value(self).expect("catalog row");
        serde_json::to_string(&Self::KEYS.iter().map(|k| &value[*k]).collect::<Vec<_>>())
            .expect("catalog key")
    }
}

macro_rules! row {
    ($name:ident, $table:literal, [$($key:literal),+], {$($field:ident: $ty:ty => $sql:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Default, Serialize, Deserialize, clickhouse::Row)]
        #[serde(default)]
        pub struct $name { $(pub $field: $ty),+ }
        impl CatalogRow for $name {
            const TABLE: &'static str = $table;
            const KEYS: &'static [&'static str] = &[$($key),+];
            fn columns() -> Vec<(&'static str, &'static str)> { vec![$((stringify!($field), $sql)),+] }
        }
    };
}

row!(Setting, "settings", ["key"], {key: String => "String", value: String => "String"});
row!(CounterRow, "counters", ["name"], {name: String => "String", value: i64 => "Int64"});
row!(SchemaRow, "schemas", ["hash"], {
    hash: String => "String", layer: i64 => "Int64", definition: String => "String"
});
row!(EpochRow, "epochs", ["id"], {
    id: i64 => "Int64", name: String => "String", path: String => "String",
    sealed: i64 => "Int64", generation: i64 => "Int64", accepting: i64 => "Int64", part: i64 => "Int64"
});
row!(PayloadRow, "payloads", ["hash"], {
    hash: String => "String", schema_hash: String => "String", root_type: String => "String",
    epoch: i64 => "Int64", block: Option<i64> => "Nullable(Int64)",
    offset: Option<i64> => "Nullable(Int64)", length: i64 => "Int64",
    journal: Vec<u8> => "Array(UInt8)"
});
row!(ObservationRow, "observations", ["id"], {
    id: i64 => "Int64", key: String => "String", kind: String => "String",
    observed: i64 => "Int64", source: String => "String", payload: String => "String",
    epoch: i64 => "Int64", metadata: String => "String", partial: i64 => "Int64",
    deleted: i64 => "Int64", transformed: i64 => "Int64", replay_key: Option<String> => "Nullable(String)"
});
row!(HeadRow, "heads", ["key"], {
    key: String => "String", observation: i64 => "Int64", revision: i64 => "Int64", partial: i64 => "Int64"
});
row!(CheckpointRow, "checkpoints", ["key"], {key: String => "String", value: String => "String"});
row!(JobRow, "jobs", ["id"], {
    id: String => "String", config: String => "String", status: String => "String",
    created: i64 => "Int64", updated: i64 => "Int64", details: String => "String"
});
row!(CoverageRow, "coverage", ["name"], {
    name: String => "String", status: String => "String", updated: i64 => "Int64", details: String => "String"
});
row!(MediaRow, "media", ["id"], {
    id: String => "String", location: String => "String", dc: i64 => "Int64",
    size: Option<i64> => "Nullable(Int64)", status: String => "String", offset: i64 => "Int64",
    hash: Option<String> => "Nullable(String)", error: Option<String> => "Nullable(String)",
    attempts: i64 => "Int64", retry_at: i64 => "Int64"
});
row!(MediaRefRow, "media_refs", ["media", "observation"], {
    media: String => "String", observation: i64 => "Int64"
});
row!(MaintenanceRow, "maintenance", ["id"], {
    id: String => "String", at: i64 => "Int64", policy: String => "String", report: String => "String"
});
row!(RetiredRow, "retired", ["path"], {path: String => "String"});
row!(WorkRow, "work", ["sequence"], {
    sequence: i64 => "Int64", kind: String => "String", dedupe: String => "String",
    config: String => "String", automatic: i64 => "Int64", state: String => "String",
    created: i64 => "Int64", updated: i64 => "Int64", retry_at: i64 => "Int64",
    attempts: i64 => "Int64", progress: String => "String", error: Option<String> => "Nullable(String)"
});
row!(RepresentationRow, "representations", ["original", "recipe"], {
    original: String => "String", hash: String => "String", recipe: String => "String",
    bytes: i64 => "Int64", created: i64 => "Int64", details: String => "String"
});
row!(TransformationRow, "media_transformations", ["id"], {
    id: i64 => "Int64", at: i64 => "Int64", original: String => "String",
    replacement: String => "String", policy: String => "String", details: String => "String"
});
row!(RuntimeRow, "runtime", ["key"], {key: String => "String", value: String => "String"});
row!(PeerRow, "peers", ["key"], {
    key: String => "String", input: String => "String", metadata: String => "String", raw: String => "String"
});
row!(FolderRow, "folders", ["id"], {id: String => "String", data: String => "String"});
row!(DictionaryRow, "dictionaries", ["id"], {
    id: i64 => "Int64", data: Vec<u8> => "Array(UInt8)", hash: String => "String"
});
row!(BlockRow, "blocks", ["id"], {
    id: i64 => "Int64", codec: String => "String", dictionary: Option<i64> => "Nullable(Int64)",
    raw_size: i64 => "Int64", checksum: String => "String", data: Vec<u8> => "Array(UInt8)"
});
row!(SearchRow, "text", ["id"], {id: i64 => "Int64", text: String => "String"});

// ClickHouse stores an absent journal as an empty byte array. TL payloads cannot be empty.
impl PayloadRow {
    pub fn has_journal(&self) -> bool {
        !self.journal.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub table: String,
    pub scope: String,
    pub key: String,
    pub row: i64,
    pub deleted: bool,
    pub data: Value,
}
impl Change {
    pub fn put<T: CatalogRow>(scope: &str, row: &T) -> anyhow::Result<Self> {
        let data = serde_json::to_value(row)?;
        Ok(Self {
            table: T::TABLE.into(),
            scope: scope.into(),
            key: row.key(),
            row: data
                .get("id")
                .or_else(|| data.get("sequence"))
                .and_then(Value::as_i64)
                .unwrap_or(0),
            deleted: false,
            data,
        })
    }
    pub fn delete<T: CatalogRow>(scope: &str, row: &T) -> anyhow::Result<Self> {
        Ok(Self {
            deleted: true,
            ..Self::put(scope, row)?
        })
    }
}
pub fn table_columns(table: &str) -> anyhow::Result<TableColumns> {
    macro_rules! tables { ($($row:ty),+) => { match table {
        $(<$row>::TABLE => Ok((<$row>::columns(),<$row>::KEYS)),)+
        _ => anyhow::bail!("unknown archive table"),
    } }; }
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
    )
}
