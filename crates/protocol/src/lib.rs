use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub sequence: i64,
    pub key: String,
    pub kind: String,
    pub observed_at: i64,
    pub source: String,
    pub payload_hash: String,
    pub root_type: String,
    pub schema_hash: String,
    pub partial: bool,
    pub deleted: bool,
    pub transformed: bool,
    pub metadata: Value,
    pub data: Value,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub representations: Vec<Representation>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct Query {
    #[arg(long, default_value = "true")]
    pub selector: String,
    #[arg(long)]
    pub key: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
    #[arg(long)]
    pub text: Option<String>,
    #[arg(long)]
    pub regex: Option<String>,
    #[arg(long)]
    pub all_versions: bool,
    /// UTC observation timestamp in microseconds.
    #[arg(long)]
    pub as_of: Option<i64>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long, default_value_t = 10000)]
    pub scan_limit: usize,
    #[arg(long)]
    pub cursor: Option<String>,
}
impl Default for Query {
    fn default() -> Self {
        Self {
            selector: "true".into(),
            key: None,
            kind: None,
            text: None,
            regex: None,
            all_versions: false,
            as_of: None,
            limit: 100,
            scan_limit: 10000,
            cursor: None,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub records: Vec<Record>,
    pub next_cursor: Option<String>,
    pub incomplete: bool,
    pub scanned: usize,
    pub snapshot: i64,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json,
    Ndjson,
    Txt,
    Html,
}
pub fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
/// Convert Telegram ID fields to strings at the public boundary, preserving native integers internally.
pub fn public_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if (key == "id" || key.ends_with("_id")) && v.is_number() {
                    *v = json!(v.to_string());
                } else {
                    public_json(v);
                }
            }
        }
        Value::Array(items) => {
            for v in items {
                public_json(v);
            }
        }
        _ => {}
    }
}
pub fn text(v: &Value) -> String {
    v.get("message")
        .or_else(|| v.get("about"))
        .or_else(|| v.get("title"))
        .or_else(|| v.get("first_name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Args)]
pub struct StatusOptions {
    /// Emit JSON even on a terminal. Watch mode emits one JSON object per line.
    #[arg(long)]
    pub json: bool,
    /// Refresh until interrupted, every N seconds (default two).
    #[arg(long, num_args=0..=1, default_missing_value="2", value_parser=clap::value_parser!(u64).range(1..))]
    pub watch: Option<u64>,
    /// Include expensive per-table and per-epoch storage accounting.
    #[arg(long)]
    pub details: bool,
}
/// Both clients render the same status response; absent totals remain unknown.
pub fn status_text(v: &Value) -> String {
    let mut out = format!(
        "Archive: {} objects, {} observations, {} pending payloads\n",
        v["objects"], v["observations"], v["pending_payloads"]
    );
    if let Some(sync) = v["sync"].as_array() {
        for job in sync {
            out += &format!(
                "Sync {}: {} — {}\n",
                job["id"].as_str().unwrap_or("?"),
                job["status"].as_str().unwrap_or("?"),
                job["details"]
            );
        }
    }
    if let Some(queue) = v["work"].as_object() {
        out += &format!(
            "Work queue: {}\n",
            queue.get("counts").unwrap_or(&Value::Null)
        );
        if let Some(items) = queue.get("items").and_then(Value::as_array) {
            for item in items {
                out += &format!(
                    "  #{} {}: {} {}\n",
                    item["sequence"],
                    item["kind"].as_str().unwrap_or("?"),
                    item["state"].as_str().unwrap_or("?"),
                    item["progress"]
                );
            }
        }
    }
    out += &format!("Resources: {}\n", v["resources"]);
    out += &format!("Scheduler: {}\n", v["scheduler"]);
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Representation {
    pub original: String,
    pub hash: String,
    pub recipe: String,
    pub bytes: u64,
    pub original_retained: bool,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum MediaSelection {
    #[default]
    Original,
    Preferred,
    All,
}
impl Record {
    pub fn media_hashes(&self, selection: MediaSelection) -> Vec<String> {
        let mut hashes = std::collections::BTreeSet::new();
        for hash in &self.attachments {
            let variants: Vec<_> = self
                .representations
                .iter()
                .filter(|r| r.original == *hash || r.hash == *hash)
                .collect();
            match selection {
                MediaSelection::Original => {
                    if variants
                        .iter()
                        .all(|r| r.hash != *hash || r.original_retained)
                    {
                        hashes.insert(hash.clone());
                    } else {
                        for r in variants.iter().filter(|r| r.original_retained) {
                            hashes.insert(r.original.clone());
                        }
                    }
                }
                MediaSelection::Preferred => {
                    hashes.insert(
                        variants
                            .iter()
                            .min_by_key(|r| r.bytes)
                            .map(|r| r.hash.clone())
                            .unwrap_or_else(|| hash.clone()),
                    );
                }
                MediaSelection::All => {
                    hashes.insert(hash.clone());
                    for r in variants {
                        hashes.insert(r.hash.clone());
                        if r.original_retained {
                            hashes.insert(r.original.clone());
                        }
                    }
                }
            }
        }
        hashes.into_iter().collect()
    }
}
