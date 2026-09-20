//! Read-only explorer wire types. Identifiers are resolved by the server, never paths or SQL.
use crate::{Query, Record};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PAGE_SIZE: usize = 100;
pub const BINARY_CHUNK: usize = 64 * 1024;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub version: u32,
    pub storage: bool,
    pub conversations: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BinaryRef {
    Payload {
        hash: String,
    },
    Attachment {
        hash: String,
    },
    Cell {
        database: String,
        table: String,
        row: i64,
        column: String,
        generation: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Cell {
    Null,
    Integer(String),
    Real(String),
    Text(String),
    LargeText { bytes: u64, reference: BinaryRef },
    Blob { bytes: u64, reference: BinaryRef },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub declared_type: String,
    pub primary_key: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "view", rename_all = "snake_case")]
pub enum Browse {
    Conversations {
        folder: Option<String>,
    },
    Folders,
    Topics {
        peer: String,
    },
    Messages {
        peer: String,
        topic: Option<String>,
    },
    Attachment {
        hash: String,
    },
    Databases,
    Tables {
        database: String,
    },
    Rows {
        database: String,
        table: String,
        row: Option<i64>,
    },
    Location {
        sequence: i64,
    },
    Operations {
        name: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowseRequest {
    pub target: Browse,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "page_size")]
    pub limit: usize,
    #[serde(default)]
    pub search: String,
}
fn page_size() -> usize {
    PAGE_SIZE
}
impl BrowseRequest {
    pub fn new(target: Browse) -> Self {
        Self {
            target,
            cursor: None,
            limit: PAGE_SIZE,
            search: String::new(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    pub label: String,
    pub detail: Value,
    pub record: Option<Record>,
    pub open: Option<Browse>,
    #[serde(default)]
    pub binaries: Vec<BinaryRef>,
}
impl Entry {
    pub fn new(id: impl Into<String>, label: impl Into<String>, detail: Value) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            detail,
            record: None,
            open: None,
            binaries: vec![],
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrowsePage {
    pub entries: Vec<Entry>,
    pub next_cursor: Option<String>,
    pub columns: Vec<Column>,
    pub incomplete: bool,
    pub live: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryRequest {
    pub reference: BinaryRef,
    pub offset: u64,
    pub limit: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryPage {
    pub hex: String,
    pub total: u64,
    pub next_offset: Option<u64>,
}
/// Backend commands also used by the local adapter. HTTP adapters retain old query endpoints.
#[derive(Debug, Clone)]
pub enum Request {
    Capabilities,
    Query(Query),
    Browse(BrowseRequest),
    Binary(BinaryRequest),
}
#[derive(Debug)]
pub enum Response {
    Capabilities(Capabilities),
    Query(crate::Page),
    Browse(BrowsePage),
    Binary(BinaryPage),
}
