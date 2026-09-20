//! Offline social graphs derived only from retained archive evidence.
use crate::archive::{Archive, Record};
use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use clap::Args;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    path::PathBuf,
};

#[derive(Debug, Args)]
pub struct Options {
    #[arg(long)]
    pub focus: String,
    /// Inclusive UTC calendar date (YYYY-MM-DD).
    #[arg(long, value_parser = parse_date)]
    pub from: Option<NaiveDate>,
    /// Inclusive UTC calendar date (YYYY-MM-DD).
    #[arg(long, value_parser = parse_date)]
    pub to: Option<NaiveDate>,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub overwrite: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub kind: String,
}
#[derive(Debug, Serialize)]
pub struct Edge {
    pub source: String,
    pub target: String,
    pub kind: String,
    pub days: BTreeMap<String, u64>,
}
#[derive(Debug, Default, Serialize)]
pub struct Skipped {
    pub undated: u64,
    pub unresolved: u64,
}
#[derive(Debug, Serialize)]
pub struct Graph {
    pub focus: String,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub skipped: Skipped,
}
fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let date = s
        .parse::<NaiveDate>()
        .map_err(|_| "expected a valid YYYY-MM-DD date".to_owned())?;
    if date.format("%Y-%m-%d").to_string() != s || s.len() != 10 {
        return Err("expected YYYY-MM-DD".into());
    }
    Ok(date)
}
fn scalar(v: &Value) -> Option<String> {
    v.as_str()
        .map(str::to_owned)
        .or_else(|| v.as_i64().map(|n| n.to_string()))
}
fn peer(v: &Value) -> Option<String> {
    crate::telegram::peer_key(v)
}
fn valid_peer(s: &str) -> bool {
    s.split_once(':').is_some_and(|(kind, id)| {
        matches!(kind, "user" | "chat" | "channel") && id.parse::<i64>().is_ok_and(|n| n > 0)
    })
}
fn date(seconds: i64) -> Option<NaiveDate> {
    chrono::DateTime::from_timestamp(seconds, 0).map(|d| d.date_naive())
}
fn number(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}
fn message_peer(r: &Record) -> Option<String> {
    let (p, id) = r.key.split_once("/message:")?;
    (r.kind == "message" && r.data["_"] == "message" && valid_peer(p) && id.parse::<i64>().is_ok())
        .then(|| p.to_owned())
}
fn sender(r: &Record, account: Option<&str>) -> Option<String> {
    peer(&r.data["from_id"]).or_else(|| {
        let p = message_peer(r)?;
        if !p.starts_with("user:") {
            return None;
        }
        let account = account?;
        match r.data["out"].as_bool() {
            Some(true) => Some(account.into()),
            // TL absent flag means false.
            _ if p != account => Some(p),
            _ => None,
        }
    })
}
struct Builder {
    nodes: BTreeMap<String, Node>,
    edges: BTreeMap<(String, String, String), BTreeMap<String, u64>>,
    skipped: Skipped,
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
}
impl Builder {
    fn node(&mut self, id: &str) {
        if valid_peer(id) {
            self.nodes.entry(id.into()).or_insert_with(|| Node {
                id: id.into(),
                label: id.into(),
                kind: if id.starts_with("user:") {
                    "user"
                } else if id.starts_with("chat:") {
                    "group"
                } else {
                    "channel"
                }
                .into(),
            });
        }
    }
    fn accepts(&mut self, day: Option<NaiveDate>) -> bool {
        match day {
            Some(d) => self.from.is_none_or(|f| d >= f) && self.to.is_none_or(|t| d <= t),
            None if self.from.is_some() || self.to.is_some() => {
                self.skipped.undated += 1;
                false
            }
            None => true,
        }
    }
    fn edge(&mut self, a: &str, b: &str, kind: &str, day: Option<NaiveDate>) {
        if !valid_peer(a) || !valid_peer(b) {
            self.skipped.unresolved += 1;
            return;
        }
        if a == b {
            return;
        }
        self.node(a);
        self.node(b);
        *self
            .edges
            .entry((a.into(), b.into(), kind.into()))
            .or_default()
            .entry(day.map(|d| d.to_string()).unwrap_or_default())
            .or_default() += 1;
    }
    fn message(&mut self, r: &Record, account: Option<&str>, senders: &HashMap<String, String>) {
        let Some(p) = message_peer(r) else {
            return;
        };
        let day = number(&r.data["date"]).and_then(date);
        if !self.accepts(day) {
            return;
        }
        let Some(author) = sender(r, account) else {
            self.skipped.unresolved += 1;
            return;
        };
        if p.starts_with("user:") {
            if let Some(own) = account {
                let recipient = if r.data["out"] == true {
                    p.as_str()
                } else {
                    own
                };
                self.edge(&author, recipient, "direct", day);
            } else {
                self.skipped.unresolved += 1;
            }
        } else {
            self.edge(&author, &p, "participation", day);
        }
        if let Some(id) = scalar(&r.data["reply_to"]["reply_to_msg_id"]) {
            let target_peer = peer(&r.data["reply_to"]["reply_to_peer_id"]).unwrap_or(p);
            if let Some(target) = senders.get(&format!("{target_peer}/message:{id}")) {
                self.edge(&author, target, "reply", day);
            } else {
                self.skipped.unresolved += 1;
            }
        }
        let mut mentions = BTreeSet::new();
        if let Some(entities) = r.data["entities"].as_array() {
            for entity in entities {
                if entity["_"] == "messageEntityMentionName" {
                    if let Some(id) = scalar(&entity["user_id"]) {
                        mentions.insert(format!("user:{id}"));
                    } else {
                        self.skipped.unresolved += 1;
                    }
                }
            }
        }
        for target in mentions {
            if valid_peer(&target) {
                self.edge(&author, &target, "mention", day);
            }
        }
    }
}
// Only explicit participant containers are accepted; arbitrary embedded users are not members.
fn memberships(v: &Value, scope: Option<&str>, out: &mut BTreeSet<(String, String)>) {
    let name = v["_"].as_str().unwrap_or("");
    let chat = if matches!(name, "chatParticipants" | "chatParticipantsForbidden") {
        scalar(&v["chat_id"]).map(|id| format!("chat:{id}"))
    } else {
        scope.map(str::to_owned)
    };
    if matches!(
        name,
        "chatParticipants"
            | "chatParticipantsForbidden"
            | "channels.channelParticipants"
            | "channels.channelParticipant"
    ) {
        let entries: Vec<&Value> = v["participants"]
            .as_array()
            .map(|a| a.iter().collect())
            .unwrap_or_else(|| vec![&v["self_participant"], &v["participant"]]);
        for entry in entries {
            if matches!(
                entry["_"].as_str(),
                Some(
                    "chatParticipant"
                        | "chatParticipantCreator"
                        | "chatParticipantAdmin"
                        | "channelParticipant"
                        | "channelParticipantSelf"
                        | "channelParticipantCreator"
                        | "channelParticipantAdmin"
                )
            ) && let (Some(id), Some(chat)) = (scalar(&entry["user_id"]), chat.as_ref())
                && valid_peer(chat)
                && !chat.starts_with("user:")
            {
                out.insert((format!("user:{id}"), chat.clone()));
            }
        }
    }
    match v {
        Value::Object(m) => {
            for child in m.values() {
                if child.is_object() || child.is_array() {
                    memberships(child, chat.as_deref(), out);
                }
            }
        }
        Value::Array(a) => {
            for child in a {
                memberships(child, chat.as_deref(), out);
            }
        }
        _ => (),
    }
}
pub fn build(archive: &Archive, options: &Options) -> Result<Graph> {
    ensure!(
        valid_peer(&options.focus) && options.focus.starts_with("user:"),
        "focus must be user:<positive Telegram ID>"
    );
    ensure!(
        options.from.zip(options.to).is_none_or(|(a, b)| a <= b),
        "from must not be after to"
    );
    ensure!(
        !archive.writable,
        "graph requires a read-only archive snapshot"
    );
    let account = archive
        .checkpoint("account_id")?
        .and_then(|v| scalar(&v))
        .map(|id| format!("user:{id}"));
    let mut b = Builder {
        nodes: BTreeMap::new(),
        edges: BTreeMap::new(),
        skipped: Skipped::default(),
        from: options.from,
        to: options.to,
    };
    if let Some(id) = &account {
        b.node(id);
    }
    let mut senders = HashMap::new();
    let mut statement = archive
        .db
        .prepare("SELECT observation FROM heads ORDER BY observation")?;
    // First pass resolves identities and reply authors regardless of the selected date range.
    for id in statement.query_map([], |r| r.get::<_, i64>(0))? {
        let r = archive.record(id?)?;
        if r.deleted {
            continue;
        }
        if valid_peer(&r.key) && matches!(r.kind.as_str(), "user" | "chat") {
            b.node(&r.key);
            if let Some(n) = b.nodes.get_mut(&r.key) {
                let label = r.data["title"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        [
                            r.data["first_name"].as_str().unwrap_or(""),
                            r.data["last_name"].as_str().unwrap_or(""),
                        ]
                        .join(" ")
                        .trim()
                        .to_owned()
                    });
                if !label.is_empty() {
                    n.label = label;
                }
                if r.data["bot"] == true {
                    n.kind = "bot".into();
                }
                if r.data["megagroup"] == true {
                    n.kind = "group".into();
                }
            }
        }
        if let Some(p) = message_peer(&r) {
            b.node(&p);
            if let Some(author) = sender(&r, account.as_deref()) {
                b.node(&author);
                senders.insert(r.key, author);
            }
        }
    }
    let mut member_days = BTreeSet::new();
    for id in statement.query_map([], |r| r.get::<_, i64>(0))? {
        let r = archive.record(id?)?;
        if r.deleted {
            continue;
        }
        b.message(&r, account.as_deref(), &senders);
        if r.kind == "rpc"
            || matches!(
                r.root_type.as_str(),
                "ChatParticipants"
                    | "channels.ChannelParticipants"
                    | "channels.ChannelParticipant"
                    | "messages.ChatFull"
            )
        {
            let scope = r.metadata["scope"]
                .as_str()
                .and_then(|s| s.split('/').find(|s| valid_peer(s)));
            let mut members = BTreeSet::new();
            memberships(&r.data, scope, &mut members);
            let day = date(r.observed_at.div_euclid(1_000_000));
            for (user, chat) in members {
                b.node(&user);
                b.node(&chat);
                if b.accepts(day) && member_days.insert((user.clone(), chat.clone(), day)) {
                    b.edge(&user, &chat, "membership", day);
                }
            }
        }
    }
    ensure!(
        b.nodes.contains_key(&options.focus),
        "focal user is absent from the archive"
    );
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for (a, c, _) in b.edges.keys() {
        adjacency.entry(a).or_default().push(c);
        adjacency.entry(c).or_default().push(a);
    }
    let mut connected = BTreeSet::new();
    let mut pending = vec![options.focus.as_str()];
    while let Some(id) = pending.pop() {
        if connected.insert(id.to_owned()) {
            pending.extend(adjacency.get(id).into_iter().flatten().copied());
        }
    }
    Ok(Graph {
        focus: options.focus.clone(),
        from: options.from,
        to: options.to,
        skipped: b.skipped,
        nodes: b
            .nodes
            .into_values()
            .filter(|n| connected.contains(&n.id))
            .collect(),
        edges: b
            .edges
            .into_iter()
            .filter(|((a, c, _), _)| connected.contains(a) && connected.contains(c))
            .map(|((source, target, kind), days)| Edge {
                source,
                target,
                kind,
                days,
            })
            .collect(),
    })
}
pub fn html(graph: &Graph) -> Result<String> {
    let data = serde_json::to_string(graph)?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    Ok(include_str!("graph.html").replace("/*GRAPH_DATA*/null", &data))
}
pub fn run(root: &std::path::Path, options: &Options) -> Result<()> {
    ensure!(
        options.overwrite || !options.output.exists(),
        "output exists; use --overwrite"
    );
    let archive = Archive::open(root, false)?;
    let graph = build(&archive, options)?;
    let content = html(&graph)?;
    let parent = options
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let temp = parent.join(format!(".tg-graph-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        drop(file);
        if options.overwrite {
            std::fs::rename(&temp, &options.output)?;
        } else {
            std::fs::hard_link(&temp, &options.output)
                .context("publishing graph (output must not already exist)")?;
        }
        Ok(())
    })();
    let _ = std::fs::remove_file(&temp);
    result?;
    eprintln!(
        "Graph: {} nodes, {} edges; skipped {} undated evidence items and {} unresolved relationships. Membership is observed evidence; archive coverage may be incomplete.",
        graph.nodes.len(),
        graph.edges.len(),
        graph.skipped.undated,
        graph.skipped.unresolved
    );
    Ok(())
}
