use crate::{
    Backend,
    export::{self, Source},
};
use anyhow::{Context, Result, bail, ensure};
use ratatui::crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, MouseEventKind,
    },
    execute,
};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use ratatui_image::{
    Image, Resize,
    picker::{Picker, ProtocolType},
    protocol::Protocol,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{Cursor, IsTerminal},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tg_backup_protocol::{Format, MediaSelection, Query, Record, explorer::*};
use tokio::{sync::mpsc, task::JoinHandle};

const HELP: &str = "1 Chats  2 Records  3 Storage  4 Operations\nTab / Shift-Tab: change pane   arrows / j,k: move\nEnter: open item / expand JSON   Esc: back / cancel\n/ search   f filters   e export   r refresh\nn / PageDown: next page   b / PageUp: previous page\nh: selected object's history   s: physical storage\na: attachments / binary fields   p: image preview\nt: forum topics   g: folders   c: all conversations\nq / Ctrl-C: quit   ?: this help\n\nBrowsing is read-only. Refresh is explicit.\nRaw operational/table rows are live; record pages use snapshots.\nExports always follow all pages in the chosen scope.\nImage previews are requested with p and limited to 16 MiB / 40 MP.\nForm: Tab changes field; Enter submits; Esc cancels.\nExport overwrite requires typing YES.\nCancelled exports remove incomplete files; completed attachments remain.";
#[derive(Clone)]
enum ViewSource {
    Query(Query),
    Browse(BrowseRequest),
    Binary(BinaryRequest),
    Static,
}
#[derive(Clone)]
struct Screen {
    title: String,
    source: ViewSource,
    entries: Vec<Entry>,
    selected: usize,
    next: Option<String>,
    live: bool,
}
impl Screen {
    fn browse(title: impl Into<String>, target: Browse) -> Self {
        Self {
            title: title.into(),
            source: ViewSource::Browse(BrowseRequest::new(target)),
            entries: vec![],
            selected: 0,
            next: None,
            live: false,
        }
    }
    fn records(query: Query) -> Self {
        Self {
            title: "Records".into(),
            source: ViewSource::Query(query),
            entries: vec![],
            selected: 0,
            next: None,
            live: false,
        }
    }
    fn selected(&self) -> Option<&Entry> {
        self.entries.get(self.selected)
    }
}
struct Form {
    title: String,
    fields: Vec<(&'static str, String)>,
    selected: usize,
    kind: FormKind,
}
enum FormKind {
    Search,
    Filter,
    Export,
}
enum Message {
    Loaded(u64, Result<Response>),
    Exported(Result<export::Progress>),
    Image(u64, Result<Protocol>),
}
struct App {
    screen: Screen,
    history: Vec<Screen>,
    roots: [Option<Screen>; 4],
    root_selection: [u64; 4],
    selection_revision: u64,
    view: usize,
    focus: usize,
    list_state: ListState,
    sidebar: Option<Screen>,
    sidebar_state: ListState,
    scalar_text: Option<String>,
    scalar_scroll: u16,
    detail_state: ListState,
    tree_selected: usize,
    expanded: BTreeSet<String>,
    notice: String,
    loading: bool,
    help: bool,
    form: Option<Form>,
    capabilities: bool,
    selected_record: Option<Record>,
    request_id: u64,
    pending: Option<JoinHandle<()>>,
    export_task: Option<JoinHandle<()>>,
    cancel: Arc<AtomicBool>,
    image: Option<Protocol>,
    picker: Option<Picker>,
    panes: Vec<Rect>,
    image_area: Rect,
}
impl App {
    fn new(capabilities: bool, picker: Option<Picker>) -> Self {
        Self {
            screen: if capabilities {
                Screen::browse("Chats", Browse::Conversations { folder: None })
            } else {
                Screen::records(Query::default())
            },
            history: vec![],
            roots: std::array::from_fn(|_| None),
            root_selection: [0; 4],
            selection_revision: 0,
            view: if capabilities { 0 } else { 1 },
            focus: 0,
            list_state: ListState::default(),
            sidebar: None,
            sidebar_state: ListState::default(),
            scalar_text: None,
            scalar_scroll: 0,
            detail_state: ListState::default(),
            tree_selected: 0,
            expanded: BTreeSet::from([String::new()]),
            notice: if capabilities {
                "Ready".into()
            } else {
                "Server has no explorer API; records and exports remain available. Upgrade server for chats/storage.".into()
            },
            loading: false,
            help: false,
            form: None,
            capabilities,
            selected_record: None,
            request_id: 0,
            pending: None,
            export_task: None,
            cancel: Arc::new(AtomicBool::new(false)),
            image: None,
            picker,
            panes: vec![],
            image_area: Rect::default(),
        }
    }
    fn remember(&mut self) {
        if let Some(record) = self.screen.selected().and_then(|e| e.record.clone()) {
            if self
                .selected_record
                .as_ref()
                .is_none_or(|old| old.sequence != record.sequence)
            {
                self.selection_revision += 1;
            }
            self.selected_record = Some(record);
        }
    }
    fn navigate(&mut self, screen: Screen) {
        if let Some(task) = self.pending.take() {
            task.abort();
        }
        self.request_id += 1;
        self.loading = false;
        self.remember();
        self.history.push(self.screen.clone());
        if self.history.len() > 32 {
            self.history.remove(0);
        }
        self.screen = screen;
        self.reset_selection();
    }
    fn reset_selection(&mut self) {
        self.list_state = ListState::default();
        self.detail_state = ListState::default();
        self.tree_selected = 0;
        self.expanded = BTreeSet::from([String::new()]);
        self.image = None;
        self.scalar_text = None;
        self.scalar_scroll = 0;
    }
    fn load(&mut self, backend: Arc<dyn Backend>, tx: mpsc::UnboundedSender<Message>) {
        if let Some(task) = self.pending.take() {
            task.abort();
        }
        self.request_id += 1;
        let id = self.request_id;
        let request = match &self.screen.source {
            ViewSource::Query(q) => Request::Query(q.clone()),
            ViewSource::Browse(q) => Request::Browse(q.clone()),
            ViewSource::Binary(q) => Request::Binary(q.clone()),
            ViewSource::Static => {
                self.loading = false;
                return;
            }
        };
        self.loading = true;
        self.image = None;
        self.pending = Some(tokio::spawn(async move {
            let result = backend.request(request).await;
            let _ = tx.send(Message::Loaded(id, result));
        }));
    }
    fn received(&mut self, response: Response) -> Result<()> {
        let selected = self.screen.selected().map(|e| e.id.clone());
        let (entries, next, live) = match response {
            Response::Query(page) => (
                page.records.into_iter().map(record_entry).collect(),
                page.next_cursor,
                false,
            ),
            Response::Browse(page) => (page.entries, page.next_cursor, page.live),
            Response::Binary(page) => {
                let bytes = hex::decode(&page.hex)?;
                let offset = match &self.screen.source {
                    ViewSource::Binary(q) => q.offset,
                    _ => 0,
                };
                let lines = bytes
                    .chunks(16)
                    .enumerate()
                    .map(|(i, chunk)| {
                        format!(
                            "{:08x}  {:47}  {}",
                            offset + i as u64 * 16,
                            chunk
                                .iter()
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" "),
                            chunk
                                .iter()
                                .map(|b| if (32..127).contains(b) {
                                    *b as char
                                } else {
                                    '.'
                                })
                                .collect::<String>()
                        )
                    })
                    .collect::<Vec<_>>();
                let mut entries = vec![Entry::new(
                    "bytes",
                    format!(
                        "Bytes {offset}–{} of {}",
                        offset + bytes.len() as u64,
                        page.total
                    ),
                    json!({"offset":offset,"total":page.total,"hex":lines,"text":String::from_utf8_lossy(&bytes)}),
                )];
                if let ViewSource::Binary(q) = &self.screen.source {
                    entries[0].binaries.push(q.reference.clone());
                }
                (entries, page.next_offset.map(|n| n.to_string()), false)
            }
            _ => bail!("unexpected backend response"),
        };
        self.screen.entries = entries;
        self.screen.next = next;
        self.screen.live = live;
        self.screen.selected = selected
            .and_then(|id| self.screen.entries.iter().position(|e| e.id == id))
            .unwrap_or(0);
        self.reset_selection();
        if let ViewSource::Browse(q) = &self.screen.source
            && matches!(
                q.target,
                Browse::Conversations { .. } | Browse::Folders | Browse::Topics { .. }
            )
        {
            self.sidebar = Some(self.screen.clone());
        }
        self.remember();
        Ok(())
    }
    fn tree(&self) -> Vec<(String, String, bool)> {
        let mut lines = vec![];
        if let Some(entry) = self.screen.selected() {
            tree_lines(&entry.detail, "", "", 0, &self.expanded, &mut lines);
        }
        lines
    }
    fn shift(&mut self, amount: isize) {
        if self.focus == 0 && self.view == 0 {
            if let Some(sidebar) = &mut self.sidebar {
                sidebar.selected = sidebar
                    .selected
                    .saturating_add_signed(amount)
                    .min(sidebar.entries.len().saturating_sub(1));
            }
        } else if self.focus == 2 && self.scalar_text.is_some() {
            self.scalar_scroll = self.scalar_scroll.saturating_add_signed(amount as i16);
        } else if self.focus == 2 {
            self.tree_selected = self
                .tree_selected
                .saturating_add_signed(amount)
                .min(self.tree().len().saturating_sub(1));
        } else if self.focus == 1 {
            self.screen.selected = self
                .screen
                .selected
                .saturating_add_signed(amount)
                .min(self.screen.entries.len().saturating_sub(1));
            self.tree_selected = 0;
            self.image = None;
            self.scalar_text = None;
            if let Some(task) = self.pending.take() {
                task.abort();
            }
            self.request_id += 1;
            self.loading = false;
            self.remember();
        }
    }
    fn export_source(&self, scope: &str) -> Result<Source> {
        let entry = self.screen.selected();
        match scope {
            "record" => Ok(Source::Record(Box::new(
                entry
                    .and_then(|e| e.record.clone())
                    .context("select a record first")?,
            ))),
            "archive" => Ok(Source::Query(Query::default())),
            "row" => Ok(Source::Row(Box::new(
                entry.cloned().context("select a row first")?,
            ))),
            "binary" => {
                let reference = match &self.screen.source {
                    ViewSource::Binary(q) => Some(q.reference.clone()),
                    _ => entry.and_then(|e| e.binaries.first().cloned()),
                }
                .context("select a binary field or attachment first")?;
                Ok(Source::Binary(reference))
            }
            "table" => {
                if let ViewSource::Browse(q) = &self.screen.source {
                    let mut q = q.clone();
                    if let Browse::Rows { row, .. } = &mut q.target {
                        *row = None;
                    } else {
                        bail!("open a storage table first");
                    }
                    Ok(Source::Rows(q))
                } else {
                    bail!("open a storage table first")
                }
            }
            "chat"
                if entry
                    .and_then(|e| e.open.as_ref())
                    .is_some_and(|target| matches!(target, Browse::Messages { .. })) =>
            {
                let Some(Browse::Messages { peer, .. }) = entry.and_then(|e| e.open.as_ref())
                else {
                    unreachable!()
                };
                Ok(Source::Query(Query {
                    peer: Some(peer.clone()),
                    kind: Some("message".into()),
                    ..Query::default()
                }))
            }
            "view" | "chat" | "topic" => match &self.screen.source {
                ViewSource::Query(q) if scope == "view" => Ok(Source::Query(q.clone())),
                ViewSource::Browse(q) => match &q.target {
                    Browse::Messages { peer, topic } => Ok(Source::Query(Query {
                        peer: Some(peer.clone()),
                        topic: if scope == "chat" { None } else { topic.clone() },
                        regex: (!q.search.is_empty())
                            .then(|| format!("(?i){}", regex::escape(&q.search))),
                        kind: Some("message".into()),
                        ..Query::default()
                    })),
                    Browse::Rows { .. } | Browse::Operations { .. } if scope == "view" => {
                        Ok(Source::Rows(q.clone()))
                    }
                    _ => bail!("open a conversation, query, or table before exporting this scope"),
                },
                _ => bail!("scope is not available in this view"),
            },
            _ => bail!("scope must be record, view, chat, topic, archive, row, table, or binary"),
        }
    }
    fn form(&mut self, kind: FormKind) {
        let (title, fields) = match kind {
            FormKind::Search => (
                "Search",
                vec![(
                    "Text",
                    match &self.screen.source {
                        ViewSource::Query(q) => q.text.clone().unwrap_or_default(),
                        ViewSource::Browse(q) => q.search.clone(),
                        _ => String::new(),
                    },
                )],
            ),
            FormKind::Filter => {
                let q = match &self.screen.source {
                    ViewSource::Query(q) => q.clone(),
                    _ => Query::default(),
                };
                (
                    "Record filters",
                    vec![
                        ("Text (FTS)", q.text.unwrap_or_default()),
                        ("Regex", q.regex.unwrap_or_default()),
                        ("Selector", q.selector),
                        ("Kind", q.kind.unwrap_or_default()),
                        (
                            "As of (UTC microseconds)",
                            q.as_of.map(|v| v.to_string()).unwrap_or_default(),
                        ),
                        ("All versions (true/false)", q.all_versions.to_string()),
                        ("Peer", q.peer.unwrap_or_default()),
                        ("Topic", q.topic.unwrap_or_default()),
                    ],
                )
            }
            FormKind::Export => {
                let scope = match &self.screen.source {
                    ViewSource::Binary(_) => "binary",
                    ViewSource::Browse(q) if matches!(q.target, Browse::Rows { .. }) => "table",
                    ViewSource::Query(_) => "view",
                    _ if self
                        .screen
                        .selected()
                        .and_then(|e| e.open.as_ref())
                        .is_some_and(|target| matches!(target, Browse::Messages { .. })) =>
                    {
                        "chat"
                    }
                    _ if self
                        .screen
                        .selected()
                        .is_some_and(|e| e.record.is_none() && !e.binaries.is_empty()) =>
                    {
                        "binary"
                    }
                    _ => {
                        if self.screen.selected().is_some_and(|e| e.record.is_some()) {
                            "record"
                        } else {
                            "view"
                        }
                    }
                };
                (
                    "Export — chat means all topics; topic/view preserves the selected topic",
                    vec![
                        (
                            "Scope: record/view/chat/topic/archive/row/table/binary",
                            scope.into(),
                        ),
                        ("Output path", String::new()),
                        ("Format: ndjson/json/txt/html", "ndjson".into()),
                        (
                            "All versions (true/false)",
                            matches!(&self.screen.source,ViewSource::Query(q) if q.all_versions)
                                .to_string(),
                        ),
                        ("Attachments directory (optional)", String::new()),
                        ("Media: original/preferred/all", "original".into()),
                        ("Overwrite existing output: type YES", String::new()),
                    ],
                )
            }
        };
        self.form = Some(Form {
            title: title.into(),
            fields,
            selected: 0,
            kind,
        });
    }
    fn submit(
        &mut self,
        backend: Arc<dyn Backend>,
        tx: mpsc::UnboundedSender<Message>,
        progress: mpsc::Sender<export::Progress>,
    ) -> Result<bool> {
        let form = self.form.as_ref().context("no form")?;
        let values = form
            .fields
            .iter()
            .map(|(_, s)| s.trim().to_owned())
            .collect::<Vec<_>>();
        let optional = |i: usize| (!values[i].is_empty()).then(|| values[i].clone());
        match form.kind {
            FormKind::Search => {
                match &mut self.screen.source {
                    ViewSource::Query(q) => {
                        q.text = optional(0);
                        q.cursor = None;
                    }
                    ViewSource::Browse(q) => {
                        q.search = values[0].clone();
                        q.cursor = None;
                    }
                    _ => bail!("search is unavailable here"),
                }
                self.form = None;
                Ok(true)
            }
            FormKind::Filter => {
                let q = Query {
                    text: optional(0),
                    regex: optional(1),
                    selector: values[2].clone(),
                    kind: optional(3),
                    as_of: optional(4).map(|s| s.parse()).transpose()?,
                    all_versions: values[5].parse()?,
                    peer: optional(6),
                    topic: optional(7),
                    ..Query::default()
                };
                self.navigate(Screen::records(q));
                self.view = 1;
                self.form = None;
                Ok(true)
            }
            FormKind::Export => {
                ensure!(self.export_task.is_none(), "an export is already running");
                let mut source = self.export_source(&values[0])?;
                let all_versions: bool = values[3].parse()?;
                if let Source::Query(q) = &mut source {
                    q.all_versions = all_versions;
                }
                if all_versions && let Source::Record(record) = &source {
                    source = Source::Query(Query {
                        key: Some(record.key.clone()),
                        all_versions: true,
                        ..Query::default()
                    });
                }
                let format = match values[2].as_str() {
                    "json" => Format::Json,
                    "ndjson" => Format::Ndjson,
                    "txt" => Format::Txt,
                    "html" => Format::Html,
                    _ => bail!("unknown format"),
                };
                let media = match values[5].as_str() {
                    "original" => MediaSelection::Original,
                    "preferred" => MediaSelection::Preferred,
                    "all" => MediaSelection::All,
                    _ => bail!("unknown media selection"),
                };
                ensure!(!values[1].is_empty(), "output path is required");
                let options = export::Options {
                    output: values[1].clone().into(),
                    format,
                    attachments: optional(4).map(Into::into),
                    media,
                    overwrite: values[6] == "YES",
                };
                ensure!(
                    options.overwrite || !options.output.exists(),
                    "output exists; type YES in the overwrite field to replace it"
                );
                self.cancel = Arc::new(AtomicBool::new(false));
                let cancel = self.cancel.clone();
                self.export_task = Some(tokio::spawn(async move {
                    let result = export::run(backend, source, options, cancel, progress).await;
                    let _ = tx.send(Message::Exported(result));
                }));
                self.notice = "Export running — Esc cancels".into();
                self.form = None;
                Ok(false)
            }
        }
    }
    fn root(&mut self, index: usize) {
        self.remember();
        self.roots[self.view] = Some(self.screen.clone());
        self.root_selection[self.view] = self.selection_revision;
        self.view = index;
        let context_changed =
            matches!(index, 1 | 2) && self.root_selection[index] != self.selection_revision;
        if !context_changed && let Some(screen) = self.roots[index].clone() {
            self.navigate(screen);
            return;
        }
        let screen = match index {
            0 => Screen::browse("Chats", Browse::Conversations { folder: None }),
            1 => {
                if let Some(record) = &self.selected_record {
                    Screen::records(Query {
                        key: Some(record.key.clone()),
                        ..Query::default()
                    })
                } else {
                    Screen::records(Query::default())
                }
            }
            2 => {
                if let Some(record) = &self.selected_record {
                    Screen::browse(
                        "Record storage",
                        Browse::Location {
                            sequence: record.sequence,
                        },
                    )
                } else {
                    Screen::browse("Storage", Browse::Databases)
                }
            }
            _ => operations(),
        };
        self.navigate(screen);
    }
    fn key(
        &mut self,
        key: KeyEvent,
        backend: Arc<dyn Backend>,
        tx: mpsc::UnboundedSender<Message>,
        progress: mpsc::Sender<export::Progress>,
    ) -> Result<bool> {
        if key.kind == KeyEventKind::Release {
            return Ok(false);
        }
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            return Ok(true);
        }
        if self.help {
            self.help = false;
            return Ok(false);
        }
        if self.form.is_some() {
            if key.code == KeyCode::Enter {
                if self.submit(backend.clone(), tx.clone(), progress)? {
                    self.load(backend, tx);
                }
                return Ok(false);
            }
            let form = self.form.as_mut().unwrap();
            match key.code {
                KeyCode::Esc => self.form = None,
                KeyCode::Tab | KeyCode::Down => {
                    form.selected = (form.selected + 1) % form.fields.len()
                }
                KeyCode::BackTab | KeyCode::Up => {
                    form.selected = (form.selected + form.fields.len() - 1) % form.fields.len()
                }
                KeyCode::Backspace => {
                    form.fields[form.selected].1.pop();
                }
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    form.fields[form.selected].1.clear()
                }
                KeyCode::Char(c)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    form.fields[form.selected].1.push(c)
                }
                _ => {}
            }
            return Ok(false);
        }
        let mut load = false;
        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('?') => self.help = true,
            KeyCode::Tab => self.focus = (self.focus + 1) % 3,
            KeyCode::BackTab => self.focus = (self.focus + 2) % 3,
            KeyCode::Char(c @ '1'..='4') => {
                let index = c as usize - '1' as usize;
                ensure!(
                    self.capabilities || index == 1,
                    "this server does not support explorer views"
                );
                self.root(index);
                load = true;
            }
            KeyCode::Down | KeyCode::Char('j') => self.shift(1),
            KeyCode::Up | KeyCode::Char('k') => self.shift(-1),
            KeyCode::Char('r') => {
                match &mut self.screen.source {
                    ViewSource::Query(q) => q.cursor = None,
                    ViewSource::Browse(q) => q.cursor = None,
                    ViewSource::Binary(q) => q.offset = 0,
                    _ => {}
                }
                load = true;
            }
            KeyCode::Char('/') => self.form(FormKind::Search),
            KeyCode::Char('f') => self.form(FormKind::Filter),
            KeyCode::Char('e') => self.form(FormKind::Export),
            KeyCode::Esc => {
                if self.scalar_text.take().is_some() {
                    self.scalar_scroll = 0;
                } else if self.export_task.is_some() {
                    self.cancel.store(true, Ordering::Relaxed);
                    if let Some(task) = self.export_task.take() {
                        task.abort();
                    }
                    self.notice = "Export cancelled; incomplete files removed; completed attachments retained".into();
                } else if let Some(screen) = self.history.pop() {
                    self.screen = screen;
                    self.reset_selection();
                    if let Some(task) = self.pending.take() {
                        task.abort();
                    }
                    self.request_id += 1;
                    self.loading = false;
                }
            }
            KeyCode::Char('b') | KeyCode::PageUp => {
                if let Some(screen) = self.history.pop() {
                    self.screen = screen;
                    self.reset_selection();
                    load = true;
                }
            }
            KeyCode::Char('n') | KeyCode::PageDown => {
                if let Some(cursor) = self.screen.next.clone() {
                    let mut screen = self.screen.clone();
                    screen.selected = 0;
                    screen.entries.clear();
                    match &mut screen.source {
                        ViewSource::Query(q) => q.cursor = Some(cursor),
                        ViewSource::Browse(q) => q.cursor = Some(cursor),
                        ViewSource::Binary(q) => q.offset = cursor.parse()?,
                        _ => {}
                    }
                    self.navigate(screen);
                    load = true;
                }
            }
            KeyCode::Char('g') => {
                ensure!(self.capabilities, "server lacks explorer API");
                self.navigate(Screen::browse("Folders", Browse::Folders));
                load = true;
            }
            KeyCode::Char('c') => {
                ensure!(self.capabilities, "server lacks explorer API");
                self.navigate(Screen::browse(
                    "All chats",
                    Browse::Conversations { folder: None },
                ));
                load = true;
            }
            KeyCode::Char('d') => {
                ensure!(self.capabilities, "server lacks explorer API");
                self.navigate(Screen::browse("Databases", Browse::Databases));
                load = true;
            }
            KeyCode::Char('t') => {
                let peer = match &self.screen.source {
                    ViewSource::Browse(q) => match &q.target {
                        Browse::Messages { peer, .. } => Some(peer.clone()),
                        _ => self.screen.selected().and_then(|e| match &e.open {
                            Some(Browse::Messages { peer, .. }) => Some(peer.clone()),
                            _ => None,
                        }),
                    },
                    _ => None,
                }
                .context("open or select a conversation first")?;
                self.navigate(Screen::browse("Forum topics", Browse::Topics { peer }));
                load = true;
            }
            KeyCode::Char('h') | KeyCode::Char('s') => {
                self.remember();
                let record = self
                    .selected_record
                    .as_ref()
                    .context("select a record first")?;
                let screen = if key.code == KeyCode::Char('h') {
                    Screen::records(Query {
                        key: Some(record.key.clone()),
                        all_versions: true,
                        ..Query::default()
                    })
                } else {
                    ensure!(self.capabilities, "server lacks storage API");
                    Screen::browse(
                        "Record storage",
                        Browse::Location {
                            sequence: record.sequence,
                        },
                    )
                };
                self.navigate(screen);
                load = true;
            }
            KeyCode::Char('a') => {
                let entry = self
                    .screen
                    .selected()
                    .context("select a record or row first")?;
                let mut entries = vec![];
                for (i, reference) in entry.binaries.iter().enumerate() {
                    let mut e = Entry::new(
                        i.to_string(),
                        binary_label(reference),
                        serde_json::to_value(reference)?,
                    );
                    if let BinaryRef::Attachment { hash } = reference {
                        e.detail = json!({"reference":reference,"media_metadata":entry.record.as_ref().map(|r| &r.data),"representations":entry.record.as_ref().map(|r| &r.representations)});
                        if self.capabilities {
                            e.open = Some(Browse::Attachment { hash: hash.clone() });
                        }
                    }
                    e.binaries.push(reference.clone());
                    entries.push(e);
                }
                ensure!(
                    !entries.is_empty(),
                    "this item has no retained attachment references or binary fields"
                );
                self.navigate(Screen {
                    title: "Attachments / binary fields — Enter inspects, p previews, e exports"
                        .into(),
                    source: ViewSource::Static,
                    entries,
                    selected: 0,
                    next: None,
                    live: false,
                });
                self.focus = 1;
            }
            KeyCode::Char('p') => {
                let picker = self.picker.clone().context(
                    "terminal image protocol unavailable; use attachment details or export",
                )?;
                let reference = self
                    .screen
                    .selected()
                    .and_then(|e| e.binaries.first())
                    .cloned()
                    .context("select an attachment first")?;
                ensure!(
                    matches!(reference, BinaryRef::Attachment { .. }),
                    "image preview applies to attachments"
                );
                if let Some(task) = self.pending.take() {
                    task.abort();
                }
                self.request_id += 1;
                let id = self.request_id;
                let area = self.image_area;
                self.loading = true;
                self.pending = Some(tokio::spawn(async move {
                    let result = async {
                        let bytes =
                            export::binary_bytes(backend.as_ref(), reference, 16 * 1024 * 1024)
                                .await?;
                        tokio::task::spawn_blocking(move || decode_image(bytes, picker, area))
                            .await?
                    }
                    .await;
                    let _ = tx.send(Message::Image(id, result));
                }));
                return Ok(false);
            }
            KeyCode::Enter | KeyCode::Right => {
                if self.focus == 0 {
                    if self.view == 0
                        && let Some(entry) =
                            self.sidebar.as_ref().and_then(Screen::selected).cloned()
                        && let Some(target) = entry.open
                    {
                        self.navigate(Screen::browse(entry.label, target));
                        load = true;
                    }
                    self.focus = 1;
                } else if self.focus == 2 {
                    if let Some((path, _, expandable)) = self.tree().get(self.tree_selected) {
                        if *expandable {
                            if !self.expanded.remove(path) {
                                self.expanded.insert(path.clone());
                            }
                        } else if let Some(value) =
                            self.screen.selected().and_then(|e| e.detail.pointer(path))
                        {
                            self.scalar_text = Some(
                                value
                                    .as_str()
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| value.to_string()),
                            );
                            self.scalar_scroll = 0;
                        }
                    }
                } else if let Some(entry) = self.screen.selected().cloned() {
                    if let Some(target) = entry.open {
                        self.navigate(Screen::browse(entry.label, target));
                        load = true;
                    } else if matches!(self.screen.source, ViewSource::Static)
                        && !entry.binaries.is_empty()
                    {
                        let reference = entry.binaries[0].clone();
                        self.navigate(Screen {
                            title: binary_label(&reference),
                            source: ViewSource::Binary(BinaryRequest {
                                reference,
                                offset: 0,
                                limit: 4096,
                            }),
                            entries: vec![],
                            selected: 0,
                            next: None,
                            live: false,
                        });
                        load = true;
                    } else {
                        self.focus = 2;
                    }
                }
            }
            KeyCode::Left => self.focus = self.focus.saturating_sub(1),
            _ => {}
        }
        if load {
            self.load(backend, tx);
        }
        Ok(false)
    }
    fn draw(&mut self, f: &mut Frame) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(1),
                Constraint::Length(3),
            ])
            .split(f.area());
        f.render_widget(
            Paragraph::new(" Telegram Archive   1 Chats   2 Records   3 Storage   4 Operations")
                .style(Style::default().fg(Color::Cyan)),
            rows[0],
        );
        let panes = if rows[1].width >= 100 {
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(22),
                    Constraint::Percentage(42),
                    Constraint::Min(25),
                ])
                .split(rows[1])
                .to_vec()
        } else {
            vec![rows[1]; 3]
        };
        self.panes = panes.clone();
        self.image_area = panes[2].inner(ratatui::layout::Margin {
            horizontal: 1,
            vertical: 1,
        });
        let visible = |n| rows[1].width >= 100 || self.focus == n;
        if visible(0)
            && self.view == 0
            && let Some(sidebar) = self.sidebar.as_ref()
        {
            let items = sidebar
                .entries
                .iter()
                .map(|e| ListItem::new(clean(&e.label)))
                .collect::<Vec<_>>();
            self.sidebar_state
                .select((!items.is_empty()).then_some(sidebar.selected));
            f.render_stateful_widget(
                List::new(items)
                    .block(block("Chats / topics · c all · g folders", self.focus == 0))
                    .highlight_style(Style::default().fg(Color::Cyan)),
                panes[0],
                &mut self.sidebar_state,
            );
        } else if visible(0) {
            let items = [
                "Chats",
                "Records",
                "Storage",
                "Operations",
                "",
                "g Folders",
                "c All chats",
                "t Forum topics",
                "d Databases",
                "h Object history",
                "s Record storage",
                "a Attachments",
                "",
                "e Export",
                "r Refresh",
                "? Help",
            ]
            .map(ListItem::new);
            let mut state = ListState::default().with_selected(Some(self.view));
            f.render_stateful_widget(
                List::new(items)
                    .block(block("Navigate", self.focus == 0))
                    .highlight_style(Style::default().fg(Color::Cyan)),
                panes[0],
                &mut state,
            );
        }
        if visible(1) {
            let title = format!(
                "{}{}{}",
                clean(&self.screen.title),
                if self.screen.live { " [live]" } else { "" },
                if self.screen.next.is_some() {
                    " · n: more"
                } else {
                    ""
                }
            );
            let items = self
                .screen
                .entries
                .iter()
                .map(|e| ListItem::new(clean(&e.label)))
                .collect::<Vec<_>>();
            self.list_state
                .select((!items.is_empty()).then_some(self.screen.selected));
            f.render_stateful_widget(
                List::new(items)
                    .block(block(&title, self.focus == 1))
                    .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White)),
                panes[1],
                &mut self.list_state,
            );
            if self.screen.entries.is_empty() {
                f.render_widget(Paragraph::new(if self.loading{"Loading…"}else{"No items on this page.\nUse n if more pages are available, or r to refresh."}).wrap(Wrap{trim:false}),panes[1].inner(ratatui::layout::Margin{horizontal:1,vertical:1}));
            }
        }
        if visible(2) {
            f.render_widget(
                block(
                    "Details · Enter expands · a attachments · p preview",
                    self.focus == 2,
                ),
                panes[2],
            );
            if let Some(image) = &self.image {
                f.render_widget(Image::new(image), self.image_area);
            } else if let Some(text) = &self.scalar_text {
                f.render_widget(
                    Paragraph::new(clean(text))
                        .wrap(Wrap { trim: false })
                        .scroll((self.scalar_scroll, 0)),
                    self.image_area,
                );
            } else {
                let lines = self.tree();
                let items = lines
                    .into_iter()
                    .map(|(_, s, _)| ListItem::new(clean(&s)))
                    .collect::<Vec<_>>();
                self.detail_state
                    .select((!items.is_empty()).then_some(self.tree_selected));
                f.render_stateful_widget(
                    List::new(items).highlight_style(Style::default().fg(Color::Cyan)),
                    self.image_area,
                    &mut self.detail_state,
                );
            }
        }
        let status = format!(
            "{}{}\nTab panes · Enter open · / search · f filters · e export · n next · Esc back · ? help · q quit",
            if self.loading { "Loading… " } else { "" },
            clean(&self.notice)
        );
        f.render_widget(Paragraph::new(status).wrap(Wrap { trim: false }), rows[2]);
        if self.help {
            let area = modal(f.area(), 85, 27);
            f.render_widget(Clear, area);
            f.render_widget(
                Paragraph::new(HELP)
                    .block(block("Help — any key closes", true))
                    .wrap(Wrap { trim: false }),
                area,
            );
        }
        if let Some(form) = &self.form {
            let area = modal(f.area(), 100, (form.fields.len() * 2 + 5) as u16);
            f.render_widget(Clear, area);
            let mut lines = vec![];
            for (i, (label, value)) in form.fields.iter().enumerate() {
                lines.push(format!(
                    "{} {}",
                    if i == form.selected { "▶" } else { " " },
                    label
                ));
                lines.push(format!(
                    "  {}{}",
                    clean(value),
                    if i == form.selected { "▏" } else { "" }
                ));
            }
            lines.push(
                "Tab / Shift-Tab changes field · Ctrl-U clears · Enter submits · Esc cancels"
                    .into(),
            );
            f.render_widget(
                Paragraph::new(lines.join("\n"))
                    .block(block(&form.title, true))
                    .wrap(Wrap { trim: false })
                    .scroll((
                        (form.selected * 2).saturating_sub(area.height.saturating_sub(6) as usize)
                            as u16,
                        0,
                    )),
                area,
            );
        }
    }
}
impl Drop for App {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(task) = self.pending.take() {
            task.abort();
        }
        if let Some(task) = self.export_task.take() {
            task.abort();
        }
    }
}
fn clean(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_control() && c != '\n' && c != '\t' {
                '�'
            } else {
                c
            }
        })
        .collect()
}
fn block(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        }))
}
fn modal(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}
fn tree_lines(
    value: &Value,
    path: &str,
    name: &str,
    depth: usize,
    expanded: &BTreeSet<String>,
    out: &mut Vec<(String, String, bool)>,
) {
    if out.len() >= 5000 {
        return;
    }
    let children = match value {
        Value::Object(v) => v.len(),
        Value::Array(v) => v.len(),
        _ => 0,
    };
    let open = expanded.contains(path);
    let indent = "  ".repeat(depth.min(32));
    let summary = match value {
        Value::Object(_) => format!("{{{children} fields}}"),
        Value::Array(_) => format!("[{children} items]"),
        _ => value.to_string().chars().take(1024).collect(),
    };
    out.push((
        path.into(),
        format!(
            "{indent}{} {name} {summary}",
            if children > 0 {
                if open { "▾" } else { "▸" }
            } else {
                " "
            }
        ),
        children > 0,
    ));
    if !open {
        return;
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let key = k.replace('~', "~0").replace('/', "~1");
                tree_lines(v, &format!("{path}/{key}"), k, depth + 1, expanded, out);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                tree_lines(
                    v,
                    &format!("{path}/{i}"),
                    &i.to_string(),
                    depth + 1,
                    expanded,
                    out,
                );
            }
        }
        _ => {}
    }
}
fn binary_label(r: &BinaryRef) -> String {
    match r {
        BinaryRef::Attachment { hash } => format!("Attachment {hash}"),
        BinaryRef::Payload { hash } => format!("TL payload {hash}"),
        BinaryRef::Cell {
            database,
            table,
            row,
            column,
            ..
        } => format!("{database}/{table}/{row}/{column}"),
    }
}
fn record_entry(record: Record) -> Entry {
    let sender = record
        .data
        .get("from_id")
        .map(Value::to_string)
        .unwrap_or_else(|| "unknown sender".into());
    let text = tg_backup_protocol::text(&record.data);
    let date = &record.data["date"];
    let reply = &record.data["reply_to"];
    let mut detail = serde_json::to_value(&record).unwrap_or(Value::Null);
    tg_backup_protocol::public_json(&mut detail);
    let mut e = Entry::new(
        &record.key,
        format!(
            "{} · {sender} · {date}{}{}\n{}{}",
            record.key,
            if record.deleted { " [deleted]" } else { "" },
            if record.partial { " [partial]" } else { "" },
            if text.is_empty() {
                record.data["_"].as_str().unwrap_or("record").into()
            } else {
                text.chars().take(240).collect::<String>()
            },
            if reply.is_null() {
                String::new()
            } else {
                format!(" ↩ {reply}")
            }
        ),
        detail,
    );
    e.binaries = record
        .media_hashes(MediaSelection::All)
        .into_iter()
        .map(|hash| BinaryRef::Attachment { hash })
        .collect();
    e.record = Some(record);
    e
}
fn operations() -> Screen {
    let entries = ["status", "jobs", "coverage", "media", "work", "maintenance"]
        .iter()
        .map(|name| {
            let mut e = Entry::new(*name, *name, Value::Null);
            e.open = Some(Browse::Operations {
                name: (*name).into(),
            });
            e
        })
        .collect();
    Screen {
        title: "Operations (read-only)".into(),
        source: ViewSource::Static,
        entries,
        selected: 0,
        next: None,
        live: true,
    }
}
fn decode_image(bytes: Vec<u8>, picker: Picker, area: Rect) -> Result<Protocol> {
    let mut reader = image::ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    ensure!(
        matches!(
            reader.format(),
            Some(
                image::ImageFormat::Jpeg
                    | image::ImageFormat::Png
                    | image::ImageFormat::Gif
                    | image::ImageFormat::WebP
            )
        ),
        "unsupported preview format"
    );
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(10000);
    limits.max_image_height = Some(10000);
    limits.max_alloc = Some(160 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode()?;
    ensure!(
        u64::from(image.width()) * u64::from(image.height()) <= 40_000_000,
        "image exceeds 40 MP preview limit"
    );
    Ok(picker.new_protocol(image, area, Resize::Fit(None))?)
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
        } else {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), DisableMouseCapture);
        ratatui::restore();
    }
}
pub async fn run(backend: Arc<dyn Backend>) -> Result<()> {
    ensure!(
        std::io::stdin().is_terminal() && std::io::stdout().is_terminal(),
        "TUI requires an interactive terminal; use query/export for pipes"
    );
    let capabilities = match backend.request(Request::Capabilities).await? {
        Response::Capabilities(c) => c.version >= 1 && c.storage && c.conversations,
        _ => false,
    };
    let mut terminal = ratatui::try_init()?;
    let _guard = TerminalGuard;
    execute!(std::io::stdout(), EnableMouseCapture)?;
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = execute!(std::io::stdout(), DisableMouseCapture);
        ratatui::restore();
        previous(info);
    }));
    let picker = Picker::from_query_stdio()
        .ok()
        .filter(|p| p.protocol_type() != ProtocolType::Halfblocks);
    let mut app = App::new(capabilities, picker);
    app.focus = 1;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (progress_tx, mut progress_rx) = mpsc::channel::<export::Progress>(1);
    app.load(backend.clone(), tx.clone());
    let mut tick = tokio::time::interval(Duration::from_millis(33));
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        terminal.draw(|f| app.draw(f))?;
        tokio::select! {
            _=tick.tick()=>{},
            _=&mut shutdown=>return Ok(()),
            message=rx.recv()=>if let Some(message)=message{match message{
                Message::Loaded(id,result) if id==app.request_id=>{app.loading=false;match result.and_then(|r|app.received(r)){Ok(())=>{},Err(e)=>app.notice=format!("{e:#} · r refresh / Esc back")}},
                Message::Exported(result)=>{app.export_task=None;app.notice=match result{Ok(p)=>format!("Export complete: {} records, {} bytes",p.records,p.bytes),Err(e)=>format!("Export incomplete: {e:#}")};},
                Message::Image(id,result) if id==app.request_id=>{app.loading=false;match result{Ok(image)=>{app.image=Some(image);app.focus=2;},Err(e)=>app.notice=format!("Preview unavailable: {e:#}")}},_=>{}
            }},
            update=progress_rx.recv()=>if let Some(p)=update && app.export_task.is_some(){app.notice=format!("Export: {} records, {} bytes · Esc cancels",p.records,p.bytes);},
        }
        while event::poll(Duration::ZERO)? {
            let result = match event::read()? {
                Event::Key(key) => app.key(key, backend.clone(), tx.clone(), progress_tx.clone()),
                Event::Resize(_, _) => {
                    app.image = None;
                    app.notice = "Resized; press p to regenerate an image preview".into();
                    Ok(false)
                }
                Event::Mouse(mouse) => {
                    if app.form.is_none() && !app.help {
                        if app.panes[0] != app.panes[1]
                            && let Some(i) = app
                                .panes
                                .iter()
                                .position(|r| r.contains((mouse.column, mouse.row).into()))
                        {
                            app.focus = i;
                        }
                        match mouse.kind {
                            MouseEventKind::ScrollDown => app.shift(3),
                            MouseEventKind::ScrollUp => app.shift(-3),
                            MouseEventKind::Down(event::MouseButton::Left) => {
                                let pane = app.panes[app.focus];
                                let row = mouse.row.saturating_sub(pane.y + 1) as usize;
                                if app.focus == 0 && app.view == 0 && app.sidebar.is_some() {
                                    if let Some(sidebar) = &mut app.sidebar {
                                        sidebar.selected = (app.sidebar_state.offset() + row)
                                            .min(sidebar.entries.len().saturating_sub(1));
                                    }
                                    if let Err(e) = app.key(
                                        KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
                                        backend.clone(),
                                        tx.clone(),
                                        progress_tx.clone(),
                                    ) {
                                        app.notice = format!("{e:#}");
                                    }
                                } else if app.focus == 0 && row < 4 {
                                    let key = KeyEvent::new(
                                        KeyCode::Char((b'1' + row as u8) as char),
                                        KeyModifiers::NONE,
                                    );
                                    if let Err(e) = app.key(
                                        key,
                                        backend.clone(),
                                        tx.clone(),
                                        progress_tx.clone(),
                                    ) {
                                        app.notice = format!("{e:#}");
                                    }
                                } else if app.focus == 1 {
                                    let mut line = 0;
                                    for (index, entry) in app
                                        .screen
                                        .entries
                                        .iter()
                                        .enumerate()
                                        .skip(app.list_state.offset())
                                    {
                                        line += entry.label.lines().count().max(1);
                                        if row < line {
                                            app.screen.selected = index;
                                            break;
                                        }
                                    }
                                    app.scalar_text = None;
                                    if let Some(task) = app.pending.take() {
                                        task.abort();
                                    }
                                    app.request_id += 1;
                                    app.loading = false;
                                    app.image = None;
                                    app.remember();
                                } else if app.focus == 2 {
                                    app.tree_selected = (app.detail_state.offset() + row)
                                        .min(app.tree().len().saturating_sub(1));
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(false)
                }
                _ => Ok(false),
            };
            match result {
                Ok(true) => {
                    app.cancel.store(true, Ordering::Relaxed);
                    if let Some(task) = app.pending.take() {
                        task.abort();
                    }
                    if let Some(task) = app.export_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    return Ok(());
                }
                Ok(false) => {}
                Err(e) => app.notice = format!("{e:#}"),
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_sequences_are_inert() {
        assert_eq!(clean("a\x1b]52;secret\x07"), "a�]52;secret�");
    }
    #[test]
    fn tree_expansion_and_paths() {
        let v = json!({"a/b":[1,2]});
        let mut rows = vec![];
        tree_lines(
            &v,
            "",
            "",
            0,
            &BTreeSet::from([String::new(), "/a~1b".into()]),
            &mut rows,
        );
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[2].0, "/a~1b/0");
    }
    #[test]
    fn rendering_empty_and_narrow() {
        let backend = ratatui::backend::TestBackend::new(50, 15);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut app = App::new(true, None);
        app.focus = 1;
        terminal.draw(|f| app.draw(f)).unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        assert!(text.contains("No items"));
    }
    #[test]
    fn export_chat_removes_topic() {
        let mut app = App::new(true, None);
        app.screen = Screen::browse(
            "topic",
            Browse::Messages {
                peer: "channel:1".into(),
                topic: Some("2".into()),
            },
        );
        let Source::Query(q) = app.export_source("chat").unwrap() else {
            panic!()
        };
        assert_eq!(q.peer.as_deref(), Some("channel:1"));
        assert!(q.topic.is_none());
    }
}

#[cfg(test)]
mod interaction_tests {
    use super::*;
    struct PendingBackend;
    impl Backend for PendingBackend {
        fn request(&self, _: Request) -> crate::BackendFuture<'_> {
            Box::pin(std::future::pending())
        }
    }
    #[tokio::test]
    async fn navigation_aborts_obsolete_loads_and_preserves_back_state() {
        let mut app = App::new(true, None);
        let (tx, _) = mpsc::unbounded_channel();
        app.screen.selected = 3;
        app.load(Arc::new(PendingBackend), tx);
        let abort = app.pending.as_ref().unwrap().abort_handle();
        let generation = app.request_id;
        app.navigate(operations());
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
        assert!(app.request_id > generation);
        assert!(!app.loading);
        assert_eq!(app.history.last().unwrap().selected, 3);
    }
    #[tokio::test]
    async fn filters_keyboard_navigation_and_preview_fallback() {
        let backend: Arc<dyn Backend> = Arc::new(PendingBackend);
        let (tx, _) = mpsc::unbounded_channel();
        let (progress, _) = mpsc::channel(1);
        let mut app = App::new(true, None);
        app.key(
            KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE),
            backend.clone(),
            tx.clone(),
            progress.clone(),
        )
        .unwrap();
        let form = app.form.as_mut().unwrap();
        form.fields[3].1 = "CustomKind".into();
        form.fields[5].1 = "true".into();
        app.key(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            backend.clone(),
            tx.clone(),
            progress.clone(),
        )
        .unwrap();
        let ViewSource::Query(q) = &app.screen.source else {
            panic!()
        };
        assert_eq!(q.kind.as_deref(), Some("CustomKind"));
        assert!(q.all_versions);
        let error = app
            .key(
                KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE),
                backend,
                tx,
                progress,
            )
            .unwrap_err();
        assert!(error.to_string().contains("protocol unavailable"));
    }
    #[test]
    fn switching_abstractions_follows_the_newly_selected_record() {
        fn entry(sequence: i64) -> Entry {
            record_entry(serde_json::from_value(json!({"sequence":sequence,"key":format!("user:1/message:{sequence}"),"kind":"message","observed_at":1,"source":"fixture","payload_hash":"hash","root_type":"Item","schema_hash":"schema","partial":false,"deleted":false,"transformed":false,"metadata":{},"data":{"message":"text"}})).unwrap())
        }
        let mut app = App::new(true, None);
        app.screen.entries = vec![entry(1)];
        app.root(1);
        let ViewSource::Query(q) = &app.screen.source else {
            panic!()
        };
        assert_eq!(q.key.as_deref(), Some("user:1/message:1"));
        app.root(0);
        app.screen.entries = vec![entry(2)];
        app.root(1);
        let ViewSource::Query(q) = &app.screen.source else {
            panic!()
        };
        assert_eq!(q.key.as_deref(), Some("user:1/message:2"));
        app.root(2);
        let ViewSource::Browse(q) = &app.screen.source else {
            panic!()
        };
        assert!(matches!(q.target, Browse::Location { sequence: 2 }));
    }
    #[test]
    fn image_protocol_rendering_and_invalid_image_fallback() {
        let mut png = Cursor::new(vec![]);
        image::DynamicImage::new_rgba8(2, 2)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        for protocol in [
            ProtocolType::Kitty,
            ProtocolType::Iterm2,
            ProtocolType::Sixel,
        ] {
            let mut picker = Picker::halfblocks();
            picker.set_protocol_type(protocol);
            let image =
                decode_image(png.get_ref().clone(), picker, Rect::new(0, 0, 20, 10)).unwrap();
            let mut terminal =
                ratatui::Terminal::new(ratatui::backend::TestBackend::new(20, 10)).unwrap();
            terminal
                .draw(|f| f.render_widget(Image::new(&image), f.area()))
                .unwrap();
        }
        assert!(
            decode_image(
                b"not an image".to_vec(),
                Picker::halfblocks(),
                Rect::new(0, 0, 20, 10)
            )
            .is_err()
        );
    }
}
