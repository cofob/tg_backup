use serde_json::{Value, json};
use tg_backup::{
    archive::{Archive, Capture},
    config::Config,
    graph::{self, Options},
    tl::Schema,
};
const SCHEMA: &str = "
user#10000001 flags:# self:flags.0?true bot:flags.1?true id:long first_name:string last_name:string = User;
channel#10000002 flags:# megagroup:flags.0?true id:long title:string = Chat;
peerUser#10000003 user_id:long = Peer;
peerChannel#10000004 channel_id:long = Peer;
messageReplyHeader#10000005 flags:# reply_to_msg_id:int reply_to_peer_id:flags.0?Peer = MessageReplyHeader;
messageEntityMentionName#10000006 user_id:long = MessageEntity;
message#10000007 flags:# out:flags.0?true id:int peer_id:Peer from_id:flags.1?Peer date:flags.2?int edit_date:flags.3?int reply_to:flags.4?MessageReplyHeader entities:Vector<MessageEntity> message:string = Message;
channelParticipant#10000008 user_id:long date:int = ChannelParticipant;
channelParticipantLeft#10000009 peer:Peer = ChannelParticipant;
channels.channelParticipants#1000000a participants:Vector<ChannelParticipant> = channels.ChannelParticipants;
";
fn opts() -> Options {
    Options {
        focus: "user:1".into(),
        from: None,
        to: None,
        output: PathBuf::from("unused.html"),
        overwrite: false,
    }
}
use std::path::PathBuf;
fn capture(key: &str, kind: &str, root: &str, data: Value, observed: i64) -> Capture {
    Capture {
        key: key.into(),
        kind: kind.into(),
        root_type: root.into(),
        bytes: Schema::parse(SCHEMA).unwrap().encode(root, &data).unwrap(),
        observed_at: observed,
        source: "fixture".into(),
        metadata: json!({"scope":"channel:10/channelParticipantsRecent/page:0","revision":observed}),
        replay_key: None,
        partial: false,
        deleted: false,
    }
}
fn user(id: i64, label: &str) -> Capture {
    capture(
        &format!("user:{id}"),
        "user",
        "User",
        json!({"_":"user","id":id,"first_name":label,"last_name":""}),
        1,
    )
}
fn msg(peer: &str, id: i64, sender: Option<i64>, date: Option<i64>) -> Capture {
    let (kind, pid) = peer.split_once(':').unwrap();
    let p = if kind == "user" {
        json!({"_":"peerUser","user_id":pid})
    } else {
        json!({"_":"peerChannel","channel_id":pid})
    };
    let mut data = json!({"_":"message","id":id,"peer_id":p,"entities":[],"message":"PRIVATE TEXT MUST NOT APPEAR","date":date});
    if let Some(sender) = sender {
        data["from_id"] = json!({"_":"peerUser","user_id":sender});
    }
    capture(
        &format!("{peer}/message:{id}"),
        "message",
        "Message",
        data,
        1_800_000_000_000_000 + id,
    )
}
fn change(c: &mut Capture, f: impl FnOnce(&mut Value)) {
    let schema = Schema::parse(SCHEMA).unwrap();
    let mut data = schema.decode(&c.root_type, &c.bytes).unwrap();
    f(&mut data);
    c.bytes = schema.encode(&c.root_type, &data).unwrap();
}
fn fixture(records: Vec<Capture>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    a.set_checkpoint("account_id", &json!("1")).unwrap();
    let hash = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(&hash, &records, None).unwrap();
    dir
}
fn count(g: &graph::Graph, kind: &str) -> u64 {
    g.edges
        .iter()
        .filter(|e| e.kind == kind)
        .flat_map(|e| e.days.values())
        .sum()
}
#[test]
fn date_bounds_edits_reply_resolution_and_mentions() {
    let day = 1_740_787_200; // 2025-03-01 UTC
    let old = msg("channel:10", 1, Some(2), Some(day - 1));
    let mut reply = msg("channel:10", 2, Some(1), Some(day));
    change(&mut reply, |v| {
        v["reply_to"] = json!({"_":"messageReplyHeader","reply_to_msg_id":1});
        v["entities"] = json!([{"_":"messageEntityMentionName","user_id":"9007199254740993"},{"_":"messageEntityMentionName","user_id":"9007199254740993"}]);
        v["edit_date"] = json!(day + 86400);
    });
    let mut edited = reply.clone();
    edited.observed_at += 100;
    edited.metadata["revision"] = json!(edited.observed_at);
    let dir = fixture(vec![
        user(1, "Alice"),
        old,
        reply,
        edited,
        msg("channel:10", 3, Some(1), Some(day + 86399)),
        msg("channel:10", 4, Some(1), Some(day + 86400)),
        msg("channel:10", 5, Some(1), None),
    ]);
    let a = Archive::open(dir.path(), false).unwrap();
    let mut o = opts();
    o.from = Some("2025-03-01".parse().unwrap());
    o.to = o.from;
    let g = graph::build(&a, &o).unwrap();
    assert_eq!(count(&g, "participation"), 2);
    assert_eq!(count(&g, "reply"), 1);
    assert_eq!(count(&g, "mention"), 1);
    assert_eq!(g.skipped.undated, 1);
    assert!(g.nodes.iter().any(|n| n.id == "user:9007199254740993"));
}
#[test]
fn membership_observation_dates_deduplication_and_components() {
    let day = 1_740_787_200;
    let data = json!({"_":"channels.channelParticipants","participants":[{"_":"channelParticipant","user_id":1,"date":0},{"_":"channelParticipant","user_id":2,"date":0},{"_":"channelParticipantLeft","peer":{"_":"peerUser","user_id":3}}]});
    let one = capture(
        "rpc:one",
        "rpc",
        "channels.ChannelParticipants",
        data.clone(),
        day * 1_000_000,
    );
    let two = capture(
        "rpc:two",
        "rpc",
        "channels.ChannelParticipants",
        data,
        (day + 1) * 1_000_000,
    );
    let dir = fixture(vec![user(1, "Alice"), user(99, "Disconnected"), one, two]);
    let a = Archive::open(dir.path(), false).unwrap();
    let mut o = opts();
    o.from = Some("2025-03-01".parse().unwrap());
    o.to = o.from;
    let g = graph::build(&a, &o).unwrap();
    assert_eq!(count(&g, "membership"), 2);
    assert_eq!(g.nodes.len(), 3);
    assert!(
        g.nodes
            .iter()
            .all(|n| n.id != "user:3" && n.id != "user:99")
    );
    o.from = Some("2025-03-02".parse().unwrap());
    o.to = o.from;
    let g = graph::build(&a, &o).unwrap();
    assert_eq!(g.nodes.len(), 1);
    assert!(g.edges.is_empty());
    o.focus = "user:999".into();
    assert!(graph::build(&a, &o).is_err());
}
#[test]
fn private_direction_exclusions_missing_identity_and_escaping() {
    let mut out = msg("user:2", 1, None, Some(0));
    change(&mut out, |v| v["out"] = json!(true));
    let incoming = msg("user:2", 2, None, Some(0));
    let mut deleted = msg("user:2", 3, Some(1), Some(0));
    deleted.deleted = true;
    let mut scheduled = msg("user:2", 4, Some(1), Some(0));
    scheduled.kind = "scheduled_message".into();
    scheduled.key = "user:2/scheduled_message:4".into();
    let dir = fixture(vec![
        user(1, "</script><script>alert(1)</script>"),
        out,
        incoming,
        deleted,
        scheduled,
        msg("channel:10", 5, None, Some(0)),
    ]);
    let a = Archive::open(dir.path(), false).unwrap();
    let g = graph::build(&a, &opts()).unwrap();
    assert_eq!(count(&g, "direct"), 2);
    assert_eq!(g.skipped.unresolved, 1);
    assert!(
        g.edges
            .iter()
            .any(|e| e.source == "user:1" && e.target == "user:2")
    );
    assert!(
        g.edges
            .iter()
            .any(|e| e.source == "user:2" && e.target == "user:1")
    );
    let html = graph::html(&g).unwrap();
    assert!(!html.contains("PRIVATE TEXT"));
    assert!(!html.contains("</script><script>alert"));
    assert!(html.contains("\\u003c/script"));
    drop(a);
    let a = Archive::open(dir.path(), true).unwrap();
    a.db.execute("DELETE FROM checkpoints WHERE key='account_id'", [])
        .unwrap();
    drop(a);
    let a = Archive::open(dir.path(), false).unwrap();
    let g = graph::build(&a, &opts()).unwrap();
    assert_eq!(count(&g, "direct"), 0);
    assert_eq!(g.skipped.unresolved, 3);
}
#[test]
fn snapshot_publication_and_invalid_ranges() {
    let dir = fixture(vec![user(1, "Alice")]);
    let a = Archive::open(dir.path(), false).unwrap();
    let g = graph::build(&a, &opts()).unwrap();
    assert_eq!(g.nodes.len(), 1);
    {
        let mut writer = Archive::open(dir.path(), true).unwrap();
        let hash = writer.register_schema(1, SCHEMA).unwrap();
        writer
            .ingest(&hash, &[msg("user:2", 1, Some(1), Some(0))], None)
            .unwrap();
    }
    assert!(graph::build(&a, &opts()).unwrap().edges.is_empty());
    drop(a);
    let mut o = opts();
    o.output = dir.path().join("graph.html");
    graph::run(dir.path(), &o).unwrap();
    assert!(graph::run(dir.path(), &o).is_err());
    o.overwrite = true;
    graph::run(dir.path(), &o).unwrap();
    let previous = std::fs::read(&o.output).unwrap();
    o.from = Some("2025-03-02".parse().unwrap());
    o.to = Some("2025-03-01".parse().unwrap());
    assert!(graph::run(dir.path(), &o).is_err());
    assert_eq!(std::fs::read(&o.output).unwrap(), previous);
    assert!(!std::fs::read_dir(dir.path()).unwrap().any(|p| {
        p.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tg-graph")
    }));
}

#[test]
fn leap_days_open_bounds_and_cross_peer_replies() {
    let day = 1_709_164_800; // 2024-02-29
    let mut reply = msg("channel:20", 2, Some(1), Some(day));
    change(
        &mut reply,
        |v| v["reply_to"] = json!({"_":"messageReplyHeader","reply_to_msg_id":1,"reply_to_peer_id":{"_":"peerChannel","channel_id":10}}),
    );
    let dir = fixture(vec![
        user(1, "Alice"),
        msg("channel:10", 1, Some(2), Some(day - 1)),
        reply,
        msg("channel:20", 3, Some(1), Some(day + 86400)),
    ]);
    let a = Archive::open(dir.path(), false).unwrap();
    let mut o = opts();
    o.to = Some("2024-02-29".parse().unwrap());
    let g = graph::build(&a, &o).unwrap();
    assert_eq!(count(&g, "reply"), 1);
    assert_eq!(count(&g, "participation"), 2);
    o.to = None;
    o.from = Some("2024-03-01".parse().unwrap());
    let g = graph::build(&a, &o).unwrap();
    assert_eq!(count(&g, "reply"), 0);
    assert_eq!(count(&g, "participation"), 1);
}

#[test]
fn cli_rejects_invalid_dates_and_accepts_leap_day() {
    use clap::{Parser, Subcommand};
    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }
    #[derive(Subcommand)]
    enum Command {
        Graph(Options),
    }
    for invalid in [
        "2025-02-29",
        "2024-2-29",
        "2024-02-30",
        "2024-02-29T00:00:00Z",
    ] {
        assert!(
            Cli::try_parse_from([
                "test", "graph", "--focus", "user:1", "--output", "x.html", "--from", invalid
            ])
            .is_err()
        );
    }
    assert!(
        Cli::try_parse_from([
            "test",
            "graph",
            "--focus",
            "user:1",
            "--output",
            "x.html",
            "--from",
            "2024-02-29"
        ])
        .is_ok()
    );
}

#[test]
fn peer_types_explicit_channel_authors_and_failed_publication() {
    let mut bot = user(2, "Helper");
    change(&mut bot, |v| v["bot"] = json!(true));
    let group = capture(
        "channel:10",
        "chat",
        "Chat",
        json!({"_":"channel","id":10,"title":"Group","megagroup":true}),
        1,
    );
    let broadcast = capture(
        "channel:20",
        "chat",
        "Chat",
        json!({"_":"channel","id":20,"title":"Broadcast"}),
        1,
    );
    let mut post = msg("channel:10", 1, Some(1), Some(0));
    change(&mut post, |v| {
        v["entities"] = json!([{"_":"messageEntityMentionName","user_id":2}])
    });
    let mut anonymous = msg("channel:10", 2, None, Some(0));
    change(&mut anonymous, |v| {
        v["from_id"] = json!({"_":"peerChannel","channel_id":20})
    });
    let dir = fixture(vec![
        user(1, "Alice"),
        bot,
        group,
        broadcast,
        post,
        anonymous,
    ]);
    let a = Archive::open(dir.path(), false).unwrap();
    let g = graph::build(&a, &opts()).unwrap();
    for (id, kind) in [
        ("user:2", "bot"),
        ("channel:10", "group"),
        ("channel:20", "channel"),
    ] {
        assert!(g.nodes.iter().any(|n| n.id == id && n.kind == kind));
    }
    assert!(
        g.edges
            .iter()
            .any(|e| e.source == "channel:20" && e.target == "channel:10")
    );
    drop(a);
    let mut o = opts();
    o.output = dir.path().join("existing_directory");
    o.overwrite = true;
    std::fs::create_dir(&o.output).unwrap();
    assert!(graph::run(dir.path(), &o).is_err());
    assert!(o.output.is_dir());
    assert!(!std::fs::read_dir(dir.path()).unwrap().any(|p| {
        p.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tg-graph")
    }));
}
