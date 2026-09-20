use serde_json::json;
use tg_backup::{
    archive::{Archive, Capture},
    config::Config,
    tl::Schema,
};
use tg_backup_protocol::{Query, explorer::*};
const SCHEMA: &str = "item#12345678 flags:# id:long message:string = Item;";
fn capture(peer: &str, id: i64, kind: &str, text: &str, topic: Option<i64>, time: i64) -> Capture {
    Capture {
        key: format!("{peer}/{kind}:{id}"),
        kind: kind.into(),
        root_type: "Item".into(),
        bytes: Schema::parse(SCHEMA)
            .unwrap()
            .encode(
                "Item",
                &json!({"_":"item","id":id.to_string(),"message":text}),
            )
            .unwrap(),
        observed_at: time,
        source: "fixture".into(),
        metadata: json!({"peer":peer,"topic":topic,"revision":time}),
        replay_key: None,
        partial: false,
        deleted: false,
    }
}
fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let time = 1_800_000_000_000_000;
    a.ingest(
        &schema,
        &[
            capture("channel:1", 30, "message", "newest", Some(10), time),
            capture("channel:1", 10, "message", "topic root", None, time + 1),
            capture("channel:1", 20, "message", "older", Some(10), time + 2),
            capture(
                "channel:1",
                40,
                "message",
                "another topic",
                Some(40),
                time + 3,
            ),
            capture("user:2", 1, "message", "private", None, time + 4),
            capture("channel:1", 20, "message", "edited", Some(10), time + 5),
            capture("channel:1", 10, "topic", "forum topic", None, time + 6),
            capture("misc", 1, "CustomKind", "unusual", None, time + 7),
        ],
        None,
    )
    .unwrap();
    dir
}
#[test]
fn message_order_topic_roots_and_snapshot_pagination() {
    let dir = fixture();
    let a = Archive::open(dir.path(), false).unwrap();
    let mut request = BrowseRequest::new(Browse::Messages {
        peer: "channel:1".into(),
        topic: Some("10".into()),
    });
    request.limit = 1;
    let first = a.explore(&request).unwrap();
    assert!(first.entries[0].id.ends_with("message:30"));
    request.cursor = first.next_cursor;
    drop(a);
    {
        let mut writer = Archive::open(dir.path(), true).unwrap();
        let schema = writer.register_schema(1, SCHEMA).unwrap();
        writer
            .ingest(
                &schema,
                &[capture(
                    "channel:1",
                    50,
                    "message",
                    "arrived later",
                    Some(10),
                    1_800_000_000_001_000,
                )],
                None,
            )
            .unwrap();
    }
    let a = Archive::open(dir.path(), false).unwrap();
    let second = a.explore(&request).unwrap();
    assert!(second.entries[0].id.ends_with("message:20"));
    assert_eq!(
        second.entries[0].record.as_ref().unwrap().data["message"],
        "edited"
    );
    request.cursor = second.next_cursor;
    let third = a.explore(&request).unwrap();
    assert!(third.entries[0].id.ends_with("message:10"));
    let q = Query {
        peer: Some("channel:1".into()),
        topic: Some("10".into()),
        ..Query::default()
    };
    assert_eq!(a.query(&q).unwrap().records.len(), 4);
}
#[test]
fn missing_dialogs_unknown_kinds_and_raw_types() {
    let dir = fixture();
    let a = Archive::open(dir.path(), false).unwrap();
    let chats = a
        .explore(&BrowseRequest::new(Browse::Conversations { folder: None }))
        .unwrap();
    assert_eq!(chats.entries.len(), 2);
    assert_eq!(
        a.query(&Query {
            kind: Some("CustomKind".into()),
            ..Query::default()
        })
        .unwrap()
        .records
        .len(),
        1
    );
    let rows = a
        .explore(&BrowseRequest::new(Browse::Rows {
            database: "catalog".into(),
            table: "observations".into(),
            row: None,
        }))
        .unwrap();
    assert_eq!(rows.entries[0].detail["id"]["type"], "integer");
    assert_eq!(rows.entries[0].detail["metadata"]["type"], "text");
    assert!(rows.entries[0].detail["observed"]["value"].is_string());
    assert!(
        a.explore(&BrowseRequest::new(Browse::Tables {
            database: "../session.sqlite3".into()
        }))
        .is_err()
    );
    assert!(
        a.explore(&BrowseRequest::new(Browse::Rows {
            database: "catalog".into(),
            table: "sqlite_master".into(),
            row: None
        }))
        .is_err()
    );
}
#[test]
fn payload_bytes_schema_export_and_generation_validation() {
    let dir = fixture();
    let a = Archive::open(dir.path(), false).unwrap();
    let record = a.query(&Query::default()).unwrap().records.remove(0);
    let location = a
        .explore(&BrowseRequest::new(Browse::Location {
            sequence: record.sequence,
        }))
        .unwrap();
    let reference = location.entries[0].binaries[0].clone();
    let mut request = BinaryRequest {
        reference,
        offset: 0,
        limit: 7,
    };
    let mut bytes = vec![];
    loop {
        let p = a.explorer_binary(&request).unwrap();
        bytes.extend(hex::decode(p.hex).unwrap());
        if let Some(offset) = p.next_offset {
            request.offset = offset;
        } else {
            break;
        }
    }
    assert_eq!(bytes, a.payload(&record.payload_hash).unwrap());
    let schema = a
        .explore(&BrowseRequest::new(Browse::Rows {
            database: "catalog".into(),
            table: "schemas".into(),
            row: None,
        }))
        .unwrap();
    assert!(!schema.entries[0].binaries.is_empty());
    request.limit = BINARY_CHUNK + 1;
    assert!(a.explorer_binary(&request).is_err());
    let mut browse = BrowseRequest::new(Browse::Messages {
        peer: "channel:1".into(),
        topic: None,
    });
    browse.limit = 1;
    browse.cursor = a.explore(&browse).unwrap().next_cursor;
    drop(a);
    {
        let a = Archive::open(dir.path(), true).unwrap();
        a.set_checkpoint("maintenance_generation", &json!(12))
            .unwrap();
    }
    assert!(
        Archive::open(dir.path(), false)
            .unwrap()
            .explore(&browse)
            .is_err()
    );
}
#[tokio::test]
async fn authenticated_http_parity() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let dir = fixture();
    let router = tg_backup::http::router(tg_backup::http::ApiState {
        root: dir.path().into(),
        token: Some("test-token".into()),
    });
    let request = BrowseRequest::new(Browse::Messages {
        peer: "channel:1".into(),
        topic: None,
    });
    let denied = router
        .clone()
        .oneshot(
            Request::post("/v2/explorer/browse")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let response = router
        .oneshot(
            Request::post("/v2/explorer/browse")
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let remote: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    let tg_backup_protocol::explorer::Response::Browse(local) = tg_backup::explorer::execute(
        dir.path(),
        tg_backup_protocol::explorer::Request::Browse(request),
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(remote, serde_json::to_value(local).unwrap());
}
#[tokio::test]
async fn exports_follow_pages_and_do_not_overwrite() {
    use std::sync::{Arc, atomic::AtomicBool};
    use tg_backup_tui::export::{Options, Source};
    let dir = fixture();
    let output = tempfile::tempdir().unwrap();
    let backend = Arc::new(tg_backup::explorer::LocalBackend {
        root: dir.path().into(),
    });
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let options = Options {
        output: output.path().join("archive.json"),
        format: tg_backup_protocol::Format::Json,
        attachments: None,
        media: tg_backup_protocol::MediaSelection::Original,
        overwrite: false,
    };
    let source = Source::Query(Query {
        limit: 1,
        all_versions: true,
        ..Query::default()
    });
    let result = tg_backup_tui::export::run(
        backend.clone(),
        source.clone(),
        options.clone(),
        Arc::new(AtomicBool::new(false)),
        tx.clone(),
    )
    .await
    .unwrap();
    assert_eq!(result.records, 8);
    let values: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(&options.output).unwrap()).unwrap();
    assert_eq!(values.len(), 8);
    assert!(
        tg_backup_tui::export::run(
            backend.clone(),
            source.clone(),
            options.clone(),
            Arc::new(AtomicBool::new(false)),
            tx.clone()
        )
        .await
        .is_err()
    );
    let cancelled = Options {
        output: output.path().join("cancelled.json"),
        ..options
    };
    assert!(
        tg_backup_tui::export::run(
            backend,
            source,
            cancelled.clone(),
            Arc::new(AtomicBool::new(true)),
            tx
        )
        .await
        .is_err()
    );
    assert!(!cancelled.output.exists());
    assert_eq!(std::fs::read_dir(output.path()).unwrap().count(), 1);
}

#[test]
fn missing_topic_metadata_and_live_table_search_are_browsable() {
    let dir = fixture();
    let a = Archive::open(dir.path(), false).unwrap();
    let topics = a
        .explore(&BrowseRequest::new(Browse::Topics {
            peer: "channel:1".into(),
        }))
        .unwrap();
    assert_eq!(topics.entries.len(), 2);
    assert!(
        topics
            .entries
            .iter()
            .any(|e| e.label.contains("40") && e.label.contains("unavailable"))
    );
    let mut req = BrowseRequest::new(Browse::Rows {
        database: "catalog".into(),
        table: "observations".into(),
        row: None,
    });
    req.limit = 1;
    req.search = "user:2".into();
    let rows = a.explore(&req).unwrap();
    assert_eq!(rows.entries.len(), 1);
    assert!(
        rows.entries[0].detail["key"]["value"]
            .as_str()
            .unwrap()
            .contains("user:2")
    );
}

#[test]
fn sealed_epoch_binary_links_and_reader_lifetime() {
    let dir = fixture();
    {
        let mut writer = Archive::open(dir.path(), true).unwrap();
        writer
            .maintain(&tg_backup::archive::Maintenance {
                apply: true,
                seal: true,
                ..Default::default()
            })
            .unwrap();
    }
    let record = {
        let a = Archive::open(dir.path(), false).unwrap();
        a.query(&Query::default()).unwrap().records.remove(0)
    };
    let Response::Browse(page) = tg_backup::explorer::execute(
        dir.path(),
        Request::Browse(BrowseRequest::new(Browse::Location {
            sequence: record.sequence,
        })),
    )
    .unwrap() else {
        panic!()
    };
    let block = page
        .entries
        .iter()
        .find(|e| e.label == "Compressed block")
        .unwrap();
    let Response::Browse(blocks) = tg_backup::explorer::execute(
        dir.path(),
        Request::Browse(BrowseRequest::new(block.open.clone().unwrap())),
    )
    .unwrap() else {
        panic!()
    };
    let reference = blocks.entries[0]
        .binaries
        .iter()
        .find(|r| matches!(r,BinaryRef::Cell{column,..} if column=="data"))
        .unwrap()
        .clone();
    let Response::Binary(bytes) = tg_backup::explorer::execute(
        dir.path(),
        Request::Binary(BinaryRequest {
            reference,
            offset: 0,
            limit: BINARY_CHUNK,
        }),
    )
    .unwrap() else {
        panic!()
    };
    assert!(bytes.total > 0);
    // The local backend does not retain a reader lease between calls.
    use fs2::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join("readers.lock"))
        .unwrap();
    file.try_lock_exclusive().unwrap();
    FileExt::unlock(&file).unwrap();
}

#[tokio::test]
async fn attachment_exports_verify_and_missing_files_never_publish() {
    use std::sync::{Arc, atomic::AtomicBool};
    use tg_backup_tui::export::{Options, Source};
    let dir = fixture();
    let bytes = b"retained attachment bytes";
    let (mut record, hash) = {
        let a = Archive::open(dir.path(), true).unwrap();
        let record = a.query(&Query::default()).unwrap().records.remove(0);
        a.queue_media(
            "test-file",
            &json!({"_":"inputDocumentFileLocation"}),
            1,
            Some(bytes.len() as u64),
            record.sequence,
        )
        .unwrap();
        a.append_media("test-file", 0, bytes).unwrap();
        let hash = a.finish_media("test-file").unwrap();
        (record, hash)
    };
    record.attachments = vec![hash.clone()];
    let backend = Arc::new(tg_backup::explorer::LocalBackend {
        root: dir.path().into(),
    });
    let output = tempfile::tempdir().unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let options = Options {
        output: output.path().join("record.json"),
        format: tg_backup_protocol::Format::Json,
        attachments: Some(output.path().join("files")),
        media: tg_backup_protocol::MediaSelection::All,
        overwrite: false,
    };
    tg_backup_tui::export::run(
        backend.clone(),
        Source::Record(Box::new(record.clone())),
        options.clone(),
        Arc::new(AtomicBool::new(false)),
        tx.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(output.path().join("files").join(&hash)).unwrap(),
        bytes
    );
    let info = Archive::open(dir.path(), false)
        .unwrap()
        .explore(&BrowseRequest::new(Browse::Attachment {
            hash: hash.clone(),
        }))
        .unwrap();
    assert_eq!(info.entries[0].detail["available"], true);
    std::fs::remove_file(tg_backup::media::attachment_path(dir.path(), &hash).unwrap()).unwrap();
    let options = Options {
        output: output.path().join("missing.json"),
        attachments: Some(output.path().join("missing-files")),
        ..options
    };
    let error = tg_backup_tui::export::run(
        backend,
        Source::Record(Box::new(record)),
        options.clone(),
        Arc::new(AtomicBool::new(false)),
        tx,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("attachment"));
    assert!(!options.output.exists());
}
