use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tg_backup::{
    archive::{Archive, Capture},
    config::Config,
    dialog_status::Options,
    http::{ApiState, router},
    tl::{self, Schema},
};
use tower::ServiceExt;

fn dialog_capture(schema: &Schema, key: &str) -> Capture {
    let (kind, id) = key.split_once(':').unwrap();
    let peer = match kind {
        "user" => json!({"_":"peerUser","user_id":id}),
        "chat" => json!({"_":"peerChat","chat_id":id}),
        "channel" => json!({"_":"peerChannel","channel_id":id}),
        _ => unreachable!(),
    };
    let value = json!({
        "_":"dialog", "peer":peer, "top_message":1,
        "read_inbox_max_id":0, "read_outbox_max_id":0,
        "unread_count":0, "unread_mentions_count":0,
        "unread_reactions_count":0, "unread_poll_votes_count":0,
        "notify_settings":{"_":"peerNotifySettings"}
    });
    Capture {
        key: format!("{key}/dialog"),
        kind: "dialog".into(),
        root_type: "Dialog".into(),
        bytes: schema.encode("Dialog", &value).unwrap(),
        observed_at: 1_700_000_000_000_000,
        source: "dialogs".into(),
        metadata: json!({}),
        replay_key: None,
        partial: false,
        deleted: false,
    }
}

fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut archive = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = Schema::current().unwrap();
    let schema_hash = archive.register_schema(tl::LAYER, tl::API_SCHEMA).unwrap();
    archive
        .db
        .execute_batch("CREATE TABLE peers(key TEXT PRIMARY KEY,input TEXT,metadata TEXT,raw TEXT)")
        .unwrap();
    for (key, raw) in [
        (
            "channel:1",
            json!({"_":"channel","id":"1","title":"Current Channel"}),
        ),
        (
            "channel:2",
            json!({"_":"channel","id":"2","title":"Community","megagroup":true}),
        ),
        ("chat:3", json!({"_":"chat","id":"3","title":"Old Group"})),
        (
            "user:4",
            json!({"_":"user","id":"4","first_name":"Current","last_name":"Person"}),
        ),
        ("user:5", json!({"_":"user","id":"5"})),
        (
            "user:6",
            json!({"_":"user","id":"6","first_name":"Unrelated"}),
        ),
    ] {
        archive
            .db
            .execute(
                "INSERT INTO peers VALUES(?1,'{}',?2,?3)",
                rusqlite::params![key, json!({"title":"STALE"}).to_string(), raw.to_string()],
            )
            .unwrap();
    }
    let dialogs = ["channel:1", "channel:2", "chat:3", "user:4", "user:5"]
        .iter()
        .map(|key| dialog_capture(&schema, key))
        .collect::<Vec<_>>();
    archive.ingest(&schema_hash, &dialogs, None).unwrap();
    archive
        .db
        .execute(
            "UPDATE peers SET metadata=?1 WHERE key='user:6'",
            [json!({"archived":true,"contact":true}).to_string()],
        )
        .unwrap();
    archive
        .coverage(
            "history:channel:1",
            "complete",
            &json!({"full_history":true,"completed_at":100}),
        )
        .unwrap();
    archive
        .coverage(
            "history:channel:2",
            "limited",
            &json!({"completed_at":200,"reason":"bounded scan"}),
        )
        .unwrap();
    archive
        .coverage("history:chat:3", "complete", &json!({"completed_at":300}))
        .unwrap();
    archive
        .coverage(
            "history:user:4",
            "incomplete",
            &json!({"error":"request failed"}),
        )
        .unwrap();
    archive
        .set_checkpoint("history_success:user:4", &json!(400))
        .unwrap();
    archive
        .coverage(
            "access:channel:2",
            "limited",
            &json!({"reason":"inaccessible older messages"}),
        )
        .unwrap();
    archive
        .coverage(
            "user:5",
            "complete",
            &json!({"method":"messages.getMessages"}),
        )
        .unwrap();
    archive
        .coverage(
            "history:user:6",
            "complete",
            &json!({"full_history":true,"completed_at":999}),
        )
        .unwrap();
    dir
}

#[test]
fn empty_archive_without_peer_cache_is_supported() {
    let dir = tempfile::tempdir().unwrap();
    let archive = Archive::init(dir.path(), &Config::default()).unwrap();
    let page = archive.dialog_status_page(&Options::default()).unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_cursor.is_none());
}

#[test]
fn live_dialog_envelope_exposes_individual_dialog_records() {
    let schema = Schema::current().unwrap();
    let capture = dialog_capture(&schema, "user:4");
    let dialog = schema.decode("Dialog", &capture.bytes).unwrap();
    let envelope = json!({
        "_":"messages.dialogs", "dialogs":[dialog],
        "messages":[], "chats":[], "users":[]
    });
    let bytes = schema.encode("messages.Dialogs", &envelope).unwrap();
    let (_, slices) = schema.decode_slices("messages.Dialogs", &bytes).unwrap();
    assert!(
        slices
            .iter()
            .any(|slice| { slice.root == "Dialog" && slice.value["peer"]["user_id"] == "4" })
    );
}

#[test]
fn restricted_access_downgrades_full_scan() {
    let dir = fixture();
    let archive = Archive::open(dir.path(), true).unwrap();
    archive
        .coverage(
            "access:channel:1",
            "limited",
            &json!({"reason":"only own messages accessible"}),
        )
        .unwrap();
    let page = archive
        .dialog_status_page(&Options {
            status: Some("limited".into()),
            ..Default::default()
        })
        .unwrap();
    let entry = page
        .items
        .iter()
        .find(|item| item.peer_id == "channel:1")
        .unwrap();
    assert_eq!(entry.history_backup_status, "limited");
    assert!(
        entry
            .coverage_gaps
            .contains(&"only own messages accessible".into())
    );
}

#[test]
fn paginated_projection_and_filters() {
    let dir = fixture();
    let archive = Archive::open(dir.path(), false).unwrap();
    let first = archive
        .dialog_status_page(&Options {
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        first.items[0].display_name.as_deref(),
        Some("Current Channel")
    );
    assert_eq!(first.items[0].history_backup_status, "complete");
    assert_eq!(first.items[0].last_successful_sync_at, Some(100));
    assert!(first.items[0].coverage_gaps.is_empty());
    assert_eq!(first.items[1].dialog_type, "group");
    assert_eq!(first.items[1].history_backup_status, "limited");
    assert!(
        first.items[1]
            .coverage_gaps
            .contains(&"inaccessible older messages".into())
    );
    let next = archive
        .dialog_status_page(&Options {
            after: first.next_cursor,
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(next.items[0].history_backup_status, "incomplete");
    assert!(next.items[0].coverage_gaps[0].contains("Legacy"));
    assert_eq!(
        next.items[1].display_name.as_deref(),
        Some("Current Person")
    );
    assert_eq!(next.items[1].errors, ["request failed"]);
    assert_eq!(next.items[1].last_successful_sync_at, Some(400));
    let last = archive
        .dialog_status_page(&Options {
            after: next.next_cursor,
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(last.items.len(), 1);
    assert_eq!(last.items[0].peer_id, "user:5");
    assert_eq!(last.items[0].history_backup_status, "not_started");
    assert_eq!(last.items[0].display_name, None);
    assert_eq!(last.next_cursor, None);
    assert!(
        archive
            .dialog_status_page(&Options {
                after: Some("user:5".into()),
                ..Default::default()
            })
            .unwrap()
            .items
            .is_empty()
    );
    let complete = archive
        .dialog_status_page(&Options {
            status: Some("complete".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(complete.items.len(), 1);
    assert_eq!(complete.items[0].peer_id, "channel:1");
    let filtered = archive
        .dialog_status_page(&Options {
            status: Some("incomplete".into()),
            dialog_type: Some("user".into()),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered.items.len(), 1);
    assert_eq!(filtered.items[0].peer_id, "user:4");
    assert!(filtered.next_cursor.is_none());
    assert!(
        archive
            .dialog_status_page(&Options {
                limit: Some(0),
                ..Default::default()
            })
            .is_err()
    );
    assert!(
        archive
            .dialog_status_page(&Options {
                status: Some("bogus".into()),
                ..Default::default()
            })
            .is_err()
    );
}

#[test]
fn exact_page_boundary_and_sparse_filtered_pages() {
    let dir = fixture();
    let mut archive = Archive::open(dir.path(), true).unwrap();
    let schema = Schema::current().unwrap();
    let schema_hash = archive.register_schema(tl::LAYER, tl::API_SCHEMA).unwrap();
    let mut dialogs = Vec::new();
    {
        let transaction = archive.db.transaction().unwrap();
        for id in 1000..1260 {
            let key = format!("user:{id}");
            transaction
                .execute(
                    "INSERT INTO peers VALUES(?1,'{}','{}',?2)",
                    rusqlite::params![key, json!({"_":"user","id":id.to_string()}).to_string()],
                )
                .unwrap();
            dialogs.push(dialog_capture(&schema, &key));
        }
        transaction.commit().unwrap();
    }
    archive.ingest(&schema_hash, &dialogs, None).unwrap();
    for id in [1000, 1259] {
        archive
            .coverage(
                &format!("history:user:{id}"),
                "complete",
                &json!({"full_history":true,"completed_at":100}),
            )
            .unwrap();
    }
    let first = archive
        .dialog_status_page(&Options {
            limit: Some(200),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.items.len(), 200);
    assert_eq!(first.items.last().unwrap().peer_id, "user:1196");
    let second = archive
        .dialog_status_page(&Options {
            after: first.next_cursor,
            limit: Some(200),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(second.items.len(), 65);
    assert_eq!(second.items.first().unwrap().peer_id, "user:1197");
    assert_eq!(second.items.last().unwrap().peer_id, "user:5");
    assert!(second.next_cursor.is_none());
    let exact = archive
        .dialog_status_page(&Options {
            after: Some("user:1061".into()),
            limit: Some(200),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(exact.items.len(), 200);
    assert_eq!(exact.items[0].peer_id, "user:1062");
    assert_eq!(exact.items.last().unwrap().peer_id, "user:5");
    assert!(exact.next_cursor.is_none());
    let filter = |after| Options {
        status: Some("complete".into()),
        dialog_type: Some("user".into()),
        limit: Some(1),
        after,
    };
    let sparse_first = archive.dialog_status_page(&filter(None)).unwrap();
    assert_eq!(sparse_first.items[0].peer_id, "user:1000");
    let sparse_last = archive
        .dialog_status_page(&filter(sparse_first.next_cursor))
        .unwrap();
    assert_eq!(sparse_last.items[0].peer_id, "user:1259");
    assert!(sparse_last.next_cursor.is_none());
}

#[tokio::test]
async fn http_authentication_and_filters() {
    let dir = fixture();
    let app = router(ApiState {
        root: dir.path().into(),
        token: Some("test-token".into()),
    });
    let denied = app
        .clone()
        .oneshot(
            Request::get("/v2/dialog-status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::get("/v2/dialog-status?type=group&status=limited&limit=1")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap()).unwrap();
    assert_eq!(body["items"][0]["peer_id"], "channel:2");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    let bad = app
        .oneshot(
            Request::get("/v2/dialog-status?limit=201")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
}
