use serde_json::json;
use tg_backup::{
    archive::{Archive, Capture, Maintenance, Retention},
    config::Config,
    query::Query,
    tl::Schema,
};
const SCHEMA: &str = "item#12345678 flags:# id:long message:string note:flags.0?string = Item;";
fn item(text: &str, time: i64) -> Capture {
    Capture {
        key: "user:1/message:1".into(),
        kind: "message".into(),
        root_type: "Item".into(),
        bytes: Schema::parse(SCHEMA)
            .unwrap()
            .encode(
                "Item",
                &json!({"_":"item","id":"1","message":text,"note":"optional"}),
            )
            .unwrap(),
        observed_at: time,
        source: "test".into(),
        metadata: json!({"category":"personal","revision":time}),
        replay_key: None,
        partial: false,
        deleted: false,
    }
}
#[test]
fn journal_recovery_occurrences_and_epoch_sealing() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    a.ingest(
        &schema,
        &[item("A", t), item("B", t + 1), item("A", t + 2)],
        Some(("cursor", &json!(3))),
    )
    .unwrap();
    assert_eq!(
        a.query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records
        .len(),
        3
    );
    assert_eq!(
        a.db.query_row("SELECT COUNT(*) FROM payloads", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    drop(a);
    let mut a = Archive::open(dir.path(), true).unwrap();
    assert_eq!(a.checkpoint("cursor").unwrap(), Some(json!(3)));
    a.verify().unwrap();
    a.maintain(&Maintenance {
        apply: true,
        seal: true,
        ..Default::default()
    })
    .unwrap();
    a.verify().unwrap();
    a.ingest(&schema, &[item("C", t + 3)], None).unwrap();
    a.materialize().unwrap();
    assert_eq!(a.epochs().unwrap().len(), 2);
    assert_eq!(
        a.query(&Query::default()).unwrap().records[0].data["message"],
        "C"
    );
}
#[test]
fn layers_and_optional_field_retention() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(&schema, &[item("A", 1_800_000_000_000_000)], None)
        .unwrap();
    let policy = Retention {
        before: Some(i64::MAX),
        remove_fields: vec!["item.note".into()],
        ..Default::default()
    };
    a.maintain(&Maintenance {
        apply: true,
        retention: policy,
        ..Default::default()
    })
    .unwrap();
    let r = a.query(&Query::default()).unwrap().records.remove(0);
    assert!(r.data.get("note").is_none());
    assert!(r.transformed);
    a.verify().unwrap();
    assert!(
        a.maintain(&Maintenance {
            apply: true,
            retention: Retention {
                before: Some(i64::MAX),
                remove_fields: vec!["item.id".into()],
                ..Default::default()
            },
            ..Default::default()
        })
        .is_err()
    );
}
#[test]
fn selectors_and_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    for i in 0..10 {
        let mut c = item(&format!("hello {i}"), 1_800_000_000_000_000 + i);
        c.key = format!("message:{i}");
        a.ingest(&schema, &[c], None).unwrap();
    }
    let mut q = Query {
        selector: "personal and not kind = user".into(),
        regex: Some("hello [13579]".into()),
        limit: 2,
        scan_limit: 3,
        ..Default::default()
    };
    let mut seen = vec![];
    loop {
        let page = a.query(&q).unwrap();
        seen.extend(page.records.into_iter().map(|r| r.key));
        if let Some(cursor) = page.next_cursor {
            q.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(seen.len(), 5);
}
#[test]
fn generated_codec_matches_schema_decoder() {
    use grammers_tl_types::Serializable;
    let value =
        grammers_tl_types::enums::Peer::User(grammers_tl_types::types::PeerUser { user_id: 123 });
    let bytes = value.to_bytes();
    let schema = Schema::current().unwrap();
    let v = schema.decode("Peer", &bytes).unwrap();
    assert_eq!(v["user_id"], "123");
    assert_eq!(schema.encode("Peer", &v).unwrap(), bytes);
}

#[test]
fn replay_and_partial_snapshots_do_not_regress_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    let mut initial = item("complete", t);
    initial.replay_key = Some("event:1".into());
    a.ingest(&schema, &[initial.clone()], None).unwrap();
    a.ingest(&schema, &[initial], None).unwrap();
    let mut partial = item("partial", t + 1);
    partial.partial = true;
    a.ingest(&schema, &[partial, item("stale", t - 1)], None)
        .unwrap();
    assert_eq!(
        a.query(&Query::default()).unwrap().records[0].data["message"],
        "complete"
    );
    assert_eq!(
        a.query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records
        .len(),
        3
    );
}
#[test]
fn crashes_after_epoch_commit_and_catalog_publication_are_replayable() {
    use tg_backup::archive::CommitPoint;
    for point in [CommitPoint::EpochCommitted, CommitPoint::CatalogPublished] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        a.ingest(
            &schema,
            &[item("durable", 1_800_000_000_000_000)],
            Some(("checkpoint", &json!(42))),
        )
        .unwrap();
        assert!(
            a.materialize_with_hook(|p| {
                if p == point {
                    anyhow::bail!("injected crash")
                }
                Ok(())
            })
            .is_err()
        );
        drop(a);
        let a = Archive::open(dir.path(), true).unwrap();
        a.verify().unwrap();
        assert_eq!(a.checkpoint("checkpoint").unwrap(), Some(json!(42)));
        assert_eq!(a.query(&Query::default()).unwrap().records.len(), 1);
    }
}
#[test]
fn interrupted_generation_replacement_never_exposes_half_an_archive() {
    use tg_backup::archive::CommitPoint;
    for point in [
        CommitPoint::GenerationReady,
        CommitPoint::GenerationPublished,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        a.ingest(
            &schema,
            &[
                item("before", 1_800_000_000_000_000),
                item("after", 1_800_000_000_000_001),
            ],
            None,
        )
        .unwrap();
        assert!(
            a.maintain_with_hook(
                &Maintenance {
                    apply: true,
                    seal: true,
                    ..Default::default()
                },
                |p| {
                    if p == point {
                        anyhow::bail!("injected crash")
                    }
                    Ok(())
                }
            )
            .is_err()
        );
        drop(a);
        let a = Archive::open(dir.path(), true).unwrap();
        a.verify().unwrap();
        assert_eq!(
            a.query(&Query {
                all_versions: true,
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            2
        );
    }
}
#[test]
fn failed_ingestion_does_not_advance_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let mut bad = item("bad", 1_800_000_000_000_000);
    bad.bytes.truncate(4);
    assert!(
        a.ingest(&schema, &[bad], Some(("checkpoint", &json!(42))))
            .is_err()
    );
    assert!(a.checkpoint("checkpoint").unwrap().is_none());
}
#[test]
fn pagination_is_stable_across_new_edits_and_as_of_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    let mut second = item("second", t);
    second.key = "second".into();
    a.ingest(&schema, &[item("first", t), second.clone()], None)
        .unwrap();
    let q = Query {
        limit: 1,
        ..Default::default()
    };
    let page = a.query(&q).unwrap();
    let cursor = page.next_cursor.unwrap();
    second.bytes = item("edited", t + 1).bytes;
    second.observed_at = t + 1;
    second.metadata["revision"] = json!(t + 1);
    a.ingest(&schema, &[second], None).unwrap();
    let page = a
        .query(&Query {
            cursor: Some(cursor),
            ..q
        })
        .unwrap();
    assert_eq!(page.records[0].data["message"], "second");
    let page = a
        .query(&Query {
            as_of: Some(t),
            key: Some("second".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.records[0].data["message"], "second");
}
#[test]
fn corrupt_blocks_are_detected_even_after_cached_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(&schema, &[item("hello", 1_800_000_000_000_000)], None)
        .unwrap();
    a.materialize().unwrap();
    a.query(&Query::default()).unwrap();
    let epoch = dir.path().join(&a.epochs().unwrap()[0].2);
    let db = rusqlite::Connection::open(epoch).unwrap();
    db.execute("UPDATE blocks SET data=x'00000000'", [])
        .unwrap();
    drop(db);
    assert!(a.verify().is_err());
}
#[test]
fn media_resume_verification_and_content_deduplication() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let ids = a
        .ingest(&schema, &[item("hello", 1_800_000_000_000_000)], None)
        .unwrap();
    a.queue_media("a", &json!({}), 1, Some(6), ids[0]).unwrap();
    a.append_media("a", 0, b"abc").unwrap();
    assert!(a.finish_media("a").is_err());
    drop(a);
    let a = Archive::open(dir.path(), true).unwrap();
    a.append_media("a", 3, b"def").unwrap();
    let first = a.finish_media("a").unwrap();
    a.queue_media("b", &json!({}), 1, Some(6), ids[0]).unwrap();
    a.append_media("b", 0, b"abcdef").unwrap();
    assert_eq!(a.finish_media("b").unwrap(), first);
    a.verify().unwrap();
}
#[test]
fn folder_rules_honor_exclusions_and_dynamic_unread_state() {
    use tg_backup::telegram::folder_matches;
    let f = json!({"contacts":true,"exclude_read":true,"include_peers":[{"_":"inputPeerUser","user_id":"2","access_hash":"0"}],"exclude_peers":[{"_":"inputPeerUser","user_id":"3","access_hash":"0"}]});
    let unread = json!({"category":"personal","contact":true,"unread":true});
    let read = json!({"category":"personal","contact":true,"unread":false});
    assert!(folder_matches(&f, "user:1", &unread));
    assert!(!folder_matches(&f, "user:1", &read));
    assert!(folder_matches(&f, "user:2", &read));
    assert!(!folder_matches(&f, "user:3", &unread));
}
#[test]
fn historical_index_and_scanning_have_identical_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(
        dir.path(),
        &Config {
            index_history: true,
            ..Default::default()
        },
    )
    .unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(
        &schema,
        &[
            item("needle one", 1_800_000_000_000_000),
            item("needle two", 1_800_000_000_000_001),
        ],
        None,
    )
    .unwrap();
    let q = Query {
        all_versions: true,
        text: Some("needle".into()),
        ..Default::default()
    };
    let indexed = a.query(&q).unwrap();
    a.config.index_history = false;
    let scan = a.query(&q).unwrap();
    assert_eq!(
        serde_json::to_value(indexed.records).unwrap(),
        serde_json::to_value(scan.records).unwrap()
    );
}
#[test]
fn retention_dry_run_is_nonmutating_and_invalidates_old_cursors_on_apply() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    a.ingest(
        &schema,
        &[item("A", t), item("B", t + 1), item("C", t + 2)],
        None,
    )
    .unwrap();
    let q = Query {
        all_versions: true,
        limit: 1,
        ..Default::default()
    };
    let cursor = a.query(&q).unwrap().next_cursor.unwrap();
    let mut policy = Maintenance {
        retention: Retention {
            before: Some(i64::MAX),
            intermediate_versions: true,
            ..Default::default()
        },
        ..Default::default()
    };
    a.maintain(&policy).unwrap();
    assert_eq!(
        a.query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records
        .len(),
        3
    );
    policy.apply = true;
    a.maintain(&policy).unwrap();
    assert_eq!(
        a.query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records
        .len(),
        2
    );
    assert!(
        a.query(&Query {
            cursor: Some(cursor),
            ..q
        })
        .is_err()
    );
}
#[tokio::test]
async fn http_matches_local_queries_and_rejects_unauthorized_requests() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(&schema, &[item("hello", 1_800_000_000_000_000)], None)
        .unwrap();
    a.materialize().unwrap();
    let q = Query::default();
    let mut expected = serde_json::to_value(a.query(&q).unwrap()).unwrap();
    tg_backup::query::public_json(&mut expected);
    drop(a);
    let app = tg_backup::http::router(tg_backup::http::ApiState {
        root: dir.path().into(),
        token: Some("test-token".into()),
    });
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/v2/query")
            .header("content-type", "application/json")
    };
    let no_auth = app
        .clone()
        .oneshot(
            request()
                .body(Body::from(serde_json::to_vec(&q).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_auth.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .oneshot(
            request()
                .header("authorization", "Bearer test-token")
                .body(Body::from(serde_json::to_vec(&q).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        expected
    );
}

#[test]
fn archived_schema_is_used_after_constructor_evolution() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let old = a.register_schema(1, SCHEMA).unwrap();
    let newer =
        "item#12345679 flags:# id:long message:string note:flags.0?string extra:int = Item;";
    a.ingest(&old, &[item("old", 1_700_000_000_000_000)], None)
        .unwrap();
    let hash = a.register_schema(2, newer).unwrap();
    let mut c = item("new", 1_800_000_000_000_000);
    c.bytes = Schema::parse(newer)
        .unwrap()
        .encode(
            "Item",
            &json!({"_":"item","id":"1","message":"new","extra":7}),
        )
        .unwrap();
    a.ingest(&hash, &[c], None).unwrap();
    a.maintain(&Maintenance {
        apply: true,
        consolidate_yearly: true,
        seal: true,
        ..Default::default()
    })
    .unwrap();
    drop(a);
    let a = Archive::open(dir.path(), false).unwrap();
    let records = a
        .query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records;
    assert_eq!(records.len(), 2);
    assert!(records[0].data.get("extra").is_none());
    assert_eq!(records[1].data["extra"], 7);
    a.verify().unwrap();
}
#[test]
fn observation_coalescing_preserves_a_b_a_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    a.ingest(
        &schema,
        &[
            item("A", t),
            item("A", t + 1),
            item("B", t + 2),
            item("A", t + 3),
        ],
        None,
    )
    .unwrap();
    a.maintain(&Maintenance {
        apply: true,
        retention: Retention {
            before: Some(i64::MAX),
            coalesce_observations: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let records = a
        .query(&Query {
            all_versions: true,
            ..Default::default()
        })
        .unwrap()
        .records;
    assert_eq!(
        records
            .iter()
            .map(|r| r.data["message"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["A", "B", "A"]
    );
}
#[test]
fn selectors_defer_message_specific_predicates_during_peer_discovery() {
    use tg_backup::selector::Selector;
    let s = Selector::parse("personal and topic = 123 and not date < 100").unwrap();
    assert!(s.may_match(&json!({"category":"personal"})));
    assert!(!s.may_match(&json!({"category":"group"})));
    assert!(s.matches(&json!({"category":"personal","topic":123,"date":101})));
    assert!(!s.matches(&json!({"category":"personal","topic":124,"date":101})));
}
#[test]
fn shared_tl_flags_cannot_be_partially_removed() {
    let s = Schema::parse("pair#12345678 flags:# a:flags.0?int b:flags.0?string = Pair;").unwrap();
    let mut value = json!({"_":"pair","a":1,"b":"retained"});
    s.redact(&mut value, &["pair.a".into()]).unwrap();
    assert!(s.encode("Pair", &value).is_err());
}
#[test]
fn generated_message_round_trip_includes_nested_media_and_entities() {
    use grammers_tl_types::{Deserializable, Serializable};
    let s = Schema::current().unwrap();
    let v = json!({"_":"message","id":17,"peer_id":{"_":"peerUser","user_id":"42"},"date":1700000000,"message":"hello","entities":[{"_":"messageEntityBold","offset":0,"length":5}],"media":{"_":"messageMediaPhoto","photo":{"_":"photoEmpty","id":"123"}}});
    let bytes = s.encode("Message", &v).unwrap();
    let generated = grammers_tl_types::enums::Message::from_bytes(&bytes).unwrap();
    assert_eq!(generated.to_bytes(), bytes);
    assert_eq!(
        s.decode("Message", &bytes).unwrap()["entities"][0]["length"],
        5
    );
}
#[test]
fn active_epoch_remains_writable_when_closed_epochs_are_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let now = chrono::Utc::now().timestamp_micros();
    a.ingest(
        &schema,
        &[
            item("old", now - 400 * 86400 * 1_000_000),
            item("current", now),
        ],
        None,
    )
    .unwrap();
    a.seal_due().unwrap();
    assert_eq!(a.epochs().unwrap().iter().filter(|e| e.3).count(), 1);
    a.ingest(&schema, &[item("still current", now + 1)], None)
        .unwrap();
    assert_eq!(a.epochs().unwrap().len(), 2);
    a.verify().unwrap();
}
#[test]
#[ignore = "synthetic archive size and memory-bound workload"]
fn synthetic_archive_storage_report() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(
        dir.path(),
        &Config {
            block_bytes: 64 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let t = 1_800_000_000_000_000;
    for batch in 0..100 {
        let records: Vec<_> = (0..100)
            .map(|i| {
                let n = batch * 100 + i;
                let mut c = item(
                    &format!("message {n} {}", "repeatable text ".repeat(40)),
                    t + n,
                );
                c.key = format!("message:{n}");
                c
            })
            .collect();
        a.ingest(&schema, &records, None).unwrap();
        a.materialize().unwrap();
    }
    a.maintain(&Maintenance {
        apply: true,
        seal: true,
        ..Default::default()
    })
    .unwrap();
    a.verify().unwrap();
    assert_eq!(
        a.query(&Query {
            regex: Some("message 9999".into()),
            scan_limit: 100000,
            ..Default::default()
        })
        .unwrap()
        .records
        .len(),
        1
    );
    println!("{}", a.status().unwrap());
}

#[test]
fn missing_dictionary_is_detected_and_dictionary_blocks_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(
        &schema,
        &[item(
            "dictionary compression fixture",
            1_800_000_000_000_000,
        )],
        None,
    )
    .unwrap();
    a.materialize().unwrap();
    let path = dir.path().join(&a.epochs().unwrap()[0].2);
    let epoch = rusqlite::Connection::open(path).unwrap();
    let (block, compressed, size): (i64, Vec<u8>, usize) = epoch
        .query_row("SELECT id,data,raw_size FROM blocks LIMIT 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    let raw = zstd::bulk::decompress(&compressed, size).unwrap();
    let dictionary =
        b"dictionary compression fixture optional field item test archive message".repeat(10);
    let encoded = zstd::bulk::Compressor::with_dictionary(9, &dictionary)
        .unwrap()
        .compress(&raw)
        .unwrap();
    epoch
        .execute(
            "INSERT INTO dictionaries VALUES(1,?1,?2)",
            rusqlite::params![dictionary, blake3::hash(&dictionary).to_hex().to_string()],
        )
        .unwrap();
    epoch
        .execute(
            "UPDATE blocks SET dictionary=1,data=?1 WHERE id=?2",
            rusqlite::params![encoded, block],
        )
        .unwrap();
    a.verify().unwrap();
    epoch.execute("DELETE FROM dictionaries", []).unwrap();
    assert!(a.verify().is_err());
}
