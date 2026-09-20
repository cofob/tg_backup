use serde_json::json;
use tg_backup::{
    archive::{Archive, Capture, Maintenance},
    config::Config,
    query::Query,
    tl::Schema,
    work::{Schedule, Task, Window},
};
const SCHEMA: &str = "item#12345678 id:long date:int message:string = Item;";
fn capture(id: i64, text: &str) -> Capture {
    Capture {
        key: format!("message:{id}"),
        kind: "message".into(),
        root_type: "Item".into(),
        bytes: Schema::parse(SCHEMA)
            .unwrap()
            .encode(
                "Item",
                &json!({"_":"item","id":id,"date":100,"message":text}),
            )
            .unwrap(),
        observed_at: 1_800_000_000_000_000 + id,
        source: "test".into(),
        metadata: json!({"category":"personal"}),
        replay_key: None,
        partial: false,
        deleted: false,
    }
}
#[test]
fn schedules_include_overnight_start_day_and_manual_jobs_remain_visible() {
    let s = Schedule {
        windows: vec![Window {
            days: vec![5],
            start: "22:00".into(),
            end: "06:00".into(),
        }],
    };
    s.validate().unwrap();
    assert!(s.allows_components(5, 23 * 60));
    assert!(s.allows_components(6, 5 * 60));
    assert!(!s.allows_components(6, 6 * 60));
    assert!(!s.allows_components(4, 23 * 60));
    assert!(Schedule::default().allows_components(1, 0));
    assert!(
        Schedule {
            windows: vec![Window {
                days: vec![0],
                start: "02:00".into(),
                end: "06:00".into()
            }]
        }
        .validate()
        .is_err()
    );
}
#[test]
fn metadata_only_growth_rolls_epochs_and_recovers_journal() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(
        dir.path(),
        &Config {
            max_epoch_bytes: 100_000,
            ..Default::default()
        },
    )
    .unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let c = capture(1, "A");
    let mut items = vec![];
    for n in 0..100 {
        let mut c = c.clone();
        c.observed_at += n;
        c.metadata["annotation"] = json!("x".repeat(4096));
        items.push(c);
    }
    a.ingest(&schema, &items, Some(("cursor", &json!(100))))
        .unwrap();
    assert!(a.epochs().unwrap().len() > 1);
    drop(a);
    let a = Archive::open(dir.path(), true).unwrap();
    a.verify().unwrap();
    assert_eq!(a.status().unwrap()["observations"], 100);
    assert_eq!(a.status().unwrap()["payloads"], 1);
    assert_eq!(a.checkpoint("cursor").unwrap(), Some(json!(100)));
    let copies: i64 = a
        .epochs()
        .unwrap()
        .iter()
        .map(|(_, _, p, _)| {
            rusqlite::Connection::open(dir.path().join(p))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM observations", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap()
        })
        .sum();
    assert_eq!(copies, 100);
}
#[test]
fn background_generation_publication_preserves_new_ingestion_and_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    a.ingest(&schema, &[capture(1, "before")], None).unwrap();
    a.materialize().unwrap();
    let task = Task {
        sequence: a.enqueue_work("repack", "test", &json!({}), false).unwrap(),
        kind: "repack".into(),
        config: json!({}),
    };
    let report = tg_backup::maintenance_worker::execute(dir.path(), &task).unwrap();
    a.ingest(&schema, &[capture(2, "during")], Some(("live", &json!(42))))
        .unwrap();
    a.materialize().unwrap();
    a.enqueue_work("verify", "later", &json!({}), false)
        .unwrap();
    tg_backup::maintenance_worker::publish(&mut a, &task, &report).unwrap();
    a.verify().unwrap();
    assert_eq!(a.checkpoint("live").unwrap(), Some(json!(42)));
    assert_eq!(a.query(&Query::default()).unwrap().records.len(), 2);
    assert_eq!(
        a.work_page(0, 20).unwrap()["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    a.ingest(&schema, &[capture(3, "after")], None).unwrap();
    a.materialize().unwrap();
    a.verify().unwrap();
}
#[test]
fn repacking_observations_honors_size_target() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(
        dir.path(),
        &Config {
            max_epoch_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let captures: Vec<_> = (0..100)
        .map(|i| {
            let mut c = capture(i, "same");
            c.metadata["large"] = json!("x".repeat(5000));
            c
        })
        .collect();
    a.ingest(&schema, &captures, None).unwrap();
    a.materialize().unwrap();
    a.config.max_epoch_bytes = 100_000;
    a.maintain(&Maintenance {
        apply: true,
        seal: true,
        ..Default::default()
    })
    .unwrap();
    a.verify().unwrap();
    assert!(a.epochs().unwrap().len() > 1);
    for (_, _, path, _) in a.epochs().unwrap() {
        assert!(std::fs::metadata(dir.path().join(path)).unwrap().len() < 150_000);
    }
}
#[tokio::test]
async fn queue_status_local_and_http_match_and_are_read_only() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let a = Archive::init(dir.path(), &Config::default()).unwrap();
    for n in 0..4 {
        a.enqueue_work("verify", &n.to_string(), &json!({}), false)
            .unwrap();
    }
    let page = a.work_page(0, 2).unwrap();
    assert_eq!(page["next_cursor"], 2);
    let router = tg_backup::http::router(tg_backup::http::ApiState {
        root: dir.path().into(),
        token: Some("secret".into()),
    });
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v2/work?limit=2")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let remote: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await.unwrap()).unwrap();
    assert_eq!(page, remote);
    assert_eq!(a.status().unwrap()["work"]["counts"]["queued"], 4);
    assert_eq!(a.status().unwrap()["worker"]["state"], "offline");
    assert!(a.work_page(0, 0).is_err());
}
#[test]
fn historical_selection_requires_all_shared_references() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let mut recent = capture(2, "recent");
    recent.metadata["category"] = json!("bot");
    let ids = a
        .ingest(&schema, &[capture(1, "old"), recent], None)
        .unwrap();
    for (n, id) in ids.into_iter().enumerate() {
        let key = n.to_string();
        a.queue_media(&key, &json!({}), 1, Some(3), id).unwrap();
        a.append_media(&key, 0, b"abc").unwrap();
        a.finish_media(&key).unwrap();
    }
    let p = tg_backup::transcode::Policy {
        selector: "category = personal".into(),
        min_age_days: 365,
        ..Default::default()
    };
    assert_eq!(
        tg_backup::transcode::enqueue(&a, &p, true, false).unwrap()["eligible"],
        0
    );
    let p = tg_backup::transcode::Policy {
        min_age_days: 365,
        ..Default::default()
    };
    assert_eq!(
        tg_backup::transcode::enqueue(&a, &p, true, false).unwrap()["eligible"],
        1
    );
    tg_backup::transcode::enqueue(&a, &p, true, false).unwrap();
    assert_eq!(
        a.work_page(0, 20).unwrap()["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn cli_worker_runs_in_a_separate_process_and_persists_completion() {
    let dir = tempfile::tempdir().unwrap();
    let a = Archive::init(dir.path(), &Config::default()).unwrap();
    a.enqueue_work("verify", "test", &json!({}), false).unwrap();
    drop(a);
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_tg-backup"))
        .arg("--dataset")
        .arg(dir.path())
        .args(["worker", "--once"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let a = Archive::open(dir.path(), false).unwrap();
    assert_eq!(a.work_page(0, 10).unwrap()["items"][0]["state"], "complete");
}

#[test]
#[ignore = "requires ffmpeg and ffprobe; run by Linux CI"]
fn real_webp_transcode_keeps_original_and_survives_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
    let schema = a.register_schema(1, SCHEMA).unwrap();
    let ids = a.ingest(&schema, &[capture(1, "image")], None).unwrap();
    a.materialize().unwrap();
    // Uncompressed 24-bit BMP gives a deterministic, independently generated input.
    let (width, height) = (256u32, 256u32);
    let size = 54 + width * height * 3;
    let mut bmp = vec![0u8; size as usize];
    bmp[0..2].copy_from_slice(b"BM");
    bmp[2..6].copy_from_slice(&size.to_le_bytes());
    bmp[10..14].copy_from_slice(&54u32.to_le_bytes());
    bmp[14..18].copy_from_slice(&40u32.to_le_bytes());
    bmp[18..22].copy_from_slice(&width.to_le_bytes());
    bmp[22..26].copy_from_slice(&height.to_le_bytes());
    bmp[26..28].copy_from_slice(&1u16.to_le_bytes());
    bmp[28..30].copy_from_slice(&24u16.to_le_bytes());
    for (n, pixel) in bmp[54..].as_chunks_mut::<3>().0.iter_mut().enumerate() {
        pixel.copy_from_slice(&[(n % 256) as u8, (n / 256) as u8, 100]);
    }
    a.queue_media("photo:1:x", &json!({}), 1, Some(u64::from(size)), ids[0])
        .unwrap();
    a.append_media("photo:1:x", 0, &bmp).unwrap();
    let original = a.finish_media("photo:1:x").unwrap();
    let p = tg_backup::transcode::Policy::default();
    tg_backup::transcode::enqueue(&a, &p, true, false).unwrap();
    let (sequence, config): (i64, String) =
        a.db.query_row("SELECT sequence,config FROM work", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let task = Task {
        sequence,
        kind: "transcode".into(),
        config: serde_json::from_str(&config).unwrap(),
    };
    let report = tg_backup::transcode::execute(dir.path(), &task, &Default::default()).unwrap();
    assert_eq!(report["state"], "complete", "{report}");
    tg_backup::transcode::publish(&mut a, &task, &report).unwrap();
    a.verify().unwrap();
    assert!(
        tg_backup::media::attachment_path(dir.path(), &original)
            .unwrap()
            .exists()
    );
    let derivative = report["hash"].as_str().unwrap();
    assert!(
        tg_backup::media::attachment_path(dir.path(), derivative)
            .unwrap()
            .exists()
    );
    a.maintain(&Maintenance {
        apply: true,
        seal: true,
        ..Default::default()
    })
    .unwrap();
    a.verify().unwrap();
    assert!(
        tg_backup::media::attachment_path(dir.path(), derivative)
            .unwrap()
            .exists()
    );
    assert!(report["bytes"].as_u64().unwrap() < u64::from(size));
}
