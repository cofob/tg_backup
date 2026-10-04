use serde_json::json;
use tg_backup::{
    archive::{Archive, Capture, CommitPoint, Maintenance, Retention},
    config::{Backend, ClickHouse, Config},
    migration::{self, ConnectionOptions, Options},
    query::Query,
    storage::{Select, rows::*},
    tl::Schema,
};
use tg_backup_protocol::explorer::*;
const SCHEMA: &str = "item#12345678 flags:# id:long message:string note:flags.0?string = Item;";
fn configs() -> Vec<Config> {
    let mut configs = vec![Config::default()];
    if let Ok(url) = std::env::var("TG_BACKUP_CLICKHOUSE_URL") {
        configs.push(Config {
            backend: Backend::Clickhouse,
            clickhouse: Some(ClickHouse {
                url,
                database: format!("test_{}", uuid::Uuid::new_v4().simple()),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    configs
}
fn item(id: i64, text: &str, at: i64) -> Capture {
    Capture {
        key: format!("user:1/message:{id}"),
        kind: "message".into(),
        root_type: "Item".into(),
        bytes: Schema::parse(SCHEMA)
            .unwrap()
            .encode(
                "Item",
                &json!({"_":"item","id":id.to_string(),"message":text,"note":"optional"}),
            )
            .unwrap(),
        observed_at: at,
        source: "test".into(),
        metadata: json!({"revision":at}),
        replay_key: Some(format!("{id}:{at}")),
        partial: false,
        deleted: false,
    }
}
fn cleanup(config: &Config) {
    if let Some(c) = &config.clickhouse {
        let db = tg_backup::storage::ClickHouseStore::connect(c, true).unwrap();
        db.execute(format!("DROP DATABASE {}", c.database)).unwrap();
    }
}
fn source_files(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, String> {
    fn scan(
        root: &std::path::Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, String>,
    ) {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                scan(&entry.path(), files);
            } else {
                files.insert(
                    entry.path(),
                    tg_backup::media::file_hash(&entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    scan(root, &mut files);
    files
}
#[test]
fn shared_archive_behavior() {
    for config in configs() {
        let root = tempfile::tempdir().unwrap();
        let mut a = Archive::init(root.path(), &config).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        let at = 1_800_000_000_000_000;
        let captures = [
            item(1, "Привет WORLD café", at),
            item(2, "hello world", at + 1),
            item(3, "world hello", at + 2),
        ];
        let ids = a
            .ingest(&schema, &captures, Some(("resume", &json!({"offset":3}))))
            .unwrap();
        assert_eq!(a.ingest(&schema, &captures, None).unwrap(), ids);
        let page = a
            .query(&Query {
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        a.ingest(
            &schema,
            &[item(2, "changed", at + 3), item(4, "new", at + 4)],
            None,
        )
        .unwrap();
        let mut q = Query {
            limit: 1,
            cursor: page.next_cursor,
            ..Default::default()
        };
        let next = a.query(&q).unwrap();
        assert_eq!(next.records[0].data["message"], "hello world");
        q.cursor = next.next_cursor;
        assert_eq!(a.query(&q).unwrap().records[0].sequence, ids[2]);
        let plain = if config.backend == Backend::Clickhouse {
            "WORLD привет"
        } else {
            "WORLD AND привет"
        };
        assert_eq!(
            a.query(&Query {
                text: Some(plain.into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        assert_eq!(
            a.query(&Query {
                text: Some("\"world hello\"".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        assert_eq!(
            a.query(&Query {
                regex: Some("café".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        let work = a.enqueue_work("verify", "once", &json!({}), false).unwrap();
        assert_eq!(
            a.enqueue_work("verify", "once", &json!({}), false).unwrap(),
            work
        );
        a.verify().unwrap();
        assert!(
            a.materialize_with_hook(|point| if point == CommitPoint::EpochCommitted {
                anyhow::bail!("interrupted")
            } else {
                Ok(())
            })
            .is_err()
        );
        drop(a);
        let mut a = Archive::open(root.path(), true).unwrap();
        a.verify().unwrap();
        assert_eq!(a.checkpoint("resume").unwrap(), Some(json!({"offset":3})));
        let request = BrowseRequest::new(Browse::Messages {
            peer: "user:1".into(),
            topic: None,
        });
        assert_eq!(a.explore(&request).unwrap().entries.len(), 4);
        for target in [
            Browse::Databases,
            Browse::Tables {
                database: "catalog".into(),
            },
            Browse::Rows {
                database: "catalog".into(),
                table: "schemas".into(),
                row: None,
            },
            Browse::Location { sequence: ids[0] },
            Browse::Conversations { folder: None },
            Browse::Folders,
            Browse::Topics {
                peer: "user:1".into(),
            },
        ] {
            a.explore(&BrowseRequest::new(target)).unwrap();
        }
        tg_backup::metrics::snapshot(&a).unwrap();
        let bytes = a.payload(&a.record(ids[0]).unwrap().payload_hash).unwrap();
        a.maintain(&Maintenance {
            apply: true,
            seal: true,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            bytes,
            a.payload(&a.record(ids[0]).unwrap().payload_hash).unwrap()
        );
        a.maintain(&Maintenance {
            apply: true,
            retention: Retention {
                before: Some(i64::MAX),
                remove_fields: vec!["item.note".into()],
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
        assert!(a.record(ids[0]).unwrap().data.get("note").is_none());
        assert!(a.query(&q).is_err());
        drop(a);
        cleanup(&config);
    }
}
#[test]
fn migration_round_trips() {
    let configs = configs();
    for source_config in &configs {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let mut a = Archive::init(&source, source_config).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        let id = a
            .ingest(
                &schema,
                &[item(1, "preserved", 1_800_000_000_000_000)],
                Some(("resume", &json!({"messages":17,"media":9}))),
            )
            .unwrap()[0];
        let record = a.record(id).unwrap();
        let work = a.enqueue_work("verify", "work", &json!({}), false).unwrap();
        a.store
            .put(&JobRow {
                id: "job".into(),
                config: "{}".into(),
                status: "running".into(),
                created: 11,
                updated: 12,
                details: "{}".into(),
            })
            .unwrap();
        let mut row = a.store.get::<WorkRow>("sequence", work).unwrap().unwrap();
        row.state = "running".into();
        row.attempts = 7;
        a.store.put(&row).unwrap();
        // Retain a pending journal and a second compressed payload.
        a.materialize().unwrap();
        a.ingest(&schema, &[item(2, "pending", 1_800_000_000_000_001)], None)
            .unwrap();
        a.queue_media("document:1", &json!({}), 1, Some(6), id)
            .unwrap();
        a.append_media("document:1", 0, b"media!").unwrap();
        let attachment = a.finish_media("document:1").unwrap();
        let original_id = a.dataset_id().unwrap();
        drop(a);
        let source_file = source.join("catalog.sqlite3");
        let before = source_file
            .exists()
            .then(|| tg_backup::media::file_hash(&source_file).unwrap());
        let mut current = source.clone();
        let mut targets = vec![];
        for (n, target_config) in configs
            .iter()
            .chain(std::iter::once(source_config))
            .enumerate()
        {
            let output = root.path().join(format!("target{n}"));
            let mut c = target_config.clone();
            if let Some(ch) = &mut c.clickhouse {
                ch.database = format!("test_{}", uuid::Uuid::new_v4().simple());
            }
            let connection = c
                .clickhouse
                .as_ref()
                .map(|c| ConnectionOptions {
                    clickhouse_url: c.url.clone(),
                    clickhouse_database: c.database.clone(),
                    clickhouse_user: c.user.clone(),
                    ..Default::default()
                })
                .unwrap_or_default();
            let options = Options {
                to: c.backend,
                output: output.clone(),
                resume: false,
                connection,
            };
            migration::run(&current, &options).unwrap();
            let a = Archive::open(&output, false).unwrap();
            assert_ne!(a.dataset_id().unwrap(), original_id);
            assert_eq!(
                tg_backup::media::file_hash(
                    &tg_backup::media::attachment_path(&output, &attachment).unwrap()
                )
                .unwrap(),
                attachment
            );
            assert_eq!(
                serde_json::to_value(a.record(id).unwrap()).unwrap(),
                serde_json::to_value(&record).unwrap()
            );
            assert_eq!(
                a.checkpoint("resume").unwrap(),
                Some(json!({"messages":17,"media":9}))
            );
            assert_eq!(
                a.store
                    .get::<WorkRow>("sequence", work)
                    .unwrap()
                    .unwrap()
                    .attempts,
                7
            );
            assert_eq!(
                a.store
                    .get::<WorkRow>("sequence", work)
                    .unwrap()
                    .unwrap()
                    .state,
                "queued"
            );
            assert_eq!(
                a.store.get::<JobRow>("id", "job").unwrap().unwrap().status,
                "paused"
            );
            assert!(
                a.store
                    .select::<PayloadRow>(&Select::default())
                    .unwrap()
                    .iter()
                    .any(PayloadRow::has_journal)
            );
            drop(a);
            current = output;
            targets.push(c);
        }
        if let Some(before) = before {
            assert_eq!(before, tg_backup::media::file_hash(&source_file).unwrap());
        }
        for c in targets {
            cleanup(&c);
        }
        cleanup(source_config);
    }
}

#[test]
fn migration_resume_and_source_guard() {
    for config in configs() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let mut a = Archive::init(&source, &config).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        let id = a
            .ingest(&schema, &[item(1, "resume", 1_800_000_000_000_000)], None)
            .unwrap()[0];
        a.queue_media("document:1", &json!({}), 1, Some(6), id)
            .unwrap();
        a.append_media("document:1", 0, b"media!").unwrap();
        let hash = a.finish_media("document:1").unwrap();
        a.store
            .put(&RepresentationRow {
                original: hash.clone(),
                hash: hash.clone(),
                recipe: "test".into(),
                bytes: 6,
                created: 11,
                details: "{}".into(),
            })
            .unwrap();
        tg_backup_credentials::private_write(&source.join("api-hash.token"), b"private").unwrap();
        let session = rusqlite::Connection::open(source.join("session.sqlite3")).unwrap();
        session
            .execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE fixture(value TEXT); INSERT INTO fixture VALUES('session');",
            )
            .unwrap();
        drop(a);
        let files = source_files(&source);
        let before = tg_backup::media::file_hash(&source.join("session.sqlite3")).unwrap();
        let mut options = Options {
            to: Backend::Sqlite,
            output: root.path().join("target"),
            resume: false,
            connection: ConnectionOptions::default(),
        };
        assert!(
            migration::run_with_hook(&source, &options, || anyhow::bail!("interrupted")).is_err()
        );
        assert!(Archive::open(&options.output, false).is_err());
        options.resume = true;
        migration::run(&source, &options).unwrap();
        assert_eq!(files, source_files(&source));
        let copied = rusqlite::Connection::open(options.output.join("session.sqlite3")).unwrap();
        assert_eq!(
            copied
                .query_row("SELECT value FROM fixture", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "session"
        );
        drop(copied);
        let target = Archive::open(&options.output, false).unwrap();
        assert_eq!(
            target
                .store
                .get::<MediaRow>("id", "document:1")
                .unwrap()
                .unwrap()
                .hash,
            Some(hash.clone())
        );
        assert_eq!(
            tg_backup::media::file_hash(
                &tg_backup::media::attachment_path(&options.output, &hash).unwrap()
            )
            .unwrap(),
            hash
        );
        assert_eq!(
            std::fs::read(options.output.join("api-hash.token")).unwrap(),
            b"private"
        );
        assert_eq!(
            before,
            tg_backup::media::file_hash(&source.join("session.sqlite3")).unwrap()
        );
        drop(session);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(options.output.join("api-hash.token"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop(target);
        options.output = root.path().join("changed");
        options.resume = false;
        assert!(
            migration::run_with_hook(&source, &options, || anyhow::bail!("interrupted")).is_err()
        );
        let a = Archive::open(&source, true).unwrap();
        a.set_checkpoint("changed", &json!(true)).unwrap();
        drop(a);
        options.resume = true;
        assert!(migration::run(&source, &options).is_err());
        options.resume = false;
        assert!(migration::run(&source, &options).is_err());
        cleanup(&config);
    }
}
#[test]
fn clickhouse_batch_failures_and_maintenance() {
    use tg_backup::storage::{BatchPoint, CATALOG};
    for config in configs()
        .into_iter()
        .filter(|c| c.backend == Backend::Clickhouse)
    {
        let root = tempfile::tempdir().unwrap();
        let a = Archive::init(root.path(), &config).unwrap();
        for (n, point) in [
            BatchPoint::JournalCommitted,
            BatchPoint::RowsInserted,
            BatchPoint::Committed,
        ]
        .into_iter()
        .enumerate()
        {
            let key = format!("batch:{n}");
            let db = a.store.clickhouse().unwrap();
            let changes = vec![
                Change::put(
                    CATALOG,
                    &CheckpointRow {
                        key: key.clone(),
                        value: json!(n).to_string(),
                    },
                )
                .unwrap(),
                Change::put(
                    CATALOG,
                    &CoverageRow {
                        name: key.clone(),
                        status: "complete".into(),
                        updated: 1,
                        details: "{}".into(),
                    },
                )
                .unwrap(),
            ];
            assert!(
                db.commit_with_hook(changes.clone(), |at| if at == point {
                    anyhow::bail!("lost response")
                } else {
                    Ok(())
                })
                .is_err()
            );
            let reader = Archive::open(root.path(), false).unwrap();
            assert_eq!(
                reader.checkpoint(&key).unwrap().is_some(),
                point == BatchPoint::Committed
            );
            drop(reader);
            // A following write first completes the durable intent. Replay keeps one logical row.
            db.commit(changes).unwrap();
            assert_eq!(a.checkpoint(&key).unwrap(), Some(json!(n)));
            assert_eq!(
                a.store
                    .count::<CoverageRow>(&Select::eq("name", key))
                    .unwrap(),
                1
            );
        }
        drop(a);
        let mut a = Archive::open(root.path(), true).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        a.ingest(&schema, &[item(1, "before", 1_800_000_000_000_000)], None)
            .unwrap();
        let sequence = a
            .enqueue_work("repack", "maintenance", &json!({}), false)
            .unwrap();
        let task = tg_backup::work::Task {
            sequence,
            kind: "repack".into(),
            config: json!({}),
        };
        let report = tg_backup::maintenance_worker::execute(root.path(), &task).unwrap();
        a.ingest(
            &schema,
            &[item(2, "during", 1_800_000_000_000_001)],
            Some(("new", &json!(2))),
        )
        .unwrap();
        a.enqueue_work("verify", "new", &json!({}), false).unwrap();
        tg_backup::maintenance_worker::publish(&mut a, &task, &report).unwrap();
        assert_eq!(a.query(&Query::default()).unwrap().records.len(), 2);
        assert_eq!(a.checkpoint("new").unwrap(), Some(json!(2)));
        assert_eq!(
            a.work_page(0, 10).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        a.verify().unwrap();
        a.queue_media("old", &json!({}), 1, None, 1).unwrap();
        a.ingest(&schema, &[item(1, "latest", 1_800_000_000_000_002)], None)
            .unwrap();
        for key in ["user:1", "user:2"] {
            a.store
                .put(&PeerRow {
                    key: key.into(),
                    input: "{}".into(),
                    metadata: "{}".into(),
                    raw: "{}".into(),
                })
                .unwrap();
        }
        let writer =
            tg_backup::storage::ClickHouseStore::connect(config.clickhouse.as_ref().unwrap(), true)
                .unwrap();
        writer.open().unwrap();
        a.maintain_with_hook(
            &Maintenance {
                apply: true,
                retention: Retention {
                    before: Some(1_800_000_000_000_001),
                    drop_kinds: vec!["message".into()],
                    remove_fields: vec!["item.note".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            |point| {
                if point == CommitPoint::GenerationReady {
                    writer.commit(vec![
                        Change::put(
                            CATALOG,
                            &MediaRow {
                                id: "new".into(),
                                location: "{}".into(),
                                dc: 1,
                                size: None,
                                status: "pending".into(),
                                offset: 0,
                                hash: None,
                                error: None,
                                attempts: 0,
                                retry_at: 0,
                            },
                        )?,
                        Change::put(
                            CATALOG,
                            &MediaRefRow {
                                media: "new".into(),
                                observation: 2,
                            },
                        )?,
                        Change::put(
                            CATALOG,
                            &PeerRow {
                                key: "user:1".into(),
                                input: "{}".into(),
                                metadata: "{\"new\":true}".into(),
                                raw: "{}".into(),
                            },
                        )?,
                    ])?;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(a.query(&Query::default()).unwrap().records.len(), 2);
        assert!(a.record(1).is_err());
        assert!(a.store.get::<MediaRow>("id", "old").unwrap().is_none());
        assert!(a.store.get::<MediaRow>("id", "new").unwrap().is_some());
        assert!(a.store.get::<PeerRow>("key", "user:2").unwrap().is_none());
        assert_eq!(
            a.store
                .get::<PeerRow>("key", "user:1")
                .unwrap()
                .unwrap()
                .metadata,
            "{\"new\":true}"
        );
        a.verify().unwrap();
        drop(a);
        cleanup(&config);
    }
}

#[test]
fn corrupt_migration_input_is_not_published() {
    for config in configs() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let mut a = Archive::init(&source, &config).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        a.ingest(&schema, &[item(1, "corrupt", 1_800_000_000_000_000)], None)
            .unwrap();
        let mut payload = a.store.all::<PayloadRow>().unwrap().remove(0);
        payload.journal[0] ^= 1;
        a.store.put(&payload).unwrap();
        drop(a);
        let options = Options {
            to: Backend::Sqlite,
            output: root.path().join("target"),
            resume: false,
            connection: ConnectionOptions::default(),
        };
        assert!(migration::run(&source, &options).is_err());
        assert!(Archive::open(&options.output, false).is_err());
        assert!(options.output.join("migration.json").exists());
        cleanup(&config);
    }
}
#[test]
fn legacy_config_and_catalog_ids_are_preserved() {
    for config in configs() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let mut a = Archive::init(&source, &Config::default()).unwrap();
        let schema = a.register_schema(1, SCHEMA).unwrap();
        a.ingest(&schema, &[item(1, "legacy", 1_800_000_000_000_000)], None)
            .unwrap();
        a.enqueue_work("verify", "work", &json!({}), false).unwrap();
        a.store.sqlite().unwrap().execute_batch("DELETE FROM settings WHERE key='extensions_version'; UPDATE settings SET rowid=1 WHERE key='dataset_id'; UPDATE sqlite_sequence SET seq=100 WHERE name='observations'; UPDATE sqlite_sequence SET seq=200 WHERE name='work';").unwrap();
        drop(a);
        let path = source.join("config.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .filter(|s| !s.starts_with("backend =") && !s.starts_with("dataset_id ="))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path, text).unwrap();
        let options = Options {
            to: config.backend,
            output: root.path().join("target"),
            resume: false,
            connection: config
                .clickhouse
                .as_ref()
                .map(|c| ConnectionOptions {
                    clickhouse_url: c.url.clone(),
                    clickhouse_database: c.database.clone(),
                    ..Default::default()
                })
                .unwrap_or_default(),
        };
        migration::run(&source, &options).unwrap();
        let mut a = Archive::open(&options.output, true).unwrap();
        assert_eq!(
            a.enqueue_work("verify", "next", &json!({}), false).unwrap(),
            201
        );
        assert_eq!(
            a.ingest(&schema, &[item(2, "next", 1_800_000_000_000_001)], None)
                .unwrap(),
            vec![101]
        );
        drop(a);
        cleanup(&config);
    }
}
