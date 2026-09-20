use grammers_session::{
    Session,
    types::{ChannelKind, PeerAuth, PeerId, PeerInfo},
};
use rusqlite::{Connection, params};
use std::{path::Path, process::Command};
use tg_backup::{archive::Archive, config::Config, session::SqliteSession};

fn make_v1(path: &Path, test_mode: bool, key_length: usize) {
    let db = Connection::open(path).unwrap();
    db.execute_batch(
        "CREATE TABLE sessions(dc_id INTEGER PRIMARY KEY,api_id INTEGER,test_mode INTEGER,auth_key BLOB,date INTEGER NOT NULL,user_id INTEGER,is_bot INTEGER);
         CREATE TABLE peers(id INTEGER PRIMARY KEY,access_hash INTEGER,type INTEGER NOT NULL,username TEXT,phone_number TEXT,last_update_on INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE version(number INTEGER PRIMARY KEY);",
    )
    .unwrap();
    db.execute(
        "INSERT INTO sessions VALUES(?,?,?,?,?,?,?)",
        params![2, 12345, test_mode, vec![42_u8; key_length], 1, 17, 0],
    )
    .unwrap();
    for (id, hash, kind) in [
        (17_i64, Some(123_i64), "user"),
        (18, Some(124), "bot"),
        (-42, None, "group"),
        (-1_000_000_000_099, Some(125), "channel"),
        (-1_000_000_000_100, Some(126), "supergroup"),
    ] {
        db.execute(
            "INSERT INTO peers(id,access_hash,type) VALUES(?,?,?)",
            params![id, hash, kind],
        )
        .unwrap();
    }
    db.execute("INSERT INTO version VALUES(3)", []).unwrap();
}

fn run_migration(source: &Path, dataset: &Path) -> std::process::Output {
    Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/migrate_v1_session.py"
        ))
        .args(["--source", source.to_str().unwrap()])
        .args(["--dataset", dataset.to_str().unwrap()])
        .output()
        .unwrap()
}

#[tokio::test]
async fn migrates_v1_authorization_and_peer_cache() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("old.session");
    let dataset = temp.path().join("archive");
    make_v1(&source, false, 256);
    Archive::init(&dataset, &Config::default()).unwrap();

    let output = run_migration(&source, &dataset);
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["account_id"], "17");
    assert_eq!(report["peers_migrated"], 5);
    assert_eq!(report["source_preserved"], true);
    assert!(source.exists());

    let session = SqliteSession::open(dataset.join("session.sqlite3"))
        .await
        .unwrap();
    assert_eq!(session.home_dc_id().unwrap(), 2);
    assert_eq!(
        session.dc_option(2).unwrap().unwrap().auth_key,
        Some([42_u8; 256])
    );
    assert_eq!(
        session.peer(PeerId::self_user()).await.unwrap(),
        Some(PeerInfo::User {
            id: 17,
            auth: Some(PeerAuth::from_hash(123)),
            bot: Some(false),
            is_self: Some(true),
        })
    );
    assert_eq!(
        session.peer(PeerId::user(18).unwrap()).await.unwrap(),
        Some(PeerInfo::User {
            id: 18,
            auth: Some(PeerAuth::from_hash(124)),
            bot: Some(true),
            is_self: None,
        })
    );
    assert_eq!(
        session.peer(PeerId::chat(42).unwrap()).await.unwrap(),
        Some(PeerInfo::Chat { id: 42 })
    );
    assert_eq!(
        session.peer(PeerId::channel(99).unwrap()).await.unwrap(),
        Some(PeerInfo::Channel {
            id: 99,
            auth: Some(PeerAuth::from_hash(125)),
            kind: Some(ChannelKind::Broadcast),
        })
    );
    assert_eq!(
        session.peer(PeerId::channel(100).unwrap()).await.unwrap(),
        Some(PeerInfo::Channel {
            id: 100,
            auth: Some(PeerAuth::from_hash(126)),
            kind: Some(ChannelKind::Megagroup),
        })
    );

    let auth: tg_backup::telegram::AuthConfig =
        toml::from_str(&std::fs::read_to_string(dataset.join("auth.toml")).unwrap()).unwrap();
    assert_eq!(auth.api_id, 12345);
    match auth.api_hash {
        tg_backup_credentials::Secret::Environment { name } => {
            assert_eq!(name, "TG_BACKUP_API_HASH")
        }
        other => panic!("unexpected API hash reference: {other:?}"),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(dataset.join("session.sqlite3"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    let second = run_migration(&source, &dataset);
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("refusing to overwrite"));
}

#[test]
fn rejects_incompatible_sessions_without_publishing_a_target() {
    for (name, test_mode, key_length, expected) in [
        ("test-mode", true, 256, "test-datacenter"),
        ("short-key", false, 255, "256-byte"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join(format!("{name}.session"));
        let dataset = temp.path().join("archive");
        make_v1(&source, test_mode, key_length);
        Archive::init(&dataset, &Config::default()).unwrap();
        let output = run_migration(&source, &dataset);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(expected));
        assert!(!dataset.join("session.sqlite3").exists());
        assert!(!dataset.join("auth.toml").exists());
    }
}
