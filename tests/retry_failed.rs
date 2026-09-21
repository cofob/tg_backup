use serde_json::{Value, json};
use tg_backup::{archive::Archive, config::Config, retry::Target};
fn fixture() -> (tempfile::TempDir, Archive) {
    let dir = tempfile::tempdir().unwrap();
    let a = Archive::init(dir.path(), &Config::default()).unwrap();
    for state in [
        "failed", "complete", "skipped", "paused", "queued", "running",
    ] {
        let id = a
            .enqueue_work("verify", state, &json!({"keep":true}), true)
            .unwrap();
        a.db.execute("UPDATE work SET state=?1,attempts=3,retry_at=99,error='old',progress='{\"fraction\":0.5}' WHERE sequence=?2",rusqlite::params![state,id]).unwrap();
    }
    for state in [
        "failed",
        "complete",
        "deferred",
        "unavailable",
        "pending",
        "downloading",
    ] {
        a.db.execute("INSERT INTO media(id,location,dc,size,status,offset,hash,error,attempts,retry_at) VALUES(?1,'{}',4,42,?1,7,'unchanged','old',8,99)",[state]).unwrap();
    }
    std::fs::write(
        tg_backup::media::stage_path(dir.path(), "failed"),
        b"partial",
    )
    .unwrap();
    (dir, a)
}
#[test]
fn preview_targets_idempotency_and_preserved_data() {
    for target in [Target::All, Target::Work, Target::Attachments] {
        let (dir, mut a) = fixture();
        let preview = a.retry_failed(target, false).unwrap();
        assert_eq!(preview["changed"], json!({"work":0,"attachments":0}));
        assert_eq!(
            a.db.query_row("SELECT attempts FROM work WHERE dedupe='failed'", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            3
        );
        let result = a.retry_failed(target, true).unwrap();
        assert_eq!(result["found"], preview["found"]);
        assert_eq!(result["changed"], preview["found"]);
        assert_eq!(
            a.retry_failed(target, true).unwrap()["found"],
            json!({"work":0,"attachments":0})
        );
        assert_eq!(
            std::fs::read(tg_backup::media::stage_path(dir.path(), "failed")).unwrap(),
            b"partial"
        );
        let media: (i64, i64, String) =
            a.db.query_row(
                "SELECT offset,size,hash FROM media WHERE id='failed'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(media, (7, 42, "unchanged".into()));
        assert_eq!(
            a.db.query_row("SELECT count(*) FROM work", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            6
        );
        assert_eq!(
            a.db.query_row(
                "SELECT count(*) FROM work WHERE dedupe!='failed' AND attempts=3 AND error='old'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            5
        );
        assert_eq!(
            a.db.query_row(
                "SELECT count(*) FROM media WHERE id!='failed' AND attempts=8 AND error='old'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            5
        );
        if !matches!(target, Target::Attachments) {
            let row:(String,i64,i64,String,String,i64)=a.db.query_row("SELECT state,attempts,retry_at,progress,config,automatic FROM work WHERE dedupe='failed'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).unwrap();
            assert_eq!(
                row,
                (
                    "queued".into(),
                    0,
                    0,
                    "{}".into(),
                    "{\"keep\":true}".into(),
                    1
                )
            );
        }
    }
}
#[test]
fn transaction_rolls_back_and_single_resume_resets_budget() {
    let (_dir, mut a) = fixture();
    a.db.execute_batch("CREATE TRIGGER deny_retry BEFORE UPDATE ON media BEGIN SELECT RAISE(ABORT,'test failure'); END;").unwrap();
    assert!(a.retry_failed(Target::All, true).is_err());
    assert_eq!(
        a.retry_failed(Target::All, false).unwrap()["found"],
        json!({"work":1,"attachments":1})
    );
    a.resume_work(1).unwrap();
    let row: (i64, i64, String) =
        a.db.query_row(
            "SELECT attempts,retry_at,progress FROM work WHERE sequence=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (0, 0, "{}".into()));
}
#[test]
fn cli_preview_is_readonly_and_apply_requires_writer_lock() {
    let (dir, a) = fixture();
    let call = |apply: bool| {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_tg-backup"));
        cmd.args(["--dataset"])
            .arg(dir.path())
            .args(["retry-failed", "--target", "all"]);
        if apply {
            cmd.arg("--apply");
        }
        cmd.output().unwrap()
    };
    let preview = call(false);
    assert!(preview.status.success());
    let value: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(value["changed"], json!({"work":0,"attachments":0}));
    let blocked = call(true);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("stop the coordinator"));
    drop(a);
    assert!(call(true).status.success());
}
