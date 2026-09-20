/// Explicitly opted-in read-only cloud smoke test. Authenticate this dataset first.
#[tokio::test]
#[ignore = "requires TG_BACKUP_SMOKE_DATASET and authenticated Telegram credentials"]
async fn authenticated_limited_sync() {
    let root = std::path::PathBuf::from(
        std::env::var("TG_BACKUP_SMOKE_DATASET")
            .expect("set TG_BACKUP_SMOKE_DATASET to a disposable authenticated v2 dataset"),
    );
    let result = tg_backup::telegram::sync(
        &root,
        tg_backup::telegram::SyncOptions {
            max_messages: Some(10),
            max_seconds: Some(120),
            max_media_bytes: Some(0),
            attachment_selector: Some("false".into()),
            ..Default::default()
        },
    )
    .await;
    let archive = tg_backup::archive::Archive::open(&root, false).unwrap();
    archive.verify().unwrap();
    if let Err(error) = result {
        panic!("limited sync ended with diagnostics: {error:#}");
    }
}
