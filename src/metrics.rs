//! Cached, low-cardinality operational metrics on a separate listener.
use crate::archive::Archive;
use anyhow::Result;
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};
#[derive(Clone)]
struct Metrics {
    text: Arc<RwLock<String>>,
    token: Option<String>,
}
async fn scrape(
    State(s): State<Metrics>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    if let Some(token) = s.token {
        let supplied = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if blake3::hash(supplied.as_bytes()) != blake3::hash(format!("Bearer {token}").as_bytes()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok((
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        s.text.read().unwrap().clone(),
    ))
}
pub fn snapshot(a: &Archive) -> Result<String> {
    let mut out = String::new();
    for (name, sql) in [
        ("observations", "SELECT COUNT(*) FROM observations"),
        ("objects", "SELECT COUNT(*) FROM heads"),
        (
            "journal_payloads",
            "SELECT COUNT(*) FROM payloads WHERE journal IS NOT NULL",
        ),
        (
            "media_pending",
            "SELECT COUNT(*) FROM media WHERE status!='complete'",
        ),
        (
            "media_bytes",
            "SELECT COALESCE(SUM(n),0) FROM (SELECT MAX(offset) n FROM media WHERE status='complete' GROUP BY hash)",
        ),
        (
            "collector_failures",
            "SELECT COUNT(*) FROM coverage WHERE status IN ('incomplete','failed_or_inaccessible','recovery_error')",
        ),
        (
            "work_queued",
            "SELECT COUNT(*) FROM work WHERE state='queued'",
        ),
        (
            "work_running",
            "SELECT COUNT(*) FROM work WHERE state='running'",
        ),
        (
            "work_failed",
            "SELECT COUNT(*) FROM work WHERE state='failed'",
        ),
        (
            "representation_bytes",
            "SELECT COALESCE(SUM(bytes),0) FROM representations",
        ),
    ] {
        let value: i64 = a.db.query_row(sql, [], |r| r.get(0))?;
        out += &format!("# TYPE tg_backup_{name} gauge\ntg_backup_{name} {value}\n");
    }
    out += &format!(
        "tg_backup_window_open {}\ntg_backup_cpu_budget {}\ntg_backup_scrape_timestamp_seconds {}\n",
        u8::from(a.config.schedule.allows(chrono::Utc::now())),
        a.config.resources.cpus,
        chrono::Utc::now().timestamp()
    );
    Ok(out)
}
pub async fn serve(root: PathBuf, bind: std::net::SocketAddr, token: Option<String>) -> Result<()> {
    let state = Metrics {
        text: Arc::new(RwLock::new(String::new())),
        token,
    };
    let cached = state.text.clone();
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let updater = tokio::spawn(async move {
        loop {
            let root = root.clone();
            match tokio::task::spawn_blocking(move || snapshot(&Archive::open(&root, false)?)).await
            {
                Ok(Ok(value)) => *cached.write().unwrap() = value,
                _ => *cached.write().unwrap() = "tg_backup_snapshot_available 0\n".into(),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    let result = axum::serve(
        listener,
        Router::new()
            .route("/metrics", get(scrape))
            .with_state(state),
    )
    .with_graceful_shutdown(async {
        let _ = crate::shutdown::signal().await;
    })
    .await;
    updater.abort();
    result?;
    Ok(())
}
