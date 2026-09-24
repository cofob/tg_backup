use crate::{
    archive::Archive,
    query::{Query, public_json},
};
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
#[derive(Clone)]
pub struct ApiState {
    pub root: PathBuf,
    pub token: Option<String>,
}
type ApiResult = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;
fn error(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":e.to_string()})),
    )
}
async fn authorize(State(state): State<Arc<ApiState>>, req: Request, next: Next) -> Response {
    if let Some(token) = &state.token {
        let expected = format!("Bearer {token}");
        let provided = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // Hash before comparison to avoid leaking token-prefix matches through timing.
        if blake3::hash(expected.as_bytes()) != blake3::hash(provided.as_bytes()) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"unauthorized"})),
            )
                .into_response();
        }
    }
    next.run(req).await
}
async fn run_query(state: Arc<ApiState>, q: Query) -> ApiResult {
    tokio::task::spawn_blocking(move || {
        let archive = Archive::open(&state.root, false)?;
        let mut value = serde_json::to_value(archive.query(&q)?)?;
        public_json(&mut value);
        Ok::<_, anyhow::Error>(Json(value))
    })
    .await
    .map_err(error)?
    .map_err(error)
}
async fn query(State(state): State<Arc<ApiState>>, Json(q): Json<Query>) -> ApiResult {
    run_query(state, q).await
}
async fn get_query(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(q): axum::extract::Query<Query>,
) -> ApiResult {
    run_query(state, q).await
}
async fn messages(State(state): State<Arc<ApiState>>, Json(mut q): Json<Query>) -> ApiResult {
    q.kind = Some("message".into());
    run_query(state, q).await
}
async fn versions(State(state): State<Arc<ApiState>>, Json(mut q): Json<Query>) -> ApiResult {
    q.all_versions = true;
    run_query(state, q).await
}
async fn events(State(state): State<Arc<ApiState>>, Json(mut q): Json<Query>) -> ApiResult {
    q.all_versions = true;
    q.selector = format!(
        "({}) and (kind = update or kind = presence or kind = read_state or kind = unresolved_deletion)",
        q.selector
    );
    run_query(state, q).await
}
async fn history(
    State(state): State<Arc<ApiState>>,
    Path(key): Path<String>,
    axum::extract::Query(mut q): axum::extract::Query<Query>,
) -> ApiResult {
    q.key = Some(key);
    q.all_versions = true;
    run_query(state, q).await
}
#[derive(serde::Deserialize, Default)]
struct TableOptions {
    #[serde(default)]
    details: bool,
    #[serde(default)]
    after: i64,
    limit: Option<usize>,
}
async fn table(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    axum::extract::Query(options): axum::extract::Query<TableOptions>,
) -> ApiResult {
    tokio::task::spawn_blocking(move || {
        let a = Archive::open(&state.root, false)?;
        if name == "status" {
            a.operational_status(options.details)
        } else if name == "work" {
            a.work_page(options.after, options.limit.unwrap_or(50))
        } else {
            a.list_table(if name == "attachments" {
                "media"
            } else {
                &name
            })
        }
    })
    .await
    .map_err(error)?
    .map(Json)
    .map_err(error)
}
async fn dialog_status(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(options): axum::extract::Query<crate::dialog_status::Options>,
) -> ApiResult {
    tokio::task::spawn_blocking(move || {
        let archive = Archive::open(&state.root, false)?;
        Ok::<_, anyhow::Error>(Json(serde_json::to_value(
            archive.dialog_status_page(&options)?,
        )?))
    })
    .await
    .map_err(error)?
    .map_err(error)
}
async fn attachment(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> std::result::Result<Response, (StatusCode, Json<Value>)> {
    let root = state.root.clone();
    let lookup = hash.clone();
    let file = tokio::task::spawn_blocking(move || {
        let a = Archive::open(&root, false)?;
        let exists: bool = a.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM media WHERE hash=?1 AND status='complete' UNION SELECT 1 FROM representations WHERE hash=?1)",
            [&lookup],
            |r| r.get(0),
        )?;
        ensure!(exists, "attachment not found");
        Ok::<_, anyhow::Error>(std::fs::File::open(crate::media::attachment_path(
            &root, &lookup,
        )?)?)
    })
    .await
    .map_err(error)?
    .map_err(error)?;
    let file = tokio::fs::File::from_std(file);
    let size = file.metadata().await.map_err(error)?.len();
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (header::CONTENT_LENGTH, size.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{hash}\""),
            ),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".into()),
        ],
        Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    )
        .into_response())
}
async fn explorer_capabilities(State(_state): State<Arc<ApiState>>) -> ApiResult {
    Ok(Json(
        serde_json::to_value(tg_backup_protocol::explorer::Capabilities {
            version: 1,
            storage: true,
            conversations: true,
        })
        .map_err(error)?,
    ))
}
async fn explorer_browse(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<tg_backup_protocol::explorer::BrowseRequest>,
) -> ApiResult {
    tokio::task::spawn_blocking(move || {
        let archive = Archive::open(&state.root, false)?;
        let mut value = serde_json::to_value(archive.explore(&request)?)?;
        public_json(&mut value);
        Ok::<_, anyhow::Error>(Json(value))
    })
    .await
    .map_err(error)?
    .map_err(error)
}
async fn explorer_binary(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<tg_backup_protocol::explorer::BinaryRequest>,
) -> ApiResult {
    tokio::task::spawn_blocking(move || {
        let archive = Archive::open(&state.root, false)?;
        Ok::<_, anyhow::Error>(Json(serde_json::to_value(
            archive.explorer_binary(&request)?,
        )?))
    })
    .await
    .map_err(error)?
    .map_err(error)
}
pub fn router(state: ApiState) -> Router {
    let state = Arc::new(state);
    Router::new()
        .route("/v2/explorer/capabilities", get(explorer_capabilities))
        .route("/v2/explorer/browse", post(explorer_browse))
        .route("/v2/explorer/binary", post(explorer_binary))
        .route("/v2/query", post(query).get(get_query))
        .route("/v2/objects", get(get_query))
        .route("/v2/messages", post(messages))
        .route("/v2/history/{key}", get(history))
        .route("/v2/versions", post(versions))
        .route("/v2/events", post(events))
        .route("/v2/attachments/{hash}", get(attachment))
        .route("/v2/dialog-status", get(dialog_status))
        .route("/v2/{name}", get(table))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state)
}
pub async fn serve(root: PathBuf, bind: std::net::SocketAddr, token: Option<String>) -> Result<()> {
    ensure!(
        bind.ip().is_loopback() || token.as_ref().is_some_and(|t| !t.is_empty()),
        "non-loopback HTTP requires a bearer token"
    );
    Archive::open(&root, false)?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind,"read-only archive API listening");
    axum::serve(listener, router(ApiState { root, token }))
        .with_graceful_shutdown(async {
            let _ = crate::shutdown::signal().await;
        })
        .await?;
    Ok(())
}
