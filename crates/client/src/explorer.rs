use anyhow::{Result, ensure};
use tg_backup_protocol::explorer::*;
pub struct HttpBackend {
    url: String,
    token: Option<String>,
    client: reqwest::Client,
}
impl HttpBackend {
    pub fn new(url: String, token: Option<String>) -> Result<Self> {
        Ok(Self {
            url,
            token,
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .read_timeout(std::time::Duration::from_secs(30))
                .build()?,
        })
    }
    fn auth(&self, r: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.token {
            r.bearer_auth(token)
        } else {
            r
        }
    }
}
impl tg_backup_tui::Backend for HttpBackend {
    fn transfer_attachment(
        &self,
        transfer: tg_backup_tui::AttachmentTransfer,
    ) -> tg_backup_tui::TransferFuture<'_> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            ensure!(
                transfer.hash.len() == 64 && transfer.hash.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid attachment hash"
            );
            let mut response = self
                .auth(
                    self.client
                        .get(format!("{}/v2/attachments/{}", self.url, transfer.hash)),
                )
                .send()
                .await?
                .error_for_status()?;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&transfer.staging)
                .await?;
            let mut bytes = 0;
            let mut progress = transfer.progress;
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    !transfer.cancel.load(std::sync::atomic::Ordering::Relaxed),
                    "export cancelled"
                );
                file.write_all(&chunk).await?;
                bytes += chunk.len() as u64;
                progress.bytes += chunk.len() as u64;
                let _ = transfer.updates.try_send(progress.clone());
            }
            file.sync_all().await?;
            Ok(Some(bytes))
        })
    }

    fn request(&self, request: Request) -> tg_backup_tui::BackendFuture<'_> {
        Box::pin(async move {
            let (path, body) = match &request {
                Request::Capabilities => ("capabilities", None),
                Request::Query(q) => ("query", Some(serde_json::to_value(q)?)),
                Request::Browse(q) => ("browse", Some(serde_json::to_value(q)?)),
                Request::Binary(q) => ("binary", Some(serde_json::to_value(q)?)),
            };
            let url = if path == "query" {
                format!("{}/v2/query", self.url)
            } else {
                format!("{}/v2/explorer/{path}", self.url)
            };
            let builder = if let Some(body) = body {
                self.client.post(url).json(&body)
            } else {
                self.client.get(url)
            };
            let mut response = self
                .auth(builder)
                .timeout(std::time::Duration::from_secs(30))
                .send()
                .await?;
            if matches!(request, Request::Capabilities)
                && matches!(response.status().as_u16(), 404 | 405)
            {
                return Ok(Response::Capabilities(Capabilities {
                    version: 0,
                    storage: false,
                    conversations: false,
                }));
            }
            let status = response.status();
            let mut body = vec![];
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    body.len() + chunk.len() <= 80 * 1024 * 1024,
                    "API response exceeds size limit"
                );
                body.extend(chunk);
            }
            ensure!(
                status.is_success(),
                "API {status}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(2048)
                    .collect::<String>()
            );
            Ok(match request {
                Request::Capabilities => Response::Capabilities(serde_json::from_slice(&body)?),
                Request::Query(_) => Response::Query(serde_json::from_slice(&body)?),
                Request::Browse(_) => Response::Browse(serde_json::from_slice(&body)?),
                Request::Binary(_) => Response::Binary(serde_json::from_slice(&body)?),
            })
        })
    }
}
