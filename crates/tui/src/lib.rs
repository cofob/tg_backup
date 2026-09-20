//! Shared terminal interface; backends own transport and archive access.
use anyhow::Result;
use std::{future::Future, pin::Pin};
use tg_backup_protocol::explorer::{Request, Response};
pub type BackendFuture<'a> = Pin<Box<dyn Future<Output = Result<Response>> + Send + 'a>>;
pub type TransferFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<u64>>> + Send + 'a>>;
pub struct AttachmentTransfer {
    pub hash: String,
    pub staging: std::path::PathBuf,
    pub cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub progress: export::Progress,
    pub updates: export::ProgressSender,
}
pub trait Backend: Send + Sync {
    /// Optional streaming transport for attachments, also supported by pre-explorer servers.
    /// The shared exporter owns staging/publication and verifies the resulting bytes.
    fn transfer_attachment(&self, _transfer: AttachmentTransfer) -> TransferFuture<'_> {
        Box::pin(async { Ok(None) })
    }
    fn request(&self, request: Request) -> BackendFuture<'_>;
}
mod app;
pub mod export;
pub use app::run;
