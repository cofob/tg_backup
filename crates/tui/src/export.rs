use crate::Backend;
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tg_backup_protocol::{Format, MediaSelection, Query, Record, explorer::*};

#[derive(Clone)]
pub enum Source {
    Query(Query),
    Record(Box<Record>),
    Rows(BrowseRequest),
    Row(Box<Entry>),
    Binary(BinaryRef),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Options {
    pub output: PathBuf,
    pub format: Format,
    pub attachments: Option<PathBuf>,
    pub media: MediaSelection,
    pub overwrite: bool,
}
#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub records: u64,
    pub bytes: u64,
}
pub type ProgressSender = tokio::sync::mpsc::Sender<Progress>;
struct Staging {
    path: PathBuf,
}
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
fn stage(target: &Path, overwrite: bool) -> Result<(Staging, std::fs::File)> {
    ensure!(!target.as_os_str().is_empty(), "output path is required");
    ensure!(
        overwrite || !target.exists(),
        "output exists; confirm overwrite first"
    );
    let path = target.with_extension(format!("{}.part", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path)?;
    Ok((Staging { path }, file))
}
fn publish(staging: &Staging, target: &Path, overwrite: bool) -> Result<()> {
    if overwrite {
        std::fs::rename(&staging.path, target)?;
    } else {
        std::fs::hard_link(&staging.path, target)
            .context("cannot publish output without overwriting")?;
        std::fs::remove_file(&staging.path)?;
    }
    Ok(())
}
fn check(cancel: &AtomicBool) -> Result<()> {
    ensure!(
        !cancel.load(Ordering::Relaxed),
        "export cancelled; incomplete output removed"
    );
    Ok(())
}
pub async fn binary_bytes(
    backend: &dyn Backend,
    reference: BinaryRef,
    max: u64,
) -> Result<Vec<u8>> {
    let mut out = vec![];
    let mut offset = 0;
    loop {
        let Response::Binary(page) = backend
            .request(Request::Binary(BinaryRequest {
                reference: reference.clone(),
                offset,
                limit: BINARY_CHUNK,
            }))
            .await?
        else {
            bail!("invalid binary response")
        };
        ensure!(
            page.total <= max,
            "preview exceeds size limit ({max} bytes)"
        );
        let bytes = hex::decode(page.hex)?;
        out.extend(bytes);
        ensure!(out.len() as u64 <= max, "preview exceeds limit");
        if let Some(next) = page.next_offset {
            ensure!(next > offset, "binary cursor did not advance");
            offset = next;
        } else {
            break;
        }
    }
    if let BinaryRef::Attachment { hash } = reference {
        ensure!(
            blake3::hash(&out).to_hex().as_str() == hash,
            "attachment checksum mismatch"
        );
    }
    Ok(out)
}
async fn binary_to_file(
    backend: &dyn Backend,
    reference: BinaryRef,
    target: &Path,
    overwrite: bool,
    cancel: &Arc<AtomicBool>,
    progress: &mut Progress,
    updates: &ProgressSender,
) -> Result<()> {
    let (staging, mut out) = stage(target, overwrite)?;
    if let BinaryRef::Attachment { hash } = &reference {
        let transfer = crate::AttachmentTransfer {
            hash: hash.clone(),
            staging: staging.path.clone(),
            cancel: cancel.clone(),
            progress: progress.clone(),
            updates: updates.clone(),
        };
        if let Some(bytes) = backend.transfer_attachment(transfer).await? {
            check(cancel)?;
            let path = staging.path.clone();
            let expected = hash.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut file = std::fs::File::open(path)?;
                let mut digest = blake3::Hasher::new();
                let read = std::io::copy(&mut file, &mut digest)?;
                ensure!(
                    read == bytes && digest.finalize().to_hex().as_str() == expected,
                    "attachment checksum mismatch"
                );
                Ok(())
            })
            .await??;
            out.sync_all()?;
            drop(out);
            check(cancel)?;
            progress.bytes += bytes;
            publish(&staging, target, overwrite)?;
            let _ = updates.try_send(progress.clone());
            return Ok(());
        }
    }
    let mut offset = 0;
    let mut digest = blake3::Hasher::new();
    let mut total = None;
    loop {
        check(cancel)?;
        let Response::Binary(page) = backend
            .request(Request::Binary(BinaryRequest {
                reference: reference.clone(),
                offset,
                limit: BINARY_CHUNK,
            }))
            .await?
        else {
            bail!("invalid binary response")
        };
        ensure!(
            total.is_none_or(|n| n == page.total),
            "binary changed during export"
        );
        total = Some(page.total);
        let bytes = hex::decode(page.hex)?;
        ensure!(bytes.len() <= BINARY_CHUNK, "oversized binary response");
        out.write_all(&bytes)?;
        digest.update(&bytes);
        offset += bytes.len() as u64;
        progress.bytes += bytes.len() as u64;
        let _ = updates.try_send(progress.clone());
        if let Some(next) = page.next_offset {
            ensure!(next == offset && !bytes.is_empty(), "invalid binary cursor");
        } else {
            ensure!(offset == page.total, "truncated binary response");
            break;
        }
    }
    if let BinaryRef::Attachment { hash } = reference {
        ensure!(
            digest.finalize().to_hex().as_str() == hash,
            "attachment checksum mismatch"
        );
    }
    out.sync_all()?;
    drop(out);
    check(cancel)?;
    publish(&staging, target, overwrite)
}
struct MediaExport<'a> {
    backend: &'a dyn Backend,
    options: &'a Options,
    cancel: &'a Arc<AtomicBool>,
    updates: &'a ProgressSender,
    seen: HashSet<String>,
}
impl MediaExport<'_> {
    async fn copy(&mut self, record: &Record, progress: &mut Progress) -> Result<()> {
        if let Some(directory) = &self.options.attachments {
            std::fs::create_dir_all(directory)?;
            for hash in record.media_hashes(self.options.media) {
                ensure!(
                    hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                    "invalid attachment hash"
                );
                if self.seen.len() >= 4096 {
                    self.seen.clear();
                }
                if !self.seen.insert(hash.clone()) {
                    continue;
                }
                let target = directory.join(&hash);
                if target.exists() {
                    let existing = target.clone();
                    let expected = hash.clone();
                    tokio::task::spawn_blocking(move || -> Result<()> {
                        let mut f = std::fs::File::open(existing)?;
                        let mut h = blake3::Hasher::new();
                        std::io::copy(&mut f, &mut h)?;
                        ensure!(
                            h.finalize().to_hex().as_str() == expected,
                            "existing attachment checksum mismatch"
                        );
                        Ok(())
                    })
                    .await??;
                } else {
                    binary_to_file(
                        self.backend,
                        BinaryRef::Attachment { hash: hash.clone() },
                        &target,
                        false,
                        self.cancel,
                        progress,
                        self.updates,
                    )
                    .await
                    .with_context(|| {
                        format!("attachment {hash} unavailable or failed; export incomplete")
                    })?;
                }
            }
        }
        Ok(())
    }
}
pub async fn run(
    backend: Arc<dyn Backend>,
    source: Source,
    options: Options,
    cancel: Arc<AtomicBool>,
    updates: ProgressSender,
) -> Result<Progress> {
    let mut progress = Progress::default();
    if let Source::Binary(reference) = source {
        binary_to_file(
            backend.as_ref(),
            reference,
            &options.output,
            options.overwrite,
            &cancel,
            &mut progress,
            &updates,
        )
        .await?;
        return Ok(progress);
    }
    let (staging, file) = stage(&options.output, options.overwrite)?;
    let mut out = std::io::BufWriter::new(file);
    match source {
        Source::Query(mut query) => {
            query.cursor = None;
            let mut writer =
                tg_backup_protocol::export::RecordWriter::new(&mut out, options.format)?;
            let mut media = MediaExport {
                backend: backend.as_ref(),
                options: &options,
                cancel: &cancel,
                updates: &updates,
                seen: HashSet::new(),
            };
            loop {
                check(&cancel)?;
                let Response::Query(page) = backend.request(Request::Query(query.clone())).await?
                else {
                    bail!("invalid query response")
                };
                for record in page.records {
                    check(&cancel)?;
                    writer.record(&record)?;
                    media.copy(&record, &mut progress).await?;
                    progress.records += 1;
                    let _ = updates.try_send(progress.clone());
                }
                let Some(next) = page.next_cursor else {
                    break;
                };
                ensure!(
                    query.cursor.as_ref() != Some(&next),
                    "query cursor did not advance"
                );
                query.cursor = Some(next);
            }
            writer.finish()?;
        }
        Source::Record(record) => {
            let mut writer =
                tg_backup_protocol::export::RecordWriter::new(&mut out, options.format)?;
            writer.record(&record)?;
            writer.finish()?;
            MediaExport {
                backend: backend.as_ref(),
                options: &options,
                cancel: &cancel,
                updates: &updates,
                seen: HashSet::new(),
            }
            .copy(&record, &mut progress)
            .await?;
            progress.records = 1;
        }
        Source::Row(entry) => {
            ensure!(
                matches!(options.format, Format::Json | Format::Ndjson),
                "raw rows require json or ndjson"
            );
            serde_json::to_writer(&mut out, &entry.detail)?;
            writeln!(out)?;
            progress.records = 1;
        }
        Source::Rows(mut request) => {
            ensure!(
                matches!(options.format, Format::Json | Format::Ndjson),
                "raw tables require json or ndjson"
            );
            request.cursor = None;
            if matches!(options.format, Format::Json) {
                write!(out, "[")?;
            }
            loop {
                check(&cancel)?;
                let Response::Browse(page) =
                    backend.request(Request::Browse(request.clone())).await?
                else {
                    bail!("invalid browse response")
                };
                for entry in page.entries {
                    check(&cancel)?;
                    if progress.records > 0 && matches!(options.format, Format::Json) {
                        write!(out, ",")?;
                    }
                    serde_json::to_writer(&mut out, &entry.detail)?;
                    writeln!(out)?;
                    progress.records += 1;
                    let _ = updates.try_send(progress.clone());
                }
                let Some(next) = page.next_cursor else {
                    break;
                };
                ensure!(
                    request.cursor.as_ref() != Some(&next),
                    "row cursor did not advance"
                );
                request.cursor = Some(next);
            }
            if matches!(options.format, Format::Json) {
                writeln!(out, "]")?;
            }
        }
        Source::Binary(_) => unreachable!(),
    }
    out.flush()?;
    out.get_ref().sync_all()?;
    progress.bytes += out.get_ref().metadata()?.len();
    drop(out);
    check(&cancel)?;
    publish(&staging, &options.output, options.overwrite)?;
    let _ = updates.try_send(progress.clone());
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Pending;
    impl Backend for Pending {
        fn request(&self, _: Request) -> crate::BackendFuture<'_> {
            Box::pin(std::future::pending())
        }
    }
    #[tokio::test]
    async fn abort_removes_staging_even_during_backend_io() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out.ndjson");
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(run(
            Arc::new(Pending),
            Source::Query(Query::default()),
            Options {
                output: output.clone(),
                format: Format::Ndjson,
                attachments: None,
                media: MediaSelection::Original,
                overwrite: false,
            },
            Arc::new(AtomicBool::new(false)),
            tx,
        ));
        for _ in 0..100 {
            if std::fs::read_dir(dir.path()).unwrap().next().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!output.exists());
        task.abort();
        let _ = task.await;
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
