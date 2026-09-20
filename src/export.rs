use crate::{archive::Archive, query::Query};
use anyhow::Result;
use std::io::Write;
pub use tg_backup_protocol::{Format, escape};
pub fn export(
    archive: &Archive,
    query: &Query,
    format: Format,
    out: impl Write,
    attachments: Option<&std::path::Path>,
) -> Result<u64> {
    export_media(
        archive,
        query,
        format,
        out,
        attachments,
        tg_backup_protocol::MediaSelection::Original,
    )
}
pub fn export_media(
    archive: &Archive,
    query: &Query,
    format: Format,
    mut out: impl Write,
    attachments: Option<&std::path::Path>,
    selection: tg_backup_protocol::MediaSelection,
) -> Result<u64> {
    let mut query = query.clone();
    let mut writer = tg_backup_protocol::export::RecordWriter::new(&mut out, format)?;
    loop {
        let page = archive.query(&query)?;
        for record in page.records {
            writer.record(&record)?;
            if let Some(dest) = attachments {
                for hash in record.media_hashes(selection) {
                    std::fs::create_dir_all(dest)?;
                    let target = dest.join(&hash);
                    if !target.exists() {
                        std::fs::copy(
                            crate::media::attachment_path(&archive.root, &hash)?,
                            target,
                        )?;
                    }
                }
            }
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
    }
    Ok(writer.finish()?)
}
