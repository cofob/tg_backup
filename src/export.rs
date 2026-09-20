use crate::{
    archive::Archive,
    query::{Query, public_json},
};
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
    let mut count = 0;
    if matches!(format, Format::Json) {
        write!(out, "[")?;
    }
    if matches!(format, Format::Html) {
        write!(
            out,
            "<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>Telegram archive</title><style>body{{font:16px system-ui;max-width:70rem;margin:2rem auto;padding:1rem}}article{{border-bottom:1px solid #ccc;padding:1rem}}pre{{white-space:pre-wrap;overflow-wrap:anywhere}}</style>"
        )?;
    }
    loop {
        let page = archive.query(&query)?;
        for record in page.records {
            match format {
                Format::Json | Format::Ndjson => {
                    if count > 0 && matches!(format, Format::Json) {
                        write!(out, ",")?;
                    }
                    let mut value = serde_json::to_value(&record)?;
                    public_json(&mut value);
                    serde_json::to_writer(&mut out, &value)?;
                    writeln!(out)?;
                }
                Format::Txt => writeln!(
                    out,
                    "[{}] {} {}{}\n{}\n",
                    record.observed_at,
                    record.key,
                    record.source,
                    if record.deleted { " [deleted]" } else { "" },
                    crate::tl::text(&record.data)
                )?,
                Format::Html => write!(
                    out,
                    "<article><h3>{}</h3><small>{} · {}{}</small><pre>{}</pre></article>",
                    escape(&record.key),
                    record.observed_at,
                    escape(&record.source),
                    if record.deleted { " · deleted" } else { "" },
                    escape(&crate::tl::text(&record.data))
                )?,
            }
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
            count += 1;
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
    }
    if matches!(format, Format::Json) {
        writeln!(out, "]")?;
    }
    out.flush()?;
    Ok(count)
}
