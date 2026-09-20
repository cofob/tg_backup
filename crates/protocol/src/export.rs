//! Streaming record formatting shared by both CLIs and the TUI.
use crate::{Format, Record, escape, public_json, text};
use std::io::{self, Write};
pub struct RecordWriter<W: Write> {
    out: W,
    format: Format,
    count: u64,
}
impl<W: Write> RecordWriter<W> {
    pub fn new(mut out: W, format: Format) -> io::Result<Self> {
        match format {
            Format::Json => write!(out, "[")?,
            Format::Html => write!(
                out,
                "<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>Telegram archive</title><style>body{{font:16px system-ui;max-width:70rem;margin:2rem auto}}article{{border-bottom:1px solid #ccc;padding:1rem}}pre{{white-space:pre-wrap;overflow-wrap:anywhere}}</style>"
            )?,
            _ => {}
        }
        Ok(Self {
            out,
            format,
            count: 0,
        })
    }
    pub fn record(&mut self, record: &Record) -> io::Result<()> {
        match self.format {
            Format::Json | Format::Ndjson => {
                if self.count > 0 && matches!(self.format, Format::Json) {
                    write!(self.out, ",")?;
                }
                let mut value = serde_json::to_value(record)?;
                public_json(&mut value);
                serde_json::to_writer(&mut self.out, &value)?;
                writeln!(self.out)?;
            }
            Format::Txt => writeln!(
                self.out,
                "[{}] {} {}{}\n{}\n",
                record.observed_at,
                record.key,
                record.source,
                if record.deleted { " [deleted]" } else { "" },
                text(&record.data)
            )?,
            Format::Html => write!(
                self.out,
                "<article><h3>{}</h3><small>{} · {}{}</small><pre>{}</pre></article>",
                escape(&record.key),
                record.observed_at,
                escape(&record.source),
                if record.deleted { " · deleted" } else { "" },
                escape(&text(&record.data))
            )?,
        }
        self.count += 1;
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<u64> {
        if matches!(self.format, Format::Json) {
            writeln!(self.out, "]")?;
        }
        self.out.flush()?;
        Ok(self.count)
    }
}
