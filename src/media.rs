use crate::archive::{Archive, sync_dir};
use anyhow::{Context, Result, ensure};
use rusqlite::params;
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub fn attachment_path(root: &Path, hash: &str) -> Result<PathBuf> {
    ensure!(
        hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid attachment hash"
    );
    Ok(root.join("attachments").join(&hash[..2]).join(hash))
}
pub fn file_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut h = blake3::Hasher::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().to_hex().to_string())
}
pub fn stage_path(root: &Path, id: &str) -> PathBuf {
    root.join("staging")
        .join(format!("{}.part", blake3::hash(id.as_bytes()).to_hex()))
}
impl Archive {
    pub fn queue_media(
        &self,
        id: &str,
        location: &Value,
        dc: i32,
        size: Option<u64>,
        observation: i64,
    ) -> Result<()> {
        self.db.execute("INSERT INTO media(id,location,dc,size) VALUES(?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET location=excluded.location,dc=excluded.dc,size=COALESCE(excluded.size,media.size)",params![id,location.to_string(),dc,size])?;
        self.db.execute(
            "INSERT OR IGNORE INTO media_refs VALUES(?1,?2)",
            params![id, observation],
        )?;
        Ok(())
    }
    pub fn append_media(&self, id: &str, offset: u64, bytes: &[u8]) -> Result<u64> {
        let path = stage_path(&self.root, id);
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        ensure!(
            f.metadata()?.len() >= offset,
            "staging file shorter than checkpoint"
        );
        f.set_len(offset)?;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(bytes)?;
        f.sync_all()?;
        let new = offset + bytes.len() as u64;
        self.db.execute(
            "UPDATE media SET offset=?2,status='downloading',error=NULL WHERE id=?1",
            params![id, new],
        )?;
        Ok(new)
    }
    pub fn finish_media(&self, id: &str) -> Result<String> {
        let path = stage_path(&self.root, id);
        let len = fs::metadata(&path)?.len();
        let expected: Option<u64> =
            self.db
                .query_row("SELECT size FROM media WHERE id=?1", [id], |r| r.get(0))?;
        if let Some(expected) = expected {
            ensure!(
                len == expected,
                "attachment size mismatch: expected {expected}, got {len}"
            );
        } else {
            ensure!(len > 0, "empty attachment without declared size");
        }
        let hash = file_hash(&path)?;
        let dest = attachment_path(&self.root, &hash)?;
        fs::create_dir_all(dest.parent().context("attachment directory")?)?;
        if dest.exists() {
            ensure!(file_hash(&dest)? == hash, "existing attachment corrupt");
            fs::remove_file(&path)?;
        } else {
            fs::rename(&path, &dest)?;
            File::open(&dest)?.sync_all()?;
            sync_dir(dest.parent().unwrap())?;
        }
        self.db.execute(
            "UPDATE media SET hash=?2,status='complete',offset=?3,error=NULL WHERE id=?1",
            params![id, hash, len],
        )?;
        Ok(hash)
    }
    pub fn media_error(&self, id: &str, error: &str, retry_at: i64) -> Result<()> {
        self.db.execute(
            "UPDATE media SET status='failed',attempts=attempts+1,error=?2,retry_at=?3 WHERE id=?1",
            params![id, error, retry_at],
        )?;
        Ok(())
    }
}

/// Derived attachment identities in a native TL value, including nested media.
fn identities(value: &Value, ids: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            if let Some(id) = crate::tl::integer(&value["id"]) {
                match value["_"].as_str() {
                    Some("photo") => {
                        ids.insert(format!("photo:{id}:*"));
                    }
                    Some("document") => {
                        ids.insert(format!("document:{id}"));
                    }
                    _ => {}
                }
            }
            for v in map.values() {
                identities(v, ids);
            }
        }
        Value::Array(values) => {
            for v in values {
                identities(v, ids);
            }
        }
        _ => {}
    }
}
impl Archive {
    pub fn link_media(&self, observation: i64, value: &Value) -> Result<()> {
        let mut ids = std::collections::BTreeSet::new();
        identities(value, &mut ids);
        for id in ids {
            self.db.execute(
                "INSERT OR IGNORE INTO media_refs SELECT id,?1 FROM media WHERE id GLOB ?2",
                params![observation, id],
            )?;
        }
        Ok(())
    }
    pub fn attachment_hashes(&self, value: &Value) -> Result<Vec<String>> {
        let mut ids = std::collections::BTreeSet::new();
        identities(value, &mut ids);
        let mut hashes = std::collections::BTreeSet::new();
        for id in ids {
            let mut st = self
                .db
                .prepare("SELECT hash FROM media WHERE id GLOB ?1 AND status='complete'")?;
            for hash in st.query_map([id], |r| r.get::<_, String>(0))? {
                hashes.insert(hash?);
            }
        }
        Ok(hashes.into_iter().collect())
    }
}
