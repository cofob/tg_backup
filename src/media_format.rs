//! Conservative input allowlist; file names and claimed MIME types do not grant admission.
use crate::{archive::Archive, tl::integer};
use anyhow::Result;
use serde_json::Value;
use std::{io::Read, path::Path};

fn sticker_document(v: &Value, id: i64) -> bool {
    if v["_"] == "document" && integer(&v["id"]) == Some(id) {
        return v["mime_type"] == "application/x-tgsticker"
            || v["attributes"].as_array().is_some_and(|a| {
                a.iter().any(|v| {
                    matches!(
                        v["_"].as_str(),
                        Some("documentAttributeSticker" | "documentAttributeCustomEmoji")
                    )
                })
            });
    }
    match v {
        Value::Object(m) => m.values().any(|v| sticker_document(v, id)),
        Value::Array(a) => a.iter().any(|v| sticker_document(v, id)),
        _ => false,
    }
}
pub(crate) fn exclusion(a: &Archive, hash: &str) -> Result<Option<&'static str>> {
    let mut st = a.db.prepare("SELECT DISTINCT m.id,r.observation FROM media m JOIN media_refs r ON r.media=m.id WHERE m.hash=?1")?;
    for row in st.query_map([hash], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })? {
        let (id, observation) = row?;
        if let Some(id) = id.strip_prefix("document:").and_then(|id| id.parse().ok())
            && sticker_document(&a.record(observation)?.data, id)
        {
            return Ok(Some("stickers and custom emoji retain originals"));
        }
    }
    file_exclusion(&crate::media::attachment_path(&a.root, hash)?)
}
pub(crate) fn file_exclusion(path: &Path) -> Result<Option<&'static str>> {
    let mut b = Vec::new();
    std::fs::File::open(path)?.take(65536).read_to_end(&mut b)?;
    Ok(if supported(&b) {
        None
    } else {
        Some("unsupported or animated input format; original retained")
    })
}
fn u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes(b.try_into().unwrap())
}
fn u32be(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}
fn vint(b: &[u8], pos: &mut usize, strip: bool) -> Option<u64> {
    let first = *b.get(*pos)?;
    let len = first.leading_zeros() as usize + 1;
    if len > 8 {
        return None;
    }
    let mut n = if strip {
        (first & (0xffu16 >> len) as u8) as u64
    } else {
        first as u64
    };
    for byte in b.get(*pos + 1..*pos + len)? {
        n = (n << 8) | u64::from(*byte);
    }
    *pos += len;
    Some(n)
}
fn supported(b: &[u8]) -> bool {
    if b.starts_with(&[0xff, 0xd8, 0xff]) {
        return true;
    }
    if b.starts_with(b"\x89PNG\r\n\x1a\n") {
        let mut p = 8;
        if b.get(12..16) != Some(b"IHDR") || b.get(8..12).map(u32be) != Some(13) {
            return false;
        }
        while p + 12 <= b.len() {
            let n = u32be(&b[p..p + 4]) as usize;
            let tag = &b[p + 4..p + 8];
            if tag == b"acTL" {
                return false;
            }
            if tag == b"IDAT" {
                return true;
            }
            let Some(next) = p.checked_add(12).and_then(|p| p.checked_add(n)) else {
                return false;
            };
            p = next;
        }
        return false;
    }
    if b.len() >= 20 && &b[..4] == b"RIFF" {
        if &b[8..12] == b"WAVE" {
            return &b[12..16] == b"fmt " && u32le(&b[16..20]) >= 16;
        }
        if &b[8..12] == b"WEBP" {
            return match &b[12..16] {
                b"VP8 " => b.get(23..26) == Some(&[0x9d, 1, 0x2a]),
                b"VP8L" => b.get(20) == Some(&0x2f),
                b"VP8X" => u32le(&b[16..20]) == 10 && b.len() >= 30 && b[20] & 2 == 0,
                _ => false,
            };
        }
        return false;
    }
    if b.len() >= 26 && b.starts_with(b"BM") {
        return u32le(&b[10..14]) >= 26
            && matches!(u32le(&b[14..18]), 12 | 40 | 52 | 56 | 108 | 124);
    }
    if b.len() >= 8 && (b.starts_with(b"II\x2a\0") || b.starts_with(b"MM\0\x2a")) {
        return true;
    }
    if b.starts_with(b"fLaC") && b.len() >= 8 {
        return b[4] & 0x7f == 0 && b[5..8] == [0, 0, 34];
    }
    if b.len() >= 28 && b.starts_with(b"OggS") {
        return b[4] == 0 && b[26] > 0 && b.len() >= 27 + usize::from(b[26]);
    }
    if b.len() >= 10 && b.starts_with(b"ID3") {
        return (2..=4).contains(&b[3]) && b[6..10].iter().all(|n| n & 0x80 == 0);
    }
    if b.len() >= 7 && b[0] == 0xff {
        // MPEG audio: non-reserved version/layer/bitrate/sample rate. ADTS: layer zero.
        let mp3 = b[1] & 0xe0 == 0xe0
            && b[1] & 0x18 != 8
            && b[1] & 6 != 0
            && b[2] >> 4 != 0
            && b[2] >> 4 != 15
            && b[2] & 12 != 12;
        let aac = b[1] & 0xf6 == 0xf0 && (b[2] >> 2) & 15 < 13;
        return mp3 || aac;
    }
    if b.len() >= 16 && &b[4..8] == b"ftyp" {
        let size = u32be(&b[..4]) as usize;
        if size < 16 || size > b.len() || !size.is_multiple_of(4) {
            return false;
        }
        return b[8..size]
            .as_chunks::<4>()
            .0
            .iter()
            .enumerate()
            .any(|(i, brand)| {
                i != 1
                    && matches!(
                        brand,
                        b"isom"
                            | b"iso2"
                            | b"iso4"
                            | b"iso5"
                            | b"iso6"
                            | b"mp41"
                            | b"mp42"
                            | b"M4A "
                            | b"M4V "
                            | b"qt  "
                            | b"avc1"
                            | b"dash"
                    )
            });
    }
    if b.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
        let mut p = 4;
        let Some(n) = vint(b, &mut p, true) else {
            return false;
        };
        let Some(end) = p.checked_add(n as usize).filter(|end| *end <= b.len()) else {
            return false;
        };
        while p < end {
            let Some(id) = vint(&b[..end], &mut p, false) else {
                return false;
            };
            let Some(n) = vint(&b[..end], &mut p, true) else {
                return false;
            };
            let Some(next) = p.checked_add(n as usize).filter(|next| *next <= end) else {
                return false;
            };
            if id == 0x4282 {
                return matches!(&b[p..next], b"webm" | b"matroska");
            }
            p = next;
        }
    }
    false
}
pub(crate) fn codec_supported(s: &Value, still: bool) -> bool {
    let name = s["codec_name"].as_str().unwrap_or("");
    match s["codec_type"].as_str() {
        Some("video") if still => matches!(name, "mjpeg" | "png" | "webp" | "bmp" | "tiff"),
        Some("video") => matches!(name, "h264" | "hevc" | "vp8" | "vp9" | "av1" | "mpeg4"),
        Some("audio") => matches!(
            name,
            "mp3"
                | "aac"
                | "opus"
                | "vorbis"
                | "flac"
                | "pcm_s16le"
                | "pcm_s16be"
                | "pcm_s24le"
                | "pcm_s24be"
                | "pcm_s32le"
                | "pcm_s32be"
                | "pcm_f32le"
                | "pcm_f32be"
                | "pcm_f64le"
                | "pcm_f64be"
        ),
        _ => false,
    }
}
pub(crate) fn container_supported(data: &Value) -> bool {
    data["format"]["format_name"].as_str().is_some_and(|names| {
        names.split(',').all(|name| {
            matches!(
                name,
                "image2"
                    | "jpeg_pipe"
                    | "png_pipe"
                    | "webp_pipe"
                    | "bmp_pipe"
                    | "tiff_pipe"
                    | "mp3"
                    | "flac"
                    | "ogg"
                    | "wav"
                    | "aac"
                    | "mov"
                    | "mp4"
                    | "m4a"
                    | "3gp"
                    | "3g2"
                    | "mj2"
                    | "matroska"
                    | "webm"
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn rejects_documents_archives_and_fake_containers() {
        for b in [
            b"%PDF-1.4".as_slice(),
            b"\x1f\x8b",
            b"PK\x03\x04",
            b"RIFF1234WEBP",
            b"OggS",
            b"\x1a\x45\xdf\xa3",
            b"unknown",
        ] {
            assert!(!supported(b));
        }
        assert!(supported(&[0xff, 0xd8, 0xff, 0xe0]));
        assert!(supported(b"\x1a\x45\xdf\xa3\x87\x42\x82\x84webm"));
        assert!(!supported(b"\x1a\x45\xdf\xa3\x87\x42\x82\x84fake"));
    }
    #[test]
    fn allows_explicit_headers_and_rejects_animated_images() {
        let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
        png.extend_from_slice(&13u32.to_be_bytes());
        png.extend_from_slice(b"IHDR");
        png.extend_from_slice(&[0; 17]);
        let mut apng = png.clone();
        apng.extend_from_slice(&8u32.to_be_bytes());
        apng.extend_from_slice(b"acTL");
        apng.extend_from_slice(&[0; 12]);
        png.extend_from_slice(&0u32.to_be_bytes());
        png.extend_from_slice(b"IDAT");
        png.extend_from_slice(&[0; 4]);
        assert!(supported(&png));
        assert!(!supported(&apng));
        let mut webp = b"RIFF".to_vec();
        webp.extend_from_slice(&22u32.to_le_bytes());
        webp.extend_from_slice(b"WEBPVP8X");
        webp.extend_from_slice(&10u32.to_le_bytes());
        webp.extend_from_slice(&[0; 10]);
        assert!(supported(&webp));
        webp[20] = 2;
        assert!(!supported(&webp));
        let mut bmp = vec![0; 54];
        bmp[..2].copy_from_slice(b"BM");
        bmp[10..14].copy_from_slice(&54u32.to_le_bytes());
        bmp[14..18].copy_from_slice(&40u32.to_le_bytes());
        assert!(supported(&bmp));
        for b in [
            b"II\x2a\0\x08\0\0\0".as_slice(),
            b"MM\0\x2a\0\0\0\x08",
            b"fLaC\0\0\0\x22",
            b"ID3\x04\0\0\0\0\0\0",
            b"\xff\xfb\x90\0\0\0\0",
            b"\xff\xf1\x50\0\0\0\0",
        ] {
            assert!(supported(b), "{b:?}");
        }
        let mut ogg = vec![0; 28];
        ogg[..4].copy_from_slice(b"OggS");
        ogg[26] = 1;
        assert!(supported(&ogg));
        ogg[4] = 1;
        assert!(!supported(&ogg));
        let mut wav = b"RIFF".to_vec();
        wav.extend_from_slice(&36u32.to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&[0; 16]);
        assert!(supported(&wav));
        for brand in [b"isom", b"qt  ", b"M4A "] {
            let mut mp4 = 16u32.to_be_bytes().to_vec();
            mp4.extend_from_slice(b"ftyp");
            mp4.extend_from_slice(brand);
            mp4.extend_from_slice(&[0; 4]);
            assert!(supported(&mp4));
        }
        assert!(!supported(b"\0\0\0\x10ftypavif\0\0\0\0"));
        assert!(supported(b"\x1a\x45\xdf\xa3\x8b\x42\x82\x88matroska"));
        // An eight-byte EBML size must not overflow the bit mask.
        assert!(!supported(b"\x1a\x45\xdf\xa3\x01\0\0\0\0\0\0\0"));
    }
    #[test]
    fn only_explicit_codecs_and_containers_pass() {
        for codec in ["h264", "hevc", "vp8", "vp9", "av1", "mpeg4"] {
            assert!(codec_supported(
                &json!({"codec_type":"video","codec_name":codec}),
                false
            ));
        }
        for codec in [
            "aac",
            "mp3",
            "opus",
            "vorbis",
            "flac",
            "pcm_s16le",
            "pcm_f64be",
        ] {
            assert!(codec_supported(
                &json!({"codec_type":"audio","codec_name":codec}),
                false
            ));
        }
        for codec in ["gif", "apng", "prores", "unknown"] {
            assert!(!codec_supported(
                &json!({"codec_type":"video","codec_name":codec}),
                false
            ));
        }
        assert!(!container_supported(
            &json!({"format":{"format_name":"avi"}})
        ));
        assert!(container_supported(
            &json!({"format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2"}})
        ));
    }
    #[test]
    fn archived_sticker_references_skip_new_and_old_tasks() {
        use crate::{archive::Capture, config::Config, tl::Schema, work::Task};
        let schema_text = "documentAttributeSticker#11111111 = Attr; documentAttributeCustomEmoji#22222222 = Attr; document#33333333 id:long mime_type:string attributes:Vector<Attr> = Doc; bundle#44444444 documents:Vector<Doc> = Bundle;";
        for (attribute, mime) in [
            (Some("documentAttributeSticker"), "image/webp"),
            (Some("documentAttributeCustomEmoji"), "video/webm"),
            (None, "application/x-tgsticker"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut a = Archive::init(dir.path(), &Config::default()).unwrap();
            let schema = a.register_schema(1, schema_text).unwrap();
            let attrs: Vec<Value> = attribute
                .into_iter()
                .map(|name| json!({"_":name}))
                .collect();
            let value = json!({"_":"bundle","documents":[{"_":"document","id":"12","mime_type":mime,"attributes":attrs},{"_":"document","id":"13","mime_type":"image/jpeg","attributes":[]}]});
            let bytes = Schema::parse(schema_text)
                .unwrap()
                .encode("Bundle", &value)
                .unwrap();
            let ids = a
                .ingest(
                    &schema,
                    &[Capture {
                        key: "bundle:1".into(),
                        kind: "message".into(),
                        root_type: "Bundle".into(),
                        bytes,
                        observed_at: 1,
                        source: "test".into(),
                        metadata: json!({}),
                        replay_key: None,
                        partial: false,
                        deleted: false,
                    }],
                    None,
                )
                .unwrap();
            for (id, bytes) in [
                ("document:12", vec![0xff, 0xd8, 0xff, 1]),
                ("document:13", vec![0xff, 0xd8, 0xff, 2]),
            ] {
                a.queue_media(id, &json!({}), 2, Some(bytes.len() as u64), ids[0])
                    .unwrap();
                a.append_media(id, 0, &bytes).unwrap();
                a.finish_media(id).unwrap();
            }
            let hash: String =
                a.db.query_row("SELECT hash FROM media WHERE id='document:12'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            let ordinary: String =
                a.db.query_row("SELECT hash FROM media WHERE id='document:13'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(exclusion(&a, &hash).unwrap().is_some());
            assert!(exclusion(&a, &ordinary).unwrap().is_none());
            let policy = crate::transcode::Policy::default();
            assert_eq!(
                crate::transcode::enqueue(&a, &policy, true, false).unwrap()["eligible"],
                1
            );
            let config = json!({"original":hash,"policy":policy,"recipe":"old"});
            let sequence = a.enqueue_work("transcode", "old", &config, false).unwrap();
            let task = Task {
                sequence,
                kind: "transcode".into(),
                config,
            };
            assert_eq!(
                crate::transcode::execute(dir.path(), &task, &Default::default()).unwrap()["state"],
                "skipped"
            );
            // Any sticker reference to a shared hash wins over ordinary references.
            a.db.execute("UPDATE media SET hash=?1 WHERE id='document:13'", [&hash])
                .unwrap();
            assert!(exclusion(&a, &hash).unwrap().is_some());
        }
    }
    #[test]
    fn sticker_matches_document_not_message() {
        let v = json!({"documents":[{"_":"document","id":"12","attributes":[{"_":"documentAttributeSticker"}]},{"_":"document","id":"13"}]});
        assert!(sticker_document(&v, 12));
        assert!(!sticker_document(&v, 13));
        for attr in ["documentAttributeSticker", "documentAttributeCustomEmoji"] {
            assert!(sticker_document(
                &json!({"_":"document","id":1,"attributes":[{"_":attr}]}),
                1
            ));
        }
    }
}
