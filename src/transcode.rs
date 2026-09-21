//! Optional media representations; Telegram TL objects are never rewritten.
use crate::{
    archive::Archive,
    work::{Resources, Task},
};
use anyhow::{Context, Result, ensure};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Stdio},
};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub enabled: bool,
    pub selector: String,
    pub kinds: Vec<String>,
    pub min_age_days: u32,
    pub max_video_bytes: u64,
    pub max_video_seconds: f64,
    pub probe_selector: String,
    pub photo_quality: u8,
    pub video_crf: u8,
    pub video_preset: u8,
    pub replace_originals: bool,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: false,
            selector: "true".into(),
            kinds: vec!["photo".into(), "audio".into(), "video".into()],
            min_age_days: 0,
            max_video_bytes: 1024 * 1024 * 1024,
            max_video_seconds: 600.0,
            probe_selector: "true".into(),
            photo_quality: 82,
            video_crf: 32,
            video_preset: 6,
            replace_originals: false,
        }
    }
}
impl Policy {
    pub fn validate(&self) -> Result<()> {
        crate::selector::Selector::parse(&self.selector)?;
        crate::selector::Selector::parse(&self.probe_selector)?;
        ensure!(
            self.photo_quality <= 100
                && self.video_crf <= 63
                && self.video_preset <= 13
                && self.max_video_seconds.is_finite()
                && self.max_video_seconds > 0.0,
            "invalid transcoding quality or duration"
        );
        ensure!(
            self.kinds
                .iter()
                .all(|k| ["photo", "audio", "video"].contains(&k.as_str())),
            "unknown media kind"
        );
        Ok(())
    }
    fn recipe(&self) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}
fn content_date(value: &Value) -> Option<i64> {
    let own = crate::tl::integer(&value["date"]);
    let nested = match value {
        Value::Object(m) => m.values().filter_map(content_date).max(),
        Value::Array(a) => a.iter().filter_map(content_date).max(),
        _ => None,
    };
    own.into_iter().chain(nested).max()
}
fn references_allow(a: &Archive, hash: &str, p: &Policy) -> Result<bool> {
    let selector = crate::selector::Selector::parse(&p.selector)?;
    let cutoff = chrono::Utc::now().timestamp() - i64::from(p.min_age_days) * 86400;
    let mut st=a.db.prepare("SELECT DISTINCT r.observation FROM media_refs r JOIN media m ON r.media=m.id WHERE m.hash=?1")?;
    let mut rows = st.query([hash])?;
    let mut any = false;
    while let Some(row) = rows.next()? {
        let record = a.record(row.get(0)?)?;
        if !selector.matches(&record.metadata)
            || (p.min_age_days > 0 && content_date(&record.data).is_none_or(|t| t > cutoff))
        {
            return Ok(false);
        }
        any = true;
    }
    Ok(any)
}
/// Scan in bounded pages. The dedupe key prevents retranscoding a completed recipe.
pub fn enqueue(a: &Archive, p: &Policy, apply: bool, automatic: bool) -> Result<Value> {
    p.validate()?;
    if automatic && !p.enabled {
        return Ok(json!({"eligible":0,"queued":0}));
    }
    let recipe = p.recipe()?;
    let scan_key = format!("transcode_scan:{recipe}");
    let mut last = if automatic {
        a.checkpoint(&scan_key)?
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let mut eligible = 0;
    loop {
        let hashes:Vec<String>=a.db.prepare("SELECT DISTINCT hash FROM media WHERE status='complete' AND hash>?1 AND hash NOT IN(SELECT hash FROM representations) ORDER BY hash LIMIT 32")?.query_map([&last],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        if hashes.is_empty() {
            if automatic && apply {
                a.set_checkpoint(&scan_key, &json!(""))?;
            }
            break;
        }
        for hash in hashes {
            last = hash.clone();
            if references_allow(a, &hash, p)? && crate::media_format::exclusion(a, &hash)?.is_none()
            {
                eligible += 1;
                if apply {
                    a.enqueue_work(
                        "transcode",
                        &format!("transcode:{hash}:{recipe}"),
                        &json!({"original":hash,"policy":p,"recipe":recipe}),
                        automatic,
                    )?;
                }
            }
        }
        if automatic {
            if apply {
                a.set_checkpoint(&scan_key, &json!(last))?;
            }
            break;
        }
    }
    Ok(json!({"eligible":eligible,"apply":apply,"automatic":automatic,"recipe":recipe}))
}
fn probe(path: &Path) -> Result<Value> {
    let output = crate::diagnostics::output(
        Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_format",
                "-show_streams",
                "-of",
                "json",
            ])
            .arg(path),
    )
    .with_context(|| {
        format!(
            "starting ffprobe for {}; install FFmpeg and ffprobe or use the ffmpeg image",
            path.display()
        )
    })?;
    ensure!(
        output.status.success(),
        "ffprobe failed for {} ({}); stderr: {}",
        path.display(),
        output.status,
        crate::diagnostics::text(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("invalid ffprobe JSON for {}", path.display()))
}
fn number(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse().ok())
}
fn skipped(reason: &str) -> Value {
    json!({"state":"skipped","reason":reason})
}
fn run_ffmpeg(args: &[String]) -> Result<()> {
    let output = crate::diagnostics::output(
        Command::new("ffmpeg")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    )
    .context("starting FFmpeg; install FFmpeg or use the ffmpeg image")?;
    ensure!(
        output.status.success(),
        "FFmpeg failed ({}); original retained; stderr: {}",
        output.status,
        crate::diagnostics::text(&output.stderr)
    );
    Ok(())
}
pub fn execute(root: &Path, task: &Task, r: &Resources) -> Result<Value> {
    if task.kind == "compact_exclusive" {
        let mut a = Archive::open(root, true)?;
        let report = a.maintain(&crate::archive::Maintenance {
            apply: true,
            retention: serde_json::from_value(task.config.clone())?,
            ..Default::default()
        })?;
        return Ok(json!({"state":"complete","result":report}));
    }
    if task.kind == "verify" {
        let a = Archive::open(root, false)?;
        return Ok(json!({"state":"complete","result":a.verify()?}));
    }
    if ["reindex", "seal", "repack", "consolidate"].contains(&task.kind.as_str()) {
        return crate::maintenance_worker::execute(root, task);
    }
    ensure!(task.kind == "transcode", "unsupported worker task");
    let hash = task.config["original"].as_str().context("original hash")?;
    let input = crate::media::attachment_path(root, hash)?;
    let p: Policy = serde_json::from_value(task.config["policy"].clone())?;
    p.validate()?;
    let archive = Archive::open(root, false)?;
    if let Some(reason) = crate::media_format::exclusion(&archive, hash)? {
        return Ok(skipped(reason));
    }
    drop(archive);
    let bytes = std::fs::metadata(&input)?.len();
    let data = probe(&input).context("probing original media")?;
    if !crate::media_format::container_supported(&data) {
        return Ok(skipped("unsupported container"));
    }
    let streams = data["streams"].as_array().context("missing streams")?;
    if streams
        .iter()
        .any(|s| matches!(s["codec_name"].as_str(), Some("gif" | "apng")))
    {
        return Ok(skipped(
            "animated image format is not supported by this recipe",
        ));
    }
    let videos: Vec<_> = streams
        .iter()
        .filter(|s| s["codec_type"] == "video")
        .collect();
    let audios: Vec<_> = streams
        .iter()
        .filter(|s| s["codec_type"] == "audio")
        .collect();
    if streams.len() != videos.len() + audios.len() || videos.len() > 1 || audios.len() > 1 {
        return Ok(skipped("unsupported stream layout"));
    }
    let still = videos.first().is_some_and(|s| {
        ["mjpeg", "png", "webp", "bmp", "tiff"].contains(&s["codec_name"].as_str().unwrap_or(""))
    }) && audios.is_empty()
        && data["format"]["format_name"].as_str().is_some_and(|f| {
            [
                "image2",
                "jpeg_pipe",
                "png_pipe",
                "webp_pipe",
                "bmp_pipe",
                "tiff_pipe",
            ]
            .contains(&f)
        });
    if streams
        .iter()
        .any(|s| !crate::media_format::codec_supported(s, still))
    {
        return Ok(skipped("unsupported codec"));
    }
    if !still
        && videos.iter().any(|v| {
            v["width"].as_u64().is_none_or(|n| n % 2 != 0)
                || v["height"].as_u64().is_none_or(|n| n % 2 != 0)
        })
    {
        return Ok(skipped("video dimensions are not even"));
    }
    let kind = if still {
        "photo"
    } else if !videos.is_empty() {
        "video"
    } else if !audios.is_empty() {
        "audio"
    } else {
        return Ok(skipped("no media streams"));
    };
    if !p.kinds.iter().any(|k| k == kind) {
        return Ok(skipped("media kind excluded"));
    }
    if let Some(v) = videos.first() {
        if v["color_transfer"] == "smpte2084"
            || v["color_transfer"] == "arib-std-b67"
            || v["pix_fmt"]
                .as_str()
                .is_some_and(|s| s.contains("10") || s.contains("12") || s.contains("16"))
        {
            return Ok(skipped(
                "HDR/high bit depth requires an explicit supported recipe",
            ));
        }
        if v["side_data_list"].as_array().is_some_and(|a| {
            a.iter()
                .any(|s| number(&s["rotation"]).is_some_and(|n| n != 0.0))
        }) || v["tags"]["rotate"].as_str().is_some_and(|s| s != "0")
        {
            return Ok(skipped(
                "rotated media requires an orientation-preserving recipe",
            ));
        }
    }
    if kind == "photo" && videos[0]["nb_frames"].as_str().is_some_and(|s| s != "1") {
        return Ok(skipped("animated or unknown frame count"));
    }
    let duration = number(&data["format"]["duration"]).or_else(|| {
        streams
            .iter()
            .filter_map(|s| number(&s["duration"]))
            .reduce(f64::max)
    });
    let fields = json!({"kind":kind,"bytes":bytes,"duration":duration,"width":videos.first().map(|v|v["width"].clone()),"height":videos.first().map(|v|v["height"].clone()),"codec":streams.first().map(|v|v["codec_name"].clone())});
    if !crate::selector::Selector::parse(&p.probe_selector)?.matches(&fields) {
        return Ok(skipped("probe selector excluded media"));
    }
    if kind == "video"
        && (bytes > p.max_video_bytes
            || duration.is_none_or(|d| d > p.max_video_seconds || d <= 0.0))
    {
        return Ok(skipped("video size/duration limit or missing duration"));
    }
    if audios
        .first()
        .is_some_and(|s| !matches!(s["channels"].as_u64(), Some(1 | 2)))
    {
        return Ok(skipped("unsupported audio channels"));
    }
    let extension = match kind {
        "photo" => "webp",
        "audio" => "opus",
        _ => "mkv",
    };
    let staging = root
        .join("staging")
        .join(format!("transcode-{}.{}", task.sequence, extension));
    let threads = r.cpus.to_string();
    let mut args: Vec<String> = [
        "-hide_banner",
        "-nostdin",
        "-v",
        "error",
        "-y",
        "-threads",
        &threads,
        "-filter_threads",
        &threads,
        "-i",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    args.push(input.to_string_lossy().into());
    args.extend(["-map", "0", "-map_metadata", "0", "-threads", &threads].map(String::from));
    match kind {
        "photo" => args.extend([
            "-c:v".into(),
            "libwebp".into(),
            "-quality".into(),
            p.photo_quality.to_string(),
        ]),
        "video" => args.extend([
            "-c:v".into(),
            "libsvtav1".into(),
            "-crf".into(),
            p.video_crf.to_string(),
            "-preset".into(),
            p.video_preset.to_string(),
            "-svtav1-params".into(),
            format!("lp={}", r.cpus),
        ]),
        _ => {}
    }
    if !audios.is_empty() {
        args.extend(
            [
                "-c:a",
                "libopus",
                "-b:a",
                if audios[0]["channels"] == 1 {
                    "48k"
                } else {
                    "128k"
                },
            ]
            .map(String::from),
        );
    }
    let progress_path = root
        .join("staging")
        .join(format!("progress-{}.txt", task.sequence));
    tg_backup_credentials::private_write(
        &root
            .join("staging")
            .join(format!("progress-{}.json", task.sequence)),
        &serde_json::to_vec(&json!({"duration_seconds":duration,"kind":kind}))?,
    )?;
    args.extend([
        "-progress".into(),
        progress_path.to_string_lossy().into(),
        "-stats_period".into(),
        "2".into(),
    ]);
    args.push(staging.to_string_lossy().into());
    run_ffmpeg(&args)
        .with_context(|| format!("encoding {} to {}", input.display(), staging.display()))?;
    let output = probe(&staging).context("probing encoded media")?;
    let outstreams = output["streams"]
        .as_array()
        .context("missing output streams")?;
    ensure!(
        outstreams.len() == streams.len(),
        "transcoding changed stream count"
    );
    for (before, after) in streams.iter().zip(outstreams) {
        ensure!(
            before["codec_type"] == after["codec_type"],
            "stream type changed"
        );
        if before["codec_type"] == "video" {
            ensure!(
                before["width"] == after["width"] && before["height"] == after["height"],
                "dimensions changed"
            );
        }
        if before["codec_type"] == "audio" {
            ensure!(before["channels"] == after["channels"], "channels changed");
        }
    }
    if let Some(d) = duration {
        let out = number(&output["format"]["duration"]).context("missing output duration")?;
        ensure!((d - out).abs() <= 0.5, "duration changed");
    }
    run_ffmpeg(&[
        "-v".into(),
        "error".into(),
        "-xerror".into(),
        "-threads".into(),
        threads,
        "-i".into(),
        staging.to_string_lossy().into(),
        "-map".into(),
        "0".into(),
        "-f".into(),
        "null".into(),
        "-".into(),
    ])
    .with_context(|| format!("validating encoded media by decoding {}", staging.display()))?;
    let output_bytes = std::fs::metadata(&staging)?.len();
    if output_bytes >= bytes {
        std::fs::remove_file(staging)?;
        return Ok(skipped("encoded representation is not smaller"));
    }
    let output_hash = crate::media::file_hash(&staging)?;
    std::fs::File::open(&staging)?.sync_all()?;
    let version = Command::new("ffmpeg").arg("-version").output()?;
    Ok(
        json!({"state":"complete","original":hash,"hash":output_hash,"staging":staging.file_name().unwrap().to_string_lossy(),"bytes":output_bytes,"original_bytes":bytes,"saved_bytes":bytes-output_bytes,"kind":kind,"input_probe":data,"output_probe":output,"tool":String::from_utf8_lossy(&version.stdout).lines().next(),"recipe":task.config["recipe"]}),
    )
}
pub fn publish(a: &mut Archive, task: &Task, report: &Value) -> Result<()> {
    if ["reindex", "seal", "repack", "consolidate"].contains(&task.kind.as_str()) {
        return crate::maintenance_worker::publish(a, task, report);
    }
    if task.kind != "transcode" || report["state"] != "complete" {
        return Ok(());
    }
    let original = task.config["original"].as_str().context("original")?;
    ensure!(
        report["original"] == original && report["recipe"] == task.config["recipe"],
        "worker report identity mismatch"
    );
    let hash = report["hash"].as_str().context("output hash")?;
    let name = report["staging"].as_str().context("staging name")?;
    ensure!(
        Path::new(name).components().count() == 1
            && name.starts_with(&format!("transcode-{}.", task.sequence)),
        "invalid staging path"
    );
    let staging = a.root.join("staging").join(name);
    let dest = crate::media::attachment_path(&a.root, hash)?;
    ensure!(
        crate::media::file_hash(&staging)? == hash,
        "output checksum changed"
    );
    ensure!(
        std::fs::metadata(&staging)?.len() == report["bytes"].as_u64().context("output size")?,
        "output size changed"
    );
    std::fs::create_dir_all(dest.parent().unwrap())?;
    if dest.exists() {
        ensure!(
            crate::media::file_hash(&dest)? == hash,
            "existing output corrupt"
        );
        std::fs::remove_file(staging)?;
    } else {
        std::fs::rename(staging, &dest)?;
        crate::archive::sync_dir(dest.parent().unwrap())?;
    }
    let policy: Policy = serde_json::from_value(task.config["policy"].clone())?;
    a.db.execute(
        "INSERT OR REPLACE INTO representations VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            original,
            hash,
            task.config["recipe"].as_str(),
            report["bytes"].as_u64(),
            chrono::Utc::now().timestamp_micros(),
            report.to_string()
        ],
    )?;
    // Original replacement requires all current references and the reader lock. Busy readers defer deletion.
    if policy.replace_originals && references_allow(a, original, &policy)? {
        let readers = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(a.root.join("readers.lock"))?;
        if fs2::FileExt::try_lock_exclusive(&readers).is_ok() {
            let tx = a.db.transaction()?;
            tx.execute("INSERT INTO media_transformations(at,original,replacement,policy,details) VALUES(?1,?2,?3,?4,?5)",params![chrono::Utc::now().timestamp_micros(),original,hash,serde_json::to_string(&policy)?,report.to_string()])?;
            tx.execute(
                "UPDATE media SET hash=?2,offset=?3 WHERE hash=?1",
                params![original, hash, report["bytes"].as_u64()],
            )?;
            tx.commit()?;
            std::fs::remove_file(crate::media::attachment_path(&a.root, original)?)?;
        }
    }
    Ok(())
}
#[derive(Deserialize)]
struct Request {
    task: Task,
    resources: Resources,
}
pub fn worker(root: &Path) -> Result<()> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().take(1024 * 1024).read_to_end(&mut buf)?;
    let request: Request = serde_json::from_slice(&buf)?;
    request.resources.validate()?;
    #[cfg(target_os = "linux")]
    {
        let limit = libc::rlimit {
            rlim_cur: request.resources.memory_bytes,
            rlim_max: request.resources.memory_bytes,
        };
        // SAFETY: setting this worker's address-space limit does not affect the coordinator.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } != 0 {
            tracing::warn!("address-space limit unavailable");
        }
    }
    let report = execute(root, &request.task, &request.resources).with_context(|| {
        format!(
            "worker task {} failed (kind={}, original={})",
            request.task.sequence,
            request.task.kind,
            request.task.config["original"].as_str().unwrap_or("n/a")
        )
    })?;
    serde_json::to_writer(std::io::stdout().lock(), &report)?;
    Ok(())
}

/// Bounded progress read: FFmpeg's output is append-only until the worker exits.
pub fn progress(root: &Path, id: i64) -> Result<Value> {
    use std::io::{Read, Seek, SeekFrom};
    let mut result: Value = serde_json::from_slice(&std::fs::read(
        root.join("staging").join(format!("progress-{id}.json")),
    )?)?;
    let mut file = std::fs::File::open(root.join("staging").join(format!("progress-{id}.txt")))?;
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(8192)))?;
    let mut bytes = Vec::new();
    file.take(8192).read_to_end(&mut bytes)?;
    let text = String::from_utf8_lossy(&bytes);
    if let Some(micros) = text
        .lines()
        .filter_map(|l| l.strip_prefix("out_time_us="))
        .filter_map(|s| s.parse::<f64>().ok())
        .next_back()
    {
        result["encoded_seconds"] = json!(micros / 1_000_000.0);
        if let Some(duration) = result["duration_seconds"].as_f64().filter(|d| *d > 0.0) {
            result["fraction"] = json!((micros / 1_000_000.0 / duration).clamp(0.0, 1.0));
        }
    }
    Ok(result)
}
