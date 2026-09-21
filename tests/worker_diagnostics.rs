#![cfg(unix)]
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tg_backup::{archive::Archive, config::Config, work::Task};
struct Service(Child);
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn script(path: &std::path::Path, text: &str) {
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}
fn fixture(bytes: &[u8]) -> (tempfile::TempDir, Archive, Task) {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let a = Archive::init(dir.path(), &Config::default()).unwrap();
    a.db.execute(
        "INSERT INTO media(id,location,dc,size) VALUES('document:1','{}',2,?1)",
        [bytes.len()],
    )
    .unwrap();
    a.append_media("document:1", 0, bytes).unwrap();
    let hash = a.finish_media("document:1").unwrap();
    let config =
        json!({"original":hash,"policy":tg_backup::transcode::Policy::default(),"recipe":"test"});
    let sequence = a.enqueue_work("transcode", "test", &config, false).unwrap();
    (
        dir,
        a,
        Task {
            sequence,
            kind: "transcode".into(),
            config,
        },
    )
}
#[test]
fn unsupported_old_task_skips_before_probe_and_original_survives() {
    for bytes in [
        b"%PDF-1.4".as_slice(),
        b"\x1f\x8b\x08",
        b"PK\x03\x04",
        b"unknown",
    ] {
        let (dir, a, task) = fixture(bytes);
        // The environment has no requirement for ffprobe: unsupported files must never invoke it.
        let result = tg_backup::transcode::execute(dir.path(), &task, &Default::default()).unwrap();
        assert_eq!(result["state"], "skipped");
        assert_eq!(
            std::fs::read(
                tg_backup::media::attachment_path(
                    dir.path(),
                    task.config["original"].as_str().unwrap()
                )
                .unwrap()
            )
            .unwrap(),
            bytes
        );
        assert_eq!(
            a.db.query_row("SELECT status FROM media", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "complete"
        );
    }
}
#[test]
fn stderr_crosses_process_socket_and_catalog_without_deadlock() {
    for stage in ["probe", "encode", "empty"] {
        let (dir, a, task) = fixture(&[0xff, 0xd8, 0xff, 0xe0]);
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let fail = "#!/bin/sh\ni=0\nwhile [ \"$i\" -lt 2000 ]; do printf 'long diagnostic padding padding padding\\n' >&2; i=$((i+1)); done\nprintf 'codec failure: подробности\\n' >&2\nexit 1\n";
        script(
            &bin.join("ffprobe"),
            if stage == "probe" {
                fail
            } else {
                "#!/bin/sh\nprintf '%s' '{\"streams\":[{\"codec_type\":\"video\",\"codec_name\":\"mjpeg\",\"width\":2,\"height\":2,\"nb_frames\":\"1\"}],\"format\":{\"format_name\":\"jpeg_pipe\"}}'\n"
            },
        );
        script(
            &bin.join("ffmpeg"),
            if stage == "empty" {
                "#!/bin/sh\nexit 1\n"
            } else {
                fail
            },
        );
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let socket = dir.path().join("worker.sock");
        let service = Command::new(env!("CARGO_BIN_EXE_tg-backup"))
            .arg("--dataset")
            .arg(dir.path())
            .args(["worker-service", "--socket"])
            .arg(&socket)
            .env("PATH", &path)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(dir.path().join("service.log")).unwrap())
            .spawn()
            .unwrap();
        let mut service = Service(service);
        let until = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(Instant::now() < until, "socket did not appear");
            assert!(
                service.0.try_wait().unwrap().is_none(),
                "service exited: {}",
                std::fs::read_to_string(dir.path().join("service.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(a);
        // Three preexisting attempts ensure one execution records a terminal error.
        let a = Archive::open(dir.path(), true).unwrap();
        a.db.execute("UPDATE work SET attempts=2", []).unwrap();
        drop(a);
        let output = Command::new(env!("CARGO_BIN_EXE_tg-backup"))
            .arg("--dataset")
            .arg(dir.path())
            .args(["worker", "--once"])
            .env("TG_BACKUP_WORKER_SOCKET", &socket)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let a = Archive::open(dir.path(), false).unwrap();
        let item = &a.work_page(0, 20).unwrap()["items"][0];
        assert_eq!(item["state"], "failed");
        let error = item["error"].as_str().unwrap();
        assert!(
            error.contains(&format!("worker task {}", task.sequence)),
            "{error}"
        );
        assert!(error.contains(task.config["original"].as_str().unwrap()));
        assert!(
            error.contains(if stage == "probe" {
                "ffprobe failed"
            } else {
                "FFmpeg failed"
            }),
            "{error}"
        );
        if stage == "empty" {
            assert!(error.contains("<empty>"));
        } else {
            assert!(error.contains("[truncated;"));
            assert!(error.contains("codec failure: подробности"));
        }
        // anyhow may indent each line; worker capture has a separate 16 KiB bound.
        assert!(error.len() < 18000);
        let log = std::fs::read_to_string(dir.path().join("service.log")).unwrap();
        assert!(
            log.contains(if stage == "probe" {
                "ffprobe failed"
            } else {
                "FFmpeg failed"
            }),
            "{error}"
        );
    }
}
