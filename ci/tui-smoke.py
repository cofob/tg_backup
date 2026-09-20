#!/usr/bin/env python3
"""PTY smoke test for both TUIs, without Telegram credentials or real archive data."""
import argparse
import fcntl
import http.server
import json
import threading
import os
import pathlib
import pty
import select
import re
import socket
import struct
import subprocess
import tempfile
import termios
import time
import urllib.request


def terminal_run(command, quit_key=b"q", export_path=None):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 120, 0, 0))
    original = termios.tcgetattr(slave)
    env = dict(os.environ, TERM="xterm-256color")
    child = subprocess.Popen(command, stdin=slave, stdout=slave, stderr=slave, env=env)
    data = bytearray()

    def read_until(needle, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                try:
                    data.extend(os.read(master, 65536))
                except OSError:
                    break
            if needle in data:
                return
            if child.poll() is not None:
                break
        clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", bytes(data))
        raise AssertionError(f"missing {needle!r}; exit={child.poll()}; output={clean[-5000:]!r}")

    try:
        read_until(b"Telegram Archive")
        if export_path is not None:
            os.write(master, b"2e\t" + str(export_path).encode() + b"\r")
            read_until(b"complete")
            assert export_path.is_file(), "export was not published"
        # Exercise all views, help, filter input, storage drill-down and resize.
        for keys in (b"2", b"f", b"\x1b", b"3", b"\r", b"4", b"\r", b"?", b" "):
            os.write(master, keys)
            time.sleep(0.12)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 20, 60, 0, 0))
        time.sleep(0.2)
        os.write(master, quit_key)
        deadline = time.monotonic() + 5
        while child.poll() is None and time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                data.extend(os.read(master, 65536))
        assert child.wait(timeout=1) == 0, bytes(data[-2000:])
        while select.select([master], [], [], 0)[0]:
            data.extend(os.read(master, 65536))
        assert termios.tcgetattr(slave) == original, "terminal settings were not restored"
        assert b"\x1b[?1049l" in data, "alternate screen was not restored"
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        os.close(master)
        os.close(slave)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", default="target/debug")
    args = parser.parse_args()
    bins = pathlib.Path(args.bin_dir).resolve()
    with tempfile.TemporaryDirectory(prefix="tg-backup-tui-") as temp:
        root = pathlib.Path(temp)
        dataset = root / "dataset"
        subprocess.run([str(bins / "tg-backup"), "--dataset", str(dataset), "init"], check=True, stdout=subprocess.DEVNULL)
        local = [str(bins / "tg-backup"), "--dataset", str(dataset), "tui"]
        terminal_run(local, export_path=root / "local.ndjson")
        terminal_run(local, b"\x03")
        piped = subprocess.run(local, capture_output=True)
        assert piped.returncode != 0 and b"interactive terminal" in piped.stderr
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = dict(os.environ, TG_BACKUP_HTTP_TOKEN="tui-smoke-token")
        server = subprocess.Popen([str(bins / "tg-backup"), "--dataset", str(dataset), "serve", "--bind", f"127.0.0.1:{port}"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            url = f"http://127.0.0.1:{port}"
            request = urllib.request.Request(url + "/v2/explorer/capabilities", headers={"Authorization": "Bearer tui-smoke-token"})
            for _ in range(100):
                try:
                    with urllib.request.urlopen(request, timeout=1) as response:
                        assert response.status == 200
                    break
                except OSError:
                    if server.poll() is not None:
                        raise AssertionError(server.stderr.read().decode())
                    time.sleep(0.05)
            else:
                raise AssertionError("server did not become ready")
            os.environ["TG_BACKUP_HTTP_TOKEN"] = "tui-smoke-token"
            terminal_run([str(bins / "tg-backup-client"), "--url", url, "--config", str(root / "client.toml"), "tui"], export_path=root / "remote.ndjson")
        finally:
            server.terminate()
            server.wait(timeout=5)
        # A pre-explorer server must keep query/export usable and receive no new null fields.
        class LegacyHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.send_response(404)
                self.end_headers()

            def do_POST(self):
                query = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                assert "peer" not in query and "topic" not in query
                body = json.dumps({"records": [], "next_cursor": None, "incomplete": False, "scanned": 0, "snapshot": 0}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        legacy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), LegacyHandler)
        worker = threading.Thread(target=legacy.serve_forever, daemon=True)
        worker.start()
        try:
            terminal_run([str(bins / "tg-backup-client"), "--url", f"http://127.0.0.1:{legacy.server_port}", "--config", str(root / "client.toml"), "tui"], export_path=root / "legacy.ndjson")
        finally:
            legacy.shutdown()
            legacy.server_close()
            worker.join(timeout=3)
    print("Both TUIs: navigation, resize, quit/Ctrl-C, non-TTY rejection, and terminal restoration passed")


if __name__ == "__main__":
    main()
