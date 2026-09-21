//! Drain child diagnostics continuously while retaining a bounded tail.
use std::io::{self, Read};
pub(crate) const LIMIT: usize = 8192;
pub(crate) fn tail(mut reader: impl Read) -> io::Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut buf = [0; 4096];
    let mut truncated = false;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        tail.extend_from_slice(&buf[..n]);
        if tail.len() > LIMIT {
            tail.drain(..tail.len() - LIMIT);
            truncated = true;
        }
    }
    if truncated {
        tail.splice(..0, b"[truncated; last 8192 bytes] ".iter().copied());
    }
    Ok(tail)
}
pub(crate) fn text(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.trim().is_empty() {
        "<empty>".into()
    } else {
        s.trim().into()
    }
}
pub(crate) fn output(cmd: &mut std::process::Command) -> io::Result<std::process::Output> {
    use std::process::Stdio;
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    let stderr = child.stderr.take().unwrap();
    let drain = std::thread::spawn(move || tail(stderr));
    let mut output = child.wait_with_output()?;
    output.stderr = drain
        .join()
        .map_err(|_| io::Error::other("stderr reader panicked"))??;
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_unicode_and_empty() {
        assert_eq!(text(&tail(&b""[..]).unwrap()), "<empty>");
        assert_eq!(text(&tail("ошибка".as_bytes()).unwrap()), "ошибка");
        let b = tail(&vec![b'x'; LIMIT * 20][..]).unwrap();
        assert!(b.starts_with(b"[truncated;"));
        assert!(b.len() < LIMIT + 100);
    }
}
