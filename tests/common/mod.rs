#![allow(dead_code)] // not every helper is used in every test binary

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Bind on a random port, spawn a vanilla server (no AOF), return its addr and
/// a one-shot sender that triggers a clean shutdown when dropped or fired.
/// The shutdown sender MUST be kept alive for the duration of the test
/// (e.g. as `_shutdown` in the test body) — dropping it immediately closes the
/// listener.
pub async fn spawn_server() -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        mini_redis_rs::server::run(listener, async move {
            let _ = rx.await;
        })
        .await
        .ok();
    });
    (addr, tx)
}

pub async fn send(sock: &mut TcpStream, raw: &[u8]) {
    sock.write_all(raw).await.unwrap();
}

pub async fn read_n(sock: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf
}

pub async fn read_some(sock: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf.truncate(n);
    buf
}

/// Encode a RESP2 array of bulk strings: `*N\r\n$L1\r\n<part1>\r\n$L2\r\n<part2>\r\n...`.
pub fn array(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}
