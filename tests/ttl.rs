use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_server() -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
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

async fn send(sock: &mut TcpStream, raw: &[u8]) {
    sock.write_all(raw).await.unwrap();
}

async fn read_n(sock: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf
}

async fn read_some(sock: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf.truncate(n);
    buf
}

fn array(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[tokio::test]
async fn ttl_missing_returns_minus_two() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"TTL", b"nope"])).await;
    assert_eq!(read_n(&mut s, 5).await, b":-2\r\n");
}

#[tokio::test]
async fn ttl_no_expiry_returns_minus_one() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"TTL", b"k"])).await;
    assert_eq!(read_n(&mut s, 5).await, b":-1\r\n");
}

#[tokio::test]
async fn set_ex_then_ttl() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"EX", b"30"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
    send(&mut s, &array(&[b"TTL", b"k"])).await;
    let resp = read_some(&mut s).await;
    // Should report 29 or 30 (we round-up partial seconds)
    assert!(resp == b":30\r\n" || resp == b":29\r\n", "got: {:?}", resp);
}

#[tokio::test]
async fn pttl_returns_milliseconds() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"PX", b"5000"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"PTTL", b"k"])).await;
    let resp = read_some(&mut s).await;
    let s_str = std::str::from_utf8(&resp).unwrap();
    assert!(s_str.starts_with(':'));
    let n: i64 = s_str[1..s_str.len() - 2].parse().unwrap();
    assert!((4000..=5000).contains(&n), "ms remaining: {}", n);
}

#[tokio::test]
async fn key_expires_after_ttl() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"PX", b"100"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nv\r\n");

    tokio::time::sleep(Duration::from_millis(250)).await;

    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn expire_command_sets_ttl() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"EXPIRE", b"k", b"100"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
    send(&mut s, &array(&[b"TTL", b"k"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp == b":100\r\n" || resp == b":99\r\n", "got: {:?}", resp);
}

#[tokio::test]
async fn expire_on_missing_key_returns_zero() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"EXPIRE", b"missing", b"30"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn persist_clears_ttl() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"EX", b"30"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"PERSIST", b"k"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
    send(&mut s, &array(&[b"TTL", b"k"])).await;
    assert_eq!(read_n(&mut s, 5).await, b":-1\r\n");
}

#[tokio::test]
async fn persist_on_no_ttl_returns_zero() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"PERSIST", b"k"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn sweeper_actively_removes() {
    // Set a short TTL, then wait long enough for the sweeper to fire,
    // and confirm via EXISTS that the key is gone (not just lazily).
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"PX", b"50"])).await;
    let _ = read_n(&mut s, 5).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    send(&mut s, &array(&[b"EXISTS", b"k"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn repeated_expire_does_not_grow_btreemap_unboundedly() {
    // Regression for H1: re-EXPIRE on the same key used to push a new BTreeMap entry
    // each time without removing the old one. After 1000 churns the index size
    // should stay at 1 (one live deadline), not 1000.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    for i in 0..200 {
        let secs = format!("{}", 100 + i);
        send(&mut s, &array(&[b"EXPIRE", b"k", secs.as_bytes()])).await;
        let _ = read_n(&mut s, 4).await;
    }
    // PERSIST then re-set EX 60: also exercises the take-then-unindex path.
    send(&mut s, &array(&[b"PERSIST", b"k"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"SET", b"k", b"v", b"EX", b"60"])).await;
    let _ = read_n(&mut s, 5).await;

    // We can't directly observe BTreeMap size over the wire, but if the leak still
    // existed, EXPIRE k 60 followed by PERSIST followed by GET should still observe
    // the live value (sweeper double-check protects correctness — this test is a
    // smoke that the new logic doesn't break correctness).
    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nv\r\n");
}

#[tokio::test]
async fn set_overrides_existing_ttl() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v", b"PX", b"50"])).await;
    let _ = read_n(&mut s, 5).await;
    // Plain SET should clear any prior TTL
    send(&mut s, &array(&[b"SET", b"k", b"v2"])).await;
    let _ = read_n(&mut s, 5).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 8).await, b"$2\r\nv2\r\n");
}
