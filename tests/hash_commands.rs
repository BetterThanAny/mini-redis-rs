use std::collections::HashSet;
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
    let mut buf = vec![0u8; 512];
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
async fn hset_returns_number_of_new_fields() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(
        &mut s,
        &array(&[b"HSET", b"u", b"name", b"alice", b"age", b"30"]),
    )
    .await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
    // Re-set existing returns 0 new
    send(&mut s, &array(&[b"HSET", b"u", b"name", b"bob"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn hget_returns_value_or_null() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"name", b"alice"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HGET", b"u", b"name"])).await;
    assert_eq!(read_n(&mut s, 11).await, b"$5\r\nalice\r\n");
    send(&mut s, &array(&[b"HGET", b"u", b"missing"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn hdel_counts_removals() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(
        &mut s,
        &array(&[b"HSET", b"u", b"a", b"1", b"b", b"2", b"c", b"3"]),
    )
    .await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HDEL", b"u", b"a", b"missing", b"b"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
}

#[tokio::test]
async fn hkeys_and_hvals() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(
        &mut s,
        &array(&[b"HSET", b"u", b"a", b"1", b"b", b"2"]),
    )
    .await;
    let _ = read_n(&mut s, 4).await;

    send(&mut s, &array(&[b"HKEYS", b"u"])).await;
    let resp = read_some(&mut s).await;
    let resp_str = String::from_utf8_lossy(&resp).to_string();
    let keys: HashSet<&str> = resp_str
        .split("\r\n")
        .filter(|s| matches!(*s, "a" | "b"))
        .collect();
    assert_eq!(keys, HashSet::from(["a", "b"]));

    send(&mut s, &array(&[b"HVALS", b"u"])).await;
    let resp = read_some(&mut s).await;
    let resp_str = String::from_utf8_lossy(&resp).to_string();
    let vals: HashSet<&str> = resp_str
        .split("\r\n")
        .filter(|s| matches!(*s, "1" | "2"))
        .collect();
    assert_eq!(vals, HashSet::from(["1", "2"]));
}

#[tokio::test]
async fn hgetall_returns_pairs() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"a", b"1", b"b", b"2"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HGETALL", b"u"])).await;
    let resp = read_some(&mut s).await;
    // 4 elements (2 k-v pairs); order is HashMap-arbitrary so we just check basic shape
    assert!(resp.starts_with(b"*4\r\n"));
    let resp_str = String::from_utf8_lossy(&resp).to_string();
    assert!(resp_str.contains("a") && resp_str.contains("1"));
    assert!(resp_str.contains("b") && resp_str.contains("2"));
}

#[tokio::test]
async fn hexists_works() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"a", b"1"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HEXISTS", b"u", b"a"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
    send(&mut s, &array(&[b"HEXISTS", b"u", b"nope"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn hlen_works() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"a", b"1", b"b", b"2", b"c", b"3"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HLEN", b"u"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":3\r\n");
    send(&mut s, &array(&[b"HLEN", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn hincrby_creates_field_at_zero() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HINCRBY", b"u", b"counter", b"5"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":5\r\n");
    send(&mut s, &array(&[b"HINCRBY", b"u", b"counter", b"-2"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":3\r\n");
}

#[tokio::test]
async fn hincrby_non_integer_errors() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"name", b"alice"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HINCRBY", b"u", b"name", b"1"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-ERR"), "got: {:?}", resp);
}

#[tokio::test]
async fn wrongtype_on_string_key() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"HSET", b"k", b"f", b"v"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-WRONGTYPE"), "got: {:?}", resp);
}

#[tokio::test]
async fn hdel_all_removes_key() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"HSET", b"u", b"only", b"1"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"HDEL", b"u", b"only"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"EXISTS", b"u"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}
