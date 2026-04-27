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
async fn rpush_then_lrange_in_order() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":3\r\n");
    send(&mut s, &array(&[b"LRANGE", b"l", b"0", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

#[tokio::test]
async fn lpush_reverses_order() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"0", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
}

#[tokio::test]
async fn lpop_single_returns_bulk() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"x", b"y", b"z"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nx\r\n");
}

#[tokio::test]
async fn rpop_single_returns_bulk() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"x", b"y", b"z"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"RPOP", b"l"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nz\r\n");
}

#[tokio::test]
async fn lpop_count_returns_array() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c", b"d"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l", b"2"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

#[tokio::test]
async fn lpop_on_empty_returns_null() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LPOP", b"missing"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn llen_works() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LLEN", b"l"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
    send(&mut s, &array(&[b"LLEN", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn lindex_positive_and_negative() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LINDEX", b"l", b"0"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\na\r\n");
    send(&mut s, &array(&[b"LINDEX", b"l", b"-1"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nc\r\n");
    send(&mut s, &array(&[b"LINDEX", b"l", b"99"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn lrange_negative_indices() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c", b"d", b"e"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"-2", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n");
}

#[tokio::test]
async fn lrange_missing_returns_empty_array() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LRANGE", b"missing", b"0", b"-1"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lpush_on_string_key_wrongtype() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"LPUSH", b"k", b"x"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-WRONGTYPE"), "got: {:?}", resp);
}

#[tokio::test]
async fn lrange_start_beyond_end_returns_empty() {
    // Regression for C1: previously returned a 1-element array because clamp wrongly
    // pinned start to len-1.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"5", b"10"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lrange_negative_stop_before_list_returns_empty() {
    // -100 on a 3-elem list resolves to "before index 0" -> empty.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"-100", b"-50"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lpop_with_count_zero_returns_empty_array() {
    // Regression for H4: previously returned $-1\r\n.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l", b"0"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn pop_empties_then_del_via_pop() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"only"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l"])).await;
    let _ = read_n(&mut s, 10).await;
    send(&mut s, &array(&[b"EXISTS", b"l"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}
