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
async fn subscribe_returns_ack() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"ch"])).await;
    // ack format: *3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n
    let resp = read_some(&mut sub).await;
    assert_eq!(resp, b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");
}

#[tokio::test]
async fn publish_to_no_subscribers_returns_zero() {
    let (addr, _g) = spawn_server().await;
    let mut pub_sock = TcpStream::connect(addr).await.unwrap();
    send(&mut pub_sock, &array(&[b"PUBLISH", b"empty", b"hi"])).await;
    assert_eq!(read_n(&mut pub_sock, 4).await, b":0\r\n");
}

#[tokio::test]
async fn subscriber_receives_published_message() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"news"])).await;
    let _ack = read_some(&mut sub).await;

    // Give subscription a moment to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();
    send(
        &mut publisher,
        &array(&[b"PUBLISH", b"news", b"hello"]),
    )
    .await;
    assert_eq!(read_n(&mut publisher, 4).await, b":1\r\n");

    let msg = read_some(&mut sub).await;
    assert_eq!(
        msg,
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
    );
}

#[tokio::test]
async fn multiple_subscribers_each_receive() {
    let (addr, _g) = spawn_server().await;
    let mut s1 = TcpStream::connect(addr).await.unwrap();
    let mut s2 = TcpStream::connect(addr).await.unwrap();
    send(&mut s1, &array(&[b"SUBSCRIBE", b"chat"])).await;
    let _ = read_some(&mut s1).await;
    send(&mut s2, &array(&[b"SUBSCRIBE", b"chat"])).await;
    let _ = read_some(&mut s2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();
    send(&mut publisher, &array(&[b"PUBLISH", b"chat", b"yo"])).await;
    assert_eq!(read_n(&mut publisher, 4).await, b":2\r\n");

    let m1 = read_some(&mut s1).await;
    let m2 = read_some(&mut s2).await;
    assert!(m1.windows(2).any(|w| w == b"yo"));
    assert!(m2.windows(2).any(|w| w == b"yo"));
}

#[tokio::test]
async fn subscribe_to_multiple_channels() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"a", b"b", b"c"])).await;
    let resp = read_some(&mut sub).await;
    // Should get 3 ack frames
    let ack_count = resp.windows(b"$9\r\nsubscribe\r\n".len())
        .filter(|w| *w == b"$9\r\nsubscribe\r\n").count();
    assert_eq!(ack_count, 3, "got: {:?}", resp);
}

#[tokio::test]
async fn unsubscribe_specific_channel() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"a", b"b"])).await;
    let _ = read_some(&mut sub).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    send(&mut sub, &array(&[b"UNSUBSCRIBE", b"a"])).await;
    let resp = read_some(&mut sub).await;
    // Should get one unsubscribe ack with count 1 (still subscribed to b)
    assert!(resp.windows(b"$11\r\nunsubscribe\r\n".len())
            .any(|w| w == b"$11\r\nunsubscribe\r\n"),
            "got: {:?}", resp);
    assert!(resp.ends_with(b":1\r\n"), "got: {:?}", resp);
}

#[tokio::test]
async fn ping_works_in_subscribed_mode() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"x"])).await;
    let _ = read_some(&mut sub).await;
    send(&mut sub, &array(&[b"PING"])).await;
    let resp = read_some(&mut sub).await;
    // Subscribed PING returns *2\r\n$4\r\npong\r\n$0\r\n\r\n
    assert!(resp.starts_with(b"*2\r\n"), "got: {:?}", resp);
    assert!(resp.windows(8).any(|w| w == b"$4\r\npong"), "got: {:?}", resp);
}

#[tokio::test]
async fn other_commands_blocked_in_subscribed_mode() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"x"])).await;
    let _ = read_some(&mut sub).await;
    send(&mut sub, &array(&[b"GET", b"foo"])).await;
    let resp = read_some(&mut sub).await;
    assert!(resp.starts_with(b"-ERR"), "got: {:?}", resp);
}
