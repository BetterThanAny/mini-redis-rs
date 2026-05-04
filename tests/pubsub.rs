use std::time::Duration;
use tokio::net::TcpStream;

mod common;
use common::{array, read_n, read_some, send, spawn_server};

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
    send(&mut publisher, &array(&[b"PUBLISH", b"news", b"hello"])).await;
    assert_eq!(read_n(&mut publisher, 4).await, b":1\r\n");

    let msg = read_some(&mut sub).await;
    assert_eq!(msg, b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
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
    // 3 acks @ 30 bytes each (single-char channel, single-digit count) = 90 bytes.
    // Use read_n rather than read_some — read_some can return after the first frame.
    let resp = read_n(&mut sub, 90).await;
    let ack_count = resp
        .windows(b"$9\r\nsubscribe\r\n".len())
        .filter(|w| *w == b"$9\r\nsubscribe\r\n")
        .count();
    assert_eq!(ack_count, 3, "got: {:?}", resp);
}

#[tokio::test]
async fn unsubscribe_specific_channel() {
    let (addr, _g) = spawn_server().await;
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send(&mut sub, &array(&[b"SUBSCRIBE", b"a", b"b"])).await;
    // 2 subscribe acks @ 30 bytes = 60 bytes
    let _ = read_n(&mut sub, 60).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    send(&mut sub, &array(&[b"UNSUBSCRIBE", b"a"])).await;
    // 1 unsubscribe ack: *3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n = 33 bytes
    let resp = read_n(&mut sub, 33).await;
    assert_eq!(resp, b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n");
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
    assert!(
        resp.windows(8).any(|w| w == b"$4\r\npong"),
        "got: {:?}",
        resp
    );
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
