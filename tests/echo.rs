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

#[tokio::test]
async fn ping_pong() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
}

#[tokio::test]
async fn ping_with_message() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 11];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"$5\r\nhello\r\n");
}

#[tokio::test]
async fn echo_works() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 11];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"$5\r\nhello\r\n");
}

#[tokio::test]
async fn unknown_command_returns_error() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*1\r\n$5\r\nWHATS\r\n").await.unwrap();
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(buf[..n].starts_with(b"-ERR"), "got: {:?}", &buf[..n]);
}

#[tokio::test]
async fn unknown_command_with_crlf_does_not_split_frame() {
    // Regression for H2: a command name containing \r\n must not produce an Error
    // frame that is interpreted as multiple frames by the client.
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    // bulk payload is exactly 8 bytes: F O O \r \n B A R
    sock.write_all(b"*1\r\n$8\r\nFOO\r\nBAR\r\n").await.unwrap();
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let resp = &buf[..n];
    assert!(resp.starts_with(b"-ERR"), "got: {resp:?}");
    // Body must not contain raw CR/LF (would let the response be parsed as 2 frames).
    let body_end = resp.len() - 2; // strip trailing \r\n
    assert!(
        !resp[..body_end].contains(&b'\r') && !resp[..body_end].contains(&b'\n'),
        "raw CR/LF found in error body: {resp:?}"
    );
}

#[tokio::test]
async fn case_insensitive_command_name() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*1\r\n$4\r\nping\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
}
