use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;
use common::spawn_server;

async fn read_available(sock: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf.truncate(n);
    buf
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
async fn inline_ping_pong() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"PING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
}

#[tokio::test]
async fn inline_set_with_quoted_value() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"SET inline \"hello world\"\r\n")
        .await
        .unwrap();
    let mut ok = [0u8; 5];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut ok))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&ok, b"+OK\r\n");

    sock.write_all(b"*2\r\n$3\r\nGET\r\n$6\r\ninline\r\n")
        .await
        .unwrap();
    let mut value = [0u8; 18];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut value))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&value, b"$11\r\nhello world\r\n");
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
async fn protocol_errors_return_err_before_disconnect() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*9999999999\r\n").await.unwrap();
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let resp = &buf[..n];
    assert!(
        resp.starts_with(b"-ERR Protocol error: array length"),
        "got: {resp:?}"
    );
    assert!(resp.ends_with(b"\r\n"), "got: {resp:?}");

    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn non_array_resp_frame_is_protocol_error_and_disconnects() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"+PING\r\n*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();

    let resp = read_available(&mut sock).await;
    assert!(resp.starts_with(b"-ERR Protocol error:"), "got: {resp:?}");

    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn non_bulk_command_argument_is_protocol_error_and_disconnects() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*1\r\n+PING\r\n*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();

    let resp = read_available(&mut sock).await;
    assert!(resp.starts_with(b"-ERR Protocol error:"), "got: {resp:?}");

    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn complete_frame_over_buffer_limit_is_rejected_before_command_runs() {
    let (addr, _shutdown) = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let value_len = 64 * 1024 * 1024 - 16;
    let mut request = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${value_len}\r\n").into_bytes();
    request.resize(request.len() + value_len, b'x');
    request.extend_from_slice(b"\r\n*1\r\n$4\r\nPING\r\n");
    sock.write_all(&request).await.unwrap();

    let resp = read_available(&mut sock).await;
    assert!(resp.starts_with(b"-ERR Protocol error:"), "got: {resp:?}");

    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);

    let mut check = TcpStream::connect(addr).await.unwrap();
    check
        .write_all(b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nbig\r\n")
        .await
        .unwrap();
    let mut len_resp = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(2), check.read_exact(&mut len_resp))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&len_resp, b":0\r\n");
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
