use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn echoes_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        mini_redis_rs::server::run(listener, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    tokio::time::timeout(Duration::from_secs(1), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"hello");

    drop(shutdown_tx);
}
