use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use bytes::Bytes;
use mini_redis_rs::cmd::string;
use mini_redis_rs::db::Db;
use mini_redis_rs::resp::Frame;

#[tokio::test]
async fn read_commands_do_not_wait_for_write_pause_gate() {
    let db = Db::new();
    assert_eq!(
        string::set_at(
            &db,
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            None
        ),
        Frame::Simple("OK".into())
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let db_for_server = db.clone();
    tokio::spawn(async move {
        mini_redis_rs::server::run_with_options(listener, db_for_server, None, async move {
            let _ = rx.await;
        })
        .await
        .ok();
    });

    let pause = db.pause_writes().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .await
        .unwrap();
    let mut resp = [0u8; 7];
    tokio::time::timeout(Duration::from_millis(200), client.read_exact(&mut resp))
        .await
        .expect("GET should not wait for the write pause gate")
        .unwrap();
    assert_eq!(&resp, b"$1\r\nv\r\n");

    drop(pause);
    let _ = tx.send(());
}
