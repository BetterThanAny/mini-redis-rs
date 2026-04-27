use mini_redis_rs::aof::{self, FsyncPolicy};
use mini_redis_rs::db::Db;
use mini_redis_rs::server;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_with_aof(
    aof_path: std::path::PathBuf,
) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let db = Db::new();
    let aof_handle = if aof_path.exists() {
        let _ = aof::replay(&aof_path, &db).await.unwrap();
        Some(aof::spawn_writer(aof_path.clone(), FsyncPolicy::Always).await.unwrap())
    } else {
        Some(aof::spawn_writer(aof_path.clone(), FsyncPolicy::Always).await.unwrap())
    };
    tokio::spawn(async move {
        server::run_with_options(listener, db, aof_handle, async move {
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
async fn aof_writes_then_replays_string_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.aof");

    // Phase 1: write some data
    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"SET", b"k1", b"hello"])).await;
        let _ = read_n(&mut s, 5).await;
        send(&mut s, &array(&[b"SET", b"k2", b"world"])).await;
        let _ = read_n(&mut s, 5).await;
        send(&mut s, &array(&[b"INCR", b"counter"])).await;
        let _ = read_n(&mut s, 4).await;
        send(&mut s, &array(&[b"INCR", b"counter"])).await;
        let _ = read_n(&mut s, 4).await;
        // Allow writer task to flush (Always policy)
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = shutdown.send(());
        // Give the writer task a moment to drain after shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Phase 2: restart and verify state survived
    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"k1"])).await;
        assert_eq!(read_n(&mut s, 11).await, b"$5\r\nhello\r\n");
        send(&mut s, &array(&[b"GET", b"k2"])).await;
        assert_eq!(read_n(&mut s, 11).await, b"$5\r\nworld\r\n");
        send(&mut s, &array(&[b"GET", b"counter"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\n2\r\n");
    }
}

#[tokio::test]
async fn aof_replays_lists_and_hashes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed.aof");

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
        let _ = read_n(&mut s, 4).await;
        send(&mut s, &array(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"])).await;
        let _ = read_n(&mut s, 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"LRANGE", b"l", b"0", b"-1"])).await;
        let mut buf = vec![0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        send(&mut s, &array(&[b"HLEN", b"h"])).await;
        assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
    }
}

#[tokio::test]
async fn aof_replays_del() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("del.aof");

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"SET", b"keep", b"yes"])).await;
        let _ = read_n(&mut s, 5).await;
        send(&mut s, &array(&[b"SET", b"drop", b"no"])).await;
        let _ = read_n(&mut s, 5).await;
        send(&mut s, &array(&[b"DEL", b"drop"])).await;
        let _ = read_n(&mut s, 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"keep"])).await;
        assert_eq!(read_n(&mut s, 9).await, b"$3\r\nyes\r\n");
        send(&mut s, &array(&[b"GET", b"drop"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
    }
}

#[tokio::test]
async fn empty_aof_starts_clean() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.aof");
    let (addr, _g) = spawn_with_aof(path).await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"GET", b"missing"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}
