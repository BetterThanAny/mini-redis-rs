use mini_redis_rs::aof::{self, FsyncPolicy};
use mini_redis_rs::db::Db;
use mini_redis_rs::server;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

mod common;
use common::{array, read_n, read_some, send};

/// AOF-aware server spawner — distinct from `common::spawn_server` because it
/// also wires up `aof::replay` + `aof::spawn_writer`.
async fn spawn_with_aof(
    aof_path: std::path::PathBuf,
) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let db = Db::new();
    if aof_path.exists() {
        let _ = aof::replay(&aof_path, &db).await.unwrap();
    }
    let aof_handle = Some(
        aof::spawn_writer(aof_path.clone(), FsyncPolicy::Always)
            .await
            .unwrap(),
    );
    tokio::spawn(async move {
        server::run_with_options(listener, db, aof_handle, async move {
            let _ = rx.await;
        })
        .await
        .ok();
    });
    (addr, tx)
}

async fn read_integer(sock: &mut TcpStream) -> i64 {
    let resp = read_some(sock).await;
    let text = std::str::from_utf8(&resp).unwrap();
    assert!(
        text.starts_with(':'),
        "expected integer frame, got {text:?}"
    );
    text[1..text.len() - 2].parse().unwrap()
}

async fn wait_for_rewrite_count(addr: std::net::SocketAddr, count: u64, status: &str) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    for _ in 0..100 {
        send(&mut sock, &array(&[b"INFO", b"persistence"])).await;
        let resp = read_some(&mut sock).await;
        let text = std::str::from_utf8(&resp).unwrap();
        if text.contains("aof_rewrite_in_progress:0\r\n")
            && text.contains(&format!("aof_rewrites:{count}\r\n"))
            && text.contains(&format!("aof_last_rewrite_status:{status}\r\n"))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("AOF rewrite did not finish with status {status}");
}

fn rewrite_temp_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| "appendonly.aof".into());
    name.push(".rewrite");
    path.with_file_name(name)
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

#[tokio::test]
async fn aof_replay_preserves_absolute_ttl_across_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ttl.aof");

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"SET", b"ttl-key", b"v", b"PX", b"3000"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"PTTL", b"ttl-key"])).await;
        let pttl = read_integer(&mut s).await;
        assert!(
            (300..2600).contains(&pttl),
            "PTTL should keep decaying across restart, got {pttl}"
        );
    }
}

#[tokio::test]
async fn aof_replays_redis_max_relative_set_ttl() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("huge-relative-ttl.aof");

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(
            &mut s,
            &array(&[b"SET", b"huge-ttl", b"v", b"PX", b"9223372036854775807"]),
        )
        .await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"PTTL", b"huge-ttl"])).await;
        let pttl = read_integer(&mut s).await;
        assert!(
            (9223372036854774000..=9223372036854775807).contains(&pttl),
            "PTTL should replay Redis's max relative SET TTL, got {pttl}"
        );
    }
}

#[tokio::test]
async fn bgrewriteaof_compacts_file_and_replays_new_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("rewrite.aof");

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"SET", b"stale", b"old"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        send(&mut s, &array(&[b"SET", b"stale", b"new"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        send(&mut s, &array(&[b"RPUSH", b"queue", b"one"])).await;
        assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
        send(&mut s, &array(&[b"HSET", b"hash", b"field", b"value"])).await;
        assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
        send(
            &mut s,
            &array(&[b"SET", b"volatile", b"yes", b"PX", b"5000"]),
        )
        .await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");

        send(&mut s, &array(&[b"BGREWRITEAOF"])).await;
        let rewrite_resp = read_some(&mut s).await;
        assert!(
            rewrite_resp.starts_with(b"+Background append only file rewriting started"),
            "unexpected BGREWRITEAOF reply: {rewrite_resp:?}"
        );

        send(&mut s, &array(&[b"SET", b"during", b"rewrite"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        send(&mut s, &array(&[b"RPUSH", b"queue", b"two"])).await;
        assert_eq!(read_n(&mut s, 4).await, b":2\r\n");

        wait_for_rewrite_count(addr, 1, "ok").await;
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let aof_text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !aof_text.contains("old"),
        "rewrite should compact away overwritten values: {aof_text:?}"
    );
    assert!(
        aof_text.contains("PXAT"),
        "expiring string should be rewritten with absolute PXAT: {aof_text:?}"
    );

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"stale"])).await;
        assert_eq!(read_n(&mut s, 9).await, b"$3\r\nnew\r\n");
        send(&mut s, &array(&[b"GET", b"during"])).await;
        assert_eq!(read_n(&mut s, 13).await, b"$7\r\nrewrite\r\n");
        send(&mut s, &array(&[b"LRANGE", b"queue", b"0", b"-1"])).await;
        assert_eq!(
            read_n(&mut s, 22).await,
            b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"
        );
        send(&mut s, &array(&[b"HGETALL", b"hash"])).await;
        assert_eq!(
            read_n(&mut s, 26).await,
            b"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n"
        );
    }
}

#[tokio::test]
async fn failed_bgrewriteaof_keeps_old_aof_replayable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("rewrite-fail.aof");
    let temp_path = rewrite_temp_path(&path);

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"SET", b"keep", b"yes"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        tokio::time::sleep(Duration::from_millis(100)).await;

        std::fs::create_dir(&temp_path).unwrap();
        send(&mut s, &array(&[b"BGREWRITEAOF"])).await;
        let rewrite_resp = read_some(&mut s).await;
        assert!(
            rewrite_resp.starts_with(b"+Background append only file rewriting started"),
            "unexpected BGREWRITEAOF reply: {rewrite_resp:?}"
        );
        wait_for_rewrite_count(addr, 1, "err").await;

        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        path.exists(),
        "old AOF should still exist after rewrite failure"
    );
    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"keep"])).await;
        assert_eq!(read_n(&mut s, 9).await, b"$3\r\nyes\r\n");
    }
}

#[tokio::test]
async fn aof_replay_truncates_partial_tail_before_appending_new_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("partial-tail.aof");
    let mut initial = array(&[b"SET", b"a", b"1"]);
    initial.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$5\r\nabc");
    std::fs::write(&path, initial).unwrap();

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"a"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\n1\r\n");

        send(&mut s, &array(&[b"SET", b"b", b"2"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"a"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\n1\r\n");
        send(&mut s, &array(&[b"GET", b"b"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\n2\r\n");
    }
}

#[tokio::test]
async fn aof_replay_truncates_command_that_fails_during_apply() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("semantic-error-tail.aof");
    let mut initial = array(&[b"SET", b"k", b"x"]);
    initial.extend_from_slice(&array(&[b"INCR", b"k"]));
    std::fs::write(&path, initial).unwrap();

    {
        let (addr, shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"k"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\nx\r\n");

        send(&mut s, &array(&[b"SET", b"after", b"1"])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
        let _ = shutdown.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    {
        let (addr, _shutdown) = spawn_with_aof(path.clone()).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        send(&mut s, &array(&[b"GET", b"k"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\nx\r\n");
        send(&mut s, &array(&[b"GET", b"after"])).await;
        assert_eq!(read_n(&mut s, 7).await, b"$1\r\n1\r\n");
    }
}
