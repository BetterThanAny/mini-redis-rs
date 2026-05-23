use tokio::net::TcpStream;

mod common;
use common::{array, read_n, read_some, send, spawn_server};

#[tokio::test]
async fn string_ttl_hash_list_and_pubsub_resp2_shapes_match_redis_cli() {
    let (addr, _shutdown) = spawn_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send(&mut client, &array(&[b"SET", b"hello", b"world"])).await;
    assert_eq!(read_n(&mut client, 5).await, b"+OK\r\n");

    send(&mut client, &array(&[b"GET", b"hello"])).await;
    assert_eq!(read_n(&mut client, 11).await, b"$5\r\nworld\r\n");

    send(&mut client, &array(&[b"EXPIRE", b"hello", b"30"])).await;
    assert_eq!(read_n(&mut client, 4).await, b":1\r\n");

    send(&mut client, &array(&[b"TTL", b"hello"])).await;
    let ttl = read_some(&mut client).await;
    assert!(
        ttl == b":30\r\n" || ttl == b":29\r\n",
        "unexpected TTL frame: {:?}",
        ttl
    );

    send(
        &mut client,
        &array(&[b"HSET", b"user:1", b"name", b"alice"]),
    )
    .await;
    assert_eq!(read_n(&mut client, 4).await, b":1\r\n");

    send(&mut client, &array(&[b"HGETALL", b"user:1"])).await;
    assert_eq!(
        read_n(&mut client, 25).await,
        b"*2\r\n$4\r\nname\r\n$5\r\nalice\r\n"
    );

    send(&mut client, &array(&[b"RPUSH", b"log", b"a", b"b", b"c"])).await;
    assert_eq!(read_n(&mut client, 4).await, b":3\r\n");

    send(&mut client, &array(&[b"LRANGE", b"log", b"0", b"-1"])).await;
    assert_eq!(
        read_n(&mut client, 25).await,
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );

    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    send(&mut subscriber, &array(&[b"SUBSCRIBE", b"news"])).await;
    assert_eq!(
        read_some(&mut subscriber).await,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
    );

    send(&mut client, &array(&[b"PUBLISH", b"news", b"hello"])).await;
    assert_eq!(read_n(&mut client, 4).await, b":1\r\n");
    assert_eq!(
        read_some(&mut subscriber).await,
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
    );
}

#[tokio::test]
async fn info_returns_bulk_sections_for_redis_cli() {
    let (addr, _shutdown) = spawn_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send(&mut client, &array(&[b"INFO"])).await;
    let resp = read_some(&mut client).await;
    assert!(
        resp.starts_with(b"$"),
        "INFO must be a bulk string: {resp:?}"
    );
    let text = std::str::from_utf8(&resp).unwrap();
    assert!(
        text.contains("# Server\r\n"),
        "missing server section: {text}"
    );
    assert!(
        text.contains("# Clients\r\n"),
        "missing clients section: {text}"
    );
    assert!(
        text.contains("# Memory\r\n"),
        "missing memory section: {text}"
    );
    assert!(
        text.contains("# Persistence\r\n"),
        "missing persistence section: {text}"
    );
    assert!(
        text.contains("aof_enabled:0\r\n"),
        "expected no-AOF persistence state: {text}"
    );
}
