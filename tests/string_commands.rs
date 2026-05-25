use bytes::Bytes;
use mini_redis_rs::{cmd::string, db::Db, resp::Frame};
use tokio::net::TcpStream;

mod common;
use common::{array, read_n, read_some, send, spawn_server};

#[tokio::test]
async fn set_then_get() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nv\r\n");
}

#[tokio::test]
async fn get_missing_returns_null() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"GET", b"nope"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn del_returns_count() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"a", b"1"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"SET", b"b", b"2"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"DEL", b"a", b"b", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
}

#[tokio::test]
async fn exists_counts_existing() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"a", b"1"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"EXISTS", b"a", b"a", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
}

#[tokio::test]
async fn incr_creates_if_missing() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"INCR", b"c"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":1\r\n");
    send(&mut s, &array(&[b"INCR", b"c"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
}

#[tokio::test]
async fn incr_non_integer_errors() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"foo"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"INCR", b"k"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-ERR"), "got: {:?}", resp);
}

#[tokio::test]
async fn incrby_decrby() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"INCRBY", b"x", b"10"])).await;
    assert_eq!(read_n(&mut s, 5).await, b":10\r\n");
    send(&mut s, &array(&[b"DECRBY", b"x", b"3"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":7\r\n");
}

#[tokio::test]
async fn incr_overflow_errors() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"x", b"9223372036854775806"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"INCRBY", b"x", b"100"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-ERR"));
}

#[tokio::test]
async fn append_grows_value() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"APPEND", b"k", b"hi"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
    send(&mut s, &array(&[b"APPEND", b"k", b"-there"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":8\r\n");
    send(&mut s, &array(&[b"GET", b"k"])).await;
    assert_eq!(read_n(&mut s, 14).await, b"$8\r\nhi-there\r\n");
}

#[tokio::test]
async fn strlen_works() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"hello"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"STRLEN", b"k"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":5\r\n");
    send(&mut s, &array(&[b"STRLEN", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn mset_mget() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(
        &mut s,
        &array(&[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"]),
    )
    .await;
    assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
    send(&mut s, &array(&[b"MGET", b"a", b"missing", b"b"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n");
}

#[test]
fn large_mget_response_is_rejected_before_cloning_all_values() {
    let db = Db::new();
    let value = Bytes::from(vec![b'x'; 1024 * 1024]);
    let keys: Vec<Bytes> = (0..65)
        .map(|idx| Bytes::from(format!("huge-mget:{idx}")))
        .collect();
    for key in &keys {
        assert_eq!(
            string::set_at(&db, key.clone(), value.clone(), None),
            Frame::Simple("OK".into())
        );
    }

    match string::mget(&db, &keys) {
        Frame::Error(err) => assert!(err.contains("response exceeds output limit")),
        other => panic!("expected response limit error, got {other:?}"),
    }
}

#[tokio::test]
async fn arity_error_on_bad_args() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"only_key"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-ERR"), "got: {:?}", resp);
}

#[tokio::test]
async fn many_keys_through_shards() {
    // hits multiple shards because of xxhash distribution
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    for i in 0..100 {
        let k = format!("k{i}").into_bytes();
        let v = format!("v{i}").into_bytes();
        send(&mut s, &array(&[b"SET", &k, &v])).await;
        assert_eq!(read_n(&mut s, 5).await, b"+OK\r\n");
    }
    for i in 0..100 {
        let k = format!("k{i}").into_bytes();
        send(&mut s, &array(&[b"GET", &k])).await;
        let expected_val = format!("v{i}");
        let expected = format!("${}\r\n{}\r\n", expected_val.len(), expected_val);
        let buf = read_n(&mut s, expected.len()).await;
        assert_eq!(buf, expected.as_bytes());
    }
}
