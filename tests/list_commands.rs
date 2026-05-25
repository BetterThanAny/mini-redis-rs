use bytes::Bytes;
use mini_redis_rs::{cmd::list, db::Db, resp::Frame};
use tokio::net::TcpStream;

mod common;
use common::{array, read_n, read_some, send, spawn_server};

#[tokio::test]
async fn rpush_then_lrange_in_order() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":3\r\n");
    send(&mut s, &array(&[b"LRANGE", b"l", b"0", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

#[tokio::test]
async fn lpush_reverses_order() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"0", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
}

#[tokio::test]
async fn lpop_single_returns_bulk() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"x", b"y", b"z"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nx\r\n");
}

#[tokio::test]
async fn rpop_single_returns_bulk() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"x", b"y", b"z"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"RPOP", b"l"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nz\r\n");
}

#[tokio::test]
async fn lpop_count_returns_array() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c", b"d"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l", b"2"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

#[tokio::test]
async fn lpop_on_empty_returns_null() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LPOP", b"missing"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn pop_missing_with_count_returns_null_array() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    send(&mut s, &array(&[b"LPOP", b"missing", b"2"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"*-1\r\n");

    send(&mut s, &array(&[b"RPOP", b"missing", b"0"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"*-1\r\n");
}

#[tokio::test]
async fn llen_works() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LLEN", b"l"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":2\r\n");
    send(&mut s, &array(&[b"LLEN", b"missing"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}

#[tokio::test]
async fn lindex_positive_and_negative() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LINDEX", b"l", b"0"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\na\r\n");
    send(&mut s, &array(&[b"LINDEX", b"l", b"-1"])).await;
    assert_eq!(read_n(&mut s, 7).await, b"$1\r\nc\r\n");
    send(&mut s, &array(&[b"LINDEX", b"l", b"99"])).await;
    assert_eq!(read_n(&mut s, 5).await, b"$-1\r\n");
}

#[tokio::test]
async fn lrange_negative_indices() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(
        &mut s,
        &array(&[b"RPUSH", b"l", b"a", b"b", b"c", b"d", b"e"]),
    )
    .await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"-2", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n");
}

#[tokio::test]
async fn lrange_missing_returns_empty_array() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"LRANGE", b"missing", b"0", b"-1"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lpush_on_string_key_wrongtype() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"SET", b"k", b"v"])).await;
    let _ = read_n(&mut s, 5).await;
    send(&mut s, &array(&[b"LPUSH", b"k", b"x"])).await;
    let resp = read_some(&mut s).await;
    assert!(resp.starts_with(b"-WRONGTYPE"), "got: {:?}", resp);
}

#[tokio::test]
async fn lrange_start_beyond_end_returns_empty() {
    // Regression for C1: previously returned a 1-element array because clamp wrongly
    // pinned start to len-1.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"5", b"10"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lrange_negative_stop_before_list_returns_empty() {
    // -100 on a 3-elem list resolves to "before index 0" -> empty.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a", b"b", b"c"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LRANGE", b"l", b"-100", b"-50"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn lpop_with_count_zero_returns_empty_array() {
    // Regression for H4: previously returned $-1\r\n.
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l", b"0"])).await;
    assert_eq!(read_n(&mut s, 4).await, b"*0\r\n");
}

#[tokio::test]
async fn pop_negative_count_matches_redis_error() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"a"])).await;
    let _ = read_n(&mut s, 4).await;

    send(&mut s, &array(&[b"LPOP", b"l", b"-1"])).await;
    let resp = read_some(&mut s).await;
    assert_eq!(resp, b"-ERR value is out of range, must be positive\r\n");
}

#[test]
fn large_lrange_response_is_rejected_before_cloning_all_values() {
    let db = Db::new();
    let value = Bytes::from(vec![b'x'; 1024 * 1024]);
    let values = (0..65).map(|_| value.clone()).collect();
    assert_eq!(
        list::rpush(&db, Bytes::from_static(b"huge-list"), values),
        Frame::Integer(65)
    );

    match list::lrange(&db, &Bytes::from_static(b"huge-list"), 0, -1) {
        Frame::Error(err) => assert!(err.contains("response exceeds output limit")),
        other => panic!("expected response limit error, got {other:?}"),
    }
}

#[test]
fn oversized_lpop_count_does_not_mutate_list() {
    let db = Db::new();
    let key = Bytes::from_static(b"huge-pop-list");
    let value = Bytes::from(vec![b'x'; 1024 * 1024]);
    let values = (0..65).map(|_| value.clone()).collect();
    assert_eq!(list::rpush(&db, key.clone(), values), Frame::Integer(65));

    match list::lpop(&db, &key, Some(65)) {
        Frame::Error(err) => assert!(err.contains("response exceeds output limit")),
        other => panic!("expected response limit error, got {other:?}"),
    }
    assert_eq!(list::llen(&db, &key), Frame::Integer(65));
}

#[tokio::test]
async fn pop_empties_then_del_via_pop() {
    let (addr, _g) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    send(&mut s, &array(&[b"RPUSH", b"l", b"only"])).await;
    let _ = read_n(&mut s, 4).await;
    send(&mut s, &array(&[b"LPOP", b"l"])).await;
    let _ = read_n(&mut s, 10).await;
    send(&mut s, &array(&[b"EXISTS", b"l"])).await;
    assert_eq!(read_n(&mut s, 4).await, b":0\r\n");
}
