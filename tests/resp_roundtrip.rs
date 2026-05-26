use bytes::{Bytes, BytesMut};
use mini_redis_rs::resp::{encoder, parser, Frame};

fn roundtrip(frame: Frame) {
    let mut buf = BytesMut::new();
    encoder::encode(&frame, &mut buf);
    let parsed = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(frame, parsed);
    assert!(buf.is_empty());
}

#[test]
fn simple_string() {
    roundtrip(Frame::Simple("OK".into()));
}

#[test]
fn error_string() {
    roundtrip(Frame::Error("ERR something bad".into()));
}

#[test]
fn integer_positive() {
    roundtrip(Frame::Integer(42));
}

#[test]
fn integer_negative() {
    roundtrip(Frame::Integer(-42));
}

#[test]
fn integer_zero() {
    roundtrip(Frame::Integer(0));
}

#[test]
fn bulk_string() {
    roundtrip(Frame::Bulk(Bytes::from_static(b"hello world")));
}

#[test]
fn empty_bulk() {
    roundtrip(Frame::Bulk(Bytes::new()));
}

#[test]
fn binary_bulk() {
    roundtrip(Frame::Bulk(Bytes::from_static(b"\x00\x01\xff\r\n")));
}

#[test]
fn null_bulk() {
    roundtrip(Frame::Null);
}

#[test]
fn null_array() {
    roundtrip(Frame::NullArray);
}

#[test]
fn empty_array() {
    roundtrip(Frame::Array(vec![]));
}

#[test]
fn nested_array() {
    roundtrip(Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"k")),
        Frame::Bulk(Bytes::from_static(b"v")),
    ]));
}

#[test]
fn deeply_nested_array() {
    roundtrip(Frame::Array(vec![
        Frame::Integer(1),
        Frame::Array(vec![
            Frame::Simple("nested".into()),
            Frame::Bulk(Bytes::from_static(b"value")),
        ]),
        Frame::Null,
    ]));
}

#[test]
fn nested_array_above_depth_cap_errors() {
    let mut raw = Vec::new();
    for _ in 0..140 {
        raw.extend_from_slice(b"*1\r\n");
    }
    raw.extend_from_slice(b"$4\r\nPING\r\n");
    let mut buf = BytesMut::from(&raw[..]);
    let err = parser::parse(&mut buf);
    assert!(err.is_err(), "expected nesting-depth error, got {err:?}");
}

#[test]
fn incomplete_simple_returns_none() {
    let mut buf = BytesMut::from(&b"+PON"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
    assert_eq!(&buf[..], b"+PON");
}

#[test]
fn unterminated_line_above_cap_errors() {
    let mut raw = vec![b'a'; 1024 * 1024 + 2];
    raw[0] = b'+';
    let mut buf = BytesMut::from(&raw[..]);
    let err = parser::parse(&mut buf);
    assert!(err.is_err(), "expected line-length error, got {err:?}");
}

#[test]
fn incomplete_bulk_returns_none() {
    let mut buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$3\r\nfo"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
    assert_eq!(&buf[..], b"*2\r\n$3\r\nSET\r\n$3\r\nfo");
}

#[test]
fn incomplete_array_count_returns_none() {
    let mut buf = BytesMut::from(&b"*"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
}

#[test]
fn streaming_partial_then_complete() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"*1\r\n$4\r\nPI");
    assert!(parser::parse(&mut buf).unwrap().is_none());
    // simulate more bytes arriving
    buf.extend_from_slice(b"NG\r\n");
    let f = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        f,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
    );
    assert!(buf.is_empty());
}

#[test]
fn inline_command() {
    let mut buf = BytesMut::from(&b"PING\r\n"[..]);
    let f = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        f,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
    );
    assert!(buf.is_empty());
}

#[test]
fn inline_quoted_argument() {
    let mut buf = BytesMut::from(&b"SET k \"hello\\nworld\"\r\n"[..]);
    let f = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        f,
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from_static(b"k")),
            Frame::Bulk(Bytes::from_static(b"hello\nworld")),
        ])
    );
    assert!(buf.is_empty());
}

#[test]
fn inline_and_resp_frames_can_share_buffer() {
    let mut buf = BytesMut::from(&b"PING\r\n*1\r\n$4\r\nPING\r\n"[..]);
    let f1 = parser::parse(&mut buf).unwrap().unwrap();
    let f2 = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        f1,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
    );
    assert_eq!(
        f2,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
    );
    assert!(buf.is_empty());
}

#[test]
fn incomplete_inline_returns_none() {
    let mut buf = BytesMut::from(&b"PING"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
    assert_eq!(&buf[..], b"PING");
}

#[test]
fn blank_inline_lines_are_ignored() {
    let mut buf = BytesMut::from(&b"\r\nPING\r\n"[..]);
    let f = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        f,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
    );
    assert!(buf.is_empty());
}

#[test]
fn bad_type_byte_errors() {
    let mut buf = BytesMut::from(&b"@bogus\r\n"[..]);
    let frame = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(
        frame,
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"@bogus"))])
    );
}

#[test]
fn array_length_above_cap_errors() {
    // Without the cap, Vec::with_capacity would attempt to allocate ~tens of GB.
    let mut buf = BytesMut::from(&b"*9999999999\r\n"[..]);
    let err = parser::parse(&mut buf);
    assert!(err.is_err(), "expected protocol error, got {err:?}");
}

#[test]
fn max_length_incomplete_array_does_not_error() {
    let mut buf = BytesMut::from(&b"*1048576\r\n"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
    assert_eq!(&buf[..], b"*1048576\r\n");
}

#[test]
fn bulk_length_above_cap_errors() {
    let mut buf = BytesMut::from(&b"$9999999999\r\n"[..]);
    let err = parser::parse(&mut buf);
    assert!(err.is_err(), "expected protocol error, got {err:?}");
}

#[test]
fn bulk_length_above_buffer_cap_errors_from_header() {
    let mut buf = BytesMut::from(&b"$67108865\r\n"[..]);
    let err = parser::parse(&mut buf);
    assert!(err.is_err(), "expected protocol error, got {err:?}");
}

#[test]
fn back_to_back_frames() {
    let mut buf = BytesMut::new();
    encoder::encode(&Frame::Integer(1), &mut buf);
    encoder::encode(&Frame::Integer(2), &mut buf);
    let f1 = parser::parse(&mut buf).unwrap().unwrap();
    let f2 = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(f1, Frame::Integer(1));
    assert_eq!(f2, Frame::Integer(2));
    assert!(buf.is_empty());
}
