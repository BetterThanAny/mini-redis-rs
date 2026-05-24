use super::Frame;
use bytes::{BufMut, BytesMut};

pub fn encode(frame: &Frame, out: &mut BytesMut) {
    match frame {
        Frame::Simple(s) => {
            out.put_u8(b'+');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            out.put_u8(b'-');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            out.put_u8(b':');
            out.put_slice(n.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Bulk(b) => {
            out.put_u8(b'$');
            out.put_slice(b.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        Frame::Null => out.put_slice(b"$-1\r\n"),
        Frame::NullArray => out.put_slice(b"*-1\r\n"),
        Frame::Array(items) => {
            out.put_u8(b'*');
            out.put_slice(items.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for item in items {
                encode(item, out);
            }
        }
    }
}
