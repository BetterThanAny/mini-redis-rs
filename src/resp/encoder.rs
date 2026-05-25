use super::Frame;
use crate::limits;
use bytes::{BufMut, BytesMut};

pub fn encoded_len(frame: &Frame) -> Option<usize> {
    match frame {
        Frame::Simple(s) | Frame::Error(s) => 1usize.checked_add(s.len())?.checked_add(2),
        Frame::Integer(n) => 1usize
            .checked_add(limits::decimal_len_i64(*n))?
            .checked_add(2),
        Frame::Bulk(b) => limits::resp_bulk_len(b.len()),
        Frame::Null | Frame::NullArray => Some(5),
        Frame::Array(items) => {
            let mut len = limits::resp_array_header_len(items.len())?;
            for item in items {
                len = len.checked_add(encoded_len(item)?)?;
            }
            Some(len)
        }
    }
}

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
