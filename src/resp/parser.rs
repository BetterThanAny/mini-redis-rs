use super::{Error, Frame};
use bytes::{Buf, Bytes, BytesMut};

/// Mirrors Redis's `proto-max-bulk-len` default (512 MB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Sensible cap on array length (1M elements) — without this a malicious
/// `*999999999\r\n` would `Vec::with_capacity` ~tens of GB.
const MAX_ARRAY_LEN: usize = 1024 * 1024;
/// Cap nested arrays so a hostile client cannot stack-overflow the parser.
const MAX_NESTING_DEPTH: usize = 128;
/// RESP line headers are tiny in normal Redis traffic. Without a cap, a client
/// can stream an unterminated line forever and grow the input buffer unboundedly.
const MAX_LINE_LEN: usize = 1024 * 1024;

pub fn parse(buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
    let mut cursor = std::io::Cursor::new(&buf[..]);
    match parse_frame(&mut cursor, 0) {
        Ok(frame) => {
            let n = cursor.position() as usize;
            buf.advance(n);
            Ok(Some(frame))
        }
        Err(Error::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_frame(c: &mut std::io::Cursor<&[u8]>, depth: usize) -> Result<Frame, Error> {
    if depth > MAX_NESTING_DEPTH {
        return Err(Error::Protocol(format!(
            "array nesting depth exceeds limit {MAX_NESTING_DEPTH}"
        )));
    }
    let tag = read_u8(c)?;
    match tag {
        b'+' => Ok(Frame::Simple(read_line_string(c)?)),
        b'-' => Ok(Frame::Error(read_line_string(c)?)),
        b':' => Ok(Frame::Integer(read_line_int(c)?)),
        b'$' => parse_bulk(c),
        b'*' => parse_array(c, depth),
        other => Err(Error::Protocol(format!("invalid type byte: 0x{other:02x}"))),
    }
}

fn parse_bulk(c: &mut std::io::Cursor<&[u8]>) -> Result<Frame, Error> {
    let len = read_line_int(c)?;
    if len == -1 {
        return Ok(Frame::Null);
    }
    let len = usize::try_from(len).map_err(|_| Error::Protocol("negative bulk len".into()))?;
    if len > MAX_BULK_LEN {
        return Err(Error::Protocol(format!(
            "bulk length {len} exceeds limit {MAX_BULK_LEN}"
        )));
    }
    let remaining = c.get_ref().len() - c.position() as usize;
    if remaining < len + 2 {
        return Err(Error::Incomplete);
    }
    let start = c.position() as usize;
    let bytes = Bytes::copy_from_slice(&c.get_ref()[start..start + len]);
    c.set_position((start + len) as u64);
    if read_u8(c)? != b'\r' || read_u8(c)? != b'\n' {
        return Err(Error::Protocol("missing CRLF after bulk".into()));
    }
    Ok(Frame::Bulk(bytes))
}

fn parse_array(c: &mut std::io::Cursor<&[u8]>, depth: usize) -> Result<Frame, Error> {
    let count = read_line_int(c)?;
    if count == -1 {
        return Ok(Frame::Null);
    }
    let count = usize::try_from(count).map_err(|_| Error::Protocol("negative array len".into()))?;
    if count > MAX_ARRAY_LEN {
        return Err(Error::Protocol(format!(
            "array length {count} exceeds limit {MAX_ARRAY_LEN}"
        )));
    }
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(parse_frame(c, depth + 1)?);
    }
    Ok(Frame::Array(items))
}

fn read_u8(c: &mut std::io::Cursor<&[u8]>) -> Result<u8, Error> {
    if !c.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(c.get_u8())
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if buf.len() < start + 2 {
        return None;
    }
    (start..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

fn read_line<'a>(c: &mut std::io::Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = c.position() as usize;
    let buf: &'a [u8] = c.get_ref();
    let crlf = match find_crlf(buf, start) {
        Some(crlf) => crlf,
        None if buf.len().saturating_sub(start) > MAX_LINE_LEN => {
            return Err(Error::Protocol(format!(
                "line length exceeds limit {MAX_LINE_LEN}"
            )));
        }
        None => return Err(Error::Incomplete),
    };
    c.set_position((crlf + 2) as u64);
    Ok(&buf[start..crlf])
}

fn read_line_string(c: &mut std::io::Cursor<&[u8]>) -> Result<String, Error> {
    let line = read_line(c)?;
    std::str::from_utf8(line)
        .map(|s| s.to_string())
        .map_err(|_| Error::Protocol("invalid utf8 in line".into()))
}

fn read_line_int(c: &mut std::io::Cursor<&[u8]>) -> Result<i64, Error> {
    let line = read_line(c)?;
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Protocol("invalid integer".into()))
}
