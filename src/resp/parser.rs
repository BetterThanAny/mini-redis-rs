use super::{Error, Frame};
use bytes::{Buf, Bytes, BytesMut};

/// Bound bulk frames to the per-connection buffer budget so an oversized
/// declaration is rejected from the header instead of after reading tens of MB.
const MAX_BULK_LEN: usize = crate::limits::MAX_BULK_LEN;
/// Sensible cap on array length (1M elements) — without this a malicious
/// `*999999999\r\n` would `Vec::with_capacity` ~tens of GB.
const MAX_ARRAY_LEN: usize = 1024 * 1024;
/// Cap nested arrays so a hostile client cannot stack-overflow the parser.
const MAX_NESTING_DEPTH: usize = 128;
/// RESP line headers are tiny in normal Redis traffic. Without a cap, a client
/// can stream an unterminated line forever and grow the input buffer unboundedly.
const MAX_LINE_LEN: usize = 1024 * 1024;

pub fn parse(buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
    loop {
        if buf.is_empty() {
            return Ok(None);
        }
        if !is_resp_type(buf[0]) {
            let before_len = buf.len();
            match parse_inline(buf)? {
                Some(frame) => return Ok(Some(frame)),
                None if buf.len() == before_len => return Ok(None),
                None => continue,
            }
        }

        let mut cursor = std::io::Cursor::new(&buf[..]);
        return match parse_frame(&mut cursor, 0) {
            Ok(frame) => {
                let n = cursor.position() as usize;
                buf.advance(n);
                Ok(Some(frame))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e),
        };
    }
}

fn is_resp_type(byte: u8) -> bool {
    matches!(byte, b'+' | b'-' | b':' | b'$' | b'*')
}

fn parse_inline(buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
    let line_end = match find_lf(buf, 0) {
        Some(pos) => pos,
        None if buf.len() > MAX_LINE_LEN => {
            return Err(Error::Protocol(format!(
                "line length exceeds limit {MAX_LINE_LEN}"
            )));
        }
        None => return Ok(None),
    };
    if line_end > MAX_LINE_LEN {
        return Err(Error::Protocol(format!(
            "line length exceeds limit {MAX_LINE_LEN}"
        )));
    }

    let mut line = &buf[..line_end];
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }
    let frame = inline_frame(line)?;
    buf.advance(line_end + 1);
    Ok(frame)
}

fn inline_frame(line: &[u8]) -> Result<Option<Frame>, Error> {
    let args = split_inline_args(line)?;
    if args.is_empty() {
        return Ok(None);
    }
    Ok(Some(Frame::Array(
        args.into_iter().map(Frame::Bulk).collect(),
    )))
}

fn split_inline_args(line: &[u8]) -> Result<Vec<Bytes>, Error> {
    let mut args = Vec::new();
    let mut idx = 0usize;
    while idx < line.len() {
        while idx < line.len() && is_inline_space(line[idx]) {
            idx += 1;
        }
        if idx == line.len() {
            break;
        }

        let mut arg = Vec::new();
        while idx < line.len() && !is_inline_space(line[idx]) {
            match line[idx] {
                b'\'' => {
                    idx += 1;
                    read_inline_quoted(line, &mut idx, &mut arg, b'\'')?;
                }
                b'"' => {
                    idx += 1;
                    read_inline_quoted(line, &mut idx, &mut arg, b'"')?;
                }
                b'\\' => {
                    idx += 1;
                    if idx == line.len() {
                        return Err(Error::Protocol(
                            "unterminated escape sequence in inline request".into(),
                        ));
                    }
                    arg.push(line[idx]);
                    idx += 1;
                }
                byte => {
                    arg.push(byte);
                    idx += 1;
                }
            }
        }
        args.push(Bytes::from(arg));
    }
    Ok(args)
}

fn read_inline_quoted(
    line: &[u8],
    idx: &mut usize,
    out: &mut Vec<u8>,
    quote: u8,
) -> Result<(), Error> {
    while *idx < line.len() {
        let byte = line[*idx];
        *idx += 1;
        if byte == quote {
            return Ok(());
        }
        if byte != b'\\' {
            out.push(byte);
            continue;
        }
        if *idx == line.len() {
            return Err(Error::Protocol(
                "unterminated escape sequence in inline request".into(),
            ));
        }

        let escaped = line[*idx];
        *idx += 1;
        match escaped {
            b'n' if quote == b'"' => out.push(b'\n'),
            b'r' if quote == b'"' => out.push(b'\r'),
            b't' if quote == b'"' => out.push(b'\t'),
            b'b' if quote == b'"' => out.push(8),
            b'a' if quote == b'"' => out.push(7),
            b'x' if quote == b'"' && *idx + 1 < line.len() => {
                if let (Some(high), Some(low)) = (hex_value(line[*idx]), hex_value(line[*idx + 1]))
                {
                    out.push((high << 4) | low);
                    *idx += 2;
                } else {
                    out.push(escaped);
                }
            }
            other => out.push(other),
        }
    }
    Err(Error::Protocol(
        "unbalanced quotes in inline request".into(),
    ))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn is_inline_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
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
        return Ok(Frame::NullArray);
    }
    let count = usize::try_from(count).map_err(|_| Error::Protocol("negative array len".into()))?;
    if count > MAX_ARRAY_LEN {
        return Err(Error::Protocol(format!(
            "array length {count} exceeds limit {MAX_ARRAY_LEN}"
        )));
    }
    let mut items = Vec::with_capacity(count.min(1024));
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

fn find_lf(buf: &[u8], start: usize) -> Option<usize> {
    buf.get(start..)?
        .iter()
        .position(|&byte| byte == b'\n')
        .map(|offset| start + offset)
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
