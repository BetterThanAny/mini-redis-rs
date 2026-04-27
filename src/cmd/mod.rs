use crate::resp::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Unknown(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("not an array")]
    NotArray,
    #[error("empty command")]
    Empty,
    #[error("argument {0} must be a bulk string")]
    NotBulk(usize),
    #[error("wrong number of arguments for {0}")]
    Arity(String),
    #[error("invalid utf8 in command name")]
    BadName,
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, ParseError> {
        let mut items = match frame {
            Frame::Array(v) => v.into_iter(),
            _ => return Err(ParseError::NotArray),
        };
        let name_frame = items.next().ok_or(ParseError::Empty)?;
        let name_bytes = expect_bulk(name_frame, 0)?;
        let name = std::str::from_utf8(&name_bytes)
            .map_err(|_| ParseError::BadName)?
            .to_ascii_uppercase();
        let rest: Vec<Bytes> = items
            .enumerate()
            .map(|(i, f)| expect_bulk(f, i + 1))
            .collect::<Result<_, _>>()?;
        match name.as_str() {
            "PING" => match rest.len() {
                0 => Ok(Command::Ping(None)),
                1 => Ok(Command::Ping(Some(rest.into_iter().next().unwrap()))),
                _ => Err(ParseError::Arity("PING".into())),
            },
            "ECHO" => {
                if rest.len() != 1 {
                    return Err(ParseError::Arity("ECHO".into()));
                }
                Ok(Command::Echo(rest.into_iter().next().unwrap()))
            }
            other => Ok(Command::Unknown(other.to_string())),
        }
    }

    pub fn apply(self) -> Frame {
        match self {
            Command::Ping(None) => Frame::Simple("PONG".into()),
            Command::Ping(Some(msg)) => Frame::Bulk(msg),
            Command::Echo(msg) => Frame::Bulk(msg),
            Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}

fn expect_bulk(frame: Frame, idx: usize) -> Result<Bytes, ParseError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        _ => Err(ParseError::NotBulk(idx)),
    }
}
