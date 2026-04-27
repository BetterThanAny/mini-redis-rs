pub mod string;

use crate::db::Db;
use crate::resp::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(Bytes),
    Set(Bytes, Bytes),
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    Incr(Bytes),
    Decr(Bytes),
    IncrBy(Bytes, i64),
    DecrBy(Bytes, i64),
    Append(Bytes, Bytes),
    Strlen(Bytes),
    MGet(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
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
    #[error("wrong number of arguments for '{0}'")]
    Arity(String),
    #[error("invalid utf8 in command name")]
    BadName,
    #[error("value is not an integer or out of range")]
    NotInt,
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
        let arity_err = || ParseError::Arity(name.clone());
        match name.as_str() {
            "PING" => match rest.len() {
                0 => Ok(Command::Ping(None)),
                1 => Ok(Command::Ping(Some(rest.into_iter().next().unwrap()))),
                _ => Err(arity_err()),
            },
            "ECHO" => one_arg(rest, &name).map(Command::Echo),
            "GET" => one_arg(rest, &name).map(Command::Get),
            "SET" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                Ok(Command::Set(it.next().unwrap(), it.next().unwrap()))
            }
            "DEL" => {
                if rest.is_empty() {
                    return Err(arity_err());
                }
                Ok(Command::Del(rest))
            }
            "EXISTS" => {
                if rest.is_empty() {
                    return Err(arity_err());
                }
                Ok(Command::Exists(rest))
            }
            "INCR" => one_arg(rest, &name).map(Command::Incr),
            "DECR" => one_arg(rest, &name).map(Command::Decr),
            "INCRBY" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let n = parse_i64(&it.next().unwrap())?;
                Ok(Command::IncrBy(key, n))
            }
            "DECRBY" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let n = parse_i64(&it.next().unwrap())?;
                Ok(Command::DecrBy(key, n))
            }
            "APPEND" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                Ok(Command::Append(it.next().unwrap(), it.next().unwrap()))
            }
            "STRLEN" => one_arg(rest, &name).map(Command::Strlen),
            "MGET" => {
                if rest.is_empty() {
                    return Err(arity_err());
                }
                Ok(Command::MGet(rest))
            }
            "MSET" => {
                if rest.is_empty() || rest.len() % 2 != 0 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let mut pairs = Vec::with_capacity(it.len() / 2);
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    pairs.push((k, v));
                }
                Ok(Command::MSet(pairs))
            }
            other => Ok(Command::Unknown(other.to_string())),
        }
    }

    pub fn apply(self, db: &Db) -> Frame {
        match self {
            Command::Ping(None) => Frame::Simple("PONG".into()),
            Command::Ping(Some(msg)) => Frame::Bulk(msg),
            Command::Echo(msg) => Frame::Bulk(msg),
            Command::Get(k) => string::get(db, &k),
            Command::Set(k, v) => string::set(db, k, v, None),
            Command::Del(keys) => string::del(db, &keys),
            Command::Exists(keys) => string::exists(db, &keys),
            Command::Incr(k) => string::incr(db, k, 1),
            Command::Decr(k) => string::incr(db, k, -1),
            Command::IncrBy(k, n) => string::incr(db, k, n),
            Command::DecrBy(k, n) => match n.checked_neg() {
                Some(neg) => string::incr(db, k, neg),
                None => Frame::Error("ERR increment or decrement would overflow".into()),
            },
            Command::Append(k, v) => string::append(db, k, v),
            Command::Strlen(k) => string::strlen(db, &k),
            Command::MGet(keys) => string::mget(db, &keys),
            Command::MSet(pairs) => string::mset(db, pairs),
            Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}

fn one_arg(rest: Vec<Bytes>, name: &str) -> Result<Bytes, ParseError> {
    if rest.len() != 1 {
        Err(ParseError::Arity(name.to_string()))
    } else {
        Ok(rest.into_iter().next().unwrap())
    }
}

fn parse_i64(b: &[u8]) -> Result<i64, ParseError> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(ParseError::NotInt)
}

fn expect_bulk(frame: Frame, idx: usize) -> Result<Bytes, ParseError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        _ => Err(ParseError::NotBulk(idx)),
    }
}
