pub mod hash;
pub mod list;
pub mod string;

use crate::db::{now_millis, Db, ExpireAt};
use crate::resp::Frame;
use bytes::Bytes;

const MAX_EXPIRE_AT_MS: ExpireAt = i64::MAX as ExpireAt;
const MILLIS_PER_SECOND: ExpireAt = 1000;

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expires_at: Option<ExpireAt>,
    },
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
    Expire(Bytes, ExpireAt),
    PExpire(Bytes, ExpireAt),
    PExpireAt(Bytes, ExpireAt),
    Ttl(Bytes),
    PTtl(Bytes),
    Persist(Bytes),
    LPush(Bytes, Vec<Bytes>),
    RPush(Bytes, Vec<Bytes>),
    LPop(Bytes, Option<usize>),
    RPop(Bytes, Option<usize>),
    LRange(Bytes, i64, i64),
    LLen(Bytes),
    LIndex(Bytes, i64),
    HSet(Bytes, Vec<(Bytes, Bytes)>),
    HGet(Bytes, Bytes),
    HDel(Bytes, Vec<Bytes>),
    HKeys(Bytes),
    HVals(Bytes),
    HGetAll(Bytes),
    HExists(Bytes, Bytes),
    HLen(Bytes),
    HIncrBy(Bytes, Bytes, i64),
    Subscribe(Vec<Bytes>),
    Unsubscribe(Option<Vec<Bytes>>),
    Publish(Bytes, Bytes),
    Info(Option<String>),
    BgRewriteAof,
    Unknown(String),
}

impl Command {
    /// Whether this command mutates persisted state (i.e. should be written to AOF).
    /// Exhaustive match — adding a new variant fails to compile until classified here.
    pub fn is_write(&self) -> bool {
        use Command::*;
        match self {
            Set { .. }
            | Del(_)
            | Incr(_)
            | Decr(_)
            | IncrBy(_, _)
            | DecrBy(_, _)
            | Append(_, _)
            | MSet(_)
            | Expire(_, _)
            | PExpire(_, _)
            | PExpireAt(_, _)
            | Persist(_)
            | LPush(_, _)
            | RPush(_, _)
            | LPop(_, _)
            | RPop(_, _)
            | HSet(_, _)
            | HDel(_, _)
            | HIncrBy(_, _, _) => true,
            Ping(_)
            | Echo(_)
            | Get(_)
            | Exists(_)
            | Strlen(_)
            | MGet(_)
            | Ttl(_)
            | PTtl(_)
            | LRange(_, _, _)
            | LLen(_)
            | LIndex(_, _)
            | HGet(_, _)
            | HKeys(_)
            | HVals(_)
            | HGetAll(_)
            | HExists(_, _)
            | HLen(_)
            | Subscribe(_)
            | Unsubscribe(_)
            | Publish(_, _)
            | Info(_)
            | BgRewriteAof
            | Unknown(_) => false,
        }
    }

    pub fn aof_frame(&self) -> Option<Frame> {
        use Command::*;
        match self {
            Set {
                key,
                value,
                expires_at,
            } => {
                let mut parts = vec![
                    bulk(b"SET"),
                    Frame::Bulk(key.clone()),
                    Frame::Bulk(value.clone()),
                ];
                if let Some(deadline) = expires_at {
                    parts.push(bulk(b"PXAT"));
                    parts.push(bulk_string(deadline));
                }
                Some(Frame::Array(parts))
            }
            Del(keys) => Some(command_with_keys(b"DEL", keys)),
            Incr(key) => Some(command_one(b"INCR", key)),
            Decr(key) => Some(command_one(b"DECR", key)),
            IncrBy(key, n) => Some(Frame::Array(vec![
                bulk(b"INCRBY"),
                Frame::Bulk(key.clone()),
                bulk_string(n),
            ])),
            DecrBy(key, n) => Some(Frame::Array(vec![
                bulk(b"DECRBY"),
                Frame::Bulk(key.clone()),
                bulk_string(n),
            ])),
            Append(key, value) => Some(Frame::Array(vec![
                bulk(b"APPEND"),
                Frame::Bulk(key.clone()),
                Frame::Bulk(value.clone()),
            ])),
            MSet(pairs) => {
                let mut parts = Vec::with_capacity(pairs.len() * 2 + 1);
                parts.push(bulk(b"MSET"));
                for (key, value) in pairs {
                    parts.push(Frame::Bulk(key.clone()));
                    parts.push(Frame::Bulk(value.clone()));
                }
                Some(Frame::Array(parts))
            }
            Expire(key, deadline) | PExpire(key, deadline) | PExpireAt(key, deadline) => {
                Some(pexpireat_frame(key, *deadline))
            }
            Persist(key) => Some(command_one(b"PERSIST", key)),
            LPush(key, values) => Some(command_with_values(b"LPUSH", key, values)),
            RPush(key, values) => Some(command_with_values(b"RPUSH", key, values)),
            LPop(key, None) => Some(command_one(b"LPOP", key)),
            LPop(key, Some(count)) => Some(Frame::Array(vec![
                bulk(b"LPOP"),
                Frame::Bulk(key.clone()),
                bulk_string(count),
            ])),
            RPop(key, None) => Some(command_one(b"RPOP", key)),
            RPop(key, Some(count)) => Some(Frame::Array(vec![
                bulk(b"RPOP"),
                Frame::Bulk(key.clone()),
                bulk_string(count),
            ])),
            HSet(key, pairs) => {
                let mut parts = Vec::with_capacity(pairs.len() * 2 + 2);
                parts.push(bulk(b"HSET"));
                parts.push(Frame::Bulk(key.clone()));
                for (field, value) in pairs {
                    parts.push(Frame::Bulk(field.clone()));
                    parts.push(Frame::Bulk(value.clone()));
                }
                Some(Frame::Array(parts))
            }
            HDel(key, fields) => {
                let mut parts = Vec::with_capacity(fields.len() + 2);
                parts.push(bulk(b"HDEL"));
                parts.push(Frame::Bulk(key.clone()));
                parts.extend(fields.iter().cloned().map(Frame::Bulk));
                Some(Frame::Array(parts))
            }
            HIncrBy(key, field, n) => Some(Frame::Array(vec![
                bulk(b"HINCRBY"),
                Frame::Bulk(key.clone()),
                Frame::Bulk(field.clone()),
                bulk_string(n),
            ])),
            Ping(_)
            | Echo(_)
            | Get(_)
            | Exists(_)
            | Strlen(_)
            | MGet(_)
            | Ttl(_)
            | PTtl(_)
            | LRange(_, _, _)
            | LLen(_)
            | LIndex(_, _)
            | HGet(_, _)
            | HKeys(_)
            | HVals(_)
            | HGetAll(_)
            | HExists(_, _)
            | HLen(_)
            | Subscribe(_)
            | Unsubscribe(_)
            | Publish(_, _)
            | Info(_)
            | BgRewriteAof
            | Unknown(_) => None,
        }
    }

    pub fn mutated_keys(&self) -> Vec<Bytes> {
        use Command::*;
        match self {
            Set { key, .. }
            | Incr(key)
            | Decr(key)
            | IncrBy(key, _)
            | DecrBy(key, _)
            | Append(key, _)
            | Expire(key, _)
            | PExpire(key, _)
            | PExpireAt(key, _)
            | Persist(key)
            | LPush(key, _)
            | RPush(key, _)
            | LPop(key, _)
            | RPop(key, _)
            | HSet(key, _)
            | HDel(key, _)
            | HIncrBy(key, _, _) => vec![key.clone()],
            Del(keys) => keys.clone(),
            MSet(pairs) => pairs.iter().map(|(key, _)| key.clone()).collect(),
            Ping(_)
            | Echo(_)
            | Get(_)
            | Exists(_)
            | Strlen(_)
            | MGet(_)
            | Ttl(_)
            | PTtl(_)
            | LRange(_, _, _)
            | LLen(_)
            | LIndex(_, _)
            | HGet(_, _)
            | HKeys(_)
            | HVals(_)
            | HGetAll(_)
            | HExists(_, _)
            | HLen(_)
            | Subscribe(_)
            | Unsubscribe(_)
            | Publish(_, _)
            | Info(_)
            | BgRewriteAof
            | Unknown(_) => Vec::new(),
        }
    }
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
    #[error("invalid expire time in '{0}' command")]
    InvalidExpireTime(String),
    #[error("syntax error")]
    Syntax,
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
            "SET" => parse_set(rest, &name),
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
            "EXPIRE" => {
                parse_expire(rest, &name, /* ms */ false).map(|(k, d)| Command::Expire(k, d))
            }
            "PEXPIRE" => {
                parse_expire(rest, &name, /* ms */ true).map(|(k, d)| Command::PExpire(k, d))
            }
            "EXPIREAT" => {
                parse_expire_at(rest, &name, /* ms */ false).map(|(k, d)| Command::PExpireAt(k, d))
            }
            "PEXPIREAT" => {
                parse_expire_at(rest, &name, /* ms */ true).map(|(k, d)| Command::PExpireAt(k, d))
            }
            "TTL" => one_arg(rest, &name).map(Command::Ttl),
            "PTTL" => one_arg(rest, &name).map(Command::PTtl),
            "PERSIST" => one_arg(rest, &name).map(Command::Persist),
            "LPUSH" => parse_push(rest, &name).map(|(k, v)| Command::LPush(k, v)),
            "RPUSH" => parse_push(rest, &name).map(|(k, v)| Command::RPush(k, v)),
            "LPOP" => parse_pop(rest, &name).map(|(k, c)| Command::LPop(k, c)),
            "RPOP" => parse_pop(rest, &name).map(|(k, c)| Command::RPop(k, c)),
            "LRANGE" => {
                if rest.len() != 3 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let start = parse_i64(&it.next().unwrap())?;
                let stop = parse_i64(&it.next().unwrap())?;
                Ok(Command::LRange(key, start, stop))
            }
            "LLEN" => one_arg(rest, &name).map(Command::LLen),
            "LINDEX" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let idx = parse_i64(&it.next().unwrap())?;
                Ok(Command::LIndex(key, idx))
            }
            "HSET" => {
                if rest.len() < 3 || (rest.len() - 1) % 2 != 0 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let mut pairs = Vec::with_capacity(it.len() / 2);
                while let (Some(f), Some(v)) = (it.next(), it.next()) {
                    pairs.push((f, v));
                }
                Ok(Command::HSet(key, pairs))
            }
            "HGET" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                Ok(Command::HGet(it.next().unwrap(), it.next().unwrap()))
            }
            "HDEL" => {
                if rest.len() < 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let fields: Vec<Bytes> = it.collect();
                Ok(Command::HDel(key, fields))
            }
            "HKEYS" => one_arg(rest, &name).map(Command::HKeys),
            "HVALS" => one_arg(rest, &name).map(Command::HVals),
            "HGETALL" => one_arg(rest, &name).map(Command::HGetAll),
            "HEXISTS" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                Ok(Command::HExists(it.next().unwrap(), it.next().unwrap()))
            }
            "HLEN" => one_arg(rest, &name).map(Command::HLen),
            "HINCRBY" => {
                if rest.len() != 3 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                let key = it.next().unwrap();
                let field = it.next().unwrap();
                let n = parse_i64(&it.next().unwrap())?;
                Ok(Command::HIncrBy(key, field, n))
            }
            "SUBSCRIBE" => {
                if rest.is_empty() {
                    return Err(arity_err());
                }
                Ok(Command::Subscribe(rest))
            }
            "UNSUBSCRIBE" => {
                if rest.is_empty() {
                    Ok(Command::Unsubscribe(None))
                } else {
                    Ok(Command::Unsubscribe(Some(rest)))
                }
            }
            "PUBLISH" => {
                if rest.len() != 2 {
                    return Err(arity_err());
                }
                let mut it = rest.into_iter();
                Ok(Command::Publish(it.next().unwrap(), it.next().unwrap()))
            }
            "INFO" => match rest.len() {
                0 => Ok(Command::Info(None)),
                1 => {
                    let section = std::str::from_utf8(&rest[0])
                        .map_err(|_| ParseError::Syntax)?
                        .to_ascii_lowercase();
                    Ok(Command::Info(Some(section)))
                }
                _ => Err(arity_err()),
            },
            "BGREWRITEAOF" => {
                if rest.is_empty() {
                    Ok(Command::BgRewriteAof)
                } else {
                    Err(arity_err())
                }
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
            Command::Set {
                key,
                value,
                expires_at,
            } => string::set_at(db, key, value, expires_at),
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
            Command::Expire(k, d) | Command::PExpire(k, d) | Command::PExpireAt(k, d) => {
                string::expire_at(db, k, d)
            }
            Command::Ttl(k) => string::ttl(db, &k, false),
            Command::PTtl(k) => string::ttl(db, &k, true),
            Command::Persist(k) => string::persist(db, &k),
            Command::LPush(k, vs) => list::lpush(db, k, vs),
            Command::RPush(k, vs) => list::rpush(db, k, vs),
            Command::LPop(k, c) => list::lpop(db, &k, c),
            Command::RPop(k, c) => list::rpop(db, &k, c),
            Command::LRange(k, s, e) => list::lrange(db, &k, s, e),
            Command::LLen(k) => list::llen(db, &k),
            Command::LIndex(k, i) => list::lindex(db, &k, i),
            Command::HSet(k, pairs) => hash::hset(db, k, pairs),
            Command::HGet(k, f) => hash::hget(db, &k, &f),
            Command::HDel(k, fs) => hash::hdel(db, &k, &fs),
            Command::HKeys(k) => hash::hkeys(db, &k),
            Command::HVals(k) => hash::hvals(db, &k),
            Command::HGetAll(k) => hash::hgetall(db, &k),
            Command::HExists(k, f) => hash::hexists(db, &k, &f),
            Command::HLen(k) => hash::hlen(db, &k),
            Command::HIncrBy(k, f, n) => hash::hincrby(db, k, f, n),
            Command::Publish(ch, msg) => Frame::Integer(db.pubsub_publish(&ch, msg) as i64),
            Command::Info(section) => crate::server::info_frame(db, None, None, section.as_deref()),
            Command::BgRewriteAof => Frame::Error("ERR AOF is not enabled".into()),
            // SUBSCRIBE / UNSUBSCRIBE are handled by the connection task itself,
            // not via this synchronous apply() path.
            Command::Subscribe(_) | Command::Unsubscribe(_) => {
                Frame::Error("ERR SUBSCRIBE/UNSUBSCRIBE must be handled by connection".into())
            }
            Command::Unknown(name) => {
                // Sanitize CR/LF to avoid protocol injection — name is user-controlled.
                let safe: String = name
                    .chars()
                    .map(|c| if c == '\r' || c == '\n' { '?' } else { c })
                    .collect();
                Frame::Error(format!("ERR unknown command '{}'", safe))
            }
        }
    }
}

fn parse_set(rest: Vec<Bytes>, name: &str) -> Result<Command, ParseError> {
    if rest.len() < 2 {
        return Err(ParseError::Arity(name.to_string()));
    }
    let mut it = rest.into_iter();
    let key = it.next().unwrap();
    let value = it.next().unwrap();
    let mut expires_at: Option<ExpireAt> = None;
    let mut expiry_seen = false;
    while let Some(opt) = it.next() {
        let opt_upper = std::str::from_utf8(&opt)
            .map_err(|_| ParseError::Syntax)?
            .to_ascii_uppercase();
        match opt_upper.as_str() {
            "EX" => {
                if expiry_seen {
                    return Err(ParseError::Syntax);
                }
                expiry_seen = true;
                let n_bytes = it.next().ok_or(ParseError::Syntax)?;
                let n = parse_i64(&n_bytes)?;
                expires_at = Some(set_relative_deadline(n, false, name)?);
            }
            "PX" => {
                if expiry_seen {
                    return Err(ParseError::Syntax);
                }
                expiry_seen = true;
                let n_bytes = it.next().ok_or(ParseError::Syntax)?;
                let n = parse_i64(&n_bytes)?;
                expires_at = Some(set_relative_deadline(n, true, name)?);
            }
            "EXAT" => {
                if expiry_seen {
                    return Err(ParseError::Syntax);
                }
                expiry_seen = true;
                let n_bytes = it.next().ok_or(ParseError::Syntax)?;
                let n = parse_i64(&n_bytes)?;
                expires_at = Some(set_absolute_deadline(n, false, name)?);
            }
            "PXAT" => {
                if expiry_seen {
                    return Err(ParseError::Syntax);
                }
                expiry_seen = true;
                let n_bytes = it.next().ok_or(ParseError::Syntax)?;
                let n = parse_i64(&n_bytes)?;
                expires_at = Some(set_absolute_deadline(n, true, name)?);
            }
            _ => return Err(ParseError::Syntax),
        }
    }
    Ok(Command::Set {
        key,
        value,
        expires_at,
    })
}

fn parse_expire(rest: Vec<Bytes>, name: &str, ms: bool) -> Result<(Bytes, ExpireAt), ParseError> {
    if rest.len() != 2 {
        return Err(ParseError::Arity(name.to_string()));
    }
    let mut it = rest.into_iter();
    let key = it.next().unwrap();
    let n = parse_i64(&it.next().unwrap())?;
    let deadline = expire_relative_deadline(n, ms, name)?;
    Ok((key, deadline))
}

fn parse_expire_at(
    rest: Vec<Bytes>,
    name: &str,
    ms: bool,
) -> Result<(Bytes, ExpireAt), ParseError> {
    if rest.len() != 2 {
        return Err(ParseError::Arity(name.to_string()));
    }
    let mut it = rest.into_iter();
    let key = it.next().unwrap();
    let n = parse_i64(&it.next().unwrap())?;
    let deadline = expire_absolute_deadline(n, ms, name)?;
    Ok((key, deadline))
}

fn parse_push(rest: Vec<Bytes>, name: &str) -> Result<(Bytes, Vec<Bytes>), ParseError> {
    if rest.len() < 2 {
        return Err(ParseError::Arity(name.to_string()));
    }
    let mut it = rest.into_iter();
    let key = it.next().unwrap();
    let values: Vec<Bytes> = it.collect();
    Ok((key, values))
}

fn parse_pop(rest: Vec<Bytes>, name: &str) -> Result<(Bytes, Option<usize>), ParseError> {
    match rest.len() {
        1 => {
            let mut it = rest.into_iter();
            Ok((it.next().unwrap(), None))
        }
        2 => {
            let mut it = rest.into_iter();
            let key = it.next().unwrap();
            let count = parse_i64(&it.next().unwrap())?;
            if count < 0 {
                return Err(ParseError::Syntax);
            }
            Ok((key, Some(count as usize)))
        }
        _ => Err(ParseError::Arity(name.to_string())),
    }
}

fn bulk(bytes: &'static [u8]) -> Frame {
    Frame::Bulk(Bytes::from_static(bytes))
}

fn bulk_string(value: impl ToString) -> Frame {
    Frame::Bulk(Bytes::from(value.to_string()))
}

fn command_one(name: &'static [u8], key: &Bytes) -> Frame {
    Frame::Array(vec![bulk(name), Frame::Bulk(key.clone())])
}

fn command_with_keys(name: &'static [u8], keys: &[Bytes]) -> Frame {
    let mut parts = Vec::with_capacity(keys.len() + 1);
    parts.push(bulk(name));
    parts.extend(keys.iter().cloned().map(Frame::Bulk));
    Frame::Array(parts)
}

fn command_with_values(name: &'static [u8], key: &Bytes, values: &[Bytes]) -> Frame {
    let mut parts = Vec::with_capacity(values.len() + 2);
    parts.push(bulk(name));
    parts.push(Frame::Bulk(key.clone()));
    parts.extend(values.iter().cloned().map(Frame::Bulk));
    Frame::Array(parts)
}

fn pexpireat_frame(key: &Bytes, deadline: ExpireAt) -> Frame {
    Frame::Array(vec![
        bulk(b"PEXPIREAT"),
        Frame::Bulk(key.clone()),
        bulk_string(deadline),
    ])
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

fn invalid_expire_time(name: &str) -> ParseError {
    ParseError::InvalidExpireTime(name.to_ascii_lowercase())
}

fn set_relative_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    if n <= 0 {
        return Err(invalid_expire_time(name));
    }
    relative_deadline(n, ms, name)
}

fn expire_relative_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    if n <= 0 {
        return Ok(0);
    }
    relative_deadline(n, ms, name)
}

fn relative_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    let delta = duration_millis(n, ms, name)?;
    now_millis()
        .checked_add(delta)
        .filter(|deadline| *deadline <= MAX_EXPIRE_AT_MS)
        .ok_or_else(|| invalid_expire_time(name))
}

fn set_absolute_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    if n <= 0 {
        return Err(invalid_expire_time(name));
    }
    absolute_deadline(n, ms, name)
}

fn expire_absolute_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    if n <= 0 {
        return Ok(0);
    }
    absolute_deadline(n, ms, name)
}

fn absolute_deadline(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    duration_millis(n, ms, name).and_then(|deadline| {
        if deadline <= MAX_EXPIRE_AT_MS {
            Ok(deadline)
        } else {
            Err(invalid_expire_time(name))
        }
    })
}

fn duration_millis(n: i64, ms: bool, name: &str) -> Result<ExpireAt, ParseError> {
    if ms {
        Ok(n as ExpireAt)
    } else {
        (n as ExpireAt)
            .checked_mul(MILLIS_PER_SECOND)
            .ok_or_else(|| invalid_expire_time(name))
    }
}

fn expect_bulk(frame: Frame, idx: usize) -> Result<Bytes, ParseError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        _ => Err(ParseError::NotBulk(idx)),
    }
}
