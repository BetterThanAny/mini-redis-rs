use crate::db::{entry_expired, Db, Entry, Value};
use crate::resp::Frame;
use bytes::Bytes;
use std::time::Instant;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const OVERFLOW: &str = "ERR increment or decrement would overflow";

pub fn get(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(entry) if !entry_expired(entry) => match &entry.value {
            Value::String(b) => Frame::Bulk(b.clone()),
            #[allow(unreachable_patterns)]
            _ => Frame::Error(WRONGTYPE.into()),
        },
        _ => Frame::Null,
    }
}

pub fn set(db: &Db, key: Bytes, value: Bytes, expires_at: Option<Instant>) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    shard.entries.insert(
        key,
        Entry {
            value: Value::String(value),
            expires_at,
        },
    );
    Frame::Simple("OK".into())
}

pub fn del(db: &Db, keys: &[Bytes]) -> Frame {
    let mut removed = 0i64;
    for key in keys {
        let mut shard = db.shard_for(key).lock().unwrap();
        if shard.entries.remove(key).is_some() {
            removed += 1;
        }
    }
    Frame::Integer(removed)
}

pub fn exists(db: &Db, keys: &[Bytes]) -> Frame {
    let mut count = 0i64;
    for key in keys {
        let shard = db.shard_for(key).lock().unwrap();
        if shard
            .entries
            .get(key)
            .map(|e| !entry_expired(e))
            .unwrap_or(false)
        {
            count += 1;
        }
    }
    Frame::Integer(count)
}

pub fn incr(db: &Db, key: Bytes, delta: i64) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::String(Bytes::from_static(b"0")),
        expires_at: None,
    });
    if entry_expired(entry) {
        entry.value = Value::String(Bytes::from_static(b"0"));
        entry.expires_at = None;
    }
    let current = match &entry.value {
        Value::String(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => return Frame::Error(NOT_INT.into()),
        },
        #[allow(unreachable_patterns)]
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let new = match current.checked_add(delta) {
        Some(n) => n,
        None => return Frame::Error(OVERFLOW.into()),
    };
    entry.value = Value::String(Bytes::from(new.to_string()));
    Frame::Integer(new)
}

pub fn append(db: &Db, key: Bytes, suffix: Bytes) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::String(Bytes::new()),
        expires_at: None,
    });
    match &mut entry.value {
        Value::String(b) => {
            let mut combined = bytes::BytesMut::with_capacity(b.len() + suffix.len());
            combined.extend_from_slice(b);
            combined.extend_from_slice(&suffix);
            *b = combined.freeze();
            Frame::Integer(b.len() as i64)
        }
        #[allow(unreachable_patterns)]
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn strlen(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => match &e.value {
            Value::String(b) => Frame::Integer(b.len() as i64),
            #[allow(unreachable_patterns)]
            _ => Frame::Error(WRONGTYPE.into()),
        },
        _ => Frame::Integer(0),
    }
}

pub fn mget(db: &Db, keys: &[Bytes]) -> Frame {
    let frames = keys
        .iter()
        .map(|k| {
            let shard = db.shard_for(k).lock().unwrap();
            match shard.entries.get(k) {
                Some(e) if !entry_expired(e) => match &e.value {
                    Value::String(b) => Frame::Bulk(b.clone()),
                    #[allow(unreachable_patterns)]
                    _ => Frame::Null,
                },
                _ => Frame::Null,
            }
        })
        .collect();
    Frame::Array(frames)
}

pub fn mset(db: &Db, pairs: Vec<(Bytes, Bytes)>) -> Frame {
    for (k, v) in pairs {
        let mut shard = db.shard_for(&k).lock().unwrap();
        shard.entries.insert(
            k,
            Entry {
                value: Value::String(v),
                expires_at: None,
            },
        );
    }
    Frame::Simple("OK".into())
}
