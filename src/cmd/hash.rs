use crate::db::{entry_expired, Db, Entry, Value};
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::HashMap;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR hash value is not an integer";
const OVERFLOW: &str = "ERR increment or decrement would overflow";

pub fn hset(db: &Db, key: Bytes, pairs: Vec<(Bytes, Bytes)>) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let stale = shard.entries.get(&key).map(entry_expired).unwrap_or(false);
    if stale {
        shard.remove_entry(&key);
    }
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::Hash(HashMap::new()),
        expires_at: None,
    });
    let map = match &mut entry.value {
        Value::Hash(m) => m,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let mut added = 0i64;
    for (f, v) in pairs {
        if map.insert(f, v).is_none() {
            added += 1;
        }
    }
    Frame::Integer(added)
}

pub fn hget(db: &Db, key: &Bytes, field: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Null,
    };
    let map = match &entry.value {
        Value::Hash(m) => m,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    match map.get(field) {
        Some(v) => Frame::Bulk(v.clone()),
        None => Frame::Null,
    }
}

pub fn hdel(db: &Db, key: &Bytes, fields: &[Bytes]) -> Frame {
    let mut shard = db.shard_for(key).lock().unwrap();
    let stale = shard.entries.get(key).map(entry_expired).unwrap_or(false);
    if stale {
        shard.remove_entry(key);
    }
    let entry = match shard.entries.get_mut(key) {
        Some(e) => e,
        None => return Frame::Integer(0),
    };
    let map = match &mut entry.value {
        Value::Hash(m) => m,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let mut removed = 0i64;
    for f in fields {
        if map.remove(f).is_some() {
            removed += 1;
        }
    }
    let now_empty = map.is_empty();
    if now_empty {
        shard.remove_entry(key);
    }
    Frame::Integer(removed)
}

pub fn hkeys(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Array(vec![]),
    };
    match &entry.value {
        Value::Hash(m) => Frame::Array(m.keys().cloned().map(Frame::Bulk).collect()),
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn hvals(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Array(vec![]),
    };
    match &entry.value {
        Value::Hash(m) => Frame::Array(m.values().cloned().map(Frame::Bulk).collect()),
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn hgetall(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Array(vec![]),
    };
    match &entry.value {
        Value::Hash(m) => {
            let mut out = Vec::with_capacity(m.len() * 2);
            for (k, v) in m {
                out.push(Frame::Bulk(k.clone()));
                out.push(Frame::Bulk(v.clone()));
            }
            Frame::Array(out)
        }
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn hexists(db: &Db, key: &Bytes, field: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Integer(0),
    };
    match &entry.value {
        Value::Hash(m) => Frame::Integer(i64::from(m.contains_key(field))),
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn hlen(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Integer(0),
    };
    match &entry.value {
        Value::Hash(m) => Frame::Integer(m.len() as i64),
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn hincrby(db: &Db, key: Bytes, field: Bytes, delta: i64) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let stale = shard.entries.get(&key).map(entry_expired).unwrap_or(false);
    if stale {
        shard.remove_entry(&key);
    }
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::Hash(HashMap::new()),
        expires_at: None,
    });
    let map = match &mut entry.value {
        Value::Hash(m) => m,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let current_v = map.entry(field).or_insert_with(|| Bytes::from_static(b"0"));
    let current: i64 = match std::str::from_utf8(current_v)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return Frame::Error(NOT_INT.into()),
    };
    let new = match current.checked_add(delta) {
        Some(n) => n,
        None => return Frame::Error(OVERFLOW.into()),
    };
    *current_v = Bytes::from(new.to_string());
    Frame::Integer(new)
}
