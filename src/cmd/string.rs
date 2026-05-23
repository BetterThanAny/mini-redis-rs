use crate::db::{entry_expired, now_millis, Db, Entry, ExpireAt, Value};
use crate::resp::Frame;
use bytes::Bytes;
use std::time::Duration;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const OVERFLOW: &str = "ERR increment or decrement would overflow";

pub fn get(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(entry) if !entry_expired(entry) => match &entry.value {
            Value::String(b) => Frame::Bulk(b.clone()),
            _ => Frame::Error(WRONGTYPE.into()),
        },
        _ => Frame::Null,
    }
}

pub fn set(db: &Db, key: Bytes, value: Bytes, ex: Option<Duration>) -> Frame {
    let expires_at = ex.map(|d| now_millis() + d.as_millis());
    set_at(db, key, value, expires_at)
}

pub fn set_at(db: &Db, key: Bytes, value: Bytes, expires_at: Option<ExpireAt>) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    // If overwriting an entry that had a TTL, drop the stale BTreeMap row
    // so the index doesn't grow unboundedly under SET EX / EXPIRE churn.
    if let Some(old) = shard.entries.get(&key).and_then(|e| e.expires_at) {
        shard.unindex_expiration(old, &key);
    }
    shard.entries.insert(
        key.clone(),
        Entry {
            value: Value::String(value),
            expires_at,
        },
    );
    if let Some(t) = expires_at {
        shard.expirations.entry(t).or_default().push(key);
    }
    Frame::Simple("OK".into())
}

pub fn del(db: &Db, keys: &[Bytes]) -> Frame {
    let mut removed = 0i64;
    for key in keys {
        let mut shard = db.shard_for(key).lock().unwrap();
        if shard.expire_if_stale(key) {
            continue;
        }
        if shard.remove_entry(key).is_some() {
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
        Value::String(b) => match std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return Frame::Error(NOT_INT.into()),
        },
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
    shard.expire_if_stale(&key);
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
        _ => Frame::Error(WRONGTYPE.into()),
    }
}

pub fn strlen(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => match &e.value {
            Value::String(b) => Frame::Integer(b.len() as i64),
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
        if let Some(old) = shard.entries.get(&k).and_then(|e| e.expires_at) {
            shard.unindex_expiration(old, &k);
        }
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

// ---- expiration commands ----

pub fn expire(db: &Db, key: Bytes, ttl: Duration) -> Frame {
    let deadline = now_millis() + ttl.as_millis();
    expire_at(db, key, deadline)
}

pub fn expire_at(db: &Db, key: Bytes, deadline: ExpireAt) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    if shard.expire_if_stale(&key) {
        return Frame::Integer(0);
    }
    if !shard.entries.contains_key(&key) {
        return Frame::Integer(0);
    }
    if deadline <= now_millis() {
        shard.remove_entry(&key);
        return Frame::Integer(1);
    }
    if let Some(old) = shard.entries.get(&key).and_then(|e| e.expires_at) {
        shard.unindex_expiration(old, &key);
    }
    if let Some(entry) = shard.entries.get_mut(&key) {
        entry.expires_at = Some(deadline);
    }
    shard.expirations.entry(deadline).or_default().push(key);
    Frame::Integer(1)
}

pub fn ttl(db: &Db, key: &Bytes, in_ms: bool) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => match entry.expires_at {
            None => Frame::Integer(-1),
            Some(t) => {
                let now = now_millis();
                if t <= now {
                    Frame::Integer(-2)
                } else {
                    if in_ms {
                        Frame::Integer((t - now) as i64)
                    } else {
                        // Round up partial seconds so e.g. 1.2s remaining returns 2,
                        // matching Redis semantics for fresh `EXPIRE k 30`.
                        let remaining_ms = t - now;
                        let secs =
                            (remaining_ms / 1000) as i64 + i64::from(remaining_ms % 1000 > 0);
                        Frame::Integer(secs)
                    }
                }
            }
        },
    }
}

pub fn persist(db: &Db, key: &Bytes) -> Frame {
    let mut shard = db.shard_for(key).lock().unwrap();
    if shard.expire_if_stale(key) {
        return Frame::Integer(0);
    }
    let old = match shard.entries.get_mut(key) {
        Some(entry) if entry.expires_at.is_some() => entry.expires_at.take().unwrap(),
        _ => return Frame::Integer(0),
    };
    shard.unindex_expiration(old, key);
    Frame::Integer(1)
}
