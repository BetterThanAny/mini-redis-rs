use crate::db::{entry_expired, Db, Entry, Value};
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::VecDeque;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

enum PushSide {
    Left,
    Right,
}

fn push(db: &Db, key: Bytes, values: Vec<Bytes>, side: PushSide) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    // If existing entry is expired, treat as missing
    let stale = shard.entries.get(&key).map(entry_expired).unwrap_or(false);
    if stale {
        shard.entries.remove(&key);
    }
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::List(VecDeque::new()),
        expires_at: None,
    });
    let list = match &mut entry.value {
        Value::List(l) => l,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    for v in values {
        match side {
            PushSide::Left => list.push_front(v),
            PushSide::Right => list.push_back(v),
        }
    }
    Frame::Integer(list.len() as i64)
}

pub fn lpush(db: &Db, key: Bytes, values: Vec<Bytes>) -> Frame {
    push(db, key, values, PushSide::Left)
}

pub fn rpush(db: &Db, key: Bytes, values: Vec<Bytes>) -> Frame {
    push(db, key, values, PushSide::Right)
}

enum PopSide {
    Left,
    Right,
}

fn pop(db: &Db, key: &Bytes, count: Option<usize>, side: PopSide) -> Frame {
    let mut shard = db.shard_for(key).lock().unwrap();
    let stale = shard.entries.get(key).map(entry_expired).unwrap_or(false);
    if stale {
        shard.entries.remove(key);
    }
    let entry = match shard.entries.get_mut(key) {
        Some(e) => e,
        None => {
            return match count {
                None => Frame::Null,
                Some(_) => Frame::Null,
            }
        }
    };
    let list = match &mut entry.value {
        Value::List(l) => l,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let n = count.unwrap_or(1).min(list.len());
    if n == 0 {
        // LPOP key (no count) on empty list -> null bulk
        // LPOP key 0 (or count > 0 on empty list) -> empty array
        return match count {
            None => Frame::Null,
            Some(_) => Frame::Array(vec![]),
        };
    }
    let mut popped = Vec::with_capacity(n);
    for _ in 0..n {
        let v = match side {
            PopSide::Left => list.pop_front(),
            PopSide::Right => list.pop_back(),
        };
        if let Some(v) = v {
            popped.push(v);
        }
    }
    let list_now_empty = list.is_empty();
    if list_now_empty {
        shard.entries.remove(key);
    }
    match count {
        None => Frame::Bulk(popped.into_iter().next().unwrap()),
        Some(_) => Frame::Array(popped.into_iter().map(Frame::Bulk).collect()),
    }
}

pub fn lpop(db: &Db, key: &Bytes, count: Option<usize>) -> Frame {
    pop(db, key, count, PopSide::Left)
}

pub fn rpop(db: &Db, key: &Bytes, count: Option<usize>) -> Frame {
    pop(db, key, count, PopSide::Right)
}

fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    if idx >= 0 {
        let u = idx as usize;
        if u < len {
            Some(u)
        } else {
            None
        }
    } else {
        let from_end = idx.unsigned_abs() as usize;
        if from_end == 0 || from_end > len {
            None
        } else {
            Some(len - from_end)
        }
    }
}

pub fn lrange(db: &Db, key: &Bytes, start: i64, stop: i64) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Array(vec![]),
    };
    let list = match &entry.value {
        Value::List(l) => l,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    let len = list.len() as i64;
    if len == 0 {
        return Frame::Array(vec![]);
    }
    // Resolve negatives (no clamping yet so we can detect "outside the list").
    let resolve = |i: i64| if i < 0 { len + i } else { i };
    let start_r = resolve(start);
    let stop_r = resolve(stop);
    // start past end, or stop entirely before start of list -> empty
    if start_r >= len || stop_r < 0 {
        return Frame::Array(vec![]);
    }
    let start_idx = start_r.max(0) as usize;
    let stop_idx = stop_r.min(len - 1) as usize;
    if start_idx > stop_idx {
        return Frame::Array(vec![]);
    }
    let frames: Vec<Frame> = list
        .iter()
        .skip(start_idx)
        .take(stop_idx - start_idx + 1)
        .map(|b| Frame::Bulk(b.clone()))
        .collect();
    Frame::Array(frames)
}

pub fn llen(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => match &e.value {
            Value::List(l) => Frame::Integer(l.len() as i64),
            _ => Frame::Error(WRONGTYPE.into()),
        },
        _ => Frame::Integer(0),
    }
}

pub fn lindex(db: &Db, key: &Bytes, idx: i64) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    let entry = match shard.entries.get(key) {
        Some(e) if !entry_expired(e) => e,
        _ => return Frame::Null,
    };
    let list = match &entry.value {
        Value::List(l) => l,
        _ => return Frame::Error(WRONGTYPE.into()),
    };
    match resolve_index(idx, list.len()) {
        Some(u) => Frame::Bulk(list[u].clone()),
        None => Frame::Null,
    }
}
