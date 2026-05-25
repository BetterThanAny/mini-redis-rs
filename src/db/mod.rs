pub mod expire;

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use xxhash_rust::xxh64::xxh64;

const SHARDS: usize = 16;
const PUBSUB_CHAN_CAP: usize = 1024;
const MAX_EXPIRE_AT_MS: ExpireAt = i64::MAX as ExpireAt;

pub type ExpireAt = u128;

pub fn now_millis() -> ExpireAt {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<ExpireAt>,
}

#[derive(Default, Debug)]
pub struct Shard {
    pub entries: HashMap<Bytes, Entry>,
    pub expirations: BTreeMap<ExpireAt, Vec<Bytes>>,
}

impl Shard {
    /// Remove `key` from the expiration index at deadline `t`. No-op if missing.
    /// Call this *before* writing a new deadline (or in PERSIST) so the BTreeMap
    /// stays bounded even under repeated SET EX / EXPIRE / PERSIST churn.
    pub fn unindex_expiration(&mut self, t: ExpireAt, key: &Bytes) {
        if let Some(keys) = self.expirations.get_mut(&t) {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.expirations.remove(&t);
            }
        }
    }

    pub fn remove_entry(&mut self, key: &Bytes) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        if let Some(t) = entry.expires_at {
            self.unindex_expiration(t, key);
        }
        Some(entry)
    }

    pub fn expire_if_stale(&mut self, key: &Bytes) -> bool {
        let Some(deadline) = self.entries.get(key).and_then(|entry| entry.expires_at) else {
            return false;
        };
        if now_millis() < deadline {
            return false;
        }
        self.remove_entry(key);
        true
    }
}

#[derive(Clone)]
pub struct Db {
    shards: Arc<Vec<Mutex<Shard>>>,
    pubsub: Arc<Mutex<HashMap<Bytes, broadcast::Sender<Bytes>>>>,
    write_gate: Arc<RwLock<()>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DbStats {
    pub keys: usize,
    pub expiring_keys: usize,
    pub strings: usize,
    pub lists: usize,
    pub hashes: usize,
    pub list_items: usize,
    pub hash_fields: usize,
    pub approx_bytes: usize,
}

impl Db {
    pub fn new() -> Self {
        let mut v = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            v.push(Mutex::new(Shard::default()));
        }
        Self {
            shards: Arc::new(v),
            pubsub: Arc::new(Mutex::new(HashMap::new())),
            write_gate: Arc::new(RwLock::new(())),
        }
    }

    pub fn shard_for(&self, key: &[u8]) -> &Mutex<Shard> {
        let idx = (xxh64(key, 0) as usize) % SHARDS;
        &self.shards[idx]
    }

    pub fn iter_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
        self.shards.iter()
    }

    pub async fn write_guard(&self) -> OwnedRwLockReadGuard<()> {
        self.write_gate.clone().read_owned().await
    }

    pub async fn pause_writes(&self) -> OwnedRwLockWriteGuard<()> {
        self.write_gate.clone().write_owned().await
    }

    pub fn stats(&self) -> DbStats {
        let now = now_millis();
        let mut stats = DbStats::default();
        for shard_mu in self.iter_shards() {
            let shard = shard_mu.lock().unwrap();
            for (key, entry) in &shard.entries {
                if entry.expires_at.is_some_and(|deadline| deadline <= now) {
                    continue;
                }
                stats.keys += 1;
                stats.approx_bytes += key.len();
                if entry.expires_at.is_some() {
                    stats.expiring_keys += 1;
                }
                match &entry.value {
                    Value::String(value) => {
                        stats.strings += 1;
                        stats.approx_bytes += value.len();
                    }
                    Value::List(values) => {
                        stats.lists += 1;
                        stats.list_items += values.len();
                        stats.approx_bytes += values.iter().map(Bytes::len).sum::<usize>();
                    }
                    Value::Hash(values) => {
                        stats.hashes += 1;
                        stats.hash_fields += values.len();
                        stats.approx_bytes += values
                            .iter()
                            .map(|(field, value)| field.len() + value.len())
                            .sum::<usize>();
                    }
                }
            }
        }
        stats
    }

    pub fn snapshot_entries(&self, keys: &[Bytes]) -> Vec<(Bytes, Option<Entry>)> {
        let mut keys: Vec<Bytes> = keys.to_vec();
        keys.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
        keys.dedup();

        keys.into_iter()
            .map(|key| {
                let shard = self.shard_for(&key).lock().unwrap();
                let entry = shard.entries.get(&key).cloned();
                (key, entry)
            })
            .collect()
    }

    pub fn restore_entries(&self, snapshots: Vec<(Bytes, Option<Entry>)>) {
        for (key, snapshot) in snapshots {
            let mut shard = self.shard_for(&key).lock().unwrap();
            shard.remove_entry(&key);
            if let Some(entry) = snapshot {
                if let Some(deadline) = entry.expires_at {
                    shard
                        .expirations
                        .entry(deadline)
                        .or_default()
                        .push(key.clone());
                }
                shard.entries.insert(key, entry);
            }
        }
    }

    pub fn aof_snapshot_frames(&self) -> Vec<crate::resp::Frame> {
        let now = now_millis();
        let mut entries: Vec<(Bytes, Entry)> = Vec::new();
        for shard_mu in self.iter_shards() {
            let shard = shard_mu.lock().unwrap();
            entries.extend(
                shard
                    .entries
                    .iter()
                    .filter(|(_, entry)| !entry.expires_at.is_some_and(|deadline| deadline <= now))
                    .map(|(key, entry)| (key.clone(), entry.clone())),
            );
        }
        entries.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));

        let mut frames = Vec::with_capacity(entries.len() * 2);
        for (key, entry) in entries {
            match entry.value {
                Value::String(value) => {
                    let mut parts = vec![
                        bulk_static(b"SET"),
                        crate::resp::Frame::Bulk(key.clone()),
                        crate::resp::Frame::Bulk(value),
                    ];
                    if let Some(deadline) = entry.expires_at {
                        push_set_expiry(&mut parts, deadline);
                    }
                    frames.push(crate::resp::Frame::Array(parts));
                }
                Value::List(values) if !values.is_empty() => {
                    let mut parts = Vec::with_capacity(values.len() + 2);
                    parts.push(bulk_static(b"RPUSH"));
                    parts.push(crate::resp::Frame::Bulk(key.clone()));
                    parts.extend(values.into_iter().map(crate::resp::Frame::Bulk));
                    frames.push(crate::resp::Frame::Array(parts));
                    if let Some(deadline) = entry.expires_at {
                        frames.push(pexpireat_frame(key, deadline));
                    }
                }
                Value::Hash(values) if !values.is_empty() => {
                    let mut fields: Vec<_> = values.into_iter().collect();
                    fields.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
                    let mut parts = Vec::with_capacity(fields.len() * 2 + 2);
                    parts.push(bulk_static(b"HSET"));
                    parts.push(crate::resp::Frame::Bulk(key.clone()));
                    for (field, value) in fields {
                        parts.push(crate::resp::Frame::Bulk(field));
                        parts.push(crate::resp::Frame::Bulk(value));
                    }
                    frames.push(crate::resp::Frame::Array(parts));
                    if let Some(deadline) = entry.expires_at {
                        frames.push(pexpireat_frame(key, deadline));
                    }
                }
                Value::List(_) | Value::Hash(_) => {}
            }
        }
        frames
    }

    /// Subscribe to a channel; returns a fresh receiver. Creates the channel if missing.
    pub fn pubsub_subscribe(&self, channel: Bytes) -> broadcast::Receiver<Bytes> {
        let mut ps = self.pubsub.lock().unwrap();
        let tx = ps
            .entry(channel)
            .or_insert_with(|| broadcast::channel(PUBSUB_CHAN_CAP).0);
        tx.subscribe()
    }

    /// Send a message to all current subscribers of `channel`. Returns number of receivers.
    pub fn pubsub_publish(&self, channel: &Bytes, msg: Bytes) -> usize {
        let mut ps = self.pubsub.lock().unwrap();
        match ps.get(channel) {
            Some(tx) => {
                let r = tx.send(msg).unwrap_or(0);
                if tx.receiver_count() == 0 {
                    ps.remove(channel);
                }
                r
            }
            None => 0,
        }
    }

    /// Drop a channel's broadcast::Sender if no receivers remain.
    /// Called on connection close to prevent orphan channels accumulating.
    pub fn pubsub_gc(&self, channel: &Bytes) {
        let mut ps = self.pubsub.lock().unwrap();
        if let Some(tx) = ps.get(channel) {
            if tx.receiver_count() == 0 {
                ps.remove(channel);
            }
        }
    }

    #[cfg(test)]
    pub fn pubsub_channel_count(&self) -> usize {
        self.pubsub.lock().unwrap().len()
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

pub fn entry_expired(entry: &Entry) -> bool {
    matches!(entry.expires_at, Some(t) if now_millis() >= t)
}

fn bulk_static(bytes: &'static [u8]) -> crate::resp::Frame {
    crate::resp::Frame::Bulk(Bytes::from_static(bytes))
}

fn bulk_string(value: impl ToString) -> crate::resp::Frame {
    crate::resp::Frame::Bulk(Bytes::from(value.to_string()))
}

fn push_set_expiry(parts: &mut Vec<crate::resp::Frame>, deadline: ExpireAt) {
    if deadline <= MAX_EXPIRE_AT_MS {
        parts.push(bulk_static(b"PXAT"));
        parts.push(bulk_string(deadline));
    } else {
        parts.push(bulk_static(b"PX"));
        parts.push(bulk_string(relative_millis_for_aof(deadline)));
    }
}

fn relative_millis_for_aof(deadline: ExpireAt) -> ExpireAt {
    deadline
        .saturating_sub(now_millis())
        .clamp(1, MAX_EXPIRE_AT_MS)
}

fn pexpireat_frame(key: Bytes, deadline: ExpireAt) -> crate::resp::Frame {
    crate::resp::Frame::Array(vec![
        bulk_static(b"PEXPIREAT"),
        crate::resp::Frame::Bulk(key),
        bulk_string(deadline),
    ])
}
