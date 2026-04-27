pub mod expire;

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use xxhash_rust::xxh64::xxh64;

const SHARDS: usize = 16;

#[derive(Debug)]
pub enum Value {
    String(Bytes),
}

#[derive(Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

#[derive(Default, Debug)]
pub struct Shard {
    pub entries: HashMap<Bytes, Entry>,
    pub expirations: BTreeMap<Instant, Vec<Bytes>>,
}

#[derive(Clone)]
pub struct Db {
    shards: Arc<Vec<Mutex<Shard>>>,
}

impl Db {
    pub fn new() -> Self {
        let mut v = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            v.push(Mutex::new(Shard::default()));
        }
        Self { shards: Arc::new(v) }
    }

    pub fn shard_for(&self, key: &[u8]) -> &Mutex<Shard> {
        let idx = (xxh64(key, 0) as usize) % SHARDS;
        &self.shards[idx]
    }

    pub fn iter_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
        self.shards.iter()
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

pub fn entry_expired(entry: &Entry) -> bool {
    matches!(entry.expires_at, Some(t) if Instant::now() >= t)
}
