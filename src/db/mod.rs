pub mod expire;

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::broadcast;
use xxhash_rust::xxh64::xxh64;

const SHARDS: usize = 16;
const PUBSUB_CHAN_CAP: usize = 1024;

#[derive(Debug)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
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
    pubsub: Arc<Mutex<HashMap<Bytes, broadcast::Sender<Bytes>>>>,
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
        }
    }

    pub fn shard_for(&self, key: &[u8]) -> &Mutex<Shard> {
        let idx = (xxh64(key, 0) as usize) % SHARDS;
        &self.shards[idx]
    }

    pub fn iter_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
        self.shards.iter()
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
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

pub fn entry_expired(entry: &Entry) -> bool {
    matches!(entry.expires_at, Some(t) if Instant::now() >= t)
}
