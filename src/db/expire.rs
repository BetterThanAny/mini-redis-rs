use crate::db::Db;
use bytes::Bytes;
use std::time::{Duration, Instant};

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

pub async fn run_sweeper(db: Db) {
    let mut tick = tokio::time::interval(SWEEP_INTERVAL);
    loop {
        tick.tick().await;
        sweep_once(&db);
    }
}

pub fn sweep_once(db: &Db) {
    let now = Instant::now();
    for shard_mu in db.iter_shards() {
        let mut shard = shard_mu.lock().unwrap();
        let due_keys: Vec<Bytes> = shard
            .expirations
            .range(..=now)
            .flat_map(|(_, ks)| ks.clone())
            .collect();
        shard.expirations.retain(|t, _| *t > now);
        for k in due_keys {
            // Double-check the entry still has a deadline at or before now
            // (key may have been re-set or PERSISTed since being scheduled).
            let still_due = shard
                .entries
                .get(&k)
                .and_then(|e| e.expires_at)
                .map(|t| t <= now)
                .unwrap_or(false);
            if still_due {
                shard.entries.remove(&k);
            }
        }
    }
}
