# Benchmark — mini-redis-rs vs Redis 8.6.2

**Date:** 2026-04-27
**Hardware:** MacBook (Apple Silicon), darwin 24.6.0
**Tool:** `redis-benchmark` from Homebrew Redis 8.6.2
**Workload:** 100,000 ops per command, concurrency 50, default value sizes

```bash
redis-benchmark -p <port> -t set,get,incr,lpush,rpush,lpop,rpop,hset \
                -n 100000 -c 50 -q
```

## Throughput (requests / second)

| Command | mini-redis-rs | Redis 8.6.2 | Ratio (theirs / ours) |
|---|---:|---:|---:|
| SET   | 184,843 | 189,036 | **1.02×** |
| GET   | 203,666 | 222,222 | 1.09× |
| INCR  | 204,918 | 233,645 | 1.14× |
| LPUSH | 180,505 | 221,239 | 1.23× |
| RPUSH | 178,891 | 225,225 | 1.26× |
| LPOP  | 189,394 | 217,391 | 1.15× |
| RPOP  | 189,753 | 226,757 | 1.20× |
| HSET  | 191,205 | 224,215 | 1.17× |

mini-redis-rs reaches **~80–98% of official Redis throughput** despite being a from-scratch educational implementation.

## p50 latency (milliseconds)

| Command | mini-redis-rs | Redis 8.6.2 |
|---|---:|---:|
| SET   | 0.143 | 0.199 |
| GET   | 0.127 | 0.175 |
| INCR  | 0.127 | 0.167 |
| LPUSH | 0.143 | 0.183 |
| RPUSH | 0.143 | 0.175 |
| LPOP  | 0.135 | 0.183 |
| RPOP  | 0.135 | 0.175 |
| HSET  | 0.135 | 0.183 |

Our p50 is consistently **lower** than official Redis here — likely because we run on a multi-threaded Tokio runtime and saturate multiple CPU cores at lower per-request queueing depth, while official Redis is single-threaded and serializes all work.

## Where the throughput gap is

The throughput gap (Redis ahead by ~10–25% on most commands) most likely comes from:

1. **Sharded `Mutex` lock acquisition cost.** Every command takes a `parking_lot`-equivalent mutex; Redis avoids this entirely with a single thread + IO multiplexing.
2. **`Bytes::clone()` on returns.** We clone the response payload for SET/GET responses; Redis writes directly from its arena.
3. **No specialized protocol fast-path.** Redis hand-rolls RESP encoding for common reply types in C; we go through a generic `Frame::Bulk` -> `BytesMut` round-trip.
4. **AOF disabled in this run** for both servers — so disk doesn't enter the picture.

Both sides are CPU-bound on the network/protocol path, so the headroom is in the protocol codec, not in the data structures.

## Reproducing

```bash
# build release
cargo build --release

# ours
./target/release/miniredisd --port 6380 &
redis-benchmark -p 6380 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -c 50 -q
kill %1

# theirs
redis-server --port 6381 --daemonize yes --save ""
redis-benchmark -p 6381 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -c 50 -q
redis-cli -p 6381 SHUTDOWN NOSAVE
```

Raw outputs are in `bench-results/ours.txt` and `bench-results/theirs.txt`.
