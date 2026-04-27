# mini-redis-rs

A from-scratch Redis-compatible server in async Rust. Speaks RESP2, supports the most common string / list / hash / pub-sub / TTL commands, persists via AOF, and benchmarks within ~20% of official Redis on a single MacBook.

Built as a learning exercise to exercise the full Redis stack: TCP handling, streaming protocol parsing, command dispatch, sharded concurrent state, expiration, pub/sub fan-out, and crash recovery.

## Quick start

```bash
cargo run --release -- --port 6380
```

In another terminal:

```bash
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello             # "world"
redis-cli -p 6380 INCR counter
redis-cli -p 6380 RPUSH log a b c
redis-cli -p 6380 LRANGE log 0 -1
redis-cli -p 6380 HSET user:1 name alice age 30
redis-cli -p 6380 SUBSCRIBE chat        # in one terminal
redis-cli -p 6380 PUBLISH chat hello    # in another
```

## Persistence

```bash
cargo run --release -- --port 6380 --aof /tmp/mr.aof --aof-fsync everysec
```

`--aof-fsync` accepts `always` | `everysec` (default) | `no`.

Replay happens automatically on startup if the file exists. Verified against `kill -9` in the test suite.

## Supported commands

- **Strings:** `GET`, `SET` (with `EX` / `PX`), `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`
- **TTL:** `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`
- **Hashes:** `HSET`, `HGET`, `HDEL`, `HKEYS`, `HVALS`, `HGETALL`, `HEXISTS`, `HLEN`, `HINCRBY`
- **Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- **Server:** `PING`, `ECHO`

## Architecture

- One `tokio::spawn` per accepted connection
- Sharded shared state: `Arc<Vec<Mutex<Shard>>>` (16 shards, key-hashed via `xxhash-rust`)
- Streaming RESP2 parser operating on `BytesMut` — returns `Incomplete` until a full frame arrives
- Active TTL sweeper task (100ms tick) using a per-shard `BTreeMap<Instant, Vec<Bytes>>`
- Pub/Sub via `tokio::sync::broadcast` channels in a separate registry
- AOF: write-side `mpsc::UnboundedSender<Bytes>` -> single-writer task with configurable fsync policy

## Tests

```bash
cargo test
```

82 integration tests cover all of the above.

## Benchmarks

See [BENCHMARK.md](./BENCHMARK.md) for the side-by-side run against official Redis 8.6.2.

## Out of scope

- Sorted sets (`ZADD` / `ZRANGE` etc.)
- Cluster mode / hash slots
- Replication (`REPLICAOF` / `PSYNC`)
- RDB snapshot format
- Transactions (`MULTI` / `EXEC`)
- Scripting (`EVAL` / Lua)
- ACL
- RESP3
