# mini-redis-rs

A from-scratch Redis-compatible cache server in async Rust. It speaks RESP2,
supports common string / list / hash / pub-sub / TTL commands, persists via AOF,
can compact its AOF with `BGREWRITEAOF`, and includes integration tests that
exercise wire-level responses.

This is an educational implementation of the core Redis stack: TCP handling,
streaming protocol parsing, command dispatch, sharded concurrent state,
expiration, pub/sub fan-out, append-only persistence, crash recovery policy, and
benchmarking against official Redis.

## Quick Start

```bash
cargo run --release -- --port 6380
```

In another terminal:

```bash
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
redis-cli -p 6380 INCR counter
redis-cli -p 6380 RPUSH log a b c
redis-cli -p 6380 LRANGE log 0 -1
redis-cli -p 6380 HSET user:1 name alice age 30
redis-cli -p 6380 HGETALL user:1
redis-cli -p 6380 INFO
```

Pub/Sub uses the usual two-terminal flow:

```bash
redis-cli -p 6380 SUBSCRIBE chat
redis-cli -p 6380 PUBLISH chat hello
```

## Persistence

```bash
cargo run --release -- --port 6380 --aof /tmp/mini-redis.aof --aof-fsync everysec
```

`--aof-fsync` accepts:

- `always`: sync after every accepted AOF write.
- `everysec`: sync roughly once per second. This is the default.
- `no`: write to the OS page cache and let the OS decide when to flush.

Writes are appended to AOF before the command response is sent. On `kill -9`,
recovery follows the configured fsync policy: `always` should keep acknowledged
writes, `everysec` may lose up to about one second of acknowledged writes, and
`no` may lose any data not flushed by the OS.

Startup replays the AOF automatically. Tail corruption from a partial frame is
tolerated: replay stops at the first incomplete or invalid frame and keeps the
valid prefix, truncating the bad tail before accepting new appends.

### AOF Rewrite

Trigger compaction with:

```bash
redis-cli -p 6380 BGREWRITEAOF
redis-cli -p 6380 INFO persistence
```

Rewrite behavior:

- The server keeps accepting reads and writes while rewrite runs.
- Rewrite creates a compact temporary AOF from a consistent DB snapshot.
- Writes that happen during rewrite are buffered and appended to the temporary
  AOF before the final switch.
- The final replacement uses atomic `rename` on the same filesystem.
- If rewrite fails, the old AOF remains active and replayable.

## TTL Semantics

TTL is enforced with both lazy expiration on access and an active sweeper running
every 100ms. AOF entries store absolute expiration times with `PXAT` /
`PEXPIREAT`, so TTL keeps decaying across process restarts instead of being reset
from the original relative duration.

Supported expiration commands include `EXPIRE`, `PEXPIRE`, `EXPIREAT`,
`PEXPIREAT`, `TTL`, `PTTL`, and `PERSIST`. `SET` supports `EX`, `PX`, `EXAT`,
and `PXAT`.

## Supported Commands

- **Strings:** `GET`, `SET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`,
  `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`
- **TTL:** `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`,
  `PERSIST`
- **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`
- **Hashes:** `HSET`, `HGET`, `HDEL`, `HKEYS`, `HVALS`, `HGETALL`, `HEXISTS`,
  `HLEN`, `HINCRBY`
- **Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- **Server:** `PING`, `ECHO`, `INFO`, `BGREWRITEAOF`

Unsupported Redis commands return `ERR unknown command`.

## Architecture

- One `tokio::spawn` per accepted connection.
- Sharded shared state: `Arc<Vec<Mutex<Shard>>>` with 16 shards, key-hashed via
  `xxhash-rust`.
- A lightweight DB write gate pauses mutating commands only while AOF rewrite
  takes its snapshot.
- Streaming RESP2 parser over `BytesMut`, with caps for bulk length, array
  length, nesting depth, and unterminated line length.
- Active TTL sweeper using a per-shard `BTreeMap<absolute_ms, Vec<Bytes>>`.
- Pub/Sub fan-out through `tokio::sync::broadcast`.
- AOF uses a single writer task plus rewrite buffering and atomic replacement.

## Tests And CI

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The integration suite currently has 103 tests covering RESP2 parsing, strings,
lists, hashes, pub/sub, TTL, AOF replay, AOF rewrite, `INFO`, and wire-level
response shapes for `redis-cli` workflows. GitHub Actions runs fmt, clippy, and
tests on push and pull request.

## Benchmarks

See [BENCHMARK.md](./BENCHMARK.md) for reproducible commands and the latest
same-machine comparison against official Redis 8.6.3. In the 2026-05-23 run,
mini-redis-rs reached roughly 84-105% of official Redis throughput on the tested
commands with AOF disabled on both servers.

## Out Of Scope

- Sorted sets (`ZADD` / `ZRANGE`)
- Cluster mode / hash slots
- Replication (`REPLICAOF` / `PSYNC`)
- RDB snapshot format
- Transactions (`MULTI` / `EXEC`)
- Scripting (`EVAL` / Lua)
- ACL
- RESP3
