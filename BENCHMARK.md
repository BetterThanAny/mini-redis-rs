# Benchmark - mini-redis-rs vs Redis 8.6.3

**Date:** 2026-05-23
**Hardware/OS:** Apple Silicon arm64, macOS 15.7.5 (24G624)
**Tool:** `redis-benchmark` from Redis 8.6.3
**Workload:** 100,000 ops per command, concurrency 50, default value sizes
**Persistence:** AOF disabled for both servers

```bash
redis-benchmark -p <port> -t set,get,incr,lpush,rpush,lpop,rpop,hset \
                -n 100000 -c 50 -q
```

`redis-benchmark` prints `WARNING: Could not fetch server CONFIG` for
mini-redis-rs because this project does not implement `CONFIG`. The benchmark
still runs the selected commands normally.

## Throughput (requests / second)

| Command | mini-redis-rs | Redis 8.6.3 | Ratio (theirs / ours) |
|---|---:|---:|---:|
| SET   | 189,394 | 209,205 | 1.10x |
| GET   | 190,114 | 225,734 | 1.19x |
| INCR  | 209,644 | 218,818 | 1.04x |
| LPUSH | 229,358 | 218,818 | 0.95x |
| RPUSH | 222,222 | 221,239 | 1.00x |
| LPOP  | 207,469 | 210,084 | 1.01x |
| RPOP  | 193,798 | 214,592 | 1.11x |
| HSET  | 216,450 | 215,517 | 1.00x |

mini-redis-rs reaches roughly **84-105% of official Redis throughput** on this
CPU-bound local benchmark. The strongest gap is `GET`; list and hash writes are
near parity in this run.

## p50 latency (milliseconds)

| Command | mini-redis-rs | Redis 8.6.3 |
|---|---:|---:|
| SET   | 0.119 | 0.183 |
| GET   | 0.119 | 0.167 |
| INCR  | 0.119 | 0.175 |
| LPUSH | 0.111 | 0.183 |
| RPUSH | 0.111 | 0.175 |
| LPOP  | 0.119 | 0.191 |
| RPOP  | 0.127 | 0.183 |
| HSET  | 0.119 | 0.183 |

The lower p50 latency for mini-redis-rs likely comes from the multi-threaded
Tokio runtime and sharded in-memory state. Official Redis is still more mature
on protocol fast paths, memory layout, operational features, and tail latency.

## Reproducing

Build release:

```bash
cargo build --release
```

Run mini-redis-rs:

```bash
./target/release/miniredisd --port 6380
redis-benchmark -p 6380 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -c 50 -q
```

Run official Redis:

```bash
redis-server --port 6381 --daemonize yes --save "" --appendonly no
redis-benchmark -p 6381 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -c 50 -q
redis-cli -p 6381 SHUTDOWN NOSAVE
```

Raw outputs are in `bench-results/ours.txt` and `bench-results/theirs.txt`.
