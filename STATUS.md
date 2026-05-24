# STATUS — mini-redis-rs

> Snapshot for the next session. Read this **before** PLAN.md — PLAN.md is the
> original design (preserved for context); STATUS.md is what's actually true now.

**Last updated:** 2026-04-27
**Branch:** `main` · 15 commits · clean working tree
**Tests:** 89/89 passing (3× consecutive clean runs, no flakiness)
**Lint:** `cargo clippy --all-targets -- -D warnings` clean
**LoC:** src 1,604 · tests 1,215 · total ≈ 2,820

---

## How to verify the project still works (1 minute)

```bash
cd $HOME/Desktop/Work/mini-redis-rs
cargo test                                      # all 89 should pass
cargo clippy --all-targets -- -D warnings       # clean
cargo build --release
./target/release/miniredisd --port 6380 &
redis-cli -p 6380 PING                          # PONG
redis-cli -p 6380 SET k v && redis-cli -p 6380 GET k
kill %1
```

If any of those fail, something has rotted since 2026-04-27.

---

## Implementation status — all 10 milestones complete

| Milestone | Status | Commit | Tests |
|---|---|---|---|
| M1: TCP echo skeleton | ✅ done | `a5e7ffa` | n/a (replaced in M3) |
| M2: RESP2 parser + encoder | ✅ done | `d994105` | 18 → 20 (+2 length-cap) |
| M3: Connection + dispatch | ✅ done | `92c47ae` | 5 → 6 (+1 CRLF inj.) |
| M4: String commands | ✅ done | `f0f5659` | 13 |
| M5: TTL / expiration | ✅ done | `bac5e84` | 11 → 12 (+1 BTreeMap leak) |
| M6: List commands | ✅ done | `cea5345` | 12 → 15 (+3 LRANGE/LPOP regression) |
| M7: Hash commands | ✅ done | `e684112` | 11 |
| M8: Pub/Sub | ✅ done | `4e891f1` | 8 |
| M9: AOF persistence | ✅ done | `8877a22` | 4 |
| M10: Benchmark vs real Redis | ✅ done | `d268ef5` | n/a (numbers in BENCHMARK.md) |

**Benchmark headline:** ~80–98% of Redis 8.6.2 throughput; **lower** p50 latency
on every op. Full table in `BENCHMARK.md`.

---

## Code review pass (independent code-reviewer agent, 2026-04-27)

Original rating: **8/10**. Findings categorized by severity.

### Fixed (5 commits after M10)

| ID | Severity | Issue | Commit |
|---|---|---|---|
| C1 | Critical | `LRANGE k 5 10` on 3-elem list returned 1 element instead of empty array | `b6f4f86` |
| C3 | Critical | `aof::replay` propagated parser errors via `?`, blocking startup on any tail corruption | `b6f4f86` |
| H4 | High | `LPOP k 0` returned null bulk; Redis returns empty array | `b6f4f86` |
| H2 | High | Unknown command name with embedded `\r\n` produced an Error frame that split mid-stream (protocol injection) | `e58baf7` |
| L2 | Low→High | RESP parser had no length cap; `*9999999999\r\n` would `Vec::with_capacity(~tens of GB)` and OOM | `e58baf7` |
| H1 | High | `SET k v EX N` / `EXPIRE k N` rewrites pushed new BTreeMap rows without removing prior; PERSIST left stale rows | `4d9cab2` |
| H6 | High | Pub/Sub registry kept the `broadcast::Sender` after the last subscriber disconnected (lazy GC only on next PUBLISH) | `4d9cab2` |
| M2 | Medium | Unused `Command::is_subscribe` | `e1ca1e3` |
| M8 | Medium | `is_write` used open-set `matches!`; new variants would silently default to "non-persistent" → AOF data loss | `e1ca1e3` |
| M1 | Medium | 7 test files duplicated ~45 lines of helpers each (~315 lines total) | `98fc545` |
| — | Quality | 2 pre-existing flaky pubsub tests (`read_some` single-read for multi-frame responses) | `98fc545` |

7 new regression tests added across the fixes.

### Deliberately not fixed

| ID | Why deferred |
|---|---|
| **C2** AOF ack-before-write (client gets `+OK` before bytes reach disk) | Real fix needs `AofHandle::write` to be `async` with oneshot completion + handler awaits before responding. Big refactor; demo-tier honesty: documented in code comments + this STATUS. |
| **L7** AOF write failure logs but continues | Production needs to propagate (close handle + reject writes). Demo OK. |
| **H3** `std::sync::Mutex` in async context | Lock hold times are short and don't cross `await`. Documented constraint, not a bug. |
| **H5** `SET EX 0` vs `EXPIRE 0` error-message inconsistency | Wire-level both `-ERR`; client doesn't care. |
| **N3** `Db::iter_shards` exposes `&Mutex<Shard>` | Only `expire::sweep_once` uses it; encapsulation gain not worth the API churn. |
| **L1** `memchr`-based CRLF scan | Microopt; benchmark is already 80–98% of Redis. |
| **M3** `parse_*` arg-extraction templates | Some duplication in `cmd/mod.rs` (`let mut it = rest.into_iter(); let key = it.next().unwrap()`) but extracting helpers for 2/3-arg cases doesn't dominate readability. |

If picking this back up, **C2 is the only one with real correctness implications** — the rest are quality-of-implementation, not bugs.

---

## Out-of-scope (Phase 2 — never started)

Listed in PLAN.md tail. Each warrants its own plan:

- Sorted sets (`ZADD` / `ZRANGE` / `ZRANGEBYSCORE`) — needs skiplist
- Cluster mode (hash slots / `MOVED` redirects)
- Replication (`REPLICAOF` / `PSYNC`)
- RDB snapshot format
- Transactions (`MULTI` / `EXEC` / `WATCH`)
- Scripting (`EVAL` / Lua)
- ACL
- RESP3

---

## Installed software for this project

| Tool | Install | Purpose | Uninstall |
|---|---|---|---|
| `redis` 8.6.2 (Homebrew) | `brew install redis` | provides `redis-cli` (live verification) and `redis-benchmark` (M10) | `brew uninstall redis` |

That's the only system-level install. All other deps are Cargo crates, declared
in `Cargo.toml`, fetched on `cargo build` — no system pollution.

---

## File map (post-cleanup)

```
mini-redis-rs/
├── Cargo.toml
├── Cargo.lock                    # gitignored
├── PLAN.md                       # original design (do not modify; it's history)
├── STATUS.md                     # this file — read first
├── README.md                     # user-facing docs
├── BENCHMARK.md                  # M10 numbers
├── bench-results/                # raw redis-benchmark outputs
├── src/
│   ├── main.rs                   # CLI (clap), AOF wiring, listener bind
│   ├── lib.rs                    # re-exports
│   ├── server.rs                 # accept loop + per-conn handler + run_subscribed
│   ├── connection.rs             # framed RESP I/O over TcpStream
│   ├── aof.rs                    # FsyncPolicy, AofHandle, replay(), spawn_writer()
│   ├── resp/
│   │   ├── mod.rs                # Frame enum + Error
│   │   ├── parser.rs             # streaming parser (with length caps post-review)
│   │   └── encoder.rs            # Frame -> bytes
│   ├── db/
│   │   ├── mod.rs                # Db handle, Shard, Value, Entry, pubsub registry
│   │   └── expire.rs             # 100ms-tick sweeper
│   └── cmd/
│       ├── mod.rs                # Command enum, from_frame, apply, is_write
│       ├── string.rs             # SET/GET/DEL/EXISTS/INCR/APPEND/STRLEN/MGET/MSET
│       │                         # + EXPIRE/PEXPIRE/TTL/PTTL/PERSIST
│       ├── list.rs               # LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX
│       └── hash.rs               # HSET/HGET/HDEL/HKEYS/HVALS/HGETALL/HEXISTS/HLEN/HINCRBY
└── tests/
    ├── common/mod.rs             # shared spawn_server / send / read_n / read_some / array
    ├── echo.rs                   # PING/ECHO + CRLF-injection regression
    ├── resp_roundtrip.rs         # parser/encoder + length-cap regression
    ├── string_commands.rs
    ├── ttl.rs                    # + BTreeMap-leak regression
    ├── list_commands.rs          # + LRANGE/LPOP regression
    ├── hash_commands.rs
    ├── pubsub.rs
    └── aof.rs
```

---

## Quick "where do I pick up" guide for the next session

1. **Read this file first** (you are here).
2. **`git log --oneline`** — see the 15-commit story.
3. **`cargo test`** — confirm the integration suite still passes.
4. If picking up further work, the most worthwhile next things are:
   - **AOF scaling**: replay still reads the full AOF into memory, and rewrite still snapshots the full DB before writing; make those paths streaming if the project grows beyond teaching-scale datasets.
   - **Sorted sets (Phase 2)**: requires a sorted data structure (BTreeMap of score→Vec<Bytes> + HashMap of member→score), then ZADD/ZRANGE/ZRANGEBYSCORE/ZREM/ZRANGEBYLEX. Probably its own milestone-sized chunk.
   - **Cluster mode** is interesting but a much bigger lift (slot routing, gossip, MOVED redirects).
