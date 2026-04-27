# mini-redis-rs Implementation Plan

> Multi-step engineering project. From-scratch Rust reimplementation of the core of Redis. Verified end-to-end against the official `redis-cli` and `redis-benchmark` (Homebrew Redis 8.6.2 installed locally).

**Goal:** Build a Tokio-based Redis-compatible server that speaks RESP2, supports the most common commands across strings/lists/hashes/pub-sub/TTL, persists via AOF, and benchmarks within an order of magnitude of official Redis on a single MacBook.

**Architecture:**

- **Single async binary** (`miniredisd`) built on Tokio. Acceptor task spawns one task per TCP connection.
- **Sharded shared state** behind `Arc<Db>`. The `Db` is N shards (16 by default), each a `Mutex<Shard>` over a `HashMap<Bytes, Entry>`. Sharding by key-hash to reduce contention.
- **RESP framing layer** is a stateless codec module operating on `BytesMut` buffers — a streaming parser that returns `Incomplete` when more bytes are needed.
- **Commands** are an enum; per-connection task parses an array frame, converts it to a `Command`, calls `Command::apply(&Db) -> Frame`, writes the frame back.
- **TTL** stored alongside each entry as `Option<Instant>`. Lazy check on access + a periodic background sweeper task that pops the soonest deadline from a per-shard `BTreeMap<Instant, Vec<Bytes>>`.
- **Pub/Sub** uses `tokio::sync::broadcast` per channel, kept in a separate `Mutex<HashMap<Bytes, broadcast::Sender<Bytes>>>`.
- **AOF** is a single-writer append loop fed by an `mpsc::UnboundedSender<Vec<u8>>`. Replay on startup feeds raw bytes back through the parser into the dispatcher.

**Tech stack:** Rust 1.95 · Tokio 1 · `bytes` · `thiserror` · `tracing` · `tracing-subscriber` · `clap` · `criterion` (benchmarks). Verification uses Homebrew `redis-cli` / `redis-benchmark` against `127.0.0.1:6380` (we use 6380 to avoid clashing with default 6379).

**Repo layout:**

```
mini-redis-rs/
├── Cargo.toml             # Cargo manifest
├── PLAN.md                # this file
├── README.md              # short usage doc (added at M10)
├── BENCHMARK.md           # benchmark report (added at M10)
├── src/
│   ├── main.rs            # binary entry point (clap-parsed CLI)
│   ├── lib.rs             # re-exports for integration tests
│   ├── server.rs          # accept loop
│   ├── connection.rs      # per-connection task, RESP framing I/O
│   ├── resp/
│   │   ├── mod.rs         # Frame enum + Error
│   │   ├── parser.rs      # streaming parser
│   │   └── encoder.rs     # Frame -> bytes
│   ├── db/
│   │   ├── mod.rs         # Db handle, Shard, Entry, Value
│   │   ├── expire.rs      # TTL wheel + sweeper task
│   │   └── pubsub.rs      # channel registry
│   ├── cmd/
│   │   ├── mod.rs         # Command enum + parse + apply dispatcher
│   │   ├── string.rs      # GET/SET/DEL/EXISTS/INCR/APPEND/STRLEN/MGET/MSET
│   │   ├── list.rs        # LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX
│   │   ├── hash.rs        # HSET/HGET/HDEL/HKEYS/HVALS/HGETALL/HEXISTS/HINCRBY/HLEN
│   │   ├── pubsub.rs      # SUBSCRIBE/UNSUBSCRIBE/PUBLISH
│   │   └── server.rs      # PING/ECHO/COMMAND/SELECT/QUIT
│   └── aof.rs             # AOF writer + replay
├── tests/
│   ├── echo.rs            # M1
│   ├── resp_roundtrip.rs  # M2
│   ├── string_commands.rs # M4
│   ├── ttl.rs             # M5
│   ├── list_commands.rs   # M6
│   ├── hash_commands.rs   # M7
│   ├── pubsub.rs          # M8
│   └── aof.rs             # M9
└── benches/
    └── ops.rs             # criterion micro-benches
```

**Tools installed for this project:**

| Tool | Install command | Why | Uninstall |
|------|---|---|---|
| `redis` (Homebrew) | `brew install redis` | provides `redis-cli` and `redis-benchmark` for verification + a reference server to benchmark against | `brew uninstall redis` |

---

## Milestone 1 — Project skeleton + TCP echo server

**Files:**
- Create: `Cargo.toml`, `src/main.rs`, `src/lib.rs`, `src/server.rs`, `tests/echo.rs`

- [ ] **Step 1: Write Cargo.toml**

```toml
[package]
name = "mini-redis-rs"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"

[[bin]]
name = "miniredisd"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "net", "io-util", "macros", "sync", "time", "signal", "fs"] }
bytes = "1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 2: Write `src/lib.rs`**

```rust
pub mod server;
```

- [ ] **Step 3: Write `src/main.rs`**

```rust
use clap::Parser;
use mini_redis_rs::server;
use tokio::net::TcpListener;
use tokio::signal;

#[derive(Parser, Debug)]
#[command(version, about = "miniredisd: a tiny Redis-compatible server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6380)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "miniredisd listening");

    server::run(listener, signal::ctrl_c()).await
}
```

Note: we'll add `anyhow` to `[dependencies]` in this step too — append:

```toml
anyhow = "1"
```

- [ ] **Step 4: Write `src/server.rs`**

```rust
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn run(listener: TcpListener, shutdown: impl Future) -> anyhow::Result<()> {
    tokio::select! {
        res = accept_loop(listener) => res,
        _ = shutdown => {
            tracing::info!("shutdown signal received");
            Ok(())
        }
    }
}

async fn accept_loop(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (mut socket, peer) = listener.accept().await?;
        tracing::debug!(?peer, "accepted");
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        });
    }
}
```

- [ ] **Step 5: Write `tests/echo.rs`**

```rust
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn echoes_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        mini_redis_rs::server::run(listener, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    tokio::time::timeout(Duration::from_secs(1), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"hello");

    drop(shutdown_tx);
}
```

- [ ] **Step 6: Run tests and `cargo check`**

```bash
cargo check && cargo test --test echo
```

Expected: build succeeds, `echoes_bytes` passes.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "M1: tokio TCP echo server skeleton"
```

---

## Milestone 2 — RESP protocol (parser + encoder)

**Files:**
- Create: `src/resp/mod.rs`, `src/resp/parser.rs`, `src/resp/encoder.rs`, `tests/resp_roundtrip.rs`
- Modify: `src/lib.rs` (add `pub mod resp;`)

**RESP2 spec recap (what we implement):**
- `+<simple>\r\n` — Simple String
- `-<error>\r\n` — Error
- `:<int>\r\n` — Integer
- `$<len>\r\n<bytes>\r\n` — Bulk String, `$-1\r\n` is null
- `*<count>\r\n<frames...>` — Array, `*-1\r\n` is null array

- [ ] **Step 1: Write `src/resp/mod.rs`**

```rust
pub mod encoder;
pub mod parser;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("incomplete frame")]
    Incomplete,
    #[error("protocol error: {0}")]
    Protocol(String),
}
```

- [ ] **Step 2: Write `src/resp/parser.rs`**

```rust
use super::{Error, Frame};
use bytes::{Buf, Bytes, BytesMut};

pub fn parse(buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
    let mut cursor = std::io::Cursor::new(&buf[..]);
    match parse_frame(&mut cursor) {
        Ok(frame) => {
            let n = cursor.position() as usize;
            buf.advance(n);
            Ok(Some(frame))
        }
        Err(Error::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_frame(c: &mut std::io::Cursor<&[u8]>) -> Result<Frame, Error> {
    let tag = read_u8(c)?;
    match tag {
        b'+' => Ok(Frame::Simple(read_line_string(c)?)),
        b'-' => Ok(Frame::Error(read_line_string(c)?)),
        b':' => Ok(Frame::Integer(read_line_int(c)?)),
        b'$' => parse_bulk(c),
        b'*' => parse_array(c),
        other => Err(Error::Protocol(format!("invalid type byte: 0x{other:02x}"))),
    }
}

fn parse_bulk(c: &mut std::io::Cursor<&[u8]>) -> Result<Frame, Error> {
    let len = read_line_int(c)?;
    if len == -1 {
        return Ok(Frame::Null);
    }
    let len = usize::try_from(len).map_err(|_| Error::Protocol("negative bulk len".into()))?;
    let remaining = c.get_ref().len() - c.position() as usize;
    if remaining < len + 2 {
        return Err(Error::Incomplete);
    }
    let start = c.position() as usize;
    let bytes = Bytes::copy_from_slice(&c.get_ref()[start..start + len]);
    c.set_position((start + len) as u64);
    if read_u8(c)? != b'\r' || read_u8(c)? != b'\n' {
        return Err(Error::Protocol("missing CRLF after bulk".into()));
    }
    Ok(Frame::Bulk(bytes))
}

fn parse_array(c: &mut std::io::Cursor<&[u8]>) -> Result<Frame, Error> {
    let count = read_line_int(c)?;
    if count == -1 {
        return Ok(Frame::Null);
    }
    let count = usize::try_from(count).map_err(|_| Error::Protocol("negative array len".into()))?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(parse_frame(c)?);
    }
    Ok(Frame::Array(items))
}

fn read_u8(c: &mut std::io::Cursor<&[u8]>) -> Result<u8, Error> {
    if !c.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(c.get_u8())
}

fn read_line<'a>(c: &mut std::io::Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let buf = c.get_ref();
    let start = c.position() as usize;
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            c.set_position((i + 2) as u64);
            return Ok(&buf[start..i]);
        }
    }
    Err(Error::Incomplete)
}

fn read_line_string(c: &mut std::io::Cursor<&[u8]>) -> Result<String, Error> {
    let line = read_line(c)?;
    std::str::from_utf8(line)
        .map(|s| s.to_string())
        .map_err(|_| Error::Protocol("invalid utf8 in line".into()))
}

fn read_line_int(c: &mut std::io::Cursor<&[u8]>) -> Result<i64, Error> {
    let line = read_line(c)?;
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Protocol("invalid integer".into()))
}
```

- [ ] **Step 3: Write `src/resp/encoder.rs`**

```rust
use super::Frame;
use bytes::{BufMut, BytesMut};

pub fn encode(frame: &Frame, out: &mut BytesMut) {
    match frame {
        Frame::Simple(s) => {
            out.put_u8(b'+');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            out.put_u8(b'-');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            out.put_u8(b':');
            out.put_slice(n.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Bulk(b) => {
            out.put_u8(b'$');
            out.put_slice(b.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        Frame::Null => out.put_slice(b"$-1\r\n"),
        Frame::Array(items) => {
            out.put_u8(b'*');
            out.put_slice(items.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for item in items {
                encode(item, out);
            }
        }
    }
}
```

- [ ] **Step 4: Add `pub mod resp;` to `src/lib.rs`**

- [ ] **Step 5: Write `tests/resp_roundtrip.rs`**

```rust
use bytes::{Bytes, BytesMut};
use mini_redis_rs::resp::{encoder, parser, Frame};

fn roundtrip(frame: Frame) {
    let mut buf = BytesMut::new();
    encoder::encode(&frame, &mut buf);
    let parsed = parser::parse(&mut buf).unwrap().unwrap();
    assert_eq!(frame, parsed);
    assert!(buf.is_empty());
}

#[test]
fn simple_string() {
    roundtrip(Frame::Simple("OK".into()));
}

#[test]
fn integer() {
    roundtrip(Frame::Integer(-42));
}

#[test]
fn bulk_string() {
    roundtrip(Frame::Bulk(Bytes::from_static(b"hello world")));
}

#[test]
fn null_bulk() {
    roundtrip(Frame::Null);
}

#[test]
fn nested_array() {
    roundtrip(Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"k")),
        Frame::Bulk(Bytes::from_static(b"v")),
    ]));
}

#[test]
fn incomplete_returns_none() {
    let mut buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$3\r\nfo"[..]);
    assert!(parser::parse(&mut buf).unwrap().is_none());
    assert_eq!(&buf[..], b"*2\r\n$3\r\nSET\r\n$3\r\nfo");
}

#[test]
fn bad_type_byte_errors() {
    let mut buf = BytesMut::from(&b"@bogus\r\n"[..]);
    assert!(parser::parse(&mut buf).is_err());
}
```

- [ ] **Step 6: Run**

```bash
cargo test --test resp_roundtrip
```

Expected: 7 passing.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "M2: RESP2 streaming parser + encoder"
```

---

## Milestone 3 — Connection handling + command dispatch

**Files:**
- Create: `src/connection.rs`, `src/cmd/mod.rs`, `src/cmd/server.rs`
- Modify: `src/server.rs` (replace echo with framed dispatch), `src/lib.rs`

- [ ] **Step 1: Write `src/connection.rs`**

```rust
use crate::resp::{encoder, parser, Frame};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
    out: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            out: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if let Some(frame) = parser::parse(&mut self.buf)? {
                return Ok(Some(frame));
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!("connection closed mid-frame"));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> anyhow::Result<()> {
        self.out.clear();
        encoder::encode(frame, &mut self.out);
        self.stream.write_all(&self.out).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
```

- [ ] **Step 2: Write `src/cmd/mod.rs`**

```rust
pub mod server;

use crate::resp::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Unknown(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("not an array")]
    NotArray,
    #[error("empty command")]
    Empty,
    #[error("argument {0} must be a bulk string")]
    NotBulk(usize),
    #[error("wrong number of arguments for {0}")]
    Arity(String),
    #[error("invalid utf8 in command name")]
    BadName,
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, ParseError> {
        let mut items = match frame {
            Frame::Array(v) => v.into_iter(),
            _ => return Err(ParseError::NotArray),
        };
        let name_frame = items.next().ok_or(ParseError::Empty)?;
        let name_bytes = expect_bulk(name_frame, 0)?;
        let name = std::str::from_utf8(&name_bytes)
            .map_err(|_| ParseError::BadName)?
            .to_ascii_uppercase();
        let rest: Vec<Bytes> = items
            .enumerate()
            .map(|(i, f)| expect_bulk(f, i + 1))
            .collect::<Result<_, _>>()?;
        match name.as_str() {
            "PING" => match rest.len() {
                0 => Ok(Command::Ping(None)),
                1 => Ok(Command::Ping(Some(rest.into_iter().next().unwrap()))),
                _ => Err(ParseError::Arity("PING".into())),
            },
            "ECHO" => {
                if rest.len() != 1 {
                    return Err(ParseError::Arity("ECHO".into()));
                }
                Ok(Command::Echo(rest.into_iter().next().unwrap()))
            }
            other => Ok(Command::Unknown(other.to_string())),
        }
    }

    pub fn apply(self) -> Frame {
        match self {
            Command::Ping(None) => Frame::Simple("PONG".into()),
            Command::Ping(Some(msg)) => Frame::Bulk(msg),
            Command::Echo(msg) => Frame::Bulk(msg),
            Command::Unknown(name) => {
                Frame::Error(format!("ERR unknown command '{}'", name))
            }
        }
    }
}

fn expect_bulk(frame: Frame, idx: usize) -> Result<Bytes, ParseError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        _ => Err(ParseError::NotBulk(idx)),
    }
}
```

- [ ] **Step 3: Write `src/cmd/server.rs`** (placeholder, real impls land in M4+)

```rust
// reserved for server-meta commands; populated in later milestones
```

- [ ] **Step 4: Replace `src/server.rs` with framed dispatcher**

```rust
use crate::cmd::Command;
use crate::connection::Connection;
use crate::resp::Frame;
use std::future::Future;
use tokio::net::TcpListener;

pub async fn run(listener: TcpListener, shutdown: impl Future) -> anyhow::Result<()> {
    tokio::select! {
        res = accept_loop(listener) => res,
        _ = shutdown => {
            tracing::info!("shutdown signal received");
            Ok(())
        }
    }
}

async fn accept_loop(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!(?peer, "accepted");
        tokio::spawn(async move {
            if let Err(e) = handle(socket).await {
                tracing::warn!(?peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(socket: tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut conn = Connection::new(socket);
    while let Some(frame) = conn.read_frame().await? {
        let response = match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(),
            Err(e) => Frame::Error(format!("ERR {}", e)),
        };
        conn.write_frame(&response).await?;
    }
    Ok(())
}
```

- [ ] **Step 5: Update `src/lib.rs`**

```rust
pub mod cmd;
pub mod connection;
pub mod resp;
pub mod server;
```

- [ ] **Step 6: Replace the old `tests/echo.rs` with a real PING test**

Delete the old file content and replace with:

```rust
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        mini_redis_rs::server::run(listener, async move {
            let _ = rx.await;
        })
        .await
        .ok();
    });
    addr
}

#[tokio::test]
async fn ping_pong() {
    let addr = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
}

#[tokio::test]
async fn echo_works() {
    let addr = spawn_server().await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 11];
    tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"$5\r\nhello\r\n");
}
```

- [ ] **Step 7: Run**

```bash
cargo test
```

Expected: all tests pass (resp_roundtrip × 7 + echo file's 2).

- [ ] **Step 8: Smoke-test against real `redis-cli`**

Terminal 1:
```bash
cargo run -q -- --port 6380
```

Terminal 2:
```bash
redis-cli -p 6380 PING       # expect: PONG
redis-cli -p 6380 ECHO hi    # expect: "hi"
```

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "M3: framed connection + Command dispatch (PING, ECHO)"
```

---

## Milestone 4 — String commands

**Files:**
- Create: `src/db/mod.rs`, `src/cmd/string.rs`, `tests/string_commands.rs`
- Modify: `src/cmd/mod.rs` (route string commands), `src/server.rs` (pass `Db` into handler), `src/lib.rs` (`pub mod db;`)

**Storage design:**
- `Db` is `Arc<Vec<Mutex<Shard>>>`. 16 shards. Key picks shard via `XxHash64(key) % 16`.
- `Shard` is `HashMap<Bytes, Entry>`. `Entry { value: Value, expires_at: Option<Instant> }`. `Value` is an enum starting with `String(Bytes)` (List/Hash added in M6/M7).

- [ ] **Step 1: Add `xxhash-rust` to Cargo.toml**

```toml
xxhash-rust = { version = "0.8", features = ["xxh64"] }
```

- [ ] **Step 2: Write `src/db/mod.rs`**

```rust
use bytes::Bytes;
use std::collections::HashMap;
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
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 3: Write `src/cmd/string.rs`**

```rust
use crate::db::{Db, Entry, Value};
use crate::resp::Frame;
use bytes::Bytes;
use std::time::Instant;

pub fn get(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(entry) if !expired(entry) => match &entry.value {
            Value::String(b) => Frame::Bulk(b.clone()),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        },
        _ => Frame::Null,
    }
}

pub fn set(db: &Db, key: Bytes, value: Bytes, expires_at: Option<Instant>) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    shard
        .entries
        .insert(key, Entry { value: Value::String(value), expires_at });
    Frame::Simple("OK".into())
}

pub fn del(db: &Db, keys: &[Bytes]) -> Frame {
    let mut removed = 0i64;
    for key in keys {
        let mut shard = db.shard_for(key).lock().unwrap();
        if shard.entries.remove(key).is_some() {
            removed += 1;
        }
    }
    Frame::Integer(removed)
}

pub fn exists(db: &Db, keys: &[Bytes]) -> Frame {
    let mut count = 0i64;
    for key in keys {
        let shard = db.shard_for(key).lock().unwrap();
        if shard.entries.get(key).map(|e| !expired(e)).unwrap_or(false) {
            count += 1;
        }
    }
    Frame::Integer(count)
}

pub fn incr(db: &Db, key: Bytes, delta: i64) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::String(Bytes::from_static(b"0")),
        expires_at: None,
    });
    let current = match &entry.value {
        Value::String(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => return Frame::Error("ERR value is not an integer or out of range".into()),
        },
        _ => return Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    };
    let new = match current.checked_add(delta) {
        Some(n) => n,
        None => return Frame::Error("ERR increment or decrement would overflow".into()),
    };
    entry.value = Value::String(Bytes::from(new.to_string()));
    Frame::Integer(new)
}

pub fn append(db: &Db, key: Bytes, suffix: Bytes) -> Frame {
    let mut shard = db.shard_for(&key).lock().unwrap();
    let entry = shard.entries.entry(key).or_insert(Entry {
        value: Value::String(Bytes::new()),
        expires_at: None,
    });
    match &mut entry.value {
        Value::String(b) => {
            let mut combined = bytes::BytesMut::with_capacity(b.len() + suffix.len());
            combined.extend_from_slice(b);
            combined.extend_from_slice(&suffix);
            *b = combined.freeze();
            Frame::Integer(b.len() as i64)
        }
        _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    }
}

pub fn strlen(db: &Db, key: &Bytes) -> Frame {
    let shard = db.shard_for(key).lock().unwrap();
    match shard.entries.get(key) {
        Some(e) if !expired(e) => match &e.value {
            Value::String(b) => Frame::Integer(b.len() as i64),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        },
        _ => Frame::Integer(0),
    }
}

pub fn mget(db: &Db, keys: &[Bytes]) -> Frame {
    let frames = keys
        .iter()
        .map(|k| {
            let shard = db.shard_for(k).lock().unwrap();
            match shard.entries.get(k) {
                Some(e) if !expired(e) => match &e.value {
                    Value::String(b) => Frame::Bulk(b.clone()),
                    _ => Frame::Null,
                },
                _ => Frame::Null,
            }
        })
        .collect();
    Frame::Array(frames)
}

pub fn mset(db: &Db, pairs: Vec<(Bytes, Bytes)>) -> Frame {
    for (k, v) in pairs {
        let mut shard = db.shard_for(&k).lock().unwrap();
        shard.entries.insert(k, Entry { value: Value::String(v), expires_at: None });
    }
    Frame::Simple("OK".into())
}

fn expired(entry: &Entry) -> bool {
    matches!(entry.expires_at, Some(t) if Instant::now() >= t)
}
```

- [ ] **Step 4: Extend `Command` enum in `src/cmd/mod.rs`**

Add variants for `Get`, `Set`, `Del`, `Exists`, `Incr`, `Decr`, `IncrBy`, `DecrBy`, `Append`, `Strlen`, `MGet`, `MSet`. Update `from_frame` to parse them. Update `apply` to take `&Db`:

```rust
// new signature
pub fn apply(self, db: &Db) -> Frame {
    use crate::cmd::string;
    match self {
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),
        Command::Get(k) => string::get(db, &k),
        Command::Set(k, v) => string::set(db, k, v, None),
        Command::Del(keys) => string::del(db, &keys),
        Command::Exists(keys) => string::exists(db, &keys),
        Command::Incr(k) => string::incr(db, k, 1),
        Command::Decr(k) => string::incr(db, k, -1),
        Command::IncrBy(k, n) => string::incr(db, k, n),
        Command::DecrBy(k, n) => string::incr(db, k, -n),
        Command::Append(k, v) => string::append(db, k, v),
        Command::Strlen(k) => string::strlen(db, &k),
        Command::MGet(keys) => string::mget(db, &keys),
        Command::MSet(pairs) => string::mset(db, pairs),
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{}'", name)),
    }
}
```

For each new variant, add an arm to the `match name.as_str()` block. Pattern: parse the right number of bulk-string args; INCR/DECR take 1 key; INCRBY/DECRBY take 1 key + 1 integer; MSET takes pairs (`rest.len()` must be even).

- [ ] **Step 5: Wire `Db` through the server**

In `src/server.rs`:

```rust
pub async fn run(listener: TcpListener, shutdown: impl Future) -> anyhow::Result<()> {
    let db = crate::db::Db::new();
    tokio::select! {
        res = accept_loop(listener, db) => res,
        _ = shutdown => Ok(()),
    }
}

async fn accept_loop(listener: TcpListener, db: crate::db::Db) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(socket, db).await {
                tracing::warn!(?peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(socket: tokio::net::TcpStream, db: crate::db::Db) -> anyhow::Result<()> {
    let mut conn = Connection::new(socket);
    while let Some(frame) = conn.read_frame().await? {
        let response = match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(&db),
            Err(e) => Frame::Error(format!("ERR {}", e)),
        };
        conn.write_frame(&response).await?;
    }
    Ok(())
}
```

- [ ] **Step 6: Add `pub mod db;` to `src/lib.rs`**

- [ ] **Step 7: Write `tests/string_commands.rs`**

A reusable helper to spawn the server + redis-cli-style assertions. Send raw RESP frames and read raw frames back. Cover: SET+GET, GET-missing returns null bulk (`$-1\r\n`), DEL counts, EXISTS, INCR success, INCR-non-int error, INCR overflow error, APPEND grows length, STRLEN, MSET+MGET. (All tests pattern-match exact expected RESP bytes.)

- [ ] **Step 8: Run tests + `cargo clippy -- -D warnings`**

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

- [ ] **Step 9: Smoke-test with real `redis-cli`**

```bash
cargo run -q -- --port 6380 &
redis-cli -p 6380 SET foo bar       # OK
redis-cli -p 6380 GET foo            # "bar"
redis-cli -p 6380 INCR counter       # (integer) 1
redis-cli -p 6380 INCR counter       # (integer) 2
redis-cli -p 6380 APPEND foo "_baz"  # (integer) 7
redis-cli -p 6380 GET foo            # "bar_baz"
kill %1
```

- [ ] **Step 10: Commit**

```bash
git add -A && git commit -m "M4: string commands (SET/GET/DEL/EXISTS/INCR/APPEND/STRLEN/MGET/MSET) with sharded storage"
```

---

## Milestone 5 — TTL / Expiration

**Files:**
- Create: `src/db/expire.rs`
- Modify: `src/db/mod.rs` (per-shard expiration index), `src/cmd/string.rs` (parse SET options), `src/cmd/mod.rs` (EXPIRE/PEXPIRE/TTL/PTTL/PERSIST), `src/server.rs` (spawn sweeper task), `tests/ttl.rs`

**Design:** Each shard gets a `BTreeMap<Instant, Vec<Bytes>>` sidecar so the sweeper task can pop the soonest deadline and check it against `Instant::now()`. Lazy expiration on read remains in place.

- [ ] **Step 1: Extend `Shard`** in `src/db/mod.rs`:

```rust
#[derive(Default, Debug)]
pub struct Shard {
    pub entries: HashMap<Bytes, Entry>,
    pub expirations: std::collections::BTreeMap<Instant, Vec<Bytes>>,
}
```

Add helper `Db::insert_expiration(shard_idx, key, deadline)` and `Db::clear_expiration(shard_idx, key)` — but it's simpler to keep per-shard and let callers manage both maps inside the same lock.

- [ ] **Step 2: Update `string::set` to accept and record expiration**

When `expires_at` is Some, also push into `shard.expirations` keyed by that instant.

- [ ] **Step 3: Write `src/db/expire.rs`** — sweeper task:

```rust
use crate::db::Db;
use std::time::{Duration, Instant};

pub async fn run_sweeper(db: Db) {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = Instant::now();
        for shard in db.iter_shards() {
            let mut shard = shard.lock().unwrap();
            let expired_keys: Vec<_> = shard
                .expirations
                .range(..=now)
                .flat_map(|(_, ks)| ks.clone())
                .collect();
            shard.expirations.retain(|t, _| *t > now);
            for k in expired_keys {
                shard.entries.remove(&k);
            }
        }
    }
}
```

Add `Db::iter_shards(&self) -> impl Iterator<Item = &Mutex<Shard>>`.

- [ ] **Step 4: Spawn sweeper from `server::run`**

```rust
let db = Db::new();
tokio::spawn(crate::db::expire::run_sweeper(db.clone()));
```

- [ ] **Step 5: Parse `SET ... EX <seconds> | PX <ms>` in `Command::from_frame`**

- [ ] **Step 6: Add EXPIRE / PEXPIRE / TTL / PTTL / PERSIST commands**

Each is a small function in `src/cmd/string.rs` (or a new `src/cmd/expire.rs`). TTL returns -2 for missing keys, -1 for no TTL, else seconds remaining.

- [ ] **Step 7: Write `tests/ttl.rs`** using `tokio::time::pause()` + `advance()` to deterministically test expiration without sleeping.

- [ ] **Step 8: Smoke test with real cli**

```bash
redis-cli -p 6380 SET k v EX 1
redis-cli -p 6380 GET k         # "v"
sleep 2
redis-cli -p 6380 GET k         # (nil)
redis-cli -p 6380 SET k v
redis-cli -p 6380 TTL k         # -1
redis-cli -p 6380 EXPIRE k 30
redis-cli -p 6380 TTL k         # 30 (or 29)
```

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "M5: TTL with active sweeper + lazy check"
```

---

## Milestone 6 — List commands

**Files:**
- Modify: `src/db/mod.rs` (add `Value::List(VecDeque<Bytes>)`)
- Create: `src/cmd/list.rs`, `tests/list_commands.rs`
- Modify: `src/cmd/mod.rs`

- [ ] **Step 1: Add `Value::List(std::collections::VecDeque<Bytes>)`** to the `Value` enum

- [ ] **Step 2: Implement LPUSH / RPUSH / LPOP / RPOP / LRANGE / LLEN / LINDEX**

Wrong-type returns `WRONGTYPE`. LRANGE indices are signed (negative = from end). LPOP/RPOP without arg pops 1; with arg pops N.

- [ ] **Step 3: Tests**

- LPUSH then LRANGE 0 -1 returns reversed insertion order
- RPUSH then LRANGE 0 -1 returns insertion order
- LPOP on empty list returns null bulk
- LPOP N on list of M < N returns array of M
- LRANGE 0 -1 on missing key returns empty array (`*0\r\n`)
- LPUSH on a string key returns WRONGTYPE

- [ ] **Step 4: Smoke**

```bash
redis-cli -p 6380 RPUSH mylist a b c       # (integer) 3
redis-cli -p 6380 LRANGE mylist 0 -1       # 1) "a" 2) "b" 3) "c"
redis-cli -p 6380 LPOP mylist              # "a"
redis-cli -p 6380 LLEN mylist              # 2
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "M6: list commands (LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX)"
```

---

## Milestone 7 — Hash commands

**Files:**
- Modify: `src/db/mod.rs` (add `Value::Hash(HashMap<Bytes, Bytes>)`)
- Create: `src/cmd/hash.rs`, `tests/hash_commands.rs`
- Modify: `src/cmd/mod.rs`

- [ ] **Step 1: Extend `Value`**

- [ ] **Step 2: Implement HSET (variadic field-value pairs), HGET, HDEL, HKEYS, HVALS, HGETALL, HEXISTS, HINCRBY, HLEN**

HSET return = number of new fields added. HINCRBY treats missing field as 0; non-integer existing value → error.

- [ ] **Step 3: Tests** — HSET + HGETALL, HINCRBY missing-field, HINCRBY wrong-type, HDEL multi-field.

- [ ] **Step 4: Smoke**

```bash
redis-cli -p 6380 HSET user:1 name alice age 30   # (integer) 2
redis-cli -p 6380 HGETALL user:1                  # 1) "name" 2) "alice" 3) "age" 4) "30"
redis-cli -p 6380 HINCRBY user:1 age 5            # (integer) 35
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "M7: hash commands"
```

---

## Milestone 8 — Pub/Sub

**Files:**
- Create: `src/db/pubsub.rs`, `src/cmd/pubsub.rs`, `tests/pubsub.rs`
- Modify: `src/connection.rs` (or `src/server.rs`'s `handle`) to support subscribed-mode I/O loop

**Design:**
- A `PubSub` registry: `Arc<Mutex<HashMap<Bytes, broadcast::Sender<Bytes>>>>`. `PUBLISH` looks up the channel and sends; returns receiver count.
- `SUBSCRIBE` switches the per-connection task into "subscribed mode": uses `tokio::select!` between `conn.read_frame()` and any active `broadcast::Receiver::recv()`.

- [ ] **Step 1: Add `PubSub` to the Db struct (or as a sibling field carried through `apply`)**

- [ ] **Step 2: Implement PUBLISH** — returns integer count of subscribers reached.

- [ ] **Step 3: Refactor `handle`** to keep a `Vec<(Bytes, broadcast::Receiver<Bytes>)>` of subscriptions, and switch into a `tokio::select!` loop once non-empty.

- [ ] **Step 4: Tests** — spawn server, connect subscriber, connect publisher, publish, assert subscriber receives within timeout.

- [ ] **Step 5: Smoke** — two terminals: `redis-cli -p 6380 SUBSCRIBE ch` and `redis-cli -p 6380 PUBLISH ch hello`.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "M8: pub/sub with broadcast channels"
```

---

## Milestone 9 — Persistence (AOF)

**Files:**
- Create: `src/aof.rs`, `tests/aof.rs`
- Modify: `src/server.rs`, `src/cmd/mod.rs`, `src/main.rs` (CLI flag `--aof <path>`)

**Design:**
- A single AOF writer task owns the file. Connections send `Vec<u8>` (the original RESP frame bytes of writing commands) over an `mpsc::UnboundedSender`.
- On startup if `--aof` path exists, open it, feed bytes through the parser and dispatch each frame through `Command::from_frame` + `apply` synchronously before accepting connections.
- fsync policy: `--aof-fsync always|everysec|no` (default everysec).

- [ ] **Step 1: Define a list of "write" commands** (SET/DEL/INCR/DECR/INCRBY/DECRBY/APPEND/MSET/EXPIRE/PEXPIRE/PERSIST/LPUSH/RPUSH/LPOP/RPOP/HSET/HDEL/HINCRBY). Tag each `Command` variant with `is_write()`.

- [ ] **Step 2: Modify `handle`** to capture the raw frame bytes before dispatching, and if `is_write()` send them to the AOF writer channel.

- [ ] **Step 3: Implement writer task** with the 3 fsync policies.

- [ ] **Step 4: Implement startup replay** — read file, repeatedly call `parser::parse`, dispatch frames.

- [ ] **Step 5: Tests** — write some data, drop the server, restart with same `--aof` path, assert data still present.

- [ ] **Step 6: Smoke** — start server with `--aof /tmp/miniredis.aof`, SET some keys, kill -9, restart, GET them.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "M9: AOF persistence with replay on startup"
```

---

## Milestone 10 — Benchmark vs official Redis

**Files:**
- Create: `BENCHMARK.md`, `README.md`, `benches/ops.rs` (criterion in-process benches)

- [ ] **Step 1: Write `benches/ops.rs`** — criterion bench that runs SET/GET inside the process bypassing TCP, to isolate per-op cost.

- [ ] **Step 2: Run `redis-benchmark` against both servers**

```bash
# our server
cargo run --release -q -- --port 6380 &
sleep 0.5
redis-benchmark -p 6380 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -q -c 50 > ours.txt
kill %1

# real redis
redis-server --port 6381 --daemonize yes
sleep 0.5
redis-benchmark -p 6381 -t set,get,incr,lpush,rpush,lpop,rpop,hset -n 100000 -q -c 50 > theirs.txt
redis-cli -p 6381 SHUTDOWN NOSAVE
```

- [ ] **Step 3: Diff the numbers, write `BENCHMARK.md`** with side-by-side table and commentary on where the gap is widest and why (likely GIL-style mutex contention on hot keys vs Redis's single-thread design — note this as a finding, not a bug).

- [ ] **Step 4: Write `README.md`** — install, run, supported commands, "what's missing" list, link to BENCHMARK.md.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "M10: benchmark vs official Redis 8.6.2 + README + BENCHMARK"
```

---

## Out of scope (Phase 2, not planned here)

- Sorted sets / ZADD / ZRANGEBYSCORE (skiplist work)
- Cluster mode / hash slots / MOVED redirects
- Replication (REPLICAOF + PSYNC)
- RESP3
- Scripting (EVAL / Lua)
- ACL
- RDB snapshot format
- Transactions (MULTI/EXEC/WATCH)

These each warrant their own plan if we continue.
