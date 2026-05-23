use crate::aof::AofHandle;
use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::Db;
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub(crate) struct ServerState {
    connected_clients: AtomicUsize,
    total_connections: AtomicU64,
    started_at_ms: u64,
}

impl ServerState {
    fn new() -> Self {
        Self {
            connected_clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            started_at_ms: crate::db::now_millis() as u64,
        }
    }
}

pub async fn run(listener: TcpListener, shutdown: impl Future) -> anyhow::Result<()> {
    run_with_options(listener, Db::new(), None, shutdown).await
}

pub async fn run_with_options(
    listener: TcpListener,
    db: Db,
    aof: Option<AofHandle>,
    shutdown: impl Future,
) -> anyhow::Result<()> {
    tokio::spawn(crate::db::expire::run_sweeper(db.clone()));
    let state = Arc::new(ServerState::new());
    tokio::select! {
        res = accept_loop(listener, db, aof, state) => res,
        _ = shutdown => {
            tracing::info!("shutdown signal received");
            Ok(())
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    db: Db,
    aof: Option<AofHandle>,
    state: Arc<ServerState>,
) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let db = db.clone();
        let aof = aof.clone();
        let state = state.clone();
        state.total_connections.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(?peer, "accepted");
        tokio::spawn(async move {
            let _guard = ClientGuard::new(state.clone());
            if let Err(e) = handle(socket, db, aof, state).await {
                tracing::warn!(?peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(
    socket: tokio::net::TcpStream,
    db: Db,
    aof: Option<AofHandle>,
    state: Arc<ServerState>,
) -> anyhow::Result<()> {
    let mut conn = Connection::new(socket);
    while let Some(frame) = conn.read_frame().await? {
        match Command::from_frame(frame) {
            Ok(Command::Subscribe(channels)) => {
                run_subscribed(&mut conn, &db, channels).await?;
            }
            Ok(Command::Info(section)) => {
                let resp = info_frame(&db, aof.as_ref(), Some(&state), section.as_deref());
                conn.write_frame(&resp).await?;
            }
            Ok(Command::BgRewriteAof) => {
                let resp = match &aof {
                    Some(aof) => match aof.schedule_rewrite(db.clone()) {
                        Ok(()) => {
                            Frame::Simple("Background append only file rewriting started".into())
                        }
                        Err(_) => Frame::Error(
                            "ERR Background append only file rewriting already in progress".into(),
                        ),
                    },
                    None => Frame::Error("ERR AOF is not enabled".into()),
                };
                conn.write_frame(&resp).await?;
            }
            Ok(cmd) => {
                let is_write = cmd.is_write();
                let aof_frame = if is_write { cmd.aof_frame() } else { None };
                let resp = if is_write {
                    let _write_guard = db.write_guard().await;
                    let resp = cmd.apply(&db);
                    if !matches!(resp, Frame::Error(_)) {
                        if let (Some(a), Some(frame)) = (&aof, &aof_frame) {
                            a.write(frame);
                        }
                    }
                    resp
                } else {
                    cmd.apply(&db)
                };
                conn.write_frame(&resp).await?;
            }
            Err(e) => {
                conn.write_frame(&Frame::Error(format!("ERR {}", e)))
                    .await?;
            }
        }
    }
    Ok(())
}

struct ClientGuard {
    state: Arc<ServerState>,
}

impl ClientGuard {
    fn new(state: Arc<ServerState>) -> Self {
        state.connected_clients.fetch_add(1, Ordering::SeqCst);
        Self { state }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.state.connected_clients.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn info_frame(
    db: &Db,
    aof: Option<&AofHandle>,
    state: Option<&ServerState>,
    section: Option<&str>,
) -> Frame {
    let wants_all = section
        .map(|s| s == "default" || s == "all")
        .unwrap_or(true);
    let include = |name: &str| wants_all || section == Some(name);
    let mut out = String::new();

    if include("server") {
        out.push_str("# Server\r\n");
        out.push_str("redis_version:mini-redis-rs-0.1.0\r\n");
        out.push_str("redis_mode:standalone\r\n");
        out.push_str("arch_bits:64\r\n");
        out.push_str(&format!("process_id:{}\r\n", std::process::id()));
        if let Some(state) = state {
            let uptime_ms = (crate::db::now_millis() as u64).saturating_sub(state.started_at_ms);
            out.push_str(&format!("uptime_in_seconds:{}\r\n", uptime_ms / 1000));
        }
        out.push_str("\r\n");
    }

    if include("clients") {
        out.push_str("# Clients\r\n");
        if let Some(state) = state {
            out.push_str(&format!(
                "connected_clients:{}\r\n",
                state.connected_clients.load(Ordering::SeqCst)
            ));
            out.push_str(&format!(
                "total_connections_received:{}\r\n",
                state.total_connections.load(Ordering::SeqCst)
            ));
        } else {
            out.push_str("connected_clients:0\r\n");
            out.push_str("total_connections_received:0\r\n");
        }
        out.push_str("\r\n");
    }

    if include("memory") {
        let stats = db.stats();
        out.push_str("# Memory\r\n");
        out.push_str(&format!("used_memory:{}\r\n", stats.approx_bytes));
        out.push_str(&format!("db_keys:{}\r\n", stats.keys));
        out.push_str(&format!("db_expiring_keys:{}\r\n", stats.expiring_keys));
        out.push_str(&format!("string_keys:{}\r\n", stats.strings));
        out.push_str(&format!("list_keys:{}\r\n", stats.lists));
        out.push_str(&format!("hash_keys:{}\r\n", stats.hashes));
        out.push_str(&format!("list_items:{}\r\n", stats.list_items));
        out.push_str(&format!("hash_fields:{}\r\n", stats.hash_fields));
        out.push_str("\r\n");
    }

    if include("persistence") {
        out.push_str("# Persistence\r\n");
        match aof {
            Some(aof) => {
                let stats = aof.stats();
                out.push_str("aof_enabled:1\r\n");
                out.push_str(&format!("aof_current_size:{}\r\n", stats.current_size));
                out.push_str(&format!("aof_fsync:{}\r\n", stats.fsync_policy.as_str()));
                out.push_str(&format!(
                    "aof_rewrite_in_progress:{}\r\n",
                    usize::from(stats.rewrite_in_progress)
                ));
                out.push_str(&format!("aof_rewrites:{}\r\n", stats.rewrite_count));
                out.push_str(&format!(
                    "aof_last_rewrite_status:{}\r\n",
                    stats.last_rewrite_status
                ));
                out.push_str(&format!(
                    "aof_last_rewrite_time_ms:{}\r\n",
                    stats.last_rewrite_finished_ms
                ));
                if let Some(err) = stats.last_error {
                    out.push_str(&format!("aof_last_error:{}\r\n", sanitize_info_value(&err)));
                }
                out.push_str(&format!(
                    "aof_filename:{}\r\n",
                    sanitize_info_value(&stats.path.display().to_string())
                ));
            }
            None => {
                out.push_str("aof_enabled:0\r\n");
                out.push_str("aof_rewrite_in_progress:0\r\n");
            }
        }
        out.push_str("\r\n");
    }

    if out.is_empty() {
        out.push_str("# Error\r\nunsupported_info_section:1\r\n\r\n");
    }

    Frame::Bulk(Bytes::from(out))
}

fn sanitize_info_value(value: &str) -> String {
    value
        .chars()
        .map(|c| if c == '\r' || c == '\n' { ' ' } else { c })
        .collect()
}

async fn run_subscribed(
    conn: &mut Connection,
    db: &Db,
    initial_channels: Vec<Bytes>,
) -> anyhow::Result<()> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<(Bytes, Bytes)>();
    let mut tasks: HashMap<Bytes, tokio::task::JoinHandle<()>> = HashMap::new();

    for ch in initial_channels {
        subscribe_one(&mut tasks, &msg_tx, db, ch.clone());
        ack_subscribe(conn, b"subscribe", &ch, tasks.len()).await?;
    }

    loop {
        tokio::select! {
            frame_res = conn.read_frame() => {
                let frame = match frame_res? {
                    None => break,
                    Some(f) => f,
                };
                match Command::from_frame(frame) {
                    Ok(Command::Subscribe(chs)) => {
                        for ch in chs {
                            subscribe_one(&mut tasks, &msg_tx, db, ch.clone());
                            ack_subscribe(conn, b"subscribe", &ch, tasks.len()).await?;
                        }
                    }
                    Ok(Command::Unsubscribe(chs_opt)) => {
                        let chs = chs_opt.unwrap_or_else(|| tasks.keys().cloned().collect());
                        if chs.is_empty() {
                            conn.write_frame(&Frame::Array(vec![
                                Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                                Frame::Null,
                                Frame::Integer(0),
                            ])).await?;
                            break;
                        }
                        for ch in chs {
                            if let Some(h) = tasks.remove(&ch) {
                                h.abort();
                            }
                            ack_subscribe(conn, b"unsubscribe", &ch, tasks.len()).await?;
                        }
                        if tasks.is_empty() {
                            break;
                        }
                    }
                    Ok(Command::Ping(msg)) => {
                        let payload = msg.unwrap_or_default();
                        conn.write_frame(&Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"pong")),
                            Frame::Bulk(payload),
                        ])).await?;
                    }
                    Ok(Command::Publish(ch, msg)) => {
                        let n = db.pubsub_publish(&ch, msg);
                        conn.write_frame(&Frame::Integer(n as i64)).await?;
                    }
                    Ok(_) => {
                        conn.write_frame(&Frame::Error(
                            "ERR Can't execute command in subscribed context".into(),
                        )).await?;
                    }
                    Err(e) => {
                        conn.write_frame(&Frame::Error(format!("ERR {}", e))).await?;
                    }
                }
            }
            recv = msg_rx.recv() => {
                let Some((channel, message)) = recv else { break; };
                conn.write_frame(&Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"message")),
                    Frame::Bulk(channel),
                    Frame::Bulk(message),
                ])).await?;
            }
        }
    }

    let channels: Vec<Bytes> = tasks.keys().cloned().collect();
    for (_, h) in tasks {
        h.abort();
    }
    // Yield once so the aborted forwarder tasks actually drop their broadcast::Receivers
    // before we try to GC the channels (best-effort; pubsub_publish also GCs lazily).
    tokio::task::yield_now().await;
    for ch in &channels {
        db.pubsub_gc(ch);
    }
    Ok(())
}

fn subscribe_one(
    tasks: &mut HashMap<Bytes, tokio::task::JoinHandle<()>>,
    msg_tx: &mpsc::UnboundedSender<(Bytes, Bytes)>,
    db: &Db,
    channel: Bytes,
) {
    if tasks.contains_key(&channel) {
        return;
    }
    let mut rx = db.pubsub_subscribe(channel.clone());
    let tx = msg_tx.clone();
    let ch_for_task = channel.clone();
    let h = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    if tx.send((ch_for_task.clone(), msg)).is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    tasks.insert(channel, h);
}

async fn ack_subscribe(
    conn: &mut Connection,
    tag: &'static [u8],
    channel: &Bytes,
    count: usize,
) -> anyhow::Result<()> {
    conn.write_frame(&Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(tag)),
        Frame::Bulk(channel.clone()),
        Frame::Integer(count as i64),
    ]))
    .await
}
