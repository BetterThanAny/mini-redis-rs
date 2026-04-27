use crate::aof::AofHandle;
use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::Db;
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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
    tokio::select! {
        res = accept_loop(listener, db, aof) => res,
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
) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let db = db.clone();
        let aof = aof.clone();
        tracing::debug!(?peer, "accepted");
        tokio::spawn(async move {
            if let Err(e) = handle(socket, db, aof).await {
                tracing::warn!(?peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(
    socket: tokio::net::TcpStream,
    db: Db,
    aof: Option<AofHandle>,
) -> anyhow::Result<()> {
    let mut conn = Connection::new(socket);
    while let Some(frame) = conn.read_frame().await? {
        let frame_for_aof = frame.clone();
        match Command::from_frame(frame) {
            Ok(Command::Subscribe(channels)) => {
                run_subscribed(&mut conn, &db, channels).await?;
            }
            Ok(cmd) => {
                let is_write = cmd.is_write();
                let resp = cmd.apply(&db);
                conn.write_frame(&resp).await?;
                if is_write {
                    if let Some(a) = &aof {
                        a.write(&frame_for_aof);
                    }
                }
            }
            Err(e) => {
                conn.write_frame(&Frame::Error(format!("ERR {}", e))).await?;
            }
        }
    }
    Ok(())
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
