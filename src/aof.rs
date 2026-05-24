use crate::cmd::Command;
use crate::db::{Db, Entry, ExpireAt, Value};
use crate::resp::{encoder, parser, Frame};
use bytes::{Bytes, BytesMut};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

const MAX_REWRITE_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            FsyncPolicy::Always => "always",
            FsyncPolicy::EverySec => "everysec",
            FsyncPolicy::No => "no",
        }
    }
}

impl std::str::FromStr for FsyncPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Ok(FsyncPolicy::Always),
            "everysec" => Ok(FsyncPolicy::EverySec),
            "no" => Ok(FsyncPolicy::No),
            other => Err(format!("invalid fsync policy: {other}")),
        }
    }
}

#[derive(Default)]
struct AofStatsInner {
    current_size: AtomicU64,
    rewrite_in_progress: AtomicBool,
    rewrite_count: AtomicU64,
    last_rewrite_ok: AtomicBool,
    last_rewrite_finished_ms: AtomicU64,
    write_failed: AtomicBool,
    last_error: Mutex<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct AofStats {
    pub enabled: bool,
    pub path: PathBuf,
    pub fsync_policy: FsyncPolicy,
    pub current_size: u64,
    pub rewrite_in_progress: bool,
    pub rewrite_count: u64,
    pub last_rewrite_status: &'static str,
    pub last_rewrite_finished_ms: u64,
    pub write_failed: bool,
    pub last_error: Option<String>,
}

enum AofMessage {
    Write {
        bytes: Bytes,
        ack: oneshot::Sender<anyhow::Result<()>>,
    },
    StartRewrite {
        ack: oneshot::Sender<anyhow::Result<()>>,
    },
    FinishRewrite {
        temp_path: PathBuf,
        ack: oneshot::Sender<anyhow::Result<()>>,
    },
    AbortRewrite {
        reason: String,
        ack: oneshot::Sender<()>,
    },
}

struct RewriteBuffer {
    frames: Vec<Bytes>,
    bytes: usize,
}

impl RewriteBuffer {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            bytes: 0,
        }
    }

    fn push(&mut self, bytes: Bytes) -> anyhow::Result<()> {
        let new_size = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("AOF rewrite buffer size overflow"))?;
        if new_size > MAX_REWRITE_BUFFER_BYTES {
            anyhow::bail!("AOF rewrite buffer exceeded limit of {MAX_REWRITE_BUFFER_BYTES} bytes");
        }
        self.bytes = new_size;
        self.frames.push(bytes);
        Ok(())
    }
}

#[derive(Clone)]
pub struct AofHandle {
    path: PathBuf,
    policy: FsyncPolicy,
    sender: mpsc::Sender<AofMessage>,
    stats: Arc<AofStatsInner>,
}

impl AofHandle {
    pub async fn write(&self, frame: &Frame) -> anyhow::Result<()> {
        if self.stats.write_failed.load(Ordering::SeqCst) {
            anyhow::bail!("AOF writer is in failed state");
        }
        let mut buf = BytesMut::new();
        encoder::encode(frame, &mut buf);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AofMessage::Write {
                bytes: buf.freeze(),
                ack: tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("AOF writer is closed"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("AOF writer stopped during write"))?
    }

    pub fn schedule_rewrite(&self, db: Db) -> Result<(), &'static str> {
        if self
            .stats
            .rewrite_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("already in progress");
        }

        let handle = self.clone();
        tokio::spawn(async move {
            if let Err(err) = rewrite_inner(handle.clone(), db).await {
                handle.finish_rewrite_stats(false, Some(err.to_string()));
                tracing::warn!(error = %err, "AOF rewrite failed");
            }
        });
        Ok(())
    }

    pub async fn rewrite_now(&self, db: Db) -> anyhow::Result<()> {
        if self
            .stats
            .rewrite_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            anyhow::bail!("AOF rewrite already in progress");
        }
        match rewrite_inner(self.clone(), db).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.finish_rewrite_stats(false, Some(err.to_string()));
                Err(err)
            }
        }
    }

    pub fn stats(&self) -> AofStats {
        let last_error = self.stats.last_error.lock().unwrap().clone();
        let rewrite_in_progress = self.stats.rewrite_in_progress.load(Ordering::SeqCst);
        let rewrite_count = self.stats.rewrite_count.load(Ordering::SeqCst);
        let last_rewrite_status = if rewrite_count == 0 {
            "none"
        } else if self.stats.last_rewrite_ok.load(Ordering::SeqCst) {
            "ok"
        } else {
            "err"
        };
        AofStats {
            enabled: true,
            path: self.path.clone(),
            fsync_policy: self.policy,
            current_size: self.stats.current_size.load(Ordering::SeqCst),
            rewrite_in_progress,
            rewrite_count,
            last_rewrite_status,
            last_rewrite_finished_ms: self.stats.last_rewrite_finished_ms.load(Ordering::SeqCst),
            write_failed: self.stats.write_failed.load(Ordering::SeqCst),
            last_error,
        }
    }

    fn finish_rewrite_stats(&self, ok: bool, err: Option<String>) {
        self.stats.last_rewrite_ok.store(ok, Ordering::SeqCst);
        self.stats
            .last_rewrite_finished_ms
            .store(crate::db::now_millis() as u64, Ordering::SeqCst);
        self.stats.rewrite_count.fetch_add(1, Ordering::SeqCst);
        *self.stats.last_error.lock().unwrap() = err;
        self.stats
            .rewrite_in_progress
            .store(false, Ordering::SeqCst);
    }

    fn mark_write_failed(stats: &Arc<AofStatsInner>, err: impl ToString) {
        stats.write_failed.store(true, Ordering::SeqCst);
        *stats.last_error.lock().unwrap() = Some(err.to_string());
    }

    async fn start_rewrite_buffering(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AofMessage::StartRewrite { ack: tx })
            .await
            .map_err(|_| anyhow::anyhow!("AOF writer is closed"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("AOF writer stopped during rewrite start"))?
    }

    async fn finish_rewrite(&self, temp_path: PathBuf) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AofMessage::FinishRewrite { temp_path, ack: tx })
            .await
            .map_err(|_| anyhow::anyhow!("AOF writer is closed"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("AOF writer stopped during rewrite finish"))?
    }

    async fn abort_rewrite(&self, reason: impl Into<String>) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(AofMessage::AbortRewrite {
                reason: reason.into(),
                ack: tx,
            })
            .await;
        let _ = rx.await;
    }
}

/// Read the AOF file (if it exists) and replay all commands into the given Db.
/// Must be called BEFORE the writer task is started, so replay doesn't get re-written.
pub async fn replay(path: &Path, db: &Db) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut f = File::open(path).await?;
    let original_len = f.metadata().await.map(|m| m.len()).unwrap_or(0);
    let mut buf = BytesMut::with_capacity(8192);
    let mut applied = 0u64;
    let mut total_read = 0u64;
    let mut valid_len = 0u64;
    let mut truncate_to: Option<u64> = None;
    let mut eof = false;
    loop {
        while !buf.is_empty() {
            let frame_start = total_read - buf.len() as u64;
            // Tolerate corruption in the tail: warn and stop, do NOT propagate the error
            // (otherwise the server can never restart on a partially-trashed AOF).
            match parser::parse(&mut buf) {
                Ok(None) => break,
                Ok(Some(frame)) => match Command::from_frame(frame) {
                    Ok(cmd) => {
                        let resp = cmd.apply(db);
                        if let Frame::Error(err) = resp {
                            tracing::warn!(
                                error = %err,
                                offset = frame_start,
                                "command failed during AOF replay; truncating to valid prefix"
                            );
                            truncate_to = Some(frame_start);
                            break;
                        }
                        applied += 1;
                        valid_len = total_read - buf.len() as u64;
                    }
                    Err(e) => {
                        tracing::warn!(
                            ?e,
                            offset = frame_start,
                            "bad command frame during AOF replay; truncating to valid prefix"
                        );
                        truncate_to = Some(frame_start);
                        break;
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        ?e,
                        offset = frame_start,
                        remaining = buf.len(),
                        "AOF parse error; stopping replay"
                    );
                    truncate_to = Some(valid_len);
                    break;
                }
            }
        }

        if truncate_to.is_some() {
            break;
        }
        if eof {
            if !buf.is_empty() {
                tracing::warn!(
                    remaining = buf.len(),
                    "AOF truncated mid-frame; stopping replay"
                );
                truncate_to = Some(valid_len);
            }
            break;
        }

        let n = f.read_buf(&mut buf).await?;
        if n == 0 {
            eof = true;
        } else {
            total_read += n as u64;
        }
    }
    if let Some(len) = truncate_to {
        if len < original_len {
            truncate_aof(path, len).await?;
            tracing::warn!(
                ?path,
                original_len,
                truncated_len = len,
                "AOF truncated to replayable prefix"
            );
        }
    }
    tracing::info!(applied, ?path, "AOF replay done");
    Ok(applied)
}

/// Spawn the writer task. Returns an AofHandle for sending writes.
pub async fn spawn_writer(path: PathBuf, policy: FsyncPolicy) -> anyhow::Result<AofHandle> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    let current_size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let (tx, rx) = mpsc::channel::<AofMessage>(1024);
    let stats = Arc::new(AofStatsInner::default());
    stats.current_size.store(current_size, Ordering::SeqCst);
    tokio::spawn(run_writer(path.clone(), file, rx, policy, stats.clone()));
    Ok(AofHandle {
        path,
        policy,
        sender: tx,
        stats,
    })
}

async fn rewrite_inner(handle: AofHandle, db: Db) -> anyhow::Result<()> {
    let temp_path = rewrite_temp_path(&handle.path);
    let write_pause = db.pause_writes().await;
    handle.start_rewrite_buffering().await?;
    let write_result = write_snapshot_from_db(&temp_path, &db).await;
    drop(write_pause);

    if let Err(err) = write_result {
        handle.abort_rewrite(err.to_string()).await;
        let _ = fs::remove_file(&temp_path).await;
        return Err(err);
    }

    if let Err(err) = handle.finish_rewrite(temp_path.clone()).await {
        let _ = fs::remove_file(&temp_path).await;
        return Err(err);
    }
    handle.finish_rewrite_stats(true, None);
    Ok(())
}

async fn write_snapshot_from_db(path: &Path, db: &Db) -> anyhow::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?;
    let mut buf = BytesMut::new();
    let now = crate::db::now_millis();
    for shard_mu in db.iter_shards() {
        let frames = {
            let shard = shard_mu.lock().unwrap();
            let mut entries: Vec<(Bytes, Entry)> = shard
                .entries
                .iter()
                .filter(|(_, entry)| !entry.expires_at.is_some_and(|deadline| deadline <= now))
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect();
            entries.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
            entries
                .into_iter()
                .flat_map(snapshot_frames_for_entry)
                .collect::<Vec<_>>()
        };

        for frame in frames {
            buf.clear();
            encoder::encode(&frame, &mut buf);
            file.write_all(&buf).await?;
        }
    }
    file.sync_data().await?;
    Ok(())
}

fn snapshot_frames_for_entry((key, entry): (Bytes, Entry)) -> Vec<Frame> {
    match entry.value {
        Value::String(value) => {
            let mut parts = vec![bulk_static(b"SET"), Frame::Bulk(key), Frame::Bulk(value)];
            if let Some(deadline) = entry.expires_at {
                parts.push(bulk_static(b"PXAT"));
                parts.push(bulk_string(deadline));
            }
            vec![Frame::Array(parts)]
        }
        Value::List(values) if !values.is_empty() => {
            let mut parts = Vec::with_capacity(values.len() + 2);
            parts.push(bulk_static(b"RPUSH"));
            parts.push(Frame::Bulk(key.clone()));
            parts.extend(values.into_iter().map(Frame::Bulk));
            let mut frames = vec![Frame::Array(parts)];
            if let Some(deadline) = entry.expires_at {
                frames.push(pexpireat_frame(key, deadline));
            }
            frames
        }
        Value::Hash(values) if !values.is_empty() => {
            let mut fields: Vec<_> = values.into_iter().collect();
            fields.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
            let mut parts = Vec::with_capacity(fields.len() * 2 + 2);
            parts.push(bulk_static(b"HSET"));
            parts.push(Frame::Bulk(key.clone()));
            for (field, value) in fields {
                parts.push(Frame::Bulk(field));
                parts.push(Frame::Bulk(value));
            }
            let mut frames = vec![Frame::Array(parts)];
            if let Some(deadline) = entry.expires_at {
                frames.push(pexpireat_frame(key, deadline));
            }
            frames
        }
        Value::List(_) | Value::Hash(_) => Vec::new(),
    }
}

fn bulk_static(bytes: &'static [u8]) -> Frame {
    Frame::Bulk(Bytes::from_static(bytes))
}

fn bulk_string(value: impl ToString) -> Frame {
    Frame::Bulk(Bytes::from(value.to_string()))
}

fn pexpireat_frame(key: Bytes, deadline: ExpireAt) -> Frame {
    Frame::Array(vec![
        bulk_static(b"PEXPIREAT"),
        Frame::Bulk(key),
        bulk_string(deadline),
    ])
}

async fn run_writer(
    path: PathBuf,
    mut file: File,
    mut rx: mpsc::Receiver<AofMessage>,
    policy: FsyncPolicy,
    stats: Arc<AofStatsInner>,
) {
    let mut last_sync = tokio::time::Instant::now();
    let sync_interval = std::time::Duration::from_secs(1);
    let mut rewrite_buffer: Option<RewriteBuffer> = None;

    loop {
        let next = if policy == FsyncPolicy::EverySec {
            tokio::select! {
                msg = rx.recv() => msg,
                _ = tokio::time::sleep_until(last_sync + sync_interval) => {
                    if let Err(e) = file.sync_data().await {
                        AofHandle::mark_write_failed(&stats, &e);
                        tracing::warn!(error = %e, "AOF fsync failed");
                    }
                    last_sync = tokio::time::Instant::now();
                    continue;
                }
            }
        } else {
            rx.recv().await
        };

        let Some(message) = next else {
            let _ = file.sync_data().await;
            return;
        };

        match message {
            AofMessage::Write { bytes, ack } => {
                if stats.write_failed.load(Ordering::SeqCst) {
                    let _ = ack.send(Err(anyhow::anyhow!("AOF writer is in failed state")));
                    continue;
                }
                let result = async {
                    let previous_len = stats.current_size.load(Ordering::SeqCst);
                    if let Err(err) = write_one(&mut file, &stats, &bytes, previous_len).await {
                        let _ = rollback_active_file(&mut file, &stats, previous_len).await;
                        return Err(err);
                    }
                    if let Err(err) = sync_after_write(&mut file, policy, &mut last_sync).await {
                        let _ = rollback_active_file(&mut file, &stats, previous_len).await;
                        return Err(err);
                    }
                    Ok(())
                }
                .await;
                if result.is_ok() {
                    if let Some(buffer) = rewrite_buffer.as_mut() {
                        if let Err(e) = buffer.push(bytes) {
                            rewrite_buffer = None;
                            *stats.last_error.lock().unwrap() = Some(e.to_string());
                            tracing::warn!(
                                error = %e,
                                "aborting AOF rewrite after buffer limit was exceeded"
                            );
                        }
                    }
                } else if let Err(e) = &result {
                    AofHandle::mark_write_failed(&stats, e);
                    tracing::error!(error = %e, "AOF write/fsync failed");
                }
                let _ = ack.send(result);
            }
            AofMessage::StartRewrite { ack } => {
                let result = if rewrite_buffer.is_some() {
                    Err(anyhow::anyhow!("AOF rewrite already in progress"))
                } else {
                    rewrite_buffer = Some(RewriteBuffer::new());
                    Ok(())
                };
                let _ = ack.send(result);
            }
            AofMessage::FinishRewrite { temp_path, ack } => {
                let result = finish_rewrite_file(
                    &path,
                    &mut file,
                    &mut rewrite_buffer,
                    &temp_path,
                    policy,
                    &stats,
                )
                .await;
                if result.is_ok() {
                    last_sync = tokio::time::Instant::now();
                }
                let _ = ack.send(result);
            }
            AofMessage::AbortRewrite { reason, ack } => {
                rewrite_buffer = None;
                *stats.last_error.lock().unwrap() = Some(reason);
                let _ = ack.send(());
            }
        }
    }
}

async fn write_one(
    file: &mut File,
    stats: &Arc<AofStatsInner>,
    bytes: &Bytes,
    previous_len: u64,
) -> anyhow::Result<()> {
    file.write_all(bytes).await?;
    stats
        .current_size
        .store(previous_len + bytes.len() as u64, Ordering::SeqCst);
    Ok(())
}

async fn rollback_active_file(
    file: &mut File,
    stats: &Arc<AofStatsInner>,
    previous_len: u64,
) -> anyhow::Result<()> {
    file.set_len(previous_len).await?;
    let _ = file.sync_data().await;
    stats.current_size.store(previous_len, Ordering::SeqCst);
    Ok(())
}

async fn sync_after_write(
    file: &mut File,
    policy: FsyncPolicy,
    last_sync: &mut tokio::time::Instant,
) -> anyhow::Result<()> {
    match policy {
        FsyncPolicy::Always => {
            file.sync_data().await?;
            *last_sync = tokio::time::Instant::now();
        }
        FsyncPolicy::EverySec => {
            if last_sync.elapsed() >= std::time::Duration::from_secs(1) {
                file.sync_data().await?;
                *last_sync = tokio::time::Instant::now();
            }
        }
        FsyncPolicy::No => {}
    }
    Ok(())
}

async fn finish_rewrite_file(
    path: &Path,
    active_file: &mut File,
    rewrite_buffer: &mut Option<RewriteBuffer>,
    temp_path: &Path,
    policy: FsyncPolicy,
    stats: &Arc<AofStatsInner>,
) -> anyhow::Result<()> {
    let Some(buffer) = rewrite_buffer.take() else {
        anyhow::bail!("AOF rewrite is not in progress");
    };

    let mut temp = OpenOptions::new().append(true).open(temp_path).await?;
    for bytes in &buffer.frames {
        temp.write_all(bytes).await?;
    }
    temp.sync_data().await?;
    drop(temp);

    fs::rename(temp_path, path).await?;
    let parent_sync = sync_parent_dir(path).await;
    *active_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    if policy == FsyncPolicy::Always {
        active_file.sync_data().await?;
    }
    let len = active_file.metadata().await.map(|m| m.len()).unwrap_or(0);
    stats.current_size.store(len, Ordering::SeqCst);
    parent_sync?;
    Ok(())
}

async fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(dir)?;
        file.sync_all()
    })
    .await
    .unwrap_or_else(|join_err| Err(std::io::Error::other(join_err)))
}

fn rewrite_temp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| "appendonly.aof".into());
    name.push(".rewrite");
    path.with_file_name(name)
}

async fn truncate_aof(path: &Path, len: u64) -> anyhow::Result<()> {
    let file = OpenOptions::new().write(true).open(path).await?;
    file.set_len(len).await?;
    file.sync_data().await?;
    Ok(())
}
