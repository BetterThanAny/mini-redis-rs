use crate::cmd::Command;
use crate::db::Db;
use crate::resp::{encoder, parser, Frame};
use bytes::{Bytes, BytesMut};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
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

#[derive(Clone)]
pub struct AofHandle {
    sender: mpsc::UnboundedSender<Bytes>,
}

impl AofHandle {
    pub fn write(&self, frame: &Frame) {
        let mut buf = BytesMut::new();
        encoder::encode(frame, &mut buf);
        let _ = self.sender.send(buf.freeze());
    }
}

/// Read the AOF file (if it exists) and replay all commands into the given Db.
/// Must be called BEFORE the writer task is started, so replay doesn't get re-written.
pub async fn replay(path: &Path, db: &Db) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut f = File::open(path).await?;
    let mut all = Vec::new();
    f.read_to_end(&mut all).await?;

    let mut buf = BytesMut::from(&all[..]);
    let mut applied = 0u64;
    while !buf.is_empty() {
        // Tolerate corruption in the tail: warn and stop, do NOT propagate the error
        // (otherwise the server can never restart on a partially-trashed AOF).
        match parser::parse(&mut buf) {
            Ok(None) => {
                tracing::warn!(
                    remaining = buf.len(),
                    "AOF truncated mid-frame; stopping replay"
                );
                break;
            }
            Ok(Some(frame)) => match Command::from_frame(frame) {
                Ok(cmd) => {
                    let _ = cmd.apply(db);
                    applied += 1;
                }
                Err(e) => {
                    tracing::warn!(?e, "skipping bad frame during AOF replay");
                }
            },
            Err(e) => {
                tracing::warn!(
                    ?e,
                    remaining = buf.len(),
                    "AOF parse error; stopping replay"
                );
                break;
            }
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
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    tokio::spawn(run_writer(file, rx, policy));
    Ok(AofHandle { sender: tx })
}

async fn run_writer(mut file: File, mut rx: mpsc::UnboundedReceiver<Bytes>, policy: FsyncPolicy) {
    let mut last_sync = tokio::time::Instant::now();
    let sync_interval = std::time::Duration::from_secs(1);

    loop {
        let next = if policy == FsyncPolicy::EverySec {
            tokio::select! {
                msg = rx.recv() => msg,
                _ = tokio::time::sleep_until(last_sync + sync_interval) => {
                    if let Err(e) = file.sync_data().await {
                        tracing::warn!(error = %e, "AOF fsync failed");
                    }
                    last_sync = tokio::time::Instant::now();
                    continue;
                }
            }
        } else {
            rx.recv().await
        };
        let bytes = match next {
            Some(b) => b,
            None => {
                let _ = file.sync_data().await;
                return;
            }
        };
        if let Err(e) = file.write_all(&bytes).await {
            tracing::error!(error = %e, "AOF write failed");
            continue;
        }
        match policy {
            FsyncPolicy::Always => {
                if let Err(e) = file.sync_data().await {
                    tracing::warn!(error = %e, "AOF fsync failed");
                }
                last_sync = tokio::time::Instant::now();
            }
            FsyncPolicy::EverySec => {
                if last_sync.elapsed() >= sync_interval {
                    let _ = file.sync_data().await;
                    last_sync = tokio::time::Instant::now();
                }
            }
            FsyncPolicy::No => {}
        }
    }
}
