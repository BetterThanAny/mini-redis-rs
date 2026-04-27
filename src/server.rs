use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::Db;
use crate::resp::Frame;
use std::future::Future;
use tokio::net::TcpListener;

pub async fn run(listener: TcpListener, shutdown: impl Future) -> anyhow::Result<()> {
    let db = Db::new();
    tokio::spawn(crate::db::expire::run_sweeper(db.clone()));
    tokio::select! {
        res = accept_loop(listener, db) => res,
        _ = shutdown => {
            tracing::info!("shutdown signal received");
            Ok(())
        }
    }
}

async fn accept_loop(listener: TcpListener, db: Db) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let db = db.clone();
        tracing::debug!(?peer, "accepted");
        tokio::spawn(async move {
            if let Err(e) = handle(socket, db).await {
                tracing::warn!(?peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(socket: tokio::net::TcpStream, db: Db) -> anyhow::Result<()> {
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
