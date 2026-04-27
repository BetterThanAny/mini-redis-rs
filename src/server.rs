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
