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
