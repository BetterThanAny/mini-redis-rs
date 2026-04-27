use clap::Parser;
use mini_redis_rs::aof::{self, FsyncPolicy};
use mini_redis_rs::db::Db;
use mini_redis_rs::server;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio::signal;

#[derive(Parser, Debug)]
#[command(version, about = "miniredisd: a tiny Redis-compatible server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6380)]
    port: u16,
    /// Path to append-only file. If set, durability is enabled.
    #[arg(long)]
    aof: Option<PathBuf>,
    /// fsync policy: always | everysec | no
    #[arg(long, default_value = "everysec")]
    aof_fsync: FsyncPolicy,
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
    let db = Db::new();

    let aof_handle = if let Some(path) = args.aof.as_ref() {
        let count = aof::replay(path, &db).await?;
        tracing::info!(replayed = count, "AOF replay complete");
        Some(aof::spawn_writer(path.clone(), args.aof_fsync).await?)
    } else {
        None
    };

    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "miniredisd listening");

    server::run_with_options(listener, db, aof_handle, signal::ctrl_c()).await
}
