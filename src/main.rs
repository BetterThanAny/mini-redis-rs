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
