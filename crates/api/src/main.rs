use anyhow::{Context, Result};
use clap::Parser;
use football_api::router;
use football_ledger::connect;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "football-api", about = "Read-only football decision API")]
struct Cli {}

#[tokio::main]
async fn main() -> Result<()> {
    Cli::parse();
    let database_url = std::env::var("DATABASE_URL")
        .context("set DATABASE_URL before starting the read-only API")?;
    let bind_address = std::env::var("API_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let pool = connect(&database_url).await?;
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("bind API listener at {bind_address}"))?;
    axum::serve(listener, router(pool))
        .await
        .context("serve read-only football API")?;
    Ok(())
}
