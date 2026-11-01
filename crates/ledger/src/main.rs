use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use football_ledger::{
    AppendDecision, AppendSettlement, append_decision, append_settlement_event, connect, migrate,
};

#[derive(Debug, Parser)]
#[command(
    name = "football-ledger",
    about = "Append and inspect decision ledger data"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Migrate,
    Append { input: PathBuf },
    Settle { input: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    let database_url = std::env::var("DATABASE_URL")
        .context("set DATABASE_URL before using the decision ledger")?;
    let pool = connect(&database_url).await?;

    match command {
        Command::Migrate => {
            migrate(&pool).await?;
            println!("decision ledger migrations are current");
        }
        Command::Append { input } => {
            let request: AppendDecision = read_json(&input)?;
            let record = append_decision(&pool, &request).await?;
            println!(
                "decision_id={} idempotency_key={}",
                record.id, record.idempotency_key
            );
        }
        Command::Settle { input } => {
            let request: AppendSettlement = read_json(&input)?;
            let event = append_settlement_event(
                &pool,
                request.decision_id,
                &request.idempotency_key,
                &request.event_type,
                request.result,
                request.settled_at,
            )
            .await?;
            println!(
                "settlement_event_id={} decision_id={}",
                event.id, event.decision_id
            );
        }
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<T> {
    let contents = fs::read(path).with_context(|| format!("read input {}", path.display()))?;
    serde_json::from_slice(&contents)
        .with_context(|| format!("decode JSON input {}", path.display()))
}
