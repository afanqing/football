use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use football_ingest::{FootballDataClient, SnapshotStore};

#[derive(Debug, Parser)]
#[command(name = "football-ingest", about = "Football data ingestion tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fetch competition matches and append the raw response to Bronze storage.
    Matches {
        #[arg(long)]
        competition: String,
        #[arg(long)]
        date_from: Option<NaiveDate>,
        #[arg(long)]
        date_to: Option<NaiveDate>,
        #[arg(long, default_value = "data/bronze")]
        output: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Matches {
            competition,
            date_from,
            date_to,
            output,
        } => {
            let token = std::env::var("FOOTBALL_DATA_TOKEN")
                .context("set FOOTBALL_DATA_TOKEN before fetching matches")?;
            let mut client = FootballDataClient::new(token)?;
            let capture = client
                .fetch_matches(&competition, date_from, date_to)
                .await?;
            let match_count = capture.match_count;
            let manifest = SnapshotStore::new(output).write_matches(capture)?;
            println!(
                "captured {match_count} matches; sha256={} bytes={} captured_at={}",
                manifest.payload_sha256, manifest.payload_bytes, manifest.captured_at
            );
        }
    }
    Ok(())
}
