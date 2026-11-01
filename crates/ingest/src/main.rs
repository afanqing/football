use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use dagster_pipes_rust::{PipesMetadataValue, open_dagster_pipes};
use football_ingest::{FootballDataClient, SnapshotStore, capture_sporttery_resource};
use std::collections::HashMap;

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
    /// Capture one Sporttery resource unchanged from the local network.
    Sporttery {
        #[arg(long, env = "SPORTTERY_ODDS_URL")]
        url: String,
        #[arg(long)]
        resource: String,
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
        Command::Sporttery {
            url,
            resource,
            output,
        } => {
            let manifest =
                capture_sporttery_resource(&url, &resource, &SnapshotStore::new(output)).await?;
            println!(
                "captured resource={} status={} bytes={} sha256={} captured_at={}",
                manifest.resource,
                manifest.http_status,
                manifest.payload_bytes,
                manifest.payload_sha256,
                manifest.captured_at
            );
            if !(200..300).contains(&manifest.http_status) {
                bail!(
                    "Sporttery returned HTTP {}; raw response was preserved",
                    manifest.http_status
                );
            }
            if std::env::var_os("DAGSTER_PIPES_CONTEXT").is_some() {
                let mut pipes = open_dagster_pipes().context("open Dagster Pipes context")?;
                pipes
                    .report_asset_materialization(
                        "sporttery_bronze",
                        HashMap::from([
                            ("source", PipesMetadataValue::from(manifest.source.clone())),
                            (
                                "resource",
                                PipesMetadataValue::from(manifest.resource.clone()),
                            ),
                            (
                                "http_status",
                                PipesMetadataValue::from(i64::from(manifest.http_status)),
                            ),
                            (
                                "payload_bytes",
                                PipesMetadataValue::from(
                                    i64::try_from(manifest.payload_bytes).unwrap_or(i64::MAX),
                                ),
                            ),
                            (
                                "payload_sha256",
                                PipesMetadataValue::from(manifest.payload_sha256.clone()),
                            ),
                        ]),
                        Some(&manifest.payload_sha256),
                    )
                    .context("report Sporttery Bronze materialization to Dagster")?;
            }
        }
    }
    Ok(())
}
