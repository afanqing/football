use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use football_contracts::ThreeWayOddsSnapshot;
use football_odds::normalize_three_way;

#[derive(Debug, Parser)]
#[command(name = "football-odds", about = "Normalize captured three-way odds")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Normalize { input: PathBuf, output: PathBuf },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Normalize { input, output } => {
            let bytes = fs::read(&input).with_context(|| format!("read {}", input.display()))?;
            let snapshot: ThreeWayOddsSnapshot = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode odds snapshot {}", input.display()))?;
            let market = normalize_three_way(&snapshot)?;
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create output directory {}", parent.display()))?;
            }
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("create immutable output {}", output.display()))?;
            serde_json::to_writer_pretty(file, &market)
                .context("write normalized market probability")?;
            println!(
                "market probabilities saved; overround={:.6} snapshot_sha256={}",
                market.overround, market.snapshot_sha256
            );
        }
    }
    Ok(())
}
