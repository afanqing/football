use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use football_pipeline::{normalize_payload, validate_unique_matches, write_silver_parquet};

#[derive(Debug, Parser)]
#[command(
    name = "football-pipeline",
    about = "Bronze to Silver football pipeline"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Normalize {
        #[arg(long)]
        input: std::path::PathBuf,
        #[arg(long)]
        output: std::path::PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Normalize { input, output } => {
            let payload = std::fs::read(&input)
                .with_context(|| format!("read Bronze payload {}", input.display()))?;
            let matches = normalize_payload(&payload)?;
            validate_unique_matches(&matches)?;
            write_silver_parquet(&output, &matches)?;
            println!(
                "wrote {} Silver matches to {}",
                matches.len(),
                output.display()
            );
        }
    }
    Ok(())
}
