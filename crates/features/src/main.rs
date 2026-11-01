use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use football_features::{generate_features, write_features_parquet};
use football_pipeline::{read_silver_parquet, validate_unique_matches};

#[derive(Debug, Parser)]
#[command(
    name = "football-features",
    about = "Generate point-in-time Gold features"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Build {
        #[arg(long)]
        silver: std::path::PathBuf,
        #[arg(long)]
        output: std::path::PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Build { silver, output } => {
            let matches = read_silver_parquet(&silver)
                .with_context(|| format!("read Silver dataset {}", silver.display()))?;
            validate_unique_matches(&matches)?;
            let features = generate_features(&matches)?;
            write_features_parquet(&output, &features)?;
            println!(
                "wrote {} point-in-time features to {}",
                features.len(),
                output.display()
            );
        }
    }
    Ok(())
}
