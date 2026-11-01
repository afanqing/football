use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use football_backtest::{WalkForwardConfig, run_walk_forward};
use football_features::read_features_parquet;

#[derive(Debug, Parser)]
#[command(
    name = "football-backtest",
    about = "Time-separated football model evaluation"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    WalkForward {
        #[arg(long)]
        input: std::path::PathBuf,
        #[arg(long, default_value_t = 50)]
        minimum_training_rows: usize,
        #[arg(long, default_value_t = 30)]
        calibration_rows: usize,
        #[arg(long, default_value_t = 20)]
        test_rows: usize,
        #[arg(long, default_value_t = 20)]
        step_rows: usize,
        #[arg(long, default_value_t = 10)]
        reliability_bins: usize,
        #[arg(long, default_value = "reports/walk-forward.json")]
        output: std::path::PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::WalkForward {
            input,
            minimum_training_rows,
            calibration_rows,
            test_rows,
            step_rows,
            reliability_bins,
            output,
        } => {
            let rows = read_features_parquet(&input)
                .with_context(|| format!("read Gold feature dataset {}", input.display()))?;
            let report = run_walk_forward(
                &rows,
                WalkForwardConfig {
                    minimum_training_rows,
                    calibration_rows,
                    test_rows,
                    step_rows,
                    reliability_bins,
                },
            )?;
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("create report {}", output.display()))?;
            serde_json::to_writer_pretty(file, &report).context("write backtest report")?;
            println!(
                "{} folds; OOS RPS={:.5}, Brier={:.5}, log-loss={:.5}; report={}",
                report.folds.len(),
                report.out_of_sample.rps,
                report.out_of_sample.brier,
                report.out_of_sample.log_loss,
                output.display()
            );
        }
    }
    Ok(())
}
