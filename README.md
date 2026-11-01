# Football Analytics

Rust-first football data research pipeline. Iteration 1 now provides a runnable Bronze -> Silver -> Gold -> walk-forward loop using football-data.org, Polars Parquet, linfa multinomial logistic regression, and one-vs-rest isotonic calibration.

## Requirements

- Rust and Cargo (workspace MSRV: 1.89)
- A football-data.org API token for live ingestion

## Run

```sh
export FOOTBALL_DATA_TOKEN="your-token"
cargo run -p football-ingest -- matches --competition PL --date-from 2026-08-01 --date-to 2026-11-01
```

Snapshots are written below `data/bronze/football-data.org/captured_date=YYYY-MM-DD/`. Each raw response has a neighboring manifest containing its capture time, byte count, source, schema version, and SHA-256 digest. The client spaces requests to stay within the free-tier 10 requests/minute limit.

## Run the offline analysis loop

Use the raw JSON file created by the fetch command as `BRONZE_FILE`:

```sh
cargo run -p football-pipeline -- normalize \
	--input "$BRONZE_FILE" \
	--output data/silver/PL.parquet

cargo run -p football-features -- build \
	--silver data/silver/PL.parquet \
	--output data/gold/PL.parquet

cargo run -p football-backtest -- walk-forward \
	--input data/gold/PL.parquet \
	--minimum-training-rows 50 \
	--calibration-rows 30 \
	--test-rows 20 \
	--step-rows 20 \
	--output reports/pl-walk-forward.json
```

The report contains per-fold and aggregated out-of-sample RPS, Brier score, log-loss, split cutoffs, and class-wise reliability-bin data. Output files use create-new semantics; choose a fresh path for each run rather than overwriting a prior snapshot.

## Point-in-time assumption

football-data.org provides match kickoff and final scores but this first slice does not have the true time at which a result became available. For completed matches, Silver temporarily assigns `result_available_at = kickoff_at + 4 hours`. Features only include results with `result_available_at < decision_at`; backtest training labels must mature before calibration starts, and calibration labels must mature before testing starts. Treat the four-hour delay as a configurable research assumption to validate against observed data before using results as evidence.

## Verify

```sh
cargo test --workspace
```

## Verify

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The workspace sets one Cargo build job to reduce peak memory during Polars and SciRS2 compilation. Live API ingestion and the empirical five-day reliability/iteration-speed acceptance checks still require the team's API token and real data.