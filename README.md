# Football Analytics

Rust-first football data research pipeline. Iteration 1 now provides a runnable Bronze -> Silver -> Gold -> walk-forward loop using football-data.org, Polars Parquet, linfa multinomial logistic regression, and one-vs-rest isotonic calibration.

See [HANDOFF.md](HANDOFF.md) for the current phase status, acceptance gaps, verification results, and next-iteration plan.

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
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Iteration 2 foundation

Iteration 2 now includes an upstream-neutral Sporttery Bronze capture command, a canonical 1X2 odds contract with multiplicative de-vig normalization, a PostgreSQL append-only decision ledger, a read-only Axum API, and a Dagster Pipes capture asset. The actual upstream payload adapter, a running PostgreSQL/Dagster deployment, and the 10-match-day reliability acceptance are still outstanding. See [HANDOFF.md](HANDOFF.md) for precise status and next actions.

Capture from the household-network host. Configure the endpoint locally; do not commit its URL or credentials:

```sh
export SPORTTERY_ODDS_URL='your-local-endpoint'
cargo run -p football-ingest -- sporttery --resource sporttery_odds
```

The raw response and a manifest are stored under `data/bronze/sporttery/captured_date=...`. Non-2xx bodies are preserved for diagnosis and the command exits unsuccessfully. After an adapter produces the canonical `ThreeWayOddsSnapshot` JSON, normalize it offline:

```sh
cargo run -p football-odds -- normalize data/odds/three-way.json data/market/three-way.json
```

The canonical snapshot contains `source`, `source_event_id`, `competition_code`, `home_team_name`, `away_team_name`, `captured_at`, and `decimal_odds` in home/draw/away order. The output contains raw implied probabilities, overround, multiplicatively de-vigged probabilities, method name, and a SHA-256 of the source snapshot.

Initialize PostgreSQL once using a migration-capable role. API runtime only connects and does not run DDL:

```sh
export DATABASE_URL='postgres://...'
cargo run -p football-ledger -- migrate
cargo run -p football-api
```

The API binds to `127.0.0.1:8080` by default and exposes `GET /health`, paginated/filterable `GET /ledger`, `GET /ledger/:id`, and `GET /ledger/:id/settlements`. It has no write routes. Do not bind it to a public interface until authentication and deployment network controls are added. Ledger append and settlement commands accept JSON input files via `cargo run -p football-ledger -- append <file>` and `settle <file>`; use unique idempotency keys and retain the referenced immutable snapshots.

Dagster capture orchestration setup and schedule configuration are in [dagster/README.md](dagster/README.md). Run Dagster only on the household-network capture host. Its current asset schedules Bronze capture; model inference, automated ledger append, and settlement are not yet wired into that DAG.

The workspace sets one Cargo build job to reduce peak memory during Polars and SciRS2 compilation. Live football-data.org ingestion and the empirical five-day reliability/iteration-speed acceptance still require the team's API token and real data.