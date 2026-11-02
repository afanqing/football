# Dagster Operator Deployment

Run these five steps on the household-network host. This deploys the current `sporttery_bronze` capture asset only; it does not deploy live model inference, ledger decisions, or settlement.

1. **Get the code and build Rust.** Check out the approved revision on the household-network host, then from the repository root run `cargo build --release -p football-ingest`.

2. **Install Python dependencies.** From the repository root run `python3 -m venv .venv`, activate it, then run `cd ops && python -m pip install -r requirements.txt`. Python 3.10+ and Rust/Cargo must be available to the Dagster process.

3. **Configure the endpoint locally.** Create an ignored `.env` in the repository root with `SPORTTERY_ODDS_URL`, `SPORTTERY_RESOURCE=sporttery_odds`, `SPORTTERY_CRON='0 8 * * *'`, and `SPORTTERY_TIMEZONE=Asia/Shanghai`. Protect it with `chmod 600 .env`; never commit it or paste a credential-bearing URL into logs/chat. Dagster does not load `.env` automatically: activate the venv and source it with `set -a; . ../.env; set +a` from `ops/`.

4. **Start Dagster from `ops/`.** Set `DAGSTER_HOME` to a persistent operator-owned directory, copy `../dagster/dagster.yaml` there, then run `dagster dev -w workspace.yaml`. Keep the process supervised on the household host and use the single-run configuration/shared fetch lock.

5. **Materialize `sporttery_bronze` once.** Open the Dagster UI URL printed by the CLI, select `sporttery_bronze`, and materialize it manually. Verify the run status and Pipes materialization, compare the Bronze body SHA-256 with its manifest, inspect retry/error behavior and logs for secrets, then record the run ID and evidence in the acceptance register. One successful smoke run alone does not close I2-02 or I2-06; their full host, failure/retry, schedule/timezone, and log-review criteria still apply.