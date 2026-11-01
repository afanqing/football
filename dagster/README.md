# Dagster Capture Control Plane

The first Dagster asset schedules only the household-network Sporttery Bronze capture. It invokes the Rust `football-ingest` binary through `dagster_pipes_rust`; the Dagster process is a control plane and does not parse odds or run model calculations.

## Setup

Install Python 3.10+ and Dagster in an isolated environment, then from this directory run:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run Dagster on the household-network host, not a cloud data center that cannot reach the upstream:

```sh
export SPORTTERY_ODDS_URL='https://configured-local-upstream-endpoint'
export SPORTTERY_RESOURCE='sporttery_odds'
export SPORTTERY_CRON='0 8 * * *'
export SPORTTERY_TIMEZONE='Asia/Shanghai'
export FOOTBALL_DATA_ROOT='../data/bronze'
export DAGSTER_HOME="$HOME/.football-analytics-dagster"
mkdir -p "$DAGSTER_HOME"
cp dagster.yaml "$DAGSTER_HOME/dagster.yaml"
dagster dev -m definitions
```

The instance configuration limits Dagster to one concurrent run. Keep this configuration active on the household-network host; the Rust CLI also holds a nonblocking OS file lock for the entire live fetch and exits with status 75 if another fetch owns it. See [`../docs/runbooks/i1-07-single-live-fetch.md`](../docs/runbooks/i1-07-single-live-fetch.md) for the concurrency checks and failure behavior.

The URL must be configured locally and must not be committed. Dagster passes its Pipes context to the Rust child process. The Rust process stores the response and manifest before reporting asset materialization. A non-2xx response remains preserved in Bronze but exits unsuccessfully so Dagster can retry and alert.

The schedule defaults to 08:00 Asia/Shanghai and can be overridden. Confirm a suitable capture time and actual endpoint with the team before enabling unattended runs. The current schedule covers capture only; it does not yet execute feature generation, live model inference, ledger append, or result settlement.
