# Football Analytics Handoff

## Current Status

Iteration 1's minimum offline analysis loop is implemented. Iteration 2 now has code for a provider-neutral odds contract, Sporttery raw capture, de-vig normalization, an append-only PostgreSQL ledger, a read-only API, and one Dagster Pipes capture asset. Iteration 2 is **not fully accepted**: no upstream adapter based on real Sporttery payloads, PostgreSQL service, Dagster runtime, or household-network reliability run has been verified.

Repository baseline at handoff:

- HEAD: `3a38782` (`Complete iteration one analysis loop`)
- Worktree was clean before this document was added.
- No Git remote is configured; no push has been made.
- Workspace members: `contracts`, `ingest`, `pipeline`, `features`, `odds`, `ledger`, `api`, `backtest`.

## Delivered in Iteration 1

- `football-contracts`: shared football-data.org response types and match, feature, and snapshot-manifest contracts.
- `football-ingest`: fetches a competition's match endpoint, spaces requests by six seconds within one client process, and writes immutable Bronze JSON with a manifest containing capture time, byte count, source, contract version, HTTP status, and SHA-256.
- `football-pipeline`: normalizes Bronze JSON to Silver Parquet, estimates completed-result availability as kickoff plus four hours, checks duplicate match identifiers, and preserves nullable scores for unplayed matches.
- `football-features`: generates point-in-time Gold features: recent-five scored/conceded averages, rest-days difference, Elo difference, and prior-match counts. Historical results are incorporated only when `result_available_at < target kickoff`.
- `football-backtest`: chronological expanding-window train/calibration/test evaluation, multinomial logistic regression, training-only standardization, one-vs-rest isotonic calibration, probability renormalization, RPS, Brier, log-loss, and class-wise reliability-bin data.
- Four CLIs connect fetch -> Silver -> Gold -> walk-forward report. Output snapshots/reports use create-new semantics to avoid accidental overwrite.

The backtest JSON contains structured reliability-bin data suitable for plotting; this repository does not yet render a calibration chart image or provide a dashboard.

## Iteration 2 Code Delivered

- `football-contracts`: `ThreeWayOddsSnapshot` and `ThreeWayMarketProbability` share a fixed home/draw/away order and preserve provider, event, competition, teams, capture time, decimal odds, source snapshot SHA-256, raw implied probabilities, normalized probabilities, overround, and de-vig method.
- `football-ingest sporttery`: fetches a configured URL from the caller's network, preserves the response body and manifest even for HTTP errors, uses a 30-second request timeout, and exits nonzero for non-2xx responses. The URL is provided by `SPORTTERY_ODDS_URL` and is not logged.
- `football-odds normalize`: validates decimal odds and identity, applies multiplicative de-vig normalization, hashes the canonical source snapshot, and writes a create-new JSON output.
- `football-ledger`: SQLx/PostgreSQL migration and library for idempotent append of decisions and settlement events, validated three-class probabilities and snapshot hashes, filtered cursor pagination, and lookup by ID. DB triggers reject update/delete on ledger and settlement tables. A CLI exposes `migrate`, `append`, and `settle` commands from JSON input files.
- `football-api`: Axum read-only `GET /health`, `GET /ledger`, `GET /ledger/:id`, and `GET /ledger/:id/settlements`. API startup connects only; DDL migration is an explicit CLI operation. Default bind is loopback.
- `dagster/definitions.py`: a daily, timezone-configurable Dagster asset invokes the Rust capture CLI through `dagster_pipes_rust` and reports Bronze materialization metadata. This first DAG covers capture only.
- `.env.example` contains placeholders only. `README.md` and `dagster/README.md` document commands and deployment boundaries.

The repository deliberately does not guess Sporttery JSON field names. Captured provider payloads remain raw; an adapter to the canonical snapshot contract must be implemented after the team supplies representative, legally/operationally usable payload fixtures. The API does not expose a decision write route.

## Verification Completed

Commands run successfully from the repository root:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Iteration 1 originally had 12 passing unit tests. The current expanded workspace has 19 passing tests: 2 API, 4 backtest, 3 features, 4 ingest, 2 ledger, 2 odds, and 2 pipeline tests. The new tests cover preservation of HTTP error bodies, multiplicative de-vig normalization, source-snapshot validation, API query validation, and read-only route construction. All Rust tests remain fixture/pure-contract checks except the existing synthetic walk-forward; none use a live Sporttery endpoint or PostgreSQL service.

The original Iteration 1 cold build took about ten minutes. The first cold full-workspace build after SQLx/Axum/SciRS2 integration took about 17 minutes 33 seconds; the final warmed format/test/Clippy gates passed. Measure feature iteration speed separately from cold dependency compilation and record both so the two-hour criterion is interpretable.

## Not Yet Done / Acceptance Gaps

1. No live football-data.org request or five-day reliability measurement has been completed. A valid `FOOTBALL_DATA_TOKEN` and network access are required. Keep the token out of Git, logs, and handoff notes.
2. No representative real-data Bronze -> Silver -> Gold -> backtest run has been recorded. There are no empirical RPS/Brier results to interpret yet.
3. The four-hour result-availability delay is a hard-coded research assumption, not an observed timestamp and not yet configurable. Validate it against observed availability before treating historical results as point-in-time evidence.
4. Feature tests establish key strict-before-time behavior, but the planned manual sample audit of at least 20 real feature rows has not been performed.
5. The two-hour feature-hypothesis-to-out-of-sample-report median has not been measured. Record start/end times for several representative changes; distinguish warm iteration time from cold build time.
6. The CLI writes reliability-bin data, not a rendered reliability diagram. A plotting/report step remains if the phase acceptance explicitly requires a visual chart.
7. Sporttery capture and market normalization code exist, but no actual upstream payload adapter, real capture, or market/model evaluation has been verified. Consequently no model/market disagreement claim is supported.
8. The request limiter is per `FootballDataClient` instance. It does not coordinate multiple concurrently launched processes; avoid parallel live fetch processes until a shared rate-limit policy exists.
9. There is no PostgreSQL or Docker service in the current environment. Migrations, queries, append-only triggers, idempotency under DB concurrency, and API routes compile but have not been exercised against a live server.
10. Python and Dagster are not installed in the current environment. `dagster/definitions.py` and its Pipes configuration are not runtime-validated; only the Rust Pipes client compiles.
11. The Dagster asset only captures Sporttery Bronze. It does not schedule odds normalization, feature/model inference, decision append, or result settlement. No automatic decision can be produced from current code.
12. The API has no authentication and should stay bound to loopback or a trusted private network until access control is implemented.

## Iteration 1 Exit Decision

**Code implementation: pass. Empirical acceptance: pending. Overall iteration 1: not fully accepted.**

Do not describe the model as better than the market or as having demonstrated incremental information. The current backtest evaluates a football-data.org-only feature set on its chronological splits; it has no market baseline. Continue the planned staged approach, but complete the missing real-data checks before treating iteration 1 as an evidence-producing baseline.

## Immediate Closeout Checklist

1. Provision the API token through the team's secret mechanism; do not commit it or paste it into shared documentation.
2. Run one bounded live fetch for a selected competition/date range. Preserve the Bronze JSON and manifest; record fetch time, response status, row count, hash, and any API errors without recording the token.
3. Run the documented Silver, Gold, and walk-forward commands using fresh output paths. Record data coverage, skipped/ineligible rows, fold boundaries, per-fold and aggregated metrics, and reliability-bin output.
4. Audit at least 20 real Gold rows against their source match history and verify every included prior result was available strictly before the feature decision time. Record the audit method and exceptions.
5. Run the planned five-day fetch observation and measure feature-change-to-report time for several changes. Report the sample size and median; do not infer acceptance from a single run.
6. Decide whether the result-availability assumption is acceptable for this research or must be replaced with observed/adjustable availability metadata.

Keep the phase boundary explicit: if these empirical checks reveal that Rust experiment iteration exceeds the agreed two-hour median, follow the original fallback and trial a Python analysis/training sidecar while keeping Rust ingestion, feature contracts, point-in-time checks, and durable outputs authoritative.

## Iteration 2 Acceptance Status

**Code foundation: partial pass. Runtime/integration acceptance: pending.** Preserve the four-plane direction and Rust data/feature boundary. Do not count compiled SQL or a declared Dagster schedule as deployment acceptance.

The implementation order so far:

1. Rust raw capture and canonical market normalization contract are implemented, but source-specific decoding awaits real fixtures.
2. PostgreSQL schema, Rust ledger operations, JSON CLI, and read-only API are implemented, but need live DB integration tests.
3. Dagster Pipes capture asset and daily schedule are implemented, but need Python/Dagster runtime validation on the home-network host.
4. End-to-end model-to-ledger decision generation and automated settlement remain unwired.

## Next Acceptance Tasks

1. Supply representative Sporttery payload fixtures (including 2xx, HTTP 567, missing selections, changed field shapes) and implement a versioned parser from provider JSON into `ThreeWayOddsSnapshot`. Record the exact source resource and local capture time; do not commit secrets or sensitive request URLs.
2. Run capture from the household-network host. Verify body bytes and SHA-256 against the manifest; perform a five-day observation and report scheduled success rate. Validate retry cadence and whether capture failures preserve raw error bodies.
3. Start PostgreSQL on a dedicated test database, run `cargo run -p football-ledger -- migrate`, and exercise append, identical idempotent retry, conflicting idempotency key, settlement, correction, and delete/update rejection. Verify stored JSON round-trips and pagination under concurrent inserts.
4. Add integration tests or a reproducible script for the above DB cases. The current unit test validates only the pure probability/hash contract.
5. Install the Dagster environment declared in `dagster/requirements.txt` on the household host. Run `cd dagster && dagster dev -m definitions`, manually materialize `sporttery_bronze`, then verify scheduled run metadata, retry handling, and Rust Pipes materialization/failed run behavior.
6. Confirm the configured endpoint, schedule time/timezone, retention policy, and whether URLs contain credentials. Keep secrets in the host environment, not Dagster logs or repository config.
7. Wire odds normalization and existing feature/model outputs into a controlled decision creation job only after the team defines a common match identity, operational decision cutoff, and market timestamp policy. Store actual lock-time `decided_at` and immutable feature/market hashes. Until then, the ledger must not imply a live model decision exists.
8. Add automated settlement only after a trusted result source and correction policy are specified. Corrections must append events and preserve their chronology.
9. Keep the read-only API on loopback; add auth/network policy before binding externally. Verify `/health`, date/competition filters, cursor pagination, not-found behavior, and error handling against PostgreSQL.
10. Re-run `cargo fmt --all -- --check`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings`. Then update this handoff with observed environment versions, dates, result evidence, and unresolved blockers.

Iteration 2 acceptance remains at least 10 match days with no missing decisions, snapshot-hash replay consistency, documented retry/idempotency tests, and at least 95% scheduled household-network odds capture success. Define whether a "match day" means calendar date or a date with scheduled fixtures before measuring. Current implementation has no end-to-end decision producer, so this acceptance cannot yet start.

## Later Roadmap

- **Iteration 3:** extend features only through paired out-of-sample ablation; add analysis-query acceleration only after representative PostgreSQL baselines; stabilize read-only API contracts; then build a read-only web panel consuming the API. Do not let the panel read PostgreSQL or Parquet directly. Require a market baseline to assess model/market disagreement.
- **Iteration 4:** add experiment/data-version isolation when concurrent work justifies it, observability, backup/restore drills, and runbooks. Evaluate mobile read-only use only after the web panel has sustained real team usage and a concrete mobile need.
- Keep IronFlow, LakeFS/Delta-RS, pg_duckdb, and mobile packaging as later decisions with explicit need and measured acceptance criteria, not prerequisites for the next iteration.

## Decisions to Resolve With the Team

- For historical backtests, kickoff is currently used as the decision timestamp. For live decisions, record the actual timestamp at which the model/market snapshot was locked. Confirm the intended operational decision horizon (for example, a fixed pre-kickoff cutoff) before adding the ledger.
- Confirm which Sporttery market(s) are the first baseline and the de-vig method. This is required to evaluate the stated goal of identifying useful model/market probability disagreements.
- Define how five-day capture reliability, 10-match-day ledger completeness, and two-hour iteration time are sampled and reported.
- Decide whether the visual calibration chart is a hard Iteration 1 exit requirement or whether persisted reliability-bin JSON is sufficient for closeout.

## Iteration 2 Commands

```sh
# On the household-network capture host; configure endpoint locally.
export SPORTTERY_ODDS_URL='<local upstream endpoint>'
cargo run -p football-ingest -- sporttery --resource sporttery_odds

# Normalize a canonical ThreeWayOddsSnapshot JSON fixture offline.
cargo run -p football-odds -- normalize data/odds/three-way.json data/market/three-way.json

# Run DB migration once with a migration-capable role; API uses a non-DDL role in deployment.
export DATABASE_URL='postgres://...'
cargo run -p football-ledger -- migrate
cargo run -p football-api

# Dagster is run from its own directory on the household-network host.
cd dagster
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
dagster dev -m definitions
```

`football-ledger append <json>` expects an `AppendDecision` with idempotency key, match key, competition, normalized `market` object, feature snapshot SHA-256, model version, actual decision timestamp, and three model probabilities. `football-ledger settle <json>` expects an `AppendSettlement` with decision ID, idempotency key, event type (`settled`, `correction`, or `voided`), JSON result, and settlement time. Never reuse an idempotency key for different content.

## Iteration 1 Commands

Run from the repository root. Set fresh paths because outputs are intentionally not overwritten.

```sh
export FOOTBALL_DATA_TOKEN="<provide locally; do not commit>"
cargo run -p football-ingest -- matches --competition PL --date-from 2026-08-01 --date-to 2026-11-01

# Set BRONZE_FILE to the captured payload JSON path under data/bronze
# (select the payload, not its neighboring .manifest.json file).
cargo run -p football-pipeline -- normalize --input "$BRONZE_FILE" --output data/silver/PL.parquet
cargo run -p football-features -- build --silver data/silver/PL.parquet --output data/gold/PL.parquet
cargo run -p football-backtest -- walk-forward --input data/gold/PL.parquet --minimum-training-rows 50 --calibration-rows 30 --test-rows 20 --step-rows 20 --output reports/pl-walk-forward.json

cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
