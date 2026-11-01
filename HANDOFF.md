# Football Analytics Handoff

## Handoff Snapshot (2026-11-01)

- **Base revision:** `abe36aa`; this handoff covers the local acceptance-phase checkpoint. The acceptance register is the status source of truth: [`docs/acceptance/iteration-1-2.md`](docs/acceptance/iteration-1-2.md).
- **Iteration 1 verified:** one bounded 2025-08-01 to 2026-06-30 football-data.org run returned 380 Premier League matches; Bronze hash/size matched its manifest; Silver and Gold each contain 380 rows; walk-forward completed 15 folds / 300 OOS samples (RPS 0.23847, Brier 0.69301, log-loss 3.24070). This is a pipeline baseline, not evidence of market outperformance.
- **Code changes in this worktree:** nonblocking `/tmp/football-live-fetch.lock` in the ingest CLI (conflict exits 75), Dagster `max_concurrent_runs: 1`, and adaptive football-data.org throttling from `X-Requests-Available-Minute` / `Retry-After` (six-second floor). The latest local format, 21 workspace tests, and strict Clippy checks passed.
- **I2-14:** Cao Yuheng confirmed review of the current uncommitted worktree on 2026-11-01; current formatting, 21 workspace tests, and strict Clippy gates passed with `CARGO_BUILD_JOBS=2`. No commit has been made for these changes.
- **Evidence location:** live Bronze/Silver/Gold/report files are under `/tmp/football-validation-2026-11-01/`; they are not in Git and should be preserved or transferred securely before cleanup. `.env` is local, mode `0600`, gitignored. On 2026-11-01 the token loaded successfully from `.env` and was verified non-empty without displaying it; never copy its value into handoff material, logs, or chat.

**Next owner, in order:**

1. Monitor the I1-02 five-day football-data.org observation started in this environment on 2026-11-02. One PL attempt is scheduled at 08:00 Asia/Shanghai daily through 2026-11-06; denominator is five, with provider/command failures, missed dates, and interrupted runs counted as failures. The observer does not survive container restart; review `/home/coder/football-validation/daily-fetch/attempts.csv`, per-day logs, and Bronze manifests before calculating the rate. See [runbook](docs/runbooks/i1-02-daily-fetch-observation.md).
2. I1-02's 08:00 rolling snapshots cover the scheduled date and prior 7 days, but only show status at observation times; they do not prove first availability or validate `kickoff + 4 hours`. Keep I1-03 blocked. After confirming provider/request budget, approve and run 15-minute polling around relevant result windows, retaining UTC `observed_at`, match ID, `utcDate`, status, score, and payload hash; bound availability between last non-`FINISHED` and first `FINISHED` observation, then obtain independent sign-off.
3. I1-06 pilot timing was accepted by Modeling Lead Cao Yuheng (曹宇恒) on 2026-11-01 (three warm feature-group ablations; median 0.737 seconds, runner compile/link reported separately). Re-measure after future feature-code changes, then record the I1-08 exit decision.
4. For Iteration 2, obtain permitted Sporttery fixtures and implement the versioned parser; then run capture and Dagster checks on the household-network host. Provision a dedicated PostgreSQL test service for ledger/API integration checks. Keep decision-production and 10-match-day/30-day observations blocked until their prerequisites pass.
5. After any further Rust code changes, rerun workspace gates and obtain a fresh review. This checkpoint is local; no remote push is implied.

## Loading the Local Token

From the repository root, load the ignored `.env` in each shell that needs the token. This prints only whether the variable is set, never its value:

```sh
set -a
. ./.env
set +a
test -n "$FOOTBALL_DATA_TOKEN" && printf 'FOOTBALL_DATA_TOKEN is set\n'
```

Do not use `echo "$FOOTBALL_DATA_TOKEN"`, `env`, or `printenv` to verify it. Keep `.env` local and mode `0600`; do not commit it.

## Current Status

Iteration 1's minimum offline analysis loop is implemented. Iteration 2 now has code for a provider-neutral odds contract, Sporttery raw capture, de-vig normalization, an append-only PostgreSQL ledger, a read-only API, and one Dagster Pipes capture asset. Iteration 2 is **not fully accepted**: no upstream adapter based on real Sporttery payloads, PostgreSQL service, Dagster runtime, or household-network reliability run has been verified.

Repository baseline at handoff:

- Base HEAD: `abe36aa` (`Implement iteration two data and ledger foundation`); working tree includes the I1-07 lock and response-header-aware throttle changes.
- Acceptance register: [`docs/acceptance/iteration-1-2.md`](docs/acceptance/iteration-1-2.md)
- The acceptance register is authoritative for Iteration 1-2 acceptance status and required evidence.
- Workspace members: `contracts`, `ingest`, `pipeline`, `features`, `odds`, `ledger`, `api`, `backtest`.

## Delivered in Iteration 1

- `football-contracts`: shared football-data.org response types and match, feature, and snapshot-manifest contracts.
- `football-ingest`: fetches a competition's match endpoint, spaces requests according to `X-Requests-Available-Minute`/`Retry-After` headers with a six-second floor, and writes immutable Bronze JSON with a manifest containing capture time, byte count, source, contract version, HTTP status, and SHA-256.
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

Iteration 1 originally had 12 passing unit tests. The current workspace has 21 passing tests, including the response-header rate-interval parser and single-live-fetch lock behavior. The 2026-11-01 evidence run also fetched 380 real football-data.org matches and generated Silver, Gold, and a 15-fold walk-forward report. This does not close the five-day reliability window or publication-time validation. The 20-row source-to-Gold consistency audit is recorded, but independent reviewer sign-off remains pending. No live Sporttery endpoint or PostgreSQL service has been verified.

The original Iteration 1 cold build took about ten minutes. The first cold full-workspace build after SQLx/Axum/SciRS2 integration took about 17 minutes 33 seconds; the final warmed format/test/Clippy gates passed. Measure feature iteration speed separately from cold dependency compilation and record both so the two-hour criterion is interpretable.

## Not Yet Done / Acceptance Gaps

1. **I1-02:** the five scheduled daily fetches run 2026-11-02 through 2026-11-06. Review all five terminal outcomes and calculate success rate against denominator 5; the observer is a container process and does not auto-restart after container restart.
2. **I1-03:** daily rolling snapshots cannot establish first-publication time or prove the `kickoff + 4 hours` assumption. Keep blocked until a prospective observation plan, request budget, and source timestamp semantics are approved. The proposed 15-minute result-window polling is not deployed.
3. **I1-04:** the 20-row Bronze/Silver/Gold consistency audit is complete, but it does not prove observed publication-time validity; independent reviewer sign-off remains pending on I1-03 evidence.
4. **I1-08:** Iteration 1 exit decision is not started. Do not claim market superiority or incremental value without a market baseline.
5. **Iteration 2 runtime:** no representative Sporttery payload/parser, household-host capture window, live PostgreSQL integration, or Dagster runtime has been verified. The current Dagster asset captures only Sporttery Bronze; decision production and settlement remain unwired.
6. **Iteration 2 endurance/security:** the 10-match-day decision/replay window and 30-day Dagster reliability window cannot start until a decision producer exists. Keep the API loopback/private-only until authentication and network controls are implemented and tested.
7. **I1-06 follow-up:** the Modeling Lead accepted the current ablation pilot. Re-measure if feature code changes; report code/build and warm report time separately.
8. The 2026-11-01 real-data report has 15 folds and 300 out-of-sample samples (RPS 0.23847, Brier 0.69301, log-loss 3.24070); these metrics are a baseline only, not evidence of market outperformance.

## Iteration 1 Exit Decision

**Code implementation: pass. Empirical acceptance: pending. Overall iteration 1: not fully accepted.**

Do not describe the model as better than the market or as having demonstrated incremental information. The current backtest evaluates a football-data.org-only feature set on its chronological splits; it has no market baseline. Continue the planned staged approach, but complete the missing real-data checks before treating iteration 1 as an evidence-producing baseline.

## Immediate Closeout Checklist

1. Monitor I1-02 through 2026-11-06; after the fifth scheduled attempt, verify one terminal outcome per date and calculate successful `FINISHED` runs divided by five. `skipped_locked`, provider/command failures, and missed dates count as failures.
2. Keep I1-03 blocked. Agree the result-window observation scope and provider request budget before enabling the proposed 15-minute polling; persist each observation's UTC time, match ID, `utcDate`, status, score, and payload hash.
3. Obtain independent review of the I1-04 audit and publication-time policy. Do not infer publication time from `lastUpdated` or daily observation time.
4. Record the signed I1-08 exit decision and finish the Iteration 2 runtime/integration prerequisites listed in the acceptance register.

Keep the phase boundary explicit: if these empirical checks reveal that Rust experiment iteration exceeds the agreed two-hour median, follow the original fallback and trial a Python analysis/training sidecar while keeping Rust ingestion, feature contracts, point-in-time checks, and durable outputs authoritative.

## Iteration 2 Acceptance Status

**Code foundation: partial pass. Runtime/integration acceptance: pending.** I2-14 code gates and current-worktree review are complete. Preserve the four-plane direction and Rust data/feature boundary. Do not count compiled SQL or a declared Dagster schedule as deployment acceptance.

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
