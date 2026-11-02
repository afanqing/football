# Football Analytics Handoff

## Handoff Snapshot (2026-11-02)

- **Base revision:** `d6452cd` (`Implement I1-03 result observer`); this local checkpoint layers the Sporttery HAD parser, source timestamp propagation, PostgreSQL/API acceptance tests, and current evidence records on that base. The acceptance register is the status source of truth: [`docs/acceptance/iteration-1-2.md`](docs/acceptance/iteration-1-2.md).
- **Iteration 1 verified:** one bounded 2025-08-01 to 2026-06-30 football-data.org run returned 380 Premier League matches; Bronze hash/size matched its manifest; Silver and Gold each contain 380 rows; walk-forward completed 15 folds / 300 OOS samples (RPS 0.23847, Brier 0.69301, log-loss 3.24070). This is a pipeline baseline, not evidence of market outperformance.
- **Code changes in this checkpoint:** nonblocking `/tmp/football-live-fetch.lock`, adaptive football-data.org throttling, the I1-03 observer, `sporttery-had-v1`, source-market timestamp propagation, and PostgreSQL/API integration coverage. Pre-commit format, workspace tests, and strict Clippy passed; the dedicated database tests and secured parser fixture also passed separately.
- **I1-02:** the 2026-11-02 08:00 CST attempt succeeded (1/5); the observer is alive. The five-day window remains open through 2026-11-06.
- **Evidence location:** live Bronze/Silver/Gold/report files are under `/tmp/football-validation-2026-11-01/`; they are not in Git and should be preserved or transferred securely before cleanup. `.env` is local, mode `0600`, gitignored. On 2026-11-01 the token loaded successfully from `.env` and was verified non-empty without displaying it; never copy its value into handoff material, logs, or chat.

**Next owner, in order:**

1. Continue I1-02's five-day football-data.org observation through 2026-11-06. The first 08:00 CST PL attempt succeeded; denominator is five, and provider/command failures, missed dates, and interrupted runs count as failures. The observer does not survive container restart; review `/home/coder/football-validation/daily-fetch/attempts.csv`, per-day logs, and Bronze manifests daily. See [runbook](docs/runbooks/i1-02-daily-fetch-observation.md).
2. I1-03's 20-fixture sample and 247-request budget are approved by Cao Yuheng (曹宇恒), as conveyed on 2026-11-02. The release observer is running as PID `309289` under the operator-confirmed no-restart Docker; state and logs are under `/home/coder/football-validation/i1-03-state/`. The one-request schedule connectivity test passed (HTTP 200, payload hash verified); it was not a result poll. First poll is scheduled for 2026-11-21 14:00 UTC. The plan spans Nov 21-23 and Nov 27-29 (six dates, not only Nov 21 and Nov 27-29). Next: check PID and logs daily; at the first slot verify the poll ledger entry, Bronze/manifest hash, lock outcome, and secret-free logs; continue monitoring every scheduled date and record all gaps without retries. The final registered window ends by 2026-11-30 00:30 UTC. Then run `summarize-results`, reconcile all 20 dispositions and the 247-request ceiling, obtain independent review, update I1-04/I1-03 evidence, and only then make the I1-08 exit decision. I1-02's 08:00 rolling snapshots cannot prove first result availability. Coordinate with I1-02 and the shared live-fetch lock. `first_result_usable_observed_at < kickoff+4h` falsifies the current assumption; `last_result_unusable_observed_at >= kickoff+4h` supports it; a crossing interval, poll gap, or missing bound is inconclusive. Polling provides an interval, not an exact publication timestamp. Do not mark I1-03 PASS until every sampled fixture is dispositioned, evidence/code consequences are reviewed, and independent sign-off is recorded.
3. I1-06 pilot timing was accepted by Modeling Lead Cao Yuheng (曹宇恒) on 2026-11-01 (three warm feature-group ablations; median 0.737 seconds, runner compile/link reported separately). Re-measure after future feature-code changes, then record the I1-08 exit decision.
4. For Iteration 2, the pre-Pipes parser-to-normalize-to-ledger test passed against the dedicated PostgreSQL test database on 2026-11-02. It uses the approved fixture and test-only placeholder feature/model inputs; the controlled producer using real feature/model outputs remains unwired. Next, the pipeline owner should implement that producer; the Operator should deploy Dagster capture on the household host. Keep live decision production and 10-match-day/30-day observations open.
5. This is a local checkpoint only; no remote push is implied. Re-run the Rust gates after further code changes.

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

Iteration 1's minimum offline analysis loop is implemented. Iteration 2 has a provider-neutral odds contract, Sporttery raw capture and local HAD parser, de-vig normalization, an append-only PostgreSQL ledger, a read-only API, and one Dagster Pipes capture asset. Iteration 2 is **not fully accepted**: household-host capture/Dagster deployment and reliability runs remain open, and the controlled decision producer has not been wired. The secured 2xx parser fixture and local mocked Dagster runtime passed; PostgreSQL migration and ledger/API integration cases passed against the dedicated test database. See the acceptance register.

Repository baseline at handoff:

- Checkpoint base: `d6452cd` (`Implement I1-03 result observer`).
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

The versioned `sporttery-had-v1` adapter parses the approved secured `getFixedBonusV1.qry` fixture into the canonical snapshot contract and preserves `source_updated_at` separately from `captured_at`. The other error/boundary variants are synthetic and household-host capture remains unverified. The API does not expose a decision write route.

## Verification Completed

Commands run successfully from the repository root:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Iteration 1 originally had 12 passing unit tests. The current workspace test run passed 28 tests with 2 database acceptance tests ignored by default; strict Clippy and format checks passed. The dedicated Ledger and API database tests each passed when explicitly enabled, and the secured Sporttery 2xx parser fixture passed separately. The 2026-11-01 evidence run fetched 380 real football-data.org matches and generated Silver, Gold, and a 15-fold walk-forward report. This does not close the five-day reliability window or publication-time validation. The 20-row source-to-Gold consistency audit is recorded, but independent reviewer sign-off remains pending. No live Sporttery endpoint or household-host runtime has been verified; see the acceptance register.

The original Iteration 1 cold build took about ten minutes. The first cold full-workspace build after SQLx/Axum/SciRS2 integration took about 17 minutes 33 seconds; the current workspace format, test, and strict Clippy gates pass. PostgreSQL migration and ledger/API integration tests also pass against the dedicated test database. Measure feature iteration speed separately from cold dependency compilation and record both so the two-hour criterion is interpretable.

## Not Yet Done / Acceptance Gaps

1. **I1-02:** the five scheduled daily fetches run 2026-11-02 through 2026-11-06. The first attempt succeeded (1/5); review all five terminal outcomes and calculate success rate against denominator 5. The observer is a container process and does not auto-restart after container restart.
2. **I1-03:** the approved observer is deployed and waiting for its first scheduled slot; the one-request network test passed and the run manifest matches the approved plan. No result poll has run, so empirical acceptance remains blocked. Monitor PID/log/ledger and preserve all gaps.
3. **I1-04:** the 20-row Bronze/Silver/Gold consistency audit is complete, but it does not prove observed publication-time validity; independent reviewer sign-off remains pending on I1-03 evidence.
4. **I1-08:** Iteration 1 exit decision is not started. Do not claim market superiority or incremental value without a market baseline.
5. **Iteration 2 runtime/decision:** the approved 2xx Sporttery fixture parses locally, but synthetic failure variants do not establish upstream behavior and household-host capture/Dagster remain unverified. PostgreSQL migration and ledger/API integration are verified on the dedicated test database. The current Dagster asset captures only Sporttery Bronze; the controlled decision producer and settlement remain unwired.
6. **Iteration 2 endurance/security:** the 10-match-day decision/replay window and 30-day Dagster reliability window cannot start until a decision producer exists. Keep the API loopback/private-only until authentication and network controls are implemented and tested.
7. **I1-06 follow-up:** the Modeling Lead accepted the current ablation pilot. Re-measure if feature code changes; report code/build and warm report time separately.
8. The 2026-11-01 real-data report has 15 folds and 300 out-of-sample samples (RPS 0.23847, Brier 0.69301, log-loss 3.24070); these metrics are a baseline only, not evidence of market outperformance.

## Iteration 1 Exit Decision

**Code implementation: pass. Empirical acceptance: pending. Overall iteration 1: not fully accepted.**

Do not describe the model as better than the market or as having demonstrated incremental information. The current backtest evaluates a football-data.org-only feature set on its chronological splits; it has no market baseline. Continue the planned staged approach, but complete the missing real-data checks before treating iteration 1 as an evidence-producing baseline.

## Immediate Closeout Checklist

1. Monitor I1-02 through 2026-11-06; the first attempt succeeded. After the fifth scheduled attempt, verify one terminal outcome per date and calculate successful `FINISHED` runs divided by five. `skipped_locked`, provider/command failures, and missed dates count as failures.
2. Monitor I1-03 PID `309289` and its state directory. No result poll is due until 2026-11-21 14:00 UTC; the 20 fixtures span Nov 21-23 and Nov 27-29. Persist each poll's UTC time, match ID, `utcDate`, status, score, and payload hash; record process interruptions and missed slots rather than silently retrying.
3. Obtain independent review of the I1-04 audit and publication-time policy. Do not infer publication time from `lastUpdated` or daily observation time.
4. Record the signed I1-08 exit decision and finish the Iteration 2 runtime/integration prerequisites listed in the acceptance register.

Keep the phase boundary explicit: if these empirical checks reveal that Rust experiment iteration exceeds the agreed two-hour median, follow the original fallback and trial a Python analysis/training sidecar while keeping Rust ingestion, feature contracts, point-in-time checks, and durable outputs authoritative.

## Iteration 2 Acceptance Status

**Code foundation: partial pass. Runtime/integration acceptance: pending.** I2-14 format/test/Clippy commands and the follow-on I2-09 code review passed on 2026-11-02; reviewer: Cao Yuheng (曹宇恒). Preserve the four-plane direction and Rust data/feature boundary. Do not count compiled SQL or a declared Dagster schedule as deployment acceptance.

The implementation order so far:

1. Rust raw capture, `sporttery-had-v1` parsing of the approved secured fixture, and canonical market normalization are implemented. Error/boundary variants remain simulated; household-host capture is still required.
2. PostgreSQL schema, Rust ledger operations, JSON CLI, and read-only API are implemented; migration and ledger/API integration tests passed against the dedicated test database on 2026-11-02 CST.
3. Dagster Pipes capture asset and daily schedule are implemented and pass local mocked runtime validation; household-host deployment, scheduling, and live capture validation remain.
4. End-to-end model-to-ledger decision generation and automated settlement remain unwired. I2-09's first offline milestone passed on 2026-11-02: the approved fixture was parsed, normalized, and appended to the dedicated test ledger; source/capture timestamps and required-field rejection were verified. Feature/model inputs in this test are placeholders, so this is not a production decision or I2-09 completion.

## Next Acceptance Tasks

1. I2-09 data/pipeline owners: the approved odds sample is Bundesliga match `2042426` (Berlin Union–Cologne), while local Gold is PL; its intake approval covers internal parser/test use only. Obtain an authorized odds snapshot that matches a Gold fixture (or matching Bundesliga Gold), then expose leakage-safe per-fixture OOS probabilities before linking market, features, and a Ledger append. Do not use the current placeholder test as a real decision.
2. Operator: follow the five-step [Dagster deployment pack](ops/deploy.md) on the household-network host. `ops/workspace.yaml` and the requirements pointer passed local validation only; host deployment, real endpoint, schedule, Pipes run, retry/error behavior, and secret-safe log review still need evidence. Configure credentials only on that host; do not bundle `.env`, raw fixtures, or `target/`.
3. Operator: manually materialize `sporttery_bronze` against the approved endpoint and verify body bytes/hash against the manifest, schedule/run metadata, retries, Pipes materialization, failure behavior, and secret-safe logs. Local mocked success/failure paths do not replace this host run.
4. Operator + reviewer: after host configuration passes, start I2-03's five-day capture window; keep I2-02/I2-06 blocked until host evidence is reviewed. Do not start I2-11/I2-12 until the production decision job exists and I2-09 passes.
5. Ledger owner + product owner: select a trusted result source and approve correction/void policy before implementing automated settlement; corrections must append events and preserve chronology.
6. API owner: keep the API on loopback/private network; add and test authentication/network controls before any public binding.
7. Rust owner: rerun `cargo fmt --all -- --check`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings` after changes; attach dated evidence and unresolved blockers here.

Iteration 2 acceptance remains at least 10 match days with no missing decisions, snapshot-hash replay consistency, documented retry/idempotency tests, and at least 95% scheduled household-network odds capture success. Define whether a "match day" means calendar date or a date with scheduled fixtures before measuring. Current implementation has no end-to-end decision producer, so this acceptance cannot yet start.

The post-window I1-03 verifier is `scripts/validate_i1_03_results.sh`; it groups per-fixture ledger rows into request attempts, verifies plan binding/fixture coverage and Bronze hashes, scans logs, and emits a reviewer-pending report. Synthetic positive, right-censored, and credential-key cases passed; it has not yet been run against live observation data.

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
