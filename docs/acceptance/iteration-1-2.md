# Iteration 1-2 Acceptance Plan

Last reviewed: 2026-11-02
Base revision: `d6452cd` (current HEAD when the 2026-11-02 evidence was recorded)
Status: acceptance is open. The single-run football-data.org pipeline now has real-data evidence; remaining Iteration 1 blockers are the five-day reliability window, result-availability validation, and manual feature audit. Iteration 2 still lacks representative Sporttery fixtures and household-host runs. Dagster definitions and mocked local runtime are verified, but household-host runtime acceptance remains open. I2-04/I2-05 PostgreSQL migration and API integration checks passed against the dedicated test database on 2026-11-02 CST.

## Purpose and Rules

This document is the acceptance register for closing Iterations 1 and 2. `HANDOFF.md` remains the implementation handoff; this register is authoritative for acceptance status and evidence.

- A passing unit test or successful compilation proves only the code-level check it exercises. It does not substitute for live-data, database, scheduler, or endurance evidence.
- Every check must have a dated evidence record and a named reviewer before it is marked `PASS`.
- `BLOCKED` means a concrete prerequisite is unavailable; its status entry names the blocker and next action. It is not a pass and does not close an iteration.
- Record `FAIL` with the observed result and a remediation issue. After remediation, rerun the same check and retain both records.
- Do not put credentials, credential-bearing URLs, or restricted raw payloads in Git. Record hashes and a secured evidence location instead.
- If a criterion is changed, record the decision and approver here before using the changed criterion.

Status values: `NOT STARTED`, `IN PROGRESS`, `BLOCKED`, `FAIL`, `PASS`.

## Acceptance Register

### Iteration 1: empirical baseline

Code implementation is reported complete. The iteration remains open until the empirical checks and the exit decision below are recorded.

| ID | Check and pass condition | Evidence required | Status / owner |
|---|---|---|---|
| I1-01 | Complete a bounded live football-data.org fetch and run Bronze -> Silver -> Gold -> walk-forward on the resulting data. Record coverage, exclusions, fold boundaries, per-fold and aggregate RPS/Brier/log-loss, and reliability-bin output. | Sanitized run record, manifest/hash, report path and revision. | PASS: live Bronze -> Silver -> Gold -> 15-fold report completed 2026-11-01; pipeline evidence reviewed, metrics are baseline only / reviewer: GitHub Copilot |
| I1-02 | Observe scheduled/live fetches for 5 consecutive natural days; success rate is at least 95%. Define denominator and treatment of provider outages before starting. | Daily attempt/success log, error categories, calculation, reviewer. | IN PROGRESS: Operator Cao Yuheng approved starting in this environment. Observer PID 228095 started 2026-11-02 03:12 CST; one PL attempt is scheduled at 08:00 Asia/Shanghai on each date 2026-11-02 through 2026-11-06 (denominator 5). Each request covers the scheduled date and prior 7 days. As checked at 08:38 CST on 2026-11-02, the observer was alive and the first attempt finished successfully at 08:00:13 CST (HTTP 200, exit 0); current count is 1/5, not a final acceptance result. Payload size 10,461 bytes and SHA-256 `0c660702c71cac58cbe43398ba241f3cba4a038a733bf2213e8f5dd67c8b3221` match its manifest. Provider/command failures, `skipped_locked`, missed dates, and interrupted runs count as failures; no retry. Evidence: `/home/coder/football-validation/daily-fetch/attempts.csv`, `logs/`, and `bronze/`. See [runbook](../runbooks/i1-02-daily-fetch-observation.md) / operator |
| I1-03 | Validate the `kickoff + 4 hours` result-availability assumption against observed usable-result availability, or implement/configure an evidence-backed alternative. No result may enter features before its validated `result_available_at < decision_time`. | Approved sample and source semantics; immutable poll ledger/response hashes; per-match availability intervals and dispositions; policy/code changes and boundary tests if needed; independent sign-off. | BLOCKED: the private plan at `/home/coder/football-validation/i1-03-proposed-plan.json` (mode 0600) is APPROVED; validation confirms 20 fixtures, 2 match weeks, and 247 worst-case requests against budget 247. The release observer is running as PID 309289 in the operator-confirmed no-restart Docker; its state is `/home/coder/football-validation/i1-03-state` (mode 0700), and `run.json` is pinned to plan SHA-256 `7fa26ad4d60e9b8d32768d1f7a4695a90539474b00f5a5e4c7397b4c8b82b5f4`. One separate schedule-fetch test succeeded (HTTP 200, 7 matches, 6,948 bytes; payload SHA-256 `bb04a0ac7be354008fc715ea0ad063a0211b9cda860bd24cd6b19bfa9abf22fc`); it was not a result poll and is outside the 247-request poll budget. No poll has run yet; the first is scheduled for 2026-11-21 14:00 UTC. The plan actually spans six UTC kickoff dates: Nov 21, 22, 23, and 27-29. PID and startup log are recorded under the state directory; the observer log is expected to remain empty until polling starts. The 15-minute protocol runs from `kickoff + 90 minutes` through `kickoff + 8 hours`; preserve last non-FINISHED, first FINISHED, last unusable-result, and first usable-result. Classify `first_result_usable < cutoff` as FALSIFIED; `last_result_unusable >= cutoff` as SUPPORTED; crossing/missing bounds as INCONCLUSIVE. Every pre-registered fixture must be SUPPORTED to pass; any FALSIFIED, INCONCLUSIVE, or RIGHT-CENSORED fixture blocks acceptance. See [I1-03 runbook](../runbooks/i1-03-result-availability-observation.md) / data engineer + operator + independent reviewer |
| I1-04 | Manually audit at least 20 real Gold rows against source match history; all included results satisfy strict-before-decision-time availability. Record exceptions and disposition. | Row identifiers/hashes, audit worksheet, reviewer sign-off. | IN PROGRESS: 20 evenly spaced rows match Bronze/Silver identity, kickoff, score, outcome, and strict-before the implemented `kickoff+4h` assumption; observed publication-time validity remains blocked on I1-03; reviewer sign-off pending. See [audit record](i1-04-manual-audit-2026-11-01.md) / analyst + reviewer |
| I1-05 | Resolve the calibration-chart requirement: either produce a rendered reliability diagram for each required evaluation, or record explicit approval that persisted reliability-bin JSON satisfies Iteration 1. | Example report/chart and signed requirement decision. | PASS: signed decision confirmed; reliability-bin JSON is required and chart rendering is optional / product owner |
| I1-06 | Measure feature-hypothesis-to-out-of-sample-report time for at least 3 representative changes; median is at most 2 hours. Report warm iteration and cold build separately. If over threshold, run the agreed Rust-interactive/Python-sidecar experiment and record its decision. | Timestamped samples, median calculation, build-cache conditions, decision. | PASS: Modeling Lead Cao Yuheng (曹宇恒) accepted the three-feature-group ablation pilot as sufficient current timing evidence on 2026-11-01. Warm Gold-to-report times were 0.737/0.901/0.660 s (median 0.737 s); runner compile/link took 7m37s separately. Re-measure timing after future feature-code changes. See [timing record](i1-06-iteration-time-2026-11-01.md) |
| I1-07 | Close the per-process rate-limit risk operationally: either enforce a single live-fetch process and document it, or implement and test shared coordination before parallel fetches. | [Runbook](../runbooks/i1-07-single-live-fetch.md), configuration, and TC-01..04 evidence. | PASS: nonblocking CLI lock returns 75; TC-01..04 pass; Dagster configured for one concurrent run / reviewer: GitHub Copilot, 2026-11-01 |
| I1-08 | Record Iteration 1 exit decision. Do not claim market superiority or incremental value without a market baseline; that comparison is outside this iteration's current evidence. | Reviewer decision, unresolved limitations, next-phase disposition. | NOT STARTED / technical lead |

**Iteration 1 exit gate:** I1-01 through I1-07 are `PASS`, and I1-08 is signed. If the team changes the visual-chart criterion, I1-05 still must be closed by an explicit decision. A failure on point-in-time integrity blocks acceptance regardless of aggregate model metrics.

### Iteration 2: source, ledger, orchestration, and decision loop

The code foundation is partial. Runtime acceptance cannot start in full until the source and business-contract prerequisites are resolved and a decision producer exists.

| ID | Check and pass condition | Evidence required | Status / owner |
|---|---|---|---|
| I2-01 | Obtain representative, operationally usable Sporttery payload fixtures for normal 2xx, HTTP 567, missing selections, and changed field shapes; implement a versioned parser into `ThreeWayOddsSnapshot`. | Sanitized fixtures or secured references with hashes, parser version, fixture tests. | IN PROGRESS: `sporttery-had-v1` parses the latest valid HAD row by `updateDate`/`updateTime`, keeps source-market time separate from UTC capture time, and has focused fixture tests (7 hermetic ingest library tests pass; the secured-capture test passes when explicitly enabled). Intake metadata confirms Cao Yuheng's internal parser/test approval and `Asia/Shanghai` policy. Error, missing-field, and changed-type examples are synthetic; household-host capture and operational verification remain open. See the secured intake review below / ingestion owner |
| I2-02 | On the household-network host, verify captured body bytes and SHA-256 against the manifest; verify non-2xx bodies are retained and failures exit nonzero. Confirm retry cadence and no secret leakage. | Sanitized capture/error run records, manifest/hash comparison, logs review. | BLOCKED: the five-step operator pack is available at [ops/deploy.md](../../ops/deploy.md); no household-host Sporttery capture/error run has been recorded yet / operator |
| I2-03 | Record 5 consecutive days of scheduled odds capture with success rate at least 95%; define denominator and outage handling before measurement. | Daily run log, schedule/run IDs, rate calculation. | BLOCKED: configure I2-02 capture schedule, then observe five days / operator |
| I2-04 | Against a dedicated PostgreSQL test database, verify migrations; append; identical idempotent retry; conflicting idempotency key; settlement and correction append; update/delete rejection; JSON round-trip; and pagination during concurrent inserts. | Repeatable integration test/script, DB version, command output, resulting row/event checks. | PASS: confirmed on 2026-11-02 against dedicated `football_ledger_test`; migration is idempotent, insert retries preserve data, conflicting keys reject mismatched content, settlement/correction rows append, append-only triggers block updates/deletes, and concurrent cursor pagination yields stable pages / reviewer: GitHub Copilot |
| I2-05 | Exercise the API against PostgreSQL: health, filters, cursor pagination, detail lookup, settlements, not-found, and error handling. Confirm startup does not run DDL and default binding remains loopback. | Integration test results, configuration/bind evidence. | PASS: confirmed on 2026-11-02 against dedicated `football_ledger_test`; health/filter/pagination/detail/settlement/not-found/error cases all respond correctly, API startup does not run migrations, and the default bind remains `127.0.0.1:8080` in the service entrypoint / reviewer: GitHub Copilot |
| I2-06 | On the household host, run Dagster and manually materialize `sporttery_bronze`; verify schedule/timezone, Pipes materialization metadata, failed-run behavior, retries, and secret-safe logs. | Environment versions, Dagster run IDs, materialization and failure/retry evidence. | BLOCKED: `ops/workspace.yaml` passed local Dagster 1.13.28 validation and `ops/requirements.txt` passed uv dry-run; the five-step pack is ready at [ops/deploy.md](../../ops/deploy.md). Household-host deployment, real endpoint run, schedule daemon, and operational log review remain / orchestration owner |
| I2-07 | Decide and document endpoint/resource, schedule time/timezone, retention, credential handling, and match-day definition before endurance measurements begin. | Approved operational configuration/runbook with secrets omitted. | PASS: signed conservative baseline accepted; `SPORTTERY_ODDS_URL`, `Asia/Shanghai`, Beijing match date, 90-day Bronze retention, long-term Silver/Gold, and environment/secret-manager credentials / technical lead + operator |
| I2-08 | Approve the live-decision contract: stable match identity, initial market, market timestamp policy, decision cutoff, and lock-time `decided_at`. | Short decision record and contract/version reference. | PASS: signed `decision-contract-v1` accepted; identity `source + source_match_id`, source timestamp with capture-time fallback, T-10-minute cutoff, lock time is ledger append / product owner + modeling lead |
| I2-09 | Wire capture/parse/normalize and existing feature/model outputs into a controlled decision job that appends immutable market and feature hashes to the ledger. Verify no decision can be represented as complete when required inputs are absent. | End-to-end test/run, linked input/output hashes, ledger row, failure-path evidence. | IN PROGRESS: offline approved-fixture parse → normalize → PostgreSQL ledger test passed 2026-11-02 against `football_ledger_test`; it verifies `matchId`, source/capture times, and rejects omitted market/feature/model fields plus invalid feature/model values without writing rejected rows. Test-only placeholder feature hash, model version, and probabilities were used; no production feature/model outputs or controlled decision producer are wired yet. The approved odds fixture is Sporttery Bundesliga match `2042426` (Berlin Union–Cologne), while local Gold is PL; intake approval is internal parser/test use only. Do not join these records or call a placeholder write a real decision. Next prerequisites: obtain an authorized odds snapshot for a Gold-matched fixture (or matching Bundesliga Gold), expose leakage-safe per-fixture OOS probabilities, then test the linked append and missing-input rejection / pipeline owner + data owner |
| I2-10 | Specify a trusted result source and correction/void policy, then implement settlement as append-only events. Corrections preserve original events and chronology. | Approved policy, integration tests, event history evidence. | BLOCKED: select trusted result source and approve correction policy / ledger owner + product owner |
| I2-11 | After I2-09 is `PASS`, observe at least 10 defined match days: no eligible decision is missing, and replay uses the locked snapshot hashes without recomputation drift. | Fixture schedule/eligibility ledger, daily reconciliation, replay hash comparison, reviewer. | BLOCKED: activate decision producer and define match-day denominator / operator + reviewer |
| I2-12 | After I2-09 passes and the decision-producing Dagster job is active, observe 30 days with no ledger gap caused by scheduling or retry behavior. Any outage must be classified and reconciled; do not silently omit it. | Dagster run history, reconciliation log, gap/outage disposition. | BLOCKED: activate decision-producing Dagster job, then observe 30 days / orchestration owner |
| I2-13 | Confirm API exposure policy. Until authentication and network controls are implemented and tested, keep the service on loopback or a trusted private network; public binding is a failure. | Deployment config and network/access test or approved loopback runbook. | PASS: API owner sign-off confirmed; loopback/trusted private network only, remote access through SSH tunnel/private network / API owner |
| I2-14 | Run `cargo fmt --all -- --check`, `cargo test --workspace`, and strict workspace Clippy after the final acceptance changes. | CI URL or complete command results tied to the accepted revision. | PASS: final worktree checks on 2026-11-02 passed format and strict Clippy; `CARGO_BUILD_JOBS=2 cargo test --workspace` passed 30 tests with 4 ignored by default. All four ignored acceptance tests (Ledger's two DB tests, API DB test, and secured parser fixture) were explicitly run and passed, including the new I2-09 slice. Reviewer: Cao Yuheng (曹宇恒), current-change review confirmed by user on 2026-11-02 |

**Iteration 2 exit gate:** I2-01 through I2-09 and I2-11 through I2-14 are `PASS`; I2-10 must also be `PASS` if automated settlement remains an Iteration 2 deliverable. Otherwise, explicitly defer settlement with an owner and phase, and ensure the iteration's stated scope no longer promises it. Required reliability thresholds are at least 95% scheduled odds capture over the defined observation and 10 match days without missing eligible decisions. The 30-day Dagster check is a separate endurance gate and cannot be inferred from the 10-match-day result.

**I2-09 data-identity blocker:** the secured `fixed-bonus-2xx.json` is the Sporttery market for Bundesliga match `2042426` (Berlin Union vs Cologne); its intake approval permits internal parser/test use only. The available Gold dataset is Premier League. No model/market pair may be created by joining them. Obtain a matching, authorized odds snapshot and an out-of-sample probability for the same fixture before claiming the real feature/model → market → ledger chain.

### I2-01 public GitHub source review (2026-11-02)

- [`zzzyyy1907-jpg/sporttery-json-public`](https://github.com/zzzyyy1907-jpg/sporttery-json-public) contains a large public JSON corpus (1,715 JSON/JSONL files found in the repository tree), but GitHub reports no license metadata and the tree has no README or license file. Public availability alone does not establish permission to reuse the payloads; none were imported.
- [`Johnserf-Seed/SportteryAPI`](https://github.com/Johnserf-Seed/SportteryAPI) is MIT-licensed as software and includes `sample.json`. Its README describes the project as an unofficial reader and states that underlying Sporttery data belongs to the rights holder and that redistribution rights are not granted by the project. Its refresh script fetches one successful upstream response and writes only the parsed JSON body, not HTTP status/headers. The sample therefore does not establish reuse authorization or cover the required response classes.
- Decision: neither GitHub candidate is accepted as an I2-01 fixture source without explicit business/legal approval covering this project’s use and storage. No GitHub payload was imported. The later secured intake is assessed separately below.

### I2-01 secured intake review (2026-11-02)

- The external intake directory `/home/coder/football-validation/sporttery-fixtures/` contains five valid JSON bodies. All five current files match their declared `body_bytes` and SHA-256. `match-list-2xx` and `fixed-bonus-2xx` are attributed to DevTools captures with HTTP 200; their endpoint paths are `getMatchList.qry` and `getFixedBonusV1.qry` respectively.
- `fixed-bonus-567`, `fixed-bonus-missing-selections-2xx`, and `fixed-bonus-changed-shape-2xx` are explicitly marked simulated variants, so they support local parser/error-path tests only and do not prove actual provider responses. The incomplete HAD sample removes the `a` property from a `hadList` row; this endpoint has no `selections` property. The changed-shape sample changes the HAD `h` value from a string to a number while preserving the row keys; it is a synthetic type-change robustness case, not evidence of an upstream schema change.
- The declared pool is `HAD`; all three normal `hadList` records have string `h`, `d`, and `a` prices, and the final record is latest by `updateDate`/`updateTime`. These timestamps have no timezone marker; the approved intake policy interprets them as `Asia/Shanghai` (fixed UTC+08:00) and converts them to UTC. The snapshot keeps this source time separate from `captured_at`; when absent, capture time remains available and `source_updated_at` is `None`.
- Intake metadata records Cao Yuheng (曹宇恒), approval reference `2026-11-02 DevTools抓包`, internal parser/test use, and sanitized Git permission. This supports the stated internal use only; it is not broader authorization to redistribute upstream data. Raw response bodies remain outside Git.
- Decision: local parser implementation and tests may proceed under the recorded internal-use approval. I2-01 remains open until the approved fixture coverage and operational requirements are accepted; synthetic variants are not represented as live upstream evidence.

### I2-01 parser implementation (2026-11-02)

- Parser: `sporttery-had-v1` in `crates/ingest` decodes `getFixedBonusV1.qry`, rejects unsuccessful/error responses and invalid HAD rows, selects the maximum parsed update timestamp (not array position), validates decimal odds, and emits `ThreeWayOddsSnapshot` with separate `source_updated_at` and `captured_at` values.
- Focused verification: `cargo test -p football-ingest` passed (7 library tests, 8 CLI/observer tests, 0 doc tests; one secured-fixture test is ignored by default). `SPORTTERY_FIXTURE_DIR=/home/coder/football-validation/sporttery-fixtures cargo test -p football-ingest parses_approved_external_fixed_bonus_capture -- --ignored` passed separately. The captured fixed-bonus body yielded odds `[2.52, 3.16, 2.35]` and source time `2026-11-01T13:03:01Z`; synthetic 567, missing-selection, and numeric-type-change payloads were rejected. These results establish local parser behavior only, not live endpoint or household-host acceptance.

## Execution Order and Dependencies

1. **Resolve prerequisites:** assign named owners; provision tokens/host/database through approved secret handling; agree chart acceptance, match-day denominator, endpoint schedule/timezone/retention, market identity, and decision cutoff. Keep this document's statuses current.
2. **Close Iteration 1 evidence:** run the live pipeline, perform the 20-row audit, continue I1-02's five-day observation, and collect at least three iteration-time samples. For I1-03, approve the sample/source/request-budget protocol in the [runbook](../runbooks/i1-03-result-availability-observation.md) before deploying its poller; do not infer availability timing from I1-02 snapshots. Coordinate live requests through the shared lock.
3. **Build Iteration 2 offline confidence:** add source fixtures/parser; local PostgreSQL integration tests and mocked Dagster runtime checks are verified. Run Rust gates after source/parser changes.
4. **Validate on the household host:** run capture and Dagster, review hashes/errors/secrets, and begin the 5-day odds-capture observation. Do not count configuration or a declared schedule as a successful run.
5. **Lock business semantics and wire decisions:** complete I2-08 before I2-09. Complete I2-10 only after the result source and correction policy are approved.
6. **Run end-to-end observation:** after I2-09 passes, start the 10-match-day completeness/replay window and 30-day Dagster reliability window. Reconcile every scheduled run and eligible match; investigate every gap.
7. **Sign off:** after all observation windows and final workspace gates pass, attach a review record for each item, then update the phase status in both this register and `HANDOFF.md`. A blocked item prevents the phase from being called accepted.

## Signed Decisions

The following approvals were confirmed as signed on 2026-11-01 and their decision-level checks are marked `PASS`: I1-05, I1-06, I2-07, I2-08, and I2-13. For I1-06, Modeling Lead Cao Yuheng (曹宇恒) accepted the three-ablation pilot as sufficient current timing evidence; future feature-code changes require a new timing measurement. I1-01's single-run pipeline behavior and I1-07 implementation/TC-01..04 evidence were reviewed by GitHub Copilot on 2026-11-01; see the [I1-07 runbook](../runbooks/i1-07-single-live-fetch.md). Cao Yuheng (曹宇恒) completed a second review of the current uncommitted I2-14 worktree, including the I1-03 observer, on 2026-11-01; the exact worktree gates are recorded below. Link durable signature artifacts when available.

## Validation Already Run

### 2026-11-01: historical local workspace checks (superseded for current I2-14)

- Revision: `abe36aa` (`Implement iteration two data and ledger foundation`).
- Commands: `cargo fmt --all -- --check`; `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`.
- Result: all passed; 19 tests passed, 0 failed.
- Coverage includes synthetic walk-forward and point-in-time unit tests, read-only API route construction, raw Sporttery error-body preservation, odds/hash contracts, and ledger probability/hash validation. This is code-level evidence only, not live-data or service integration acceptance.
- Environment blockers observed (values not recorded): `FOOTBALL_DATA_TOKEN`, `SPORTTERY_ODDS_URL`, and `DATABASE_URL` are unset; Python/Dagster, PostgreSQL clients/probes, Docker, and Podman are unavailable. Live source, database, and Dagster checks therefore remain blocked pending team-provided access and runtime hosts.
- Reviewer: Cao Yuheng (曹宇恒), signed 2026-11-01 for revision `abe36aa`. These results do not close current I2-14 after I1-07 code changes.

### 2026-11-01: I1-07 single-fetch lock

- Code: `football-ingest` acquires `/tmp/football-live-fetch.lock` before either live-fetch command; lock contention exits `75`. Dagster instance configuration sets `max_concurrent_runs: 1`.
- Tests: `cargo fmt --all -- --check`; `cargo test -p football-ingest`; `cargo test --workspace` (20 passed, 0 failed); `cargo clippy --workspace --all-targets -- -D warnings` all passed.
- TC-01/02: with an external `flock` holder, both `matches` and `sporttery` exited `75` without starting a request.
- TC-03: a second nonblocking lock was rejected while held; after terminating the holder, a nonblocking lock succeeded and the CLI proceeded to token validation.
- TC-04: a simulated closed-loopback-port request using dummy token and URL credentials exited `1`; captured output contained the generic `URL omitted` message and none of the dummy credentials, URL markers, or `Authorization` string.
- Reviewer: GitHub Copilot, 2026-11-01. The response-header throttle change followed the earlier review; Cao Yuheng (曹宇恒) subsequently confirmed the current worktree review and I2-14 gates were rerun as recorded below. Dagster config loading on the household host remains part of I2-06.

### 2026-11-01: football-data.org live analysis and rate headers

- Credential handling: loaded locally from ignored `.env` (mode `0600`); token value was not recorded. One bounded `PL` request for `2025-08-01` through `2026-06-30` returned HTTP 200 and 380 matches.
- Bronze: 391,877 bytes; manifest SHA-256 `1b046719d88b2ec5340c4a311822c1f7abea8a03c4021eb7cad0dbc67defd913`; independently calculated hash and byte count matched the manifest.
- Silver and Gold: each contains 380 rows. Local temporary artifacts: `/tmp/football-validation-2026-11-01/silver/PL.parquet` (SHA-256 `6dde28057149bfe7e78733297b6a01c093597f09c5e053fcbef65a660005dfdd`) and `/tmp/football-validation-2026-11-01/gold/PL.parquet` (SHA-256 `d38cd0f97b89443d9f9d865531ba89b242e2feab2a69f387c5a376751ccd898b`). These are outside Git; preserve them if the audit needs them.
- Walk-forward: 15 folds, 300 out-of-sample samples; RPS `0.2384652489`, Brier `0.6930115634`, log-loss `3.2407041838`; report SHA-256 `9d2f5214abbdb52e93b0cb19dc318d1eb6674a512cba2e621815ad875f548f6a` at `/tmp/football-validation-2026-11-01/reports/pl-walk-forward.json`.
- Interpretation: this closes the single-run pipeline behavior criterion pending reviewer sign-off. It is not evidence of market outperformance or result-availability correctness; the four-hour availability assumption remains under I1-03.
- Rate headers: one metadata request returned `x-requests-available-minute: 9`. The client now honors this header and `Retry-After`, with a 6-second floor and safe 60-second wait when no requests remain. Added unit coverage for header parsing/fallbacks; full workspace gates passed with 21 tests.
- I1-02 observation: this single bounded run does not count as a five-day scheduled reliability window. The team must define the schedule/denominator and run all five days.

### 2026-11-01: current worktree gates and two-job build probe

- Revision/worktree: `abe36aa` with uncommitted changes; Cao Yuheng (曹宇恒) confirmed review of this worktree on 2026-11-01.
- Commands: `cargo fmt --all -- --check`; `CARGO_BUILD_JOBS=2 cargo test --workspace`; `CARGO_BUILD_JOBS=2 cargo clippy --workspace --all-targets -- -D warnings`.
- Result: all gates passed; 21 tests passed, 0 failed.
- Build probe: `CARGO_BUILD_JOBS=2 cargo test --workspace --no-run --timings` completed successfully in 26.48 seconds and wrote a Cargo timing report under `target/cargo-timings/`. This was a warm/current-cache run, not a controlled comparison with one job or a cold build; keep repository default at one job until comparable timing and memory measurements justify changing it.

### 2026-11-01: I1-03 observer implementation

- Added `football-ingest observe-results`, `summarize-results`, and `validate-results`. The observer reuses the authenticated/rate-aware football-data.org client, records request start and body-complete observation timestamps, preserves raw Bronze responses/manifests, and serializes each live fetch through the shared `/tmp/football-live-fetch.lock`.
- The plan gate requires status `APPROVED`, named data/technical lead, data engineer, and operator, explicit sample/source/terms/schedule approvals, at least 20 fixtures across two match weeks, and an approved request budget at least equal to the computed worst-case query count. Observer state is bound to the exact plan SHA-256; changing the plan requires a new state directory.
- Per-fixture interval logic records last non-FINISHED, first FINISHED, last unusable-result, and first usable-result observations. It distinguishes FALSIFIED, SUPPORTED, INCONCLUSIVE, and RIGHT_CENSORED; an unobserved usable result cannot be counted as supported.
- Verification: `cargo fmt --all -- --check`; `cargo test --workspace` (28 passed); `cargo clippy --workspace --all-targets -- -D warnings`; and offline `target/debug/football-ingest validate-results --plan docs/templates/i1-03-plan.json` all passed. Eight observer/CLI tests cover interval boundaries, scheduling, I1-02 priority, budget, approvals, result-state separation, and plan-hash pinning.
- Reviewer: Cao Yuheng (曹宇恒) completed a second review of this current uncommitted worktree on 2026-11-01 and approved the I1-03 observer changes; these same workspace gates passed after the review.
- No live provider request was made. The checked-in plan is `PENDING` with no fixture sample or approved budget; the observer is not deployed or running. I1-03 remains BLOCKED. Current code-server container has no service manager, so real collection must run on a persistent supervised host after approvals.

### 2026-11-01: I1 empirical follow-up prerequisites

- The 380-match Bronze response remains available under `/tmp/football-validation-2026-11-01/`; a sample record has `lastUpdated` set to 2026-06-07 although its kickoff was 2025-08-15. This field is not an observed first-publication time, so it cannot validate the four-hour assumption. No historical availability-time series is present in the payload.
- The current container runs code-server as PID 1 and has no `cron`, `crontab`, or `systemctl`; no persistent five-day schedule was started and no extra live request was made. Run the observation on an operator-managed persistent host after confirming the proposed denominator above.

### 2026-11-01: I1-04 and I1-06 pilots

- I1-04: 20 evenly spaced Gold rows were cross-checked against source Bronze and Silver. All 20 matched IDs, teams, kickoff, score/outcome, and decision-before-implemented-availability checks. This does not establish observed first-publication time; see the [audit record](i1-04-manual-audit-2026-11-01.md). Independent reviewer sign-off remains pending.
- I1-06: three one-feature-group ablations were run sequentially from the existing Gold Parquet through the fixed default 15-fold walk-forward to separate OOS reports. Warm times were 737, 901, and 660 ms (median 737 ms). The temporary runner's code/build/link cost was 7m37s; this is reported separately and is not included in the three warm samples. These pilot measurements do not include hypothesis implementation/recompilation and require modeling-lead review before they can count as representative samples. See the [timing record](i1-06-iteration-time-2026-11-01.md).

### 2026-11-02: PostgreSQL test database connectivity and ledger/API acceptance

- Target: dedicated database `football_ledger_test`, PostgreSQL 15.19; connection identity was verified as `testuser`. The connection string and password are intentionally not recorded.
- Environment: `.env` has mode `0600`, contains one `DATABASE_URL`, and is ignored by Git. `football-pg-test` resolves from the development container.
- Read-only checks: `pg_isready -d "$DATABASE_URL"` reported that the server accepts connections; `psql "$DATABASE_URL" -XAtqc 'SELECT 1'` returned `1`; a read-only identity query confirmed the database and server version.
- Acceptance runs:
  - `cd /home/coder/football-analytics && set -a && . ./.env && set +a && cargo test -p football-ledger -- --ignored --nocapture`
  - `cd /home/coder/football-analytics && set -a && . ./.env && set +a && cargo test -p football-api -- --ignored --nocapture`
- Result: both commands passed end-to-end against the dedicated database. Ledger checks covered migrations, idempotent retry on the same key, conflicting content rejection, settlement/correction append semantics, append-only update/delete rejection, and concurrent cursor pagination. API checks covered health, filter validation, list/detail/settlement lookups, not-found handling, invalid range rejection, and internal-error response shape. The service entrypoint in [crates/api/src/main.rs](../../crates/api/src/main.rs) does not run migrations at startup and defaults to `127.0.0.1:8080`.
- Re-run: repeated successfully on 2026-11-02 CST. `cargo fmt --all -- --check`, `cargo test --workspace` (28 passed), and `cargo clippy --workspace --all-targets -- -D warnings` also passed; the default workspace run skips these database tests because they are explicitly marked `#[ignore]`.
- Decision: I2-04 and I2-05 close as PASS on this dedicated PostgreSQL test database based on the above commands and row-level evidence.

### 2026-11-02: local Dagster runtime smoke test

- Environment: Python 3.12.16 installed under the user-managed uv cache; Dagster/Dagster Webserver 1.13.28 installed in an isolated virtual environment outside the repository. No system Python packages were changed. `DAGSTER_HOME` points to a user-local directory using the repository's `dagster.yaml`.
- Definition checks from `dagster/`: `dagster definitions validate -m definitions` passed; `dagster asset list -m definitions` listed `sporttery_bronze`. The schedule resolves to `0 8 * * *`, timezone `Asia/Shanghai`; it is STOPPED in this local instance and no unattended schedule was enabled.
- Success path: `dagster job execute -m definitions -j sporttery_capture_job -d "$PWD"` ran against a temporary HTTP mock bound only to `127.0.0.1`. Run `a920c5ed-a4ac-4e09-8057-32af6662baf1` completed `RUN_SUCCESS`; Dagster received the `sporttery_bronze` Pipes materialization. The Bronze payload was 47 bytes and its manifest SHA-256 matched the actual payload (`849e48a07583efb69d81807fb0fd4d2f260c52d059e6d94bcaaedd1ff898ff10`).
- Failure/retry path: run `20634e67-106f-4a25-af9e-210c5d2d699e` used a temporary HTTP 567 mock and explicitly enabled executor retries. Dagster attempted the step three times (initial attempt plus two retries, 30-second delay), then ended `RUN_FAILURE`; each attempt preserved a 46-byte raw response and a manifest with status 567. All three payload byte counts and SHA-256 values matched their manifests. Captured logs did not contain `DATABASE_URL` or the mock host; no provider credentials were supplied to the local test.
- Scope: these are local mock-runtime checks only. They do not satisfy I2-02 or I2-06's household-network deployment, live endpoint, supervised schedule, real Pipes materialization, host retry, or operational log-review requirements. I2-06 remains BLOCKED until those checks run on the household host.

### 2026-11-02: local checkpoint verification before handoff

- Worktree base: `d6452cd`; checks were run before the local handoff commit.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- `CARGO_BUILD_JOBS=2 cargo test --workspace` passed: 28 tests passed, 2 database acceptance tests ignored by default; doc-tests passed.
- `CARGO_BUILD_JOBS=2 cargo clippy --workspace --all-targets -- -D warnings` passed.
- With the ignored `.env` loaded locally, `cargo test -p football-ledger -- --ignored --nocapture` passed against dedicated `football_ledger_test`; `cargo test -p football-api -- --ignored --nocapture` also passed. No connection string or credential is recorded here.
- With `SPORTTERY_FIXTURE_DIR=/home/coder/football-validation/sporttery-fixtures`, `cargo test -p football-ingest parses_approved_external_fixed_bonus_capture -- --ignored --nocapture` passed. The fixture remains outside Git.
- Scope: these checks verify parser behavior, database/API contracts, and workspace code gates. They do not verify the production I2-09 controlled decision producer using real feature/model outputs, household-host capture, or Dagster deployment.

### 2026-11-02: I2-09 approved-fixture ledger slice

- Test: `cargo test -p football-ledger approved_sporttery_fixture_flows_to_ledger_and_rejects_missing_inputs -- --ignored --nocapture`, with `DATABASE_URL` loaded from the ignored local `.env` and `SPORTTERY_FIXTURE_DIR=/home/coder/football-validation/sporttery-fixtures`.
- Target: dedicated `football_ledger_test`; the test asserts the database identity before writing. Approved `fixed-bonus-2xx.json` remains outside Git.
- Result: passed. `sporttery-had-v1` parsed `matchId` `2042426`; normalization preserved source time `2026-11-01T13:03:01Z` and capture time `2026-11-02T08:00:00Z` in the appended market JSON. The ledger row retained the market and feature snapshot hashes.
- Failure path: omission of market, feature hash, model version, or model probabilities fails required-field deserialization. Empty feature hash and invalid/missing model values are rejected by ledger validation; a database query confirmed no row was written for either rejected idempotency key.
- Scope: feature hash, model version, and model probabilities are explicit acceptance-test placeholders, not actual inference outputs. This verifies the pre-Pipes parser/normalize/ledger contract only; it does not close I2-09's controlled producer, household-host runtime, or endurance criteria.

## Evidence Record Template

For each ID, add a dated record below or link a sanitized, version-controlled artifact. Keep raw or restricted data outside Git and record its content hash and secured location.

```text
ID:
Status:
Owner / reviewer:
Run date and timezone:
Accepted repository revision:
Environment and dependency versions:
Command, job, or Dagster run ID:
Inputs and hashes:
Expected result:
Observed result:
Evidence path or secured reference:
Failures, exceptions, and remediation:
Decision / sign-off:
```

## Acceptance Management Panel Decision

**Do not build a dedicated acceptance panel for Iterations 1-2.** At the current scale, this Markdown register in Git provides review history, ownership, dependencies, and evidence links with less operational surface than a new service. Existing API contracts are for football data and the ledger; they should not be expanded solely to track project acceptance.

Reconsider a panel or generated status view only when several people update this register frequently, evidence is arriving from automated runs, or manual cross-iteration reporting becomes a recurring bottleneck. First automate extraction from CI/Dagster artifacts or generate a read-only summary from the register; build a UI only if those cheaper approaches do not meet the need. Any future UI must remain a consumer of an acceptance data contract and must not query the ledger or storage directly.

## Iterations 3-4 Boundary

This register closes Iterations 1 and 2 only. Iteration 3/4 proposals (pg_duckdb, web panel, experiment isolation, observability, backup/restore, and mobile validation) are future acceptance plans, not current passes or blockers. Before each iteration begins, create its checks with measurable thresholds, evidence locations, owners, and explicit exit gates. Do not mark future gates complete based on this document.
