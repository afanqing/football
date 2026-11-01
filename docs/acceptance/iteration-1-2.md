# Iteration 1-2 Acceptance Plan

Last reviewed: 2026-11-01
Base revision: `abe36aa` (working tree includes I1-07 implementation changes)
Status: acceptance is open. The single-run football-data.org pipeline now has real-data evidence; remaining Iteration 1 blockers are the five-day reliability window, result-availability validation, and manual feature audit. Iteration 2 still lacks representative Sporttery fixtures, household-host runs, PostgreSQL service, and Dagster runtime.

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
| I1-02 | Observe scheduled/live fetches for 5 consecutive natural days; success rate is at least 95%. Define denominator and treatment of provider outages before starting. | Daily attempt/success log, error categories, calculation, reviewer. | IN PROGRESS: Operator Cao Yuheng approved starting in this environment. Observer PID 228095 started 2026-11-02 03:12 CST; one PL attempt is scheduled at 08:00 Asia/Shanghai on each date 2026-11-02 through 2026-11-06 (denominator 5). Each request covers the scheduled date and prior 7 days. Provider/command failures, `skipped_locked`, missed dates, and interrupted runs count as failures; no retry. Evidence: `/home/coder/football-validation/daily-fetch/attempts.csv`, `logs/`, and `bronze/`. The container process does not auto-restart after container restart; no scheduled fetch has completed yet. See [runbook](../runbooks/i1-02-daily-fetch-observation.md) / operator |
| I1-03 | Validate the `kickoff + 4 hours` result-availability assumption against observed usable-result availability, or implement/configure an evidence-backed alternative. No result may enter features before its validated `result_available_at < decision_time`. | Approved sample and source semantics; immutable poll ledger/response hashes; per-match availability intervals and dispositions; policy/code changes and boundary tests if needed; independent sign-off. | BLOCKED: football-data.org `lastUpdated` is not first-publication time. I1-02's once-daily 08:00 snapshots cannot prove first availability. Rust observer and offline summarizer are implemented and `cargo test -p football-ingest` passes, but no live I1-03 poll has run. The plan template remains PENDING with no approved fixtures or request budget. Protocol: 15-minute polling from `kickoff + 90 minutes` through `kickoff + 8 hours`; preserve last non-FINISHED, first FINISHED, last unusable-result, and first usable-result observations. Classify `first_result_usable < cutoff` as FALSIFIED; `last_result_unusable >= cutoff` as SUPPORTED; crossing/missing bounds as INCONCLUSIVE. Proposed minimum is 20 pre-registered PL fixtures across at least two match weeks, pending approval. Every pre-registered fixture must be SUPPORTED to pass; any FALSIFIED, INCONCLUSIVE, or RIGHT-CENSORED fixture blocks acceptance. Approve named roles, source semantics, request budget, and schedule before deploying on a persistent host. See [I1-03 runbook](../runbooks/i1-03-result-availability-observation.md) / data engineer + operator + independent reviewer |
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
| I2-01 | Obtain representative, operationally usable Sporttery payload fixtures for normal 2xx, HTTP 567, missing selections, and changed field shapes; implement a versioned parser into `ThreeWayOddsSnapshot`. | Sanitized fixtures or secured references with hashes, parser version, fixture tests. | BLOCKED: obtain permitted representative payload fixtures, then implement parser / ingestion owner |
| I2-02 | On the household-network host, verify captured body bytes and SHA-256 against the manifest; verify non-2xx bodies are retained and failures exit nonzero. Confirm retry cadence and no secret leakage. | Sanitized capture/error run records, manifest/hash comparison, logs review. | BLOCKED: schedule a run on household-network host / operator |
| I2-03 | Record 5 consecutive days of scheduled odds capture with success rate at least 95%; define denominator and outage handling before measurement. | Daily run log, schedule/run IDs, rate calculation. | BLOCKED: configure I2-02 capture schedule, then observe five days / operator |
| I2-04 | Against a dedicated PostgreSQL test database, verify migrations; append; identical idempotent retry; conflicting idempotency key; settlement and correction append; update/delete rejection; JSON round-trip; and pagination during concurrent inserts. | Repeatable integration test/script, DB version, command output, resulting row/event checks. | BLOCKED: provision dedicated PostgreSQL test service / ledger owner |
| I2-05 | Exercise the API against PostgreSQL: health, filters, cursor pagination, detail lookup, settlements, not-found, and error handling. Confirm startup does not run DDL and default binding remains loopback. | Integration test results, configuration/bind evidence. | BLOCKED: provision PostgreSQL test service, then run API integration checks / API owner |
| I2-06 | On the household host, run Dagster and manually materialize `sporttery_bronze`; verify schedule/timezone, Pipes materialization metadata, failed-run behavior, retries, and secret-safe logs. | Environment versions, Dagster run IDs, materialization and failure/retry evidence. | BLOCKED: install/run Dagster on household-network host / orchestration owner |
| I2-07 | Decide and document endpoint/resource, schedule time/timezone, retention, credential handling, and match-day definition before endurance measurements begin. | Approved operational configuration/runbook with secrets omitted. | PASS: signed conservative baseline accepted; `SPORTTERY_ODDS_URL`, `Asia/Shanghai`, Beijing match date, 90-day Bronze retention, long-term Silver/Gold, and environment/secret-manager credentials / technical lead + operator |
| I2-08 | Approve the live-decision contract: stable match identity, initial market, market timestamp policy, decision cutoff, and lock-time `decided_at`. | Short decision record and contract/version reference. | PASS: signed `decision-contract-v1` accepted; identity `source + source_match_id`, source timestamp with capture-time fallback, T-10-minute cutoff, lock time is ledger append / product owner + modeling lead |
| I2-09 | Wire capture/parse/normalize and existing feature/model outputs into a controlled decision job that appends immutable market and feature hashes to the ledger. Verify no decision can be represented as complete when required inputs are absent. | End-to-end test/run, linked input/output hashes, ledger row, failure-path evidence. | BLOCKED: finish I2-01 and I2-08; requires working ledger/database integration / pipeline owner |
| I2-10 | Specify a trusted result source and correction/void policy, then implement settlement as append-only events. Corrections preserve original events and chronology. | Approved policy, integration tests, event history evidence. | BLOCKED: select trusted result source and approve correction policy / ledger owner + product owner |
| I2-11 | After I2-09 is `PASS`, observe at least 10 defined match days: no eligible decision is missing, and replay uses the locked snapshot hashes without recomputation drift. | Fixture schedule/eligibility ledger, daily reconciliation, replay hash comparison, reviewer. | BLOCKED: activate decision producer and define match-day denominator / operator + reviewer |
| I2-12 | After I2-09 passes and the decision-producing Dagster job is active, observe 30 days with no ledger gap caused by scheduling or retry behavior. Any outage must be classified and reconciled; do not silently omit it. | Dagster run history, reconciliation log, gap/outage disposition. | BLOCKED: activate decision-producing Dagster job, then observe 30 days / orchestration owner |
| I2-13 | Confirm API exposure policy. Until authentication and network controls are implemented and tested, keep the service on loopback or a trusted private network; public binding is a failure. | Deployment config and network/access test or approved loopback runbook. | PASS: API owner sign-off confirmed; loopback/trusted private network only, remote access through SSH tunnel/private network / API owner |
| I2-14 | Run `cargo fmt --all -- --check`, `cargo test --workspace`, and strict workspace Clippy after the final acceptance changes. | CI URL or complete command results tied to the accepted revision. | PASS: Cao Yuheng (曹宇恒) completed a second review of the current uncommitted worktree, including the I1-03 Rust observer changes, on 2026-11-01; final format, 28 workspace tests, and strict Clippy all pass / Rust owner |

**Iteration 2 exit gate:** I2-01 through I2-09 and I2-11 through I2-14 are `PASS`; I2-10 must also be `PASS` if automated settlement remains an Iteration 2 deliverable. Otherwise, explicitly defer settlement with an owner and phase, and ensure the iteration's stated scope no longer promises it. Required reliability thresholds are at least 95% scheduled odds capture over the defined observation and 10 match days without missing eligible decisions. The 30-day Dagster check is a separate endurance gate and cannot be inferred from the 10-match-day result.

## Execution Order and Dependencies

1. **Resolve prerequisites:** assign named owners; provision tokens/host/database through approved secret handling; agree chart acceptance, match-day denominator, endpoint schedule/timezone/retention, market identity, and decision cutoff. Keep this document's statuses current.
2. **Close Iteration 1 evidence:** run the live pipeline, perform the 20-row audit, continue I1-02's five-day observation, and collect at least three iteration-time samples. For I1-03, approve the sample/source/request-budget protocol in the [runbook](../runbooks/i1-03-result-availability-observation.md) before deploying its poller; do not infer availability timing from I1-02 snapshots. Coordinate live requests through the shared lock.
3. **Build Iteration 2 offline confidence:** add source fixtures/parser; add repeatable PostgreSQL integration checks; verify API routes and security defaults. Run Rust gates after changes.
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
