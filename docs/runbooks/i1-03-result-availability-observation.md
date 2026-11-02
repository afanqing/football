# I1-03 Result Availability Observation

## Purpose and boundary

I1-03 tests whether the current result-availability assumption, `kickoff + 4 hours`, is conservative enough for point-in-time features. It is separate from I1-02: daily snapshots measure scheduled-fetch reliability and cannot establish result publication or availability time.

The provider's `lastUpdated` is not accepted as first-publication time. Polling observes state transitions; unless the provider supplies an independently validated first-publication timestamp, the evidence is an interval, not an exact publication instant. Do not describe an interval bound as the exact first-publication time.

## Definitions and per-match decision

All timestamps are UTC. For each preselected Premier League match, define:

- `kickoff_at`: provider `utcDate` for the match.
- `cutoff_at`: `kickoff_at + 4 hours`.
- `last_non_finished_observed_at`: response-received time of the last successful observation with status other than `FINISHED`.
- `first_finished_observed_at`: response-received time of the first successful observation with status `FINISHED`, regardless of score completeness.
- `last_result_unusable_observed_at`: response-received time of the last successful observation where a complete result was not yet usable.
- `first_result_usable_observed_at`: response-received time of the first successful observation with `FINISHED` and all required full-time scores present.

Keep both status-transition fields and result-usability fields. A `FINISHED` status without complete scores is not a usable result and must not be treated as the first available training label. Preserve request-start and response-received timestamps separately; use response-received time as `observed_at`, since that is when the client could first consume the response.

For usable-result availability, the observation interval is `(last_result_unusable_observed_at, first_result_usable_observed_at]`:

| Observation | Per-match classification |
|---|---|
| `first_result_usable_observed_at < cutoff_at` | FALSIFIED: usable result was observed before the four-hour cutoff. |
| `last_result_unusable_observed_at >= cutoff_at` | SUPPORTED: result was still unusable at or after the cutoff. |
| Interval crosses the cutoff, a required bound is missing, or a poll gap spans the cutoff | INCONCLUSIVE: do not count as support. |

Equality is conservative: an upper bound equal to the cutoff with a lower bound before it is INCONCLUSIVE. An upper bound before the cutoff falsifies the assumption. A lower bound equal to the cutoff supports it. A match with no usable result by the observation deadline is RIGHT-CENSORED, not a pass; investigate and retain it in the sample disposition.

## Proposed observation protocol (approval required)

The following are proposed defaults, not approved evidence or a deployed schedule. The operator and data/technical lead must approve the sample, provider semantics, and request budget before launch.

1. Pre-register at least 20 completed Premier League fixtures spanning at least two separate match weeks. Select fixtures/windows before observing outcomes; include every eligible fixture in each declared window, not only convenient results. The 20-match / two-week threshold is a proposed minimum and requires explicit approval.
2. Confirm the provider's `FINISHED` and score-field semantics, competition query coverage, timezone handling, terms, and request budget. Estimate worst-case calls as polling rounds times requests per round. For a `kickoff + 90 minutes` through `kickoff + 8 hours` window at 15-minute intervals, this is up to 27 polling rounds per match or fixture batch, before any retries; do not retry failed polls silently.
3. Begin polling by `kickoff + 90 minutes`, every 15 minutes, and continue until a usable result is first observed or `kickoff + 8 hours`. If the result is still unusable at the deadline, mark RIGHT-CENSORED and investigate. A source change or operational outage must not silently remove a fixture from the pre-registered sample.
4. Coordinate with I1-02 and the shared `/tmp/football-live-fetch.lock`. Never run concurrent live-fetch commands or risk the scheduled I1-02 attempt. Stagger I1-03 polling where practical. Record lock contention/failed polls as gaps; a gap spanning `cutoff_at` makes the match INCONCLUSIVE unless another validated source resolves it.
5. Save every successful raw response outside Git with restricted permissions. Record a SHA-256 per response and a sanitized manifest. Never store tokens, credential-bearing URLs, or raw restricted payloads in the repository.
6. Preserve every poll outcome, including HTTP/provider errors, timeout, lock contention, and missing scheduled polls. Do not interpolate a status across a gap or use provider `lastUpdated` as publication time.

## Local implementation and operation

The observer is implemented as Rust subcommands in `football-ingest`; it uses the existing HTTP client, rate-limit headers, append-only Bronze snapshots, and `/tmp/football-live-fetch.lock`. No Python/Dagster runtime is required. Each poll slot groups eligible fixtures by UTC kickoff date so the API is queried once per distinct date per slot. The plan validator computes that worst-case request count before any live run.

1. Copy [the plan template](../templates/i1-03-plan.json) to a private, persistent location outside Git. Populate the pre-registered fixtures with numeric football-data.org match IDs, exact UTC kickoffs, and approved match-week labels. Keep the plan status `PENDING` while approvals are outstanding.
2. Before approval, validate the fixture count and request estimate without network access:

	```sh
	cargo run -p football-ingest -- validate-results --plan /secure/path/i1-03-plan.json
	```

3. Obtain and record the named data/technical lead, data engineer, and operator approvals. Set `status` to `APPROVED`, record `approved_at_utc` and a non-sensitive `approval_reference`, set all four approval booleans to `true`, and ensure `request_budget_limit` is at least the printed worst-case count. The implementation refuses live polling if any gate, sample minimum, match-week minimum, or budget check fails.
4. On a persistent operator-managed host, export `FOOTBALL_DATA_TOKEN` through the approved secret mechanism and start the observer:

	```sh
	cargo run -p football-ingest -- observe-results \
	  --plan /secure/path/i1-03-plan.json \
	  --state /secure/path/i1-03-state
	```

	The command remains active until the registered schedule completes. It starts the first poll at `kickoff + 90 minutes`, repeats every 15 minutes from that kickoff-relative time, and stops that match after the first usable result or `kickoff + 8 hours`. During I1-02's 2026-11-02 through 2026-11-06 window, I1-03 deliberately records a missed poll and makes no request from 07:55 through 08:10 Asia/Shanghai, protecting the daily 08:00 fetch. It holds the shared live-fetch lock only during a network fetch, never while sleeping. A slot delayed by more than five minutes is recorded as missed rather than replayed; provider errors and lock contention are recorded as failures and are not silently retried. Run it under a persistent supervisor on the operator host; the current code-server container has no service manager and a process here will not survive container restart.
5. After collection, derive intervals offline:

	```sh
	cargo run -p football-ingest -- summarize-results \
	  --plan /secure/path/i1-03-plan.json \
	  --state /secure/path/i1-03-state
	```

	`run.json` binds the observer state to the exact approved plan bytes by SHA-256; a resumed run or summary using a different plan is rejected. `polls.jsonl` is the append-only per-poll evidence ledger; Bronze payloads and manifests are stored below the state directory. `intervals.json` is derived output and can be regenerated from that same plan. Restrict the state directory to the operator; raw payloads are not for Git.

Implementation status (2026-11-02): poller, plan validation, interval summarizer, and boundary tests are present in the repository. `cargo test -p football-ingest` passed locally. The checked-in plan template remains `PENDING`; the approved private plan is deployed and waiting. No live I1-03 result poll has run, so no empirical availability claim may be made yet.

### Deployment check (2026-11-02)

- The operator confirmed this Docker will not restart during the observation window. Because no service manager is installed, the release observer was detached with `nohup` as PID `309289`; its PID is in `/home/coder/football-validation/i1-03-state/observer.pid`. The observer holds `observer.lock`; `supervisor.log` records startup and stdout/stderr go to `observer.log`. Check the PID and logs daily; do not silently restart after an interruption.
- The private state directory is mode `0700`; state files are mode `0600`. `run.json` binds the run to the approved plan SHA-256 `7fa26ad4d60e9b8d32768d1f7a4695a90539474b00f5a5e4c7397b4c8b82b5f4`. `observer.log` and `polls.jsonl` are empty/not yet created before the first poll; this is expected while all fixtures are in the future.
- A separate `matches --competition PL --date-from 2026-11-21 --date-to 2026-11-21` network/storage test returned 7 matches (HTTP 200), 6,948 bytes, SHA-256 `bb04a0ac7be354008fc715ea0ad063a0211b9cda860bd24cd6b19bfa9abf22fc`. Its sanitized summary is `/home/coder/football-validation/i1-03-deploy-test/network-test.log`; the payload and manifest are in the same private directory, all mode `0600`. This is a connectivity/logging test, not an I1-03 result poll and not part of the 247-request budget.
- The plan has 20 fixtures on six UTC kickoff dates: 2026-11-21 (7), 11-22 (2), 11-23 (1), 11-27 (1), 11-28 (3), and 11-29 (6). The first observation slot is 2026-11-21 14:00 UTC. No result poll has run yet; I1-03 remains BLOCKED pending all dispositions and independent review.
- `scripts/validate_i1_03_results.sh` is prepared for the post-window audit. Synthetic testing produced `READY_FOR_INDEPENDENT_REVIEW` for 20 SUPPORTED fixtures and `BLOCKED` for 20 RIGHT_CENSORED fixtures; a structured `Cookie` field was rejected without echoing its value. This is verifier testing only, not evidence from the live observation. After the final deadline, run it against the approved plan and state; the generated report always leaves independent sign-off `PENDING`.

## Data record

Maintain one poll ledger row per attempt with at least:

```text
sample_id, match_id, competition, kickoff_at_utc, request_started_at_utc,
response_received_at_utc, observed_at_utc, poll_status, http_status,
match_status, home_score, away_score, result_usable, payload_sha256,
artifact_reference, error_category, poll_sequence
```

Produce one derived row per match with the four transition/usability timestamps above, `cutoff_at`, poll count, largest poll gap, interval classification, censoring/exception reason, and reviewer disposition. Keep source fields and derived classification reproducible from the poll ledger and immutable response hashes.

## Acceptance and evidence

Before collection, record named approvers for the sample threshold/window, source-field semantics, request budget, and poller version. Acceptance requires:

- Approved sample target reached: proposed minimum is 20 fixtures across at least two match weeks.
- Every pre-registered fixture has a disposition; errors, missing polls, right-censoring, and inconclusive intervals are reported, not dropped.
- Every pre-registered fixture in the approved sample is classified SUPPORTED with `last_result_unusable_observed_at >= cutoff_at`. Any FALSIFIED, INCONCLUSIVE, or RIGHT-CENSORED fixture prevents acceptance; it cannot be dropped or masked by other matches.
- Raw response hashes, poll ledger, derived intervals, source semantics, and exceptions are independently reviewed and signed.
- Point-in-time code/tests enforce that a result is included only when its validated `result_available_at < decision_time`. Test immediately-before, equal-to, and immediately-after boundaries. If evidence falsifies the current assumption, update the availability policy/code and rerun affected I1-04 audits and point-in-time tests before acceptance.

I1-03 stays BLOCKED until the approved observation evidence and code/policy consequences are reviewed. If the literal requirement is an exact first-publication timestamp, polling alone cannot meet it; obtain a provider audit timestamp with validated semantics or revise the evidence requirement through an explicit signed decision.

## To-do before first poll

- [x] Data/technical lead approves the proposed 20-fixture, two-match-week minimum or records a replacement target.
- [x] Operator confirms the target fixture windows and no-restart Docker/supervision assumption for this run.
- [ ] Data engineer verifies status, score, timestamp, and query semantics; explicitly rejects `lastUpdated` as first-publication evidence unless independently validated.
- [ ] Operator calculates and gets approval for worst-case request budget and confirms provider terms/limits.
- [x] Implement and version the Rust poller, append-only poll ledger, immutable Bronze response hashes, explicit failure/gap outcomes, offline plan validator, and interval summarizer; `cargo test -p football-ingest` passes.
- [x] Copy the plan template to a private location, register at least 20 real fixtures across at least two match weeks, fill named approvers and flags, and set a request budget no lower than the validator's worst-case estimate.
- [x] Dry-run `validate-results` on the populated plan and review its request estimate before setting approval status.
- [ ] Run one supervised poll slot on the operator-managed persistent host and verify a single ledger entry, Bronze hash/manifest match, and no secret in logs before enabling the full registered window.
- [ ] Coordinate cadence with I1-02 and verify shared-lock behavior, with I1-02's scheduled fetch taking priority.
- [x] Pre-register fixtures, start/end rules, cutoff classification, and approvers before the first live poll.

## To-do after observation

- [ ] After the final fixture deadline and observer exit, run `scripts/validate_i1_03_results.sh` with the approved private plan and state directory; review the generated report and attach it for independent sign-off. The script is a pre-review aid, not an automatic acceptance decision.
- [ ] Recompute transition fields and classifications from immutable poll records; verify row counts, hashes, clock/timezone handling, and all gaps.
- [ ] Publish the per-match interval table, sample summary, exceptions, and signed reviewer decision in the acceptance evidence record.
- [ ] If falsified or inconclusive, keep I1-03 BLOCKED, revise the availability policy using conservative evidence, update code/tests, and repeat affected I1-04 audit rows.
- [ ] Only after all acceptance criteria pass, update I1-03 status and the Iteration 1 exit decision; do not infer iteration acceptance from this check alone.