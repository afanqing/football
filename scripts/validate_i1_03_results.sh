#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
plan=${1:-/home/coder/football-validation/i1-03-proposed-plan.json}
state=${2:-/home/coder/football-validation/i1-03-state}
report=${3:-$state/i1-03-observation-report.json}
ingest_bin=${FOOTBALL_INGEST_BIN:-$repo_root/target/release/football-ingest}
max_requests=247

fail() {
	printf 'I1-03 validation failed: %s\n' "$1" >&2
	exit 1
}

for command_name in date find grep jq ps sha256sum sort; do
	command -v "$command_name" >/dev/null 2>&1 || fail "required command not found: $command_name"
done

[[ -r "$plan" ]] || fail "plan is not readable: $plan"
[[ -d "$state" ]] || fail "state directory is missing: $state"
[[ -x "$ingest_bin" ]] || fail "football-ingest binary is not executable: $ingest_bin"
[[ -r "$state/run.json" ]] || fail "run manifest is missing"
[[ -r "$state/polls.jsonl" ]] || fail "poll ledger is missing"
"$ingest_bin" validate-results --plan "$plan" >/dev/null \
	|| fail "plan does not pass the Rust observer validator"

jq -e -s 'length > 0 and all(.[]; type == "object")' "$state/polls.jsonl" >/dev/null \
	|| fail "poll ledger is empty or contains invalid JSON rows"

plan_sha=$(sha256sum "$plan" | awk '{print $1}')
run_sha=$(jq -er '.plan_sha256' "$state/run.json") || fail "invalid run manifest"
[[ "$plan_sha" == "$run_sha" ]] || fail "run manifest is bound to a different plan"

fixture_count=$(jq -er '.fixtures | length' "$plan") || fail "plan fixtures are missing"
week_count=$(jq -er '[.fixtures[].match_week] | unique | length' "$plan") || fail "plan match weeks are missing"
budget=$(jq -er '.request_budget_limit' "$plan") || fail "plan request budget is missing"
worst_case=$(jq -er '.worst_case_request_count' "$state/run.json") || fail "run manifest worst-case budget is missing"
[[ $(jq -r '.status' "$plan") == APPROVED ]] || fail "plan is not APPROVED"
(( fixture_count >= 20 )) || fail "plan has fewer than 20 fixtures"
(( week_count >= 2 )) || fail "plan spans fewer than two match weeks"
(( budget <= max_requests && worst_case <= budget && worst_case <= max_requests )) \
	|| fail "approved or worst-case budget exceeds $max_requests"

latest_kickoff=$(jq -er '[.fixtures[].kickoff_at_utc] | max' "$plan") || fail "plan kickoffs are missing"
latest_epoch=$(date -u -d "$latest_kickoff" +%s) || fail "cannot parse latest kickoff"
final_deadline=$((latest_epoch + 8 * 60 * 60))
now_epoch=$(date -u +%s)
(( now_epoch >= final_deadline )) || fail "the final fixture observation deadline has not passed"

if [[ -r "$state/observer.pid" ]]; then
	observer_pid=$(<"$state/observer.pid")
	if [[ "$observer_pid" =~ ^[0-9]+$ ]] && ps -p "$observer_pid" -o args= 2>/dev/null | grep -Fq 'football-ingest observe-results'; then
		fail "observer PID $observer_pid is still running; do not summarize a live ledger"
	fi
fi

"$ingest_bin" summarize-results --plan "$plan" --state "$state"
intervals="$state/intervals.json"
jq -e 'type == "array" and length > 0 and all(.[]; (.sample_id | type) == "string" and (.match_id != null) and (.classification | type) == "string")' "$intervals" >/dev/null \
	|| fail "interval summary is missing or malformed"

plan_keys=$(jq -c '[.fixtures[] | [.sample_id, (.match_id | tostring)]] | sort' "$plan")
summary_keys=$(jq -c '[.[] | [.sample_id, (.match_id | tostring)]] | sort' "$intervals")
[[ "$plan_keys" == "$summary_keys" ]] || fail "interval summary does not cover exactly the pre-registered fixtures"

# Ledger rows are per fixture; group response rows and shared fetch failures into request attempts.
request_count=$(jq -s '
	([.[] | select(
		((.poll_status == "returned") or (.poll_status == "not_returned") or (.poll_status == "fixture_mismatch"))
		and (.request_started_at_utc != null)
		and (.payload_sha256 != null)
	) | ["response", .scheduled_at_utc, .request_started_at_utc, .response_received_at_utc, .payload_sha256] | @json])
	+
	([.[] | select(
		(.poll_status == "fetch_error")
		and (.error_category != "skipped_locked")
		and (.error_category != "lock_error")
		and (.attempt_started_at_utc != null)
	) | ["attempt", .scheduled_at_utc, .attempt_started_at_utc, .response_received_at_utc, .error_category] | @json])
	| unique | length
' "$state/polls.jsonl") || fail "cannot group ledger rows into request attempts"

manifest_count=0
while IFS= read -r manifest; do
	[[ -n "$manifest" ]] || continue
	manifest_count=$((manifest_count + 1))
	digest=$(jq -er '.payload_sha256 | select(test("^[0-9a-f]{64}$"))' "$manifest") \
		|| fail "manifest has an invalid payload_sha256: $manifest"
	payload=${manifest%.manifest.json}.json
	[[ -f "$payload" ]] || fail "manifest payload is missing: $payload"
	actual_digest=$(sha256sum "$payload" | awk '{print $1}')
	[[ "$actual_digest" == "$digest" ]] || fail "payload hash differs from manifest: $payload"
done < <(find "$state/bronze" -type f -name '*.manifest.json' -print | sort)
(( manifest_count > 0 )) || fail "no Bronze manifests were found"

mapfile -t artifact_pairs < <(jq -r '
	select((.payload_sha256 | type) == "string" and (.artifact_reference | type) == "string")
	| [.payload_sha256, .artifact_reference] | @tsv
' "$state/polls.jsonl" | sort -u)
for pair in "${artifact_pairs[@]}"; do
	IFS=$'\t' read -r digest payload <<< "$pair"
	[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || fail "ledger contains an invalid payload hash"
	[[ -f "$payload" ]] || fail "ledger artifact reference is missing: $payload"
	actual_digest=$(sha256sum "$payload" | awk '{print $1}')
	[[ "$actual_digest" == "$digest" ]] || fail "ledger artifact hash differs: $payload"
	manifest="${payload%.json}.manifest.json"
	[[ -f "$manifest" ]] || fail "ledger artifact manifest is missing: $manifest"
	manifest_digest=$(jq -er '.payload_sha256' "$manifest") || fail "invalid artifact manifest: $manifest"
	[[ "$manifest_digest" == "$digest" ]] || fail "ledger hash differs from artifact manifest: $manifest"
done

mapfile -t log_files < <(find "$state" -type f \( -name '*.log' -o -name 'polls.jsonl' \) -print | sort)
(( ${#log_files[@]} > 0 )) || fail "no logs or poll ledger were found to scan"
for log_file in "${log_files[@]}"; do
	if grep -Eiq '"?(X-Auth-Token|Cookie|Set-Cookie|Authorization)"?[[:space:]]*:' "$log_file"; then
		fail "credential header name found in $log_file (value omitted)"
	fi
	if [[ -n "${FOOTBALL_DATA_TOKEN:-}" ]] && grep -Fq -- "$FOOTBALL_DATA_TOKEN" "$log_file"; then
		fail "provider token value found in $log_file (value omitted)"
	fi
done

dispositions=$(jq -c '[group_by(.classification)[] | {(.[0].classification): length}] | add // {}' "$intervals")
all_supported=$(jq -r "length == $fixture_count and all(.[]; .classification == \"SUPPORTED\")" "$intervals")
within_budget=true
(( request_count <= max_requests )) || within_budget=false
review_status=READY_FOR_INDEPENDENT_REVIEW
if [[ "$all_supported" != true || "$within_budget" != true ]]; then
	review_status=BLOCKED
fi

umask 077
report_dir=$(dirname -- "$report")
mkdir -p "$report_dir"
temporary_report="$report.tmp.$$"
trap 'rm -f -- "$temporary_report"' EXIT
jq -n \
	--arg generated_at_utc "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
	--arg plan_sha256 "$plan_sha" \
	--arg review_status "$review_status" \
	--argjson fixture_count "$fixture_count" \
	--argjson week_count "$week_count" \
	--argjson request_count "$request_count" \
	--argjson request_budget "$budget" \
	--argjson worst_case "$worst_case" \
	--argjson manifest_count "$manifest_count" \
	--argjson log_files_scanned "${#log_files[@]}" \
	--argjson dispositions "$dispositions" \
	--slurpfile fixtures "$intervals" \
	'{generated_at_utc:$generated_at_utc, plan_sha256:$plan_sha256, review_status:$review_status, reviewer_signoff:"PENDING", fixtures_expected:$fixture_count, match_weeks:$week_count, request_attempt_groups:$request_count, approved_request_budget:$request_budget, approved_worst_case_budget:$worst_case, request_budget_limit:247, within_request_budget:($request_count <= 247), bronze_manifests_verified:$manifest_count, secret_scan:{passed:true, files_scanned:$log_files_scanned}, disposition_counts:$dispositions, fixtures:$fixtures[0], acceptance_note:"This is an automated pre-review report, not independent sign-off. I1-03 acceptance requires every fixture SUPPORTED and independent evidence review."}' \
	> "$temporary_report"
chmod 600 "$temporary_report"
mv -f -- "$temporary_report" "$report"
trap - EXIT

printf 'I1-03 report: %s\n' "$report"
printf 'status=%s fixtures=%s request_attempt_groups=%s budget=%s manifest_count=%s\n' \
	"$review_status" "$fixture_count" "$request_count" "$max_requests" "$manifest_count"
[[ "$review_status" == READY_FOR_INDEPENDENT_REVIEW ]] || exit 2