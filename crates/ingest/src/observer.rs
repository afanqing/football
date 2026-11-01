use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use football_contracts::SnapshotManifest;
use football_ingest::{FootballDataClient, SnapshotStore};
use serde_json::{Value, json};

use crate::{LIVE_FETCH_LOCK_PATH, acquire_live_fetch_lock};

const POLL_INTERVAL_SECONDS: i64 = 900;
const START_OFFSET_MINUTES: i64 = 90;
const DEADLINE_HOURS: i64 = 8;
const LATE_GRACE_SECONDS: i64 = 300;
const I1_02_START_DATE: &str = "2026-11-02";
const I1_02_END_DATE: &str = "2026-11-06";

#[derive(Debug, Clone)]
struct Fixture {
    sample_id: String,
    match_id: String,
    kickoff_at: DateTime<Utc>,
    match_week: String,
}

#[derive(Debug, Clone)]
struct Plan {
    request_budget_limit: usize,
    minimum_fixtures: usize,
    minimum_match_weeks: usize,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Clone)]
struct PollSlot {
    scheduled_at: DateTime<Utc>,
    fixtures: Vec<Fixture>,
}

fn read_plan(path: &Path) -> Result<(Value, Plan)> {
    let value: Value = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("read plan {}", path.display()))?,
    )
    .context("parse I1-03 plan JSON")?;
    let fixtures_value = value
        .get("fixtures")
        .and_then(Value::as_array)
        .context("plan fixtures must be an array")?;
    let mut fixtures = Vec::with_capacity(fixtures_value.len());
    let mut match_ids = BTreeSet::new();
    for row in fixtures_value {
        let sample_id = required_string(row, "sample_id")?;
        let match_id = required_string(row, "match_id")?;
        match_id
            .parse::<u64>()
            .with_context(|| format!("match_id must be a numeric provider ID: {match_id}"))?;
        let match_week = required_string(row, "match_week")?;
        let kickoff_text = required_string(row, "kickoff_at_utc")?;
        let kickoff_at = DateTime::parse_from_rfc3339(&kickoff_text)
            .with_context(|| format!("invalid kickoff_at_utc for match {match_id}"))?
            .with_timezone(&Utc);
        if !match_ids.insert(match_id.clone()) {
            bail!("duplicate match_id in I1-03 fixture sample: {match_id}");
        }
        fixtures.push(Fixture {
            sample_id,
            match_id,
            kickoff_at,
            match_week,
        });
    }
    let plan = Plan {
        request_budget_limit: required_usize(&value, "request_budget_limit")?,
        minimum_fixtures: required_usize(&value, "minimum_fixtures")?,
        minimum_match_weeks: required_usize(&value, "minimum_match_weeks")?,
        fixtures,
    };
    Ok((value, plan))
}

fn required_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .with_context(|| format!("missing or empty {key}"))
}

fn required_usize(value: &Value, key: &str) -> Result<usize> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|number| usize::try_from(number).ok())
        .with_context(|| format!("missing or invalid {key}"))
}

fn validate_approved_plan(value: &Value, plan: &Plan) -> Result<()> {
    if value.get("status").and_then(Value::as_str) != Some("APPROVED") {
        bail!("live polling disabled: plan status must be APPROVED");
    }
    if value.get("competition").and_then(Value::as_str) != Some("PL") {
        bail!("this observer currently supports only the PL competition");
    }
    for (field, expected) in [
        ("poll_interval_seconds", POLL_INTERVAL_SECONDS),
        ("start_offset_minutes", START_OFFSET_MINUTES),
        ("deadline_hours_after_kickoff", DEADLINE_HOURS),
    ] {
        if value.get(field).and_then(Value::as_i64) != Some(expected) {
            bail!("approved plan {field} does not match the implemented observer");
        }
    }
    let approved_at = required_string(value, "approved_at_utc")?;
    let approved_at = DateTime::parse_from_rfc3339(&approved_at)
        .context("approved_at_utc must be an RFC3339 timestamp")?
        .with_timezone(&Utc);
    if approved_at > Utc::now() {
        bail!("approved_at_utc cannot be in the future");
    }
    required_string(value, "approval_reference")?;
    let approvers = value
        .get("approvers")
        .and_then(Value::as_object)
        .context("approvers must name the required approval roles")?;
    for role in ["data_technical_lead", "data_engineer", "operator"] {
        if approvers
            .get(role)
            .and_then(Value::as_str)
            .is_none_or(|name| name.trim().is_empty())
        {
            bail!("named approver is required for role {role}");
        }
    }
    for approval in [
        "sample_selection_approved",
        "provider_semantics_approved",
        "provider_terms_approved",
        "poll_schedule_approved",
    ] {
        if value.get(approval).and_then(Value::as_bool) != Some(true) {
            bail!("approval gate {approval} is not true");
        }
    }
    if plan.minimum_fixtures < 20 || plan.minimum_match_weeks < 2 {
        bail!("approved target cannot be below 20 fixtures across two match weeks");
    }
    if plan.fixtures.len() < plan.minimum_fixtures {
        bail!(
            "fixture sample has {} matches; approved minimum is {}",
            plan.fixtures.len(),
            plan.minimum_fixtures
        );
    }
    let match_weeks = plan
        .fixtures
        .iter()
        .map(|fixture| fixture.match_week.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    if match_weeks < plan.minimum_match_weeks {
        bail!(
            "fixture sample spans {match_weeks} match weeks; approved minimum is {}",
            plan.minimum_match_weeks
        );
    }
    let budget = planned_request_budget(plan);
    if plan.request_budget_limit < budget {
        bail!(
            "approved request budget {} is below worst-case required budget {budget}",
            plan.request_budget_limit
        );
    }
    Ok(())
}

fn schedule(plan: &Plan) -> Vec<PollSlot> {
    let mut slots: BTreeMap<DateTime<Utc>, Vec<Fixture>> = BTreeMap::new();
    for fixture in &plan.fixtures {
        let first = fixture.kickoff_at + ChronoDuration::minutes(START_OFFSET_MINUTES);
        let deadline = fixture.kickoff_at + ChronoDuration::hours(DEADLINE_HOURS);
        let mut scheduled = first;
        while scheduled <= deadline {
            slots.entry(scheduled).or_default().push(fixture.clone());
            scheduled += ChronoDuration::seconds(POLL_INTERVAL_SECONDS);
        }
    }
    slots
        .into_iter()
        .map(|(scheduled_at, fixtures)| PollSlot {
            scheduled_at,
            fixtures,
        })
        .collect()
}

fn conflicts_with_i1_02_schedule(slot: DateTime<Utc>) -> bool {
    let shanghai_time = slot + ChronoDuration::hours(8);
    let date = shanghai_time.date_naive();
    let start = chrono::NaiveDate::parse_from_str(I1_02_START_DATE, "%Y-%m-%d").unwrap();
    let end = chrono::NaiveDate::parse_from_str(I1_02_END_DATE, "%Y-%m-%d").unwrap();
    let minute_of_day = i64::from(shanghai_time.hour()) * 60 + i64::from(shanghai_time.minute());
    date >= start && date <= end && (7 * 60 + 55..=8 * 60 + 10).contains(&minute_of_day)
}

fn planned_request_budget(plan: &Plan) -> usize {
    schedule(plan)
        .iter()
        .map(|slot| {
            slot.fixtures
                .iter()
                .map(|fixture| fixture.kickoff_at.date_naive())
                .collect::<BTreeSet<_>>()
                .len()
        })
        .sum()
}

fn append_jsonl(path: &Path, event: &Value) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("open observer ledger {}", path.display()))?;
    #[cfg(unix)]
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    serde_json::to_writer(&mut file, event).context("serialize observer event")?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn read_events(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let reader = BufReader::new(File::open(path)?);
    reader
        .lines()
        .map(|line| {
            let line = line?;
            serde_json::from_str(&line).context("parse I1-03 poll ledger row")
        })
        .collect()
}

fn event_key(event: &Value) -> Option<(u64, String)> {
    let scheduled_at = event
        .get("scheduled_at_utc")?
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())?
        .with_timezone(&Utc);
    Some((event.get("match_id")?.as_u64()?, scheduled_at.to_rfc3339()))
}

fn is_usable_event(event: &Value) -> bool {
    event.get("result_usable").and_then(Value::as_bool) == Some(true)
        && event.get("poll_status").and_then(Value::as_str) == Some("returned")
}

fn has_usable_result(events: &[Value], match_id: u64) -> bool {
    events.iter().any(|event| {
        event.get("match_id").and_then(Value::as_u64) == Some(match_id) && is_usable_event(event)
    })
}

fn poll_event(
    fixture: &Fixture,
    slot: DateTime<Utc>,
    started_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    sequence: usize,
    poll_status: &str,
) -> Value {
    json!({
        "sample_id": fixture.sample_id,
        "match_id": fixture.match_id.parse::<u64>().unwrap_or_default(),
        "competition": "PL",
        "kickoff_at_utc": fixture.kickoff_at,
        "scheduled_at_utc": slot,
        "request_started_at_utc": started_at,
        "response_received_at_utc": received_at,
        "observed_at_utc": received_at,
        "poll_sequence": sequence,
        "poll_status": poll_status,
    })
}

fn record_missed(
    fixture: &Fixture,
    slot: DateTime<Utc>,
    sequence: usize,
    ledger: &Path,
    reason: &str,
) -> Result<()> {
    let now = Utc::now();
    let mut event = poll_event(fixture, slot, now, now, sequence, "missed");
    event["request_started_at_utc"] = Value::Null;
    event["attempt_started_at_utc"] = json!(now);
    event["error_category"] = json!(reason);
    append_jsonl(ledger, &event)
}

fn artifact_for_hash(bronze_root: &Path, digest: &str) -> Result<PathBuf> {
    let mut pending = vec![bronze_root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "json")
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .ends_with(&format!("{digest}.manifest.json"))
                })
            {
                let manifest: SnapshotManifest = serde_json::from_slice(&std::fs::read(&path)?)?;
                if manifest.payload_sha256 != digest {
                    continue;
                }
                let payload =
                    PathBuf::from(path.to_string_lossy().replace(".manifest.json", ".json"));
                let actual = hex::encode(sha2::Sha256::digest(std::fs::read(&payload)?));
                if actual != digest {
                    bail!("Bronze payload hash differs from manifest");
                }
                return Ok(payload);
            }
        }
    }
    bail!("Bronze manifest not found for captured payload hash")
}

use sha2::Digest;

fn restrict_tree_permissions(root: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        for entry in std::fs::read_dir(root)? {
            let path = entry?.path();
            if path.is_dir() {
                restrict_tree_permissions(&path)?;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
            } else {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
        }
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn ensure_run_manifest(
    plan_path: &Path,
    plan_value: &Value,
    plan: &Plan,
    state_root: &Path,
) -> Result<()> {
    let plan_sha256 = hex::encode(sha2::Sha256::digest(std::fs::read(plan_path)?));
    let match_week_count = plan
        .fixtures
        .iter()
        .map(|fixture| fixture.match_week.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let run_manifest = json!({
        "observer_version": env!("CARGO_PKG_VERSION"),
        "plan_sha256": plan_sha256,
        "approved_at_utc": plan_value.get("approved_at_utc"),
        "approval_reference": plan_value.get("approval_reference"),
        "approvers": plan_value.get("approvers"),
        "fixture_count": plan.fixtures.len(),
        "match_week_count": match_week_count,
        "worst_case_request_count": planned_request_budget(plan),
    });
    let path = state_root.join("run.json");
    if path.exists() {
        let existing: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        if existing.get("plan_sha256") != run_manifest.get("plan_sha256") {
            bail!("existing observer state is bound to a different plan SHA-256");
        }
        return Ok(());
    }

    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).context("create I1-03 run manifest")?;
    serde_json::to_writer_pretty(&mut file, &run_manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn validate_run_manifest(plan_path: &Path, state_root: &Path) -> Result<()> {
    let path = state_root.join("run.json");
    let existing: Value =
        serde_json::from_slice(&std::fs::read(&path).context("I1-03 run manifest is missing")?)?;
    let expected_hash = hex::encode(sha2::Sha256::digest(std::fs::read(plan_path)?));
    if existing.get("plan_sha256").and_then(Value::as_str) != Some(expected_hash.as_str()) {
        bail!("summary plan SHA-256 differs from the plan used by the observer");
    }
    Ok(())
}

struct PollResultContext<'a> {
    slot: DateTime<Utc>,
    sequence_by_match: &'a HashMap<u64, usize>,
    started_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    manifest: &'a SnapshotManifest,
    artifact: &'a Path,
    ledger: &'a Path,
}

fn write_poll_result(
    fixtures: &[Fixture],
    matches: &[football_contracts::FootballDataMatch],
    context: PollResultContext<'_>,
) -> Result<()> {
    let matches_by_id = matches
        .iter()
        .map(|record| (record.id, record))
        .collect::<HashMap<_, _>>();
    for fixture in fixtures {
        let match_id = fixture.match_id.parse::<u64>()?;
        let sequence = *context.sequence_by_match.get(&match_id).unwrap_or(&1);
        let mut event = poll_event(
            fixture,
            context.slot,
            context.started_at,
            context.received_at,
            sequence,
            "returned",
        );
        let record = matches_by_id.get(&match_id);
        if let Some(record) = record {
            let home_score = record.score.full_time.home;
            let away_score = record.score.full_time.away;
            let usable =
                record.status == "FINISHED" && home_score.is_some() && away_score.is_some();
            event["source_utc_date"] = json!(record.utc_date);
            event["match_status"] = json!(record.status);
            event["home_score"] = json!(home_score);
            event["away_score"] = json!(away_score);
            event["result_usable"] = json!(usable);
            if record.utc_date != fixture.kickoff_at {
                event["poll_status"] = json!("fixture_mismatch");
                event["error_category"] = json!("source_kickoff_changed");
            }
        } else {
            event["poll_status"] = json!("not_returned");
            event["error_category"] = json!("fixture_missing_from_response");
        }
        event["http_status"] = json!(context.manifest.http_status);
        event["payload_sha256"] = json!(context.manifest.payload_sha256);
        event["artifact_reference"] = json!(context.artifact.display().to_string());
        append_jsonl(context.ledger, &event)?;
    }
    Ok(())
}

fn sequence_numbers(events: &[Value], fixtures: &[Fixture]) -> HashMap<u64, usize> {
    fixtures
        .iter()
        .filter_map(|fixture| fixture.match_id.parse::<u64>().ok())
        .map(|match_id| {
            let count = events
                .iter()
                .filter(|event| event.get("match_id").and_then(Value::as_u64) == Some(match_id))
                .count();
            (match_id, count + 1)
        })
        .collect()
}

fn poll_failures(
    fixtures: &[Fixture],
    slot: DateTime<Utc>,
    sequences: &HashMap<u64, usize>,
    started_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    category: &str,
    ledger: &Path,
) -> Result<()> {
    for fixture in fixtures {
        let sequence = fixture
            .match_id
            .parse::<u64>()
            .ok()
            .and_then(|id| sequences.get(&id).copied())
            .unwrap_or(1);
        let mut event = poll_event(
            fixture,
            slot,
            started_at,
            received_at,
            sequence,
            "fetch_error",
        );
        event["request_started_at_utc"] = Value::Null;
        event["attempt_started_at_utc"] = json!(started_at);
        event["error_category"] = json!(category);
        append_jsonl(ledger, &event)?;
    }
    Ok(())
}

fn date_groups(fixtures: &[Fixture]) -> BTreeMap<chrono::NaiveDate, Vec<Fixture>> {
    let mut groups = BTreeMap::new();
    for fixture in fixtures {
        groups
            .entry(fixture.kickoff_at.date_naive())
            .or_insert_with(Vec::new)
            .push(fixture.clone());
    }
    groups
}

pub async fn run(plan_path: &Path, state_root: &Path) -> Result<()> {
    let (value, plan) = read_plan(plan_path)?;
    validate_approved_plan(&value, &plan)?;
    if std::env::var_os("FOOTBALL_DATA_TOKEN").is_none() {
        bail!("FOOTBALL_DATA_TOKEN is required; secret values are never logged");
    }
    let mut permissions = std::fs::DirBuilder::new();
    permissions.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        permissions.mode(0o700);
    }
    permissions.create(state_root)?;
    #[cfg(unix)]
    std::fs::set_permissions(state_root, std::fs::Permissions::from_mode(0o700))?;
    let observer_lock_path = state_root.join("observer.lock");
    let mut observer_lock_options = OpenOptions::new();
    observer_lock_options
        .create(true)
        .truncate(false)
        .read(true)
        .write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        observer_lock_options.mode(0o600);
    }
    let observer_lock = observer_lock_options.open(&observer_lock_path)?;
    #[cfg(unix)]
    std::fs::set_permissions(&observer_lock_path, std::fs::Permissions::from_mode(0o600))?;
    observer_lock
        .try_lock()
        .context("another I1-03 observer is active for this state directory")?;
    ensure_run_manifest(plan_path, &value, &plan, state_root)?;
    let ledger = state_root.join("polls.jsonl");
    let bronze_root = state_root.join("bronze");
    std::fs::create_dir_all(&bronze_root)?;
    #[cfg(unix)]
    std::fs::set_permissions(&bronze_root, std::fs::Permissions::from_mode(0o700))?;
    let token = std::env::var("FOOTBALL_DATA_TOKEN")
        .context("read FOOTBALL_DATA_TOKEN; secret values are never logged")?;
    let mut client = FootballDataClient::new(token)?;
    let poll_slots = schedule(&plan);

    loop {
        let events = read_events(&ledger)?;
        let keys = events.iter().filter_map(event_key).collect::<BTreeSet<_>>();
        let now = Utc::now();
        let next = poll_slots.iter().find_map(|slot| {
            let mut fixtures = slot
                .fixtures
                .iter()
                .filter(|fixture| {
                    fixture
                        .match_id
                        .parse::<u64>()
                        .ok()
                        .is_some_and(|id| !has_usable_result(&events, id))
                })
                .filter(|fixture| {
                    let match_id = fixture.match_id.parse::<u64>().unwrap_or_default();
                    !keys.contains(&(match_id, slot.scheduled_at.to_rfc3339()))
                })
                .cloned()
                .collect::<Vec<_>>();
            if fixtures.is_empty() {
                return None;
            }
            fixtures.sort_by(|left, right| left.match_id.cmp(&right.match_id));
            Some((slot.scheduled_at, fixtures))
        });
        let Some((scheduled_at, fixtures)) = next else {
            break;
        };
        if scheduled_at > now {
            tokio::time::sleep(
                (scheduled_at - now)
                    .to_std()
                    .unwrap_or(Duration::from_secs(60))
                    .min(Duration::from_secs(60)),
            )
            .await;
            continue;
        }
        if now - scheduled_at > ChronoDuration::seconds(LATE_GRACE_SECONDS) {
            for fixture in fixtures {
                let sequence = fixture
                    .match_id
                    .parse::<u64>()
                    .ok()
                    .map(|id| sequence_numbers(&events, std::slice::from_ref(&fixture))[&id])
                    .unwrap_or(1);
                record_missed(
                    &fixture,
                    scheduled_at,
                    sequence,
                    &ledger,
                    "observer_late_or_restarted",
                )?;
            }
            continue;
        }
        if conflicts_with_i1_02_schedule(scheduled_at) {
            for fixture in fixtures {
                let sequence = fixture
                    .match_id
                    .parse::<u64>()
                    .ok()
                    .map(|id| sequence_numbers(&events, std::slice::from_ref(&fixture))[&id])
                    .unwrap_or(1);
                record_missed(
                    &fixture,
                    scheduled_at,
                    sequence,
                    &ledger,
                    "i1_02_priority_window",
                )?;
            }
            continue;
        }

        for (match_date, date_fixtures) in date_groups(&fixtures) {
            let started_at = Utc::now();
            let sequences = sequence_numbers(&events, &date_fixtures);
            client.wait_until_ready().await;
            let live_fetch_lock = match acquire_live_fetch_lock(Path::new(LIVE_FETCH_LOCK_PATH)) {
                Ok(lock) => lock,
                Err(error) => {
                    let received_at = Utc::now();
                    poll_failures(
                        &date_fixtures,
                        scheduled_at,
                        &sequences,
                        started_at,
                        received_at,
                        if error.downcast_ref::<crate::LiveFetchBusy>().is_some() {
                            "skipped_locked"
                        } else {
                            "lock_error"
                        },
                        &ledger,
                    )?;
                    continue;
                }
            };
            let capture = match client
                .fetch_matches("PL", Some(match_date), Some(match_date))
                .await
            {
                Ok(capture) => capture,
                Err(_) => {
                    poll_failures(
                        &date_fixtures,
                        scheduled_at,
                        &sequences,
                        started_at,
                        Utc::now(),
                        "provider_or_decode_error",
                        &ledger,
                    )?;
                    continue;
                }
            };
            let request_started_at = capture.request_started_at;
            let received_at = capture.captured_at;
            drop(live_fetch_lock);
            let matches = capture.matches.clone();
            let manifest = SnapshotStore::new(&bronze_root).write_matches(capture)?;
            restrict_tree_permissions(&bronze_root)?;
            let artifact = artifact_for_hash(&bronze_root, &manifest.payload_sha256)?;
            write_poll_result(
                &date_fixtures,
                &matches,
                PollResultContext {
                    slot: scheduled_at,
                    sequence_by_match: &sequences,
                    started_at: request_started_at,
                    received_at,
                    manifest: &manifest,
                    artifact: &artifact,
                    ledger: &ledger,
                },
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Classification {
    Falsified,
    Supported,
    Inconclusive,
    RightCensored,
}

impl Classification {
    fn as_str(self) -> &'static str {
        match self {
            Self::Falsified => "FALSIFIED",
            Self::Supported => "SUPPORTED",
            Self::Inconclusive => "INCONCLUSIVE",
            Self::RightCensored => "RIGHT_CENSORED",
        }
    }
}

fn classify_interval(
    cutoff: DateTime<Utc>,
    last_unusable: Option<DateTime<Utc>>,
    first_usable: Option<DateTime<Utc>>,
    deadline_passed: bool,
) -> Classification {
    if first_usable.is_some_and(|time| time < cutoff) {
        return Classification::Falsified;
    }
    if first_usable.is_none() && deadline_passed {
        return Classification::RightCensored;
    }
    if first_usable.is_some() && last_unusable.is_some_and(|time| time >= cutoff) {
        return Classification::Supported;
    }
    Classification::Inconclusive
}

fn observed_time(event: &Value) -> Option<DateTime<Utc>> {
    event
        .get("observed_at_utc")?
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn derive_summary(fixture: &Fixture, events: &[Value], now: DateTime<Utc>) -> Value {
    let mut matching = events
        .iter()
        .filter(|event| {
            event.get("match_id").and_then(Value::as_u64) == fixture.match_id.parse::<u64>().ok()
        })
        .cloned()
        .collect::<Vec<_>>();
    matching.sort_by_key(observed_time);
    let successful = matching
        .iter()
        .filter(|event| event.get("poll_status").and_then(Value::as_str) == Some("returned"))
        .collect::<Vec<_>>();
    let last_non_finished = successful.iter().rev().find(|event| {
        event
            .get("match_status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != "FINISHED")
    });
    let first_finished = successful
        .iter()
        .find(|event| event.get("match_status").and_then(Value::as_str) == Some("FINISHED"));
    let last_unusable = successful
        .iter()
        .rev()
        .find(|event| event.get("result_usable").and_then(Value::as_bool) != Some(true));
    let first_usable = successful.iter().find(|event| is_usable_event(event));
    let last_unusable_at = last_unusable.and_then(|event| observed_time(event));
    let first_usable_at = first_usable.and_then(|event| observed_time(event));
    let cutoff = fixture.kickoff_at + ChronoDuration::hours(4);
    let deadline = fixture.kickoff_at + ChronoDuration::hours(DEADLINE_HOURS);
    let classification =
        classify_interval(cutoff, last_unusable_at, first_usable_at, now >= deadline);
    let times = successful
        .iter()
        .filter_map(|event| observed_time(event))
        .collect::<Vec<_>>();
    let largest_gap_seconds = times
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).num_seconds())
        .max()
        .unwrap_or_default();
    let exceptions = matching
        .iter()
        .filter_map(|event| event.get("error_category").and_then(Value::as_str))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    json!({
        "sample_id": fixture.sample_id,
        "match_id": fixture.match_id,
        "competition": "PL",
        "kickoff_at_utc": fixture.kickoff_at,
        "cutoff_at_utc": cutoff,
        "last_non_finished_observed_at": last_non_finished.and_then(|event| observed_time(event)),
        "first_finished_observed_at": first_finished.and_then(|event| observed_time(event)),
        "last_result_unusable_observed_at": last_unusable_at,
        "first_result_usable_observed_at": first_usable_at,
        "poll_count": matching.len(),
        "largest_poll_gap_seconds": largest_gap_seconds,
        "classification": classification.as_str(),
        "exception_reason": exceptions,
    })
}

pub fn summarize(plan_path: &Path, state_root: &Path) -> Result<()> {
    let (_, plan) = read_plan(plan_path)?;
    validate_run_manifest(plan_path, state_root)?;
    let events = read_events(&state_root.join("polls.jsonl"))?;
    let now = Utc::now();
    let summaries = plan
        .fixtures
        .iter()
        .map(|fixture| derive_summary(fixture, &events, now))
        .collect::<Vec<_>>();
    let path = state_root.join("intervals.json");
    let temporary = state_root.join("intervals.json.tmp");
    let bytes = serde_json::to_vec_pretty(&summaries).context("serialize interval summary")?;
    {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(temporary, &path)?;
    let mut counts = BTreeMap::<&str, usize>::new();
    for row in &summaries {
        *counts
            .entry(row["classification"].as_str().unwrap_or("UNKNOWN"))
            .or_default() += 1;
    }
    println!("interval summary: {counts:?}; output={}", path.display());
    Ok(())
}

pub fn validate(plan_path: &Path) -> Result<()> {
    let (value, plan) = read_plan(plan_path)?;
    let request_budget = planned_request_budget(&plan);
    let match_weeks = plan
        .fixtures
        .iter()
        .map(|fixture| fixture.match_week.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    println!(
        "plan: status={} fixtures={} match_weeks={} worst_case_requests={request_budget} approved_budget={}",
        value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("MISSING"),
        plan.fixtures.len(),
        match_weeks,
        plan.request_budget_limit
    );
    if value.get("status").and_then(Value::as_str) == Some("APPROVED") {
        validate_approved_plan(&value, &plan)?;
        println!(
            "approved plan is internally consistent; this command did not make network requests"
        );
    } else {
        println!(
            "network polling remains disabled until a named approval and sufficient request budget are recorded"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(hour: u32, minute: u32) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&format!("2026-11-01T{hour:02}:{minute:02}:00Z"))
            .unwrap()
            .with_timezone(&Utc)
    }

    fn fixture(match_id: &str, kickoff_at: DateTime<Utc>, match_week: &str) -> Fixture {
        Fixture {
            sample_id: "sample".to_owned(),
            match_id: match_id.to_owned(),
            kickoff_at,
            match_week: match_week.to_owned(),
        }
    }

    #[test]
    fn interval_classification_checks_both_bounds_and_equality() {
        let cutoff = utc(16, 0);
        assert_eq!(
            classify_interval(cutoff, Some(utc(15, 45)), Some(utc(15, 59)), false),
            Classification::Falsified
        );
        assert_eq!(
            classify_interval(cutoff, Some(cutoff), Some(utc(16, 15)), false),
            Classification::Supported
        );
        assert_eq!(
            classify_interval(cutoff, Some(utc(15, 45)), Some(utc(16, 15)), false),
            Classification::Inconclusive
        );
        assert_eq!(
            classify_interval(cutoff, Some(utc(15, 45)), Some(cutoff), false),
            Classification::Inconclusive
        );
        assert_eq!(
            classify_interval(cutoff, Some(utc(16, 15)), None, false),
            Classification::Inconclusive
        );
        assert_eq!(
            classify_interval(cutoff, Some(utc(17, 0)), None, true),
            Classification::RightCensored
        );
    }

    #[test]
    fn schedule_starts_at_kickoff_plus_90_minutes_and_includes_deadline() {
        let plan = Plan {
            request_budget_limit: 100,
            minimum_fixtures: 20,
            minimum_match_weeks: 2,
            fixtures: vec![fixture("42", utc(12, 7), "week-1")],
        };
        let slots = schedule(&plan);
        assert_eq!(slots.first().unwrap().scheduled_at, utc(13, 37));
        assert_eq!(slots.last().unwrap().scheduled_at, utc(20, 7));
        assert_eq!(slots.len(), 27);
        assert_eq!(
            slots[0].scheduled_at,
            utc(12, 7) + ChronoDuration::minutes(90)
        );
    }

    #[test]
    fn i1_02_daily_fetch_window_is_reserved() {
        let first_day = utc(0, 0) + ChronoDuration::days(1);
        assert!(conflicts_with_i1_02_schedule(first_day));
        assert!(conflicts_with_i1_02_schedule(
            first_day + ChronoDuration::minutes(10)
        ));
        assert!(!conflicts_with_i1_02_schedule(
            first_day + ChronoDuration::minutes(15)
        ));
        assert!(conflicts_with_i1_02_schedule(
            first_day + ChronoDuration::days(4)
        ));
        assert!(!conflicts_with_i1_02_schedule(
            first_day + ChronoDuration::days(5)
        ));
    }

    #[test]
    fn summary_separates_finished_status_from_usable_result() {
        let fixture = fixture("42", utc(12, 0), "week-1");
        let events = vec![
            json!({"match_id": 42, "poll_status": "returned", "observed_at_utc": "2026-11-01T15:45:00Z", "match_status": "IN_PLAY", "result_usable": false}),
            json!({"match_id": 42, "poll_status": "returned", "observed_at_utc": "2026-11-01T16:00:00Z", "match_status": "FINISHED", "result_usable": false}),
            json!({"match_id": 42, "poll_status": "returned", "observed_at_utc": "2026-11-01T16:15:00Z", "match_status": "FINISHED", "result_usable": true}),
        ];
        let summary = derive_summary(&fixture, &events, utc(17, 0));
        assert_eq!(
            summary["last_non_finished_observed_at"],
            "2026-11-01T15:45:00Z"
        );
        assert_eq!(
            summary["first_finished_observed_at"],
            "2026-11-01T16:00:00Z"
        );
        assert_eq!(
            summary["last_result_unusable_observed_at"],
            "2026-11-01T16:00:00Z"
        );
        assert_eq!(
            summary["first_result_usable_observed_at"],
            "2026-11-01T16:15:00Z"
        );
        assert_eq!(summary["classification"], "SUPPORTED");
    }

    #[test]
    fn request_budget_batches_fixtures_by_utc_match_date_and_slot() {
        let plan = Plan {
            request_budget_limit: 100,
            minimum_fixtures: 20,
            minimum_match_weeks: 2,
            fixtures: vec![
                fixture("42", utc(12, 0), "week-1"),
                fixture("43", utc(12, 0), "week-1"),
            ],
        };
        assert_eq!(planned_request_budget(&plan), 27);
    }

    fn approved_plan(status: &str, request_budget_limit: usize) -> (Value, Plan) {
        let fixtures = (0..20)
            .map(|index| {
                let week = index / 10;
                let date = if week == 0 {
                    "2026-11-07"
                } else {
                    "2026-11-14"
                };
                Fixture {
                    sample_id: "approved-sample".to_owned(),
                    match_id: (1000 + index).to_string(),
                    kickoff_at: DateTime::parse_from_rfc3339(&format!("{date}T12:00:00Z"))
                        .unwrap()
                        .with_timezone(&Utc),
                    match_week: format!("week-{}", week + 1),
                }
            })
            .collect::<Vec<_>>();
        let plan = Plan {
            request_budget_limit,
            minimum_fixtures: 20,
            minimum_match_weeks: 2,
            fixtures,
        };
        let value = json!({
            "status": status,
            "competition": "PL",
            "poll_interval_seconds": 900,
            "start_offset_minutes": 90,
            "deadline_hours_after_kickoff": 8,
            "approved_at_utc": "2026-11-01T00:00:00Z",
            "approval_reference": "decision-record-I1-03",
            "sample_selection_approved": true,
            "provider_semantics_approved": true,
            "provider_terms_approved": true,
            "poll_schedule_approved": true,
            "approvers": {
                "data_technical_lead": "Lead",
                "data_engineer": "Engineer",
                "operator": "Operator"
            }
        });
        (value, plan)
    }

    #[test]
    fn approved_plan_gate_rejects_pending_and_underbudget_plans() {
        let (_, plan) = approved_plan("PENDING", usize::MAX);
        let budget = planned_request_budget(&plan);
        let (pending, _) = approved_plan("PENDING", budget);
        assert!(validate_approved_plan(&pending, &plan).is_err());

        let (underbudget, plan) = approved_plan("APPROVED", budget - 1);
        assert!(validate_approved_plan(&underbudget, &plan).is_err());

        let (approved, plan) = approved_plan("APPROVED", budget);
        assert!(validate_approved_plan(&approved, &plan).is_ok());
    }

    #[test]
    fn observer_state_is_bound_to_the_exact_plan_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let plan_path = directory.path().join("plan.json");
        let state_root = directory.path().join("state");
        std::fs::create_dir(&state_root).unwrap();
        let (value, plan) = approved_plan("APPROVED", 100);
        std::fs::write(&plan_path, serde_json::to_vec(&value).unwrap()).unwrap();

        ensure_run_manifest(&plan_path, &value, &plan, &state_root).unwrap();
        validate_run_manifest(&plan_path, &state_root).unwrap();

        std::fs::write(&plan_path, b"{}\n").unwrap();
        assert!(validate_run_manifest(&plan_path, &state_root).is_err());
    }
}
