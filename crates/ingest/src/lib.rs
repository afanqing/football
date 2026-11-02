use std::{fs::OpenOptions, io::Write, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use football_contracts::{CONTRACT_VERSION, MatchesResponse, SnapshotManifest};
use reqwest::{Client, StatusCode, header::HeaderMap};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const SOURCE: &str = "football-data.org";
const SPORTTERY_SOURCE: &str = "sporttery";
pub const SPORTTERY_HAD_PARSER_VERSION: &str = "sporttery-had-v1";
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(6);
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
struct SportteryFixedBonusResponse {
    #[serde(default)]
    success: bool,
    #[serde(rename = "errorCode", default)]
    error_code: Option<String>,
    #[serde(default)]
    value: Option<SportteryFixedBonusValue>,
}

#[derive(Debug, Deserialize)]
struct SportteryFixedBonusValue {
    #[serde(rename = "oddsHistory", default)]
    odds_history: Option<SportteryOddsHistory>,
}

#[derive(Debug, Deserialize)]
struct SportteryOddsHistory {
    #[serde(rename = "matchId", default)]
    match_id: Option<u64>,
    #[serde(rename = "homeTeamAllName", default)]
    home_team_all_name: Option<String>,
    #[serde(rename = "awayTeamAllName", default)]
    away_team_all_name: Option<String>,
    #[serde(rename = "hadList", default)]
    had_list: Vec<SportteryHadRow>,
}

#[derive(Debug, Deserialize)]
struct SportteryHadRow {
    #[serde(default)]
    h: Option<String>,
    #[serde(default)]
    d: Option<String>,
    #[serde(default)]
    a: Option<String>,
    #[serde(rename = "updateDate", default)]
    update_date: Option<String>,
    #[serde(rename = "updateTime", default)]
    update_time: Option<String>,
}

pub fn parse_sporttery_had_v1(
    payload: &[u8],
    captured_at: DateTime<Utc>,
) -> Result<football_contracts::ThreeWayOddsSnapshot> {
    let response: SportteryFixedBonusResponse =
        serde_json::from_slice(payload).context("decode Sporttery HAD response")?;

    if !response.success || response.error_code.as_deref() != Some("0") {
        bail!("Sporttery HAD response is unsuccessful or indicates an upstream error");
    }

    let history = response
        .value
        .and_then(|value| value.odds_history)
        .context("Sporttery HAD response missing value.oddsHistory")?;

    let source_event_id = history
        .match_id
        .map(|id| id.to_string())
        .context("missing matchId in Sporttery HAD response")?;
    let home_team_name = history
        .home_team_all_name
        .filter(|name| !name.trim().is_empty())
        .context("missing homeTeamAllName in Sporttery HAD response")?;
    let away_team_name = history
        .away_team_all_name
        .filter(|name| !name.trim().is_empty())
        .context("missing awayTeamAllName in Sporttery HAD response")?;

    let latest = history
        .had_list
        .into_iter()
        .filter_map(|row| {
            let (home, draw, away, date, time) = match (
                row.h.as_deref(),
                row.d.as_deref(),
                row.a.as_deref(),
                row.update_date.as_deref(),
                row.update_time.as_deref(),
            ) {
                (Some(home), Some(draw), Some(away), Some(date), Some(time)) => {
                    (home, draw, away, date, time)
                }
                _ => return None,
            };

            let source_updated_at = match parse_market_update_timestamp(date, time) {
                Ok(value) => value,
                Err(_) => return None,
            };

            let decimal_odds = match (
                parse_decimal_odd(home, "home"),
                parse_decimal_odd(draw, "draw"),
                parse_decimal_odd(away, "away"),
            ) {
                (Ok(home), Ok(draw), Ok(away)) => [home, draw, away],
                _ => return None,
            };

            Some(ParsedHadMarket {
                source_updated_at,
                decimal_odds,
            })
        })
        .max_by_key(|market| market.source_updated_at)
        .context("Sporttery HAD response contains no valid market rows")?;

    Ok(football_contracts::ThreeWayOddsSnapshot {
        source: SPORTTERY_SOURCE.to_owned(),
        source_event_id,
        competition_code: None,
        home_team_name,
        away_team_name,
        captured_at,
        source_updated_at: Some(latest.source_updated_at),
        decimal_odds: latest.decimal_odds,
    })
}

#[derive(Debug)]
struct ParsedHadMarket {
    source_updated_at: DateTime<Utc>,
    decimal_odds: [f64; 3],
}

fn parse_market_update_timestamp(date: &str, time: &str) -> Result<DateTime<Utc>> {
    let parsed_date = NaiveDate::parse_from_str(date.trim(), "%Y-%m-%d")
        .context("parse updateDate in Asia/Shanghai market timestamp")?;
    let parsed_time = NaiveTime::parse_from_str(time.trim(), "%H:%M:%S")
        .context("parse updateTime in Asia/Shanghai market timestamp")?;

    let shanghai = chrono::FixedOffset::east_opt(8 * 3600)
        .context("construct Asia/Shanghai timezone offset")?
        .from_local_datetime(&parsed_date.and_time(parsed_time))
        .single()
        .context("resolve local Asia/Shanghai timestamp")?;

    Ok(shanghai.with_timezone(&Utc))
}

fn parse_decimal_odd(raw: &str, label: &str) -> Result<f64> {
    let parsed = raw
        .trim()
        .parse::<f64>()
        .with_context(|| format!("parse {label} odds value '{raw}'"))?;

    if !parsed.is_finite() || parsed <= 1.0 {
        bail!("{label} odds must be a finite decimal greater than 1.0");
    }
    Ok(parsed)
}

pub struct FootballDataClient {
    client: Client,
    token: String,
    last_request_started: Option<std::time::Instant>,
    request_interval: Duration,
}

impl FootballDataClient {
    pub fn new(token: String) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("football-analytics/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()
            .context("build HTTP client")?;
        Ok(Self {
            client,
            token,
            last_request_started: None,
            request_interval: MIN_REQUEST_INTERVAL,
        })
    }

    pub async fn fetch_matches(
        &mut self,
        competition: &str,
        date_from: Option<NaiveDate>,
        date_to: Option<NaiveDate>,
    ) -> Result<CapturedMatches> {
        self.wait_until_ready().await;

        let mut request = self
            .client
            .get(format!(
                "https://api.football-data.org/v4/competitions/{competition}/matches"
            ))
            .header("X-Auth-Token", &self.token);
        let date_from_text = date_from.map(|date| date.format("%Y-%m-%d").to_string());
        let date_to_text = date_to.map(|date| date.format("%Y-%m-%d").to_string());
        if let Some(date) = date_from_text.as_deref() {
            request = request.query(&[("dateFrom", date)]);
        }
        if let Some(date) = date_to_text.as_deref() {
            request = request.query(&[("dateTo", date)]);
        }

        let request_started_at = Utc::now();
        self.last_request_started = Some(std::time::Instant::now());
        let response = request.send().await.context("request football-data.org")?;
        let status = response.status();
        self.request_interval = request_interval_from_headers(response.headers());
        let body = response.bytes().await.context("read API response")?;
        let captured_at = Utc::now();
        if status != StatusCode::OK {
            bail!("football-data.org returned HTTP {status}");
        }
        let parsed: MatchesResponse =
            serde_json::from_slice(&body).context("decode football-data.org matches response")?;
        let matches = parsed.matches;

        Ok(CapturedMatches {
            competition: competition.to_owned(),
            match_count: matches.len(),
            matches,
            payload: body.to_vec(),
            request_started_at,
            captured_at,
            http_status: status.as_u16(),
        })
    }

    pub async fn wait_until_ready(&self) {
        if let Some(started) = self.last_request_started {
            let elapsed = started.elapsed();
            if elapsed < self.request_interval {
                tokio::time::sleep(self.request_interval - elapsed).await;
            }
        }
    }
}

fn request_interval_from_headers(headers: &HeaderMap) -> Duration {
    if let Some(seconds) = headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(seconds).max(MIN_REQUEST_INTERVAL);
    }

    let remaining = headers
        .get("x-requests-available-minute")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    remaining.map_or(MIN_REQUEST_INTERVAL, |remaining| {
        Duration::from_secs(
            (RATE_LIMIT_WINDOW.as_secs() / remaining.saturating_add(1))
                .clamp(MIN_REQUEST_INTERVAL.as_secs(), RATE_LIMIT_WINDOW.as_secs()),
        )
    })
}

pub struct CapturedMatches {
    pub competition: String,
    pub match_count: usize,
    pub matches: Vec<football_contracts::FootballDataMatch>,
    payload: Vec<u8>,
    pub request_started_at: DateTime<Utc>,
    pub captured_at: DateTime<Utc>,
    http_status: u16,
}

pub struct SnapshotStore {
    root: std::path::PathBuf,
}

impl SnapshotStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn write_matches(&self, capture: CapturedMatches) -> Result<SnapshotManifest> {
        let digest = hex::encode(Sha256::digest(&capture.payload));
        let date_partition = capture.captured_at.format("%Y-%m-%d").to_string();
        let directory = self
            .root
            .join(SOURCE)
            .join(format!("captured_date={date_partition}"));
        std::fs::create_dir_all(&directory)
            .with_context(|| format!("create snapshot directory {}", directory.display()))?;

        let stem = format!(
            "{}-matches-{}-{digest}",
            capture.captured_at.timestamp_millis(),
            capture.competition
        );
        let payload_path = directory.join(format!("{stem}.json"));
        let manifest_path = directory.join(format!("{stem}.manifest.json"));
        write_new_file(&payload_path, &capture.payload)?;

        let manifest = SnapshotManifest {
            contract_version: CONTRACT_VERSION,
            source: SOURCE.to_owned(),
            resource: format!("matches:{}", capture.competition),
            captured_at: capture.captured_at,
            http_status: capture.http_status,
            payload_bytes: capture.payload.len() as u64,
            payload_sha256: digest,
        };
        let serialized = serde_json::to_vec_pretty(&manifest).context("serialize manifest")?;
        write_new_file(&manifest_path, &serialized)?;
        Ok(manifest)
    }

    pub fn write_sporttery_response(
        &self,
        resource: &str,
        captured_at: DateTime<Utc>,
        http_status: u16,
        payload: &[u8],
    ) -> Result<SnapshotManifest> {
        if resource.trim().is_empty() || resource.contains(['/', '\\']) {
            bail!("resource must be a non-empty path-safe identifier");
        }
        let digest = hex::encode(Sha256::digest(payload));
        let date_partition = captured_at.format("%Y-%m-%d").to_string();
        let directory = self
            .root
            .join(SPORTTERY_SOURCE)
            .join(format!("captured_date={date_partition}"));
        std::fs::create_dir_all(&directory)
            .with_context(|| format!("create snapshot directory {}", directory.display()))?;

        let stem = format!("{}-{resource}-{digest}", captured_at.timestamp_millis());
        write_new_file(&directory.join(format!("{stem}.json")), payload)?;
        let manifest = SnapshotManifest {
            contract_version: CONTRACT_VERSION,
            source: SPORTTERY_SOURCE.to_owned(),
            resource: resource.to_owned(),
            captured_at,
            http_status,
            payload_bytes: payload.len() as u64,
            payload_sha256: digest,
        };
        write_new_file(
            &directory.join(format!("{stem}.manifest.json")),
            &serde_json::to_vec_pretty(&manifest).context("serialize manifest")?,
        )?;
        Ok(manifest)
    }
}

pub async fn capture_sporttery_resource(
    url: &str,
    resource: &str,
    store: &SnapshotStore,
) -> Result<SnapshotManifest> {
    let response = Client::builder()
        .user_agent(concat!("football-analytics/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()?
        .get(url)
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("request Sporttery resource failed; URL omitted"))?;
    let status = response.status().as_u16();
    let body = response
        .bytes()
        .await
        .context("read Sporttery response body")?;
    store.write_sporttery_response(resource, Utc::now(), status, &body)
}

fn write_new_file(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create append-only snapshot {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("write snapshot {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync snapshot {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use football_contracts::{MatchRecord, MatchesResponse};

    const HAD_FIXTURE: &[u8] = br#"{
            "success": true,
            "errorCode": "0",
            "value": {
                "oddsHistory": {
                    "matchId": 42,
                    "homeTeamAllName": "Home FC",
                    "awayTeamAllName": "Away FC",
                    "hadList": [
                        {"h":"2.52","d":"3.16","a":"2.35","updateDate":"2026-11-01","updateTime":"21:03:01"},
                        {"h":"2.45","d":"3.35","a":"2.31","updateDate":"2026-10-30","updateTime":"09:35:14"}
                    ]
                }
            }
        }"#;

    const FIXTURE: &[u8] = br#"{
      "matches": [{
        "id": 42,
        "utcDate": "2026-10-31T14:00:00Z",
        "status": "FINISHED",
        "matchday": 9,
        "competition": {"code": "PL"},
        "homeTeam": {"id": 1, "name": "Home FC"},
        "awayTeam": {"id": 2, "name": "Away FC"},
        "score": {"fullTime": {"home": 2, "away": 1}}
      }]
    }"#;

    #[test]
    fn parses_completed_match_into_shared_contract() {
        let response: MatchesResponse = serde_json::from_slice(FIXTURE).unwrap();
        let record = MatchRecord::from(response.matches.into_iter().next().unwrap());

        assert_eq!(record.source_match_id, 42);
        assert_eq!(record.competition_code.as_deref(), Some("PL"));
        assert_eq!(record.home_score, Some(2));
        assert_eq!(record.away_score, Some(1));
    }

    #[test]
    fn request_interval_uses_rate_limit_headers_with_safe_fallbacks() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            request_interval_from_headers(&headers),
            MIN_REQUEST_INTERVAL
        );

        headers.insert("x-requests-available-minute", "4".parse().unwrap());
        assert_eq!(
            request_interval_from_headers(&headers),
            Duration::from_secs(12)
        );

        headers.insert("x-requests-available-minute", "0".parse().unwrap());
        assert_eq!(request_interval_from_headers(&headers), RATE_LIMIT_WINDOW);

        headers.insert("retry-after", "15".parse().unwrap());
        assert_eq!(
            request_interval_from_headers(&headers),
            Duration::from_secs(15)
        );

        headers.insert("retry-after", "invalid".parse().unwrap());
        assert_eq!(request_interval_from_headers(&headers), RATE_LIMIT_WINDOW);
    }

    #[test]
    fn accepts_unplayed_match_without_scores() {
        let fixture = br#"{
          "matches": [{
            "id": 43,
            "utcDate": "2026-11-02T18:00:00Z",
            "status": "SCHEDULED",
            "competition": {"code": "PL"},
            "homeTeam": {"id": 1, "name": "Home FC"},
            "awayTeam": {"id": 2, "name": "Away FC"},
            "score": {"fullTime": {"home": null, "away": null}}
          }]
        }"#;
        let response: MatchesResponse = serde_json::from_slice(fixture).unwrap();
        let record = MatchRecord::from(response.matches.into_iter().next().unwrap());

        assert_eq!(record.home_score, None);
        assert_eq!(record.away_score, None);
        assert_eq!(record.matchday, None);
    }

    #[test]
    fn writes_raw_payload_and_hash_manifest_without_overwriting() {
        let directory = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(directory.path());
        let captured_at = Utc::now();
        let capture = || CapturedMatches {
            competition: "PL".to_owned(),
            match_count: 1,
            matches: serde_json::from_slice::<MatchesResponse>(FIXTURE)
                .unwrap()
                .matches,
            payload: FIXTURE.to_vec(),
            request_started_at: captured_at,
            captured_at,
            http_status: 200,
        };

        let manifest = store.write_matches(capture()).unwrap();
        assert_eq!(manifest.payload_bytes, FIXTURE.len() as u64);
        assert_eq!(
            manifest.payload_sha256,
            hex::encode(Sha256::digest(FIXTURE))
        );
        assert!(store.write_matches(capture()).is_err());
        let files = std::fs::read_dir(directory.path().join(SOURCE))
            .unwrap()
            .flat_map(|date_dir| std::fs::read_dir(date_dir.unwrap().path()).unwrap())
            .count();
        assert_eq!(files, 2);
    }

    #[test]
    fn parses_latest_had_market_using_shanghai_time_not_array_order() {
        let captured_at = DateTime::parse_from_rfc3339("2026-11-02T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let snapshot = parse_sporttery_had_v1(HAD_FIXTURE, captured_at).unwrap();

        assert_eq!(snapshot.source, SPORTTERY_SOURCE);
        assert_eq!(snapshot.source_event_id, "42");
        assert_eq!(snapshot.home_team_name, "Home FC");
        assert_eq!(snapshot.away_team_name, "Away FC");
        assert_eq!(snapshot.decimal_odds, [2.52, 3.16, 2.35]);
        assert_eq!(
            snapshot.source_updated_at,
            Some(
                DateTime::parse_from_rfc3339("2026-11-01T13:03:01Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(snapshot.captured_at, captured_at);
    }

    #[test]
    #[ignore = "requires the secured Sporttery fixture directory outside the repository"]
    fn parses_approved_external_fixed_bonus_capture() {
        let fixture_directory = std::env::var("SPORTTERY_FIXTURE_DIR")
            .expect("SPORTTERY_FIXTURE_DIR must point to the secured fixture directory");
        let payload =
            std::fs::read(std::path::Path::new(&fixture_directory).join("fixed-bonus-2xx.json"))
                .unwrap();
        let captured_at = DateTime::parse_from_rfc3339("2026-11-02T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let snapshot = parse_sporttery_had_v1(&payload, captured_at).unwrap();

        assert_eq!(snapshot.source_event_id, "2042426");
        assert_eq!(snapshot.decimal_odds, [2.52, 3.16, 2.35]);
        assert_eq!(
            snapshot.source_updated_at,
            Some(
                DateTime::parse_from_rfc3339("2026-11-01T13:03:01Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn rejects_error_and_malformed_had_payloads() {
        let captured_at = DateTime::parse_from_rfc3339("2026-11-02T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let error_payload = br#"{"success":false,"errorCode":"567"}"#;
        assert!(parse_sporttery_had_v1(error_payload, captured_at).is_err());

        let mut missing_selection: serde_json::Value = serde_json::from_slice(HAD_FIXTURE).unwrap();
        let mut missing_row = missing_selection["value"]["oddsHistory"]["hadList"][0]
            .as_object_mut()
            .unwrap()
            .clone();
        missing_row.remove("a");
        missing_selection["value"]["oddsHistory"]["hadList"] = serde_json::json!([missing_row]);
        let missing_selection_payload = serde_json::to_vec(&missing_selection).unwrap();
        assert!(parse_sporttery_had_v1(&missing_selection_payload, captured_at).is_err());

        let mut changed_shape: serde_json::Value = serde_json::from_slice(HAD_FIXTURE).unwrap();
        let mut changed_row = changed_shape["value"]["oddsHistory"]["hadList"][0]
            .as_object()
            .unwrap()
            .clone();
        changed_row.insert("h".to_owned(), serde_json::json!(2.52));
        changed_shape["value"]["oddsHistory"]["hadList"] = serde_json::json!([changed_row]);
        let changed_shape_payload = serde_json::to_vec(&changed_shape).unwrap();
        assert!(parse_sporttery_had_v1(&changed_shape_payload, captured_at).is_err());
    }

    #[test]
    fn writes_sporttery_raw_body_even_for_upstream_error_status() {
        let directory = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(directory.path());
        let captured_at = Utc::now();
        let payload = br#"{"code":567,"message":"blocked"}"#;
        let manifest = store
            .write_sporttery_response("odds", captured_at, 567, payload)
            .unwrap();

        assert_eq!(manifest.source, SPORTTERY_SOURCE);
        assert_eq!(manifest.http_status, 567);
        assert_eq!(
            manifest.payload_sha256,
            hex::encode(Sha256::digest(payload))
        );
        let directory = directory
            .path()
            .join(SPORTTERY_SOURCE)
            .join(format!("captured_date={}", captured_at.format("%Y-%m-%d")));
        let payload_file = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
                    && !path.to_string_lossy().ends_with(".manifest.json")
            })
            .unwrap();
        assert_eq!(std::fs::read(payload_file).unwrap(), payload);
    }
}
