use std::{fs::OpenOptions, io::Write, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use football_contracts::{CONTRACT_VERSION, MatchesResponse, SnapshotManifest};
use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};

const SOURCE: &str = "football-data.org";
const SPORTTERY_SOURCE: &str = "sporttery";
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(6);

pub struct FootballDataClient {
    client: Client,
    token: String,
    last_request_started: Option<std::time::Instant>,
}

impl FootballDataClient {
    pub fn new(token: String) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("football-analytics/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build HTTP client")?;
        Ok(Self {
            client,
            token,
            last_request_started: None,
        })
    }

    pub async fn fetch_matches(
        &mut self,
        competition: &str,
        date_from: Option<NaiveDate>,
        date_to: Option<NaiveDate>,
    ) -> Result<CapturedMatches> {
        if let Some(started) = self.last_request_started {
            let elapsed = started.elapsed();
            if elapsed < MIN_REQUEST_INTERVAL {
                tokio::time::sleep(MIN_REQUEST_INTERVAL - elapsed).await;
            }
        }

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

        self.last_request_started = Some(std::time::Instant::now());
        let response = request.send().await.context("request football-data.org")?;
        let status = response.status();
        let body = response.bytes().await.context("read API response")?;
        if status != StatusCode::OK {
            bail!("football-data.org returned HTTP {status}");
        }
        let parsed: MatchesResponse =
            serde_json::from_slice(&body).context("decode football-data.org matches response")?;

        Ok(CapturedMatches {
            competition: competition.to_owned(),
            match_count: parsed.matches.len(),
            payload: body.to_vec(),
            captured_at: Utc::now(),
            http_status: status.as_u16(),
        })
    }
}

pub struct CapturedMatches {
    pub competition: String,
    pub match_count: usize,
    payload: Vec<u8>,
    captured_at: DateTime<Utc>,
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
            payload: FIXTURE.to_vec(),
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
