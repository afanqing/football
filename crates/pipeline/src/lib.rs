use std::{fs::OpenOptions, path::Path};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use football_contracts::{FootballDataMatch, MatchRecord, MatchesResponse, SilverMatch};
use polars::prelude::*;

const RESULT_AVAILABILITY_HOURS: i64 = 4;

pub fn normalize_payload(payload: &[u8]) -> Result<Vec<SilverMatch>> {
    let response: MatchesResponse =
        serde_json::from_slice(payload).context("decode football-data.org payload")?;
    Ok(response.matches.into_iter().map(normalize_match).collect())
}

fn normalize_match(record: FootballDataMatch) -> SilverMatch {
    let record = MatchRecord::from(record);
    let completed =
        record.status == "FINISHED" && record.home_score.is_some() && record.away_score.is_some();
    SilverMatch {
        source_match_id: record.source_match_id,
        competition_code: record.competition_code,
        kickoff_at: record.kickoff_at,
        result_available_at: completed
            .then(|| record.kickoff_at + Duration::hours(RESULT_AVAILABILITY_HOURS)),
        status: record.status,
        home_team_id: record.home_team_id,
        home_team_name: record.home_team_name,
        away_team_id: record.away_team_id,
        away_team_name: record.away_team_name,
        home_score: record.home_score,
        away_score: record.away_score,
    }
}

pub fn write_silver_parquet(path: impl AsRef<Path>, matches: &[SilverMatch]) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create immutable Silver file {}", path.display()))?;

    let mut frame = df!(
        "source_match_id" => matches.iter().map(|row| row.source_match_id).collect::<Vec<_>>(),
        "competition_code" => matches.iter().map(|row| row.competition_code.clone()).collect::<Vec<_>>(),
        "kickoff_at_ms" => matches.iter().map(|row| row.kickoff_at.timestamp_millis()).collect::<Vec<_>>(),
        "result_available_at_ms" => matches.iter().map(|row| row.result_available_at.map(|time| time.timestamp_millis())).collect::<Vec<_>>(),
        "status" => matches.iter().map(|row| row.status.clone()).collect::<Vec<_>>(),
        "home_team_id" => matches.iter().map(|row| row.home_team_id).collect::<Vec<_>>(),
        "home_team_name" => matches.iter().map(|row| row.home_team_name.clone()).collect::<Vec<_>>(),
        "away_team_id" => matches.iter().map(|row| row.away_team_id).collect::<Vec<_>>(),
        "away_team_name" => matches.iter().map(|row| row.away_team_name.clone()).collect::<Vec<_>>(),
        "home_score" => matches.iter().map(|row| row.home_score.map(i32::from)).collect::<Vec<_>>(),
        "away_score" => matches.iter().map(|row| row.away_score.map(i32::from)).collect::<Vec<_>>(),
    )?;
    anyhow::Context::context(
        ParquetWriter::new(file).finish(&mut frame),
        "write Silver Parquet",
    )?;
    Ok(())
}

pub fn read_silver_parquet(path: impl AsRef<Path>) -> Result<Vec<SilverMatch>> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .with_context(|| format!("open Silver file {}", path.display()))?;
    let frame = anyhow::Context::context(ParquetReader::new(file).finish(), "read Silver Parquet")?;
    let ids = frame.column("source_match_id")?.u64()?;
    let competitions = frame.column("competition_code")?.str()?;
    let kickoffs = frame.column("kickoff_at_ms")?.i64()?;
    let available = frame.column("result_available_at_ms")?.i64()?;
    let statuses = frame.column("status")?.str()?;
    let home_ids = frame.column("home_team_id")?.u64()?;
    let home_names = frame.column("home_team_name")?.str()?;
    let away_ids = frame.column("away_team_id")?.u64()?;
    let away_names = frame.column("away_team_name")?.str()?;
    let home_scores = frame.column("home_score")?.i32()?;
    let away_scores = frame.column("away_score")?.i32()?;
    let mut rows = Vec::with_capacity(frame.height());

    for index in 0..frame.height() {
        let kickoff_at = timestamp(kickoffs.get(index), "kickoff_at_ms", index)?;
        rows.push(SilverMatch {
            source_match_id: ids
                .get(index)
                .with_context(|| format!("null source_match_id at row {index}"))?,
            competition_code: competitions.get(index).map(str::to_owned),
            kickoff_at,
            result_available_at: available
                .get(index)
                .map(|value| timestamp(Some(value), "result_available_at_ms", index))
                .transpose()?,
            status: statuses
                .get(index)
                .with_context(|| format!("null status at row {index}"))?
                .to_owned(),
            home_team_id: home_ids.get(index),
            home_team_name: home_names
                .get(index)
                .with_context(|| format!("null home_team_name at row {index}"))?
                .to_owned(),
            away_team_id: away_ids.get(index),
            away_team_name: away_names
                .get(index)
                .with_context(|| format!("null away_team_name at row {index}"))?
                .to_owned(),
            home_score: home_scores
                .get(index)
                .map(|value| {
                    anyhow::Context::context(u16::try_from(value), "home score out of range")
                })
                .transpose()?,
            away_score: away_scores
                .get(index)
                .map(|value| {
                    anyhow::Context::context(u16::try_from(value), "away score out of range")
                })
                .transpose()?,
        });
    }
    Ok(rows)
}

fn timestamp(value: Option<i64>, column: &str, row: usize) -> Result<chrono::DateTime<Utc>> {
    let value = value.with_context(|| format!("null {column} at row {row}"))?;
    DateTime::from_timestamp_millis(value)
        .with_context(|| format!("invalid timestamp in {column} at row {row}"))
}

pub fn validate_unique_matches(matches: &[SilverMatch]) -> Result<()> {
    let mut ids = std::collections::HashSet::with_capacity(matches.len());
    for row in matches {
        if !ids.insert((row.competition_code.clone(), row.source_match_id)) {
            bail!(
                "duplicate match id {} in competition {:?}",
                row.source_match_id,
                row.competition_code
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = br#"{
      "matches": [
        {"id": 42, "utcDate": "2026-10-31T14:00:00Z", "status": "FINISHED",
         "competition": {"code": "PL"}, "homeTeam": {"id": 1, "name": "Home FC"},
         "awayTeam": {"id": 2, "name": "Away FC"},
         "score": {"fullTime": {"home": 2, "away": 1}}},
        {"id": 43, "utcDate": "2026-11-02T18:00:00Z", "status": "SCHEDULED",
         "competition": {"code": "PL"}, "homeTeam": {"id": 1, "name": "Home FC"},
         "awayTeam": {"id": 2, "name": "Away FC"},
         "score": {"fullTime": {"home": null, "away": null}}}
      ]
    }"#;

    #[test]
    fn normalization_marks_unplayed_results_unavailable() {
        let matches = normalize_payload(FIXTURE).unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(
            matches[0].result_available_at,
            Some(matches[0].kickoff_at + Duration::hours(RESULT_AVAILABILITY_HOURS))
        );
        assert_eq!(matches[1].result_available_at, None);
        assert_eq!(matches[1].home_score, None);
    }

    #[test]
    fn silver_parquet_round_trips_nullable_fields() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("silver.parquet");
        let before = normalize_payload(FIXTURE).unwrap();
        write_silver_parquet(&path, &before).unwrap();
        let after = read_silver_parquet(&path).unwrap();
        assert_eq!(before, after);
        assert!(write_silver_parquet(&path, &before).is_err());
    }
}
