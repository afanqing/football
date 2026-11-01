//! Point-in-time feature generation for football matches.

use std::{fs::OpenOptions, path::Path};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use football_contracts::{FeatureRecord, MatchOutcome, SilverMatch};
use polars::prelude::*;

const ROLLING_WINDOW: usize = 5;
const INITIAL_ELO: f64 = 1500.0;
const ELO_K: f64 = 20.0;
const HOME_ADVANTAGE: f64 = 60.0;

#[derive(Debug, Clone)]
struct ResultEvent {
    match_index: usize,
    available_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct TeamResult {
    kickoff_at: DateTime<Utc>,
    scored: u16,
    conceded: u16,
}

#[derive(Debug)]
struct TeamState {
    results: Vec<TeamResult>,
    elo: f64,
}

impl TeamState {
    fn recent_averages(&self) -> (f64, f64, u32) {
        let recent = self.results.iter().rev().take(ROLLING_WINDOW);
        let mut count = 0usize;
        let mut scored = 0u64;
        let mut conceded = 0u64;
        for result in recent {
            count += 1;
            scored += u64::from(result.scored);
            conceded += u64::from(result.conceded);
        }
        if count == 0 {
            return (0.0, 0.0, 0);
        }
        (
            scored as f64 / count as f64,
            conceded as f64 / count as f64,
            count as u32,
        )
    }
}

impl Default for TeamState {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            elo: INITIAL_ELO,
        }
    }
}

pub fn generate_features(matches: &[SilverMatch]) -> Result<Vec<FeatureRecord>> {
    let mut decision_order: Vec<usize> = (0..matches.len()).collect();
    decision_order
        .sort_by_key(|&index| (matches[index].kickoff_at, matches[index].source_match_id));

    let mut result_events: Vec<ResultEvent> = matches
        .iter()
        .enumerate()
        .filter_map(|(match_index, row)| {
            row.result_available_at.map(|available_at| ResultEvent {
                match_index,
                available_at,
            })
        })
        .collect();
    result_events.sort_by_key(|event| {
        (
            event.available_at,
            matches[event.match_index].source_match_id,
        )
    });

    let mut states: std::collections::HashMap<u64, TeamState> = std::collections::HashMap::new();
    let mut names: std::collections::HashMap<String, TeamState> = std::collections::HashMap::new();
    let mut event_cursor = 0usize;
    let mut features = Vec::new();

    for match_index in decision_order {
        let target = &matches[match_index];
        while event_cursor < result_events.len()
            && result_events[event_cursor].available_at < target.kickoff_at
        {
            apply_result(
                &matches[result_events[event_cursor].match_index],
                &mut states,
                &mut names,
            )?;
            event_cursor += 1;
        }

        let home = team_state(&states, &names, target.home_team_id, &target.home_team_name);
        let away = team_state(&states, &names, target.away_team_id, &target.away_team_name);
        let (home_scored, home_conceded, home_prior_matches) = home.recent_averages();
        let (away_scored, away_conceded, away_prior_matches) = away.recent_averages();
        let home_rest_days = rest_days(home, target.kickoff_at);
        let away_rest_days = rest_days(away, target.kickoff_at);
        let Some(available_at) = target.result_available_at else {
            continue;
        };
        let (Some(home_score), Some(away_score)) = (target.home_score, target.away_score) else {
            continue;
        };
        if available_at <= target.kickoff_at {
            bail!(
                "match {} result is available before its kickoff",
                target.source_match_id
            );
        }

        features.push(FeatureRecord {
            source_match_id: target.source_match_id,
            competition_code: target.competition_code.clone(),
            decision_at: target.kickoff_at,
            target_result_available_at: available_at,
            home_team_name: target.home_team_name.clone(),
            away_team_name: target.away_team_name.clone(),
            home_scored_avg_5: home_scored,
            home_conceded_avg_5: home_conceded,
            away_scored_avg_5: away_scored,
            away_conceded_avg_5: away_conceded,
            rest_days_diff: home_rest_days - away_rest_days,
            elo_diff: home.elo - away.elo,
            home_prior_matches,
            away_prior_matches,
            outcome: match_outcome(home_score, away_score),
        });
    }
    Ok(features)
}

fn team_state<'a>(
    states: &'a std::collections::HashMap<u64, TeamState>,
    names: &'a std::collections::HashMap<String, TeamState>,
    team_id: Option<u64>,
    name: &str,
) -> &'a TeamState {
    team_id
        .and_then(|id| states.get(&id))
        .or_else(|| names.get(name))
        .unwrap_or(&EMPTY_TEAM_STATE)
}

static EMPTY_TEAM_STATE: TeamState = TeamState {
    results: Vec::new(),
    elo: INITIAL_ELO,
};

fn rest_days(state: &TeamState, decision_at: DateTime<Utc>) -> f64 {
    state
        .results
        .last()
        .map(|result| (decision_at - result.kickoff_at).num_seconds() as f64 / 86_400.0)
        .unwrap_or(0.0)
}

fn apply_result(
    row: &SilverMatch,
    states: &mut std::collections::HashMap<u64, TeamState>,
    names: &mut std::collections::HashMap<String, TeamState>,
) -> Result<()> {
    let (Some(home_score), Some(away_score)) = (row.home_score, row.away_score) else {
        return Ok(());
    };
    let home_rating = team_state(states, names, row.home_team_id, &row.home_team_name).elo;
    let away_rating = team_state(states, names, row.away_team_id, &row.away_team_name).elo;
    let expected_home =
        1.0 / (1.0 + 10.0_f64.powf((away_rating - home_rating - HOME_ADVANTAGE) / 400.0));
    let actual_home = match match_outcome(home_score, away_score) {
        MatchOutcome::HomeWin => 1.0,
        MatchOutcome::Draw => 0.5,
        MatchOutcome::AwayWin => 0.0,
    };
    let elo_delta = ELO_K * (actual_home - expected_home);

    insert_result(
        states,
        names,
        row.home_team_id,
        &row.home_team_name,
        TeamResult {
            kickoff_at: row.kickoff_at,
            scored: home_score,
            conceded: away_score,
        },
        elo_delta,
    );
    insert_result(
        states,
        names,
        row.away_team_id,
        &row.away_team_name,
        TeamResult {
            kickoff_at: row.kickoff_at,
            scored: away_score,
            conceded: home_score,
        },
        -elo_delta,
    );
    Ok(())
}

fn insert_result(
    states: &mut std::collections::HashMap<u64, TeamState>,
    names: &mut std::collections::HashMap<String, TeamState>,
    team_id: Option<u64>,
    name: &str,
    result: TeamResult,
    elo_delta: f64,
) {
    if let Some(team_id) = team_id {
        let state = states.entry(team_id).or_default();
        state.results.push(result.clone());
        state.elo += elo_delta;
    }
    let state = names.entry(name.to_owned()).or_default();
    if team_id.is_none() {
        state.results.push(result);
        state.elo += elo_delta;
    }
}

fn match_outcome(home: u16, away: u16) -> MatchOutcome {
    match home.cmp(&away) {
        std::cmp::Ordering::Greater => MatchOutcome::HomeWin,
        std::cmp::Ordering::Equal => MatchOutcome::Draw,
        std::cmp::Ordering::Less => MatchOutcome::AwayWin,
    }
}

pub fn write_features_parquet(path: impl AsRef<Path>, features: &[FeatureRecord]) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create immutable Gold file {}", path.display()))?;
    let mut frame = df!(
        "source_match_id" => features.iter().map(|row| row.source_match_id).collect::<Vec<_>>(),
        "competition_code" => features.iter().map(|row| row.competition_code.clone()).collect::<Vec<_>>(),
        "decision_at_ms" => features.iter().map(|row| row.decision_at.timestamp_millis()).collect::<Vec<_>>(),
        "target_result_available_at_ms" => features.iter().map(|row| row.target_result_available_at.timestamp_millis()).collect::<Vec<_>>(),
        "home_team_name" => features.iter().map(|row| row.home_team_name.clone()).collect::<Vec<_>>(),
        "away_team_name" => features.iter().map(|row| row.away_team_name.clone()).collect::<Vec<_>>(),
        "home_scored_avg_5" => features.iter().map(|row| row.home_scored_avg_5).collect::<Vec<_>>(),
        "home_conceded_avg_5" => features.iter().map(|row| row.home_conceded_avg_5).collect::<Vec<_>>(),
        "away_scored_avg_5" => features.iter().map(|row| row.away_scored_avg_5).collect::<Vec<_>>(),
        "away_conceded_avg_5" => features.iter().map(|row| row.away_conceded_avg_5).collect::<Vec<_>>(),
        "rest_days_diff" => features.iter().map(|row| row.rest_days_diff).collect::<Vec<_>>(),
        "elo_diff" => features.iter().map(|row| row.elo_diff).collect::<Vec<_>>(),
        "home_prior_matches" => features.iter().map(|row| row.home_prior_matches).collect::<Vec<_>>(),
        "away_prior_matches" => features.iter().map(|row| row.away_prior_matches).collect::<Vec<_>>(),
        "outcome" => features.iter().map(|row| row.outcome.class_index() as u32).collect::<Vec<_>>(),
    )?;
    anyhow::Context::context(
        ParquetWriter::new(file).finish(&mut frame),
        "write Gold Parquet",
    )?;
    Ok(())
}

pub fn read_features_parquet(path: impl AsRef<Path>) -> Result<Vec<FeatureRecord>> {
    let path = path.as_ref();
    let file =
        std::fs::File::open(path).with_context(|| format!("open Gold file {}", path.display()))?;
    let frame = ParquetReader::new(file).finish()?;
    let ids = frame.column("source_match_id")?.u64()?;
    let competitions = frame.column("competition_code")?.str()?;
    let decision_times = frame.column("decision_at_ms")?.i64()?;
    let result_times = frame.column("target_result_available_at_ms")?.i64()?;
    let home_names = frame.column("home_team_name")?.str()?;
    let away_names = frame.column("away_team_name")?.str()?;
    let home_scored = frame.column("home_scored_avg_5")?.f64()?;
    let home_conceded = frame.column("home_conceded_avg_5")?.f64()?;
    let away_scored = frame.column("away_scored_avg_5")?.f64()?;
    let away_conceded = frame.column("away_conceded_avg_5")?.f64()?;
    let rest_days = frame.column("rest_days_diff")?.f64()?;
    let elo = frame.column("elo_diff")?.f64()?;
    let home_counts = frame.column("home_prior_matches")?.u32()?;
    let away_counts = frame.column("away_prior_matches")?.u32()?;
    let outcomes = frame.column("outcome")?.u32()?;
    let mut features = Vec::with_capacity(frame.height());

    for index in 0..frame.height() {
        let value = |column: &str, value: Option<f64>| {
            value.with_context(|| format!("null {column} at row {index}"))
        };
        let decision_at = timestamp(decision_times.get(index), "decision_at_ms", index)?;
        let target_result_available_at = timestamp(
            result_times.get(index),
            "target_result_available_at_ms",
            index,
        )?;
        if target_result_available_at <= decision_at {
            bail!("Gold row {index} contains a result available at or before decision time");
        }
        let outcome = match outcomes
            .get(index)
            .with_context(|| format!("null outcome at row {index}"))?
        {
            0 => MatchOutcome::HomeWin,
            1 => MatchOutcome::Draw,
            2 => MatchOutcome::AwayWin,
            value => bail!("invalid outcome class {value} at row {index}"),
        };
        features.push(FeatureRecord {
            source_match_id: ids
                .get(index)
                .with_context(|| format!("null source_match_id at row {index}"))?,
            competition_code: competitions.get(index).map(str::to_owned),
            decision_at,
            target_result_available_at,
            home_team_name: home_names
                .get(index)
                .with_context(|| format!("null home_team_name at row {index}"))?
                .to_owned(),
            away_team_name: away_names
                .get(index)
                .with_context(|| format!("null away_team_name at row {index}"))?
                .to_owned(),
            home_scored_avg_5: value("home_scored_avg_5", home_scored.get(index))?,
            home_conceded_avg_5: value("home_conceded_avg_5", home_conceded.get(index))?,
            away_scored_avg_5: value("away_scored_avg_5", away_scored.get(index))?,
            away_conceded_avg_5: value("away_conceded_avg_5", away_conceded.get(index))?,
            rest_days_diff: value("rest_days_diff", rest_days.get(index))?,
            elo_diff: value("elo_diff", elo.get(index))?,
            home_prior_matches: home_counts
                .get(index)
                .with_context(|| format!("null home_prior_matches at row {index}"))?,
            away_prior_matches: away_counts
                .get(index)
                .with_context(|| format!("null away_prior_matches at row {index}"))?,
            outcome,
        });
    }
    Ok(features)
}

fn timestamp(value: Option<i64>, column: &str, row: usize) -> Result<DateTime<Utc>> {
    let value = value.with_context(|| format!("null {column} at row {row}"))?;
    DateTime::from_timestamp_millis(value)
        .with_context(|| format!("invalid timestamp in {column} at row {row}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn match_row(
        id: u64,
        kickoff_hours: i64,
        result_available_hours: Option<i64>,
        home_id: u64,
        away_id: u64,
        score: Option<(u16, u16)>,
    ) -> SilverMatch {
        let kickoff_at =
            DateTime::from_timestamp(1_800_000_000, 0).unwrap() + Duration::hours(kickoff_hours);
        SilverMatch {
            source_match_id: id,
            competition_code: Some("TST".to_owned()),
            kickoff_at,
            result_available_at: result_available_hours
                .map(|hours| kickoff_at + Duration::hours(hours)),
            status: if score.is_some() {
                "FINISHED"
            } else {
                "SCHEDULED"
            }
            .to_owned(),
            home_team_id: Some(home_id),
            home_team_name: format!("Team {home_id}"),
            away_team_id: Some(away_id),
            away_team_name: format!("Team {away_id}"),
            home_score: score.map(|value| value.0),
            away_score: score.map(|value| value.1),
        }
    }

    #[test]
    fn result_is_not_visible_until_strictly_before_decision_time() {
        let rows = vec![
            match_row(1, 0, Some(4), 1, 2, Some((3, 0))),
            match_row(2, 4, Some(4), 1, 2, Some((0, 1))),
            match_row(3, 5, Some(4), 1, 2, Some((2, 0))),
        ];
        let features = generate_features(&rows).unwrap();
        let third = features
            .iter()
            .find(|row| row.source_match_id == 3)
            .unwrap();
        assert_eq!(third.home_prior_matches, 1);
        assert_eq!(third.away_prior_matches, 1);
        assert_eq!(third.home_scored_avg_5, 3.0);
        assert_eq!(third.away_scored_avg_5, 0.0);
    }

    #[test]
    fn generated_labels_are_after_their_decision_time() {
        let rows = vec![match_row(1, 0, Some(4), 1, 2, Some((1, 1)))];
        let features = generate_features(&rows).unwrap();
        assert!(features[0].target_result_available_at > features[0].decision_at);
        assert_eq!(features[0].outcome, MatchOutcome::Draw);
    }

    #[test]
    fn gold_parquet_round_trips_feature_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gold.parquet");
        let expected = generate_features(&[
            match_row(1, 0, Some(4), 1, 2, Some((2, 0))),
            match_row(2, 30, Some(4), 2, 1, Some((1, 1))),
        ])
        .unwrap();
        write_features_parquet(&path, &expected).unwrap();
        let actual = read_features_parquet(&path).unwrap();

        assert_eq!(actual, expected);
        assert!(write_features_parquet(&path, &expected).is_err());
    }
}
