use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const CONTRACT_VERSION: u16 = 1;

#[derive(Debug, Clone, Deserialize)]
pub struct MatchesResponse {
    pub matches: Vec<FootballDataMatch>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FootballDataMatch {
    pub id: u64,
    #[serde(rename = "utcDate")]
    pub utc_date: DateTime<Utc>,
    pub status: String,
    pub matchday: Option<u32>,
    pub competition: CompetitionRef,
    #[serde(rename = "homeTeam")]
    pub home_team: TeamRef,
    #[serde(rename = "awayTeam")]
    pub away_team: TeamRef,
    pub score: Score,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompetitionRef {
    pub code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TeamRef {
    pub id: Option<u64>,
    pub name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Score {
    #[serde(rename = "fullTime", default)]
    pub full_time: ScorePair,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScorePair {
    pub home: Option<u16>,
    pub away: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchRecord {
    pub source_match_id: u64,
    pub competition_code: Option<String>,
    pub kickoff_at: DateTime<Utc>,
    pub status: String,
    pub matchday: Option<u32>,
    pub home_team_id: Option<u64>,
    pub home_team_name: String,
    pub away_team_id: Option<u64>,
    pub away_team_name: String,
    pub home_score: Option<u16>,
    pub away_score: Option<u16>,
}

impl From<FootballDataMatch> for MatchRecord {
    fn from(value: FootballDataMatch) -> Self {
        Self {
            source_match_id: value.id,
            competition_code: value.competition.code,
            kickoff_at: value.utc_date,
            status: value.status,
            matchday: value.matchday,
            home_team_id: value.home_team.id,
            home_team_name: value.home_team.name,
            away_team_id: value.away_team.id,
            away_team_name: value.away_team.name,
            home_score: value.score.full_time.home,
            away_score: value.score.full_time.away,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SilverMatch {
    pub source_match_id: u64,
    pub competition_code: Option<String>,
    pub kickoff_at: DateTime<Utc>,
    pub result_available_at: Option<DateTime<Utc>>,
    pub status: String,
    pub home_team_id: Option<u64>,
    pub home_team_name: String,
    pub away_team_id: Option<u64>,
    pub away_team_name: String,
    pub home_score: Option<u16>,
    pub away_score: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchOutcome {
    HomeWin,
    Draw,
    AwayWin,
}

impl MatchOutcome {
    pub fn class_index(self) -> usize {
        match self {
            Self::HomeWin => 0,
            Self::Draw => 1,
            Self::AwayWin => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureRecord {
    pub source_match_id: u64,
    pub competition_code: Option<String>,
    pub decision_at: DateTime<Utc>,
    pub target_result_available_at: DateTime<Utc>,
    pub home_team_name: String,
    pub away_team_name: String,
    pub home_scored_avg_5: f64,
    pub home_conceded_avg_5: f64,
    pub away_scored_avg_5: f64,
    pub away_conceded_avg_5: f64,
    pub rest_days_diff: f64,
    pub elo_diff: f64,
    pub home_prior_matches: u32,
    pub away_prior_matches: u32,
    pub outcome: MatchOutcome,
}

impl FeatureRecord {
    pub fn model_features(&self) -> [f64; 8] {
        [
            self.home_scored_avg_5,
            self.home_conceded_avg_5,
            self.away_scored_avg_5,
            self.away_conceded_avg_5,
            self.rest_days_diff,
            self.elo_diff / 400.0,
            self.home_prior_matches as f64,
            self.away_prior_matches as f64,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub contract_version: u16,
    pub source: String,
    pub resource: String,
    pub captured_at: DateTime<Utc>,
    pub http_status: u16,
    pub payload_bytes: u64,
    pub payload_sha256: String,
}
