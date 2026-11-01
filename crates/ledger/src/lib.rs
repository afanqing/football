use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use football_contracts::{ThreeWayMarketProbability, ThreeWayOddsSnapshot};
use football_odds::normalize_three_way;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, types::Json};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendDecision {
    pub idempotency_key: String,
    pub match_key: String,
    pub competition_code: String,
    pub market: ThreeWayMarketProbability,
    pub feature_snapshot_sha256: String,
    pub model_version: String,
    pub decided_at: DateTime<Utc>,
    pub model_probabilities: [f64; 3],
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DecisionRecord {
    pub id: i64,
    pub idempotency_key: String,
    pub match_key: String,
    pub competition_code: String,
    pub source: String,
    pub source_event_id: String,
    pub market_snapshot_sha256: String,
    pub feature_snapshot_sha256: String,
    pub model_version: String,
    pub decided_at: DateTime<Utc>,
    pub market_probabilities: Json<ThreeWayMarketProbability>,
    pub model_probabilities: Json<[f64; 3]>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SettlementEvent {
    pub id: i64,
    pub decision_id: i64,
    pub idempotency_key: String,
    pub event_type: String,
    pub result: Json<serde_json::Value>,
    pub settled_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendSettlement {
    pub decision_id: i64,
    pub idempotency_key: String,
    pub event_type: String,
    pub result: serde_json::Value,
    pub settled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct DecisionFilter {
    pub competition_code: Option<String>,
    pub decided_from: Option<DateTime<Utc>>,
    pub decided_before: Option<DateTime<Utc>>,
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

pub async fn connect(database_url: &str) -> Result<PgPool> {
    PgPool::connect(database_url)
        .await
        .context("connect to PostgreSQL")
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("apply decision ledger migrations")?;
    Ok(())
}

pub async fn append_decision(pool: &PgPool, input: &AppendDecision) -> Result<DecisionRecord> {
    validate_decision(input)?;
    let mut transaction = pool.begin().await.context("begin decision transaction")?;
    let inserted = sqlx::query_as::<_, DecisionRecord>(
        "INSERT INTO decision_ledger (
            idempotency_key, match_key, competition_code, source, source_event_id,
            market_snapshot_sha256, feature_snapshot_sha256, model_version, decided_at,
            market_probabilities, model_probabilities
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING id, idempotency_key, match_key, competition_code, source, source_event_id,
                   market_snapshot_sha256, feature_snapshot_sha256, model_version, decided_at,
                   market_probabilities, model_probabilities, created_at",
    )
    .bind(&input.idempotency_key)
    .bind(&input.match_key)
    .bind(&input.competition_code)
    .bind(&input.market.source)
    .bind(&input.market.source_event_id)
    .bind(&input.market.snapshot_sha256)
    .bind(&input.feature_snapshot_sha256)
    .bind(&input.model_version)
    .bind(input.decided_at)
    .bind(Json(input.market.clone()))
    .bind(Json(input.model_probabilities))
    .fetch_optional(&mut *transaction)
    .await
    .context("append decision ledger row")?;

    let record = if let Some(record) = inserted {
        record
    } else {
        let existing = sqlx::query_as::<_, DecisionRecord>(
            "SELECT id, idempotency_key, match_key, competition_code, source, source_event_id,
                    market_snapshot_sha256, feature_snapshot_sha256, model_version, decided_at,
                    market_probabilities, model_probabilities, created_at
             FROM decision_ledger WHERE idempotency_key = $1",
        )
        .bind(&input.idempotency_key)
        .fetch_one(&mut *transaction)
        .await
        .context("load existing idempotent decision")?;
        if !same_decision(&existing, input) {
            bail!("idempotency key already exists with different decision content");
        }
        existing
    };
    transaction
        .commit()
        .await
        .context("commit decision transaction")?;
    Ok(record)
}

pub async fn append_settlement_event(
    pool: &PgPool,
    decision_id: i64,
    idempotency_key: &str,
    event_type: &str,
    result: serde_json::Value,
    settled_at: DateTime<Utc>,
) -> Result<SettlementEvent> {
    if idempotency_key.trim().is_empty() {
        bail!("settlement idempotency key must be non-empty");
    }
    if !matches!(event_type, "settled" | "correction" | "voided") {
        bail!("event type must be settled, correction, or voided");
    }
    let inserted = sqlx::query_as::<_, SettlementEvent>(
        "INSERT INTO decision_settlement_events
            (decision_id, idempotency_key, event_type, result, settled_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING id, decision_id, idempotency_key, event_type, result, settled_at, recorded_at",
    )
    .bind(decision_id)
    .bind(idempotency_key)
    .bind(event_type)
    .bind(Json(result.clone()))
    .bind(settled_at)
    .fetch_optional(pool)
    .await
    .context("append settlement event")?;
    if let Some(event) = inserted {
        return Ok(event);
    }

    let existing = sqlx::query_as::<_, SettlementEvent>(
        "SELECT id, decision_id, idempotency_key, event_type, result, settled_at, recorded_at
         FROM decision_settlement_events WHERE idempotency_key = $1",
    )
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
    .context("load existing idempotent settlement event")?;
    if existing.decision_id != decision_id
        || existing.event_type != event_type
        || existing.result.0 != result
        || existing.settled_at != settled_at
    {
        bail!("settlement idempotency key already exists with different event content");
    }
    Ok(existing)
}

pub async fn list_decisions(pool: &PgPool, filter: &DecisionFilter) -> Result<Vec<DecisionRecord>> {
    let limit = filter.limit.unwrap_or(50).clamp(1, 200);
    sqlx::query_as::<_, DecisionRecord>(
        "SELECT id, idempotency_key, match_key, competition_code, source, source_event_id,
                market_snapshot_sha256, feature_snapshot_sha256, model_version, decided_at,
                market_probabilities, model_probabilities, created_at
         FROM decision_ledger
         WHERE ($1::text IS NULL OR competition_code = $1)
           AND ($2::timestamptz IS NULL OR decided_at >= $2)
           AND ($3::timestamptz IS NULL OR decided_at < $3)
           AND ($4::bigint IS NULL OR id < $4)
         ORDER BY id DESC LIMIT $5",
    )
    .bind(&filter.competition_code)
    .bind(filter.decided_from)
    .bind(filter.decided_before)
    .bind(filter.before_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("list decision ledger rows")
}

pub async fn get_decision(pool: &PgPool, id: i64) -> Result<Option<DecisionRecord>> {
    sqlx::query_as::<_, DecisionRecord>(
        "SELECT id, idempotency_key, match_key, competition_code, source, source_event_id,
                market_snapshot_sha256, feature_snapshot_sha256, model_version, decided_at,
                market_probabilities, model_probabilities, created_at
         FROM decision_ledger WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("get decision ledger row")
}

pub async fn list_settlement_events(
    pool: &PgPool,
    decision_id: i64,
) -> Result<Vec<SettlementEvent>> {
    sqlx::query_as::<_, SettlementEvent>(
        "SELECT id, decision_id, idempotency_key, event_type, result, settled_at, recorded_at
         FROM decision_settlement_events
         WHERE decision_id = $1
         ORDER BY recorded_at, id",
    )
    .bind(decision_id)
    .fetch_all(pool)
    .await
    .context("list decision settlement events")
}

fn validate_decision(input: &AppendDecision) -> Result<()> {
    if input.idempotency_key.trim().is_empty()
        || input.match_key.trim().is_empty()
        || input.competition_code.trim().is_empty()
        || input.model_version.trim().is_empty()
    {
        bail!("decision keys, competition, and model version must be non-empty");
    }
    validate_hash(&input.market.snapshot_sha256, "market snapshot")?;
    validate_hash(&input.feature_snapshot_sha256, "feature snapshot")?;
    let raw_snapshot = ThreeWayOddsSnapshot {
        source: input.market.source.clone(),
        source_event_id: input.market.source_event_id.clone(),
        competition_code: input.market.competition_code.clone(),
        home_team_name: input.market.home_team_name.clone(),
        away_team_name: input.market.away_team_name.clone(),
        captured_at: input.market.captured_at,
        decimal_odds: input.market.decimal_odds,
    };
    let expected_market = normalize_three_way(&raw_snapshot)?;
    if expected_market != input.market {
        bail!("market probability record does not match its source odds snapshot");
    }
    validate_probabilities(&input.market.probabilities, "market")?;
    validate_probabilities(&input.model_probabilities, "model")?;
    Ok(())
}

fn validate_probabilities(probabilities: &[f64; 3], label: &str) -> Result<()> {
    if probabilities
        .iter()
        .any(|value| !value.is_finite() || !(0.0..=1.0).contains(value))
        || (probabilities.iter().sum::<f64>() - 1.0).abs() > 1e-8
    {
        bail!("{label} probabilities must be finite, in [0, 1], and sum to 1");
    }
    Ok(())
}

fn validate_hash(hash: &str, label: &str) -> Result<()> {
    if hash.len() != 64 || hex::decode(hash).is_err() {
        bail!("{label} hash must be a 64-character SHA-256 digest");
    }
    Ok(())
}

fn same_decision(existing: &DecisionRecord, input: &AppendDecision) -> bool {
    existing.match_key == input.match_key
        && existing.competition_code == input.competition_code
        && existing.market_snapshot_sha256 == input.market.snapshot_sha256
        && existing.feature_snapshot_sha256 == input.feature_snapshot_sha256
        && existing.model_version == input.model_version
        && existing.decided_at == input.decided_at
        && existing.market_probabilities.0 == input.market
        && existing.model_probabilities.0 == input.model_probabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn validates_probability_simplex_and_hashes() {
        assert!(validate_probabilities(&[0.4, 0.3, 0.3], "test").is_ok());
        assert!(validate_probabilities(&[0.4, 0.3, 0.2], "test").is_err());
        assert!(validate_probabilities(&[0.4, f64::NAN, 0.6], "test").is_err());
        assert!(validate_hash(&"z".repeat(64), "test").is_err());
        let digest = hex::encode(Sha256::digest(b"snapshot"));
        assert!(validate_hash(&digest, "test").is_ok());
    }

    #[test]
    fn validates_market_record_against_canonical_source_snapshot() {
        let snapshot = ThreeWayOddsSnapshot {
            source: "sporttery".to_owned(),
            source_event_id: "event-1".to_owned(),
            competition_code: Some("TST".to_owned()),
            home_team_name: "Home".to_owned(),
            away_team_name: "Away".to_owned(),
            captured_at: DateTime::from_timestamp(1_800_000_000, 0).unwrap(),
            decimal_odds: [2.0, 3.0, 4.0],
        };
        let market = normalize_three_way(&snapshot).unwrap();
        let mut input = AppendDecision {
            idempotency_key: "decision-1".to_owned(),
            match_key: "TST:event-1".to_owned(),
            competition_code: "TST".to_owned(),
            market: market.clone(),
            feature_snapshot_sha256: hex::encode([0_u8; 32]),
            model_version: "model-v1".to_owned(),
            decided_at: snapshot.captured_at,
            model_probabilities: [0.4, 0.3, 0.3],
        };

        assert!(validate_decision(&input).is_ok());
        input.market.probabilities = [0.5, 0.25, 0.25];
        assert!(validate_decision(&input).is_err());
    }
}
