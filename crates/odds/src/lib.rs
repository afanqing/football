use anyhow::{Context, Result, bail};
use football_contracts::{ThreeWayMarketProbability, ThreeWayOddsSnapshot};
use sha2::{Digest, Sha256};

pub fn normalize_three_way(snapshot: &ThreeWayOddsSnapshot) -> Result<ThreeWayMarketProbability> {
    if snapshot.source.trim().is_empty()
        || snapshot.source_event_id.trim().is_empty()
        || snapshot.home_team_name.trim().is_empty()
        || snapshot.away_team_name.trim().is_empty()
    {
        bail!("source, event id, and team names must be non-empty");
    }
    if snapshot
        .decimal_odds
        .iter()
        .any(|odd| !odd.is_finite() || *odd <= 1.0)
    {
        bail!("decimal odds must be finite values greater than 1.0");
    }

    let raw_implied_probabilities = snapshot.decimal_odds.map(|odd| 1.0 / odd);
    let overround: f64 = raw_implied_probabilities.iter().sum();
    if !overround.is_finite() || overround <= 0.0 {
        bail!("invalid implied probability total");
    }
    let probabilities = raw_implied_probabilities.map(|probability| probability / overround);
    let snapshot_bytes = serde_json::to_vec(snapshot).context("serialize odds snapshot")?;
    let snapshot_sha256 = hex::encode(Sha256::digest(snapshot_bytes));

    Ok(ThreeWayMarketProbability {
        snapshot_sha256,
        source: snapshot.source.clone(),
        source_event_id: snapshot.source_event_id.clone(),
        competition_code: snapshot.competition_code.clone(),
        home_team_name: snapshot.home_team_name.clone(),
        away_team_name: snapshot.away_team_name.clone(),
        captured_at: snapshot.captured_at,
        decimal_odds: snapshot.decimal_odds,
        raw_implied_probabilities,
        probabilities,
        overround,
        de_vig_method: "multiplicative".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    fn snapshot(decimal_odds: [f64; 3]) -> ThreeWayOddsSnapshot {
        ThreeWayOddsSnapshot {
            source: "sporttery".to_owned(),
            source_event_id: "fixture-42".to_owned(),
            competition_code: Some("TST".to_owned()),
            home_team_name: "Home FC".to_owned(),
            away_team_name: "Away FC".to_owned(),
            captured_at: DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap(),
            decimal_odds,
        }
    }

    #[test]
    fn removes_overround_with_multiplicative_method_and_hashes_source_snapshot() {
        let input = snapshot([2.0, 3.0, 4.0]);
        let result = normalize_three_way(&input).unwrap();
        let expected_overround = 1.0 / 2.0 + 1.0 / 3.0 + 1.0 / 4.0;

        assert!((result.overround - expected_overround).abs() < 1e-12);
        assert!((result.probabilities.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert_eq!(result.de_vig_method, "multiplicative");
        assert_eq!(result.snapshot_sha256.len(), 64);
        assert_eq!(result.raw_implied_probabilities, [0.5, 1.0 / 3.0, 0.25]);
    }

    #[test]
    fn rejects_invalid_odds_and_missing_identity() {
        assert!(normalize_three_way(&snapshot([2.0, 1.0, 4.0])).is_err());
        let mut missing_identity = snapshot([2.0, 3.0, 4.0]);
        missing_identity.source_event_id.clear();
        assert!(normalize_three_way(&missing_identity).is_err());
    }
}
