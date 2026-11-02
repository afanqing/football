use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use football_ledger::{
    DecisionFilter, DecisionRecord, SettlementEvent, get_decision, list_decisions,
    list_settlement_events,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub struct LedgerQuery {
    pub competition: Option<String>,
    pub decided_from: Option<DateTime<Utc>>,
    pub decided_before: Option<DateTime<Utc>>,
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct LedgerPage {
    pub items: Vec<DecisionRecord>,
    pub next_before_id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ledger", get(list_ledger))
        .route("/ledger/:id", get(get_ledger_record))
        .route("/ledger/:id/settlements", get(get_settlement_events))
        .with_state(pool)
}

async fn health(State(pool): State<PgPool>) -> Result<Json<serde_json::Value>, ApiError> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

async fn list_ledger(
    State(pool): State<PgPool>,
    Query(query): Query<LedgerQuery>,
) -> Result<Json<LedgerPage>, ApiError> {
    validate_query(&query)?;
    let rows = list_decisions(
        &pool,
        &DecisionFilter {
            competition_code: query.competition,
            decided_from: query.decided_from,
            decided_before: query.decided_before,
            before_id: query.before_id,
            limit: query.limit,
        },
    )
    .await
    .map_err(ApiError::internal)?;
    let requested_limit = query.limit.unwrap_or(50).clamp(1, 200) as usize;
    let next_before_id = (rows.len() == requested_limit)
        .then(|| rows.last().map(|record| record.id))
        .flatten();
    Ok(Json(LedgerPage {
        items: rows,
        next_before_id,
    }))
}

fn validate_query(query: &LedgerQuery) -> Result<(), ApiError> {
    if query
        .decided_from
        .zip(query.decided_before)
        .is_some_and(|(from, before)| from >= before)
        || query.before_id.is_some_and(|id| id <= 0)
    {
        return Err(ApiError::bad_request());
    }
    Ok(())
}

async fn get_ledger_record(
    State(pool): State<PgPool>,
    Path(id): Path<i64>,
) -> Result<Json<DecisionRecord>, ApiError> {
    if id <= 0 {
        return Err(ApiError::not_found());
    }
    get_decision(&pool, id)
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn get_settlement_events(
    State(pool): State<PgPool>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<SettlementEvent>>, ApiError> {
    if id <= 0 {
        return Err(ApiError::not_found());
    }
    let events = list_settlement_events(&pool, id)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(events))
}

struct ApiError {
    status: StatusCode,
}

impl ApiError {
    fn bad_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
        }
    }

    fn internal(_: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: "request failed",
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use football_contracts::ThreeWayOddsSnapshot;
    use football_ledger::{AppendDecision, append_decision, append_settlement_event, connect};
    use football_odds::normalize_three_way;
    use sqlx::postgres::PgPoolOptions;

    #[test]
    fn rejects_non_increasing_time_range_and_invalid_cursor() {
        let timestamp = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let invalid_range = LedgerQuery {
            competition: None,
            decided_from: Some(timestamp),
            decided_before: Some(timestamp),
            before_id: None,
            limit: Some(50),
        };
        assert!(validate_query(&invalid_range).is_err());

        let invalid_cursor = LedgerQuery {
            competition: None,
            decided_from: None,
            decided_before: None,
            before_id: Some(0),
            limit: Some(50),
        };
        assert!(validate_query(&invalid_cursor).is_err());
    }

    #[tokio::test]
    async fn registers_read_only_routes_without_database_connection() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://test:test@127.0.0.1/football")
            .unwrap();
        let _router = router(pool);
    }

    #[tokio::test]
    #[ignore = "requires the dedicated football_ledger_test database and writes deterministic fixtures"]
    async fn postgres_api_acceptance_exercises_read_routes() {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = connect(&database_url).await.unwrap();
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(database_name, "football_ledger_test");
        let migration_count_before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
                .fetch_one(&pool)
                .await
                .unwrap();

        let run_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();
        let competition_code = format!("API-{run_id}");
        let decided_at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut records = Vec::new();
        for index in 0..3 {
            let input = api_acceptance_decision(
                &run_id,
                &competition_code,
                index,
                decided_at + Duration::seconds(index),
            );
            records.push(append_decision(&pool, &input).await.unwrap());
        }
        append_settlement_event(
            &pool,
            records[0].id,
            &format!("api-{run_id}-settled"),
            "settled",
            serde_json::json!({"outcome": "home", "score": "1-0"}),
            decided_at + Duration::seconds(10),
        )
        .await
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let api = router(pool.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, api).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base_url = format!("http://{address}");

        let health = client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(
            health.json::<serde_json::Value>().await.unwrap()["status"],
            "ok"
        );

        let filtered = client
            .get(format!("{base_url}/ledger"))
            .query(&[
                ("competition", competition_code.as_str()),
                ("decided_from", "2027-01-15T07:59:59Z"),
                ("decided_before", "2027-01-15T08:00:03Z"),
                ("limit", "2"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(filtered.status(), StatusCode::OK);
        let first_page = filtered.json::<serde_json::Value>().await.unwrap();
        let first_items = first_page["items"].as_array().unwrap();
        assert_eq!(first_items.len(), 2);
        let cursor = first_page["next_before_id"].as_i64().unwrap();
        let first_record_id = first_items[0]["id"].as_i64().unwrap();

        let next_page = client
            .get(format!("{base_url}/ledger"))
            .query(&[
                ("competition", competition_code.as_str()),
                ("before_id", &cursor.to_string()),
                ("limit", "2"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(next_page.status(), StatusCode::OK);
        let next_page = next_page.json::<serde_json::Value>().await.unwrap();
        let next_items = next_page["items"].as_array().unwrap();
        assert_eq!(next_items.len(), 1);
        assert!(next_items[0]["id"].as_i64().unwrap() < cursor);

        let detail = client
            .get(format!("{base_url}/ledger/{first_record_id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let detail = detail.json::<serde_json::Value>().await.unwrap();
        assert_eq!(detail["id"].as_i64().unwrap(), first_record_id);

        let settlements = client
            .get(format!("{base_url}/ledger/{}/settlements", records[0].id))
            .send()
            .await
            .unwrap();
        assert_eq!(settlements.status(), StatusCode::OK);
        let settlements = settlements.json::<serde_json::Value>().await.unwrap();
        assert_eq!(settlements.as_array().unwrap().len(), 1);
        assert_eq!(settlements[0]["event_type"], "settled");

        let missing = client
            .get(format!("{base_url}/ledger/9223372036854775807"))
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let invalid = client
            .get(format!("{base_url}/ledger"))
            .query(&[
                ("decided_from", "2027-01-15T08:00:00Z"),
                ("decided_before", "2027-01-15T08:00:00Z"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

        let migration_count_after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(migration_count_after, migration_count_before);

        pool.close().await;
        let internal_error = client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(internal_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let error_body = internal_error.json::<serde_json::Value>().await.unwrap();
        assert_eq!(error_body["error"], "request failed");
        server.abort();
    }

    fn api_acceptance_decision(
        run_id: &str,
        competition_code: &str,
        index: i64,
        decided_at: DateTime<Utc>,
    ) -> AppendDecision {
        let source_event_id = format!("api-{run_id}-{index}");
        let snapshot = ThreeWayOddsSnapshot {
            source: "api-acceptance-test".to_owned(),
            source_event_id: source_event_id.clone(),
            competition_code: Some(competition_code.to_owned()),
            home_team_name: "Home Test".to_owned(),
            away_team_name: "Away Test".to_owned(),
            captured_at: decided_at,
            source_updated_at: None,
            decimal_odds: [2.0, 3.0, 4.0],
        };
        AppendDecision {
            idempotency_key: format!("api-{run_id}-{index}"),
            match_key: source_event_id,
            competition_code: competition_code.to_owned(),
            market: normalize_three_way(&snapshot).unwrap(),
            feature_snapshot_sha256: "a".repeat(64),
            model_version: "api-acceptance-v1".to_owned(),
            decided_at,
            model_probabilities: [0.4, 0.3, 0.3],
        }
    }
}
