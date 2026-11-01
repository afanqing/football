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
}
