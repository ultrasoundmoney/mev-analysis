use axum::{extract::State, http::StatusCode, Json};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::postgres::types::PgInterval;

use super::{internal_error, AppState};

type ApiResponse<T> = Result<Json<T>, (StatusCode, String)>;

pub enum Timeframe {
    SevenDays,
    ThirtyDays,
}

impl Timeframe {
    fn to_interval(&self) -> PgInterval {
        match self {
            Timeframe::SevenDays => PgInterval::try_from(Duration::days(7)).unwrap(),
            Timeframe::ThirtyDays => PgInterval::try_from(Duration::days(30)).unwrap(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Timeframed<T> {
    pub seven_days: T,
    pub thirty_days: T,
}

// TODO: Add NOT NULL modifiers to all columns in matviews. These will never actually be null
// but without not null modifiers, sqlx doesn't know this
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LidoOperator {
    pub operator_id: Option<String>,
    pub validator_count: Option<i64>,
    pub relays: Option<Vec<String>>,
}

pub async fn operators(State(state): State<AppState>) -> ApiResponse<Vec<LidoOperator>> {
    sqlx::query_as!(
        LidoOperator,
        r#"
        SELECT
            operator_id,
            validator_count,
            relays
        FROM
            operators_all
        "#,
    )
    .fetch_all(&state.mev_db_pool)
    .await
    .map(Json)
    .map_err(internal_error)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuilderCensorship {
    pub builder_id: Option<String>,
    pub censoring_pubkeys: Option<i64>,
    pub total_pubkeys: Option<i64>,
    pub block_count: Option<i64>,
}

pub async fn builders(
    State(state): State<AppState>,
) -> ApiResponse<Timeframed<Vec<BuilderCensorship>>> {
    let (seven_days, thirty_days) = tokio::try_join!(
        sqlx::query_as!(
            BuilderCensorship,
            r#"
            SELECT
                bid as builder_id,
                COALESCE(censoring_pubkeys, 0) AS censoring_pubkeys,
                COALESCE(total_pubkeys, 0) AS total_pubkeys,
                COALESCE(number_of_blocks, 0) AS block_count
            FROM
                builders_7d
            "#,
        )
        .fetch_all(&state.mev_db_pool),
        sqlx::query_as!(
            BuilderCensorship,
            r#"
            SELECT
                bid as builder_id,
                COALESCE(censoring_pubkeys, 0) AS censoring_pubkeys,
                COALESCE(total_pubkeys, 0) AS total_pubkeys,
                COALESCE(number_of_blocks, 0) AS block_count
            FROM
                builders_30d
            "#,
        )
        .fetch_all(&state.mev_db_pool)
    )
    .map_err(internal_error)?;

    Ok(Json(Timeframed {
        seven_days,
        thirty_days,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayCensorship {
    pub relay_id: Option<String>,
    pub total_blocks: Option<i64>,
    pub uncensored_blocks: Option<i64>,
}

pub async fn relays(
    State(state): State<AppState>,
) -> ApiResponse<Timeframed<Vec<RelayCensorship>>> {
    let (seven_days, thirty_days) = tokio::try_join!(
        sqlx::query_file_as!(
            RelayCensorship,
            "sql/api/relay_censorship.sql",
            Timeframe::SevenDays.to_interval()
        )
        .fetch_all(&state.mev_db_pool),
        sqlx::query_file_as!(
            RelayCensorship,
            "sql/api/relay_censorship.sql",
            Timeframe::ThirtyDays.to_interval()
        )
        .fetch_all(&state.mev_db_pool)
    )
    .map_err(internal_error)?;

    Ok(Json(Timeframed {
        seven_days,
        thirty_days,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CensorshipDelay {
    pub tx_type: Option<String>,
    pub avg_delay: Option<f64>,
    pub tx_count: Option<i64>,
}

pub async fn censorship_categories(
    State(state): State<AppState>,
) -> ApiResponse<Timeframed<Vec<CensorshipDelay>>> {
    let (seven_days, thirty_days) = tokio::try_join!(
        sqlx::query_file_as!(
            CensorshipDelay,
            "sql/api/censorship_delay.sql",
            Timeframe::SevenDays.to_interval()
        )
        .fetch_all(&state.mev_db_pool),
        sqlx::query_file_as!(
            CensorshipDelay,
            "sql/api/censorship_delay.sql",
            Timeframe::ThirtyDays.to_interval()
        )
        .fetch_all(&state.mev_db_pool)
    )
    .map_err(internal_error)?;

    Ok(Json(Timeframed {
        seven_days,
        thirty_days,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InclusionDelay {
    pub delay_type: Option<String>,
    pub avg_delay: Option<f64>,
    pub avg_block_delay: Option<f64>,
    pub tx_count: Option<i64>,
}

pub async fn delay_categories(
    State(state): State<AppState>,
) -> ApiResponse<Timeframed<Vec<InclusionDelay>>> {
    let (seven_days, thirty_days) = tokio::try_join!(
        sqlx::query_as!(
            InclusionDelay,
            r#"
            SELECT
                t_type AS delay_type,
                avg_delay::float,
                avg_block_delay::float,
                n AS tx_count
            FROM inclusion_delay_7d
            "#,
        )
        .fetch_all(&state.mev_db_pool),
        sqlx::query_as!(
            InclusionDelay,
            r#"
            SELECT
                t_type AS delay_type,
                avg_delay::float,
                avg_block_delay::float,
                n AS tx_count
            FROM inclusion_delay_30d
            "#,
        )
        .fetch_all(&state.mev_db_pool)
    )
    .map_err(internal_error)?;

    Ok(Json(Timeframed {
        seven_days,
        thirty_days,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayedTx {
    pub transaction_hash: String,
    pub mined: DateTime<Utc>,
    pub delay: Option<f64>,
    pub block_number: i64,
    pub block_delay: i32,
    pub blacklist: Option<Vec<String>>,
    pub reason: Option<String>,
}

pub async fn delayed_txs(State(state): State<AppState>) -> ApiResponse<Vec<DelayedTx>> {
    sqlx::query_file_as!(
        DelayedTx,
        "sql/api/delayed_txs.sql",
        Timeframe::ThirtyDays.to_interval()
    )
    .fetch_all(&state.mev_db_pool)
    .await
    .map(Json)
    .map_err(internal_error)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CensoredTx {
    pub transaction_hash: String,
    pub mined: DateTime<Utc>,
    pub delay: Option<f64>,
    pub block_number: i64,
    pub block_delay: i32,
    pub blacklist: Vec<String>,
}

pub async fn censored_txs(State(state): State<AppState>) -> ApiResponse<Vec<CensoredTx>> {
    sqlx::query_file_as!(
        CensoredTx,
        "sql/api/censored_txs.sql",
        Timeframe::ThirtyDays.to_interval()
    )
    .fetch_all(&state.mev_db_pool)
    .await
    .map(Json)
    .map_err(internal_error)
}
