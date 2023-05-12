use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use super::{
    internal_error,
    timeframe::{Timeframe, Timeframed},
    ApiResponse, AppState,
};

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct Cursor {
    start_slot: i64,
    end_slot: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockProduction {
    pub slot_number: i64,
    pub block_number: i64,
    pub block_hash: String,
    pub builder_pubkey: Option<String>,
    pub proposer_pubkey: Option<String>,
    pub relays: Option<Vec<String>>,
    pub value: Option<String>,
}

pub async fn block_production(
    cursor: Query<Cursor>,
    State(state): State<AppState>,
) -> ApiResponse<Vec<BlockProduction>> {
    sqlx::query_as!(
        BlockProduction,
        r#"
        SELECT
            slot_number,
            block_number,
            block_hash,
            builder_pubkey,
            proposer_pubkey,
            relays,
            value::text
        FROM block_production
        WHERE
           slot_number >= $1
           AND slot_number <= $2
        ORDER BY slot_number ASC
        LIMIT 200
        "#,
        cursor.start_slot,
        cursor.end_slot
    )
    .fetch_all(&state.mev_db_pool)
    .await
    .map(Json)
    .map_err(internal_error)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayDominance {
    pub relay_id: Option<String>,
    pub block_count: Option<i64>,
    pub total_value: Option<f64>,
    pub avg_value: Option<f64>,
}

pub async fn relay_dominance(
    State(state): State<AppState>,
) -> ApiResponse<Timeframed<Vec<RelayDominance>>> {
    let (seven_days, thirty_days) = tokio::try_join!(
        sqlx::query_as!(
            RelayDominance,
            r#"
            SELECT
                UNNEST(relays) AS relay_id,
                COUNT(*) AS block_count,
                SUM(value / array_length(relays, 1)) / 10^18 AS total_value,
                AVG(value / array_length(relays, 1)) / 10^18 AS avg_value
            FROM
                block_production
            WHERE
                inserted_at >= NOW() - $1::interval
            GROUP BY
                relay_id
            "#,
            Timeframe::SevenDays.to_interval()
        )
        .fetch_all(&state.mev_db_pool),
        sqlx::query_as!(
            RelayDominance,
            r#"
            SELECT
                UNNEST(relays) AS relay_id,
                COUNT(*) AS block_count,
                SUM(value / array_length(relays, 1)) / 10^18 AS total_value,
                AVG(value / array_length(relays, 1)) / 10^18 AS avg_value
            FROM
                block_production
            WHERE
                inserted_at >= NOW() - $1::interval
            GROUP BY
                relay_id
            "#,
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
