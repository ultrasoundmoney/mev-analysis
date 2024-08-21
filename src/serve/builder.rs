use anyhow::Result;
use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

use super::{internal_error, ApiResponse, AppState};

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Builder {
    extra_data: Option<String>,
    builder_name: String,
    block_count: i64,
}

#[derive(Serialize)]
pub struct BuildersBody {
    builders: Vec<Builder>,
}

pub struct PubkeyBlockCount {
    pub pubkey: String,
    pub block_count: i64,
}

async fn fetch_pubkey_block_counts(relay_pool: &PgPool) -> Result<Vec<PubkeyBlockCount>> {
    let query = "
        SELECT
            builder_pubkey AS pubkey,
            COUNT(*) AS block_count
        FROM
            payload_delivered
        GROUP BY
            builder_pubkey
        ";

    sqlx::query(query)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| PubkeyBlockCount {
                    pubkey: row.get("pubkey"),
                    block_count: row.get("block_count"),
                })
                .collect()
        })
        .map_err(Into::into)
}

struct BuilderIdMapping {
    pubkey: String,
    builder_name: Option<String>,
}

async fn get_top_builders(relay_pool: &PgPool, mev_pool: &PgPool) -> Result<Vec<Builder>> {
    let counts = fetch_pubkey_block_counts(relay_pool).await?;
    let names = sqlx::query_as!(
        BuilderIdMapping,
        r#"
        SELECT
            pubkey,
            builder_name
        FROM
            builder_pubkeys
        WHERE
            pubkey = ANY($1)
        "#,
        &counts
            .iter()
            .map(|count| count.pubkey.clone())
            .collect::<Vec<String>>()
    )
    .fetch_all(mev_pool)
    .await?;

    let aggregated = names
        .iter()
        .fold(HashMap::new(), |mut acc, id| {
            let count = counts
                .iter()
                .find(|count| count.pubkey == id.pubkey)
                .map(|count| count.block_count)
                .unwrap_or(0);

            let entry = acc.entry(id.builder_name.clone()).or_insert(0);
            *entry += count;
            acc
        })
        .into_iter()
        .filter_map(|(name, block_count)| {
            name.map(|builder_name| Builder {
                builder_name,
                block_count,
                // backwards compatibility
                extra_data: Some("".to_string()),
            })
        })
        .collect();

    Ok(aggregated)
}

pub async fn top_builders(State(state): State<AppState>) -> ApiResponse<BuildersBody> {
    get_top_builders(&state.relay_db_pool, &state.mev_db_pool)
        .await
        .map(|builders| Json(BuildersBody { builders }))
        .map_err(internal_error)
}
