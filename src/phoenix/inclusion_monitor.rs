use std::str::FromStr;

use anyhow::Result;
use chrono::Duration;
use reqwest::StatusCode;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, PgPool, Row,
};
use tracing::{error, info, warn};

use crate::{beacon_api::BeaconApi, env::ToNetwork};

use super::env::APP_CONFIG;

#[derive(Debug)]
struct DeliveredPayload {
    slot: i64,
    block_hash: String,
}

struct CheckedPayload {
    slot_number: i64,
    relayed_block_hash: String,
    canonical_block_hash: String,
}

async fn get_last_checked_slot(mev_pool: &PgPool) -> Result<Option<i64>> {
    let slot_number = sqlx::query_scalar!(
        r#"
        SELECT slot_number
        FROM inclusion_monitor
        ORDER BY slot_number DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(mev_pool)
    .await?;

    Ok(slot_number)
}

async fn put_checked_payload(mev_pool: &PgPool, payload: CheckedPayload) -> Result<()> {
    sqlx::query!(
        r#"
            INSERT INTO inclusion_monitor (
                slot_number,
                relayed_block_hash,
                canonical_block_hash
            ) VALUES (
                $1,
                $2,
                $3
            )
            "#,
        payload.slot_number,
        payload.relayed_block_hash,
        payload.canonical_block_hash
    )
    .execute(mev_pool)
    .await?;

    Ok(())
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    checkpoint: &Option<i64>,
) -> Result<Vec<DeliveredPayload>> {
    let query = match checkpoint {
        Some(checkpoint) => format!(
            "
            SELECT
                slot,
                block_hash
            FROM {}_payload_delivered
            WHERE slot > {}
            AND inserted_at <= NOW() - '3 minutes'::interval
            ORDER BY slot ASC
            ",
            &APP_CONFIG.env.to_network().to_string(),
            checkpoint
        ),
        // No checkpoint, get last payload
        None => format!(
            "
            SELECT
                slot,
                block_hash
            FROM {}_payload_delivered
            ORDER BY slot DESC
            LIMIT 1
            ",
            &APP_CONFIG.env.to_network().to_string()
        ),
    };

    sqlx::query(&query)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| DeliveredPayload {
                    slot: row.get("slot"),
                    block_hash: row.get("block_hash"),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub async fn start_inclusion_monitor() -> Result<()> {
    let beacon_api = BeaconApi::new(&APP_CONFIG.consensus_nodes);

    let relay_pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.relay_database_url)
        .await?;

    let mev_opts = PgConnectOptions::from_str(&APP_CONFIG.database_url)?
        .disable_statement_logging()
        .to_owned();
    let mev_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(mev_opts)
        .await?;

    loop {
        let checkpoint = get_last_checked_slot(&mev_pool).await?;
        let payloads = get_delivered_payloads(&relay_pool, &checkpoint).await?;

        info!(
            "validating block hash for {} delivered payloads",
            payloads.len()
        );

        for payload in payloads {
            let block_hash = beacon_api.get_block_hash(&payload.slot).await;

            match block_hash {
                Ok(block_hash) => {
                    if payload.block_hash == block_hash {
                        info!("found matching block hash for slot {}", payload.slot);
                    } else {
                        error!("block hash mismatch for slot {}", payload.slot);
                    }

                    put_checked_payload(
                        &mev_pool,
                        CheckedPayload {
                            slot_number: payload.slot,
                            relayed_block_hash: payload.block_hash,
                            canonical_block_hash: block_hash,
                        },
                    )
                    .await?;
                }
                Err(err) => {
                    if err.status() == Some(StatusCode::NOT_FOUND) {
                        warn!("block not found for slot {}", payload.slot);
                    } else {
                        error!(
                            "error getting block hash for slot {}: {}",
                            payload.slot, err
                        );
                        break;
                    }
                }
            }
        }

        tokio::time::sleep(Duration::minutes(1).to_std()?).await;
    }
}
